use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use fslock::LockFile;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// ---- TLS connection metadata emission (consumed by Python daemon
// `_try_parse_connection_meta_line` in dpk_daemon.py) -----------------------
//
// Schema written as a single line on stderr:
//   [T3thr:CONNECTION_META] {"tls_fingerprint_sha256": "...",
//                            "tls_subject": "CN=...,O=...",
//                            "tls_subject_alt_names": ["host.example", "host2.example"],
//                            "tls_issuer": "CN=Authority,O=..."}
//
// All four fields are optional and emitted only when a value is present and
// passes UTF-8 sanitization. The Python parser tolerates missing fields.

/// Sanitize an arbitrary byte slice into a safe UTF-8 string for JSON serialization.
///
/// X.509 Subject and Issuer fields can carry malformed bytes from legacy or
/// non-standard certificates. To guarantee the Python `json.loads` parser on
/// the receiving side never encounters invalid UTF-8, we:
///   1. Decode lossily (replacing invalid sequences with U+FFFD).
///   2. Strip ASCII control characters (0x00..0x1F, 0x7F) which would render
///      poorly in audit logs and could confuse JSON line splitting.
///
/// Returns `None` when the input is empty after sanitization, so callers can
/// drop the field instead of emitting an empty string.
/// Expand `${FORS33_SECRET_*}` env-var placeholders inside a string at apply
/// time. Used by REST/WS connectors to resolve auth-bearing header values
/// without persisting the secret to TOML on disk.
///
/// Substitution rules:
///   * Only tokens of the form `${FORS33_SECRET_[A-Z0-9_]+}` are recognised.
///   * If the placeholder is well-formed but the env var is not set, this
///     function returns `Err(...)` so the caller can fail-fast with a
///     non-zero exit code instead of silently transmitting the literal
///     placeholder over the wire (a "fail loud" guarantee for the secret
///     pipeline).
///   * Malformed `${FORS33_SECRET_...}` fragments (e.g. lowercase letters
///     or unmatched `}`) are passed through as literal `${` characters so
///     a single malformed sequence cannot lock the parser into a loop.
///
/// The error message intentionally includes only the placeholder name,
/// never any environment value.
pub fn expand_fors33_secret_placeholders(s: &str) -> Result<String> {
    const PREFIX: &str = "${FORS33_SECRET_";
    if !s.contains(PREFIX) {
        return Ok(s.to_string());
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find(PREFIX) {
        out.push_str(&rest[..start]);
        let after_dollar = &rest[start + 2..];
        if let Some(end_rel) = after_dollar.find('}') {
            let name = &after_dollar[..end_rel];
            let name_ok = !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
            if name_ok {
                match std::env::var(name) {
                    Ok(v) => {
                        out.push_str(&v);
                        rest = &after_dollar[end_rel + 1..];
                        continue;
                    }
                    Err(_) => {
                        return Err(anyhow!(
                            "FORS33_SECRET placeholder unresolved at apply time: {} (env var not set; refusing to transmit literal placeholder)",
                            name
                        ));
                    }
                }
            }
        }
        // Malformed placeholder. Emit the literal `${` and advance past it so
        // we don't loop forever on unmatched prefixes. We do NOT error here
        // because this is not a recognised secret token at all (e.g. a
        // user-typed `${literal}` that happens to start with the same prefix).
        out.push_str("${");
        rest = &rest[start + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

// ---- `T3THR_*` env tables and legacy whole-value `${T3THR_*}` placeholders (standalone compat) -----

/// If `value` is exactly `${VAR}` with VAR matching `[A-Z0-9_]+`, return Some(VAR).
pub fn parse_env_placeholder(value: &str) -> Option<String> {
    const PREFIX: &str = "${";
    const SUFFIX: char = '}';
    if !value.starts_with(PREFIX) || !value.ends_with(SUFFIX) {
        return None;
    }
    let inner = value.get(PREFIX.len()..value.len() - 1)?;
    if inner.is_empty() {
        return None;
    }
    if !inner
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
    {
        return None;
    }
    Some(inner.to_string())
}

/// Env names in `env_*` maps and legacy `${VAR}` placeholders must use the `T3THR_` prefix.
pub fn validate_t3thr_env_var_name(var: &str) -> Result<()> {
    if !var.starts_with("T3THR_") || var.len() <= 6 {
        return Err(anyhow!(
            "environment variable name must start with T3THR_ (got `{var}`)"
        ));
    }
    Ok(())
}

pub fn warn_deprecated_placeholders_in_literal_map(
    map: &HashMap<String, String>,
    literal_path: &str,
    migrate_to: &str,
) {
    for (k, v) in map {
        if parse_env_placeholder(v).is_some() {
            eprintln!(
                "[Fors33] [DEPRECATION] Template parsing in `{literal_path}` (key `{k}`) is deprecated. \
                 Migrate to {migrate_to}: use env var **names** as values (no `${{…}}`). \
                 Put composite wire values (e.g. a full `Bearer …` string) entirely in the environment variable; t3thr does not concatenate strings."
            );
        }
    }
}

pub fn merge_env_binding_map_into(
    dest: &mut HashMap<String, String>,
    bindings: &HashMap<String, String>,
    connector: &str,
    table: &str,
) -> Result<()> {
    for (wire_key, env_var_name) in bindings {
        validate_t3thr_env_var_name(env_var_name)
            .map_err(|e| anyhow!("{e} (in [{connector}.{table}] for wire key `{wire_key}`)"))?;
        let raw = std::env::var(env_var_name).map_err(|_| {
            anyhow!(
                "missing environment variable `{env_var_name}` required by [{connector}.{table}] wire key `{wire_key}`"
            )
        })?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(anyhow!(
                "environment variable `{env_var_name}` is empty after trim (required by [{connector}.{table}] wire key `{wire_key}`)"
            ));
        }
        dest.insert(wire_key.clone(), trimmed.to_string());
    }
    Ok(())
}

pub fn resolve_map_value(value: &str, connector: &str, table: &str, key: &str) -> Result<String> {
    if let Some(var) = parse_env_placeholder(value) {
        validate_t3thr_env_var_name(&var)?;
        let raw = std::env::var(&var).map_err(|_| {
            anyhow!(
                "missing environment variable `{var}` for placeholder in [{connector}.{table}] key `{key}`"
            )
        })?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(anyhow!(
                "environment variable `{var}` is empty after trim (required by [{connector}.{table}] key `{key}`)"
            ));
        }
        Ok(trimmed.to_string())
    } else {
        Ok(value.to_string())
    }
}

pub fn resolve_string_map_placeholders(
    map: &mut HashMap<String, String>,
    connector: &str,
    table: &str,
) -> Result<()> {
    let keys: Vec<String> = map.keys().cloned().collect();
    for k in keys {
        let v = map.get(&k).cloned().unwrap_or_default();
        let resolved = resolve_map_value(&v, connector, table, &k)?;
        map.insert(k, resolved);
    }
    Ok(())
}

pub fn sanitize_utf8_for_json(input: &[u8]) -> Option<String> {
    let lossy = String::from_utf8_lossy(input);
    let cleaned: String = lossy
        .chars()
        .filter(|c| !c.is_control() || *c == '\t')
        .collect();
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Sanitize an already-decoded `&str` candidate into a safe JSON string.
pub fn sanitize_str_for_json(s: &str) -> Option<String> {
    sanitize_utf8_for_json(s.as_bytes())
}

/// Build the canonical CONNECTION_META JSON value.
///
/// Each input is optional. Fields that are `None` after sanitization are
/// omitted entirely, matching the Python parser's "field optional" contract.
/// Output keys are emitted in a fixed order so the line is deterministic and
/// easy to diff in audit logs.
pub fn build_connection_meta_value(
    tls_fingerprint_sha256: Option<&str>,
    tls_subject: Option<&str>,
    tls_subject_alt_names: Option<&[&str]>,
    tls_issuer: Option<&str>,
) -> Value {
    let mut map = serde_json::Map::new();

    if let Some(fp) = tls_fingerprint_sha256.and_then(sanitize_str_for_json) {
        map.insert("tls_fingerprint_sha256".to_string(), json!(fp));
    }
    if let Some(subj) = tls_subject.and_then(sanitize_str_for_json) {
        map.insert("tls_subject".to_string(), json!(subj));
    }
    if let Some(sans) = tls_subject_alt_names {
        let cleaned: Vec<String> = sans
            .iter()
            .filter_map(|s| sanitize_str_for_json(s))
            .collect();
        if !cleaned.is_empty() {
            map.insert("tls_subject_alt_names".to_string(), json!(cleaned));
        }
    }
    if let Some(iss) = tls_issuer.and_then(sanitize_str_for_json) {
        map.insert("tls_issuer".to_string(), json!(iss));
    }
    Value::Object(map)
}

/// Emit a CONNECTION_META log line on stderr. Caller-provided values are
/// sanitized; if every field is empty/invalid, no line is emitted.
pub fn emit_connection_meta(
    tls_fingerprint_sha256: Option<&str>,
    tls_subject: Option<&str>,
    tls_subject_alt_names: Option<&[&str]>,
    tls_issuer: Option<&str>,
) {
    let value = build_connection_meta_value(
        tls_fingerprint_sha256,
        tls_subject,
        tls_subject_alt_names,
        tls_issuer,
    );
    if value.as_object().map(|m| m.is_empty()).unwrap_or(true) {
        return;
    }
    eprintln!("[T3thr:CONNECTION_META] {}", value);
}

/// Read the first non-empty line of a text file.
pub fn read_first_nonempty_line(path: &Path) -> Result<Option<String>> {
    let file = File::open(path).with_context(|| format!("failed opening {}", path.display()))?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            return Ok(Some(trimmed.to_string()));
        }
    }

    Ok(None)
}

/// Reverse-seek helper: read the last non-empty line of a text file efficiently by seeking near EOF.
///
/// Seeks to max(file_size - 1024, 0), reads that tail chunk, and returns the final non-empty line.
#[allow(dead_code)] // Shared utility retained for future state-tail recovery flows.
pub fn read_last_nonempty_line(path: &Path) -> Result<Option<String>> {
    let file = File::open(path).with_context(|| format!("failed opening {}", path.display()))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("failed reading metadata for {}", path.display()))?;
    let len = metadata.len();
    if len == 0 {
        return Ok(None);
    }

    // Seek window chosen to safely capture the final record for local outputs.
    // (T3THR no longer appends chain_hash, but we still want this helper to be robust.)
    const TAIL_WINDOW_BYTES: u64 = 5_242_880; // 5 MiB
    let start = if len > TAIL_WINDOW_BYTES {
        len - TAIL_WINDOW_BYTES
    } else {
        0
    };
    let mut file = file;
    file.seek(SeekFrom::Start(start))?;
    let reader = BufReader::new(file);

    let mut last_nonempty: Option<String> = None;
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            last_nonempty = Some(trimmed.to_string());
        }
    }

    Ok(last_nonempty)
}

/// State tracking for batch mode resume capability.
/// O(1) scalar cursors only - never stores arrays or grows with dataset size.
#[derive(Debug, Serialize, Deserialize)]
pub struct State {
    pub version: u32,
    pub connector_type: String,
    pub status: String, // "in_progress" or "completed"
    /// File connector: last processed file path (lexicographical cursor)
    #[serde(default)]
    pub last_processed_file_path: Option<String>,
    /// REST connector: API cursor for pagination
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Load state from .t3thr-state.json file.
/// Returns Ok(None) if file doesn't exist.
/// Returns Ok(Some(state)) if file exists and is valid.
/// Returns error if file is corrupted (caller should log warning and start fresh).
pub fn load_state(state_path: &Path) -> Result<Option<State>> {
    if !state_path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(state_path)
        .with_context(|| format!("failed reading state file {}", state_path.display()))?;

    let state: State = serde_json::from_str(&content)
        .with_context(|| format!("failed parsing state file {}", state_path.display()))?;

    // Version check
    if state.version != 1 {
        return Err(anyhow!(
            "State file version mismatch: expected 1, got {}",
            state.version
        ));
    }

    Ok(Some(state))
}

/// Save state to .t3thr-state.json file with atomic write.
/// Writes to temp file first, then renames to avoid corruption.
pub fn save_state(state_path: &Path, state: &State) -> Result<()> {
    let temp_path = state_path.with_extension("json.tmp");

    let content = serde_json::to_string_pretty(state).context("failed to serialize state")?;

    fs::write(&temp_path, content)
        .with_context(|| format!("failed writing temp state file {}", temp_path.display()))?;

    fs::rename(&temp_path, state_path).with_context(|| {
        format!(
            "failed renaming temp state file to {}",
            state_path.display()
        )
    })?;

    Ok(())
}

/// Acquire exclusive file lock on state file for concurrent run protection.
/// Returns FileLock handle that releases lock when dropped.
/// Fails immediately if lock is already held by another process.
pub fn acquire_state_lock(state_path: &Path) -> Result<FileLock> {
    // Create parent directory if it doesn't exist
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating state directory {}", parent.display()))?;
    }

    // Create or open state file for locking
    let mut lock = LockFile::open(state_path).with_context(|| {
        format!(
            "failed opening state file for locking {}",
            state_path.display()
        )
    })?;

    // Try to acquire exclusive lock (non-blocking)
    lock.try_lock()
        .with_context(|| "State file is locked by another process")?;

    Ok(FileLock { _lock: lock })
}

/// File lock handle that releases lock on drop.
pub struct FileLock {
    _lock: LockFile,
}

impl Drop for FileLock {
    fn drop(&mut self) {
        // Lock is automatically released when LockFile is dropped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_drops_invalid_utf8_and_control_bytes() {
        // Mix of valid UTF-8, an invalid lead byte, and ASCII control chars.
        let input = b"CN=Test,O=Example\x00\x01\xFFCorp\n";
        let cleaned = sanitize_utf8_for_json(input).expect("should not be empty");
        assert!(!cleaned.contains('\x00'));
        assert!(!cleaned.contains('\x01'));
        assert!(!cleaned.contains('\n'));
        assert!(cleaned.starts_with("CN=Test,O=Example"));
    }

    #[test]
    fn sanitize_returns_none_for_only_invalid() {
        // \x00 alone -> stripped -> empty -> None.
        assert!(sanitize_utf8_for_json(b"\x00\x01").is_none());
        assert!(sanitize_utf8_for_json(b"   ").is_none());
        assert!(sanitize_utf8_for_json(b"").is_none());
    }

    #[test]
    fn expand_fors33_secret_placeholders_replaces_with_env_var() {
        unsafe {
            std::env::set_var("FORS33_SECRET_HEADER_TEST_42", "real-token");
        }
        let out = expand_fors33_secret_placeholders("Bearer ${FORS33_SECRET_HEADER_TEST_42}")
            .expect("should resolve when env is set");
        assert_eq!(out, "Bearer real-token");
        unsafe {
            std::env::remove_var("FORS33_SECRET_HEADER_TEST_42");
        }
    }

    #[test]
    fn expand_fors33_secret_placeholders_errors_when_unset() {
        // Fail-fast: an unset placeholder must produce an Err so the caller
        // terminates the process non-zero. We must never transmit the literal
        // `${...}` over the network as a header value.
        unsafe {
            std::env::remove_var("FORS33_SECRET_HEADER_TEST_NEVER");
        }
        let res = expand_fors33_secret_placeholders("X-${FORS33_SECRET_HEADER_TEST_NEVER}-Y");
        assert!(res.is_err(), "unresolved placeholder must error, got Ok");
        let msg = format!("{}", res.err().unwrap());
        assert!(
            msg.contains("FORS33_SECRET_HEADER_TEST_NEVER"),
            "error must include placeholder name: {msg}",
        );
    }

    #[test]
    fn expand_fors33_secret_placeholders_no_op_without_prefix() {
        assert_eq!(expand_fors33_secret_placeholders("plain").unwrap(), "plain");
        assert_eq!(expand_fors33_secret_placeholders("").unwrap(), "");
    }

    #[test]
    fn expand_fors33_secret_placeholders_passes_through_malformed_token() {
        // Lowercase placeholder is not a recognised FORS33 secret token; must
        // not error and must not be expanded.
        let out = expand_fors33_secret_placeholders("a${not_a_secret}b").unwrap();
        assert_eq!(out, "a${not_a_secret}b");
    }

    #[test]
    fn build_connection_meta_omits_absent_fields() {
        let v = build_connection_meta_value(Some("abc123"), None, None, None);
        let s = serde_json::to_string(&v).unwrap();
        assert!(s.contains("tls_fingerprint_sha256"));
        assert!(!s.contains("tls_subject"));
        assert!(!s.contains("tls_subject_alt_names"));
        assert!(!s.contains("tls_issuer"));
    }

    #[test]
    fn build_connection_meta_emits_all_fields_when_present() {
        let sans = vec!["host1.example", "host2.example"];
        let san_refs: Vec<&str> = sans.iter().map(|s| *s).collect();
        let v = build_connection_meta_value(
            Some("aabbccdd"),
            Some("CN=client.example,O=Acme"),
            Some(&san_refs),
            Some("CN=Authority,O=Acme"),
        );
        let obj = v.as_object().unwrap();
        assert!(obj.contains_key("tls_fingerprint_sha256"));
        assert!(obj.contains_key("tls_subject"));
        assert!(obj.contains_key("tls_subject_alt_names"));
        assert!(obj.contains_key("tls_issuer"));

        let arr = obj
            .get("tls_subject_alt_names")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn build_connection_meta_drops_invalid_san_entries() {
        let cleaned: Vec<String> = vec!["good.example".to_string(), "\x00\x01".to_string()];
        let san_refs: Vec<&str> = cleaned.iter().map(|s| s.as_str()).collect();
        let v = build_connection_meta_value(Some("aabbccdd"), None, Some(&san_refs), None);
        let arr = v
            .as_object()
            .unwrap()
            .get("tls_subject_alt_names")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].as_str().unwrap(), "good.example");
    }

    #[test]
    fn build_connection_meta_returns_empty_object_when_no_inputs() {
        let v = build_connection_meta_value(None, None, None, None);
        assert!(v.as_object().unwrap().is_empty());
    }
}
