use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

/// State file for batch processing resume capability
/// O(1) scalar cursors - never stores arrays or grows with dataset size
#[derive(Debug, Serialize, Deserialize)]
pub struct State {
    pub version: u32,
    pub connector_type: String,
    pub status: String, // "in_progress" | "completed"
    // File connector state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_processed_file_path: Option<String>,
    // REST connector state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl Default for State {
    fn default() -> Self {
        State {
            version: 1,
            connector_type: String::new(),
            status: "in_progress".to_string(),
            last_processed_file_path: None,
            cursor: None,
        }
    }
}

/// Get state file path (hidden dotfile to prevent L3dgr sealing pollution)
pub fn state_file_path(output_dir: &Path) -> PathBuf {
    output_dir.join(".t3thr-state.json")
}

/// Load state from file
pub fn load_state(state_path: &Path) -> Result<Option<State>> {
    if !state_path.exists() {
        return Ok(None);
    }

    let contents = fs::read_to_string(state_path)
        .with_context(|| format!("failed to read state file: {}", state_path.display()))?;

    if contents.trim().is_empty() {
        return Ok(None);
    }

    let state: State = serde_json::from_str(&contents)
        .map_err(|e| anyhow!("corrupted state file: {} (starting fresh)", e))?;

    Ok(Some(state))
}

/// Save state to file (atomic write: temp file + rename)
pub fn save_state(state_path: &Path, state: &State) -> Result<()> {
    // Ensure parent directory exists
    if let Some(parent) = state_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Write to temp file first
    let temp_path = state_path.with_extension("tmp");
    let contents = serde_json::to_string_pretty(state)?;

    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)
            .with_context(|| format!("failed to create temp state file: {}", temp_path.display()))?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
    }

    // Atomic rename
    Ok(())
}

/// Delete state file
pub fn delete_state(state_path: &Path) -> Result<()> {
    if state_path.exists() {
        fs::remove_file(state_path)?;
    }
    Ok(())
}

/// If `value` is exactly `${VAR}` with VAR matching `[A-Z0-9_]+`, return Some(VAR).
/// Otherwise None (treat as literal).
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

/// Env var names used in `${…}` placeholders or `env_*` maps must use the `T3THR_` prefix (vs `FORS33_*` license).
pub fn validate_t3thr_env_var_name(var: &str) -> Result<()> {
    if !var.starts_with("T3THR_") || var.len() <= 6 {
        return Err(anyhow!(
            "environment variable name must start with T3THR_ (got `{var}`)"
        ));
    }
    Ok(())
}

/// If any value is a legacy whole-value `${VAR}` placeholder, warn once per entry (still resolved this release).
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

/// Merge `wire_key → env_var_name` bindings into `dest` (resolved secret values). Same `T3THR_*` name rules; values trimmed.
/// Later keys overwrite earlier ones on collision; `env_*` entries are applied after literals are resolved.
pub fn merge_env_binding_map_into(
    dest: &mut HashMap<String, String>,
    bindings: &HashMap<String, String>,
    connector: &str,
    table: &str,
) -> Result<()> {
    for (wire_key, env_var_name) in bindings {
        validate_t3thr_env_var_name(env_var_name).map_err(|e| {
            anyhow!(
                "{e} (in [{connector}.{table}] for wire key `{wire_key}`)"
            )
        })?;
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

/// Resolve `${VAR}` in a single map value; literals unchanged.
pub fn resolve_map_value(
    value: &str,
    connector: &str,
    table: &str,
    key: &str,
) -> Result<String> {
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

/// Walk a string→string map and replace `${T3THR_*}` values in place.
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
