//! REST polling connector for CME-style or generic HTTP APIs.
//! Polls URL at configurable interval; parses JSON or CSV response.
//! Uses N-dimensional field_paths for JSONPath extraction (supports deep paths like "sensors.0.vitals.heart_rate").

use std::io::Read;
use std::sync::mpsc::SyncSender;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};

use crate::tls_verifier;
use crate::{DataPoint, FilterCfg, FilterState, OutputCfg, now_unix_ms, parse_datetime_to_ns};

const MAX_BYTES: usize = 5_242_880; // 5 MiB

#[derive(Debug, Clone)]
pub struct RestCfg {
    pub url: String,
    pub poll_interval_ms: u64,
    /// Ordered JSONPaths mapping to metrics[index]. Required (synthesized from price_path/volume_path if legacy).
    pub field_paths: Vec<String>,
    /// JSONPath for timestamp (optional). If number, treated as Unix ms; if string, parsed via parse_datetime_to_ns.
    pub timestamp_path: Option<String>,
    pub response_format: String,
    pub mode: Option<String>, // "stream" (default) or "batch"
    /// Pagination: JSONPath for cursor in response (batch mode)
    pub cursor_field: Option<String>,
    /// Pagination: Maximum items per page (batch mode)
    #[allow(dead_code)] // Reserved for batch pagination controls.
    pub page_limit: Option<usize>,
    /// Bearer token populated by the
    /// `FORS33_SECRET_CONNECTOR__REST__TOKEN` env overlay. Sent as
    /// `Authorization: Bearer <token>` on every request when present.
    pub token: Option<String>,
    /// API key populated by `FORS33_SECRET_CONNECTOR__REST__API_KEY`. Sent as
    /// `X-Api-Key: <api_key>` on every request when present.
    pub api_key: Option<String>,
    /// Custom HTTP headers (key/value). Auth-bearing values may use
    /// `${FORS33_SECRET_HEADER_<n>}` placeholders that the bridge env overlay
    /// expands at TOML load time.
    pub headers: Vec<HeaderKv>,
}

#[derive(Debug, Clone)]
pub struct HeaderKv {
    pub key: String,
    pub value: String,
}

/// Construct the default `HeaderMap` reqwest uses for every REST request.
/// Auth fields are populated by the `FORS33_SECRET_CONNECTOR__REST__*` env
/// overlay; this function never logs or persists secret values.
///
/// Returns `Err` (propagated up to the connector caller and ultimately to
/// process exit) if any `${FORS33_SECRET_*}` placeholder fails to resolve at
/// apply time. We never transmit a literal placeholder string over the wire.
pub(crate) fn build_rest_default_headers(cfg: &RestCfg) -> Result<reqwest::header::HeaderMap> {
    let mut hm = reqwest::header::HeaderMap::new();
    if let Some(t) = cfg.token.as_deref() {
        let t = t.trim();
        if !t.is_empty() {
            let v = format!("Bearer {}", t);
            if let Ok(hv) = reqwest::header::HeaderValue::from_str(&v) {
                hm.insert(reqwest::header::AUTHORIZATION, hv);
            }
        }
    }
    if let Some(k) = cfg.api_key.as_deref() {
        let k = k.trim();
        if !k.is_empty() {
            if let Ok(hv) = reqwest::header::HeaderValue::from_str(k) {
                hm.insert(reqwest::header::HeaderName::from_static("x-api-key"), hv);
            }
        }
    }
    for h in cfg.headers.iter() {
        let key = h.key.trim();
        if key.is_empty() {
            continue;
        }
        // Expand any `${FORS33_SECRET_*}` placeholders before forwarding the
        // header value so the actual secret never lives on disk in the TOML.
        // An unresolved placeholder is a hard failure: bail out so the daemon
        // observes a non-zero exit and surfaces it to the operator.
        let expanded = crate::utils::expand_fors33_secret_placeholders(h.value.trim())
            .with_context(|| format!("REST header {:?} placeholder expansion failed", key))?;
        let expanded = expanded.trim();
        if expanded.is_empty() {
            continue;
        }
        let key_lc = key.to_ascii_lowercase();
        if let (Ok(name), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(key_lc.as_bytes()),
            reqwest::header::HeaderValue::from_str(expanded),
        ) {
            hm.insert(name, val);
        }
    }
    Ok(hm)
}

/// Deep JSONPath extraction: supports "field", "nested.field", "array.0.field".
fn json_get_value<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;
    for part in parts {
        current = match current {
            serde_json::Value::Object(map) => map.get(part)?,
            serde_json::Value::Array(arr) => {
                let i: usize = part.parse().ok()?;
                arr.get(i)?
            }
            _ => return None,
        };
    }
    Some(current)
}

fn json_get_f64(value: &serde_json::Value, path: &str) -> Option<f64> {
    let current = json_get_value(value, path)?;
    match current {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn sha256_hex_bytes(raw: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(raw);
    let digest: [u8; 32] = hasher.finalize().into();
    hex::encode(digest)
}

fn shape_deadletter_for_rest_truncation(output_cfg: &OutputCfg, raw: &[u8]) -> String {
    if output_cfg.hash_raw_records {
        return sha256_hex_bytes(raw);
    }

    let preview_len = raw.len().min(512);
    let preview = String::from_utf8_lossy(&raw[..preview_len]).to_string();
    format!("{preview} (truncated, invalid UTF-8 replaced)")
}

fn validate_ssrf_url(url: &str) -> Result<()> {
    use reqwest::Url;
    use std::net::{IpAddr, ToSocketAddrs};

    fn bad_ip(ip: &IpAddr) -> bool {
        match ip {
            IpAddr::V4(v4) => {
                v4.is_loopback()
                    || v4.is_private()
                    || v4.is_link_local()
                    || v4.is_multicast()
                    || v4.is_unspecified()
            }
            IpAddr::V6(v6) => {
                v6.is_loopback()
                    || v6.is_unique_local()
                    || v6.is_unicast_link_local()
                    || v6.is_multicast()
                    || v6.is_unspecified()
            }
        }
    }

    let parsed = Url::parse(url).map_err(|e| anyhow!("invalid REST url: {e}"))?;
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(anyhow!("invalid REST url scheme (allowed: http/https)"));
    }

    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow!("REST url missing host"))?;

    let host_lc = host.to_ascii_lowercase();
    if host_lc == "localhost" || host_lc.ends_with(".local") {
        return Err(anyhow!("REST url host is not allowed"));
    }

    let port = parsed
        .port_or_known_default()
        .ok_or_else(|| anyhow!("REST url missing port"))?;
    if port != 80 && port != 443 {
        return Err(anyhow!("REST url port is not allowed (allowed: 80/443)"));
    }

    // Resolve *all* A/AAAA; if ANY resolve is bad, reject the entire URL.
    let addrs: Vec<_> = (host, port)
        .to_socket_addrs()
        .map_err(|e| anyhow!("failed resolving host: {e}"))?
        .collect();
    if addrs.is_empty() {
        return Err(anyhow!("host did not resolve to any addresses"));
    }
    for a in addrs {
        if bad_ip(&a.ip()) {
            return Err(anyhow!("REST url resolves to blocked address"));
        }
    }

    Ok(())
}

fn parse_json_ndimensional(body: &str, cfg: &RestCfg) -> Result<DataPoint> {
    parse_json_with_paths(body, &cfg.field_paths, cfg.timestamp_path.as_deref())
}

fn parse_json_with_paths(
    body: &str,
    field_paths: &[String],
    timestamp_path: Option<&str>,
) -> Result<DataPoint> {
    let v: serde_json::Value = serde_json::from_str(body).context("invalid JSON")?;
    let mut metrics = Vec::with_capacity(field_paths.len());
    for path in field_paths {
        let value = json_get_f64(&v, path).ok_or_else(|| anyhow!("Missing Field: {}", path))?;
        if !value.is_finite() {
            return Err(anyhow!("Non-finite value at path {}", path));
        }
        metrics.push(value);
    }
    let timestamp_ns = if let Some(ts_path) = timestamp_path {
        let ts_val =
            json_get_value(&v, ts_path).ok_or_else(|| anyhow!("Missing Field: {}", ts_path))?;
        match ts_val {
            serde_json::Value::Number(n) => {
                let ms = n
                    .as_f64()
                    .ok_or_else(|| anyhow!("timestamp at {} must be numeric", ts_path))?;
                (ms as u64) * 1_000_000
            }
            serde_json::Value::String(s) => parse_datetime_to_ns(s, "%Y-%m-%d %H:%M:%S%.f", None)?,
            _ => now_unix_ms() * 1_000_000,
        }
    } else {
        now_unix_ms() * 1_000_000
    };
    Ok(DataPoint {
        timestamp_ns,
        metrics,
    })
}

fn parse_csv_ndimensional(body: &str, field_paths: &[String]) -> Result<DataPoint> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(body.as_bytes());
    let headers = rdr.headers().context("no CSV headers")?;
    let indices: Vec<usize> = field_paths
        .iter()
        .map(|path| {
            headers
                .iter()
                .position(|h| h == path)
                .ok_or_else(|| anyhow!("Missing Field: column '{}' not found", path))
        })
        .collect::<Result<Vec<_>>>()?;
    let row = rdr
        .records()
        .next()
        .transpose()
        .context("CSV parse error")?
        .ok_or_else(|| anyhow!("no data row"))?;
    let mut metrics = Vec::with_capacity(indices.len());
    for (_i, &idx) in indices.iter().enumerate() {
        let value: f64 = row
            .get(idx)
            .ok_or_else(|| anyhow!("Missing Field: column at index {}", idx))?
            .parse()
            .context("invalid numeric value")?;
        metrics.push(value);
    }
    Ok(DataPoint {
        timestamp_ns: now_unix_ms() * 1_000_000,
        metrics,
    })
}

/// Run REST connector; blocks until error or shutdown.
pub fn run_rest_connector(
    cfg: &RestCfg,
    filter_cfg: &FilterCfg,
    output_cfg: &OutputCfg,
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
    state_path: Option<&std::path::Path>,
) -> Result<()> {
    // TLS observability: hand reqwest a preconfigured rustls client whose
    // certificate verifier wraps `WebPkiVerifier` and emits one
    // `[T3thr:CONNECTION_META]` line per successful handshake. reqwest's
    // connection pool means this fires once per (scheme, host, port) tuple
    // for the lifetime of the pooled connection.
    let rustls_cfg = tls_verifier::observing_client_config();
    let default_headers = build_rest_default_headers(cfg).context(
        "REST default headers construction failed (unresolved FORS33_SECRET placeholder)",
    )?;
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .use_preconfigured_tls(rustls_cfg)
        .default_headers(default_headers)
        .build()?;
    let mut state = FilterState::default();

    // Check if batch mode is enabled
    let is_batch = cfg.mode.as_deref() == Some("batch");

    if is_batch {
        eprintln!("[FORS33] REST batch mode: paginating until data exhausted");
        return run_rest_batch_pagination(
            client, cfg, filter_cfg, output_cfg, tx, &mut state, state_path,
        );
    }

    loop {
        if let Err(e) = validate_ssrf_url(&cfg.url) {
            // Clinical rejection, no request made.
            eprintln!("[FORS33] REST SSRF REJECTED: {} | url={}", e, cfg.url);
            std::thread::sleep(Duration::from_millis(cfg.poll_interval_ms));
            continue;
        }

        match client.get(&cfg.url).send() {
            Ok(resp) if resp.status().is_success() => {
                let mut reader = resp.take(MAX_BYTES as u64 + 1);
                let mut buf: Vec<u8> = Vec::new();
                std::io::Read::read_to_end(&mut reader, &mut buf).context("read response body")?;

                if buf.len() > MAX_BYTES {
                    eprintln!(
                        "[FORS33] WARN: REST response exceeded MAX_BYTES ({}). Truncated and dead-lettered. url={}",
                        MAX_BYTES, cfg.url
                    );
                    let shaped =
                        shape_deadletter_for_rest_truncation(output_cfg, &buf[..MAX_BYTES]);
                    let _ = tx.send(Err((
                        format!("REST payload exceeded MAX_BYTES ({MAX_BYTES})"),
                        shaped,
                        None,
                    )));
                    std::thread::sleep(Duration::from_millis(cfg.poll_interval_ms));
                    continue;
                }

                let body = String::from_utf8(buf.clone())
                    .map_err(|_| anyhow!("REST response body is not valid UTF-8"))?;
                let tick = if cfg.response_format.to_lowercase() == "csv" {
                    parse_csv_ndimensional(&body, &cfg.field_paths)
                } else {
                    parse_json_ndimensional(&body, cfg)
                };
                match tick {
                    Ok(t) => match state.check(&t, filter_cfg) {
                        Ok(()) => {
                            if tx.send(Ok(t)).is_err() {
                                eprintln!(
                                    "[FORS33] FATAL: Writer channel closed. Stopping rest connector."
                                );
                                std::process::exit(1);
                            }
                        }
                        Err(reason) => {
                            if tx
                                .send(Err((reason, body.clone(), Some(t.timestamp_ns))))
                                .is_err()
                            {
                                eprintln!(
                                    "[FORS33] FATAL: Writer channel closed. Stopping rest connector."
                                );
                                std::process::exit(1);
                            }
                        }
                    },
                    Err(e) => {
                        if tx
                            .send(Err((format!("Parse Error: {}", e), body.clone(), None)))
                            .is_err()
                        {
                            eprintln!(
                                "[FORS33] FATAL: Writer channel closed. Stopping rest connector."
                            );
                            std::process::exit(1);
                        }
                    }
                }
            }
            Ok(resp) => {
                eprintln!("[BRIDGE] REST HTTP {}: {}", resp.status(), cfg.url);
            }
            Err(e) => {
                eprintln!("[BRIDGE] REST error: {}", e);
            }
        }
        std::thread::sleep(Duration::from_millis(cfg.poll_interval_ms));
    }
}

/// Run REST connector in batch mode with pagination and transient error retry logic.
/// Continues paginating until data is exhausted or max retries exceeded.
fn run_rest_batch_pagination(
    client: reqwest::blocking::Client,
    cfg: &RestCfg,
    filter_cfg: &FilterCfg,
    output_cfg: &OutputCfg,
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
    state: &mut FilterState,
    state_path: Option<&std::path::Path>,
) -> Result<()> {
    let mut current_cursor: Option<String> = None;
    let mut page_count = 0;
    let max_retries = 4;
    let retry_delay_ms: [u64; 4] = [1000, 2000, 4000, 8000]; // Exponential backoff

    // Load state for resume capability
    if let Some(path) = state_path {
        match crate::utils::load_state(path) {
            Ok(Some(state)) => {
                if state.status == "in_progress" {
                    current_cursor = state.cursor;
                    eprintln!(
                        "[FORS33] Resuming from previous run (cursor: {:?})",
                        current_cursor
                    );
                }
            }
            Ok(None) => {
                // No state file, start fresh
            }
            Err(e) => {
                eprintln!(
                    "[WARNING] State file corrupted: {}. Starting batch extraction from zero.",
                    e
                );
            }
        }
    }

    loop {
        let mut request_url = cfg.url.clone();

        // Append pagination parameters if configured
        if let Some(ref cursor) = current_cursor {
            if let Some(ref cursor_field) = cfg.cursor_field {
                // Simple cursor appending (can be enhanced for more complex pagination)
                request_url = format!("{}?{}={}", request_url, cursor_field, cursor);
            }
        }

        // Transient error retry loop
        let mut retry_count = 0;
        let mut response_body: Option<String> = None;

        while retry_count < max_retries {
            if let Err(e) = validate_ssrf_url(&request_url) {
                eprintln!("[FORS33] REST SSRF REJECTED: {} | url={}", e, request_url);
                return Err(e.into());
            }

            match client.get(&request_url).send() {
                Ok(resp) if resp.status().is_success() => {
                    let mut reader = resp.take(MAX_BYTES as u64 + 1);
                    let mut buf: Vec<u8> = Vec::new();
                    std::io::Read::read_to_end(&mut reader, &mut buf)
                        .context("read response body")?;

                    if buf.len() > MAX_BYTES {
                        eprintln!(
                            "[FORS33] WARN: REST response exceeded MAX_BYTES ({}). Truncated and dead-lettered.",
                            MAX_BYTES
                        );
                        let shaped =
                            shape_deadletter_for_rest_truncation(output_cfg, &buf[..MAX_BYTES]);
                        let _ = tx.send(Err((
                            format!("REST payload exceeded MAX_BYTES ({MAX_BYTES})"),
                            shaped,
                            None,
                        )));
                        // Continue to next page even if this page had errors
                        response_body = Some(String::from_utf8(buf.clone()).unwrap_or_default());
                        break;
                    }

                    response_body = Some(
                        String::from_utf8(buf.clone())
                            .map_err(|_| anyhow!("REST response body is not valid UTF-8"))?,
                    );
                    break;
                }
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_server_error() || status == 429 {
                        // 5xx or 429 - transient error, retry with backoff
                        eprintln!(
                            "[FORS33] REST transient error (status {}), retry {}/{}",
                            status,
                            retry_count + 1,
                            max_retries
                        );
                        if retry_count < max_retries - 1 {
                            std::thread::sleep(Duration::from_millis(retry_delay_ms[retry_count]));
                            retry_count += 1;
                        } else {
                            eprintln!("[FORS33] REST max retries exceeded for page {}", page_count);
                            return Err(anyhow!(
                                "REST max retries exceeded after {} attempts",
                                max_retries
                            ));
                        }
                    } else {
                        // 4xx client error (except 429) - not transient, fail immediately
                        eprintln!(
                            "[FORS33] REST client error (status {}): {}",
                            status, request_url
                        );
                        return Err(anyhow!("REST client error: {}", status));
                    }
                }
                Err(e) => {
                    // Network error - transient, retry with backoff
                    eprintln!(
                        "[FORS33] REST network error, retry {}/{}: {}",
                        retry_count + 1,
                        max_retries,
                        e
                    );
                    if retry_count < max_retries - 1 {
                        std::thread::sleep(Duration::from_millis(retry_delay_ms[retry_count]));
                        retry_count += 1;
                    } else {
                        eprintln!("[FORS33] REST max retries exceeded for page {}", page_count);
                        return Err(anyhow!(
                            "REST max retries exceeded after {} attempts: {}",
                            max_retries,
                            e
                        ));
                    }
                }
            }
        }

        let body = match response_body {
            Some(b) => b,
            None => {
                eprintln!(
                    "[FORS33] REST batch mode: failed after retries on page {}",
                    page_count
                );
                return Err(anyhow!("REST batch mode failed on page {}", page_count));
            }
        };

        // Try to parse as array first (batch mode), fall back to single object
        let json_value: serde_json::Value = serde_json::from_str(&body).context("invalid JSON")?;
        let ticks = if let Some(arr) = json_value.as_array() {
            // Array response - parse each element
            let mut results = Vec::new();
            for item in arr {
                let item_str =
                    serde_json::to_string(item).context("failed to serialize array item")?;
                let tick = if cfg.response_format.to_lowercase() == "csv" {
                    parse_csv_ndimensional(&item_str, &cfg.field_paths)
                } else {
                    parse_json_ndimensional(&item_str, cfg)
                };
                match tick {
                    Ok(t) => results.push(t),
                    Err(e) => {
                        eprintln!("[FORS33] Failed to parse array item: {}", e);
                    }
                }
            }
            results
        } else {
            // Single object response
            let tick = if cfg.response_format.to_lowercase() == "csv" {
                parse_csv_ndimensional(&body, &cfg.field_paths)
            } else {
                parse_json_ndimensional(&body, cfg)
            };
            match tick {
                Ok(t) => vec![t],
                Err(_e) => vec![],
            }
        };

        if ticks.is_empty() {
            // No more data - pagination complete
            eprintln!(
                "[FORS33] REST batch mode: no more data (page {})",
                page_count
            );
            eprintln!(
                "[FORS33] REST batch mode complete: {} pages processed",
                page_count
            );
            return Ok(());
        }

        // Process ticks
        for t in ticks {
            match state.check(&t, filter_cfg) {
                Ok(()) => {
                    if tx.send(Ok(t)).is_err() {
                        eprintln!(
                            "[FORS33] FATAL: Writer channel closed. Stopping rest connector."
                        );
                        std::process::exit(1);
                    }
                }
                Err(reason) => {
                    if tx
                        .send(Err((reason, body.clone(), Some(t.timestamp_ns))))
                        .is_err()
                    {
                        eprintln!(
                            "[FORS33] FATAL: Writer channel closed. Stopping rest connector."
                        );
                        std::process::exit(1);
                    }
                }
            }
        }

        // Extract cursor for next page if configured
        if let Some(ref cursor_field) = cfg.cursor_field {
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                if let Some(cursor_value) = json_get_value(&json, cursor_field) {
                    current_cursor = cursor_value.as_str().map(|s| s.to_string());
                    eprintln!(
                        "[FORS33] REST batch mode: extracted cursor for page {}",
                        page_count + 1
                    );

                    // Update state after each successful page
                    if let Some(path) = state_path {
                        let state = crate::utils::State {
                            version: 1,
                            connector_type: "rest".to_string(),
                            status: "in_progress".to_string(),
                            last_processed_file_path: None,
                            cursor: current_cursor.clone(),
                        };
                        if let Err(e) = crate::utils::save_state(path, &state) {
                            eprintln!("[WARNING] Failed to save state: {}", e);
                        }
                    }
                } else {
                    // No cursor in response - assume pagination complete
                    eprintln!(
                        "[FORS33] REST batch mode: no cursor in response, pagination complete"
                    );

                    // Set status to completed
                    if let Some(path) = state_path {
                        let state = crate::utils::State {
                            version: 1,
                            connector_type: "rest".to_string(),
                            status: "completed".to_string(),
                            last_processed_file_path: None,
                            cursor: current_cursor,
                        };
                        if let Err(e) = crate::utils::save_state(path, &state) {
                            eprintln!("[WARNING] Failed to save completion state: {}", e);
                        }
                    }

                    return Ok(());
                }
            }
        } else {
            // No cursor_field configured - assume single page
            eprintln!("[FORS33] REST batch mode: no cursor_field configured, single page complete");

            // Set status to completed
            if let Some(path) = state_path {
                let state = crate::utils::State {
                    version: 1,
                    connector_type: "rest".to_string(),
                    status: "completed".to_string(),
                    last_processed_file_path: None,
                    cursor: current_cursor,
                };
                if let Err(e) = crate::utils::save_state(path, &state) {
                    eprintln!("[WARNING] Failed to save completion state: {}", e);
                }
            }

            return Ok(());
        }

        page_count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_cfg() -> RestCfg {
        RestCfg {
            url: "https://example.com".to_string(),
            poll_interval_ms: 1000,
            field_paths: vec![],
            timestamp_path: None,
            response_format: "json".to_string(),
            mode: None,
            cursor_field: None,
            page_limit: None,
            token: None,
            api_key: None,
            headers: vec![],
        }
    }

    #[test]
    fn build_rest_default_headers_includes_bearer_token() {
        let mut cfg = empty_cfg();
        cfg.token = Some("abc123".to_string());
        let hm = build_rest_default_headers(&cfg).expect("no placeholder, should be Ok");
        assert_eq!(
            hm.get(reqwest::header::AUTHORIZATION).unwrap(),
            "Bearer abc123"
        );
    }

    #[test]
    fn build_rest_default_headers_includes_api_key() {
        let mut cfg = empty_cfg();
        cfg.api_key = Some("k-xyz".to_string());
        let hm = build_rest_default_headers(&cfg).expect("no placeholder, should be Ok");
        assert_eq!(hm.get("x-api-key").unwrap(), "k-xyz");
    }

    #[test]
    fn build_rest_default_headers_merges_custom_headers() {
        let mut cfg = empty_cfg();
        cfg.headers = vec![HeaderKv {
            key: "X-Trace".to_string(),
            value: "1".to_string(),
        }];
        let hm = build_rest_default_headers(&cfg).expect("no placeholder, should be Ok");
        assert_eq!(hm.get("x-trace").unwrap(), "1");
    }

    #[test]
    fn build_rest_default_headers_skips_empty_fields() {
        let mut cfg = empty_cfg();
        cfg.token = Some("   ".to_string());
        cfg.api_key = Some("".to_string());
        cfg.headers = vec![HeaderKv {
            key: "  ".to_string(),
            value: "x".to_string(),
        }];
        let hm = build_rest_default_headers(&cfg).expect("no placeholder, should be Ok");
        assert!(hm.get(reqwest::header::AUTHORIZATION).is_none());
        assert!(hm.get("x-api-key").is_none());
        assert!(hm.is_empty());
    }

    #[test]
    fn build_rest_default_headers_fails_when_placeholder_unresolved() {
        unsafe {
            std::env::remove_var("FORS33_SECRET_HEADER_REST_TEST_MISSING");
        }
        let mut cfg = empty_cfg();
        cfg.headers = vec![HeaderKv {
            key: "Authorization".to_string(),
            value: "Bearer ${FORS33_SECRET_HEADER_REST_TEST_MISSING}".to_string(),
        }];
        let res = build_rest_default_headers(&cfg);
        assert!(res.is_err(), "unresolved placeholder must propagate error");
    }
}
