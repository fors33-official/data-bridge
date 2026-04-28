//! REST polling connector for CME-style or generic HTTP APIs.
//! Polls URL at configurable interval; parses JSON or CSV response.
//! Uses N-dimensional field_paths for JSONPath extraction (supports deep paths like "sensors.0.vitals.heart_rate").

use std::collections::HashMap;
use std::sync::mpsc::SyncSender;
use std::time::Duration;
use std::io::Read;

use anyhow::{anyhow, Context, Result};

use crate::{now_unix_ms, parse_datetime_to_ns, DataPoint, FilterCfg, FilterState, OutputCfg};

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
    /// Resolved HTTP headers (placeholders already substituted by main).
    pub headers: HashMap<String, String>,
    /// Mode: "stream" (default) or "batch" for historical data extraction
    pub mode: Option<String>,
    /// Pagination cursor field for batch mode (JSONPath in response)
    pub cursor_field: Option<String>,
    /// Maximum items per page for batch mode
    pub page_limit: Option<usize>,
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
    let v: serde_json::Value = serde_json::from_str(body).context("invalid JSON")?;
    let mut metrics = Vec::with_capacity(cfg.field_paths.len());
    for path in &cfg.field_paths {
        let value = json_get_f64(&v, path)
            .ok_or_else(|| anyhow!("Missing Field: {}", path))?;
        if !value.is_finite() {
            return Err(anyhow!("Non-finite value at path {}", path));
        }
        metrics.push(value);
    }
    let timestamp_ns = if let Some(ref ts_path) = cfg.timestamp_path {
        let ts_val = json_get_value(&v, ts_path)
            .ok_or_else(|| anyhow!("Missing Field: {}", ts_path))?;
        match ts_val {
            serde_json::Value::Number(n) => {
                let ms = n.as_f64().ok_or_else(|| anyhow!("timestamp at {} must be numeric", ts_path))?;
                (ms as u64) * 1_000_000
            }
            serde_json::Value::String(s) => {
                parse_datetime_to_ns(s, "%Y-%m-%d %H:%M:%S%.f", None)?
            }
            _ => now_unix_ms() * 1_000_000,
        }
    } else {
        now_unix_ms() * 1_000_000
    };
    Ok(DataPoint { timestamp_ns, metrics })
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
    tx: SyncSender<Result<DataPoint, (String, String)>>,
) -> Result<()> {
    let is_batch = cfg.mode.as_deref() == Some("batch");

    let mut builder = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy();

    if !cfg.headers.is_empty() {
        let mut header_map = reqwest::header::HeaderMap::new();
        for (k, v) in &cfg.headers {
            let name = reqwest::header::HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
                anyhow!("invalid HTTP header name `{k}`: {e}")
            })?;
            let value = reqwest::header::HeaderValue::from_str(v).map_err(|e| {
                anyhow!("invalid HTTP header value for `{k}`: {e}")
            })?;
            header_map.insert(name, value);
        }
        builder = builder.default_headers(header_map);
    }

    let client = builder.build()?;
    let mut state = FilterState::default();

    // Batch mode: process once and exit
    if is_batch {
        let mut cursor: Option<String> = None;
        let mut total_processed = 0;

        loop {
            let url = if let Some(ref c) = cursor {
                // Append cursor to URL (simple query param approach)
                if let Some(ref cursor_field) = cfg.cursor_field {
                    format!("{}?{}={}", cfg.url, cursor_field, c)
                } else {
                    cfg.url.clone()
                }
            } else {
                cfg.url.clone()
            };

            if let Err(e) = validate_ssrf_url(&url) {
                eprintln!("[Fors33] REST SSRF REJECTED: {} | url={}", e, url);
                break;
            }

            match client.get(&url).send() {
                Ok(resp) if resp.status().is_success() => {
                    let mut reader = resp.take(MAX_BYTES as u64 + 1);
                    let mut buf: Vec<u8> = Vec::new();
                    std::io::Read::read_to_end(&mut reader, &mut buf).context("read response body")?;

                    if buf.len() > MAX_BYTES {
                        eprintln!("[Fors33] REST response exceeded MAX_BYTES");
                        break;
                    }

                    let body = String::from_utf8(buf.clone()).map_err(|_| anyhow!("REST response body is not valid UTF-8"))?;
                    let tick = if cfg.response_format.to_lowercase() == "csv" {
                        parse_csv_ndimensional(&body, &cfg.field_paths)
                    } else {
                        parse_json_ndimensional(&body, cfg)
                    };

                    match tick {
                        Ok(t) => {
                            match state.check(&t, filter_cfg) {
                                Ok(()) => {
                                    if tx.send(Ok(t)).is_err() {
                                        eprintln!("[Fors33] Writer channel closed");
                                        break;
                                    }
                                    total_processed += 1;
                                }
                                Err(reason) => {
                                    if tx.send(Err((reason, body.clone()))).is_err() {
                                        eprintln!("[Fors33] Writer channel closed");
                                        break;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("[Fors33] Parse error: {}", e);
                        }
                    }

                    // Check for pagination cursor
                    if let Some(ref cursor_field) = cfg.cursor_field {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                            if let Some(cursor_val) = json_get_value(&json, cursor_field) {
                                if let Some(cursor_str) = cursor_val.as_str() {
                                    cursor = Some(cursor_str.to_string());
                                    // Check page limit
                                    if let Some(limit) = cfg.page_limit {
                                        if total_processed >= limit {
                                            eprintln!("[Fors33] Batch mode: reached page limit {}", limit);
                                            break;
                                        }
                                    }
                                    continue;
                                }
                            }
                        }
                    }
                    // No cursor found, end of pagination
                    break;
                }
                Ok(resp) => {
                    eprintln!("[BRIDGE] REST HTTP {}: {}", resp.status(), url);
                    break;
                }
                Err(e) => {
                    eprintln!("[BRIDGE] REST error: {}", e);
                    break;
                }
            }
        }

        eprintln!("[Fors33] Batch mode complete: processed {} records", total_processed);
        return Ok(());
    }

    // Stream mode: continuous polling
    loop {
        if let Err(e) = validate_ssrf_url(&cfg.url) {
            // Clinical rejection, no request made.
            eprintln!("[Fors33] REST SSRF REJECTED: {} | url={}", e, cfg.url);
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
                        "[Fors33] WARN: REST response exceeded MAX_BYTES ({}). Truncated and dead-lettered. url={}",
                        MAX_BYTES,
                        cfg.url
                    );
                    let shaped = shape_deadletter_for_rest_truncation(output_cfg, &buf[..MAX_BYTES]);
                    let _ = tx.send(Err((
                        format!("REST payload exceeded MAX_BYTES ({MAX_BYTES})"),
                        shaped,
                    )));
                    std::thread::sleep(Duration::from_millis(cfg.poll_interval_ms));
                    continue;
                }

                let body = String::from_utf8(buf.clone()).map_err(|_| anyhow!("REST response body is not valid UTF-8"))?;
                let tick = if cfg.response_format.to_lowercase() == "csv" {
                    parse_csv_ndimensional(&body, &cfg.field_paths)
                } else {
                    parse_json_ndimensional(&body, cfg)
                };
                match tick {
                    Ok(t) => {
                        match state.check(&t, filter_cfg) {
                            Ok(()) => {
                                if tx.send(Ok(t)).is_err() {
                                    eprintln!("[Fors33] FATAL: Writer channel closed. Stopping rest connector.");
                                    std::process::exit(1);
                                }
                            }
                            Err(reason) => {
                                if tx.send(Err((reason, body.clone()))).is_err() {
                                    eprintln!("[Fors33] FATAL: Writer channel closed. Stopping rest connector.");
                                    std::process::exit(1);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if tx.send(Err((format!("Parse Error: {}", e), body.clone()))).is_err() {
                            eprintln!("[Fors33] FATAL: Writer channel closed. Stopping rest connector.");
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
