//! UDP Raw connector for ingesting JSON datagrams.
//! Supports binding to specific addresses with configurable datagram size limits.

use std::net::SocketAddr;
use std::sync::mpsc::SyncSender;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use tokio::net::UdpSocket;

use crate::{now_unix_ms, DataPoint, FilterCfg, FilterState};

const DEFAULT_MAX_DATAGRAM_BYTES: usize = 65507; // Max UDP payload size
const MIN_DATAGRAM_BYTES: usize = 512;
const MAX_DATAGRAM_BYTES_LIMIT: usize = 1_048_576; // 1 MiB

#[derive(Debug, Deserialize, Clone)]
pub struct UdpRawCfg {
    #[serde(default = "default_udp_bind")]
    pub bind: String, // "0.0.0.0" | "::" | "127.0.0.1"
    #[serde(default)]
    pub port: u16,
    #[serde(default = "default_max_datagram_bytes")]
    pub max_datagram_bytes: usize, // 512 to 1048576
    pub timestamp_path: Option<String>, // JSONPath in payload for timestamp
    #[serde(default)]
    pub field_paths: Vec<String>, // JSONPaths to extract metrics
}

fn default_udp_bind() -> String {
    "0.0.0.0".to_string()
}

fn default_max_datagram_bytes() -> usize {
    DEFAULT_MAX_DATAGRAM_BYTES
}

impl UdpRawCfg {
    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        // Validate bind address
        match self.bind.as_str() {
            "0.0.0.0" | "::" | "127.0.0.1" | "::1" => {}
            addr => {
                // Try to parse as IP address
                if addr.parse::<std::net::IpAddr>().is_err() {
                    return Err(anyhow!("invalid bind address: {} (allowed: 0.0.0.0, ::, 127.0.0.1, or valid IP)", addr));
                }
            }
        }

        // Validate max_datagram_bytes
        if self.max_datagram_bytes < MIN_DATAGRAM_BYTES {
            return Err(anyhow!(
                "max_datagram_bytes must be at least {} bytes (got {})",
                MIN_DATAGRAM_BYTES,
                self.max_datagram_bytes
            ));
        }
        if self.max_datagram_bytes > MAX_DATAGRAM_BYTES_LIMIT {
            return Err(anyhow!(
                "max_datagram_bytes must not exceed {} bytes (got {})",
                MAX_DATAGRAM_BYTES_LIMIT,
                self.max_datagram_bytes
            ));
        }

        // Validate port
        if self.port == 0 {
            return Err(anyhow!("port must be specified and non-zero"));
        }

        Ok(())
    }
}

/// Deep JSONPath extraction: supports "field", "nested.field", "array.0.field"
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

/// Process a single UDP datagram
fn process_datagram(
    data: &[u8],
    cfg: &UdpRawCfg,
    _filter_state: &mut FilterState,
    peer_addr: SocketAddr,
) -> Result<DataPoint> {
    // Parse as UTF-8
    let json_str = std::str::from_utf8(data)
        .map_err(|e| anyhow!("invalid UTF-8 in datagram from {}: {}", peer_addr, e))?;

    // Parse JSON
    let json: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| anyhow!("invalid JSON in datagram from {}: {}", peer_addr, e))?;

    // Extract timestamp
    let timestamp_ns = if let Some(ref ts_path) = cfg.timestamp_path {
        if let Some(ts_value) = json_get_value(&json, ts_path) {
            if let Some(ms) = ts_value.as_f64() {
                (ms * 1_000_000.0) as u64
            } else {
                now_unix_ms() * 1_000_000
            }
        } else {
            now_unix_ms() * 1_000_000
        }
    } else {
        now_unix_ms() * 1_000_000
    };

    // Extract metrics
    let mut metrics = Vec::with_capacity(cfg.field_paths.len());

    if cfg.field_paths.is_empty() {
        // If no field paths specified, try to extract all numeric values at root level
        if let serde_json::Value::Object(map) = &json {
            for (_, v) in map.iter() {
                if let Some(n) = v.as_f64() {
                    metrics.push(n);
                }
            }
        }

        // If still no metrics, treat the entire payload as a single string metric
        if metrics.is_empty() {
            metrics.push(0.0); // Placeholder for non-numeric payload
        }
    } else {
        // Extract specified fields
        for path in &cfg.field_paths {
            let val = json_get_f64(&json, path).unwrap_or(0.0);
            metrics.push(val);
        }
    }

    Ok(DataPoint {
        timestamp_ns,
        metrics,
    })
}

/// Run UDP raw connector
pub async fn run_udp_raw_connector(
    cfg: &UdpRawCfg,
    tx: SyncSender<Result<DataPoint, (String, String)>>,
    filter_cfg: &FilterCfg,
) -> Result<()> {
    // Validate config
    cfg.validate()?;

    // Bind to socket
    let bind_addr = format!("{}:{}", cfg.bind, cfg.port);
    let socket = UdpSocket::bind(&bind_addr).await
        .with_context(|| format!("failed to bind UDP socket to {}", bind_addr))?;

    eprintln!("[Fors33] UDP Raw server listening on {}", bind_addr);

    // Prepare buffer
    let mut buf = vec![0u8; cfg.max_datagram_bytes];
    let mut filter_state = FilterState::with_capacity(2); // UDP raw typically produces 2 metrics

    loop {
        // Receive datagram
        let (len, peer_addr) = match socket.recv_from(&mut buf).await {
            Ok((len, addr)) => (len, addr),
            Err(e) => {
                eprintln!("[Fors33] UDP receive error: {}", e);
                continue;
            }
        };

        // Process datagram
        match process_datagram(&buf[..len], cfg, &mut filter_state, peer_addr) {
            Ok(data_point) => {
                // Apply filter
                match filter_state.check(&data_point, filter_cfg) {
                    Ok(()) => {
                        if let Err(_) = tx.send(Ok(data_point)) {
                            return Err(anyhow!("channel closed"));
                        }
                    }
                    Err(reason) => {
                        tx.send(Err((reason, "UDP raw filter violation".to_string())))
                            .map_err(|_| anyhow!("channel closed"))?;
                    }
                }
            }
            Err(e) => {
                // Send to dead-letter
                let raw_record = String::from_utf8_lossy(&buf[..len]).to_string();
                if let Err(_) = tx.send(Err((
                    format!("UDP datagram processing error: {}", e),
                    raw_record,
                ))) {
                    return Err(anyhow!("channel closed"));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = UdpRawCfg {
            bind: default_udp_bind(),
            port: 8080,
            max_datagram_bytes: default_max_datagram_bytes(),
            timestamp_path: None,
            field_paths: vec![],
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_invalid_bind_address() {
        let cfg = UdpRawCfg {
            bind: "invalid".to_string(),
            port: 8080,
            max_datagram_bytes: default_max_datagram_bytes(),
            timestamp_path: None,
            field_paths: vec![],
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_datagram_size_limits() {
        let cfg_small = UdpRawCfg {
            bind: "0.0.0.0".to_string(),
            port: 8080,
            max_datagram_bytes: 100, // Below minimum
            timestamp_path: None,
            field_paths: vec![],
        };
        assert!(cfg_small.validate().is_err());

        let cfg_large = UdpRawCfg {
            bind: "0.0.0.0".to_string(),
            port: 8080,
            max_datagram_bytes: 2_000_000, // Above maximum
            timestamp_path: None,
            field_paths: vec![],
        };
        assert!(cfg_large.validate().is_err());
    }

    #[test]
    fn test_json_extraction() {
        let json = serde_json::json!({
            "price": 100.5,
            "volume": 500,
            "timestamp": 1640995200000i64,
            "nested": {
                "value": 42.0
            }
        });

        assert!((json_get_f64(&json, "price").unwrap() - 100.5).abs() < 0.001);
        assert!((json_get_f64(&json, "volume").unwrap() - 500.0).abs() < 0.001);
        assert!((json_get_f64(&json, "nested.value").unwrap() - 42.0).abs() < 0.001);
        assert_eq!(json_get_f64(&json, "nonexistent"), None);
    }

    #[test]
    fn test_timestamp_parsing() {
        // Test numeric timestamp (milliseconds)
        let json_num = serde_json::json!({"ts": 1640995200000i64});
        let ts = parse_timestamp_from_value(&json_num, Some("ts"));
        assert!(ts > 0);

        // Test RFC 3339 timestamp
        let json_str = serde_json::json!({"ts": "2022-01-01T00:00:00Z"});
        let ts = parse_timestamp_from_value(&json_str, Some("ts"));
        assert!(ts > 0);

        // Test no timestamp path
        let ts = parse_timestamp_from_value(&json_num, None);
        assert!(ts > 0); // Should return current time
    }
}
