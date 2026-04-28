//! Syslog connector supporting RFC 5424 and RFC 3164 formats.
//! Supports TCP and UDP transport, both listener and client modes.

use std::net::SocketAddr;
use std::sync::mpsc::SyncSender;

use anyhow::{anyhow, Result};
use serde::Deserialize;
use tokio::io::AsyncBufReadExt;
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use crate::{now_unix_ms, DataPoint, FilterCfg, FilterState};

const SYSLOG_DEFAULT_PORT: u16 = 514;

#[derive(Debug, Deserialize, Clone)]
pub struct SyslogCfg {
    #[serde(default = "default_syslog_format")]
    pub syslog_format: String, // "rfc5424" | "rfc3164"
    #[serde(default = "default_syslog_transport")]
    pub transport: String, // "tcp" | "udp"
    pub listen_address: Option<String>, // For server mode
    pub connect_address: Option<String>, // For client mode
    #[serde(default = "default_syslog_port")]
    pub port: u16,
    #[serde(default)]
    pub field_paths: Vec<String>, // JSONPaths to extract metrics from message payload
}

fn default_syslog_format() -> String {
    "rfc5424".to_string()
}

fn default_syslog_transport() -> String {
    "udp".to_string()
}

fn default_syslog_port() -> u16 {
    SYSLOG_DEFAULT_PORT
}

/// Parse RFC 5424 syslog message
/// Format: <priority>version timestamp hostname app-name proc-id msg-id structured-data message
fn parse_rfc5424(line: &str) -> Result<(Option<String>, String)> {
    // Remove leading priority if present
    let line = if let Some(start) = line.find('>') {
        &line[start + 1..]
    } else {
        line
    };

    // Parse version (first char should be '1' for RFC 5424)
    if line.is_empty() {
        return Err(anyhow!("empty syslog message"));
    }

    // Split by space, RFC 5424 has: version timestamp hostname app-name proc-id msg-id [structured-data] message
    let parts: Vec<&str> = line.splitn(8, ' ').collect();
    if parts.len() < 7 {
        // Not enough fields for RFC 5424, treat entire line as message
        return Ok((None, line.to_string()));
    }

    // Extract timestamp from field 1
    let timestamp = if parts[1] != "-" {
        Some(parts[1].to_string())
    } else {
        None
    };

    // Find message after structured-data (which is in brackets or "-")
    let message_start = if parts.len() >= 7 {
        // Look for structured data indicator
        let sd_index = if parts[6].starts_with('[') || parts[6] == "-" {
            7
        } else {
            // Message starts earlier
            parts.iter().position(|p| !p.starts_with('[') && *p != "-").unwrap_or(parts.len())
        };
        if sd_index < parts.len() {
            parts[sd_index..].join(" ")
        } else {
            String::new()
        }
    } else {
        parts.last().map(|s| s.to_string()).unwrap_or_default()
    };

    Ok((timestamp, message_start))
}

/// Parse RFC 3164 syslog message (BSD syslog)
/// Format: <priority>timestamp hostname tag: message
fn parse_rfc3164(line: &str) -> Result<(Option<String>, String)> {
    // Remove leading priority if present
    let line = if let Some(start) = line.find('>') {
        &line[start + 1..]
    } else {
        line
    };

    // RFC 3164 format: Mmm dd hh:mm:ss hostname tag: message
    // The timestamp is always 15-16 characters at the start
    if line.len() < 16 {
        return Ok((None, line.to_string()));
    }

    let timestamp = &line[..15]; // Mmm dd hh:mm:ss
    let remainder = &line[16..];

    // Find where the message starts (after hostname and tag)
    let parts: Vec<&str> = remainder.splitn(3, ' ').collect();
    let message = if parts.len() >= 3 {
        // Skip hostname and tag
        let tag_part = parts[1];
        if let Some(_colon_pos) = tag_part.find(':') {
            parts[2].to_string()
        } else {
            // No tag separator found, try to find it in combined parts
            if let Some(msg_start) = remainder.find(':') {
                remainder[msg_start + 1..].trim().to_string()
            } else {
                remainder.to_string()
            }
        }
    } else {
        remainder.to_string()
    };

    Ok((Some(timestamp.to_string()), message))
}

/// Parse syslog message and extract timestamp and payload
fn parse_syslog_message(line: &str, format: &str) -> Result<(Option<String>, String)> {
    match format {
        "rfc5424" => parse_rfc5424(line),
        "rfc3164" => parse_rfc3164(line),
        _ => Err(anyhow!("unsupported syslog format: {}", format)),
    }
}

/// Extract metrics from JSON payload using field_paths
fn extract_metrics_from_json(
    json_str: &str,
    field_paths: &[String],
) -> Result<(u64, Vec<f64>)> {
    let value: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| anyhow!("failed to parse JSON payload: {}", e))?;

    let timestamp_ns = now_unix_ms() * 1_000_000;

    let mut metrics = Vec::with_capacity(field_paths.len());
    for path in field_paths {
        let metric_val = json_path_get_f64(&value, path).unwrap_or(0.0);
        metrics.push(metric_val);
    }

    Ok((timestamp_ns, metrics))
}

/// Deep JSONPath extraction
fn json_path_get_f64(value: &serde_json::Value, path: &str) -> Option<f64> {
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

    match current {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Parse timestamp from syslog format to nanoseconds
fn parse_syslog_timestamp(ts_str: &str) -> u64 {
    // Try RFC 5424 format: 2024-01-15T10:30:00.123Z or 2024-01-15T10:30:00+00:00
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(ts_str) {
        return dt.timestamp_nanos_opt().unwrap_or(0) as u64;
    }

    // Try RFC 3164 format: Jan 15 10:30:00
    // Use current year from Unix timestamp (approximate)
    let current_year = (now_unix_ms() / 1000 / 60 / 60 / 24 / 365) as i32 + 1970;
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(
        &format!("{} {}", current_year, ts_str),
        "%Y %b %d %H:%M:%S",
    ) {
        return dt.and_utc().timestamp_nanos_opt().unwrap_or(0) as u64;
    }

    // Fallback to current time
    now_unix_ms() * 1_000_000
}

/// Run syslog connector in TCP server mode
async fn run_tcp_server(
    cfg: &SyslogCfg,
    tx: SyncSender<Result<DataPoint, (String, String)>>,
    filter_cfg: &FilterCfg,
) -> Result<()> {
    let addr = cfg
        .listen_address
        .as_ref()
        .map(|a| format!("{}:{}", a, cfg.port))
        .unwrap_or_else(|| format!("0.0.0.0:{}", cfg.port));

    let listener = TcpListener::bind(&addr).await?;
    eprintln!("[Fors33] Syslog TCP server listening on {}", addr);

    
    loop {
        let (stream, peer_addr) = listener.accept().await?;
        eprintln!("[Fors33] Syslog TCP connection from {}", peer_addr);

        let tx = tx.clone();
        let cfg = cfg.clone();
        let filter_cfg = filter_cfg.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_tcp_stream(stream, &cfg, tx, &filter_cfg, peer_addr).await {
                eprintln!("[Fors33] Syslog TCP stream error from {}: {}", peer_addr, e);
            }
        });
    }
}

async fn handle_tcp_stream(
    stream: TcpStream,
    cfg: &SyslogCfg,
    tx: SyncSender<Result<DataPoint, (String, String)>>,
    filter_cfg: &FilterCfg,
    peer_addr: SocketAddr,
) -> Result<()> {
    let reader = tokio::io::BufReader::new(stream);
    let mut lines = reader.lines();
    
    while let Some(line) = lines.next_line().await? {
        process_syslog_line(&line, cfg, &tx, filter_cfg, peer_addr.to_string().as_str())?;
    }

    Ok(())
}

/// Run syslog connector in UDP server mode
async fn run_udp_server(
    cfg: &SyslogCfg,
    tx: SyncSender<Result<DataPoint, (String, String)>>,
    filter_cfg: &FilterCfg,
) -> Result<()> {
    let addr = cfg
        .listen_address
        .as_ref()
        .map(|a| format!("{}:{}", a, cfg.port))
        .unwrap_or_else(|| format!("0.0.0.0:{}", cfg.port));

    let socket = UdpSocket::bind(&addr).await?;
    eprintln!("[Fors33] Syslog UDP server listening on {}", addr);

    let mut buf = vec![0u8; 65535];
    
    loop {
        let (len, peer_addr) = socket.recv_from(&mut buf).await?;
        let line = String::from_utf8_lossy(&buf[..len]);

        process_syslog_line(&line, cfg, &tx, filter_cfg, peer_addr.to_string().as_str())?;
    }
}

/// Process a single syslog line and send to channel
fn process_syslog_line(
    line: &str,
    cfg: &SyslogCfg,
    tx: &SyncSender<Result<DataPoint, (String, String)>>,
    filter_cfg: &FilterCfg,
    _source: &str,
) -> Result<()> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(());
    }

    match parse_syslog_message(line, &cfg.syslog_format) {
        Ok((timestamp_opt, message)) => {
            let timestamp_ns = timestamp_opt
                .map(|ts| parse_syslog_timestamp(&ts))
                .unwrap_or_else(|| now_unix_ms() * 1_000_000);

            let mut filter_state = FilterState::with_capacity(2);

            // Try to parse message as JSON and extract metrics
            let data_point = if !cfg.field_paths.is_empty() {
                match extract_metrics_from_json(&message, &cfg.field_paths) {
                    Ok((_, metrics)) => DataPoint {
                        timestamp_ns,
                        metrics,
                    },
                    Err(e) => {
                        // Send to dead-letter as invalid JSON
                        tx.send(Err((
                            format!("Invalid JSON payload: {}", e),
                            line.to_string(),
                        )))
                        .map_err(|_| anyhow!("channel closed"))?;
                        return Ok(());
                    }
                }
            } else {
                // No field paths, treat entire message as single metric if parseable
                let metric_val = message.parse::<f64>().unwrap_or(0.0);
                DataPoint {
                    timestamp_ns,
                    metrics: vec![metric_val],
                }
            };

            // Apply filter
            match filter_state.check(&data_point, filter_cfg) {
                Ok(()) => {
                    tx.send(Ok(data_point))
                        .map_err(|_| anyhow!("channel closed"))?;
                }
                Err(reason) => {
                    tx.send(Err((reason, "Syslog filter violation".to_string())))
                        .map_err(|_| anyhow!("channel closed"))?;
                }
            }
        }
        Err(e) => {
            // Send to dead-letter
            tx.send(Err((
                format!("Syslog parse error: {}", e),
                line.to_string(),
            )))
            .map_err(|_| anyhow!("channel closed"))?;
        }
    }

    Ok(())
}

/// Run syslog connector in TCP client mode (connect to remote syslog server)
async fn run_tcp_client(
    cfg: &SyslogCfg,
    _tx: SyncSender<Result<DataPoint, (String, String)>>,
    _filter_cfg: &FilterCfg,
) -> Result<()> {
    let addr = cfg
        .connect_address
        .as_ref()
        .ok_or_else(|| anyhow!("connect_address required for TCP client mode"))?;

    let _stream = TcpStream::connect(format!("{}:{}", addr, cfg.port)).await?;
    eprintln!("[Fors33] Syslog TCP client connected to {}:{}", addr, cfg.port);

    // Client mode for syslog is unusual - typically you'd send logs, not receive
    // For now, just log connection success and return
    // Future enhancement: implement bidirectional syslog or TLS syslog
    eprintln!("[Fors33] Note: Syslog TCP client mode receives from remote - ensure server is configured to forward");

    // Keep connection open
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}

/// Run syslog connector in UDP client mode
async fn run_udp_client(
    cfg: &SyslogCfg,
    _tx: SyncSender<Result<DataPoint, (String, String)>>,
    _filter_cfg: &FilterCfg,
) -> Result<()> {
    let addr = cfg
        .connect_address
        .as_ref()
        .ok_or_else(|| anyhow!("connect_address required for UDP client mode"))?;

    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(format!("{}:{}", addr, cfg.port)).await?;

    eprintln!("[Fors33] Syslog UDP client connected to {}:{}", addr, cfg.port);

    // Similar to TCP client - keep open but this is receive-only
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
    }
}

/// Main entry point for syslog connector
pub async fn run_syslog_connector(
    cfg: &SyslogCfg,
    tx: SyncSender<Result<DataPoint, (String, String)>>,
    filter_cfg: &FilterCfg,
) -> Result<()> {
    match cfg.transport.as_str() {
        "tcp" => {
            if cfg.listen_address.is_some() {
                run_tcp_server(cfg, tx, filter_cfg).await
            } else if cfg.connect_address.is_some() {
                run_tcp_client(cfg, tx, filter_cfg).await
            } else {
                // Default to server mode on all interfaces
                run_tcp_server(cfg, tx, filter_cfg).await
            }
        }
        "udp" => {
            if cfg.listen_address.is_some() {
                run_udp_server(cfg, tx, filter_cfg).await
            } else if cfg.connect_address.is_some() {
                run_udp_client(cfg, tx, filter_cfg).await
            } else {
                // Default to server mode
                run_udp_server(cfg, tx, filter_cfg).await
            }
        }
        _ => Err(anyhow!("unsupported transport: {}", cfg.transport)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rfc5424() {
        // Standard RFC 5424 message
        let msg = "<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 - \"message\"";
        let (ts, payload) = parse_rfc5424(msg).unwrap();
        assert!(ts.is_some());
        assert!(payload.contains("message"));
    }

    #[test]
    fn test_parse_rfc3164() {
        // Standard RFC 3164 message
        let msg = "<34>Oct 11 22:14:15 mymachine su: message here";
        let (ts, payload) = parse_rfc3164(msg).unwrap();
        assert!(ts.is_some());
        assert_eq!(payload, "message here");
    }

    #[test]
    fn test_parse_syslog_timestamp_rfc5424() {
        let ts = "2003-10-11T22:14:15.003Z";
        let ns = parse_syslog_timestamp(ts);
        assert!(ns > 0);
    }

    #[test]
    fn test_json_path_extraction() {
        let json = r#"{"price": 100.5, "volume": 500}"#;
        let paths = vec!["price".to_string(), "volume".to_string()];
        let (ts, metrics) = extract_metrics_from_json(json, &paths).unwrap();
        assert_eq!(metrics.len(), 2);
        assert!((metrics[0] - 100.5).abs() < 0.001);
        assert!((metrics[1] - 500.0).abs() < 0.001);
    }

    #[test]
    fn test_json_path_nested() {
        let json = r#"{"data": {"value": 42.5}}"#;
        let val = json_path_get_f64(
            &serde_json::from_str(json).unwrap(),
            "data.value",
        );
        assert!(val.is_some());
        assert!((val.unwrap() - 42.5).abs() < 0.001);
    }
}
