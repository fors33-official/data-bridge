//! One UDP datagram equals one UTF-8 JSON object; map field_paths to metrics (same JSONPath rules as message_bus).

use std::sync::mpsc::SyncSender;

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use tokio::net::UdpSocket;

use crate::{DataPoint, FilterCfg, FilterState, now_unix_ms, parse_datetime_to_ns};

#[derive(Debug, Clone)]
pub struct UdpRawCfg {
    pub bind_address: String,
    pub port: u16,
    pub max_datagram_bytes: usize,
    pub field_paths: Vec<String>,
    pub timestamp_path: Option<String>,
}

fn json_get_value<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;
    for part in parts {
        current = match current {
            Value::Object(map) => map.get(part)?,
            Value::Array(arr) => {
                let i: usize = part.parse().ok()?;
                arr.get(i)?
            }
            _ => return None,
        };
    }
    Some(current)
}

fn json_get_f64(value: &Value, path: &str) -> Option<f64> {
    let current = json_get_value(value, path)?;
    match current {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn parse_datagram_json(payload: &str, cfg: &UdpRawCfg) -> Result<DataPoint> {
    let v: Value = serde_json::from_str(payload).context("invalid JSON in UDP datagram")?;
    let mut metrics = Vec::with_capacity(cfg.field_paths.len());
    for path in &cfg.field_paths {
        let value = json_get_f64(&v, path).ok_or_else(|| anyhow!("Missing Field: {}", path))?;
        if !value.is_finite() {
            return Err(anyhow!("Non-finite value at path {}", path));
        }
        metrics.push(value);
    }
    let timestamp_ns = if let Some(ref ts_path) = cfg.timestamp_path {
        let ts_val =
            json_get_value(&v, ts_path).ok_or_else(|| anyhow!("Missing Field: {}", ts_path))?;
        match ts_val {
            Value::Number(n) => {
                let ms = n
                    .as_f64()
                    .ok_or_else(|| anyhow!("timestamp at {} must be numeric", ts_path))?;
                (ms as u64) * 1_000_000
            }
            Value::String(s) => parse_datetime_to_ns(s, "%Y-%m-%d %H:%M:%S%.f", None)?,
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

pub async fn run_udp_raw_connector(
    cfg: &UdpRawCfg,
    filter_cfg: &FilterCfg,
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
) -> Result<()> {
    let cap = cfg.max_datagram_bytes.clamp(512, 1_048_576);
    let bind = format!("{}:{}", cfg.bind_address.trim(), cfg.port);
    let sock = UdpSocket::bind(&bind)
        .await
        .with_context(|| format!("udp_raw bind {}", bind))?;
    let mut buf = vec![0u8; cap];
    let mut state = FilterState::default();
    loop {
        let (len, _src) = sock.recv_from(&mut buf).await?;
        if len == 0 {
            continue;
        }
        let payload =
            std::str::from_utf8(&buf[..len]).map_err(|e| anyhow!("datagram not UTF-8: {}", e))?;
        let tick = match parse_datagram_json(payload.trim(), cfg) {
            Ok(t) => t,
            Err(e) => {
                if tx
                    .send(Err((
                        format!("Parse Error: {}", e),
                        payload.to_string(),
                        None,
                    )))
                    .is_err()
                {
                    eprintln!("[FORS33] FATAL: Writer channel closed. Stopping udp_raw connector.");
                    std::process::exit(1);
                }
                continue;
            }
        };
        match state.check(&tick, filter_cfg) {
            Ok(()) => {
                if tx.send(Ok(tick)).is_err() {
                    eprintln!("[FORS33] FATAL: Writer channel closed. Stopping udp_raw connector.");
                    std::process::exit(1);
                }
            }
            Err(reason) => {
                if tx
                    .send(Err((reason, payload.to_string(), Some(tick.timestamp_ns))))
                    .is_err()
                {
                    eprintln!("[FORS33] FATAL: Writer channel closed. Stopping udp_raw connector.");
                    std::process::exit(1);
                }
            }
        }
    }
}
