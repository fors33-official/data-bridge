//! Unified WebSocket connector for live streaming data.
//!
//! Built-in providers: kraken, alchemy, infura, binance (legacy financial support)
//! Custom provider: Use provider="custom" with JSONPath field extraction
//! All output DataPoint { timestamp_ns, metrics: Vec<f64> } for the filter pipeline.

use std::collections::HashMap;
use std::sync::mpsc::SyncSender;

use anyhow::{anyhow, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::{now_unix_ms, DataPoint, FilterCfg, FilterState};

/// Provider-specific WebSocket endpoint constants
/// These are the standard endpoints for known providers - no auto-detection
#[allow(dead_code)] // Constants available for future use and reference
pub const KRAKEN_PUBLIC_URL: &str = "wss://ws.kraken.com/v2";
#[allow(dead_code)] // Constants available for future use and reference
pub const KRAKEN_AUTH_URL: &str = "wss://ws-auth.kraken.com/v2";
#[allow(dead_code)] // Constants available for future use and reference
pub const BINANCE_SPOT_URL: &str = "wss://stream.binance.com:9443/ws/";
#[allow(dead_code)] // Constants available for future use and reference
pub const BINANCE_FUTURES_URL: &str = "wss://fstream.binance.com/ws/";
#[allow(dead_code)] // Constants available for future use and reference
pub const ALCHEMY_MAINNET_URL: &str = "wss://eth-mainnet.g.alchemy.com/v2/";
#[allow(dead_code)] // Constants available for future use and reference
pub const INFURA_MAINNET_URL: &str = "wss://mainnet.infura.io/ws/v3/";

/// Provider-specific config. Only fields for the selected provider are used.
#[derive(Debug, Clone)]
pub struct WebSocketCfg {
    pub url: String,
    pub provider: String, // "kraken" | "alchemy" | "infura" | "binance" | "custom"
    /// Kraken: symbol e.g. "BTC/USD"
    pub symbol: Option<String>,
    /// Alchemy/Infura: "newHeads" | "alchemy_pendingTransactions"
    pub subscription: Option<String>,
    /// Binance: stream e.g. "btcusdt@trade"
    pub stream: Option<String>,
    /// Custom provider: JSONPath expressions for field extraction
    pub field_paths: Option<Vec<String>>,
    /// Custom provider: JSONPath for timestamp field (optional)
    pub timestamp_path: Option<String>,
    /// Delay in seconds before reconnect after disconnect (default 10)
    pub reconnect_delay_secs: u64,
    /// Resolved handshake headers (placeholders substituted in main).
    pub headers: HashMap<String, String>,
}

fn hex_to_u64(s: &str) -> Option<u64> {
    let s = s.trim_start_matches("0x");
    u64::from_str_radix(s, 16).ok()
}

fn hex_to_f64(s: &str) -> Option<f64> {
    hex_to_u64(s).map(|u| u as f64)
}

// --- Kraken ---
fn kraken_subscribe_msg(symbol: &str) -> String {
    serde_json::json!({
        "method": "subscribe",
        "params": {
            "channel": "trade",
            "symbol": [symbol],
            "snapshot": true
        }
    })
    .to_string()
}

// Kraken exchange provider (legacy financial data)
fn parse_kraken_message(data: &serde_json::Value) -> Option<DataPoint> {
    if data.get("channel").and_then(|v| v.as_str()) != Some("trade") {
        return None;
    }
    let trades = data.get("data")?.as_array()?;
    if trades.is_empty() {
        return None;
    }
    let obj = trades[0].as_object()?;
    let price: f64 = obj.get("price")?.as_str()?.parse().ok().filter(|p| *p > 0.0)?;
    let qty: f64 = obj.get("qty").or_else(|| obj.get("quantity"))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    if qty <= 0.0 {
        return None;
    }
    Some(DataPoint::from_legacy(now_unix_ms() * 1_000_000, price, qty))
}

// --- Alchemy / Infura (JSON-RPC eth_subscribe) ---
fn eth_subscribe_msg(subscription: &str) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": if subscription == "alchemy_pendingTransactions" {
            serde_json::json!([subscription, {}])
        } else {
            serde_json::json!([subscription])
        }
    })
    .to_string()
}

// Alchemy/Infura blockchain provider - new block headers
fn parse_ethereum_block_header(result: &serde_json::Value) -> Option<DataPoint> {
    let timestamp_hex = result.get("timestamp")?.as_str()?;
    let gas_used_hex = result.get("gasUsed")?.as_str()?;
    let timestamp = hex_to_u64(timestamp_hex)?;
    let gas_used = hex_to_f64(gas_used_hex)?;
    let price = result
        .get("baseFeePerGas")
        .and_then(|v| v.as_str())
        .and_then(hex_to_f64)
        .unwrap_or(gas_used);
    Some(DataPoint::from_legacy(timestamp * 1_000_000_000, price, gas_used))
}

// Alchemy/Infura blockchain provider - pending transactions
fn parse_ethereum_pending_tx(result: &serde_json::Value) -> Option<DataPoint> {
    let gas_price_hex = result.get("gasPrice")?.as_str()?;
    let gas_hex = result.get("gas").and_then(|v| v.as_str()).unwrap_or("0x5208");
    let price = hex_to_f64(gas_price_hex)?;
    let volume = hex_to_f64(gas_hex).unwrap_or(21000.0);
    Some(DataPoint::from_legacy(now_unix_ms() * 1_000_000, price, volume))
}

// --- Binance ---
// Binance exchange provider (legacy financial data)
fn parse_binance_message(data: &serde_json::Value) -> Option<DataPoint> {
    if data.get("e").and_then(|v| v.as_str()) != Some("trade") {
        return None;
    }
    let price: f64 = data.get("p")?.as_str()?.parse().ok().filter(|p| *p > 0.0)?;
    let qty: f64 = data.get("q")?.as_str()?.parse().ok().filter(|q| *q > 0.0)?;
    let ts_ms: u64 = data.get("E").and_then(|v| v.as_u64()).unwrap_or_else(now_unix_ms);
    Some(DataPoint::from_legacy(ts_ms * 1_000_000, price, qty))
}

/// Generic JSONPath parser for custom WebSocket providers
/// Extracts metrics using simple dot-notation paths (e.g., "data.temperature", "sensors.0.value")
fn parse_custom_message(
    data: &serde_json::Value,
    field_paths: &[String],
    timestamp_path: Option<&str>,
) -> Option<DataPoint> {
    let mut metrics = Vec::with_capacity(field_paths.len());
    
    // Extract metrics using JSONPath
    for path in field_paths {
        let value = json_path_get(data, path)?;
        metrics.push(value);
    }
    
    // Extract timestamp if provided, otherwise use current time
    let timestamp_ns = if let Some(ts_path) = timestamp_path {
        let ts_value = json_path_get(data, ts_path)?;
        (ts_value as u64) * 1_000_000 // Assume milliseconds, convert to nanoseconds
    } else {
        now_unix_ms() * 1_000_000
    };
    
    Some(DataPoint { timestamp_ns, metrics })
}

/// Simple JSONPath getter using dot notation
/// Supports: "field", "nested.field", "array.0.field"
fn json_path_get(value: &serde_json::Value, path: &str) -> Option<f64> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;
    
    for part in parts {
        // Check if this is an array index
        if let Ok(idx) = part.parse::<usize>() {
            current = current.get(idx)?;
        } else {
            current = current.get(part)?;
        }
    }
    
    // Convert to f64
    match current {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Run unified WebSocket connector; blocks until shutdown. Auto-reconnects on disconnect.
pub async fn run_websocket_connector(
    cfg: &WebSocketCfg,
    filter_cfg: &FilterCfg,
    tx: SyncSender<Result<DataPoint, (String, String)>>,
) -> Result<()> {
    let reconnect_delay = Duration::from_secs(cfg.reconnect_delay_secs.max(1));

    // Dev-only test hook: force a single send immediately so we can validate
    // PID1 exit(1) behavior deterministically in Docker by dropping the receiver.
    if cfg!(feature = "dev_license_bypass")
        && std::env::var("T3THR_TEST_FORCE_SEND").ok().as_deref() == Some("1")
    {
        let dummy = DataPoint::from_legacy(now_unix_ms() * 1_000_000, 1.0, 1.0);
        if tx.send(Ok(dummy)).is_err() {
            eprintln!("[Fors33] FATAL: Writer channel closed. Stopping websocket connector.");
            std::process::exit(1);
        }
    }

    loop {
        let url = match resolve_url(cfg) {
            Ok(u) => u,
            Err(e) => {
                eprintln!("[BRIDGE] WebSocket config error: {}, reconnecting in {:?}...", e, reconnect_delay);
                sleep(reconnect_delay).await;
                continue;
            }
        };

        match connect_and_run(cfg, filter_cfg, &tx, &url).await {
            Ok(()) => {
                eprintln!("[BRIDGE] WebSocket stream closed, reconnecting in {:?}...", reconnect_delay);
            }
            Err(e) => {
                eprintln!(
                    "[BRIDGE] WebSocket disconnect: {}, reconnecting in {:?}...",
                    e, reconnect_delay
                );
            }
        }
        sleep(reconnect_delay).await;
    }
}

async fn connect_and_run(
    cfg: &WebSocketCfg,
    filter_cfg: &FilterCfg,
    tx: &SyncSender<Result<DataPoint, (String, String)>>,
    url: &str,
) -> Result<()> {
    let (ws_stream, _) = if cfg.headers.is_empty() {
        connect_async(url).await?
    } else {
        use tokio_tungstenite::tungstenite::http::{Request, Uri};
        let uri: Uri = url
            .parse()
            .map_err(|e| anyhow!("invalid WebSocket URL: {e}"))?;
        let host = uri
            .host()
            .ok_or_else(|| anyhow!("WebSocket URL missing host"))?;
        let mut req_builder = Request::builder()
            .method("GET")
            .uri(&uri)
            .header("Host", host);
        for (k, v) in &cfg.headers {
            req_builder = req_builder.header(k, v);
        }
        let request = req_builder
            .body(())
            .map_err(|e| anyhow!("failed building WebSocket request: {e}"))?;
        connect_async(request).await?
    };
    if cfg!(feature = "dev_license_bypass")
        && std::env::var("T3THR_TEST_LOG_CONNECT").ok().as_deref() == Some("1")
    {
        eprintln!("[Fors33] TEST: WebSocket connected: {url}");
    }
    let (mut write, mut read) = ws_stream.split();

    let provider = cfg.provider.to_lowercase();
    match provider.as_str() {
        "kraken" => {
            let sub = kraken_subscribe_msg(cfg.symbol.as_deref().unwrap_or("BTC/USD"));
            write.send(Message::Text(sub)).await?;
        }
        "alchemy" | "infura" => {
            let sub = eth_subscribe_msg(cfg.subscription.as_deref().unwrap_or("newHeads"));
            write.send(Message::Text(sub)).await?;
        }
        "binance" => {
            let url_lower = url.to_lowercase();
            if url_lower.contains("/stream") && !url_lower.contains('@') {
                let stream = cfg.stream.as_deref().unwrap_or("btcusdt@trade");
                let sub = format!(r#"{{"method":"SUBSCRIBE","params":["{}"],"id":1}}"#, stream);
                write.send(Message::Text(sub)).await?;
            }
        }
        _ => return Err(anyhow::anyhow!("Unknown WebSocket provider: {}", cfg.provider)),
    }

    let mut state = FilterState::default();
    let subscription = cfg.subscription.as_deref().unwrap_or("newHeads");

    while let Some(msg) = read.next().await {
        let msg = msg?;
        let text = match msg {
            Message::Text(t) => t,
            _ => continue,
        };

        let data: serde_json::Value = match serde_json::from_str(&text) {
            Ok(d) => d,
            Err(e) => {
                if tx
                    .send(Err((format!("Parse Error: {}", e), text.clone())))
                    .is_err()
                {
                    eprintln!("[Fors33] FATAL: Writer channel closed. Stopping websocket connector.");
                    std::process::exit(1);
                }
                continue;
            }
        };

        let tick = match provider.as_str() {
            "kraken" => {
                if data.get("method").is_some() {
                    continue;
                }
                parse_kraken_message(&data)
            }
            "alchemy" | "infura" => {
                if data.get("method").and_then(|v| v.as_str()) != Some("eth_subscription") {
                    continue;
                }
                let result = match data.get("params").and_then(|p| p.get("result")) {
                    Some(r) => r,
                    None => continue,
                };
                if subscription == "alchemy_pendingTransactions" {
                    parse_ethereum_pending_tx(result)
                } else {
                    parse_ethereum_block_header(result)
                }
            }
            "binance" => parse_binance_message(&data),
            "custom" => {
                let field_paths = match &cfg.field_paths {
                    Some(paths) => paths,
                    None => {
                        eprintln!("Custom provider requires field_paths configuration");
                        continue;
                    }
                };
                parse_custom_message(&data, field_paths, cfg.timestamp_path.as_deref())
            }
            _ => {
                eprintln!("Unknown provider: {}. Supported: kraken, alchemy, infura, binance, custom", provider);
                continue;
            }
        };

        let tick = match tick {
            Some(t) => t,
            None => continue,
        };

        match state.check(&tick, filter_cfg) {
            Ok(()) => {
                if tx.send(Ok(tick)).is_err() {
                    eprintln!("[Fors33] FATAL: Writer channel closed. Stopping websocket connector.");
                    std::process::exit(1);
                }
            }
            Err(reason) => {
                if tx.send(Err((reason, text.clone()))).is_err() {
                    eprintln!("[Fors33] FATAL: Writer channel closed. Stopping websocket connector.");
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}

fn resolve_url(cfg: &WebSocketCfg) -> Result<String> {
    let url = cfg.url.trim();
    if url.is_empty() || url == "env" {
        match cfg.provider.to_lowercase().as_str() {
            "alchemy" | "infura" => std::env::var("ALCHEMY_WS_URL")
                .or_else(|_| std::env::var("INFURA_WS_URL"))
                .map_err(|_| anyhow::anyhow!("ALCHEMY_WS_URL or INFURA_WS_URL not set")),
            "binance" => {
                let stream = cfg.stream.as_deref().unwrap_or("btcusdt@trade");
                Ok(format!("wss://stream.binance.com:9443/ws/{}", stream))
            }
            _ => Err(anyhow::anyhow!("WebSocket url required for provider {}", cfg.provider)),
        }
    } else {
        Ok(url.to_string())
    }
}

