//! Unified WebSocket connector for live streaming data.
//!
//! Built-in providers: kraken, binance, binance_futures, binance_ws_api, alchemy, infura
//! Custom provider: Use provider="custom" with JSONPath field extraction
//! All output DataPoint { timestamp_ns, metrics, optional feed } for the filter pipeline.

use std::sync::Arc;
use std::sync::mpsc::SyncSender;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use tokio::time::{Duration, sleep};
use tokio::{select, time::MissedTickBehavior};
use tokio_tungstenite::{
    Connector, connect_async, connect_async_tls_with_config,
    tungstenite::Message,
    tungstenite::client::IntoClientRequest,
    tungstenite::http::{HeaderName, HeaderValue},
};

use crate::tls_verifier;
use crate::{DataPoint, FilterCfg, FilterState, now_unix_ms};

const MAX_WS_TEXT_BYTES: usize = 1_048_576;
const MAX_RECONNECT_DELAY_SECS: u64 = 60;

/// Provider-specific config. Only fields for the selected provider are used.
#[derive(Debug, Clone)]
pub struct WebSocketCfg {
    pub url: String,
    pub provider: String, // kraken | binance | binance_futures | binance_ws_api | alchemy | infura | custom
    /// Kraken: symbol e.g. "BTC/USD"
    pub symbol: Option<String>,
    /// Legacy single-subscribe hint: "newHeads" | "alchemy_pendingTransactions" | "newPendingTransactions"
    pub subscription: Option<String>,
    /// Binance: stream e.g. "btcusdt@trade"
    pub stream: Option<String>,
    /// Custom provider: JSONPath expressions for field extraction
    pub field_paths: Option<Vec<String>>,
    /// Custom provider: JSONPath for timestamp field (optional)
    pub timestamp_path: Option<String>,
    /// Delay in seconds before reconnect after disconnect (default 10)
    pub reconnect_delay_secs: u64,
    /// Bearer token populated at runtime by the
    /// `FORS33_SECRET_CONNECTOR__WEBSOCKET__TOKEN` env overlay. When present
    /// it is sent as `Authorization: Bearer <token>` on the upgrade request.
    pub token: Option<String>,
    /// API key populated by `FORS33_SECRET_CONNECTOR__WEBSOCKET__API_KEY`. When
    /// present it is sent as `X-Api-Key: <api_key>` on the upgrade request.
    pub api_key: Option<String>,
    /// Custom HTTP upgrade headers. Auth-bearing values may use
    /// `${FORS33_SECRET_HEADER_<n>}` placeholders that the bridge env overlay
    /// expands at TOML load time.
    pub headers: Vec<HeaderKv>,
    /// Built-in provider venue channels (e.g. Kraken trade/ticker/book/ohlc).
    pub channels: Option<Vec<String>>,
    /// API secret for Binance WebSocket API user-data signature subscribe.
    pub api_secret: Option<String>,
    /// Poll interval for Binance WebSocket API market RPC channels (milliseconds).
    pub rpc_poll_interval_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct HeaderKv {
    pub key: String,
    pub value: String,
}

/// Build the WebSocket upgrade `Request` with auth-bearing headers attached.
/// Auth fields are populated by the `FORS33_SECRET_CONNECTOR__WEBSOCKET__*`
/// env overlay; this helper never logs or persists secret values.
pub(crate) fn build_ws_upgrade_request(
    url: &str,
    cfg: &WebSocketCfg,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request> {
    let mut request = url.into_client_request()?;
    if let Some(t) = cfg.token.as_deref() {
        let t = t.trim();
        if !t.is_empty() {
            let v = format!("Bearer {}", t);
            if let Ok(hv) = HeaderValue::from_str(&v) {
                request
                    .headers_mut()
                    .insert(HeaderName::from_static("authorization"), hv);
            }
        }
    }
    if let Some(k) = cfg.api_key.as_deref() {
        let k = k.trim();
        if !k.is_empty() {
            if let Ok(hv) = HeaderValue::from_str(k) {
                request
                    .headers_mut()
                    .insert(HeaderName::from_static("x-api-key"), hv);
            }
        }
    }
    for h in cfg.headers.iter() {
        let key = h.key.trim();
        if key.is_empty() {
            continue;
        }
        // Fail-fast on unresolved `${FORS33_SECRET_*}` placeholders. Returning
        // the error here propagates up the WebSocket connector and out of the
        // `tokio::spawn` future, which terminates the t3thr process non-zero.
        let expanded = crate::utils::expand_fors33_secret_placeholders(h.value.trim())
            .with_context(|| format!("WebSocket header {:?} placeholder expansion failed", key))?;
        let expanded = expanded.trim();
        if expanded.is_empty() {
            continue;
        }
        let key_lc = key.to_ascii_lowercase();
        if let (Ok(name), Ok(val)) = (
            HeaderName::from_bytes(key_lc.as_bytes()),
            HeaderValue::from_str(expanded),
        ) {
            request.headers_mut().insert(name, val);
        }
    }
    Ok(request)
}

fn hex_to_u64(s: &str) -> Option<u64> {
    let s = s.trim_start_matches("0x");
    u64::from_str_radix(s, 16).ok()
}

fn hex_to_f64(s: &str) -> Option<f64> {
    hex_to_u64(s).map(|u| u as f64)
}

fn json_hex_to_u64(v: &serde_json::Value) -> Option<u64> {
    if let Some(s) = v.as_str() {
        return hex_to_u64(s);
    }
    v.as_u64()
        .or_else(|| v.as_i64().and_then(|i| u64::try_from(i).ok()))
}

fn json_hex_to_f64(v: &serde_json::Value) -> Option<f64> {
    if let Some(s) = v.as_str() {
        return hex_to_f64(s);
    }
    json_value_to_f64(v)
}

/// Kraken v2 publishes price/qty as JSON numbers; v1-style string fields still accepted.
fn json_value_to_f64(v: &serde_json::Value) -> Option<f64> {
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    if let Some(i) = v.as_i64() {
        return Some(i as f64);
    }
    if let Some(u) = v.as_u64() {
        return Some(u as f64);
    }
    v.as_str()?.parse().ok()
}

pub(crate) fn resolve_ws_channels(cfg: &WebSocketCfg) -> Vec<String> {
    if let Some(ch) = cfg.channels.as_ref() {
        let filtered: Vec<String> = ch
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !filtered.is_empty() {
            return filtered;
        }
    }
    match cfg.provider.to_lowercase().as_str() {
        "kraken" | "binance" => vec!["trade".to_string()],
        "binance_futures" => vec!["aggTrade".to_string()],
        "binance_ws_api" => vec!["trades_recent".to_string()],
        "alchemy" | "infura" => {
            let sub = cfg.subscription.as_deref().unwrap_or("newHeads");
            if sub == "alchemy_pendingTransactions" || sub == "newPendingTransactions" {
                vec!["pending".to_string()]
            } else {
                vec!["newHeads".to_string()]
            }
        }
        _ => Vec::new(),
    }
}

fn eth_subscription_name(provider: &str, channel: &str) -> &'static str {
    match (provider, channel) {
        ("alchemy", "pending") => "alchemy_pendingTransactions",
        ("infura", "pending") => "newPendingTransactions",
        (_, "pending") => "newPendingTransactions",
        (_, "newHeads") | (_, _) => "newHeads",
    }
}

// --- Kraken ---
fn kraken_subscribe_msg(channel: &str, symbol: &str) -> String {
    let mut params = serde_json::json!({
        "channel": channel,
        "symbol": [symbol],
        "snapshot": true
    });
    if channel == "book" {
        params["depth"] = serde_json::json!(10);
    } else if channel == "ohlc" {
        params["interval"] = serde_json::json!(1);
    }
    serde_json::json!({
        "method": "subscribe",
        "params": params
    })
    .to_string()
}

fn parse_kraken_trade(data: &serde_json::Value) -> Option<DataPoint> {
    let trades = data.get("data")?.as_array()?;
    if trades.is_empty() {
        return None;
    }
    let obj = trades[0].as_object()?;
    let price = json_value_to_f64(obj.get("price")?).filter(|p| *p > 0.0)?;
    let qty = obj
        .get("qty")
        .or_else(|| obj.get("quantity"))
        .and_then(json_value_to_f64)
        .filter(|q| *q > 0.0)?;
    Some(DataPoint::with_feed(
        now_unix_ms() * 1_000_000,
        vec![price, qty],
        "trade",
    ))
}

fn parse_kraken_ticker(data: &serde_json::Value) -> Option<DataPoint> {
    let obj = data.get("data")?.as_array()?.first()?.as_object()?;
    let bid = json_value_to_f64(obj.get("bid")?)?;
    let ask = json_value_to_f64(obj.get("ask")?)?;
    let last = json_value_to_f64(obj.get("last")?)?;
    let volume = json_value_to_f64(obj.get("volume")?).unwrap_or(0.0);
    Some(DataPoint::with_feed(
        now_unix_ms() * 1_000_000,
        vec![bid, ask, last, volume],
        "ticker",
    ))
}

fn parse_kraken_book(data: &serde_json::Value) -> Option<DataPoint> {
    let row = data.get("data")?.as_array()?.first()?.as_object()?;
    let bids = row.get("bids")?.as_array()?;
    let asks = row.get("asks")?.as_array()?;
    let bid = bids.first()?.as_object()?;
    let ask = asks.first()?.as_object()?;
    let bid_price = json_value_to_f64(bid.get("price")?)?;
    let bid_qty = json_value_to_f64(bid.get("qty")?)?;
    let ask_price = json_value_to_f64(ask.get("price")?)?;
    let ask_qty = json_value_to_f64(ask.get("qty")?)?;
    Some(DataPoint::with_feed(
        now_unix_ms() * 1_000_000,
        vec![bid_price, bid_qty, ask_price, ask_qty],
        "book",
    ))
}

fn parse_kraken_ohlc(data: &serde_json::Value) -> Option<DataPoint> {
    let obj = data.get("data")?.as_array()?.first()?.as_object()?;
    let open = json_value_to_f64(obj.get("open")?)?;
    let high = json_value_to_f64(obj.get("high")?)?;
    let low = json_value_to_f64(obj.get("low")?)?;
    let close = json_value_to_f64(obj.get("close")?)?;
    let volume = json_value_to_f64(obj.get("volume")?).unwrap_or(0.0);
    Some(DataPoint::with_feed(
        now_unix_ms() * 1_000_000,
        vec![open, high, low, close, volume],
        "ohlc",
    ))
}

fn parse_kraken_message(data: &serde_json::Value) -> Option<DataPoint> {
    let channel = data.get("channel").and_then(|v| v.as_str())?;
    match channel {
        "trade" => parse_kraken_trade(data),
        "ticker" => parse_kraken_ticker(data),
        "book" => parse_kraken_book(data),
        "ohlc" => parse_kraken_ohlc(data),
        _ => None,
    }
}

// --- Alchemy / Infura (JSON-RPC eth_subscribe) ---
fn eth_subscribe_msg(provider: &str, channel: &str, id: u64) -> String {
    let subscription = eth_subscription_name(provider, channel);
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
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
    let timestamp = json_hex_to_u64(result.get("timestamp")?)?;
    let gas_used = json_hex_to_f64(result.get("gasUsed")?)?;
    let price = result
        .get("baseFeePerGas")
        .and_then(json_hex_to_f64)
        .filter(|p| p.is_finite() && *p > 0.0)
        .unwrap_or(gas_used);
    Some(DataPoint::with_feed(
        timestamp * 1_000_000_000,
        vec![price, gas_used],
        "newHeads",
    ))
}

fn ethereum_pending_gas_price(result: &serde_json::Value) -> Option<f64> {
    for key in ["gasPrice", "maxFeePerGas", "maxPriorityFeePerGas"] {
        if let Some(v) = result.get(key).and_then(json_hex_to_f64) {
            if v.is_finite() && v > 0.0 {
                return Some(v);
            }
        }
    }
    None
}

// Alchemy/Infura blockchain provider - pending transactions
fn parse_ethereum_pending_tx(result: &serde_json::Value) -> Option<DataPoint> {
    let price = ethereum_pending_gas_price(result)?;
    let volume = result
        .get("gas")
        .and_then(json_hex_to_f64)
        .filter(|g| g.is_finite() && *g > 0.0)
        .unwrap_or(21_000.0);
    Some(DataPoint::with_feed(
        now_unix_ms() * 1_000_000,
        vec![price, volume],
        "pending",
    ))
}

fn parse_ethereum_subscription_message(
    data: &serde_json::Value,
    channels: &[String],
) -> Option<DataPoint> {
    if data.get("method").and_then(|v| v.as_str()) != Some("eth_subscription") {
        return None;
    }
    let result = data.get("params").and_then(|p| p.get("result"))?;
    parse_ethereum_notification(result, channels)
}

fn parse_ethereum_notification(
    result: &serde_json::Value,
    channels: &[String],
) -> Option<DataPoint> {
    if result.get("number").is_some() && channels.iter().any(|c| c == "newHeads") {
        return parse_ethereum_block_header(result);
    }
    if result.get("number").is_none()
        && channels.iter().any(|c| c == "pending")
        && (result.get("gasPrice").is_some()
            || result.get("maxFeePerGas").is_some()
            || result.get("maxPriorityFeePerGas").is_some()
            || result.get("hash").is_some())
    {
        return parse_ethereum_pending_tx(result);
    }
    None
}

// --- Binance ---
fn binance_pair_from_stream(stream: &str) -> String {
    stream
        .split('@')
        .next()
        .unwrap_or("btcusdt")
        .to_ascii_lowercase()
}

fn binance_stream_for_channel(pair: &str, channel: &str) -> String {
    match channel {
        "trade" => format!("{pair}@trade"),
        "ticker" => format!("{pair}@ticker"),
        "depth" => format!("{pair}@depth"),
        "kline" => format!("{pair}@kline_1m"),
        other => format!("{pair}@{other}"),
    }
}

const BINANCE_STREAM_HOST: &str = "wss://stream.binance.com:9443";

fn binance_subscribe_stream_names(cfg: &WebSocketCfg) -> Vec<String> {
    let stream = cfg.stream.as_deref().unwrap_or("btcusdt@trade");
    let pair = binance_pair_from_stream(stream);
    let channels = resolve_ws_channels(cfg);
    if channels.is_empty() {
        vec![stream.trim().to_ascii_lowercase()]
    } else {
        channels
            .iter()
            .map(|ch| binance_stream_for_channel(&pair, ch))
            .collect()
    }
}

fn binance_connection_base(url: &str) -> String {
    let trimmed = url.trim();
    if trimmed.is_empty() || trimmed == "env" {
        return BINANCE_STREAM_HOST.to_string();
    }
    let lower = trimmed.to_ascii_lowercase();
    let cut = lower
        .find("/ws")
        .or_else(|| lower.find("/stream"))
        .unwrap_or(lower.len());
    trimmed[..cut].trim_end_matches('/').to_string()
}

/// Normalize Binance Spot WebSocket URLs per stream.binance.com docs:
/// raw `/ws/{streamName}` (lowercase) or combined `/stream` when subscribing to multiple streams.
pub(crate) fn resolve_binance_url(cfg: &WebSocketCfg, url: &str) -> String {
    let trimmed = url.trim();
    let lower = trimmed.to_ascii_lowercase();

    if lower.contains('@') {
        if let Some(ws_idx) = lower.rfind("/ws/") {
            let base = trimmed[..ws_idx + 4].trim_end_matches('/');
            let stream_seg = trimmed[ws_idx + 4..]
                .split('?')
                .next()
                .unwrap_or("")
                .trim();
            if !stream_seg.is_empty() {
                return format!("{base}/{}", stream_seg.to_ascii_lowercase());
            }
        }
        return trimmed.to_string();
    }

    let streams = binance_subscribe_stream_names(cfg);
    let base = binance_connection_base(url);

    if streams.len() > 1 {
        return format!("{base}/stream");
    }

    let stream = streams
        .first()
        .map(|s| s.as_str())
        .unwrap_or("btcusdt@trade");
    format!("{base}/ws/{stream}")
}

fn parse_binance_trade(inner: &serde_json::Value) -> Option<DataPoint> {
    let price = json_value_to_f64(inner.get("p")?).filter(|p| *p > 0.0)?;
    let qty = json_value_to_f64(inner.get("q")?).filter(|q| *q > 0.0)?;
    let ts_ms: u64 = inner
        .get("E")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(now_unix_ms);
    Some(DataPoint::with_feed(
        ts_ms * 1_000_000,
        vec![price, qty],
        "trade",
    ))
}

fn parse_binance_ticker(inner: &serde_json::Value) -> Option<DataPoint> {
    let bid = json_value_to_f64(inner.get("b")?)?;
    let ask = json_value_to_f64(inner.get("a")?)?;
    let last = json_value_to_f64(inner.get("c")?)?;
    let volume = inner
        .get("v")
        .and_then(json_value_to_f64)
        .unwrap_or(0.0);
    let ts_ms: u64 = inner
        .get("E")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(now_unix_ms);
    Some(DataPoint::with_feed(
        ts_ms * 1_000_000,
        vec![bid, ask, last, volume],
        "ticker",
    ))
}

fn parse_binance_depth(inner: &serde_json::Value) -> Option<DataPoint> {
    let bids = inner.get("b")?.as_array()?;
    let asks = inner.get("a")?.as_array()?;
    let bid = bids.first()?.as_array()?;
    let ask = asks.first()?.as_array()?;
    let bid_price = json_value_to_f64(bid.first()?)?;
    let bid_qty = json_value_to_f64(bid.get(1)?)?;
    let ask_price = json_value_to_f64(ask.first()?)?;
    let ask_qty = json_value_to_f64(ask.get(1)?)?;
    let ts_ms: u64 = inner
        .get("E")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(now_unix_ms);
    Some(DataPoint::with_feed(
        ts_ms * 1_000_000,
        vec![bid_price, bid_qty, ask_price, ask_qty],
        "depth",
    ))
}

fn parse_binance_kline(inner: &serde_json::Value) -> Option<DataPoint> {
    let k = inner.get("k")?.as_object()?;
    let open = json_value_to_f64(k.get("o")?)?;
    let high = json_value_to_f64(k.get("h")?)?;
    let low = json_value_to_f64(k.get("l")?)?;
    let close = json_value_to_f64(k.get("c")?)?;
    let volume = k.get("v").and_then(json_value_to_f64).unwrap_or(0.0);
    let ts_ms: u64 = k
        .get("T")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(now_unix_ms);
    Some(DataPoint::with_feed(
        ts_ms * 1_000_000,
        vec![open, high, low, close, volume],
        "kline",
    ))
}

fn parse_binance_message(data: &serde_json::Value) -> Option<DataPoint> {
    let inner = if data.get("stream").is_some() {
        data.get("data")?
    } else {
        data
    };
    match inner.get("e").and_then(|v| v.as_str())? {
        "trade" => parse_binance_trade(inner),
        "aggTrade" => parse_binance_trade(inner),
        "24hrTicker" => parse_binance_ticker(inner),
        "depthUpdate" => parse_binance_depth(inner),
        "kline" => parse_binance_kline(inner),
        "markPriceUpdate" => parse_binance_mark_price(inner),
        _ => None,
    }
}

fn parse_binance_mark_price(inner: &serde_json::Value) -> Option<DataPoint> {
    let mark = json_value_to_f64(inner.get("p")?)?;
    let funding = inner
        .get("r")
        .and_then(json_value_to_f64)
        .unwrap_or(0.0);
    let ts_ms: u64 = inner
        .get("E")
        .and_then(|v| v.as_u64())
        .unwrap_or_else(now_unix_ms);
    Some(DataPoint::with_feed(
        ts_ms * 1_000_000,
        vec![mark, funding],
        "markPrice",
    ))
}

fn parse_binance_futures_message(data: &serde_json::Value) -> Option<DataPoint> {
    parse_binance_message(data)
}

const BINANCE_FUTURES_HOST: &str = "wss://fstream.binance.com";

fn binance_futures_route_for_channel(channel: &str) -> &'static str {
    match channel {
        "depth" => "public",
        _ => "market",
    }
}

fn binance_futures_stream_for_channel(pair: &str, channel: &str) -> String {
    match channel {
        "aggTrade" => format!("{pair}@aggTrade"),
        "trade" => format!("{pair}@trade"),
        "ticker" => format!("{pair}@ticker"),
        "depth" => format!("{pair}@depth"),
        "kline" => format!("{pair}@kline_1m"),
        "markPrice" => format!("{pair}@markPrice"),
        other => format!("{pair}@{other}"),
    }
}

fn binance_futures_subscribe_stream_names(cfg: &WebSocketCfg) -> Vec<String> {
    let stream = cfg.stream.as_deref().unwrap_or("btcusdt@aggTrade");
    let pair = binance_pair_from_stream(stream);
    let channels = resolve_ws_channels(cfg);
    if channels.is_empty() {
        vec![stream.trim().to_ascii_lowercase()]
    } else {
        channels
            .iter()
            .map(|ch| binance_futures_stream_for_channel(&pair, ch))
            .collect()
    }
}

fn binance_futures_connection_base(url: &str) -> String {
    let trimmed = url.trim();
    if trimmed.is_empty() || trimmed == "env" {
        return BINANCE_FUTURES_HOST.to_string();
    }
    let lower = trimmed.to_ascii_lowercase();
    for marker in ["/market/ws", "/public/ws", "/market/stream", "/public/stream", "/ws", "/stream"] {
        if let Some(idx) = lower.find(marker) {
            return trimmed[..idx].trim_end_matches('/').to_string();
        }
    }
    trimmed.trim_end_matches('/').to_string()
}

/// Binance USDⓈ-M `/market` routed streams are reliable in `stream?streams=` mode from the extension VM;
/// direct `/market/ws/<stream>` can connect without pushing frames.
fn binance_futures_market_stream_query_url(base: &str, stream: &str) -> String {
    let base = base.trim_end_matches('/');
    format!(
        "{base}/market/stream?streams={}",
        stream.trim().to_ascii_lowercase()
    )
}

/// Normalize USDⓈ-M futures WebSocket URLs with /public or /market route segments.
pub(crate) fn resolve_binance_futures_url(cfg: &WebSocketCfg, url: &str) -> String {
    let trimmed = url.trim();
    let lower = trimmed.to_ascii_lowercase();

    if lower.contains("/market/ws/") && lower.contains('@') {
        if let Some(idx) = lower.find("/market/ws/") {
            let base = trimmed[..idx].trim_end_matches('/');
            let stream_seg = trimmed[idx + "/market/ws/".len()..]
                .split('?')
                .next()
                .unwrap_or("")
                .trim();
            if !stream_seg.is_empty() {
                return binance_futures_market_stream_query_url(base, stream_seg);
            }
        }
    }

    if lower.contains('@') {
        if lower.contains("/stream?streams=") {
            return trimmed.to_string();
        }
        if let Some(ws_idx) = lower.rfind("/ws/") {
            let base = trimmed[..ws_idx + 4].trim_end_matches('/');
            let stream_seg = trimmed[ws_idx + 4..]
                .split('?')
                .next()
                .unwrap_or("")
                .trim();
            if !stream_seg.is_empty() {
                return format!("{base}/{}", stream_seg.to_ascii_lowercase());
            }
        }
        return trimmed.to_string();
    }

    let streams = binance_futures_subscribe_stream_names(cfg);
    let base = binance_futures_connection_base(url);
    let channels = resolve_ws_channels(cfg);
    let primary_channel = channels
        .first()
        .map(|s| s.as_str())
        .unwrap_or("aggTrade");
    let route = binance_futures_route_for_channel(primary_channel);

    if streams.len() > 1 {
        return format!("{base}/{route}/stream");
    }

    let stream = streams
        .first()
        .map(|s| s.as_str())
        .unwrap_or("btcusdt@aggTrade")
        .to_ascii_lowercase();
    if route == "market" {
        return binance_futures_market_stream_query_url(&base, &stream);
    }
    format!("{base}/{route}/ws/{stream}")
}

fn binance_ws_api_symbol(cfg: &WebSocketCfg) -> String {
    let stream = cfg.stream.as_deref().unwrap_or("btcusdt@trade");
    binance_pair_from_stream(stream).to_ascii_uppercase()
}

fn binance_ws_api_signature(api_key: &str, api_secret: &str, timestamp: u64) -> Option<String> {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;
    let payload = format!("apiKey={api_key}&timestamp={timestamp}");
    let mut mac = HmacSha256::new_from_slice(api_secret.as_bytes()).ok()?;
    mac.update(payload.as_bytes());
    Some(STANDARD.encode(mac.finalize().into_bytes()))
}

fn binance_ws_api_user_stream_subscribe(cfg: &WebSocketCfg) -> Option<String> {
    let api_key = cfg.api_key.as_deref()?.trim();
    let api_secret = cfg.api_secret.as_deref()?.trim();
    if api_key.is_empty() || api_secret.is_empty() {
        return None;
    }
    let timestamp = now_unix_ms();
    let signature = binance_ws_api_signature(api_key, api_secret, timestamp)?;
    Some(
        serde_json::json!({
            "id": "fors33-user-data",
            "method": "userDataStream.subscribe.signature",
            "params": {
                "apiKey": api_key,
                "timestamp": timestamp,
                "signature": signature,
            }
        })
        .to_string(),
    )
}

fn binance_ws_api_poll_request(cfg: &WebSocketCfg, channel: &str, rpc_id: u64) -> Option<String> {
    let symbol = binance_ws_api_symbol(cfg);
    let (method, params) = match channel {
        "ticker_price" => (
            "ticker.price",
            serde_json::json!({ "symbol": symbol }),
        ),
        "trades_recent" => (
            "trades.recent",
            serde_json::json!({ "symbol": symbol, "limit": 1 }),
        ),
        "depth_snapshot" => (
            "depth",
            serde_json::json!({ "symbol": symbol, "limit": 5 }),
        ),
        "user_data" => return None,
        _ => return None,
    };
    Some(
        serde_json::json!({
            "id": rpc_id,
            "method": method,
            "params": params,
        })
        .to_string(),
    )
}

fn parse_binance_ws_api_message(data: &serde_json::Value, channels: &[String]) -> Option<DataPoint> {
    if let Some(event) = data.get("event") {
        if event.get("e").and_then(|v| v.as_str()) == Some("executionReport") {
            let price = json_value_to_f64(event.get("p")?).filter(|p| *p > 0.0)?;
            let qty = json_value_to_f64(event.get("q")?).filter(|q| *q > 0.0)?;
            let ts_ms: u64 = event
                .get("E")
                .or_else(|| event.get("T"))
                .and_then(|v| v.as_u64())
                .unwrap_or_else(now_unix_ms);
            return Some(DataPoint::with_feed(
                ts_ms * 1_000_000,
                vec![price, qty],
                "user_data",
            ));
        }
        return None;
    }

    if data.get("status").and_then(|v| v.as_u64()) != Some(200) {
        return None;
    }
    let result = data.get("result")?;
    let primary = channels
        .iter()
        .find(|c| *c != "user_data")
        .map(|s| s.as_str())
        .unwrap_or("trades_recent");

    match primary {
        "ticker_price" => {
            let price = json_value_to_f64(result.get("price")?)?;
            let ts_ms = now_unix_ms();
            Some(DataPoint::with_feed(
                ts_ms * 1_000_000,
                vec![price],
                "ticker_price",
            ))
        }
        "trades_recent" => {
            let trade = result.as_array()?.first()?;
            let price = json_value_to_f64(trade.get("price")?).filter(|p| *p > 0.0)?;
            let qty = json_value_to_f64(trade.get("qty")?).filter(|q| *q > 0.0)?;
            let ts_ms: u64 = trade
                .get("time")
                .and_then(|v| v.as_u64())
                .unwrap_or_else(now_unix_ms);
            Some(DataPoint::with_feed(
                ts_ms * 1_000_000,
                vec![price, qty],
                "trades_recent",
            ))
        }
        "depth_snapshot" => {
            let bids = result.get("bids")?.as_array()?;
            let asks = result.get("asks")?.as_array()?;
            let bid = bids.first()?.as_array()?;
            let ask = asks.first()?.as_array()?;
            let bid_price = json_value_to_f64(bid.first()?)?;
            let bid_qty = json_value_to_f64(bid.get(1)?)?;
            let ask_price = json_value_to_f64(ask.first()?)?;
            let ask_qty = json_value_to_f64(ask.get(1)?)?;
            let ts_ms = now_unix_ms();
            Some(DataPoint::with_feed(
                ts_ms * 1_000_000,
                vec![bid_price, bid_qty, ask_price, ask_qty],
                "depth_snapshot",
            ))
        }
        _ => None,
    }
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

    Some(DataPoint {
        timestamp_ns,
        metrics,
        feed: None,
    })
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
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
    is_batch: bool,
) -> Result<()> {
    let base_reconnect_secs = cfg.reconnect_delay_secs.max(1);
    let mut reconnect_attempts = 0u32;

    // Dev-only test hook: force a single send immediately so we can validate
    // PID1 exit(1) behavior deterministically in Docker by dropping the receiver.
    if cfg!(feature = "dev_license_bypass")
        && std::env::var("T3THR_TEST_FORCE_SEND").ok().as_deref() == Some("1")
    {
        let dummy = DataPoint::from_legacy(now_unix_ms() * 1_000_000, 1.0, 1.0);
        if tx.send(Ok(dummy)).is_err() {
            eprintln!("[FORS33] FATAL: Writer channel closed. Stopping websocket connector.");
            std::process::exit(1);
        }
    }

    loop {
        let url = match resolve_url(cfg) {
            Ok(u) => u,
            Err(e) => {
                let reconnect_delay =
                    reconnect_delay_with_jitter(base_reconnect_secs, reconnect_attempts);
                reconnect_attempts = reconnect_attempts.saturating_add(1).min(16);
                eprintln!(
                    "[BRIDGE] WebSocket config error: {}, reconnecting in {:?}.",
                    e, reconnect_delay
                );
                sleep(reconnect_delay).await;
                continue;
            }
        };

        match connect_and_run(cfg, filter_cfg, &tx, &url, is_batch).await {
            Ok(()) => {
                if is_batch {
                    eprintln!("[FORS33] WebSocket batch listen window ended.");
                    return Ok(());
                }
                eprintln!(
                    "[BRIDGE] WebSocket stream closed; scheduling reconnect attempt {}...",
                    reconnect_attempts.saturating_add(1)
                );
            }
            Err(e) => {
                if is_batch {
                    eprintln!("[FORS33] WebSocket batch ended: {}", e);
                    return Ok(());
                }
                eprintln!(
                    "[BRIDGE] WebSocket disconnect: {}; scheduling reconnect attempt {}...",
                    e,
                    reconnect_attempts.saturating_add(1)
                );
            }
        }
        if is_batch {
            return Ok(());
        }
        let reconnect_delay = reconnect_delay_with_jitter(base_reconnect_secs, reconnect_attempts);
        reconnect_attempts = reconnect_attempts.saturating_add(1).min(16);
        eprintln!("[BRIDGE] WebSocket reconnect in {:?}.", reconnect_delay);
        sleep(reconnect_delay).await;
    }
}

fn reconnect_delay_with_jitter(base_secs: u64, attempts: u32) -> Duration {
    let exp = 1u64 << attempts.min(5);
    let capped_secs = base_secs
        .saturating_mul(exp)
        .clamp(1, MAX_RECONNECT_DELAY_SECS);
    let jitter_ms = now_unix_ms() % 1000;
    Duration::from_millis(capped_secs.saturating_mul(1000).saturating_add(jitter_ms))
}

async fn connect_and_run(
    cfg: &WebSocketCfg,
    filter_cfg: &FilterCfg,
    tx: &SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
    url: &str,
    is_batch: bool,
) -> Result<()> {
    // Build an HTTP upgrade request so we can attach Authorization/X-Api-Key
    // and any custom headers. Auth fields are populated by the
    // `FORS33_SECRET_CONNECTOR__WEBSOCKET__*` env overlay at TOML load time;
    // the connector code itself never sees plaintext tokens unless they are
    // already in memory.
    let request = build_ws_upgrade_request(url, cfg)?;

    // TLS observability: for `wss://` URLs, hand the handshake a rustls config
    // whose certificate verifier delegates to `WebPkiVerifier` and emits a
    // `[T3thr:CONNECTION_META]` line through the shared `tls_meta` module on
    // every successful handshake. For `ws://` we keep the plain path so we
    // never silently upgrade an explicit cleartext URL.
    let url_lower = url.to_ascii_lowercase();
    let (ws_stream, _) = if url_lower.starts_with("wss://") {
        let rustls_cfg = Arc::new(tls_verifier::observing_client_config());
        let connector = Connector::Rustls(rustls_cfg);
        connect_async_tls_with_config(request, None, false, Some(connector)).await?
    } else {
        connect_async(request).await?
    };
    if cfg!(feature = "dev_license_bypass")
        && std::env::var("T3THR_TEST_LOG_CONNECT").ok().as_deref() == Some("1")
    {
        eprintln!("[FORS33] TEST: WebSocket connected: {url}");
    }
    let (mut write, mut read) = ws_stream.split();

    let provider = cfg.provider.to_lowercase();
    let channels = resolve_ws_channels(cfg);
    match provider.as_str() {
        "kraken" => {
            let symbol = cfg.symbol.as_deref().unwrap_or("BTC/USD");
            for ch in channels.iter() {
                let sub = kraken_subscribe_msg(ch, symbol);
                write.send(Message::Text(sub)).await?;
            }
        }
        "alchemy" | "infura" => {
            for (idx, ch) in channels.iter().enumerate() {
                let sub = eth_subscribe_msg(&provider, ch, (idx as u64) + 1);
                write.send(Message::Text(sub)).await?;
            }
        }
        "binance" | "binance_futures" => {
            let url_lower = url.to_lowercase();
            if !url_lower.contains('@') {
                let stream = cfg.stream.as_deref().unwrap_or(if provider == "binance_futures" {
                    "btcusdt@aggTrade"
                } else {
                    "btcusdt@trade"
                });
                let pair = binance_pair_from_stream(stream);
                let stream_fn = if provider == "binance_futures" {
                    binance_futures_stream_for_channel
                } else {
                    binance_stream_for_channel
                };
                let params: Vec<String> = if channels.is_empty() {
                    vec![stream.to_string()]
                } else {
                    channels
                        .iter()
                        .map(|ch| stream_fn(&pair, ch))
                        .collect()
                };
                let sub = serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": params,
                    "id": 1
                })
                .to_string();
                write.send(Message::Text(sub)).await?;
            }
        }
        "binance_ws_api" => {
            if channels.iter().any(|c| c == "user_data") {
                if let Some(sub) = binance_ws_api_user_stream_subscribe(cfg) {
                    write.send(Message::Text(sub)).await?;
                }
            }
            let mut rpc_id: u64 = 1;
            for ch in channels.iter() {
                if ch == "user_data" {
                    continue;
                }
                if let Some(req) = binance_ws_api_poll_request(cfg, ch, rpc_id) {
                    write.send(Message::Text(req)).await?;
                    rpc_id = rpc_id.saturating_add(1);
                }
            }
        }
        "custom" => {}
        _ => {
            return Err(anyhow::anyhow!(
                "Unknown WebSocket provider: {}",
                cfg.provider
            ));
        }
    }

    let mut state = FilterState::default();
    let eth_channels = if provider == "alchemy" || provider == "infura" {
        channels.clone()
    } else {
        Vec::new()
    };

    let ws_api_market_poll = provider == "binance_ws_api"
        && channels.iter().any(|c| c.as_str() != "user_data");
    let poll_ms = cfg.rpc_poll_interval_ms.unwrap_or(1500).max(500);
    let mut poll_interval = tokio::time::interval(Duration::from_millis(poll_ms));
    poll_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    if ws_api_market_poll {
        poll_interval.tick().await;
    }
    let mut rpc_seq: u64 = channels.len() as u64 + 1;

    loop {
        let msg = if ws_api_market_poll {
            select! {
                msg = read.next() => msg,
                _ = poll_interval.tick() => {
                    if let Some(ch) = channels.iter().find(|c| c.as_str() != "user_data") {
                        if let Some(req) = binance_ws_api_poll_request(cfg, ch, rpc_seq) {
                            rpc_seq = rpc_seq.saturating_add(1);
                            let _ = write.send(Message::Text(req)).await;
                        }
                    }
                    continue;
                }
            }
        } else {
            read.next().await
        };
        let Some(msg) = msg else { break };
        let msg = msg?;
        let text = match msg {
            Message::Text(t) => {
                if t.len() > MAX_WS_TEXT_BYTES {
                    let reason = format!("WebSocket message exceeded {} bytes", MAX_WS_TEXT_BYTES);
                    if tx
                        .send(Err((
                            reason,
                            "[oversize websocket text frame]".to_string(),
                            None,
                        )))
                        .is_err()
                        && crate::batch_limits::writer_send_disconnected(is_batch, "websocket")
                    {
                        return Ok(());
                    }
                    continue;
                }
                t
            }
            Message::Binary(b) => {
                if b.len() > MAX_WS_TEXT_BYTES {
                    let reason = format!(
                        "WebSocket binary message exceeded {} bytes",
                        MAX_WS_TEXT_BYTES
                    );
                    if tx
                        .send(Err((
                            reason,
                            "[oversize websocket binary frame]".to_string(),
                            None,
                        )))
                        .is_err()
                        && crate::batch_limits::writer_send_disconnected(is_batch, "websocket")
                    {
                        return Ok(());
                    }
                }
                continue;
            }
            Message::Ping(payload) => {
                write.send(Message::Pong(payload)).await?;
                continue;
            }
            Message::Pong(_) => continue,
            _ => continue,
        };

        let data: serde_json::Value = match serde_json::from_str(&text) {
            Ok(d) => d,
            Err(e) => {
                if tx
                    .send(Err((format!("Parse Error: {}", e), text.clone(), None)))
                    .is_err()
                    && crate::batch_limits::writer_send_disconnected(is_batch, "websocket")
                {
                    return Ok(());
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
            "alchemy" | "infura" => parse_ethereum_subscription_message(&data, &eth_channels),
            "binance" => parse_binance_message(&data),
            "binance_futures" => parse_binance_futures_message(&data),
            "binance_ws_api" => parse_binance_ws_api_message(&data, &channels),
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
                eprintln!(
                    "Unknown provider: {}. Supported: kraken, alchemy, infura, binance, binance_futures, binance_ws_api, custom",
                    provider
                );
                continue;
            }
        };

        let tick = match tick {
            Some(t) => t,
            None => continue,
        };

        match state.check(&tick, filter_cfg) {
            Ok(()) => {
                if tx.send(Ok(tick)).is_err()
                    && crate::batch_limits::writer_send_disconnected(is_batch, "websocket")
                {
                    return Ok(());
                }
            }
            Err(reason) => {
                if tx
                    .send(Err((reason, text.clone(), Some(tick.timestamp_ns))))
                    .is_err()
                    && crate::batch_limits::writer_send_disconnected(is_batch, "websocket")
                {
                    return Ok(());
                }
            }
        }
    }

    Ok(())
}

fn resolve_url(cfg: &WebSocketCfg) -> Result<String> {
    let url = cfg.url.trim();
    if cfg.provider.eq_ignore_ascii_case("binance") {
        return Ok(resolve_binance_url(cfg, url));
    }
    if cfg.provider.eq_ignore_ascii_case("binance_futures") {
        return Ok(resolve_binance_futures_url(cfg, url));
    }
    if url.is_empty() || url == "env" {
        match cfg.provider.to_lowercase().as_str() {
            "alchemy" | "infura" => std::env::var("ALCHEMY_WS_URL")
                .or_else(|_| std::env::var("INFURA_WS_URL"))
                .map_err(|_| anyhow::anyhow!("ALCHEMY_WS_URL or INFURA_WS_URL not set")),
            _ => Err(anyhow::anyhow!(
                "WebSocket url required for provider {}",
                cfg.provider
            )),
        }
    } else {
        Ok(url.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_cfg(url: &str) -> WebSocketCfg {
        WebSocketCfg {
            url: url.to_string(),
            provider: "custom".to_string(),
            symbol: None,
            subscription: None,
            stream: None,
            field_paths: None,
            timestamp_path: None,
            reconnect_delay_secs: 10,
            token: None,
            api_key: None,
            api_secret: None,
            rpc_poll_interval_ms: None,
            headers: vec![],
            channels: None,
        }
    }

    #[test]
    fn build_ws_upgrade_request_attaches_authorization_header() {
        let mut cfg = empty_cfg("wss://example.com/ws");
        cfg.token = Some("abc123".to_string());
        let req = build_ws_upgrade_request(&cfg.url, &cfg).unwrap();
        assert_eq!(req.headers().get("authorization").unwrap(), "Bearer abc123");
    }

    #[test]
    fn build_ws_upgrade_request_attaches_api_key_header() {
        let mut cfg = empty_cfg("wss://example.com/ws");
        cfg.api_key = Some("k-xyz".to_string());
        let req = build_ws_upgrade_request(&cfg.url, &cfg).unwrap();
        assert_eq!(req.headers().get("x-api-key").unwrap(), "k-xyz");
    }

    #[test]
    fn build_ws_upgrade_request_merges_custom_headers() {
        let mut cfg = empty_cfg("wss://example.com/ws");
        cfg.headers = vec![HeaderKv {
            key: "X-Trace".to_string(),
            value: "1".to_string(),
        }];
        let req = build_ws_upgrade_request(&cfg.url, &cfg).unwrap();
        assert_eq!(req.headers().get("x-trace").unwrap(), "1");
    }

    #[test]
    fn build_ws_upgrade_request_skips_empty_fields() {
        let mut cfg = empty_cfg("wss://example.com/ws");
        cfg.token = Some("   ".to_string());
        cfg.api_key = Some("".to_string());
        let req = build_ws_upgrade_request(&cfg.url, &cfg).unwrap();
        assert!(req.headers().get("authorization").is_none());
        assert!(req.headers().get("x-api-key").is_none());
    }

    #[test]
    fn build_ws_upgrade_request_fails_when_placeholder_unresolved() {
        unsafe {
            std::env::remove_var("FORS33_SECRET_HEADER_WS_TEST_MISSING");
        }
        let mut cfg = empty_cfg("wss://example.com/ws");
        cfg.headers = vec![HeaderKv {
            key: "Authorization".to_string(),
            value: "Bearer ${FORS33_SECRET_HEADER_WS_TEST_MISSING}".to_string(),
        }];
        let res = build_ws_upgrade_request(&cfg.url, &cfg);
        assert!(res.is_err(), "unresolved placeholder must propagate error");
    }

    #[test]
    fn parse_kraken_v2_trade_numeric_price_qty() {
        let msg = serde_json::json!({
            "channel": "trade",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "side": "buy",
                "qty": 0.001,
                "price": 95000.5,
                "ord_type": "market",
                "trade_id": 1,
                "timestamp": "2022-12-25T09:30:59.123456Z"
            }]
        });
        let point = parse_kraken_message(&msg);
        assert!(point.is_some());
        let point = point.unwrap();
        assert_eq!(point.feed.as_deref(), Some("trade"));
        assert!(point.metrics[0] > 0.0);
        assert!(point.metrics[1] > 0.0);
    }

    #[test]
    fn kraken_subscribe_msg_includes_channel_and_symbol() {
        let sub: serde_json::Value = serde_json::from_str(&kraken_subscribe_msg("ticker", "ETH/USD"))
            .unwrap();
        assert_eq!(sub["params"]["channel"], "ticker");
        assert_eq!(sub["params"]["symbol"][0], "ETH/USD");
    }

    #[test]
    fn binance_stream_for_channel_maps_kline() {
        assert_eq!(
            binance_stream_for_channel("btcusdt", "kline"),
            "btcusdt@kline_1m"
        );
    }

    #[test]
    fn parse_kraken_trade_string_price_qty_still_works() {
        let msg = serde_json::json!({
            "channel": "trade",
            "type": "update",
            "data": [{
                "price": "42000.1",
                "qty": "0.5"
            }]
        });
        assert!(parse_kraken_message(&msg).is_some());
    }

    #[test]
    fn parse_binance_trade_direct_stream_event() {
        let msg = serde_json::json!({
            "e": "trade",
            "E": 1_700_000_000_000u64,
            "s": "BTCUSDT",
            "p": "42000.10",
            "q": "0.010"
        });
        let point = parse_binance_message(&msg).expect("direct trade event");
        assert_eq!(point.feed.as_deref(), Some("trade"));
        assert!(point.metrics[0] > 0.0);
        assert!(point.metrics[1] > 0.0);
    }

    #[test]
    fn parse_binance_trade_wrapped_multiplex_event() {
        let msg = serde_json::json!({
            "stream": "btcusdt@trade",
            "data": {
                "e": "trade",
                "E": 1_700_000_000_000u64,
                "p": "42000.10",
                "q": "0.010"
            }
        });
        assert!(parse_binance_message(&msg).is_some());
    }

    #[test]
    fn parse_binance_trade_numeric_price_qty() {
        let msg = serde_json::json!({
            "e": "trade",
            "p": 42000.10,
            "q": 0.010
        });
        assert!(parse_binance_message(&msg).is_some());
    }

    #[test]
    fn parse_binance_subscribe_ack_is_ignored() {
        let ack = serde_json::json!({"result": null, "id": 1});
        assert!(parse_binance_message(&ack).is_none());
    }

    fn binance_cfg(url: &str, stream: Option<&str>, channels: Option<Vec<String>>) -> WebSocketCfg {
        WebSocketCfg {
            url: url.to_string(),
            provider: "binance".to_string(),
            symbol: None,
            subscription: None,
            stream: stream.map(|s| s.to_string()),
            field_paths: None,
            timestamp_path: None,
            reconnect_delay_secs: 10,
            token: None,
            api_key: None,
            api_secret: None,
            rpc_poll_interval_ms: None,
            headers: vec![],
            channels,
        }
    }

    #[test]
    fn resolve_binance_url_bare_ws_appends_stream() {
        let cfg = binance_cfg(
            "wss://stream.binance.com:9443/ws",
            Some("btcusdt@trade"),
            Some(vec!["trade".to_string()]),
        );
        assert_eq!(
            resolve_binance_url(&cfg, &cfg.url),
            "wss://stream.binance.com:9443/ws/btcusdt@trade"
        );
    }

    #[test]
    fn resolve_binance_url_uppercase_stream_normalizes() {
        let cfg = binance_cfg(
            "wss://stream.binance.com:9443/ws/BTCUSDT@trade",
            None,
            None,
        );
        assert_eq!(
            resolve_binance_url(&cfg, &cfg.url),
            "wss://stream.binance.com:9443/ws/btcusdt@trade"
        );
    }

    #[test]
    fn resolve_binance_url_multi_channel_uses_stream_endpoint() {
        let cfg = binance_cfg(
            "wss://stream.binance.com:9443/ws",
            Some("btcusdt@trade"),
            Some(vec!["trade".to_string(), "ticker".to_string()]),
        );
        assert_eq!(
            resolve_binance_url(&cfg, &cfg.url),
            "wss://stream.binance.com:9443/stream"
        );
    }

    #[test]
    fn resolve_url_binance_empty_url_uses_direct_stream() {
        let cfg = binance_cfg("", Some("ethusdt@trade"), None);
        assert_eq!(
            resolve_url(&cfg).unwrap(),
            "wss://stream.binance.com:9443/ws/ethusdt@trade"
        );
    }

    #[test]
    fn eth_subscribe_msg_infura_pending_uses_new_pending_transactions() {
        let sub: serde_json::Value =
            serde_json::from_str(&eth_subscribe_msg("infura", "pending", 1)).unwrap();
        assert_eq!(sub["params"][0], "newPendingTransactions");
    }

    #[test]
    fn eth_subscribe_msg_alchemy_pending_uses_alchemy_filter() {
        let sub: serde_json::Value =
            serde_json::from_str(&eth_subscribe_msg("alchemy", "pending", 2)).unwrap();
        assert_eq!(sub["params"][0], "alchemy_pendingTransactions");
        assert!(sub["params"].as_array().unwrap().len() >= 2);
    }

    #[test]
    fn parse_ethereum_new_heads_subscription_notification() {
        let msg = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_subscription",
            "params": {
                "subscription": "0x1",
                "result": {
                    "number": "0x10",
                    "timestamp": "0x65a00000",
                    "gasUsed": "0x5208",
                    "baseFeePerGas": "0x3b9aca00"
                }
            }
        });
        let point = parse_ethereum_subscription_message(&msg, &["newHeads".to_string()])
            .expect("newHeads notification");
        assert_eq!(point.feed.as_deref(), Some("newHeads"));
        assert_eq!(point.metrics.len(), 2);
    }

    #[test]
    fn parse_ethereum_pending_subscription_notification() {
        let msg = serde_json::json!({
            "method": "eth_subscription",
            "params": {
                "result": {
                    "hash": "0xabc",
                    "gasPrice": "0x4a817c800",
                    "gas": "0x5208"
                }
            }
        });
        let point = parse_ethereum_subscription_message(&msg, &["pending".to_string()])
            .expect("pending notification");
        assert_eq!(point.feed.as_deref(), Some("pending"));
    }

    #[test]
    fn parse_ethereum_pending_eip1559_max_fee() {
        let tx = serde_json::json!({
            "hash": "0xabc",
            "maxFeePerGas": "0x4a817c800",
            "gas": "0x5208"
        });
        assert!(parse_ethereum_pending_tx(&tx).is_some());
    }

    #[test]
    fn parse_ethereum_subscribe_ack_is_ignored() {
        let ack = serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": "0x1"});
        assert!(parse_ethereum_subscription_message(&ack, &["newHeads".to_string()]).is_none());
    }

    #[test]
    fn resolve_binance_url_us_host_passthrough() {
        let cfg = binance_cfg(
            "wss://stream.binance.us:9443/ws",
            Some("btcusdt@trade"),
            Some(vec!["trade".to_string()]),
        );
        assert_eq!(
            resolve_binance_url(&cfg, &cfg.url),
            "wss://stream.binance.us:9443/ws/btcusdt@trade"
        );
    }

    #[test]
    fn resolve_binance_url_vision_host_passthrough() {
        let cfg = binance_cfg(
            "wss://data-stream.binance.vision/ws",
            Some("btcusdt@trade"),
            Some(vec!["trade".to_string()]),
        );
        assert_eq!(
            resolve_binance_url(&cfg, &cfg.url),
            "wss://data-stream.binance.vision/ws/btcusdt@trade"
        );
    }

    fn futures_cfg(url: &str, stream: Option<&str>, channels: Option<Vec<String>>) -> WebSocketCfg {
        WebSocketCfg {
            url: url.to_string(),
            provider: "binance_futures".to_string(),
            symbol: None,
            subscription: None,
            stream: stream.map(|s| s.to_string()),
            field_paths: None,
            timestamp_path: None,
            reconnect_delay_secs: 10,
            token: None,
            api_key: None,
            api_secret: None,
            rpc_poll_interval_ms: None,
            headers: vec![],
            channels,
        }
    }

    #[test]
    fn resolve_binance_futures_url_market_agg_trade() {
        let cfg = futures_cfg(
            "wss://fstream.binance.com/market/ws",
            Some("btcusdt@aggTrade"),
            Some(vec!["aggTrade".to_string()]),
        );
        assert_eq!(
            resolve_binance_futures_url(&cfg, &cfg.url),
            "wss://fstream.binance.com/market/stream?streams=btcusdt@aggtrade"
        );
    }

    #[test]
    fn resolve_binance_futures_url_rewrites_legacy_market_ws_path() {
        let cfg = futures_cfg(
            "wss://fstream.binance.com/market/ws/btcusdt@aggTrade",
            Some("btcusdt@aggTrade"),
            Some(vec!["aggTrade".to_string()]),
        );
        assert_eq!(
            resolve_binance_futures_url(&cfg, &cfg.url),
            "wss://fstream.binance.com/market/stream?streams=btcusdt@aggtrade"
        );
    }

    #[test]
    fn resolve_binance_futures_url_public_depth_keeps_ws_path() {
        let cfg = futures_cfg(
            "wss://fstream.binance.com/public/ws",
            Some("btcusdt@depth"),
            Some(vec!["depth".to_string()]),
        );
        assert_eq!(
            resolve_binance_futures_url(&cfg, &cfg.url),
            "wss://fstream.binance.com/public/ws/btcusdt@depth"
        );
    }

    #[test]
    fn parse_binance_futures_agg_trade_event() {
        let msg = serde_json::json!({
            "e": "aggTrade",
            "E": 1672515782136u64,
            "p": "0.001",
            "q": "100"
        });
        let point = parse_binance_futures_message(&msg).expect("aggTrade");
        assert_eq!(point.metrics.len(), 2);
    }

    #[test]
    fn parse_binance_ws_api_trades_recent_response() {
        let msg = serde_json::json!({
            "status": 200,
            "result": [{
                "price": "0.01361000",
                "qty": "0.01400000",
                "time": 1660009530807u64
            }]
        });
        let point = parse_binance_ws_api_message(&msg, &["trades_recent".to_string()])
            .expect("trades.recent");
        assert_eq!(point.metrics.len(), 2);
    }

    #[test]
    fn binance_ws_api_signature_deterministic() {
        let sig = binance_ws_api_signature("test-key", "test-secret", 1_700_000_000_000).unwrap();
        assert!(!sig.is_empty());
        let again = binance_ws_api_signature("test-key", "test-secret", 1_700_000_000_000).unwrap();
        assert_eq!(sig, again);
    }
}
