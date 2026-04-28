// Copyright (c) 2026 FORS33. All rights reserved.
// Use of this software is governed by the FORS33 End User License Agreement.
// Unauthorized reproduction, distribution, or reverse engineering is strictly prohibited.

mod connector_file;
#[cfg(feature = "full_engine")]
mod connector_grpc;
#[cfg(feature = "full_engine")]
mod connector_message_bus;
mod connector_rest;
#[cfg(feature = "full_engine")]
mod connector_websocket;
#[cfg(feature = "full_engine")]
mod connector_syslog;
#[cfg(feature = "full_engine")]
mod connector_udp_raw;
#[cfg(feature = "full_engine")]
mod connector_cdc_postgres;
#[cfg(feature = "full_engine")]
mod connector_cdc_mysql;
mod utils;

use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use chrono::{NaiveDateTime, TimeZone, Utc};
use clap::Parser;
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use serde::Deserialize;
use jsonwebtoken::{self, Algorithm, DecodingKey, Validation};

#[derive(Debug, Parser)]
#[command(name = "t3thr")]
#[command(about = "Fors33 T3thr - Config-driven time-series processor")]
struct Cli {
    /// Path to TOML config
    #[arg(long, default_value = "config/default.toml")]
    config: PathBuf,
    
    /// Explain config options and exit (prints documentation for config file)
    #[arg(long)]
    explain: bool,

    /// Parse config, resolve literal-map placeholders (deprecated) and `env_*` bindings, validate live license (if applicable), and exit without running connectors
    #[arg(long)]
    validate_only: bool,

    /// Reset state file for fresh start (batch mode only)
    #[arg(long)]
    reset_state: bool,

    /// Disable state tracking (batch mode only)
    #[arg(long)]
    no_state: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct BridgeConfig {
    pub(crate) connector: ConnectorCfg,
    pub(crate) normalizer: NormalizerCfg,
    pub(crate) filter: FilterCfg,
    pub(crate) output: OutputCfg,
    #[serde(default)]
    pub(crate) runtime: Option<RuntimeCfg>,
}

/// Runtime settings for live streaming (channel capacity, etc.).
#[derive(Debug, Deserialize)]
pub(crate) struct RuntimeCfg {
    /// Bounded channel capacity for connector→writer backpressure. Default 10_000.
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
}

fn default_channel_capacity() -> usize {
    10_000
}

impl BridgeConfig {
    /// Translate legacy config fields into N-dimensional equivalents at load time.
    /// Emits a single [Deprecation] warning when legacy keys are normalized.
    pub fn normalize_and_validate(&mut self) {
        use std::collections::HashMap;
        let mut deprecation_emitted = false;

        // Normalizer: price_field/volume_field -> field_count + field_map
        if self.normalizer.price_field.is_some() && self.normalizer.volume_field.is_some() {
            if self.normalizer.field_count.is_none() || self.normalizer.field_map.is_none() {
                let pf = self.normalizer.price_field.as_ref().unwrap().clone();
                let vf = self.normalizer.volume_field.as_ref().unwrap().clone();
                let mut map = HashMap::new();
                map.insert(pf, 0);
                map.insert(vf, 1);
                self.normalizer.field_count = Some(2);
                self.normalizer.field_map = Some(map);
                deprecation_emitted = true;
            }
        }

        // REST: price_path/volume_path -> field_paths when field_paths is absent
        // LEGACY COMPATIBILITY: This fallback maintains backward compatibility with older configs.
        // New configurations should always specify explicit field_paths for predictability.
        if let Some(ref mut rest) = self.connector.rest {
            if rest.field_paths.is_none() {
                rest.field_paths = Some(vec![rest.price_path.clone(), rest.volume_path.clone()]);
                deprecation_emitted = true;
            }
        }

        // Message bus: price_path/volume_path -> field_paths when field_paths is absent
        // LEGACY COMPATIBILITY: This fallback maintains backward compatibility with older configs.
        // New configurations should always specify explicit field_paths for predictability.
        if let Some(ref mut mb) = self.connector.message_bus {
            if mb.field_paths.is_none() {
                mb.field_paths = Some(vec![mb.price_path.clone(), mb.volume_path.clone()]);
                deprecation_emitted = true;
            }
        }

        if deprecation_emitted {
            eprintln!(
                "[Deprecation] Legacy config keys (price_field/volume_field, price_path/volume_path) were normalized. \
                 Please migrate to field_count/field_map and field_paths. See TERMINOLOGY.md."
            );
        }
    }

    /// Resolve `${T3THR_*}` placeholders in literal maps (deprecated one release), then merge `env_*` direct bindings.
    pub fn resolve_connector_env_placeholders(&mut self) -> Result<()> {
        if let Some(r) = self.connector.rest.as_mut() {
            utils::warn_deprecated_placeholders_in_literal_map(
                &r.headers,
                "connector.rest.headers",
                "`[connector.rest.env_headers]`",
            );
            utils::resolve_string_map_placeholders(&mut r.headers, "connector.rest", "headers")?;
            utils::merge_env_binding_map_into(
                &mut r.headers,
                &r.env_headers,
                "connector.rest",
                "env_headers",
            )?;
        }
        if let Some(w) = self.connector.websocket.as_mut() {
            utils::warn_deprecated_placeholders_in_literal_map(
                &w.headers,
                "connector.websocket.headers",
                "`[connector.websocket.env_headers]`",
            );
            utils::resolve_string_map_placeholders(&mut w.headers, "connector.websocket", "headers")?;
            utils::merge_env_binding_map_into(
                &mut w.headers,
                &w.env_headers,
                "connector.websocket",
                "env_headers",
            )?;
        }
        if let Some(g) = self.connector.grpc.as_mut() {
            utils::warn_deprecated_placeholders_in_literal_map(
                &g.metadata,
                "connector.grpc.metadata",
                "`[connector.grpc.env_metadata]`",
            );
            utils::resolve_string_map_placeholders(&mut g.metadata, "connector.grpc", "metadata")?;
            utils::merge_env_binding_map_into(
                &mut g.metadata,
                &g.env_metadata,
                "connector.grpc",
                "env_metadata",
            )?;
        }
        if let Some(m) = self.connector.message_bus.as_mut() {
            utils::warn_deprecated_placeholders_in_literal_map(
                &m.client_properties,
                "connector.message_bus.client_properties",
                "`[connector.message_bus.env_client_properties]`",
            );
            utils::resolve_string_map_placeholders(
                &mut m.client_properties,
                "connector.message_bus",
                "client_properties",
            )?;
            utils::merge_env_binding_map_into(
                &mut m.client_properties,
                &m.env_client_properties,
                "connector.message_bus",
                "env_client_properties",
            )?;
        }
        Ok(())
    }

    /// Channel capacity for live streaming (bounded backpressure). Default 10_000.
    pub fn channel_capacity(&self) -> usize {
        self.runtime
            .as_ref()
            .map(|r| r.channel_capacity)
            .unwrap_or_else(default_channel_capacity)
    }
}

/// Claims carried by a FORS33 license token.
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // sub/tier/exp are enforced by `jsonwebtoken`; we only branch on `allowed_connectors`.
struct LicenseClaims {
    sub: String,
    tier: String,
    exp: usize,
    #[serde(default)]
    allowed_connectors: Option<Vec<String>>,
}

// Public Ed25519 key for verifying FORS33 license tokens (JWT EdDSA).
// PEM-encoded public key compiled into the binary (not replaceable via mounts).
// Must match the Ed25519 **private** key used by the license issuer (`T3THR_EDDSA_PRIVATE_KEY` on the server).
// Note: `f33_dpk_public.pem` at repo root is RSA (DPK/L3dgr), not this key.
const FORS33_LICENSE_PUBKEY_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MCowBQYDK2VwAyEAPvZqQPBQnYm2ULY/KxuSqowk2aGJc3dqLgc6goK65E8=\n\
-----END PUBLIC KEY-----\n";

fn license_decoding_key() -> Result<DecodingKey> {
    if let Ok(pem) = std::env::var("FORS33_RUNTIME_PUBKEY_PEM") {
        let trimmed = pem.trim();
        if !trimmed.is_empty() {
            return DecodingKey::from_ed_pem(trimmed.as_bytes()).map_err(|e| {
                anyhow!("failed to construct license decoding key from FORS33_RUNTIME_PUBKEY_PEM: {e}")
            });
        }
    }
    DecodingKey::from_ed_pem(FORS33_LICENSE_PUBKEY_PEM.as_bytes())
        .map_err(|e| anyhow!("failed to construct license decoding key: {e}"))
}

fn verify_fors33_license(requested_connector: &str) -> Result<LicenseClaims> {
    let token = std::env::var("FORS33_LICENSE_KEY")
        .map_err(|_| anyhow!("FORS33_LICENSE_KEY is not set"))?;

    let mut validation = Validation::new(Algorithm::EdDSA);
    // exp is validated by default; we do not enforce aud/iss for now.
    validation.validate_exp = true;

    let decoding_key = license_decoding_key()?;

    let token_data = jsonwebtoken::decode::<LicenseClaims>(&token, &decoding_key, &validation)
        .map_err(|e| anyhow!("invalid or expired FORS33 license token: {e}"))?;
    let claims = token_data.claims;

    if let Some(ref allowed) = claims.allowed_connectors {
        if !allowed.iter().any(|c| c.eq_ignore_ascii_case(requested_connector)) {
            return Err(anyhow!(
                "license does not permit connector type: {requested_connector}"
            ));
        }
    }

    Ok(claims)
}

#[derive(Debug, Deserialize)]
struct ConnectorCfg {
    #[serde(default = "default_connector_type")]
    #[allow(dead_code)] // Deserialized for documentation/compat; mode is inferred from present sub-tables.
    r#type: String,  // "csv" | "websocket" | "rest" | "kafka" | "mqtt" | "grpc" | "syslog" | "udp_raw" | "cdc"
    #[serde(default)]
    #[allow(dead_code)] // Available for future use
    mode: Option<String>,  // "stream" | "batch" (default: "stream")
    file: Option<connector_file::FileCfg>,
    csv: Option<CsvCfg>,
    websocket: Option<WebSocketCfgUnified>,
    rest: Option<RestCfg>,
    message_bus: Option<MessageBusCfgUnified>,
    grpc: Option<GrpcCfg>,
    #[cfg(feature = "full_engine")]
    syslog: Option<connector_syslog::SyslogCfg>,
    #[cfg(feature = "full_engine")]
    udp_raw: Option<connector_udp_raw::UdpRawCfg>,
    #[cfg(feature = "full_engine")]
    cdc: Option<CdcCfg>,
}


fn default_connector_type() -> String {
    "csv".to_string()
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Many fields are only read by `full_engine` connectors; slim builds still deserialize TOML.
struct WebSocketCfgUnified {
    #[serde(default = "default_ws_provider")]
    provider: String, // "kraken" | "alchemy" | "infura" | "binance" | "custom"
    #[serde(default)]
    url: String,
    #[serde(default)]
    symbol: Option<String>,       // Kraken: e.g. "BTC/USD"
    #[serde(default)]
    subscription: Option<String>, // Alchemy/Infura: "newHeads" | "alchemy_pendingTransactions"
    #[serde(default)]
    stream: Option<String>,       // Binance: e.g. "btcusdt@trade"
    #[serde(default)]
    field_paths: Option<Vec<String>>, // Custom: JSONPath expressions for metrics
    #[serde(default)]
    timestamp_path: Option<String>, // Custom: JSONPath for timestamp
    #[serde(default = "default_reconnect_delay")]
    reconnect_delay_secs: u64,
    /// Optional HTTP-style headers for the WebSocket handshake (literals only; `${…}` deprecated—use `env_headers`).
    #[serde(default)]
    headers: HashMap<String, String>,
    /// Optional handshake headers: value is an environment variable **name** (e.g. `T3THR_WS_TOKEN`); resolved value is sent as-is (no concatenation).
    #[serde(default)]
    env_headers: HashMap<String, String>,
}

fn default_reconnect_delay() -> u64 {
    10
}

fn default_ws_provider() -> String {
    "kraken".to_string()
}

#[derive(Debug, Deserialize)]
struct CsvCfg {
    input_path: String,
    has_headers: bool,
}

#[derive(Debug, Deserialize)]
struct RestCfg {
    url: String,
    #[serde(default = "default_poll_interval")]
    poll_interval_ms: u64,
    #[serde(default = "default_price_path")]
    price_path: String,
    #[serde(default = "default_volume_path")]
    volume_path: String,
    #[serde(default)]
    response_format: String, // "json" | "csv", default json
    #[serde(default)]
    field_paths: Option<Vec<String>>,
    #[serde(default)]
    timestamp_path: Option<String>,
    /// Optional request headers (literals only; `${…}` deprecated—use `env_headers`).
    #[serde(default)]
    headers: HashMap<String, String>,
    /// Mode: "stream" (default) or "batch" for historical data extraction
    #[serde(default)]
    mode: Option<String>,
    /// Pagination cursor field for batch mode (JSONPath in response)
    #[serde(default)]
    cursor_field: Option<String>,
    /// Maximum items per page for batch mode
    #[serde(default)]
    page_limit: Option<usize>,
    /// Wire header name → `T3THR_*` environment variable name whose value is sent verbatim.
    #[serde(default)]
    env_headers: HashMap<String, String>,
}

fn default_poll_interval() -> u64 {
    1000
}

fn default_price_path() -> String {
    "price".to_string()
}

fn default_volume_path() -> String {
    "volume".to_string()
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Connector body fields used only with `full_engine`; config is always deserialized.
struct MessageBusCfgUnified {
    #[serde(default = "default_message_bus_provider")]
    provider: String, // "kafka" | "mqtt"
    // Legacy fields (deprecated, use kafka_config/mqtt_config)
    #[serde(default)]
    bootstrap_servers: String, // Kafka
    #[serde(default)]
    topic: String,
    #[serde(default = "default_message_bus_group_id")]
    group_id: String, // Kafka
    #[serde(default)]
    broker: String, // MQTT (host or host:port)
    #[serde(default = "default_price_path")]
    price_path: String,
    #[serde(default = "default_volume_path")]
    volume_path: String,
    #[serde(default)]
    field_paths: Option<Vec<String>>,
    #[serde(default)]
    timestamp_path: Option<String>,
    /// Kafka/MQTT client string literals (e.g. `security.protocol`). `${…}` here is deprecated—use `env_client_properties`.
    #[serde(default)]
    client_properties: HashMap<String, String>,
    /// Property key → `T3THR_*` env var name; resolved value replaces/sets that client property.
    #[serde(default)]
    env_client_properties: HashMap<String, String>,
    // New nested config tables (0.4.0)
    #[serde(default)]
    kafka_config: Option<KafkaCfg>,
    #[serde(default)]
    mqtt_config: Option<MqttCfg>,
}

/// Kafka-specific configuration (0.4.0)
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct KafkaCfg {
    bootstrap_servers: String,
    topic: String,
    #[serde(default = "default_message_bus_group_id")]
    group_id: String,
    #[serde(default)]
    client_properties: HashMap<String, String>,
    #[serde(default)]
    env_client_properties: HashMap<String, String>,
}

/// MQTT-specific configuration (0.4.0)
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct MqttCfg {
    broker: String, // host:port
    topic: String,
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    clean_session: bool,
    #[serde(default)]
    keep_alive_secs: u16,
    #[serde(default)]
    client_properties: HashMap<String, String>,
    #[serde(default)]
    env_client_properties: HashMap<String, String>,
}

fn default_message_bus_provider() -> String {
    "kafka".to_string()
}

fn default_message_bus_group_id() -> String {
    "aos2_bridge".to_string()
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Wire fields used only by `full_engine` gRPC connector; metadata still merged in all builds.
struct GrpcCfg {
    url: String,
    #[serde(default = "default_grpc_service")]
    service: String,
    /// Deprecated. Ignored. gRPC msg.metrics map to metric_0..metric_N via OutputCfg.headers.
    #[serde(default = "default_price_path")]
    price_path: String,
    /// Deprecated. Ignored. gRPC msg.metrics map to metric_0..metric_N via OutputCfg.headers.
    #[serde(default = "default_volume_path")]
    volume_path: String,
    /// Outbound gRPC metadata literals. `${…}` deprecated—use `env_metadata`.
    #[serde(default)]
    metadata: HashMap<String, String>,
    /// Metadata key → `T3THR_*` env var name; resolved value is sent as metadata value.
    #[serde(default)]
    env_metadata: HashMap<String, String>,
}

fn default_grpc_service() -> String {
    "market.MarketData".to_string()
}

/// CDC connector configuration with engine selector
#[derive(Debug, Deserialize, Clone)]
#[cfg(feature = "full_engine")]
#[allow(dead_code)]
struct CdcCfg {
    #[serde(default = "default_cdc_engine")]
    engine: String, // "postgres" | "mysql"
    #[cfg(feature = "full_engine")]
    postgres_config: Option<connector_cdc_postgres::CdcPostgresCfg>,
    #[cfg(feature = "full_engine")]
    mysql_config: Option<connector_cdc_mysql::CdcMysqlCfg>,
}

#[cfg(feature = "full_engine")]
fn default_cdc_engine() -> String {
    "postgres".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct NormalizerCfg {
    // Legacy 2-field support (deprecated but functional)
    pub price_field: Option<String>,
    pub volume_field: Option<String>,
    
    // N-field support
    pub field_count: Option<usize>,
    pub field_map: Option<std::collections::HashMap<String, usize>>,  // source_field -> vector index
    
    pub timestamp_field: Option<String>,
    pub timestamp_unit: Option<String>,    // s, ms, ns, tick
    pub timestamp_tick_hz: Option<f64>,    // required when unit = "tick"
    pub timestamp_format: Option<String>,  // "datetime_utc" | "datetime_utc_ms" | "time_utc"
    /// When timestamp column is time-of-day only (e.g. "00:00:00.0140000"), combine with this date (YYYY-MM-DD)
    pub timestamp_date_override: Option<String>,
}

impl NormalizerCfg {
    /// Get the expected field count (either from field_count or legacy 2-field)
    pub fn get_field_count(&self) -> usize {
        if let Some(count) = self.field_count {
            count
        } else if self.price_field.is_some() && self.volume_field.is_some() {
            2  // Legacy mode
        } else {
            0
        }
    }
    
    /// Check if using legacy 2-field mode
    pub fn is_legacy_mode(&self) -> bool {
        let is_legacy = self.price_field.is_some() && self.volume_field.is_some();
        if is_legacy {
            eprintln!("⚠️  DEPRECATION WARNING: price_field/volume_field are deprecated.");
            eprintln!("   Please migrate to N-field mode using field_count and field_map.");
            eprintln!("   See TERMINOLOGY.md for migration guide.");
        }
        is_legacy
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct FilterCfg {
    // Global settings
    reject_nan_inf: bool,
    future_tolerance_ms: u64,
    stale_tolerance_ms: u64,
    replay_mode: Option<bool>,
    drop_on_parse_error: bool,
    fail_fast: Option<bool>,  // Stop at first filter violation (default: true)
    
    // Legacy 2-field bounds (deprecated but functional)
    price_min: Option<f64>,
    price_max: Option<f64>,
    volume_min: Option<f64>,
    volume_max: Option<f64>,
    #[allow(dead_code)] // Accepted from legacy TOML; filtering uses spike_detection / N-field bounds instead.
    volume_burst_max_ratio: Option<f64>,
    burst_ema_alpha: Option<f64>,
    
    // N-field bounds (indexed by metric position)
    bounds: Option<MetricBounds>,
    spike_detection: Option<SpikeDetection>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricBounds {
    #[serde(flatten)]
    pub metrics: std::collections::HashMap<String, MetricBound>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricBound {
    pub min: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SpikeDetection {
    #[serde(flatten)]
    pub metrics: std::collections::HashMap<String, f64>,  // metric_N_max_delta -> threshold
    pub ema_alpha: Option<f64>,
}

impl FilterCfg {
    /// Get bounds for a specific metric index
    pub fn get_metric_bounds(&self, index: usize) -> (f64, f64) {
        let key = format!("metric_{}", index);
        if let Some(ref bounds) = self.bounds {
            if let Some(bound) = bounds.metrics.get(&key) {
                return (
                    bound.min.unwrap_or(f64::NEG_INFINITY),
                    bound.max.unwrap_or(f64::INFINITY),
                );
            }
        }
        // Legacy fallback for 2-field mode
        if index == 0 {
            (self.price_min.unwrap_or(f64::NEG_INFINITY), self.price_max.unwrap_or(f64::INFINITY))
        } else if index == 1 {
            (self.volume_min.unwrap_or(0.0), self.volume_max.unwrap_or(f64::INFINITY))
        } else {
            (f64::NEG_INFINITY, f64::INFINITY)
        }
    }
    
    /// Get spike threshold for a specific metric index
    pub fn get_spike_threshold(&self, index: usize) -> Option<f64> {
        let key = format!("metric_{}_max_delta", index);
        if let Some(ref spike) = self.spike_detection {
            return spike.metrics.get(&key).copied();
        }
        None
    }
    
    /// Get EMA alpha for spike detection
    pub fn get_ema_alpha(&self) -> f64 {
        if let Some(ref spike) = self.spike_detection {
            spike.ema_alpha.unwrap_or(0.05)
        } else {
            self.burst_ema_alpha.unwrap_or(0.05)
        }
    }
    
    /// Check if fail-fast is enabled (default: true)
    pub fn is_fail_fast(&self) -> bool {
        self.fail_fast.unwrap_or(true)
    }
}

fn default_output_format() -> String {
    "csv".to_string()
}

fn default_false() -> bool {
    false
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct OutputCfg {
    accepted_path: String,
    dead_letter_path: String,
    /// Output format: "csv" | "jsonl" | "parquet". Default "csv". Parquet only for batch/file modes.
    #[serde(default = "default_output_format")]
    pub(crate) format: String,
    /// When set, write accepted ticks to this pipe (live mode).
    pipe_path: Option<String>,
    /// Custom output headers (for N-field mode). If not set, uses legacy "timestamp_ns,price,volume"
    headers: Option<Vec<String>>,
    /// When true, truncate raw rejected payloads before writing to dead-letter.
    #[serde(default = "default_false")]
    truncate_raw_records: bool,
    /// Max bytes of raw payload to keep when truncation is enabled. If truncation is enabled
    /// and this is not set, the engine defaults to exactly 512 bytes.
    max_raw_record_bytes: Option<usize>,
    /// When true, write only SHA-256 hex of the raw rejected payload to dead-letter (no plaintext).
    #[serde(default = "default_false")]
    hash_raw_records: bool,
}

impl OutputCfg {
    /// Get output headers (either custom or legacy default)
    pub fn get_headers(&self, field_count: usize) -> Vec<String> {
        if let Some(ref headers) = self.headers {
            let mut r = vec!["timestamp_ns".to_string()];
            r.extend(headers.clone());
            r
        } else {
            // Legacy mode or default
            if field_count == 2 {
                vec!["timestamp_ns".to_string(), "price".to_string(), "volume".to_string()]
            } else {
                let mut r = vec!["timestamp_ns".to_string()];
                for i in 0..field_count {
                    r.push(format!("metric_{}", i));
                }
                r
            }
        }
    }

    pub(crate) fn max_raw_record_bytes_effective(&self) -> usize {
        self.max_raw_record_bytes.unwrap_or(512)
    }

    pub(crate) fn shape_deadletter_raw_record(&self, raw_record: &str) -> String {
        if self.hash_raw_records {
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(raw_record.as_bytes());
            let digest: [u8; 32] = hasher.finalize().into();
            return hex::encode(digest);
        }

        if self.truncate_raw_records {
            let max = self.max_raw_record_bytes_effective();
            let bytes = raw_record.as_bytes();
            if bytes.len() > max {
                let truncated = String::from_utf8_lossy(&bytes[..max]).to_string();
                return format!("{truncated} (truncated)");
            }
        }

        raw_record.to_string()
    }
}

/// Accepted output format for the sink.
enum AcceptedFormat {
    Csv(csv::Writer<File>),
    Jsonl(File),
    // Parquet will be added for file/batch modes in a later step.
}

/// Unified sink for accepted and dead-letter outputs.
///
/// - Ensures headers are written once for CSV.
/// - Writes dead-letter records as flat JSONL regardless of accepted format.
pub struct DataSink {
    accepted: AcceptedFormat,
    dead_letter: File,
    headers_written: bool,
    field_count: usize,
    output_cfg: OutputCfg,
}

impl DataSink {
    pub(crate) fn new(cfg: &BridgeConfig, field_count: usize) -> Result<Self> {
        let accepted_path = PathBuf::from(&cfg.output.accepted_path);
        let dead_path = PathBuf::from(&cfg.output.dead_letter_path);

        ensure_parent(&accepted_path)?;
        ensure_parent(&dead_path)?;

        let format_lower = cfg.output.format.to_lowercase();
        let accepted = if format_lower == "jsonl" {
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&accepted_path)
                .with_context(|| format!("failed opening accepted output {}", accepted_path.display()))?;
            AcceptedFormat::Jsonl(f)
        } else {
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&accepted_path)
                .with_context(|| format!("failed opening accepted output {}", accepted_path.display()))?;
            AcceptedFormat::Csv(WriterBuilder::new().from_writer(f))
        };

        let dead_file = File::create(&dead_path)
            .with_context(|| format!("failed opening dead-letter output {}", dead_path.display()))?;

        let mut sink = Self {
            accepted,
            dead_letter: dead_file,
            headers_written: false,
            field_count,
            output_cfg: cfg.output.clone(),
        };

        // If the accepted file already exists and is non-empty, validate schema.
        if accepted_path.exists() {
            let metadata = std::fs::metadata(&accepted_path)
                .with_context(|| format!("failed reading metadata for {}", accepted_path.display()))?;
            if metadata.len() > 0 {
                // Header validation only for CSV (JSONL has no header row).
                if format_lower != "jsonl" {
                    if let Some(first_line) = utils::read_first_nonempty_line(&accepted_path)? {
                        let existing: Vec<String> = first_line
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .collect();
                        let expected = cfg.output.get_headers(field_count);
                        if existing != expected {
                            return Err(anyhow!(
                                "Header mismatch in accepted file.\n  existing: {:?}\n  expected: {:?}",
                                existing,
                                expected
                            ));
                        }
                        sink.headers_written = true;
                    }
                }
            }
        }

        Ok(sink)
    }

    /// Write an accepted DataPoint.
    pub fn write_accepted(&mut self, point: &DataPoint) -> Result<()> {
        match self.accepted {
            AcceptedFormat::Csv(ref mut w) => {
                if !self.headers_written {
                    let headers = self.output_cfg.get_headers(self.field_count);
                    w.write_record(&headers)?;
                    self.headers_written = true;
                }
                let mut row = Vec::with_capacity(2 + point.metrics.len());
                row.push(point.timestamp_ns.to_string());
                for m in &point.metrics {
                    row.push(m.to_string());
                }
                w.write_record(&row)?;
                Ok(())
            }
            AcceptedFormat::Jsonl(ref mut f) => {
                let mut obj = serde_json::Map::new();
                obj.insert(
                    "timestamp_ns".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(point.timestamp_ns)),
                );
                for (idx, m) in point.metrics.iter().enumerate() {
                    let key = format!("metric_{}", idx);
                    obj.insert(
                        key,
                        serde_json::Value::Number(
                            serde_json::Number::from_f64(*m)
                                .ok_or_else(|| anyhow!("non-finite metric in accepted record"))?,
                        ),
                    );
                }
                serde_json::to_writer(&mut *f, &serde_json::Value::Object(obj))?;
                f.write_all(b"\n")?;
                Ok(())
            }
        }
    }

    /// Write a rejected record to the dead-letter JSONL file.
    pub fn write_rejected(&mut self, reason: &str, raw_record: &str) -> Result<()> {
        let now_ns = now_unix_ms() * 1_000_000;
        let raw_record = self.output_cfg.shape_deadletter_raw_record(raw_record);
        let obj = serde_json::json!({
            "timestamp_ns": now_ns,
            "reason": reason,
            "raw_record": raw_record,
        });
        serde_json::to_writer(&mut self.dead_letter, &obj)?;
        self.dead_letter.write_all(b"\n")?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DataPoint {
    pub(crate) timestamp_ns: u64,
    pub(crate) metrics: Vec<f64>,
}

impl DataPoint {
    /// Create a new DataPoint with pre-allocated capacity
    pub fn with_capacity(timestamp_ns: u64, capacity: usize) -> Self {
        Self {
            timestamp_ns,
            metrics: Vec::with_capacity(capacity),
        }
    }
    
    /// Create from legacy 2-field format (price, volume)
    pub fn from_legacy(timestamp_ns: u64, price: f64, volume: f64) -> Self {
        Self {
            timestamp_ns,
            metrics: vec![price, volume],
        }
    }
    
    /// Get metric by index (for legacy compatibility)
    pub fn get_metric(&self, index: usize) -> Option<f64> {
        self.metrics.get(index).copied()
    }
}

fn fmt_nonfinite(v: f64) -> &'static str {
    if v.is_nan() {
        "NaN"
    } else if v.is_infinite() && v > 0.0 {
        "Inf"
    } else if v.is_infinite() && v < 0.0 {
        "-Inf"
    } else {
        "?"
    }
}

#[derive(Default)]
pub struct FilterState {
    // Per-metric EMA baselines for spike detection
    metric_baselines: Vec<Option<f64>>,
}

impl FilterState {
    /// Create new FilterState with capacity for N metrics
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            metric_baselines: vec![None; capacity],
        }
    }
}

impl FilterState {
    /// Returns Err with B2B-auditable reason including numeric context.
    /// Implements fail-fast logic: stops at first violation if enabled.
    fn check(&mut self, point: &DataPoint, cfg: &FilterCfg) -> Result<(), String> {
        let fail_fast = cfg.is_fail_fast();
        
        // Check for NaN/Inf in all metrics
        if cfg.reject_nan_inf {
            for (idx, &value) in point.metrics.iter().enumerate() {
                if !value.is_finite() {
                    return Err(format!(
                        "Non-finite Value: metric[{}]={}",
                        idx,
                        fmt_nonfinite(value)
                    ));
                }
            }
        }
        
        // Check bounds for each metric (fail-fast if enabled)
        for (idx, &value) in point.metrics.iter().enumerate() {
            let (min, max) = cfg.get_metric_bounds(idx);
            if value < min {
                let err = format!(
                    "Value below minimum: metric_{}={:.2} < min={:.2}",
                    idx, value, min
                );
                if fail_fast {
                    return Err(err);
                }
            }
            if value > max {
                let err = format!(
                    "Value exceeds maximum: metric_{}={:.2} > max={:.2}",
                    idx, value, max
                );
                if fail_fast {
                    return Err(err);
                }
            }
        }
        
        // Timestamp validity checks are skipped in replay_mode.
        let replay = cfg.replay_mode.unwrap_or(false);
        if !replay {
            let now_ms = now_unix_ms();
            let ts_ms = point.timestamp_ns / 1_000_000;

            if ts_ms > now_ms.saturating_add(cfg.future_tolerance_ms) {
                let excess = ts_ms.saturating_sub(now_ms.saturating_add(cfg.future_tolerance_ms));
                return Err(format!(
                    "Timestamp too far in future: {}ms ahead of tolerance",
                    excess
                ));
            }

            if now_ms.saturating_sub(ts_ms) > cfg.stale_tolerance_ms {
                let drift_ms = now_ms.saturating_sub(ts_ms);
                return Err(format!(
                    "Timestamp too old: {}ms exceeds staleness limit of {}ms",
                    drift_ms, cfg.stale_tolerance_ms
                ));
            }
        }
        
        // Spike detection: EMA-based per-metric (skip in replay_mode)
        if !replay {
            let alpha = cfg.get_ema_alpha();
            for (idx, &value) in point.metrics.iter().enumerate() {
                if let Some(threshold) = cfg.get_spike_threshold(idx) {
                    // Ensure baseline vector has capacity
                    while self.metric_baselines.len() <= idx {
                        self.metric_baselines.push(None);
                    }
                    
                    if let Some(baseline) = self.metric_baselines[idx] {
                        if baseline > 0.0 {
                            let delta = (value - baseline).abs();
                            if delta > threshold {
                                let err = format!(
                                    "Sudden change detected: metric_{} delta={:.2} exceeds threshold {:.2} (baseline={:.2})",
                                    idx, delta, threshold, baseline
                                );
                                if fail_fast {
                                    return Err(err);
                                }
                            }
                        }
                    }
                    
                    // Update baseline (EMA)
                    self.metric_baselines[idx] = Some(match self.metric_baselines[idx] {
                        Some(prev) => (alpha * value) + ((1.0 - alpha) * prev),
                        None => value,
                    });
                }
            }
        }
        
        Ok(())
    }
}

pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn header_idx(headers: &StringRecord, field: &str) -> Result<usize> {
    headers
        .iter()
        .position(|h| h == field)
        .ok_or_else(|| anyhow!("missing field in CSV headers: {}", field))
}

pub(crate) fn parse_ts_to_ns(value: &str, unit: &str, tick_hz: Option<f64>) -> Result<u64> {
    let raw: f64 = value
        .parse()
        .with_context(|| format!("invalid timestamp value: {}", value))?;
    if !raw.is_finite() || raw < 0.0 {
        return Err(anyhow!("invalid timestamp numeric value: {}", value));
    }
    let ns = match unit {
        "s" => raw * 1_000_000_000.0,
        "ms" => raw * 1_000_000.0,
        "ns" => raw,
        "tick" => {
            let hz = tick_hz.unwrap_or(64.0);
            (raw / hz) * 1_000_000_000.0
        }
        other => return Err(anyhow!("unsupported timestamp unit: {}", other)),
    };
    Ok(ns as u64)
}

/// Parse a datetime string to nanoseconds since Unix epoch.
/// 
/// Supported preset formats:
///   "datetime_utc"    — "%Y-%m-%d %H:%M:%S" treated as UTC
///   "datetime_utc_ms" — "%Y-%m-%d %H:%M:%S%.f" with sub-second precision
///   "time_utc"        — time of day only "HH:MM:SS" or "HH:MM:SS.ffffff"; requires date_override (YYYY-MM-DD)
/// 
/// Or use any chrono format string directly, e.g.:
///   "%Y-%m-%d %H:%M:%S"
///   "%Y/%m/%d %H:%M:%S"
///   "%d-%m-%Y %H:%M:%S"
///   "%Y-%m-%dT%H:%M:%S"
///   "%Y-%m-%d %H:%M:%S%.3f"
pub fn parse_datetime_to_ns(value: &str, format: &str, date_override: Option<&str>) -> Result<u64> {
    // Handle special "time_utc" format that requires date override
    if format == "time_utc" {
        let date = date_override.ok_or_else(|| anyhow!("time_utc format requires timestamp_date_override (YYYY-MM-DD)"))?;
        let time_str = value.trim();
        // Parse HH:MM:SS or HH:MM:SS.ffffff
        let naive_time = chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f")
            .or_else(|_| chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S"))
            .with_context(|| format!("failed parsing time '{}'", time_str))?;
        let naive_date = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .with_context(|| format!("failed parsing date '{}'", date))?;
        let naive = chrono::NaiveDateTime::new(naive_date, naive_time);
        let dt = Utc.from_utc_datetime(&naive);
        let ns = dt.timestamp_nanos_opt()
            .ok_or_else(|| anyhow!("datetime out of nanosecond range: {}", value))?;
        if ns < 0 {
            return Err(anyhow!("datetime before Unix epoch: {}", value));
        }
        return Ok(ns as u64);
    }
    
    // Map preset format names to chrono format strings
    let fmt = match format {
        "datetime_utc" => "%Y-%m-%d %H:%M:%S",
        "datetime_utc_ms" => "%Y-%m-%d %H:%M:%S%.f",
        // If not a preset, assume it's a raw chrono format string
        other => other,
    };
    
    // Try to parse the datetime
    let naive = NaiveDateTime::parse_from_str(value.trim(), fmt)
        .with_context(|| format!(
            "Failed parsing datetime '{}' with format '{}'. \
            Use a chrono format string like '%Y-%m-%d %H:%M:%S' or a preset like 'datetime_utc'. \
            See https://docs.rs/chrono/latest/chrono/format/strftime/index.html for format codes.",
            value, fmt
        ))?;
    let dt = Utc.from_utc_datetime(&naive);
    let ns = dt.timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("datetime out of nanosecond range: {}", value))?;
    if ns < 0 {
        return Err(anyhow!("datetime before Unix epoch: {}", value));
    }
    Ok(ns as u64)
}

fn parse_data_point(
    record: &StringRecord,
    headers: &StringRecord,
    ncfg: &NormalizerCfg,
) -> Result<DataPoint> {
    // Parse timestamp first
    let timestamp_ns = if let Some(ts_field) = &ncfg.timestamp_field {
        let tidx = header_idx(headers, ts_field)?;
        let ts_raw = record
            .get(tidx)
            .ok_or_else(|| anyhow!("missing timestamp cell"))?;
        if let Some(fmt) = &ncfg.timestamp_format {
            parse_datetime_to_ns(ts_raw, fmt, ncfg.timestamp_date_override.as_deref())?
        } else {
            parse_ts_to_ns(
                ts_raw,
                ncfg.timestamp_unit.as_deref().unwrap_or("ms"),
                ncfg.timestamp_tick_hz,
            )?
        }
    } else {
        now_unix_ms() * 1_000_000
    };

    // N-field mode (field_count/field_map; legacy price_field/volume_field normalized at config load)
    let field_count = ncfg.get_field_count();
    if field_count == 0 {
        return Err(anyhow!("No field configuration found (need field_count or legacy price_field/volume_field)"));
    }

    let field_map = ncfg.field_map.as_ref()
        .ok_or_else(|| anyhow!("field_map required for N-field mode"))?;

    // Pre-allocate vector with exact capacity
    let mut metrics = vec![0.0; field_count];
    let mut fields_found = 0;

    // Map source fields to vector positions
    for (source_field, &index) in field_map.iter() {
        if index >= field_count {
            return Err(anyhow!("field_map index {} exceeds field_count {}", index, field_count));
        }
        
        let field_idx = header_idx(headers, source_field)?;
        let value_raw = record
            .get(field_idx)
            .ok_or_else(|| anyhow!("missing field: {}", source_field))?;
        
        let value: f64 = value_raw
            .parse()
            .with_context(|| format!("Type Error: Could not parse '{}' as f64", value_raw))?;
        
        metrics[index] = value;
        fields_found += 1;
    }

    // Strict field count validation
    if fields_found != field_count {
        return Err(anyhow!(
            "Missing Field: Expected {}, got {}",
            field_count,
            fields_found
        ));
    }

    Ok(DataPoint {
        timestamp_ns,
        metrics,
    })
}

pub(crate) fn ensure_parent(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating parent directory for {}", path.display()))?;
    }
    Ok(())
}

fn run_csv_mode(cfg: &BridgeConfig) -> Result<()> {
    let csv_cfg = cfg
        .connector
        .csv
        .as_ref()
        .ok_or_else(|| anyhow!("connector.csv required for csv mode"))?;
    let input_path = PathBuf::from(&csv_cfg.input_path);
    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    // Configure CSV reader
    // Note: flexible(true) doesn't work as expected - it still validates consistency
    // We handle field extraction at the parsing level via field_map
    let mut reader = ReaderBuilder::new()
        .has_headers(csv_cfg.has_headers)
        .trim(csv::Trim::All)
        .from_path(&input_path)
        .with_context(|| format!("failed opening input CSV {}", input_path.display()))?;

    let headers = reader
        .headers()
        .context("failed reading CSV headers")?
        .clone();

    let mut state = FilterState::with_capacity(field_count);
    let mut accepted = 0usize;
    let mut dropped = 0usize;

    // Stream processing: read-parse-filter-write-drop (constant memory)
    // Note: The csv crate can be overly strict about field counts. We catch errors
    // and route to dead-letter queue per spec: "Extra fields are simply ignored"
    let records_iter = reader.records();
    for row in records_iter {
        let record = match row {
            Ok(r) => r,
            Err(e) => {
                // CSV reader error (field count mismatch, malformed row, etc.)
                dropped += 1;
                let error_msg = format!("CSV Read Error: {}", e);
                sink.write_rejected(&error_msg, "")?;
                if cfg.filter.drop_on_parse_error {
                    continue;
                } else {
                    return Err(anyhow!("CSV reading failed: {}", e));
                }
            }
        };
        
        // Skip empty records (e.g., trailing newlines)
        if record.is_empty() || record.iter().all(|f| f.trim().is_empty()) {
            continue;
        }

        // Parse to DataPoint
        let point = match parse_data_point(&record, &headers, &cfg.normalizer) {
            Ok(p) => p,
            Err(e) => {
                dropped += 1;
                let prefix = if cfg.filter.drop_on_parse_error {
                    "Parse Error"
                } else {
                    "Parse Warning"
                };
                let reason = format!("{}: {}", prefix, e.to_string());
                let raw_record = record.iter().collect::<Vec<_>>().join("|");
                sink.write_rejected(&reason, &raw_record)?;
                if cfg.filter.drop_on_parse_error {
                    continue;
                }
                continue;
            }
        };

        // Filter check with fail-fast logic
        match state.check(&point, &cfg.filter) {
            Ok(()) => {
                accepted += 1;
                sink.write_accepted(&point)?;
            }
            Err(reason) => {
                dropped += 1;
                let raw_record = record.iter().collect::<Vec<_>>().join("|");
                sink.write_rejected(&reason, &raw_record)?;
            }
        }
    }

    println!(
        "data_bridge done | accepted={} dropped={} | accepted_path={} dead_letter_path={}",
        accepted,
        dropped,
        cfg.output.accepted_path,
        cfg.output.dead_letter_path
    );

    Ok(())
}

#[cfg(feature = "full_engine")]
fn run_websocket_mode(cfg: &BridgeConfig) -> Result<()> {
    let w = cfg
        .connector
        .websocket
        .as_ref()
        .ok_or_else(|| anyhow!("connector.websocket required for websocket mode"))?;
    let ws_cfg = connector_websocket::WebSocketCfg {
        url: w.url.clone(),
        provider: w.provider.clone(),
        symbol: w.symbol.clone(),
        subscription: w.subscription.clone(),
        stream: w.stream.clone(),
        field_paths: w.field_paths.clone(),
        timestamp_path: w.timestamp_path.clone(),
        reconnect_delay_secs: w.reconnect_delay_secs,
        headers: w.headers.clone(),
    };

    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    let capacity = cfg.channel_capacity();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String)>>(capacity);

    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();

    let rt = tokio::runtime::Runtime::new()?;
    let conn_handle = std::thread::spawn(move || {
        rt.block_on(async {
            if let Err(e) = connector_websocket::run_websocket_connector(&ws_cfg, &filter_cfg, tx).await {
                eprintln!("[BRIDGE] WebSocket connector error: {}", e);
            }
        })
    });

    // Dev-only: force writer death to validate fail-fast behavior (PID1 exit in Docker).
    if cfg!(feature = "dev_license_bypass")
        && std::env::var("T3THR_TEST_DROP_RX").ok().as_deref() == Some("1")
    {
        drop(rx);
        // Give the connector a moment to receive a message and attempt a send.
        std::thread::sleep(std::time::Duration::from_secs(15));
        let _ = conn_handle.join();
        return Ok(());
    }

    let mut pipe_writer: Option<std::fs::File> = if let Some(ref p) = pipe_path {
        match std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(p)
        {
            Ok(f) => {
                println!("[BRIDGE] Writing accepted data to pipe: {}", p);
                Some(f)
            }
            Err(e) => {
                eprintln!("[BRIDGE] Failed to open pipe {}: {}", p, e);
                None
            }
        }
    } else {
        None
    };

    let mut accepted = 0usize;
    let mut dropped = 0usize;

    for msg in rx {
        match msg {
            Ok(point) => {
                accepted += 1;
                sink.write_accepted(&point)?;
                if let Some(ref mut w) = pipe_writer {
                    // Mirror CSV line to pipe for now.
                    let mut row = vec![point.timestamp_ns.to_string()];
                    for m in &point.metrics {
                        row.push(m.to_string());
                    }
                    let line = format!("{}\n", row.join(","));
                    let _ = w.write_all(line.as_bytes());
                    let _ = w.flush();
                }
            }
            Err((reason, raw)) => {
                dropped += 1;
                sink.write_rejected(&reason, &raw)?;
            }
        }
    }

    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }

    let _ = conn_handle.join();
    println!(
        "t3thr websocket done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

fn run_rest_mode(cfg: &BridgeConfig) -> Result<()> {
    let rest_cfg = cfg
        .connector
        .rest
        .as_ref()
        .ok_or_else(|| anyhow!("connector.rest required for rest mode"))?;
    let field_paths = rest_cfg.field_paths.clone().unwrap_or_else(|| {
        vec![rest_cfg.price_path.clone(), rest_cfg.volume_path.clone()]
    });
    let rest_cfg = connector_rest::RestCfg {
        url: rest_cfg.url.clone(),
        poll_interval_ms: rest_cfg.poll_interval_ms,
        field_paths,
        timestamp_path: rest_cfg.timestamp_path.clone(),
        response_format: rest_cfg.response_format.clone(),
        headers: rest_cfg.headers.clone(),
        mode: rest_cfg.mode.clone(),
        cursor_field: rest_cfg.cursor_field.clone(),
        page_limit: rest_cfg.page_limit,
    };

    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    let (tx, rx) =
        std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String)>>(cfg.channel_capacity());
    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();
    let output_cfg = cfg.output.clone();

    let _conn_handle = std::thread::spawn(move || {
        if let Err(e) =
            connector_rest::run_rest_connector(&rest_cfg, &filter_cfg, &output_cfg, tx)
        {
            eprintln!("[BRIDGE] REST connector error: {}", e);
        }
    });

    let mut pipe_writer: Option<std::fs::File> = pipe_path.as_ref().and_then(|p| {
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(p)
            .map(|f| {
                println!("[BRIDGE] Writing accepted ticks to: {}", p);
                f
            })
            .ok()
    });

    let mut accepted = 0usize;
    let mut dropped = 0usize;

    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted += 1;
                sink.write_accepted(&tick)?;
                if let Some(ref mut w) = pipe_writer {
                    let mut row = vec![tick.timestamp_ns.to_string()];
                    for m in &tick.metrics {
                        row.push(m.to_string());
                    }
                    let line = format!("{}\n", row.join(","));
                    let _ = w.write_all(line.as_bytes());
                    let _ = w.flush();
                }
            }
            Err((reason, raw)) => {
                dropped += 1;
                sink.write_rejected(&reason, &raw)?;
            }
        }
    }

    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }
    println!(
        "t3thr rest done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

#[cfg(feature = "full_engine")]
fn run_message_bus_mode(cfg: &BridgeConfig) -> Result<()> {
    let mb = cfg
        .connector
        .message_bus
        .as_ref()
        .ok_or_else(|| anyhow!("connector.message_bus required for message_bus mode"))?;

    let mb_cfg = connector_message_bus::MessageBusCfg {
        provider: mb.provider.clone(),
        bootstrap_servers: mb.bootstrap_servers.clone(),
        topic: mb.topic.clone(),
        group_id: mb.group_id.clone(),
        broker: mb.broker.clone(),
        field_paths: mb
            .field_paths
            .clone()
            .unwrap_or_else(|| vec![mb.price_path.clone(), mb.volume_path.clone()]),
        timestamp_path: mb.timestamp_path.clone(),
        client_properties: mb.client_properties.clone(),
    };

    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    let capacity = cfg.channel_capacity();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String)>>(capacity);

    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();

    let rt = tokio::runtime::Runtime::new()?;
    let conn_handle = std::thread::spawn(move || {
        rt.block_on(async {
            if let Err(e) =
                connector_message_bus::run_message_bus_connector(&mb_cfg, &filter_cfg, tx).await
            {
                eprintln!("[BRIDGE] Message bus connector error: {}", e);
            }
        })
    });

    let mut pipe_writer: Option<std::fs::File> = pipe_path.as_ref().and_then(|p| {
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(p)
            .map(|f| {
                println!("[BRIDGE] Writing accepted ticks to pipe: {}", p);
                f
            })
            .ok()
    });

    let mut accepted = 0usize;
    let mut dropped = 0usize;

    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted += 1;
                sink.write_accepted(&tick)?;
                if let Some(ref mut w) = pipe_writer {
                    let mut row = vec![tick.timestamp_ns.to_string()];
                    for m in &tick.metrics {
                        row.push(m.to_string());
                    }
                    let line = format!("{}\n", row.join(","));
                    let _ = w.write_all(line.as_bytes());
                    let _ = w.flush();
                }
            }
            Err((reason, raw)) => {
                dropped += 1;
                sink.write_rejected(&reason, &raw)?;
            }
        }
    }

    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }

    let _ = conn_handle.join();
    println!(
        "t3thr message_bus done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

#[cfg(feature = "full_engine")]
fn run_grpc_mode(cfg: &BridgeConfig) -> Result<()> {
    let g = cfg
        .connector
        .grpc
        .as_ref()
        .ok_or_else(|| anyhow!("connector.grpc required for grpc mode"))?;

    let grpc_cfg = connector_grpc::GrpcCfg {
        url: g.url.clone(),
        service: g.service.clone(),
        price_path: g.price_path.clone(),
        volume_path: g.volume_path.clone(),
        metadata: g.metadata.clone(),
    };

    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    let capacity = cfg.channel_capacity();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String)>>(capacity);

    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();

    let rt = tokio::runtime::Runtime::new()?;
    let conn_handle = std::thread::spawn(move || {
        rt.block_on(async {
            if let Err(e) =
                connector_grpc::run_grpc_connector(&grpc_cfg, &filter_cfg, tx).await
            {
                eprintln!("[BRIDGE] gRPC connector error: {}", e);
            }
        })
    });

    let mut pipe_writer: Option<std::fs::File> = pipe_path.as_ref().and_then(|p| {
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(p)
            .map(|f| {
                println!("[BRIDGE] Writing accepted ticks to pipe: {}", p);
                f
            })
            .ok()
    });

    let mut accepted = 0usize;
    let mut dropped = 0usize;

    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted += 1;
                sink.write_accepted(&tick)?;
                if let Some(ref mut w) = pipe_writer {
                    let mut row = vec![tick.timestamp_ns.to_string()];
                    for m in &tick.metrics {
                        row.push(m.to_string());
                    }
                    let line = format!("{}\n", row.join(","));
                    let _ = w.write_all(line.as_bytes());
                    let _ = w.flush();
                }
            }
            Err((reason, raw)) => {
                dropped += 1;
                sink.write_rejected(&reason, &raw)?;
            }
        }
    }

    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }

    let _ = conn_handle.join();
    println!(
        "t3thr grpc done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

/// Fail fast when this binary was built without optional connectors (slim_engine default).
#[cfg(not(feature = "full_engine"))]
fn assert_binary_supports_config(cfg: &BridgeConfig) -> Result<()> {
    if cfg.connector.websocket.is_some()
        || cfg.connector.message_bus.is_some()
        || cfg.connector.grpc.is_some()
    {
        return Err(anyhow!(
            "This t3thr binary was built without the full_engine feature; \
             websocket, message_bus, and grpc connectors are unavailable. \
             Rebuild with: cargo build --release --features full_engine"
        ));
    }
    if let Some(ref f) = cfg.connector.file {
        if connector_file::resolve_format(f) == "parquet" {
            return Err(anyhow!(
                "Parquet file input requires t3thr built with --features full_engine"
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "full_engine")]
fn assert_binary_supports_config(_cfg: &BridgeConfig) -> Result<()> {
    Ok(())
}

fn print_config_help() {
    println!(r#"
=== T3thr Configuration Guide ===

T3thr uses TOML configuration files. Here's what each section does:

[connector.csv]
  input_path = "path/to/data.csv"    # Path to your CSV file
  has_headers = true                 # Does the first row contain column names?

[connector.rest]
  url = "https://api.example.com"    # HTTP endpoint to poll
  poll_interval_ms = 1000            # How often to poll (milliseconds)

  # Optional: literal headers only (legacy whole-value T3THR env templates in values are deprecated).
  [connector.rest.headers]
  X-Client-Id = "public-id"

  # Preferred: header name → environment variable name (value sent verbatim; e.g. put "Bearer …" in the env var).
  [connector.rest.env_headers]
  Authorization = "T3THR_REST_TOKEN"

[connector.websocket]
  url = "wss://stream.example.com"   # WebSocket URL
  provider = "custom"                # "kraken", "binance", "alchemy", or "custom"

[connector.message_bus]
  provider = "mqtt"                  # "mqtt" or "kafka"
  broker = "broker.example.com:1883"          # MQTT broker (host:port)
  topic = "data/stream"              # Topic to subscribe to

[normalizer]
  field_count = 3                    # Number of metrics per record
  timestamp_field = "timestamp"      # Column name for timestamp
  timestamp_format = "%Y-%m-%d %H:%M:%S"  # Any chrono format string
  
  [normalizer.field_map]
  "sensor_temp" = 0                  # Map CSV column to metric index
  "sensor_humidity" = 1
  "sensor_pressure" = 2

[filter]
  reject_nan_inf = true              # Reject records with NaN/Infinity
  replay_mode = false                # true = skip timestamp checks (for historical data)
  drop_on_parse_error = true         # Continue processing on errors
  fail_fast = true                   # Stop at first filter violation per record
  future_tolerance_ms = 60000        # Max milliseconds in the future allowed
  stale_tolerance_ms = 300000        # Max milliseconds old allowed
  
  [filter.bounds]
  metric_0.min = 0.0                 # Minimum value for metric 0
  metric_0.max = 100.0               # Maximum value for metric 0
  
  [filter.spike_detection]
  ema_alpha = 0.1                    # EMA smoothing factor (0.0-1.0)
  metric_0_max_delta = 50.0          # Max change from baseline for metric 0

[output]
  accepted_path = "out/accepted.csv"      # Where to write validated data
  dead_letter_path = "out/rejected.csv"   # Where to write rejected data
  format = "csv"                          # "csv" | "jsonl" (parquet only for file mode)
  headers = ["temp", "humidity", "pressure"]  # Optional: custom column names

[runtime]  # Optional: live streaming backpressure
  channel_capacity = 10000                # Bounded channel size (default 10_000)

=== Quick Start ===

1. Run the config wizard to generate a config:
   cargo run --bin config_wizard

2. Or copy an example config:
   cp config/v1_cyber.toml config/my_data.toml

3. Edit the config for your data source

4. Run the bridge:
   cargo run --release -- --config config/my_data.toml

=== Example Configs ===

- config/v1_cyber.toml         - Cybersecurity threat data
- config/v3_logistics.toml     - Supply chain metrics
- config/v2_mqtt_example.toml  - IoT sensor data via MQTT
- config/v2_rest_inventory.example.toml - Inventory REST API

For full documentation, see README.md and TERMINOLOGY.md
"#);
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    
    if cli.explain {
        print_config_help();
        return Ok(());
    }
    
    let cfg_text = fs::read_to_string(&cli.config)
        .with_context(|| format!("failed reading config {}", cli.config.display()))?;
    let mut cfg: BridgeConfig = toml::from_str(&cfg_text).context("failed parsing TOML config")?;
    cfg.normalize_and_validate();
    cfg.resolve_connector_env_placeholders()
        .context("failed resolving connector environment bindings (literal placeholders and env_* maps)")?;
    assert_binary_supports_config(&cfg)?;

    if cli.validate_only {
        let connector_count = (cfg.connector.file.is_some() as usize)
            + (cfg.connector.csv.is_some() as usize)
            + (cfg.connector.websocket.is_some() as usize)
            + (cfg.connector.rest.is_some() as usize)
            + (cfg.connector.message_bus.is_some() as usize)
            + (cfg.connector.grpc.is_some() as usize);
        if connector_count >= 2 {
            return Err(anyhow!(
                "[Fors33] CONFIG ERROR: Multiple connectors detected. Exactly one live connector block is allowed (websocket, rest, message_bus, or grpc). Found {connector_count}."
            ));
        }
        let is_live = cfg.connector.websocket.is_some()
            || cfg.connector.rest.is_some()
            || cfg.connector.message_bus.is_some()
            || cfg.connector.grpc.is_some();
        if is_live && cfg.output.format.to_lowercase() == "parquet" {
            return Err(anyhow!(
                "output.format = \"parquet\" is not supported for live connectors (websocket, rest, message_bus, grpc)."
            ));
        }
        if is_live && !cfg!(feature = "dev_license_bypass") {
            let requested_connector = if cfg.connector.websocket.is_some() {
                "websocket"
            } else if cfg.connector.rest.is_some() {
                "rest"
            } else if cfg.connector.message_bus.is_some() {
                "message_bus"
            } else if cfg.connector.grpc.is_some() {
                "grpc"
            } else {
                "unknown"
            };
            verify_fors33_license(requested_connector)
                .context("license validation failed for live connector")?;
        }
        println!("Configuration valid.");
        return Ok(());
    }

    eprintln!(
        "[Fors33] T3thr Ingestion Engine Initialized (v{}).",
        env!("CARGO_PKG_VERSION")
    );
    eprintln!("===============================================================================");
    eprintln!("LEGAL DISCLAIMER & NOTICE OF USE:");
    eprintln!("This software is provided \"AS IS\", without warranty of any kind, express or");
    eprintln!("implied, including but not limited to the warranties of merchantability,");
    eprintln!("fitness for a particular purpose and non-infringement. In no event shall");
    eprintln!("Fors33 be liable for any claim, damages or other liability, whether in an");
    eprintln!("action of contract, tort or otherwise, arising from, out of or in connection");
    eprintln!("with the software or the use or other dealings in the software.");
    eprintln!("");
    eprintln!("The operator assumes all responsibility for data retention, network stability,");
    eprintln!("and regulatory compliance. Review the full EULA at fors33.com/products/t3thr.");
    eprintln!("===============================================================================");

    let connector_count = (cfg.connector.file.is_some() as usize)
        + (cfg.connector.csv.is_some() as usize)
        + (cfg.connector.websocket.is_some() as usize)
        + (cfg.connector.rest.is_some() as usize)
        + (cfg.connector.message_bus.is_some() as usize)
        + (cfg.connector.grpc.is_some() as usize);

    if connector_count >= 2 {
        return Err(anyhow!(format!(
            "[Fors33] CONFIG ERROR: Multiple connectors detected. Exactly one live connector block is allowed (websocket, rest, message_bus, or grpc). Found {}.",
            connector_count
        )));
    }

    // 0 connectors: preserve the free-tier default by silently falling back to CSV mode.
    //
    // To avoid forcing users to write a verbose config, we default the input file to
    // `input.csv` next to the config TOML (typically `config/input.csv`).
    if connector_count == 0 {
        let default_input = cli
            .config
            .with_file_name("input.csv")
            .to_string_lossy()
            .to_string();
        cfg.connector.csv = Some(CsvCfg {
            input_path: default_input,
            has_headers: true,
        });
        return run_csv_mode(&cfg);
    }

    // Parquet is columnar and cannot be safely written row-by-row in live streaming.
    // Hard-fail when live connectors are used with output.format = "parquet".
    let is_live = cfg.connector.websocket.is_some()
        || cfg.connector.rest.is_some()
        || cfg.connector.message_bus.is_some()
        || cfg.connector.grpc.is_some();
    if is_live && cfg.output.format.to_lowercase() == "parquet" {
        return Err(anyhow!(
            "output.format = \"parquet\" is not supported for live connectors (websocket, rest, message_bus, grpc). \
             Parquet is a columnar format and cannot be safely written row-by-row. \
             Use output.format = \"csv\" or \"jsonl\" for live streaming."
        ));
    }

    // License gate: live/network connectors require a valid FORS33 license.
    if is_live {
        // Dev-only: deterministic PID1 exit validation without a real license or network activity.
        // Simulates "writer channel closed" fatal shutdown.
        if cfg!(feature = "dev_license_bypass")
            && std::env::var("T3THR_TEST_FORCE_WRITER_CLOSED").ok().as_deref() == Some("1")
        {
            eprintln!("[Fors33] FATAL: Writer channel closed. Stopping websocket connector.");
            std::process::exit(1);
        }

        let requested_connector = if cfg.connector.websocket.is_some() {
            "websocket"
        } else if cfg.connector.rest.is_some() {
            "rest"
        } else if cfg.connector.message_bus.is_some() {
            "message_bus"
        } else if cfg.connector.grpc.is_some() {
            "grpc"
        } else {
            "unknown"
        };

        if !cfg!(feature = "dev_license_bypass") {
            if let Err(err) = verify_fors33_license(requested_connector) {
            eprintln!();
            eprintln!("[Fors33] ACCESS DENIED: Live Streaming Requires Active Subscription. The requested connector is restricted.");
            eprintln!("Reason: {err}");
            eprintln!("1. Purchase access at https://fors33.com/products/t3thr.");
            eprintln!("2. After receiving your license key, run with:");
            eprintln!("   docker run -e FORS33_LICENSE_KEY=\"your_key\" -v $(pwd)/config:/app/config fors33/data-bridge ...");
            return Err(anyhow!("license validation failed for live connector"));
            }
        }
    }

    if cfg.connector.file.is_some() {
        connector_file::run_file_mode(&cfg, &cli)
    } else if cfg.connector.websocket.is_some() {
        #[cfg(feature = "full_engine")]
        {
            run_websocket_mode(&cfg)
        }
        #[cfg(not(feature = "full_engine"))]
        {
            Err(anyhow!("websocket connector requires full_engine"))
        }
    } else if cfg.connector.rest.is_some() {
        run_rest_mode(&cfg)
    } else if cfg.connector.message_bus.is_some() {
        #[cfg(feature = "full_engine")]
        {
            run_message_bus_mode(&cfg)
        }
        #[cfg(not(feature = "full_engine"))]
        {
            Err(anyhow!("message_bus connector requires full_engine"))
        }
    } else if cfg.connector.grpc.is_some() {
        #[cfg(feature = "full_engine")]
        {
            run_grpc_mode(&cfg)
        }
        #[cfg(not(feature = "full_engine"))]
        {
            Err(anyhow!("grpc connector requires full_engine"))
        }
    } else {
        #[cfg(feature = "full_engine")]
        {
            if cfg.connector.syslog.is_some() {
                run_syslog_mode(&cfg)
            } else if cfg.connector.udp_raw.is_some() {
                run_udp_raw_mode(&cfg)
            } else if cfg.connector.cdc.is_some() {
                run_cdc_mode(&cfg)
            } else {
                run_csv_mode(&cfg)
            }
        }
        #[cfg(not(feature = "full_engine"))]
        {
            run_csv_mode(&cfg)
        }
    }
}

#[cfg(feature = "full_engine")]
fn run_syslog_mode(cfg: &BridgeConfig) -> Result<()> {
    let syslog_cfg = cfg
        .connector
        .syslog
        .as_ref()
        .ok_or_else(|| anyhow!("connector.syslog required for syslog mode"))?
        .clone();

    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    let capacity = cfg.channel_capacity();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String)>>(capacity);

    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();

    let rt = tokio::runtime::Runtime::new()?;
    let _conn_handle = std::thread::spawn(move || {
        rt.block_on(async {
            if let Err(e) = connector_syslog::run_syslog_connector(&syslog_cfg, tx, &filter_cfg).await {
                eprintln!("[BRIDGE] Syslog connector error: {}", e);
            }
        })
    });

    let mut pipe_writer: Option<std::fs::File> = pipe_path.as_ref().and_then(|p| {
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(p)
            .map(|f| {
                println!("[BRIDGE] Writing accepted ticks to: {}", p);
                f
            })
            .ok()
    });

    let mut accepted = 0usize;
    let mut dropped = 0usize;

    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted += 1;
                sink.write_accepted(&tick)?;
                if let Some(ref mut w) = pipe_writer {
                    let mut row = vec![tick.timestamp_ns.to_string()];
                    for m in &tick.metrics {
                        row.push(m.to_string());
                    }
                    let line = format!("{}\n", row.join(","));
                    let _ = w.write_all(line.as_bytes());
                    let _ = w.flush();
                }
            }
            Err((reason, raw)) => {
                dropped += 1;
                sink.write_rejected(&reason, &raw)?;
            }
        }
    }

    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }
    println!(
        "t3thr syslog done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

#[cfg(feature = "full_engine")]
fn run_udp_raw_mode(cfg: &BridgeConfig) -> Result<()> {
    let udp_cfg = cfg
        .connector
        .udp_raw
        .as_ref()
        .ok_or_else(|| anyhow!("connector.udp_raw required for udp_raw mode"))?
        .clone();

    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    let capacity = cfg.channel_capacity();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String)>>(capacity);

    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();

    let rt = tokio::runtime::Runtime::new()?;
    let _conn_handle = std::thread::spawn(move || {
        rt.block_on(async {
            if let Err(e) = connector_udp_raw::run_udp_raw_connector(&udp_cfg, tx, &filter_cfg).await {
                eprintln!("[BRIDGE] UDP raw connector error: {}", e);
            }
        })
    });

    let mut pipe_writer: Option<std::fs::File> = pipe_path.as_ref().and_then(|p| {
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(p)
            .map(|f| {
                println!("[BRIDGE] Writing accepted ticks to: {}", p);
                f
            })
            .ok()
    });

    let mut accepted = 0usize;
    let mut dropped = 0usize;

    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted += 1;
                sink.write_accepted(&tick)?;
                if let Some(ref mut w) = pipe_writer {
                    let mut row = vec![tick.timestamp_ns.to_string()];
                    for m in &tick.metrics {
                        row.push(m.to_string());
                    }
                    let line = format!("{}\n", row.join(","));
                    let _ = w.write_all(line.as_bytes());
                    let _ = w.flush();
                }
            }
            Err((reason, raw)) => {
                dropped += 1;
                sink.write_rejected(&reason, &raw)?;
            }
        }
    }

    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }
    println!(
        "t3thr udp_raw done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

#[cfg(feature = "full_engine")]
fn run_cdc_mode(cfg: &BridgeConfig) -> Result<()> {
    let cdc_cfg = cfg
        .connector
        .cdc
        .as_ref()
        .ok_or_else(|| anyhow!("connector.cdc required for cdc mode"))?
        .clone();

    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    let capacity = cfg.channel_capacity();
    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String)>>(capacity);

    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();

    let rt = tokio::runtime::Runtime::new()?;
    let _conn_handle = std::thread::spawn(move || {
        rt.block_on(async {
            if let Some(ref postgres_config) = cdc_cfg.postgres_config {
                if let Err(e) = connector_cdc_postgres::run_cdc_postgres_mode(postgres_config, tx, &filter_cfg).await {
                    eprintln!("[BRIDGE] CDC Postgres connector error: {}", e);
                }
            } else if let Some(ref mysql_config) = cdc_cfg.mysql_config {
                if let Err(e) = connector_cdc_mysql::run_cdc_mysql_mode(mysql_config, tx, &filter_cfg).await {
                    eprintln!("[BRIDGE] CDC MySQL connector error: {}", e);
                }
            } else {
                eprintln!("[BRIDGE] CDC error: no postgres_config or mysql_config specified");
            }
        })
    });

    let mut pipe_writer: Option<std::fs::File> = pipe_path.as_ref().and_then(|p| {
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(p)
            .map(|f| {
                println!("[BRIDGE] Writing accepted ticks to: {}", p);
                f
            })
            .ok()
    });

    let mut accepted = 0usize;
    let mut dropped = 0usize;

    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted += 1;
                sink.write_accepted(&tick)?;
                if let Some(ref mut w) = pipe_writer {
                    let mut row = vec![tick.timestamp_ns.to_string()];
                    for m in &tick.metrics {
                        row.push(m.to_string());
                    }
                    let line = format!("{}\n", row.join(","));
                    let _ = w.write_all(line.as_bytes());
                    let _ = w.flush();
                }
            }
            Err((reason, raw)) => {
                dropped += 1;
                sink.write_rejected(&reason, &raw)?;
            }
        }
    }

    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }
    println!(
        "t3thr cdc done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}
