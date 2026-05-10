// Copyright (c) 2026 FORS33. All rights reserved.
// Use of this software is governed by the FORS33 End User License Agreement.
// Unauthorized reproduction, distribution, or reverse engineering is strictly prohibited.

mod commands;
#[cfg(feature = "full_engine")]
mod connector_cdc_mysql;
#[cfg(feature = "full_engine")]
mod connector_cdc_postgres;
mod connector_file;
#[cfg(feature = "full_engine")]
mod connector_grpc;
#[cfg(feature = "full_engine")]
mod connector_message_bus;
mod connector_rest;
#[cfg(feature = "full_engine")]
mod connector_syslog;
#[cfg(feature = "full_engine")]
mod connector_udp_raw;
#[cfg(feature = "full_engine")]
mod connector_websocket;
mod tls_meta;
mod tls_verifier;
mod utils;

use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use chrono::{NaiveDateTime, TimeZone, Utc};
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use jsonwebtoken::{self, Algorithm, DecodingKey, Validation};
use ryu::Buffer;
use serde::Deserialize;
use serde::Deserializer;

/// Contract `chronological_anchor`: explicit timing route for JSON path extraction and verifier alignment.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ChronologicalAnchor {
    PayloadNative,
    LocalContainer,
    HardwarePtp,
}

fn default_chronological_anchor() -> ChronologicalAnchor {
    ChronologicalAnchor::PayloadNative
}

#[derive(Debug, Deserialize)]
pub(crate) struct BridgeConfig {
    #[serde(default = "default_chronological_anchor")]
    pub(crate) chronological_anchor: ChronologicalAnchor,
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
    /// Emits a single [DEPRECATION] warning when legacy keys are normalized.
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
        if let Some(ref mut rest) = self.connector.rest {
            if rest.field_paths.is_none() {
                rest.field_paths = Some(vec![rest.price_path.clone(), rest.volume_path.clone()]);
                deprecation_emitted = true;
            }
        }

        // Message bus: price_path/volume_path -> field_paths when field_paths is absent
        if let Some(ref mut mb) = self.connector.message_bus {
            if mb.field_paths.is_none() {
                mb.field_paths = Some(vec![mb.price_path.clone(), mb.volume_path.clone()]);
                deprecation_emitted = true;
            }
            let p = mb.provider.to_lowercase();
            if p == "kafka"
                && mb.kafka.is_none()
                && !(mb.bootstrap_servers.is_empty()
                    && mb.topic.is_empty()
                    && mb.group_id.is_empty())
            {
                mb.kafka = Some(KafkaNestedCfg {
                    bootstrap_servers: std::mem::take(&mut mb.bootstrap_servers),
                    topic: std::mem::take(&mut mb.topic),
                    group_id: std::mem::take(&mut mb.group_id),
                    sasl_username: None,
                    sasl_password: None,
                    sasl_mechanism: None,
                    security_protocol: None,
                    client_properties: HashMap::new(),
                    env_client_properties: HashMap::new(),
                });
            } else if p == "mqtt" && mb.mqtt.is_none() && !mb.broker.is_empty() {
                let ts = mb.timestamp_path.take().unwrap_or_default();
                mb.mqtt = Some(MqttNestedCfg {
                    broker: std::mem::take(&mut mb.broker),
                    topic: std::mem::take(&mut mb.topic),
                    timestamp_path: ts,
                    username: mb.username.take(),
                    password: mb.password.take(),
                    tls_cert_path: mb.tls_cert_path.take(),
                    client_properties: HashMap::new(),
                    env_client_properties: HashMap::new(),
                });
            }
            if p == "kafka" {
                mb.mqtt = None;
            } else if p == "mqtt" {
                mb.kafka = None;
            }
        }

        if deprecation_emitted {
            eprintln!(
                "[DEPRECATION] Legacy config keys (price_field/volume_field, price_path/volume_path) were normalized. \
                 Please migrate to field_count/field_map and field_paths. See TERMINOLOGY.md."
            );
        }
    }

    /// Merge `T3THR_*` env tables and resolve legacy `${T3THR_*}` whole-value placeholders into header maps.
    pub fn resolve_connector_env_placeholders(&mut self) -> Result<()> {
        if let Some(r) = self.connector.rest.as_mut() {
            let mut map = header_kv_vec_to_map(&r.headers);
            utils::warn_deprecated_placeholders_in_literal_map(
                &map,
                "connector.rest.headers",
                "`[connector.rest.env_headers]`",
            );
            utils::resolve_string_map_placeholders(&mut map, "connector.rest", "headers")?;
            utils::merge_env_binding_map_into(
                &mut map,
                &r.env_headers,
                "connector.rest",
                "env_headers",
            )?;
            r.headers = map_to_header_kv_vec(map);
        }
        if let Some(w) = self.connector.websocket.as_mut() {
            let mut map = header_kv_vec_to_map(&w.headers);
            utils::warn_deprecated_placeholders_in_literal_map(
                &map,
                "connector.websocket.headers",
                "`[connector.websocket.env_headers]`",
            );
            utils::resolve_string_map_placeholders(&mut map, "connector.websocket", "headers")?;
            utils::merge_env_binding_map_into(
                &mut map,
                &w.env_headers,
                "connector.websocket",
                "env_headers",
            )?;
            w.headers = map_to_header_kv_vec(map);
        }
        if let Some(g) = self.connector.grpc.as_mut() {
            let mut map = header_kv_vec_to_map(&g.metadata);
            utils::warn_deprecated_placeholders_in_literal_map(
                &map,
                "connector.grpc.metadata",
                "`[connector.grpc.env_metadata]`",
            );
            utils::resolve_string_map_placeholders(&mut map, "connector.grpc", "metadata")?;
            utils::merge_env_binding_map_into(
                &mut map,
                &g.env_metadata,
                "connector.grpc",
                "env_metadata",
            )?;
            g.metadata = map_to_header_kv_vec(map);
        }
        if let Some(m) = self.connector.message_bus.as_mut() {
            if let Some(ref mut k) = m.kafka {
                utils::warn_deprecated_placeholders_in_literal_map(
                    &k.client_properties,
                    "connector.message_bus.kafka.client_properties",
                    "`[connector.message_bus.kafka.env_client_properties]`",
                );
                utils::resolve_string_map_placeholders(
                    &mut k.client_properties,
                    "connector.message_bus.kafka",
                    "client_properties",
                )?;
                utils::merge_env_binding_map_into(
                    &mut k.client_properties,
                    &k.env_client_properties,
                    "connector.message_bus.kafka",
                    "env_client_properties",
                )?;
            }
            if let Some(ref mut mq) = m.mqtt {
                utils::warn_deprecated_placeholders_in_literal_map(
                    &mq.client_properties,
                    "connector.message_bus.mqtt.client_properties",
                    "`[connector.message_bus.mqtt.env_client_properties]`",
                );
                utils::resolve_string_map_placeholders(
                    &mut mq.client_properties,
                    "connector.message_bus.mqtt",
                    "client_properties",
                )?;
                utils::merge_env_binding_map_into(
                    &mut mq.client_properties,
                    &mq.env_client_properties,
                    "connector.message_bus.mqtt",
                    "env_client_properties",
                )?;
            }
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

/// Claims carried by a FORS33 license token (local daemon-minted or cloud-issued).
#[allow(dead_code)] // JWT carries these canonical claims; not all are consumed in this binary.
#[derive(Debug, Deserialize)]
struct LicenseClaims {
    sub: String,
    tier: String,
    exp: usize,
    #[serde(default)]
    t3thr_entitled: Option<bool>,
    #[serde(default)]
    allowed_connectors: Option<Vec<String>>,
}

// Public Ed25519 key for verifying FORS33 license tokens (JWT EdDSA).
// This is a PEM-encoded public key compiled into the binary so it cannot
// be replaced via a mounted file inside a container.
const FORS33_LICENSE_PUBKEY_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MCowBQYDK2VwAyEA/////////////////////////////////////////w==\n\
-----END PUBLIC KEY-----\n";

/// Decode JWT with EdDSA: try `FORS33_RUNTIME_PUBKEY_PEM` first, then embedded cloud issuer key.
fn decode_license_claims(token: &str) -> Result<LicenseClaims> {
    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.validate_exp = true;

    if let Ok(runtime_pem) = std::env::var("FORS33_RUNTIME_PUBKEY_PEM") {
        let runtime_pem = runtime_pem.trim();
        if !runtime_pem.is_empty() {
            if let Ok(key) = DecodingKey::from_ed_pem(runtime_pem.as_bytes()) {
                if let Ok(td) = jsonwebtoken::decode::<LicenseClaims>(token, &key, &validation) {
                    return Ok(td.claims);
                }
            }
        }
    }

    let decoding_key = DecodingKey::from_ed_pem(FORS33_LICENSE_PUBKEY_PEM.as_bytes())
        .map_err(|e| anyhow!("failed to construct cloud license decoding key: {e}"))?;
    let token_data = jsonwebtoken::decode::<LicenseClaims>(token, &decoding_key, &validation)
        .map_err(|e| anyhow!("invalid or expired FORS33 license token: {e}"))?;
    Ok(token_data.claims)
}

fn verify_fors33_license(requested_connector: &str) -> Result<LicenseClaims> {
    let token = std::env::var("FORS33_LICENSE_KEY")
        .map_err(|_| anyhow!("FORS33_LICENSE_KEY is not set"))?;

    let claims = decode_license_claims(&token)?;

    if claims.t3thr_entitled == Some(true) {
        return Ok(claims);
    }

    if let Some(ref allowed) = claims.allowed_connectors {
        if !allowed
            .iter()
            .any(|c| c.eq_ignore_ascii_case(requested_connector))
        {
            return Err(anyhow!(
                "license does not permit connector type: {requested_connector}"
            ));
        }
    }

    Ok(claims)
}

#[allow(dead_code)] // Parsed for schema compatibility; connector-specific runtime may not consume all fields.
#[derive(Debug, Deserialize)]
struct SyslogCfgTbl {
    format: String,
    transport: String,
    #[serde(default)]
    listen_address: Option<String>,
    #[serde(default)]
    connect_address: Option<String>,
}

#[allow(dead_code)] // Parsed for schema compatibility; connector-specific runtime may not consume all fields.
#[derive(Debug, Deserialize)]
struct UdpRawCfgTbl {
    bind_address: String,
    port: u16,
    #[serde(default = "default_udp_max_datagram")]
    max_datagram_bytes: usize,
    #[serde(default)]
    field_paths: Option<Vec<String>>,
    #[serde(default)]
    timestamp_path: Option<String>,
}

fn default_udp_max_datagram() -> usize {
    65_535
}

#[allow(dead_code)] // Parsed for schema compatibility; connector-specific runtime may not consume all fields.
#[derive(Debug, Deserialize)]
struct CdcPostgresNestTbl {
    host: String,
    #[serde(default = "default_pg_port")]
    port: u16,
    database: String,
    user: String,
    slot_name: String,
    publication_name: String,
    #[serde(default)]
    resume_lsn: Option<String>,
    #[serde(default)]
    timestamp_path: Option<String>,
}

fn default_pg_port() -> u16 {
    5432
}

#[allow(dead_code)] // Parsed for schema compatibility; connector-specific runtime may not consume all fields.
#[derive(Debug, Deserialize)]
struct CdcMysqlNestTbl {
    host: String,
    #[serde(default = "default_mysql_port")]
    port: u16,
    database: String,
    user: String,
    #[serde(default = "default_cdc_mysql_gtid_mode")]
    gtid_mode: String,
    #[serde(default)]
    resume_gtid: Option<String>,
}

fn default_cdc_mysql_gtid_mode() -> String {
    "server_executed".to_string()
}

fn default_mysql_port() -> u16 {
    3306
}

#[allow(dead_code)] // Parsed for schema compatibility; connector-specific runtime may not consume all fields.
#[derive(Debug, Deserialize)]
struct CdcCfgTbl {
    engine: String,
    #[serde(default)]
    field_paths: Option<Vec<String>>,
    #[serde(default, rename = "postgres_config")]
    postgres_config: Option<CdcPostgresNestTbl>,
    #[serde(default, rename = "mysql_config")]
    mysql_config: Option<CdcMysqlNestTbl>,
}

#[derive(Debug, Deserialize)]
struct ConnectorCfg {
    #[allow(dead_code)] // Kept to deserialize explicit connector type from TOML.
    #[serde(default = "default_connector_type")]
    r#type: String, // "csv" | "websocket" | "rest" | "kafka" | "mqtt" | "grpc"
    #[serde(default = "default_mode")]
    mode: Option<String>, // "stream" (default) or "batch"
    file: Option<connector_file::FileCfg>,
    csv: Option<CsvCfg>,
    websocket: Option<WebSocketCfgUnified>,
    rest: Option<RestCfg>,
    message_bus: Option<MessageBusCfgUnified>,
    grpc: Option<GrpcCfg>,
    syslog: Option<SyslogCfgTbl>,
    udp_raw: Option<UdpRawCfgTbl>,
    cdc: Option<CdcCfgTbl>,
}

fn default_connector_type() -> String {
    "csv".to_string()
}

fn default_mode() -> Option<String> {
    Some("stream".to_string())
}

#[allow(dead_code)] // Parsed for schema compatibility; connector-specific runtime may not consume all fields.
#[derive(Debug, Deserialize)]
struct WebSocketCfgUnified {
    #[serde(default = "default_ws_provider")]
    provider: String, // "kraken" | "alchemy" | "infura" | "binance" | "custom"
    #[serde(default)]
    url: String,
    #[serde(default)]
    symbol: Option<String>, // Kraken: e.g. "BTC/USD"
    #[serde(default)]
    subscription: Option<String>, // Alchemy/Infura: "newHeads" | "alchemy_pendingTransactions"
    #[serde(default)]
    stream: Option<String>, // Binance: e.g. "btcusdt@trade"
    #[serde(default)]
    field_paths: Option<Vec<String>>, // Custom: JSONPath expressions for metrics
    #[serde(default)]
    timestamp_path: Option<String>, // Custom: JSONPath for timestamp
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default, deserialize_with = "deserialize_header_kv_vec")]
    headers: Vec<HeaderKv>,
    #[serde(default)]
    env_headers: HashMap<String, String>,
    #[serde(default = "default_reconnect_delay")]
    reconnect_delay_secs: u64,
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
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default, deserialize_with = "deserialize_header_kv_vec")]
    headers: Vec<HeaderKv>,
    #[serde(default)]
    env_headers: HashMap<String, String>,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    cursor_field: Option<String>,
    #[serde(default)]
    page_limit: Option<usize>,
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

#[allow(dead_code)] // Parsed for schema compatibility; connector-specific runtime may not consume all fields.
#[derive(Debug, Deserialize, Clone, Default)]
struct KafkaNestedCfg {
    #[serde(default)]
    bootstrap_servers: String,
    #[serde(default)]
    topic: String,
    #[serde(default)]
    group_id: String,
    /// SASL credentials. Populated at runtime via the
    /// `FORS33_SECRET_CONNECTOR__MESSAGE_BUS__KAFKA__SASL_USERNAME/PASSWORD`
    /// env overlay; never read from the on-disk TOML.
    #[serde(default)]
    sasl_username: Option<String>,
    #[serde(default)]
    sasl_password: Option<String>,
    /// Optional mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512). Defaults to PLAIN
    /// when SASL credentials are present.
    #[serde(default)]
    sasl_mechanism: Option<String>,
    /// Optional `security.protocol` override (e.g. `SASL_SSL`, `SASL_PLAINTEXT`).
    /// Defaults to `SASL_SSL` when SASL credentials are present.
    #[serde(default)]
    security_protocol: Option<String>,
    /// Extra rdkafka `ClientConfig` key/value literals (legacy); prefer `env_client_properties`.
    #[serde(default)]
    client_properties: HashMap<String, String>,
    /// Property key maps to `T3THR_*` env name (resolved verbatim).
    #[serde(default)]
    env_client_properties: HashMap<String, String>,
}

#[allow(dead_code)] // Parsed for schema compatibility; connector-specific runtime may not consume all fields.
#[derive(Debug, Deserialize, Clone, Default)]
struct MqttNestedCfg {
    #[serde(default)]
    broker: String,
    #[serde(default)]
    topic: String,
    #[serde(default)]
    timestamp_path: String,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    tls_cert_path: Option<String>,
    #[serde(default)]
    client_properties: HashMap<String, String>,
    #[serde(default)]
    env_client_properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct MessageBusCfgUnified {
    #[serde(default = "default_message_bus_provider")]
    provider: String, // "kafka" | "mqtt"
    #[serde(default)]
    kafka: Option<KafkaNestedCfg>,
    #[serde(default)]
    mqtt: Option<MqttNestedCfg>,
    /// Legacy flat Kafka fields (pre nested `kafka` table). Normalized in `normalize_and_validate`.
    #[serde(default)]
    bootstrap_servers: String,
    #[serde(default)]
    topic: String,
    #[serde(default = "default_message_bus_group_id")]
    group_id: String,
    /// Legacy flat MQTT broker (pre nested `mqtt` table).
    #[serde(default)]
    broker: String,
    #[serde(default = "default_price_path")]
    price_path: String,
    #[serde(default = "default_volume_path")]
    volume_path: String,
    #[serde(default)]
    field_paths: Option<Vec<String>>,
    #[serde(default)]
    timestamp_path: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    tls_cert_path: Option<String>,
}

fn default_message_bus_provider() -> String {
    "kafka".to_string()
}

fn default_message_bus_group_id() -> String {
    "aos2_bridge".to_string()
}

#[allow(dead_code)] // Parsed for schema compatibility; connector-specific runtime may not consume all fields.
#[derive(Debug, Deserialize)]
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
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    tls_cert_path: Option<String>,
    #[serde(default, deserialize_with = "deserialize_header_kv_vec")]
    metadata: Vec<HeaderKv>,
    #[serde(default)]
    env_metadata: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct HeaderKv {
    key: String,
    value: String,
}

fn header_kv_vec_to_map(v: &[HeaderKv]) -> HashMap<String, String> {
    v.iter().map(|h| (h.key.clone(), h.value.clone())).collect()
}

fn map_to_header_kv_vec(m: HashMap<String, String>) -> Vec<HeaderKv> {
    let mut pairs: Vec<_> = m.into_iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
        .into_iter()
        .map(|(key, value)| HeaderKv { key, value })
        .collect()
}

fn deserialize_header_kv_vec<'de, D>(deserializer: D) -> Result<Vec<HeaderKv>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Flex {
        Map(HashMap<String, String>),
        List(Vec<HeaderKv>),
    }
    match Option::<Flex>::deserialize(deserializer)? {
        None => Ok(Vec::new()),
        Some(Flex::Map(m)) => Ok(map_to_header_kv_vec(m)),
        Some(Flex::List(l)) => Ok(l),
    }
}

fn default_grpc_service() -> String {
    "market.MarketData".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct NormalizerCfg {
    // Legacy 2-field support (deprecated but functional)
    pub price_field: Option<String>,
    pub volume_field: Option<String>,

    // N-field support
    pub field_count: Option<usize>,
    pub field_map: Option<std::collections::HashMap<String, usize>>, // source_field -> vector index

    pub timestamp_field: Option<String>,
    pub timestamp_unit: Option<String>,   // s, ms, ns, tick
    pub timestamp_tick_hz: Option<f64>,   // required when unit = "tick"
    pub timestamp_format: Option<String>, // "datetime_utc" | "datetime_utc_ms" | "time_utc"
    /// When timestamp column is time-of-day only (e.g. "00:00:00.0140000"), combine with this date (YYYY-MM-DD)
    pub timestamp_date_override: Option<String>,
}

impl NormalizerCfg {
    /// Get the expected field count (either from field_count or legacy 2-field)
    pub fn get_field_count(&self) -> usize {
        if let Some(count) = self.field_count {
            count
        } else if self.price_field.is_some() && self.volume_field.is_some() {
            2 // Legacy mode
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
    fail_fast: Option<bool>, // Stop at first filter violation (default: true)

    // Legacy 2-field bounds (deprecated but functional)
    price_min: Option<f64>,
    price_max: Option<f64>,
    volume_min: Option<f64>,
    volume_max: Option<f64>,
    #[allow(dead_code)] // Legacy tuning knob retained for backward-compatible config decoding.
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
    pub metrics: std::collections::HashMap<String, f64>, // metric_N_max_delta -> threshold
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
            (
                self.price_min.unwrap_or(f64::NEG_INFINITY),
                self.price_max.unwrap_or(f64::INFINITY),
            )
        } else if index == 1 {
            (
                self.volume_min.unwrap_or(0.0),
                self.volume_max.unwrap_or(f64::INFINITY),
            )
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
    /// Legacy single-file path. Required when `output_dir` and `file_prefix` are not set.
    #[serde(default)]
    accepted_path: String,
    dead_letter_path: String,
    /// Output format: "csv" | "jsonl" | "canonical_jsonl" | "parquet".
    /// Default "csv". Parquet only for batch/file modes.
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
    /// Directory for rotated or single-prefix accepted files (live attestation). Optional; uses `accepted_path` when unset.
    #[serde(default)]
    pub(crate) output_dir: Option<String>,
    /// File name prefix inside `output_dir` (no extension). Optional.
    #[serde(default)]
    pub(crate) file_prefix: Option<String>,
    /// `none` (single file `{prefix}.{ext}`) or `daily` (UTC `{prefix}_YYYY-MM-DD.{ext}`).
    #[serde(default)]
    pub(crate) rotation: Option<String>,
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
                vec![
                    "timestamp_ns".to_string(),
                    "price".to_string(),
                    "volume".to_string(),
                ]
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

    fn format_extension(&self) -> &'static str {
        match self.format.to_lowercase().as_str() {
            "canonical_jsonl" | "jsonl" => "jsonl",
            "csv" => "csv",
            "parquet" => "parquet",
            _ => "csv",
        }
    }

    /// Resolved accepted output path. When `utc_date` is `Some`, used for `daily` rotation shard name (YYYY-MM-DD UTC).
    pub(crate) fn resolved_accepted_path_utc(&self, utc_date: Option<&str>) -> Result<PathBuf> {
        let dir_o = self
            .output_dir
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());
        let pre_o = self
            .file_prefix
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());
        if let (Some(dir), Some(prefix)) = (dir_o, pre_o) {
            let rot = self
                .rotation
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("none")
                .to_lowercase();
            let ext = self.format_extension();
            if rot == "daily" {
                let d = utc_date
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| Utc::now().format("%Y-%m-%d").to_string());
                Ok(PathBuf::from(dir).join(format!("{prefix}_{d}.{ext}")))
            } else if rot == "none" {
                Ok(PathBuf::from(dir).join(format!("{prefix}.{ext}")))
            } else {
                Err(anyhow!(
                    "output.rotation must be 'none' or 'daily' (got {rot})"
                ))
            }
        } else if !self.accepted_path.trim().is_empty() {
            Ok(PathBuf::from(self.accepted_path.trim()))
        } else {
            Err(anyhow!(
                "output.accepted_path is required unless output_dir and file_prefix are set"
            ))
        }
    }

    pub(crate) fn is_daily_rotation(&self) -> bool {
        let dir_o = self
            .output_dir
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());
        let pre_o = self
            .file_prefix
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());
        if dir_o.is_none() || pre_o.is_none() {
            return false;
        }
        self.rotation
            .as_deref()
            .map(str::trim)
            .unwrap_or("")
            .eq_ignore_ascii_case("daily")
    }
}

/// Accepted output format for the sink.
enum AcceptedFormat {
    Csv(csv::Writer<File>),
    Jsonl(File),
    CanonicalJsonl(File),
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
    rotating_daily: bool,
    last_shard_utc_day: String,
    active_accepted_path: PathBuf,
}

impl DataSink {
    fn open_accepted_handle(path: &Path, format_lower: &str) -> Result<AcceptedFormat> {
        if format_lower == "jsonl" {
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("failed opening accepted output {}", path.display()))?;
            Ok(AcceptedFormat::Jsonl(f))
        } else if format_lower == "canonical_jsonl" {
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("failed opening accepted output {}", path.display()))?;
            Ok(AcceptedFormat::CanonicalJsonl(f))
        } else {
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .with_context(|| format!("failed opening accepted output {}", path.display()))?;
            Ok(AcceptedFormat::Csv(WriterBuilder::new().from_writer(f)))
        }
    }

    pub(crate) fn new(cfg: &BridgeConfig, field_count: usize) -> Result<Self> {
        let accepted_path = cfg.output.resolved_accepted_path_utc(None)?;
        let dead_path = PathBuf::from(&cfg.output.dead_letter_path);

        ensure_parent(&accepted_path)?;
        ensure_parent(&dead_path)?;

        let format_lower = cfg.output.format.to_lowercase();
        let accepted = Self::open_accepted_handle(&accepted_path, &format_lower)?;

        let dead_file = File::create(&dead_path).with_context(|| {
            format!("failed opening dead-letter output {}", dead_path.display())
        })?;

        let rotating_daily = cfg.output.is_daily_rotation();
        let last_shard_utc_day = if rotating_daily {
            Utc::now().format("%Y-%m-%d").to_string()
        } else {
            String::new()
        };

        let mut sink = Self {
            accepted,
            dead_letter: dead_file,
            headers_written: false,
            field_count,
            output_cfg: cfg.output.clone(),
            rotating_daily,
            last_shard_utc_day,
            active_accepted_path: accepted_path.clone(),
        };

        // If the accepted file already exists and is non-empty, validate schema.
        if accepted_path.exists() {
            let metadata = std::fs::metadata(&accepted_path).with_context(|| {
                format!("failed reading metadata for {}", accepted_path.display())
            })?;
            if metadata.len() > 0 {
                // Header validation only for CSV (JSONL has no header row).
                if format_lower != "jsonl" && format_lower != "canonical_jsonl" {
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

    fn rotate_accepted_if_needed(&mut self) -> Result<()> {
        if !self.rotating_daily {
            return Ok(());
        }
        let today = Utc::now().format("%Y-%m-%d").to_string();
        if today == self.last_shard_utc_day {
            return Ok(());
        }
        let new_path = self.output_cfg.resolved_accepted_path_utc(Some(&today))?;
        if new_path == self.active_accepted_path {
            self.last_shard_utc_day = today;
            return Ok(());
        }
        ensure_parent(&new_path)?;
        let format_lower = self.output_cfg.format.to_lowercase();
        self.accepted = Self::open_accepted_handle(&new_path, &format_lower)?;
        self.headers_written = false;
        self.active_accepted_path = new_path;
        self.last_shard_utc_day = today;

        if self.active_accepted_path.exists() {
            let metadata = std::fs::metadata(&self.active_accepted_path).with_context(|| {
                format!(
                    "failed reading metadata for {}",
                    self.active_accepted_path.display()
                )
            })?;
            if metadata.len() > 0 && format_lower != "jsonl" && format_lower != "canonical_jsonl" {
                if let Some(first_line) =
                    utils::read_first_nonempty_line(&self.active_accepted_path)?
                {
                    let existing: Vec<String> = first_line
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect();
                    let expected = self.output_cfg.get_headers(self.field_count);
                    if existing != expected {
                        return Err(anyhow!(
                            "Header mismatch in accepted file.\n  existing: {:?}\n  expected: {:?}",
                            existing,
                            expected
                        ));
                    }
                    self.headers_written = true;
                }
            }
        }
        Ok(())
    }

    /// Write an accepted DataPoint.
    pub fn write_accepted(&mut self, point: &DataPoint) -> Result<()> {
        self.rotate_accepted_if_needed()?;
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
            AcceptedFormat::CanonicalJsonl(ref mut f) => {
                // Deterministic JSON text:
                // - key order fixed: timestamp_ns, then metric_<i> in Vec index order
                // - floats formatted via ryu (round-trip shortest form)
                let line = canonical_jsonl_line(point)?;
                f.write_all(line.as_bytes())?;
                Ok(())
            }
        }
    }

    /// Write a rejected record to the dead-letter JSONL file.
    pub fn write_rejected(
        &mut self,
        reason: &str,
        raw_record: &str,
        record_timestamp_ns: Option<u64>,
    ) -> Result<()> {
        let now_ns = now_unix_ms() * 1_000_000;
        let ts_ns = record_timestamp_ns.unwrap_or(now_ns);

        // Deterministic payload for SEC/non-finite evidence.
        if reason == "Non-finite metric detected" {
            let raw_json = serde_json::to_string(raw_record)?;
            write!(
                &mut self.dead_letter,
                "{{\"timestamp_ns\":{},\"reason\":\"Non-finite metric detected\",\"raw_record\":{}}}\n",
                ts_ns, raw_json
            )?;
            return Ok(());
        }

        let shaped = self.output_cfg.shape_deadletter_raw_record(raw_record);
        let obj = serde_json::json!({
            "timestamp_ns": ts_ns,
            "reason": reason,
            "raw_record": shaped,
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

fn canonical_jsonl_line(point: &DataPoint) -> Result<String> {
    if point.metrics.iter().any(|m| !m.is_finite()) {
        return Err(anyhow!(
            "non-finite metric in canonical_jsonl accepted record"
        ));
    }
    let mut out = String::with_capacity(64 + (point.metrics.len() * 24));
    out.push_str("{\"timestamp_ns\":");
    out.push_str(&point.timestamp_ns.to_string());
    let mut buf = Buffer::new();
    for (idx, m) in point.metrics.iter().enumerate() {
        out.push_str(",\"metric_");
        out.push_str(&idx.to_string());
        out.push_str("\":");
        out.push_str(buf.format(*m));
    }
    out.push_str("}\n");
    Ok(out)
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
                    // Contract: dead-letter evidence must use a stable reason string.
                    // Additional numeric context is intentionally omitted for deterministic payload.
                    let _ = idx;
                    return Err("Non-finite metric detected".to_string());
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
        let date = date_override.ok_or_else(|| {
            anyhow!("time_utc format requires timestamp_date_override (YYYY-MM-DD)")
        })?;
        let time_str = value.trim();
        // Parse HH:MM:SS or HH:MM:SS.ffffff
        let naive_time = chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f")
            .or_else(|_| chrono::NaiveTime::parse_from_str(time_str, "%H:%M:%S"))
            .with_context(|| format!("failed parsing time '{}'", time_str))?;
        let naive_date = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .with_context(|| format!("failed parsing date '{}'", date))?;
        let naive = chrono::NaiveDateTime::new(naive_date, naive_time);
        let dt = Utc.from_utc_datetime(&naive);
        let ns = dt
            .timestamp_nanos_opt()
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
    let naive = NaiveDateTime::parse_from_str(value.trim(), fmt).with_context(|| {
        format!(
            "Failed parsing datetime '{}' with format '{}'. \
            Use a chrono format string like '%Y-%m-%d %H:%M:%S' or a preset like 'datetime_utc'. \
            See https://docs.rs/chrono/latest/chrono/format/strftime/index.html for format codes.",
            value, fmt
        )
    })?;
    let dt = Utc.from_utc_datetime(&naive);
    let ns = dt
        .timestamp_nanos_opt()
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
        return Err(anyhow!(
            "No field configuration found (need field_count or legacy price_field/volume_field)"
        ));
    }

    let field_map = ncfg
        .field_map
        .as_ref()
        .ok_or_else(|| anyhow!("field_map required for N-field mode"))?;

    // Pre-allocate vector with exact capacity
    let mut metrics = vec![0.0; field_count];
    let mut fields_found = 0;

    // Map source fields to vector positions
    for (source_field, &index) in field_map.iter() {
        if index >= field_count {
            return Err(anyhow!(
                "field_map index {} exceeds field_count {}",
                index,
                field_count
            ));
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

    let accepted_ctr = Arc::new(AtomicUsize::new(0));
    let dropped_ctr = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let accepted_for_thread = accepted_ctr.clone();
    let dropped_for_thread = dropped_ctr.clone();
    let done_for_thread = done.clone();
    let metrics_status = "RECEIVING";
    let metrics_thread = std::thread::spawn(move || {
        while !done_for_thread.load(AtomicOrdering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if done_for_thread.load(AtomicOrdering::Relaxed) {
                break;
            }
            let a = accepted_for_thread.load(AtomicOrdering::Relaxed);
            let d = dropped_for_thread.load(AtomicOrdering::Relaxed);
            println!(
                "t3thr_metrics accepted={} dropped={} status={}",
                a, d, metrics_status
            );
        }
    });

    // Stream processing: read-parse-filter-write-drop (constant memory)
    // Note: The csv crate can be overly strict about field counts. We catch errors
    // and route to dead-letter queue per spec: "Extra fields are simply ignored"
    let records_iter = reader.records();
    for row in records_iter {
        let record = match row {
            Ok(r) => r,
            Err(e) => {
                // CSV reader error (field count mismatch, malformed row, etc.)
                dropped_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                let error_msg = format!("CSV Read Error: {}", e);
                sink.write_rejected(&error_msg, "", None)?;
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
                dropped_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                let prefix = if cfg.filter.drop_on_parse_error {
                    "Parse Error"
                } else {
                    "Parse Warning"
                };
                let reason = format!("{}: {}", prefix, e.to_string());
                let raw_record = record.iter().collect::<Vec<_>>().join("|");
                sink.write_rejected(&reason, &raw_record, None)?;
                if cfg.filter.drop_on_parse_error {
                    continue;
                }
                continue;
            }
        };

        // Filter check with fail-fast logic
        match state.check(&point, &cfg.filter) {
            Ok(()) => {
                accepted_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                sink.write_accepted(&point)?;
            }
            Err(reason) => {
                dropped_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                let raw_record = record.iter().collect::<Vec<_>>().join("|");
                sink.write_rejected(&reason, &raw_record, Some(point.timestamp_ns))?;
            }
        }
    }

    done.store(true, AtomicOrdering::Relaxed);
    let _ = metrics_thread.join();
    let accepted = accepted_ctr.load(AtomicOrdering::Relaxed);
    let dropped = dropped_ctr.load(AtomicOrdering::Relaxed);

    let acc_log = cfg
        .output
        .resolved_accepted_path_utc(None)
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| cfg.output.accepted_path.clone());
    println!(
        "data_bridge done | accepted={} dropped={} | accepted_path={} dead_letter_path={}",
        accepted, dropped, acc_log, cfg.output.dead_letter_path
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
        token: w.token.clone(),
        api_key: w.api_key.clone(),
        headers: w
            .headers
            .iter()
            .map(|h| connector_websocket::HeaderKv {
                key: h.key.clone(),
                value: h.value.clone(),
            })
            .collect(),
    };

    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    let capacity = cfg.channel_capacity();
    let (tx, rx) =
        std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String, Option<u64>)>>(capacity);

    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();

    let rt = tokio::runtime::Runtime::new()?;
    let conn_handle = std::thread::spawn(move || {
        rt.block_on(async {
            if let Err(e) =
                connector_websocket::run_websocket_connector(&ws_cfg, &filter_cfg, tx).await
            {
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

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

    let accepted_ctr = Arc::new(AtomicUsize::new(0));
    let dropped_ctr = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let accepted_for_thread = accepted_ctr.clone();
    let dropped_for_thread = dropped_ctr.clone();
    let done_for_thread = done.clone();
    let metrics_status = "RECEIVING";
    let metrics_thread = std::thread::spawn(move || {
        while !done_for_thread.load(AtomicOrdering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if done_for_thread.load(AtomicOrdering::Relaxed) {
                break;
            }
            let a = accepted_for_thread.load(AtomicOrdering::Relaxed);
            let d = dropped_for_thread.load(AtomicOrdering::Relaxed);
            println!(
                "t3thr_metrics accepted={} dropped={} status={}",
                a, d, metrics_status
            );
        }
    });

    for msg in rx {
        match msg {
            Ok(point) => {
                accepted_ctr.fetch_add(1, AtomicOrdering::Relaxed);
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
            Err((reason, raw, ts_ns_opt)) => {
                dropped_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                sink.write_rejected(&reason, &raw, ts_ns_opt)?;
            }
        }
    }

    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }

    let _ = conn_handle.join();

    done.store(true, AtomicOrdering::Relaxed);
    let _ = metrics_thread.join();

    let accepted = accepted_ctr.load(AtomicOrdering::Relaxed);
    let dropped = dropped_ctr.load(AtomicOrdering::Relaxed);
    println!(
        "t3thr websocket done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

#[cfg(not(feature = "full_engine"))]
fn run_websocket_mode(_cfg: &BridgeConfig) -> Result<()> {
    Err(anyhow!("websocket connector requires full_engine feature"))
}

fn run_rest_mode(cfg: &BridgeConfig, state_path: Option<&Path>) -> Result<()> {
    let rest_cfg = cfg
        .connector
        .rest
        .as_ref()
        .ok_or_else(|| anyhow!("connector.rest required for rest mode"))?;
    let field_paths = rest_cfg
        .field_paths
        .clone()
        .unwrap_or_else(|| vec![rest_cfg.price_path.clone(), rest_cfg.volume_path.clone()]);
    let rest_token = rest_cfg.token.clone();
    let rest_api_key = rest_cfg.api_key.clone();
    let rest_headers: Vec<connector_rest::HeaderKv> = rest_cfg
        .headers
        .iter()
        .map(|h| connector_rest::HeaderKv {
            key: h.key.clone(),
            value: h.value.clone(),
        })
        .collect();
    let rest_cfg = connector_rest::RestCfg {
        url: rest_cfg.url.clone(),
        poll_interval_ms: rest_cfg.poll_interval_ms,
        field_paths,
        timestamp_path: rest_cfg.timestamp_path.clone(),
        response_format: rest_cfg.response_format.clone(),
        mode: rest_cfg.mode.clone(),
        cursor_field: rest_cfg.cursor_field.clone(),
        page_limit: rest_cfg.page_limit,
        token: rest_token,
        api_key: rest_api_key,
        headers: rest_headers,
    };

    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    let (tx, rx) = std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String, Option<u64>)>>(
        cfg.channel_capacity(),
    );
    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();
    let output_cfg = cfg.output.clone();
    let state_path_clone = state_path.map(|p| p.to_path_buf());

    let _conn_handle = std::thread::spawn(move || {
        if let Err(e) = connector_rest::run_rest_connector(
            &rest_cfg,
            &filter_cfg,
            &output_cfg,
            tx,
            state_path_clone.as_deref(),
        ) {
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

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

    let accepted_ctr = Arc::new(AtomicUsize::new(0));
    let dropped_ctr = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let accepted_for_thread = accepted_ctr.clone();
    let dropped_for_thread = dropped_ctr.clone();
    let done_for_thread = done.clone();
    let metrics_status = "RECEIVING";
    let metrics_thread = std::thread::spawn(move || {
        while !done_for_thread.load(AtomicOrdering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if done_for_thread.load(AtomicOrdering::Relaxed) {
                break;
            }
            let a = accepted_for_thread.load(AtomicOrdering::Relaxed);
            let d = dropped_for_thread.load(AtomicOrdering::Relaxed);
            println!(
                "t3thr_metrics accepted={} dropped={} status={}",
                a, d, metrics_status
            );
        }
    });

    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted_ctr.fetch_add(1, AtomicOrdering::Relaxed);
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
            Err((reason, raw, ts_ns_opt)) => {
                dropped_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                sink.write_rejected(&reason, &raw, ts_ns_opt)?;
            }
        }
    }

    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }

    done.store(true, AtomicOrdering::Relaxed);
    let _ = metrics_thread.join();

    let accepted = accepted_ctr.load(AtomicOrdering::Relaxed);
    let dropped = dropped_ctr.load(AtomicOrdering::Relaxed);
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

    let mb_cfg = match mb.provider.to_lowercase().as_str() {
        "kafka" => {
            let k = mb.kafka.as_ref().ok_or_else(|| {
                anyhow!("connector.message_bus.kafka table is required when provider = \"kafka\"")
            })?;
            if k.bootstrap_servers.is_empty() || k.topic.is_empty() || k.group_id.is_empty() {
                return Err(anyhow!(
                    "connector.message_bus.kafka requires bootstrap_servers, topic, and group_id"
                ));
            }
            connector_message_bus::MessageBusCfg {
                provider: mb.provider.clone(),
                bootstrap_servers: k.bootstrap_servers.clone(),
                topic: k.topic.clone(),
                group_id: k.group_id.clone(),
                broker: String::new(),
                field_paths: mb
                    .field_paths
                    .clone()
                    .unwrap_or_else(|| vec![mb.price_path.clone(), mb.volume_path.clone()]),
                timestamp_path: mb.timestamp_path.clone(),
                username: None,
                password: None,
                kafka_sasl_username: k.sasl_username.clone(),
                kafka_sasl_password: k.sasl_password.clone(),
                kafka_sasl_mechanism: k.sasl_mechanism.clone(),
                kafka_security_protocol: k.security_protocol.clone(),
                kafka_extra_props: k.client_properties.clone(),
            }
        }
        "mqtt" => {
            let m = mb.mqtt.as_ref().ok_or_else(|| {
                anyhow!("connector.message_bus.mqtt table is required when provider = \"mqtt\"")
            })?;
            if m.broker.is_empty() || m.topic.is_empty() || m.timestamp_path.is_empty() {
                return Err(anyhow!(
                    "connector.message_bus.mqtt requires broker, topic, and timestamp_path"
                ));
            }
            connector_message_bus::MessageBusCfg {
                provider: mb.provider.clone(),
                bootstrap_servers: String::new(),
                topic: m.topic.clone(),
                group_id: String::new(),
                broker: m.broker.clone(),
                field_paths: mb
                    .field_paths
                    .clone()
                    .unwrap_or_else(|| vec![mb.price_path.clone(), mb.volume_path.clone()]),
                timestamp_path: Some(m.timestamp_path.clone()),
                username: m.username.clone(),
                password: m.password.clone(),
                kafka_sasl_username: None,
                kafka_sasl_password: None,
                kafka_sasl_mechanism: None,
                kafka_security_protocol: None,
                kafka_extra_props: HashMap::new(),
            }
        }
        _ => {
            return Err(anyhow!(
                "connector.message_bus.provider must be \"kafka\" or \"mqtt\""
            ));
        }
    };

    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    let capacity = cfg.channel_capacity();
    let (tx, rx) =
        std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String, Option<u64>)>>(capacity);

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

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

    let accepted_ctr = Arc::new(AtomicUsize::new(0));
    let dropped_ctr = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let accepted_for_thread = accepted_ctr.clone();
    let dropped_for_thread = dropped_ctr.clone();
    let done_for_thread = done.clone();
    let metrics_status = "RECEIVING";
    let metrics_thread = std::thread::spawn(move || {
        while !done_for_thread.load(AtomicOrdering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if done_for_thread.load(AtomicOrdering::Relaxed) {
                break;
            }
            let a = accepted_for_thread.load(AtomicOrdering::Relaxed);
            let d = dropped_for_thread.load(AtomicOrdering::Relaxed);
            println!(
                "t3thr_metrics accepted={} dropped={} status={}",
                a, d, metrics_status
            );
        }
    });

    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted_ctr.fetch_add(1, AtomicOrdering::Relaxed);
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
            Err((reason, raw, ts_ns_opt)) => {
                dropped_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                sink.write_rejected(&reason, &raw, ts_ns_opt)?;
            }
        }
    }

    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }

    let _ = conn_handle.join();

    done.store(true, AtomicOrdering::Relaxed);
    let _ = metrics_thread.join();

    let accepted = accepted_ctr.load(AtomicOrdering::Relaxed);
    let dropped = dropped_ctr.load(AtomicOrdering::Relaxed);
    println!(
        "t3thr message_bus done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

#[cfg(not(feature = "full_engine"))]
fn run_message_bus_mode(_cfg: &BridgeConfig) -> Result<()> {
    Err(anyhow!(
        "message_bus connector requires full_engine feature"
    ))
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
        token: g.token.clone(),
        metadata_pairs: g
            .metadata
            .iter()
            .map(|h| (h.key.clone(), h.value.clone()))
            .collect(),
    };

    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;

    let capacity = cfg.channel_capacity();
    let (tx, rx) =
        std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String, Option<u64>)>>(capacity);

    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();

    let rt = tokio::runtime::Runtime::new()?;
    let conn_handle = std::thread::spawn(move || {
        rt.block_on(async {
            if let Err(e) = connector_grpc::run_grpc_connector(&grpc_cfg, &filter_cfg, tx).await {
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

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

    let accepted_ctr = Arc::new(AtomicUsize::new(0));
    let dropped_ctr = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));

    let accepted_for_thread = accepted_ctr.clone();
    let dropped_for_thread = dropped_ctr.clone();
    let done_for_thread = done.clone();
    let metrics_status = "RECEIVING";
    let metrics_thread = std::thread::spawn(move || {
        while !done_for_thread.load(AtomicOrdering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if done_for_thread.load(AtomicOrdering::Relaxed) {
                break;
            }
            let a = accepted_for_thread.load(AtomicOrdering::Relaxed);
            let d = dropped_for_thread.load(AtomicOrdering::Relaxed);
            println!(
                "t3thr_metrics accepted={} dropped={} status={}",
                a, d, metrics_status
            );
        }
    });

    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted_ctr.fetch_add(1, AtomicOrdering::Relaxed);
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
            Err((reason, raw, ts_ns_opt)) => {
                dropped_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                sink.write_rejected(&reason, &raw, ts_ns_opt)?;
            }
        }
    }

    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }

    let _ = conn_handle.join();
    done.store(true, AtomicOrdering::Relaxed);
    let _ = metrics_thread.join();

    let accepted = accepted_ctr.load(AtomicOrdering::Relaxed);
    let dropped = dropped_ctr.load(AtomicOrdering::Relaxed);
    println!(
        "t3thr grpc done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

#[cfg(not(feature = "full_engine"))]
fn run_grpc_mode(_cfg: &BridgeConfig) -> Result<()> {
    Err(anyhow!("grpc connector requires full_engine feature"))
}

#[cfg(feature = "full_engine")]
fn run_syslog_mode(cfg: &BridgeConfig) -> Result<()> {
    let s = cfg
        .connector
        .syslog
        .as_ref()
        .ok_or_else(|| anyhow!("connector.syslog required for syslog mode"))?;
    let scfg = connector_syslog::SyslogCfg {
        format: s.format.clone(),
        transport: s.transport.clone(),
        listen_address: s.listen_address.clone(),
        connect_address: s.connect_address.clone(),
    };
    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;
    let capacity = cfg.channel_capacity();
    let (tx, rx) =
        std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String, Option<u64>)>>(capacity);
    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();
    let rt = tokio::runtime::Runtime::new()?;
    let conn_handle = std::thread::spawn(move || {
        rt.block_on(async {
            if let Err(e) =
                connector_syslog::run_syslog_connector(&scfg, &filter_cfg, field_count, tx).await
            {
                eprintln!("[BRIDGE] syslog connector error: {}", e);
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    let accepted_ctr = Arc::new(AtomicUsize::new(0));
    let dropped_ctr = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let accepted_for_thread = accepted_ctr.clone();
    let dropped_for_thread = dropped_ctr.clone();
    let done_for_thread = done.clone();
    let metrics_status = "RECEIVING";
    let metrics_thread = std::thread::spawn(move || {
        while !done_for_thread.load(AtomicOrdering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if done_for_thread.load(AtomicOrdering::Relaxed) {
                break;
            }
            let a = accepted_for_thread.load(AtomicOrdering::Relaxed);
            let d = dropped_for_thread.load(AtomicOrdering::Relaxed);
            println!(
                "t3thr_metrics accepted={} dropped={} status={}",
                a, d, metrics_status
            );
        }
    });
    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted_ctr.fetch_add(1, AtomicOrdering::Relaxed);
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
            Err((reason, raw, ts_ns_opt)) => {
                dropped_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                sink.write_rejected(&reason, &raw, ts_ns_opt)?;
            }
        }
    }
    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }
    let _ = conn_handle.join();
    done.store(true, AtomicOrdering::Relaxed);
    let _ = metrics_thread.join();
    let accepted = accepted_ctr.load(AtomicOrdering::Relaxed);
    let dropped = dropped_ctr.load(AtomicOrdering::Relaxed);
    println!(
        "t3thr syslog done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

#[cfg(not(feature = "full_engine"))]
fn run_syslog_mode(_cfg: &BridgeConfig) -> Result<()> {
    Err(anyhow!("syslog connector requires full_engine feature"))
}

#[cfg(feature = "full_engine")]
fn run_udp_raw_mode(cfg: &BridgeConfig) -> Result<()> {
    let u = cfg
        .connector
        .udp_raw
        .as_ref()
        .ok_or_else(|| anyhow!("connector.udp_raw required for udp_raw mode"))?;
    let paths = u
        .field_paths
        .as_ref()
        .cloned()
        .filter(|p| !p.is_empty())
        .ok_or_else(|| anyhow!("connector.udp_raw.field_paths is required"))?;
    let ucfg = connector_udp_raw::UdpRawCfg {
        bind_address: u.bind_address.clone(),
        port: u.port,
        max_datagram_bytes: u.max_datagram_bytes,
        field_paths: paths,
        timestamp_path: u.timestamp_path.clone(),
    };
    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;
    let capacity = cfg.channel_capacity();
    let (tx, rx) =
        std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String, Option<u64>)>>(capacity);
    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();
    let rt = tokio::runtime::Runtime::new()?;
    let conn_handle = std::thread::spawn(move || {
        rt.block_on(async {
            if let Err(e) = connector_udp_raw::run_udp_raw_connector(&ucfg, &filter_cfg, tx).await {
                eprintln!("[BRIDGE] udp_raw connector error: {}", e);
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    let accepted_ctr = Arc::new(AtomicUsize::new(0));
    let dropped_ctr = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let accepted_for_thread = accepted_ctr.clone();
    let dropped_for_thread = dropped_ctr.clone();
    let done_for_thread = done.clone();
    let metrics_status = "RECEIVING";
    let metrics_thread = std::thread::spawn(move || {
        while !done_for_thread.load(AtomicOrdering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if done_for_thread.load(AtomicOrdering::Relaxed) {
                break;
            }
            let a = accepted_for_thread.load(AtomicOrdering::Relaxed);
            let d = dropped_for_thread.load(AtomicOrdering::Relaxed);
            println!(
                "t3thr_metrics accepted={} dropped={} status={}",
                a, d, metrics_status
            );
        }
    });
    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted_ctr.fetch_add(1, AtomicOrdering::Relaxed);
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
            Err((reason, raw, ts_ns_opt)) => {
                dropped_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                sink.write_rejected(&reason, &raw, ts_ns_opt)?;
            }
        }
    }
    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }
    let _ = conn_handle.join();
    done.store(true, AtomicOrdering::Relaxed);
    let _ = metrics_thread.join();
    let accepted = accepted_ctr.load(AtomicOrdering::Relaxed);
    let dropped = dropped_ctr.load(AtomicOrdering::Relaxed);
    println!(
        "t3thr udp_raw done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

#[cfg(not(feature = "full_engine"))]
fn run_udp_raw_mode(_cfg: &BridgeConfig) -> Result<()> {
    Err(anyhow!("udp_raw connector requires full_engine feature"))
}

#[cfg(feature = "full_engine")]
fn run_cdc_postgres_mode(cfg: &BridgeConfig) -> Result<()> {
    let cdc = cfg
        .connector
        .cdc
        .as_ref()
        .ok_or_else(|| anyhow!("connector.cdc required"))?;
    if !cdc.engine.eq_ignore_ascii_case("postgres") {
        return Err(anyhow!("cdc.engine must be postgres"));
    }
    let c = cdc
        .postgres_config
        .as_ref()
        .ok_or_else(|| anyhow!("cdc.postgres_config required"))?;
    let paths = cdc
        .field_paths
        .as_ref()
        .cloned()
        .filter(|p| !p.is_empty())
        .ok_or_else(|| anyhow!("connector.cdc.field_paths is required"))?;
    let ccfg = connector_cdc_postgres::CdcPostgresCfg {
        host: c.host.clone(),
        port: c.port,
        database: c.database.clone(),
        user: c.user.clone(),
        slot_name: c.slot_name.clone(),
        publication_name: c.publication_name.clone(),
        field_paths: paths,
        timestamp_path: c.timestamp_path.clone(),
    };
    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;
    let capacity = cfg.channel_capacity();
    let (tx, rx) =
        std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String, Option<u64>)>>(capacity);
    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();
    let rt = tokio::runtime::Runtime::new()?;
    let conn_handle = std::thread::spawn(move || {
        rt.block_on(async {
            if let Err(e) =
                connector_cdc_postgres::run_cdc_postgres_connector(&ccfg, &filter_cfg, tx).await
            {
                eprintln!("[BRIDGE] cdc_postgres connector error: {}", e);
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    let accepted_ctr = Arc::new(AtomicUsize::new(0));
    let dropped_ctr = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let accepted_for_thread = accepted_ctr.clone();
    let dropped_for_thread = dropped_ctr.clone();
    let done_for_thread = done.clone();
    let metrics_status = "RECEIVING";
    let metrics_thread = std::thread::spawn(move || {
        while !done_for_thread.load(AtomicOrdering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if done_for_thread.load(AtomicOrdering::Relaxed) {
                break;
            }
            let a = accepted_for_thread.load(AtomicOrdering::Relaxed);
            let d = dropped_for_thread.load(AtomicOrdering::Relaxed);
            println!(
                "t3thr_metrics accepted={} dropped={} status={}",
                a, d, metrics_status
            );
        }
    });
    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted_ctr.fetch_add(1, AtomicOrdering::Relaxed);
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
            Err((reason, raw, ts_ns_opt)) => {
                dropped_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                sink.write_rejected(&reason, &raw, ts_ns_opt)?;
            }
        }
    }
    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }
    let _ = conn_handle.join();
    done.store(true, AtomicOrdering::Relaxed);
    let _ = metrics_thread.join();
    let accepted = accepted_ctr.load(AtomicOrdering::Relaxed);
    let dropped = dropped_ctr.load(AtomicOrdering::Relaxed);
    println!(
        "t3thr cdc_postgres done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

#[cfg(not(feature = "full_engine"))]
fn run_cdc_postgres_mode(_cfg: &BridgeConfig) -> Result<()> {
    Err(anyhow!(
        "cdc_postgres connector requires full_engine feature"
    ))
}

#[cfg(feature = "full_engine")]
fn run_cdc_mysql_mode(cfg: &BridgeConfig) -> Result<()> {
    use mysql::OptsBuilder;
    use mysql::prelude::Queryable;

    let cdc = cfg
        .connector
        .cdc
        .as_ref()
        .ok_or_else(|| anyhow!("connector.cdc required"))?;
    if !cdc.engine.eq_ignore_ascii_case("mysql") {
        return Err(anyhow!("cdc.engine must be mysql"));
    }
    let c = cdc
        .mysql_config
        .as_ref()
        .ok_or_else(|| anyhow!("cdc.mysql_config required"))?;
    let gm = c.gtid_mode.trim();
    if !gm.eq_ignore_ascii_case("server_executed") {
        return Err(anyhow!(
            "cdc_mysql: gtid_mode must be server_executed (got {:?})",
            c.gtid_mode
        ));
    }
    let pw = std::env::var("FORS33_SECRET_CONNECTOR__CDC_MYSQL__PASSWORD").unwrap_or_default();
    let probe_opts: mysql::Opts = OptsBuilder::default()
        .ip_or_hostname(Some(c.host.clone()))
        .tcp_port(c.port)
        .db_name(Some(c.database.clone()))
        .user(Some(c.user.clone()))
        .pass(Some(pw.clone()))
        .into();
    let mut probe =
        mysql::Conn::new(probe_opts).map_err(|e| anyhow!("cdc_mysql connect: {}", e))?;
    let gtid: Option<String> = probe
        .query_first("SELECT @@GLOBAL.GTID_EXECUTED")
        .map_err(|e| anyhow!("cdc_mysql read GTID_EXECUTED: {}", e))?;
    let g = gtid.unwrap_or_default();
    if g.trim().is_empty() {
        return Err(anyhow!(
            "cdc_mysql: @@GLOBAL.GTID_EXECUTED is empty; enable GTID on the server"
        ));
    }
    let ccfg = connector_cdc_mysql::CdcMysqlCfg {
        host: c.host.clone(),
        port: c.port,
        database: c.database.clone(),
        user: c.user.clone(),
    };
    let field_count = cfg.normalizer.get_field_count();
    let mut sink = DataSink::new(cfg, field_count)?;
    let capacity = cfg.channel_capacity();
    let (tx, rx) =
        std::sync::mpsc::sync_channel::<Result<DataPoint, (String, String, Option<u64>)>>(capacity);
    let filter_cfg = cfg.filter.clone();
    let pipe_path = cfg.output.pipe_path.clone();
    let conn_handle = std::thread::spawn(move || {
        let _ = connector_cdc_mysql::run_cdc_mysql_blocking(&ccfg, &filter_cfg, field_count, tx);
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    let accepted_ctr = Arc::new(AtomicUsize::new(0));
    let dropped_ctr = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let accepted_for_thread = accepted_ctr.clone();
    let dropped_for_thread = dropped_ctr.clone();
    let done_for_thread = done.clone();
    let metrics_status = "RECEIVING";
    let metrics_thread = std::thread::spawn(move || {
        while !done_for_thread.load(AtomicOrdering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if done_for_thread.load(AtomicOrdering::Relaxed) {
                break;
            }
            let a = accepted_for_thread.load(AtomicOrdering::Relaxed);
            let d = dropped_for_thread.load(AtomicOrdering::Relaxed);
            println!(
                "t3thr_metrics accepted={} dropped={} status={}",
                a, d, metrics_status
            );
        }
    });
    for msg in rx {
        match msg {
            Ok(tick) => {
                accepted_ctr.fetch_add(1, AtomicOrdering::Relaxed);
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
            Err((reason, raw, ts_ns_opt)) => {
                dropped_ctr.fetch_add(1, AtomicOrdering::Relaxed);
                sink.write_rejected(&reason, &raw, ts_ns_opt)?;
            }
        }
    }
    if let Some(mut w) = pipe_writer {
        let _ = w.flush();
    }
    let _ = conn_handle.join();
    done.store(true, AtomicOrdering::Relaxed);
    let _ = metrics_thread.join();
    let accepted = accepted_ctr.load(AtomicOrdering::Relaxed);
    let dropped = dropped_ctr.load(AtomicOrdering::Relaxed);
    println!(
        "t3thr cdc_mysql done | accepted={} dropped={}",
        accepted, dropped
    );
    Ok(())
}

#[cfg(not(feature = "full_engine"))]
fn run_cdc_mysql_mode(_cfg: &BridgeConfig) -> Result<()> {
    Err(anyhow!("cdc_mysql connector requires full_engine feature"))
}

fn print_config_help() {
    println!(
        r#"
=== T3thr configuration guide ===

T3thr uses TOML configuration files. Here's what each section does:

[connector.csv]
  input_path = "path/to/data.csv"    # Path to your CSV file
  has_headers = true                 # Does the first row contain column names?

[connector.rest]
  url = "https://api.example.com"    # HTTP endpoint to poll
  poll_interval_ms = 1000            # How often to poll (milliseconds)

[connector.websocket]
  url = "wss://stream.example.com"   # WebSocket URL
  provider = "custom"                # "kraken", "binance", "alchemy", or "custom"

[connector.message_bus]
  provider = "mqtt"                  # "mqtt" or "kafka"
  broker = "localhost:1883"          # MQTT broker (host:port)
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
"#
    );
}

/// Fail fast when this binary was built without optional connectors (`slim_engine` default).
#[cfg(not(feature = "full_engine"))]
fn assert_binary_supports_config(cfg: &BridgeConfig) -> Result<()> {
    if cfg.connector.websocket.is_some()
        || cfg.connector.message_bus.is_some()
        || cfg.connector.grpc.is_some()
        || cfg.connector.syslog.is_some()
        || cfg.connector.udp_raw.is_some()
        || cfg.connector.cdc.is_some()
    {
        return Err(anyhow!(
            "This t3thr binary was built without the full_engine feature; \
             websocket, message_bus, grpc, syslog, udp_raw, and cdc connectors are unavailable. \
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

fn validate_config_only_pipeline(cfg: &BridgeConfig) -> Result<()> {
    let connector_count = (cfg.connector.file.is_some() as usize)
        + (cfg.connector.csv.is_some() as usize)
        + (cfg.connector.websocket.is_some() as usize)
        + (cfg.connector.rest.is_some() as usize)
        + (cfg.connector.message_bus.is_some() as usize)
        + (cfg.connector.grpc.is_some() as usize)
        + (cfg.connector.syslog.is_some() as usize)
        + (cfg.connector.udp_raw.is_some() as usize)
        + (cfg.connector.cdc.is_some() as usize);

    if connector_count >= 2 {
        return Err(anyhow!(
            "[Fors33] CONFIG ERROR: Multiple connectors detected. Exactly one connector block is allowed. Found {connector_count}."
        ));
    }

    let is_live = cfg.connector.websocket.is_some()
        || cfg.connector.rest.is_some()
        || cfg.connector.message_bus.is_some()
        || cfg.connector.grpc.is_some()
        || cfg.connector.syslog.is_some()
        || cfg.connector.udp_raw.is_some()
        || cfg.connector.cdc.is_some();

    if is_live && cfg.output.format.to_ascii_lowercase() == "parquet" {
        return Err(anyhow!(
            "output.format = \"parquet\" is not supported for live connectors (websocket, rest, message_bus, grpc, syslog, udp_raw, cdc)."
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
        } else if cfg.connector.syslog.is_some() {
            "syslog"
        } else if cfg.connector.udp_raw.is_some() {
            "udp_raw"
        } else if cfg.connector.cdc.is_some() {
            "cdc"
        } else {
            "unknown"
        };
        verify_fors33_license(requested_connector)
            .context("license validation failed for live connector")?;
    }

    Ok(())
}

fn load_bridge_config(path: &Path) -> Result<BridgeConfig> {
    let path_s = path
        .to_str()
        .ok_or_else(|| anyhow!("config path is not valid UTF-8"))?;
    let built = config::Config::builder()
        .add_source(config::File::with_name(path_s))
        .add_source(
            config::Environment::with_prefix("FORS33_SECRET")
                .separator("__")
                .prefix_separator("_"),
        )
        .build()
        .context("failed loading config with env overlay")?;
    let mut cfg: BridgeConfig = built
        .try_deserialize()
        .context("failed deserializing bridge config")?;
    cfg.normalize_and_validate();
    cfg.resolve_connector_env_placeholders()
        .context("failed resolving connector environment bindings")?;
    Ok(cfg)
}

pub(crate) fn execute_run(args: &commands::RunArgs) -> Result<()> {
    let cli = args.clone();

    if cli.explain {
        print_config_help();
        return Ok(());
    }

    if cli.config_wizard {
        return crate::commands::wizard::execute();
    }

    let mut cfg = load_bridge_config(&cli.config)?;
    assert_binary_supports_config(&cfg)?;

    if cli.validate_only {
        validate_config_only_pipeline(&cfg)?;
        println!("Configuration valid.");
        return Ok(());
    }

    match cfg.chronological_anchor {
        ChronologicalAnchor::PayloadNative
        | ChronologicalAnchor::LocalContainer
        | ChronologicalAnchor::HardwarePtp => {}
    }

    eprintln!("[Fors33] T3thr Ingestion Engine Initialized.");
    eprintln!("[Fors33] NOTICE: Software provided \"AS IS\". Operator assumes all responsibility");
    eprintln!("[Fors33] for data retention and network stability.");
    eprintln!("[Fors33] go to fors33.com/t3thr for the full EULA.");

    let connector_count = (cfg.connector.file.is_some() as usize)
        + (cfg.connector.csv.is_some() as usize)
        + (cfg.connector.websocket.is_some() as usize)
        + (cfg.connector.rest.is_some() as usize)
        + (cfg.connector.message_bus.is_some() as usize)
        + (cfg.connector.grpc.is_some() as usize)
        + (cfg.connector.syslog.is_some() as usize)
        + (cfg.connector.udp_raw.is_some() as usize)
        + (cfg.connector.cdc.is_some() as usize);

    if connector_count >= 2 {
        return Err(anyhow!(format!(
            "[Fors33] CONFIG ERROR: Multiple connectors detected. Exactly one connector block is allowed. Found {}.",
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

    let is_live = cfg.connector.websocket.is_some()
        || cfg.connector.rest.is_some()
        || cfg.connector.message_bus.is_some()
        || cfg.connector.grpc.is_some()
        || cfg.connector.syslog.is_some()
        || cfg.connector.udp_raw.is_some()
        || cfg.connector.cdc.is_some();

    // License gate: live/network connectors require a valid FORS33 license.
    if is_live {
        // Dev-only: deterministic PID1 exit validation without a real license or network activity.
        // Simulates "writer channel closed" fatal shutdown.
        if cfg!(feature = "dev_license_bypass")
            && std::env::var("T3THR_TEST_FORCE_WRITER_CLOSED")
                .ok()
                .as_deref()
                == Some("1")
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
        } else if cfg.connector.syslog.is_some() {
            "syslog"
        } else if cfg.connector.udp_raw.is_some() {
            "udp_raw"
        } else if cfg.connector.cdc.is_some() {
            "cdc"
        } else {
            "unknown"
        };

        if !cfg!(feature = "dev_license_bypass") {
            if let Err(err) = verify_fors33_license(requested_connector) {
                eprintln!();
                eprintln!(
                    "[Fors33] ACCESS DENIED: Live Streaming Requires Active Subscription. The requested connector is restricted."
                );
                eprintln!("Reason: {err}");
                eprintln!("1. Purchase access at https://fors33.com/t3thr.");
                eprintln!("2. After receiving your license key, run with:");
                eprintln!(
                    "   docker run -e FORS33_LICENSE_KEY=\"your_key\" -v $(pwd)/config:/app/config fors33/t3thr ..."
                );
                return Err(anyhow!("license validation failed for live connector"));
            }
        }
    }

    // Determine if this is a batch job (exits after completion) or stream job (runs indefinitely)
    let is_batch = cfg.connector.mode.as_deref() == Some("batch");

    // State tracking for batch mode resume capability
    let state_path = if is_batch && !cli.no_state {
        // Auto-detect state path from accepted_path parent directory
        let accepted_path = Path::new(&cfg.output.accepted_path);
        let parent = accepted_path.parent().unwrap_or(accepted_path);
        Some(parent.join(".t3thr-state.json"))
    } else {
        None
    };

    // Handle --reset-state flag: delete state file if exists
    if cli.reset_state {
        if let Some(ref path) = state_path {
            if path.exists() {
                eprintln!("[Fors33] Resetting state file: {}", path.display());
                std::fs::remove_file(path)
                    .with_context(|| format!("failed to remove state file {}", path.display()))?;
            }
        }
    }

    // Acquire state lock for concurrent run protection (batch mode only)
    let _state_lock = if let Some(ref path) = state_path {
        Some(utils::acquire_state_lock(path).with_context(|| {
            "Another T3thr instance is already running this batch job. Use --reset-state to force a fresh start."
        })?)
    } else {
        None
    };

    // Check if state file has completed status (prevent accidental re-run)
    if let Some(ref path) = state_path {
        if let Ok(Some(state)) = utils::load_state(path) {
            if state.status == "completed" {
                eprintln!("[Fors33] Batch job already completed (status: completed).");
                eprintln!(
                    "[Fors33] To run again, use --reset-state flag or delete the state file."
                );
                std::process::exit(1);
            }
        }
    }

    // File-connector batch and stream paths are CPU-heavy synchronous workloads.
    // We host them inside a multi-thread tokio runtime and use `block_in_place`
    // so that the runtime can swap in a fresh worker for any other async tasks
    // while this OS thread is held by the file walker. This is the architectural
    // analogue of `tokio::task::spawn_blocking` for closures that borrow owned
    // state (BridgeConfig is intentionally not `Clone`/`'static`-bound here, so
    // `spawn_blocking`'s `Send + 'static` requirement would force a much larger
    // refactor without behavioral upside in the current single-process layout).
    // Practical effect today: OS-level CPU fairness on tight Docker Desktop VM
    // budgets; the architectural property (reactor stays responsive) is preserved
    // for any future revision that multiplexes live + batch in one runtime.
    let result = if cfg.connector.file.is_some() {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            tokio::task::block_in_place(|| {
                connector_file::run_file_mode(&cfg, state_path.as_deref())
            })
        })
    } else if cfg.connector.websocket.is_some() {
        run_websocket_mode(&cfg)
    } else if cfg.connector.rest.is_some() {
        run_rest_mode(&cfg, state_path.as_deref())
    } else if cfg.connector.message_bus.is_some() {
        run_message_bus_mode(&cfg)
    } else if cfg.connector.grpc.is_some() {
        run_grpc_mode(&cfg)
    } else if cfg.connector.syslog.is_some() {
        run_syslog_mode(&cfg)
    } else if cfg.connector.udp_raw.is_some() {
        run_udp_raw_mode(&cfg)
    } else if let Some(ref cdc) = cfg.connector.cdc {
        if cdc.engine.eq_ignore_ascii_case("postgres") {
            run_cdc_postgres_mode(&cfg)
        } else if cdc.engine.eq_ignore_ascii_case("mysql") {
            run_cdc_mysql_mode(&cfg)
        } else {
            Err(anyhow!("cdc.engine must be postgres or mysql"))
        }
    } else {
        run_csv_mode(&cfg)
    };

    // If this was a batch job and it completed successfully, exit with code 0
    // This enables CI/CD pipelines and Cron jobs to detect completion
    if is_batch {
        if result.is_ok() {
            eprintln!("[Fors33] Batch job completed successfully. Exiting with code 0.");
            std::process::exit(0);
        } else {
            eprintln!("[Fors33] Batch job failed. Exiting with code 1.");
            std::process::exit(1);
        }
    }

    result
}

fn main() -> Result<()> {
    use clap::Parser as _;
    let argv = commands::argv_with_default_run(std::env::args());
    let cli = commands::Cli::parse_from(argv);
    commands::dispatch(cli)
}

#[cfg(test)]
mod tests {
    use super::{DataPoint, canonical_jsonl_line};

    #[test]
    fn canonical_jsonl_is_deterministic_and_ordered() {
        let p = DataPoint {
            timestamp_ns: 42,
            metrics: vec![1.25, 2.0, 3.5],
        };
        let line = canonical_jsonl_line(&p).expect("canonical line");
        assert_eq!(
            line,
            "{\"timestamp_ns\":42,\"metric_0\":1.25,\"metric_1\":2.0,\"metric_2\":3.5}\n"
        );
    }

    #[test]
    fn canonical_jsonl_rejects_non_finite() {
        let p = DataPoint {
            timestamp_ns: 42,
            metrics: vec![1.25, f64::NAN],
        };
        let err = canonical_jsonl_line(&p).expect_err("non-finite must fail");
        assert!(err.to_string().contains("non-finite"));
    }

    #[test]
    fn message_bus_nested_kafka_toml_loads() {
        let toml_src = r#"
[connector.message_bus]
provider = "kafka"
field_paths = ["price", "volume"]

[connector.message_bus.kafka]
bootstrap_servers = "localhost:9092"
topic = "ticks"
group_id = "g1"

[normalizer]
field_count = 2
field_map = { price = 0, volume = 1 }

[filter]
reject_nan_inf = true
replay_mode = false
drop_on_parse_error = true
fail_fast = true
future_tolerance_ms = 60000
stale_tolerance_ms = 300000

[filter.bounds]
metric_0.min = 0.0
metric_0.max = 1.0e12
metric_1.min = 0.0
metric_1.max = 1.0e12

[output]
accepted_path = "out/a.csv"
dead_letter_path = "out/d.csv"
format = "csv"
"#;
        let mut cfg: super::BridgeConfig = toml::from_str(toml_src).expect("toml parse");
        cfg.normalize_and_validate();
        let mb = cfg.connector.message_bus.as_ref().expect("message_bus");
        assert_eq!(mb.provider, "kafka");
        assert!(mb.kafka.is_some());
        assert!(mb.mqtt.is_none());
        let k = mb.kafka.as_ref().unwrap();
        assert_eq!(k.bootstrap_servers, "localhost:9092");
        assert_eq!(k.topic, "ticks");
        assert_eq!(k.group_id, "g1");
    }

    #[test]
    fn cdc_postgres_nested_toml_loads() {
        let toml_src = r#"
[connector.cdc]
engine = "postgres"
field_paths = ["metric_0"]

[connector.cdc.postgres_config]
host = "localhost"
port = 5432
database = "app"
user = "u"
slot_name = "s1"
publication_name = "pub1"

[normalizer]
field_count = 1
field_map = { metric_0 = 0 }

[filter]
reject_nan_inf = true
replay_mode = false
drop_on_parse_error = true
fail_fast = true
future_tolerance_ms = 60000
stale_tolerance_ms = 300000

[filter.bounds]
metric_0.min = 0.0
metric_0.max = 1.0e12

[output]
accepted_path = "out/a.csv"
dead_letter_path = "out/d.csv"
format = "csv"
"#;
        let mut cfg: super::BridgeConfig = toml::from_str(toml_src).expect("toml parse");
        cfg.normalize_and_validate();
        let cdc = cfg.connector.cdc.as_ref().expect("cdc");
        assert!(cdc.engine.eq_ignore_ascii_case("postgres"));
        let pg = cdc.postgres_config.as_ref().expect("postgres_config");
        assert_eq!(pg.host, "localhost");
        assert!(cdc.mysql_config.is_none());
    }
}
