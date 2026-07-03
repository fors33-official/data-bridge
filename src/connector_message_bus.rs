//! Unified Message Bus connector for Kafka and MQTT.
//!
//! Single module with provider-specific connection; shared parse path:
//! subscribe → parse JSON payload → map to DataPoint → filter pipeline.
//! Uses N-dimensional field_paths for JSONPath extraction (deep paths like "sensors.0.vitals.heart_rate").

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde_json::Value;

use crate::tls_verifier;
use crate::{DataPoint, FilterCfg, FilterState, now_unix_ms, parse_datetime_to_ns};

/// Unified config for Kafka or MQTT. Provider determines which fields are used.
#[derive(Debug, Clone)]
pub struct MessageBusCfg {
    pub provider: String, // "kafka" | "mqtt"
    pub bootstrap_servers: String,
    pub topic: String,
    pub group_id: String,
    pub broker: String,
    /// Ordered JSONPaths mapping to metrics\[index\]. Required (synthesized from price_path/volume_path if legacy).
    pub field_paths: Vec<String>,
    pub timestamp_path: Option<String>,
    /// MQTT credentials (from TOML or FORS33_SECRET env overlay).
    pub username: Option<String>,
    pub password: Option<String>,
    /// Kafka SASL credentials populated by the
    /// `FORS33_SECRET_CONNECTOR__MESSAGE_BUS__KAFKA__SASL_USERNAME/PASSWORD`
    /// env overlay; never read from on-disk TOML.
    pub kafka_sasl_username: Option<String>,
    pub kafka_sasl_password: Option<String>,
    /// Optional SASL mechanism (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`).
    /// Defaults to `PLAIN` when SASL credentials are present.
    pub kafka_sasl_mechanism: Option<String>,
    /// Optional `security.protocol` override. Defaults to `SASL_SSL` when SASL
    /// credentials are present.
    pub kafka_security_protocol: Option<String>,
    /// Additional rdkafka `ClientConfig` keys (from TOML after T3THR / FORS33 expansion).
    pub kafka_extra_props: HashMap<String, String>,
}

/// Deep JSONPath extraction: supports "field", "nested.field", "array.0.field".
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

fn parse_json_ndimensional(payload: &str, cfg: &MessageBusCfg) -> Result<DataPoint> {
    let v: Value = serde_json::from_str(payload).context("invalid JSON")?;
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
        feed: None,
    })
}

fn parse_broker(broker: &str) -> (String, u16) {
    if let Some((host, port_str)) = broker.split_once(':') {
        let port: u16 = port_str.parse().unwrap_or(1883);
        (host.to_string(), port)
    } else {
        (broker.to_string(), 1883)
    }
}

/// Run unified message bus connector; async, call from tokio runtime.
pub async fn run_message_bus_connector(
    cfg: &MessageBusCfg,
    filter_cfg: &FilterCfg,
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
) -> Result<()> {
    match cfg.provider.to_lowercase().as_str() {
        "kafka" => run_kafka_connector(cfg, filter_cfg, tx).await,
        "mqtt" => run_mqtt_connector(cfg, filter_cfg, tx).await,
        _ => Err(anyhow!("Unknown message bus provider: {}", cfg.provider)),
    }
}

async fn run_kafka_connector(
    cfg: &MessageBusCfg,
    filter_cfg: &FilterCfg,
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
) -> Result<()> {
    use rdkafka::Message;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};

    let mut client_cfg = ClientConfig::new();
    client_cfg
        .set("bootstrap.servers", &cfg.bootstrap_servers)
        .set("group.id", &cfg.group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true");

    // SASL credentials are loaded from env (memory-only) so they never appear
    // in the TOML on disk. We default to SASL_SSL + PLAIN when credentials are
    // present; operators can override by setting `sasl_mechanism` /
    // `security_protocol` in the [connector.message_bus.kafka] table.
    if let (Some(u), Some(p)) = (
        cfg.kafka_sasl_username.as_deref(),
        cfg.kafka_sasl_password.as_deref(),
    ) {
        let u = u.trim();
        let p = p.trim();
        if !u.is_empty() && !p.is_empty() {
            let mech = cfg
                .kafka_sasl_mechanism
                .as_deref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .unwrap_or("PLAIN");
            let proto = cfg
                .kafka_security_protocol
                .as_deref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .unwrap_or("SASL_SSL");
            client_cfg
                .set("security.protocol", proto)
                .set("sasl.mechanism", mech)
                .set("sasl.username", u)
                .set("sasl.password", p);
        }
    }

    for (prop_key, raw_val) in &cfg.kafka_extra_props {
        let pk = prop_key.trim();
        if pk.is_empty() {
            continue;
        }
        let expanded =
            crate::utils::expand_fors33_secret_placeholders(raw_val).with_context(|| {
                format!("Kafka client property {pk:?} placeholder expansion failed")
            })?;
        client_cfg.set(pk, expanded.trim());
    }

    let consumer: StreamConsumer = client_cfg
        .create()
        .context("failed to create Kafka consumer")?;

    consumer
        .subscribe(&[&cfg.topic])
        .context("failed to subscribe to topic")?;

    let mut state = FilterState::default();

    loop {
        let msg = consumer.recv().await;
        match msg {
            Ok(m) => {
                let payload = match m.payload() {
                    Some(p) => match std::str::from_utf8(p) {
                        Ok(s) => s.to_string(),
                        Err(e) => {
                            if tx
                                .send(Err((format!("Parse Error: {}", e), String::new(), None)))
                                .is_err()
                            {
                                eprintln!(
                                    "[FORS33] FATAL: Writer channel closed. Stopping message_bus connector."
                                );
                                std::process::exit(1);
                            }
                            continue;
                        }
                    },
                    None => continue,
                };

                let tick = match parse_json_ndimensional(&payload, cfg) {
                    Ok(t) => t,
                    Err(e) => {
                        if tx
                            .send(Err((format!("Parse Error: {}", e), payload.clone(), None)))
                            .is_err()
                        {
                            eprintln!(
                                "[FORS33] FATAL: Writer channel closed. Stopping message_bus connector."
                            );
                            std::process::exit(1);
                        }
                        continue;
                    }
                };

                match state.check(&tick, filter_cfg) {
                    Ok(()) => {
                        if tx.send(Ok(tick)).is_err() {
                            eprintln!(
                                "[FORS33] FATAL: Writer channel closed. Stopping message_bus connector."
                            );
                            std::process::exit(1);
                        }
                    }
                    Err(reason) => {
                        if tx
                            .send(Err((reason, payload.clone(), Some(tick.timestamp_ns))))
                            .is_err()
                        {
                            eprintln!(
                                "[FORS33] FATAL: Writer channel closed. Stopping message_bus connector."
                            );
                            std::process::exit(1);
                        }
                    }
                }
            }
            Err(e) => return Err(anyhow!("Kafka recv error: {}", e)),
        }
    }
}

async fn run_mqtt_connector(
    cfg: &MessageBusCfg,
    filter_cfg: &FilterCfg,
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
) -> Result<()> {
    use rumqttc::v5::mqttbytes::QoS;
    use rumqttc::v5::mqttbytes::v5::Packet;
    use rumqttc::v5::{AsyncClient, Event, MqttOptions};
    use rumqttc::{TlsConfiguration, Transport};

    let (host, port) = parse_broker(&cfg.broker);

    // Treat `mqtts://...` URLs and the standard MQTT-over-TLS port (8883)
    // as TLS-enabled sessions. Non-TLS sessions skip the rustls bridge
    // entirely so a plain `mqtt://` broker keeps its existing behavior.
    let is_tls = cfg.broker.to_ascii_lowercase().starts_with("mqtts://") || port == 8883;

    let mut mqttoptions = MqttOptions::new("aos2_bridge", host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));
    mqttoptions.set_clean_start(true);
    mqttoptions.set_session_expiry_interval(Some(0));
    if is_tls {
        // TLS observability: rumqttc 0.25 accepts a fully-built rustls config
        // through `TlsConfiguration::Rustls`. The verifier inside our config
        // emits `[T3thr:CONNECTION_META]` after delegating trust validation
        // to `WebPkiVerifier`. The MQTT control flow below reacts to the
        // first ConnAck so the daemon parser sees CONNECTION_META and
        // ConnAck in the same logical session.
        let rustls_cfg = Arc::new(tls_verifier::observing_client_config());
        mqttoptions.set_transport(Transport::Tls(TlsConfiguration::Rustls(rustls_cfg)));
    }
    if let (Some(u), Some(p)) = (&cfg.username, &cfg.password) {
        let u = u.trim();
        let p = p.trim();
        if !u.is_empty() && !p.is_empty() {
            mqttoptions.set_credentials(u, p);
        }
    }

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    client
        .subscribe(&cfg.topic, QoS::AtLeastOnce)
        .await
        .map_err(|e| anyhow!("failed to subscribe to MQTT topic: {:?}", e))?;

    let mut state = FilterState::default();

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                let payload = match std::str::from_utf8(&publish.payload) {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        if tx
                            .send(Err((format!("Parse Error: {}", e), String::new(), None)))
                            .is_err()
                        {
                            eprintln!(
                                "[FORS33] FATAL: Writer channel closed. Stopping message_bus connector."
                            );
                            std::process::exit(1);
                        }
                        continue;
                    }
                };

                let tick = match parse_json_ndimensional(&payload, cfg) {
                    Ok(t) => t,
                    Err(e) => {
                        if tx
                            .send(Err((format!("Parse Error: {}", e), payload.clone(), None)))
                            .is_err()
                        {
                            eprintln!(
                                "[FORS33] FATAL: Writer channel closed. Stopping message_bus connector."
                            );
                            std::process::exit(1);
                        }
                        continue;
                    }
                };

                match state.check(&tick, filter_cfg) {
                    Ok(()) => {
                        if tx.send(Ok(tick)).is_err() {
                            eprintln!(
                                "[FORS33] FATAL: Writer channel closed. Stopping message_bus connector."
                            );
                            std::process::exit(1);
                        }
                    }
                    Err(reason) => {
                        if tx
                            .send(Err((reason, payload.clone(), Some(tick.timestamp_ns))))
                            .is_err()
                        {
                            eprintln!(
                                "[FORS33] FATAL: Writer channel closed. Stopping message_bus connector."
                            );
                            std::process::exit(1);
                        }
                    }
                }
            }
            Ok(_) => {}
            Err(e) => return Err(anyhow!("MQTT poll error: {}", e)),
        }
    }
}
