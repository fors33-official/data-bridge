//! Unified Message Bus connector for Kafka and MQTT.
//!
//! Single module with provider-specific connection; shared parse path:
//! subscribe → parse JSON payload → map to DataPoint → filter pipeline.
//! Uses N-dimensional field_paths for JSONPath extraction (deep paths like "sensors.0.vitals.heart_rate").

use std::collections::HashMap;
use std::sync::mpsc::SyncSender;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde_json::Value;

use crate::{now_unix_ms, parse_datetime_to_ns, DataPoint, FilterCfg, FilterState};

/// Unified config for Kafka or MQTT. Provider determines which fields are used.
#[derive(Debug, Clone)]
pub struct MessageBusCfg {
    pub provider: String, // "kafka" | "mqtt"
    pub bootstrap_servers: String,
    pub topic: String,
    pub group_id: String,
    pub broker: String,
    /// Ordered JSONPaths mapping to metrics[index]. Required (synthesized from price_path/volume_path if legacy).
    pub field_paths: Vec<String>,
    pub timestamp_path: Option<String>,
    /// Kafka/MQTT client string properties (resolved placeholders).
    pub client_properties: HashMap<String, String>,
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
            Value::Number(n) => {
                let ms = n.as_f64().ok_or_else(|| anyhow!("timestamp at {} must be numeric", ts_path))?;
                (ms as u64) * 1_000_000
            }
            Value::String(s) => {
                parse_datetime_to_ns(s, "%Y-%m-%d %H:%M:%S%.f", None)?
            }
            _ => now_unix_ms() * 1_000_000,
        }
    } else {
        now_unix_ms() * 1_000_000
    };
    Ok(DataPoint { timestamp_ns, metrics })
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
    tx: SyncSender<Result<DataPoint, (String, String)>>,
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
    tx: SyncSender<Result<DataPoint, (String, String)>>,
) -> Result<()> {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::Message;

    let mut cc = ClientConfig::new();
    for (k, v) in &cfg.client_properties {
        cc.set(k, v);
    }
    let consumer: StreamConsumer = cc
        .set("bootstrap.servers", &cfg.bootstrap_servers)
        .set("group.id", &cfg.group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "true")
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
                                .send(Err((format!("Parse Error: {}", e), String::new())))
                                .is_err()
                            {
                                eprintln!("[Fors33] FATAL: Writer channel closed. Stopping message_bus connector.");
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
                            .send(Err((format!("Parse Error: {}", e), payload.clone())))
                            .is_err()
                        {
                            eprintln!("[Fors33] FATAL: Writer channel closed. Stopping message_bus connector.");
                            std::process::exit(1);
                        }
                        continue;
                    }
                };

                match state.check(&tick, filter_cfg) {
                    Ok(()) => {
                        if tx.send(Ok(tick)).is_err() {
                            eprintln!("[Fors33] FATAL: Writer channel closed. Stopping message_bus connector.");
                            std::process::exit(1);
                        }
                    }
                    Err(reason) => {
                        if tx.send(Err((reason, payload.clone()))).is_err() {
                            eprintln!("[Fors33] FATAL: Writer channel closed. Stopping message_bus connector.");
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
    tx: SyncSender<Result<DataPoint, (String, String)>>,
) -> Result<()> {
    use rumqttc::{AsyncClient, Event, MqttOptions, QoS};

    let (host, port) = parse_broker(&cfg.broker);
    let mut mqttoptions = MqttOptions::new("aos2_bridge", host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));
    let mut mqtt_user: Option<String> = None;
    let mut mqtt_pass: Option<String> = None;
    for (k, v) in &cfg.client_properties {
        match k.as_str() {
            "username" => mqtt_user = Some(v.clone()),
            "password" => mqtt_pass = Some(v.clone()),
            _ => {}
        }
    }
    if let (Some(u), Some(p)) = (mqtt_user.as_ref(), mqtt_pass.as_ref()) {
        mqttoptions.set_credentials(u, p);
    }

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    client
        .subscribe(&cfg.topic, QoS::AtLeastOnce)
        .await
        .context("failed to subscribe to MQTT topic")?;

    let mut state = FilterState::default();

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(rumqttc::Packet::Publish(publish))) => {
                let payload = match std::str::from_utf8(&publish.payload) {
                    Ok(s) => s.to_string(),
                    Err(e) => {
                        if tx
                            .send(Err((format!("Parse Error: {}", e), String::new())))
                            .is_err()
                        {
                            eprintln!("[Fors33] FATAL: Writer channel closed. Stopping message_bus connector.");
                            std::process::exit(1);
                        }
                        continue;
                    }
                };

                let tick = match parse_json_ndimensional(&payload, cfg) {
                    Ok(t) => t,
                    Err(e) => {
                        if tx
                            .send(Err((format!("Parse Error: {}", e), payload.clone())))
                            .is_err()
                        {
                            eprintln!("[Fors33] FATAL: Writer channel closed. Stopping message_bus connector.");
                            std::process::exit(1);
                        }
                        continue;
                    }
                };

                match state.check(&tick, filter_cfg) {
                    Ok(()) => {
                        if tx.send(Ok(tick)).is_err() {
                            eprintln!("[Fors33] FATAL: Writer channel closed. Stopping message_bus connector.");
                            std::process::exit(1);
                        }
                    }
                    Err(reason) => {
                        if tx.send(Err((reason, payload.clone()))).is_err() {
                            eprintln!("[Fors33] FATAL: Writer channel closed. Stopping message_bus connector.");
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