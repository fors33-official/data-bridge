//! `t3thr validate` subcommand: parse a config file and emit a human-readable
//! validation report. Behavior is preserved verbatim from the legacy
//! `src/bin/validate_config.rs` binary; only the entry-point shape changes.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use serde::Deserialize;

use super::ValidateArgs;

#[derive(Debug, Deserialize)]
struct BridgeConfig {
    #[serde(default)]
    #[allow(dead_code)]
    chronological_anchor: Option<String>,
    connector: ConnectorCfg,
    normalizer: NormalizerCfg,
    filter: FilterCfg,
    output: OutputCfg,
}

#[derive(Debug, Deserialize)]
struct ConnectorCfg {
    csv: Option<CsvCfg>,
    file: Option<FileCfg>,
    websocket: Option<WebSocketCfg>,
    rest: Option<RestCfg>,
    message_bus: Option<MessageBusCfg>,
    grpc: Option<GrpcCfg>,
    syslog: Option<SyslogCfgTbl>,
    udp_raw: Option<UdpRawCfgTbl>,
    cdc: Option<CdcCfgTbl>,
}

#[derive(Debug, Deserialize)]
struct SyslogCfgTbl {
    #[serde(default)]
    format: String,
    #[serde(default)]
    transport: String,
    #[serde(default)]
    listen_address: Option<String>,
    #[serde(default)]
    connect_address: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UdpRawCfgTbl {
    #[serde(default)]
    bind_address: String,
    port: u16,
    #[serde(default = "default_udp_max")]
    max_datagram_bytes: usize,
}

fn default_udp_max() -> usize {
    65_535
}

fn default_pg_port_cfg() -> u16 {
    5432
}

fn default_mysql_port_cfg() -> u16 {
    3306
}

#[derive(Debug, Deserialize)]
struct CdcPostgresNestTbl {
    #[serde(default)]
    host: String,
    #[serde(default = "default_pg_port_cfg")]
    port: u16,
    #[serde(default)]
    database: String,
    #[serde(default)]
    user: String,
    #[serde(default)]
    slot_name: String,
    #[serde(default)]
    publication_name: String,
}

#[derive(Debug, Deserialize)]
struct CdcMysqlNestTbl {
    #[serde(default)]
    host: String,
    #[serde(default = "default_mysql_port_cfg")]
    port: u16,
    #[serde(default)]
    database: String,
    #[serde(default)]
    user: String,
    #[serde(default = "default_cdc_mysql_gtid_mode_cfg")]
    gtid_mode: String,
}

fn default_cdc_mysql_gtid_mode_cfg() -> String {
    "server_executed".to_string()
}

#[derive(Debug, Deserialize)]
struct CdcCfgTbl {
    #[serde(default)]
    engine: String,
    #[serde(default, rename = "postgres_config")]
    postgres_config: Option<CdcPostgresNestTbl>,
    #[serde(default, rename = "mysql_config")]
    mysql_config: Option<CdcMysqlNestTbl>,
}

#[derive(Debug, Deserialize)]
struct CsvCfg {
    input_path: String,
    #[allow(dead_code)]
    has_headers: bool,
}

#[derive(Debug, Deserialize)]
struct FileCfg {
    #[allow(dead_code)]
    input_path: String,
    #[allow(dead_code)]
    format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WebSocketCfg {
    #[allow(dead_code)]
    url: String,
    #[allow(dead_code)]
    provider: String,
}

#[derive(Debug, Deserialize)]
struct RestCfg {
    #[allow(dead_code)]
    url: String,
    #[allow(dead_code)]
    poll_interval_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct KafkaMsgBusNest {
    #[serde(default)]
    bootstrap_servers: String,
    #[serde(default)]
    topic: String,
    #[serde(default)]
    group_id: String,
}

#[derive(Debug, Deserialize, Default)]
struct MqttMsgBusNest {
    #[serde(default)]
    broker: String,
    #[serde(default)]
    topic: String,
    #[serde(default)]
    timestamp_path: String,
}

#[derive(Debug, Deserialize)]
struct MessageBusCfg {
    provider: String,
    #[serde(default)]
    kafka: Option<KafkaMsgBusNest>,
    #[serde(default)]
    mqtt: Option<MqttMsgBusNest>,
    #[serde(default)]
    bootstrap_servers: String,
    #[serde(default)]
    topic: String,
    #[serde(default)]
    group_id: String,
    #[serde(default)]
    broker: String,
    #[serde(default)]
    timestamp_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GrpcCfg {
    #[allow(dead_code)]
    url: String,
}

#[derive(Debug, Deserialize)]
struct NormalizerCfg {
    price_field: Option<String>,
    volume_field: Option<String>,
    field_count: Option<usize>,
    field_map: Option<HashMap<String, usize>>,
    timestamp_field: Option<String>,
    #[allow(dead_code)]
    timestamp_format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FilterCfg {
    #[allow(dead_code)]
    reject_nan_inf: bool,
    replay_mode: Option<bool>,
    bounds: Option<HashMap<String, MetricBound>>,
    spike_detection: Option<HashMap<String, f64>>,
}

#[derive(Debug, Deserialize)]
struct MetricBound {
    min: Option<f64>,
    max: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct OutputCfg {
    #[serde(default)]
    accepted_path: String,
    #[allow(dead_code)]
    dead_letter_path: String,
    #[allow(dead_code)]
    headers: Option<Vec<String>>,
    #[serde(default)]
    output_dir: Option<String>,
    #[serde(default)]
    file_prefix: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    rotation: Option<String>,
}

pub fn execute(args: &ValidateArgs) -> Result<()> {
    println!("Validating config: {}", args.config.display());
    println!();

    let mut errors: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();
    let mut suggestions: Vec<String> = Vec::new();

    let config_text =
        fs::read_to_string(&args.config).map_err(|e| anyhow!("Cannot read config file: {}", e))?;
    let config: BridgeConfig =
        toml::from_str(&config_text).map_err(|e| anyhow!("Invalid TOML syntax: {}", e))?;

    let connector_count = [
        config.connector.csv.is_some(),
        config.connector.file.is_some(),
        config.connector.websocket.is_some(),
        config.connector.rest.is_some(),
        config.connector.message_bus.is_some(),
        config.connector.grpc.is_some(),
        config.connector.syslog.is_some(),
        config.connector.udp_raw.is_some(),
        config.connector.cdc.is_some(),
    ]
    .iter()
    .filter(|&&x| x)
    .count();

    if connector_count == 0 {
        errors.push("No connector configured. Must specify exactly one of: csv, file, websocket, rest, message_bus, grpc, syslog, udp_raw, cdc.".to_string());
    } else if connector_count > 1 {
        errors.push(
            "Multiple connectors configured. Only one connector type is allowed per config."
                .to_string(),
        );
    }

    if let Some(mb) = &config.connector.message_bus {
        match mb.provider.to_lowercase().as_str() {
            "kafka" => {
                let k = mb.kafka.as_ref();
                let bs = k
                    .map(|x| x.bootstrap_servers.as_str())
                    .unwrap_or(&mb.bootstrap_servers);
                let topic = k.map(|x| x.topic.as_str()).unwrap_or(&mb.topic);
                let gid = k.map(|x| x.group_id.as_str()).unwrap_or(&mb.group_id);
                if bs.trim().is_empty() || topic.trim().is_empty() || gid.trim().is_empty() {
                    errors.push(
                        "message_bus kafka: set [connector.message_bus.kafka] with bootstrap_servers, topic, and group_id (or legacy flat fields)."
                            .to_string(),
                    );
                }
            }
            "mqtt" => {
                let m = mb.mqtt.as_ref();
                let broker = m.map(|x| x.broker.as_str()).unwrap_or(&mb.broker);
                let topic = m.map(|x| x.topic.as_str()).unwrap_or(&mb.topic);
                let tsp = m
                    .map(|x| x.timestamp_path.as_str())
                    .unwrap_or_else(|| mb.timestamp_path.as_deref().unwrap_or(""));
                if broker.trim().is_empty() || topic.trim().is_empty() || tsp.trim().is_empty() {
                    errors.push(
                        "message_bus mqtt: set [connector.message_bus.mqtt] with broker, topic, and timestamp_path (or legacy flat broker/topic plus timestamp_path)."
                            .to_string(),
                    );
                }
            }
            _ => errors.push(format!(
                "message_bus provider must be kafka or mqtt, got {:?}",
                mb.provider
            )),
        }
    }

    if let Some(sg) = &config.connector.syslog {
        let fmt = sg.format.to_ascii_lowercase();
        if fmt != "rfc5424" && fmt != "rfc3164" {
            errors.push("syslog: format must be rfc5424 or rfc3164".to_string());
        }
        let tr = sg.transport.to_ascii_lowercase();
        if tr != "tcp" && tr != "udp" {
            errors.push("syslog: transport must be tcp or udp".to_string());
        }
        let la = sg
            .listen_address
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let ca = sg
            .connect_address
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        match (la, ca) {
            (Some(_), None) | (None, Some(_)) => {}
            _ => errors.push(
                "syslog: set exactly one of listen_address or connect_address (non-empty)"
                    .to_string(),
            ),
        }
    }

    if let Some(u) = &config.connector.udp_raw {
        let b = u.bind_address.trim();
        if !matches!(b, "0.0.0.0" | "::" | "127.0.0.1") {
            errors.push("udp_raw: bind_address must be 0.0.0.0, ::, or 127.0.0.1".to_string());
        }
        if u.port == 0 {
            errors.push("udp_raw: port must be 1-65535".to_string());
        }
        if u.max_datagram_bytes < 512 || u.max_datagram_bytes > 1_048_576 {
            errors.push("udp_raw: max_datagram_bytes must be 512-1048576".to_string());
        }
    }

    if let Some(cdc) = &config.connector.cdc {
        let eng = cdc.engine.to_ascii_lowercase();
        if eng == "postgres" {
            if let Some(pg) = &cdc.postgres_config {
                if pg.host.trim().is_empty()
                    || pg.database.trim().is_empty()
                    || pg.user.trim().is_empty()
                    || pg.slot_name.trim().is_empty()
                    || pg.publication_name.trim().is_empty()
                {
                    errors.push(
                        "cdc postgres_config: host, database, user, slot_name, and publication_name must be non-empty"
                            .to_string(),
                    );
                }
                if pg.port == 0 {
                    errors.push("cdc postgres_config: port must be 1-65535".to_string());
                }
            } else {
                errors.push("cdc: engine postgres requires postgres_config table".to_string());
            }
            if cdc.mysql_config.is_some() {
                errors
                    .push("cdc: mysql_config must not be set when engine is postgres".to_string());
            }
        } else if eng == "mysql" {
            if let Some(my) = &cdc.mysql_config {
                if my.host.trim().is_empty()
                    || my.database.trim().is_empty()
                    || my.user.trim().is_empty()
                {
                    errors.push(
                        "cdc mysql_config: host, database, and user must be non-empty".to_string(),
                    );
                }
                if my.port == 0 {
                    errors.push("cdc mysql_config: port must be 1-65535".to_string());
                }
                if !my.gtid_mode.eq_ignore_ascii_case("server_executed") {
                    errors.push("cdc mysql_config: gtid_mode must be server_executed".to_string());
                }
            } else {
                errors.push("cdc: engine mysql requires mysql_config table".to_string());
            }
            if cdc.postgres_config.is_some() {
                errors
                    .push("cdc: postgres_config must not be set when engine is mysql".to_string());
            }
        } else {
            errors.push("cdc: engine must be postgres or mysql".to_string());
        }
    }

    if let Some(csv) = &config.connector.csv {
        if !PathBuf::from(&csv.input_path).exists() {
            warnings.push(format!("CSV input file does not exist: {}", csv.input_path));
            suggestions.push(
                "Make sure the input_path is correct and the file exists before running."
                    .to_string(),
            );
        }
    }

    let is_legacy =
        config.normalizer.price_field.is_some() && config.normalizer.volume_field.is_some();
    let is_nfield = config.normalizer.field_count.is_some();

    if !is_legacy && !is_nfield {
        errors.push("Normalizer must specify either (field_count + field_map) for N-field mode OR (price_field + volume_field) for legacy 2-field mode.".to_string());
    }

    if is_legacy && is_nfield {
        warnings.push("Both legacy (price_field/volume_field) and N-field (field_count/field_map) are specified. Legacy mode will be used.".to_string());
        suggestions.push("Consider migrating to N-field mode for better flexibility.".to_string());
    }

    if is_nfield {
        if let Some(field_count) = config.normalizer.field_count {
            if field_count == 0 {
                errors.push("field_count must be at least 1".to_string());
            }
            if let Some(ref field_map) = config.normalizer.field_map {
                if field_map.is_empty() {
                    warnings.push("field_map is empty. No fields will be extracted.".to_string());
                }
                for (field_name, &index) in field_map {
                    if index >= field_count {
                        errors.push(format!(
                            "field_map['{}'] = {} exceeds field_count = {}",
                            field_name, index, field_count
                        ));
                    }
                }
            } else {
                errors.push("N-field mode requires field_map to be specified".to_string());
            }
        }
    }

    if let Some(ref bounds) = config.filter.bounds {
        for (key, bound) in bounds {
            if let (Some(min), Some(max)) = (bound.min, bound.max) {
                if min > max {
                    errors.push(format!(
                        "Invalid bounds for {}: min ({}) > max ({})",
                        key, min, max
                    ));
                }
            }
        }
    }

    if let Some(ref spike) = config.filter.spike_detection {
        if let Some(&alpha) = spike.get("ema_alpha") {
            if alpha <= 0.0 || alpha > 1.0 {
                errors.push(format!(
                    "ema_alpha must be between 0.0 and 1.0, got {}",
                    alpha
                ));
            }
        }
        for (key, &value) in spike {
            if key.ends_with("_max_delta") && value <= 0.0 {
                warnings.push(format!("{} should be positive, got {}", key, value));
            }
        }
    }

    let od = config
        .output
        .output_dir
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let fp = config
        .output
        .file_prefix
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let out_parent = if let (Some(dir), Some(_)) = (od, fp) {
        PathBuf::from(dir)
    } else if !config.output.accepted_path.trim().is_empty() {
        PathBuf::from(&config.output.accepted_path)
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_default()
    } else {
        PathBuf::new()
    };
    if !out_parent.as_os_str().is_empty() && out_parent != PathBuf::from("") && !out_parent.exists()
    {
        warnings.push(format!(
            "Output directory does not exist: {}",
            out_parent.display()
        ));
        suggestions.push(format!(
            "Create output directory: mkdir -p {}",
            out_parent.display()
        ));
    }

    if config.filter.replay_mode.is_none() {
        suggestions.push(
            "Consider setting replay_mode = true for historical data or false for live data."
                .to_string(),
        );
    }
    if config.normalizer.timestamp_field.is_none() {
        warnings.push(
            "No timestamp_field specified. Will use current system time for each record."
                .to_string(),
        );
    }

    println!("=== Validation Results ===\n");
    if !errors.is_empty() {
        println!("ERRORS ({}):", errors.len());
        for (i, err) in errors.iter().enumerate() {
            println!("  {}. {}", i + 1, err);
        }
        println!();
    }
    if !warnings.is_empty() {
        println!("WARNINGS ({}):", warnings.len());
        for (i, w) in warnings.iter().enumerate() {
            println!("  {}. {}", i + 1, w);
        }
        println!();
    }
    if !suggestions.is_empty() {
        println!("SUGGESTIONS ({}):", suggestions.len());
        for (i, s) in suggestions.iter().enumerate() {
            println!("  {}. {}", i + 1, s);
        }
        println!();
    }
    if errors.is_empty() && warnings.is_empty() && suggestions.is_empty() {
        println!("Configuration is valid. No issues found.\n");
    } else if errors.is_empty() {
        println!(
            "Configuration is valid (with {} warnings and {} suggestions).\n",
            warnings.len(),
            suggestions.len()
        );
    }

    if !errors.is_empty() {
        return Err(anyhow!("validation failed with {} errors", errors.len()));
    }
    Ok(())
}
