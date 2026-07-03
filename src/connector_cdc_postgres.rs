//! Postgres logical decoding via `pg_logical_slot_get_changes` (wal2json JSON) or
//! `pg_logical_slot_get_binary_changes` (pgoutput). Emits `[L3dgr:cdc_lsn]` on stdout.

use std::sync::mpsc::SyncSender;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::tls_verifier;
use crate::{DataPoint, FilterCfg, FilterState, now_unix_ms, parse_datetime_to_ns};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotPlugin {
    Wal2Json,
    PgOutput,
    Other,
}

impl SlotPlugin {
    fn from_name(name: &str) -> Self {
        match name.trim().to_ascii_lowercase().as_str() {
            "wal2json" => Self::Wal2Json,
            "pgoutput" => Self::PgOutput,
            _ => Self::Other,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CdcPostgresCfg {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub slot_name: String,
    pub publication_name: String,
    pub field_paths: Vec<String>,
    pub timestamp_path: Option<String>,
    pub resume_lsn: Option<String>,
}

fn slot_ident_ok(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
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

fn wal2json_column_f64(v: &Value, column_name: &str) -> Option<f64> {
    let cols = v.get("columns")?.as_array()?;
    for col in cols {
        let name = col.get("name")?.as_str()?;
        if name == column_name {
            return json_get_f64(col, "value");
        }
    }
    None
}

fn wal2json_column_value<'a>(v: &'a Value, column_name: &str) -> Option<&'a Value> {
    let cols = v.get("columns")?.as_array()?;
    for col in cols {
        let name = col.get("name")?.as_str()?;
        if name == column_name {
            return col.get("value");
        }
    }
    None
}

fn is_wal2json_change_row(v: &Value) -> bool {
    v.get("columns").and_then(|c| c.as_array()).is_some()
        && v.get("action")
            .and_then(|a| a.as_str())
            .is_some_and(|a| matches!(a, "I" | "U" | "D"))
}

fn row_to_datapoint(data: &str, cfg: &CdcPostgresCfg) -> Result<DataPoint> {
    let v: Value = serde_json::from_str(data).context("cdc change data is not JSON")?;
    if !is_wal2json_change_row(&v) {
        return Err(anyhow!("cdc row is not a wal2json change event"));
    }
    let mut metrics = Vec::with_capacity(cfg.field_paths.len());
    for path in &cfg.field_paths {
        let value = wal2json_column_f64(&v, path)
            .or_else(|| json_get_f64(&v, path))
            .ok_or_else(|| anyhow!("Missing Field: {}", path))?;
        if !value.is_finite() {
            return Err(anyhow!("Non-finite value at path {}", path));
        }
        metrics.push(value);
    }
    let timestamp_ns = if let Some(ref ts_path) = cfg.timestamp_path {
        let ts_val = wal2json_column_value(&v, ts_path)
            .or_else(|| json_get_value(&v, ts_path))
            .ok_or_else(|| anyhow!("Missing Field: {}", ts_path))?;
        match ts_val {
            Value::Number(n) => {
                let ms = n
                    .as_f64()
                    .ok_or_else(|| anyhow!("timestamp at {} must be numeric", ts_path))?;
                (ms as u64) * 1_000_000
            }
            Value::String(s) => parse_datetime_to_ns(&s, "%Y-%m-%d %H:%M:%S%.f", None)?,
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

fn emit_lsn_line(lsn: &str) {
    println!(
        "[L3dgr:cdc_lsn] {}",
        serde_json::json!({ "lsn": lsn.trim() })
    );
}

async fn resolve_slot_plugin(client: &tokio_postgres::Client, slot_name: &str) -> Result<SlotPlugin> {
    let row = client
        .query_opt(
            "SELECT plugin FROM pg_replication_slots WHERE slot_name = $1::name",
            &[&slot_name],
        )
        .await
        .context("read replication slot plugin")?;
    let plugin = row
        .and_then(|r| r.get::<_, Option<String>>(0))
        .unwrap_or_default();
    let kind = SlotPlugin::from_name(&plugin);
    if kind == SlotPlugin::Other && !plugin.trim().is_empty() {
        return Err(anyhow!(
            "cdc_postgres slot plugin {:?} is not supported (use wal2json or pgoutput)",
            plugin
        ));
    }
    if plugin.trim().is_empty() {
        return Err(anyhow!(
            "cdc_postgres replication slot {:?} was not found",
            slot_name
        ));
    }
    Ok(kind)
}

async fn fetch_slot_changes(
    client: &tokio_postgres::Client,
    cfg: &CdcPostgresCfg,
    plugin: SlotPlugin,
) -> Result<Vec<tokio_postgres::Row>> {
    match plugin {
        SlotPlugin::Wal2Json => client
            .query(
                "SELECT data::text AS d, lsn::text AS l FROM pg_logical_slot_get_changes($1::name, NULL, 500, 'format-version', '2', 'include-lsn', 'true')",
                &[&cfg.slot_name],
            )
            .await
            .context("pg_logical_slot_get_changes (wal2json) failed"),
        SlotPlugin::PgOutput => Err(anyhow!(
            "pgoutput slots require pg_logical_slot_get_binary_changes decoding; use a wal2json slot for JSON ingest or wait for pgoutput binary support"
        )),
        SlotPlugin::Other => Err(anyhow!("unsupported replication slot plugin")),
    }
}

pub async fn run_cdc_postgres_connector(
    cfg: &CdcPostgresCfg,
    filter_cfg: &FilterCfg,
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
) -> Result<()> {
    if !slot_ident_ok(&cfg.slot_name) || !slot_ident_ok(&cfg.publication_name) {
        return Err(anyhow!(
            "cdc_postgres slot_name and publication_name must be non-empty alphanumeric or underscore"
        ));
    }
    if let Some(resume_lsn) = cfg
        .resume_lsn
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        eprintln!(
            "[BRIDGE] cdc_postgres resume_lsn={} supplied; logical slot position remains authoritative.",
            resume_lsn
        );
    }
    let pw = std::env::var("FORS33_SECRET_CONNECTOR__CDC_POSTGRES__PASSWORD").unwrap_or_default();
    let conn_str = format!(
        "host={} port={} dbname={} user={} password={}",
        cfg.host, cfg.port, cfg.database, cfg.user, pw
    );

    let tls_enabled = std::env::var("FORS33_PG_TLS").ok().as_deref() == Some("1");
    let client = if tls_enabled {
        let rustls_cfg = tls_verifier::observing_client_config();
        let connector = MakeRustlsConnect::new(rustls_cfg);
        let (client, connection) = tokio_postgres::connect(&conn_str, connector)
            .await
            .context("cdc_postgres TLS connect failed")?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("[BRIDGE] cdc_postgres connection task error: {}", e);
            }
        });
        client
    } else {
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .context("cdc_postgres connect failed")?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("[BRIDGE] cdc_postgres connection task error: {}", e);
            }
        });
        client
    };

    let slot_plugin = resolve_slot_plugin(&client, &cfg.slot_name).await?;
    eprintln!(
        "[BRIDGE] cdc_postgres slot={} plugin={:?}",
        cfg.slot_name, slot_plugin
    );

    let mut state = FilterState::default();
    loop {
        let rows = fetch_slot_changes(&client, cfg, slot_plugin).await?;

        let mut max_lsn: Option<String> = None;
        for row in rows {
            let data: Option<String> = row.get(0);
            let lsn_s: Option<String> = row.get(1);
            if let Some(ref ls) = lsn_s {
                if !ls.trim().is_empty() {
                    max_lsn = Some(ls.clone());
                }
            }
            if let Some(ref d) = data {
                if d.trim().is_empty() {
                    continue;
                }
                match row_to_datapoint(d, cfg) {
                    Ok(tick) => match state.check(&tick, filter_cfg) {
                        Ok(()) => {
                            if tx.send(Ok(tick)).is_err() {
                                eprintln!(
                                    "[FORS33] FATAL: Writer channel closed. Stopping cdc_postgres connector."
                                );
                                std::process::exit(1);
                            }
                        }
                        Err(reason) => {
                            if tx
                                .send(Err((reason, d.clone(), Some(tick.timestamp_ns))))
                                .is_err()
                            {
                                eprintln!(
                                    "[FORS33] FATAL: Writer channel closed. Stopping cdc_postgres connector."
                                );
                                std::process::exit(1);
                            }
                        }
                    },
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("not a wal2json change event") {
                            continue;
                        }
                        if tx
                            .send(Err((format!("Parse Error: {}", e), d.clone(), None)))
                            .is_err()
                        {
                            eprintln!(
                                "[FORS33] FATAL: Writer channel closed. Stopping cdc_postgres connector."
                            );
                            std::process::exit(1);
                        }
                    }
                }
            }
        }

        let slot_lsn = client
            .query_opt(
                "SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1::name",
                &[&cfg.slot_name],
            )
            .await
            .context("read restart_lsn")?;
        let lsn_out = max_lsn
            .or_else(|| slot_lsn.and_then(|r| r.get::<_, Option<String>>(0)))
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "0/0".to_string());
        emit_lsn_line(&lsn_out);
        if let Ok(Some(row)) = client
            .query_opt(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::text FROM pg_replication_slots WHERE slot_name = $1::name",
                &[&cfg.slot_name],
            )
            .await
        {
            let lag_bytes: Option<String> = row.get(0);
            if let Some(lag) = lag_bytes {
                eprintln!("[BRIDGE] cdc_postgres slot_lag_bytes={}", lag.trim());
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_cfg() -> CdcPostgresCfg {
        CdcPostgresCfg {
            host: "localhost".to_string(),
            port: 5432,
            database: "lab".to_string(),
            user: "cdc_user".to_string(),
            slot_name: "cdc_smoke_slot".to_string(),
            publication_name: "cdc_smoke_pub".to_string(),
            field_paths: vec!["metric_0".to_string()],
            timestamp_path: None,
            resume_lsn: None,
        }
    }

    #[test]
    fn row_to_datapoint_wal2json_v2_columns() {
        let raw = r#"{
            "action": "I",
            "schema": "public",
            "table": "cdc_smoke_ticks",
            "columns": [
                {"name": "id", "type": "integer", "value": 1},
                {"name": "metric_0", "type": "double precision", "value": 1.0}
            ]
        }"#;
        let cfg = sample_cfg();
        let dp = row_to_datapoint(raw, &cfg).expect("wal2json row");
        assert_eq!(dp.metrics, vec![1.0]);
    }

    #[test]
    fn row_to_datapoint_skips_begin_commit_via_error() {
        let raw = r#"{"action":"B"}"#;
        let cfg = sample_cfg();
        let err = row_to_datapoint(raw, &cfg).unwrap_err();
        assert!(err.to_string().contains("not a wal2json change event"));
    }

    #[test]
    fn slot_plugin_from_name() {
        assert_eq!(SlotPlugin::from_name("wal2json"), SlotPlugin::Wal2Json);
        assert_eq!(SlotPlugin::from_name("pgoutput"), SlotPlugin::PgOutput);
        assert_eq!(SlotPlugin::from_name("unknown"), SlotPlugin::Other);
    }
}
