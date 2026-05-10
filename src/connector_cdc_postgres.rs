//! Postgres logical decoding via `pg_logical_slot_peek_changes` (pgoutput). Emits `[L3dgr:cdc_lsn]` on stdout.

use std::sync::mpsc::SyncSender;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use serde_json::Value;
use tokio_postgres::NoTls;
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::tls_verifier;
use crate::{DataPoint, FilterCfg, FilterState, now_unix_ms, parse_datetime_to_ns};

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

fn row_to_datapoint(data: &str, cfg: &CdcPostgresCfg) -> Result<DataPoint> {
    let v: Value = serde_json::from_str(data).context("cdc pgoutput data is not JSON")?;
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

fn emit_lsn_line(lsn: &str) {
    println!(
        "[L3dgr:cdc_lsn] {}",
        serde_json::json!({ "lsn": lsn.trim() })
    );
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
    let pw = std::env::var("FORS33_SECRET_CONNECTOR__CDC_POSTGRES__PASSWORD").unwrap_or_default();
    let conn_str = format!(
        "host={} port={} dbname={} user={} password={}",
        cfg.host, cfg.port, cfg.database, cfg.user, pw
    );

    // TLS observability: when the operator has set FORS33_PG_TLS=1 (or the
    // libpq sslmode keyword inside conn_str opts in elsewhere), connect via
    // tokio-postgres-rustls + our observing verifier so the leaf cert is
    // emitted through the shared `tls_meta` path. Otherwise fall back to
    // NoTls to preserve the existing development workflow.
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

    let mut state = FilterState::default();
    loop {
        let rows = client
            .query(
                "SELECT data::text AS d, lsn::text AS l FROM pg_logical_slot_peek_changes($1::name, NULL, 500, 'proto_version', '1', 'publication_names', $2)",
                &[&cfg.slot_name, &cfg.publication_name],
            )
            .await
            .context("pg_logical_slot_peek_changes failed")?;

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
                if let Ok(tick) = row_to_datapoint(d, cfg) {
                    match state.check(&tick, filter_cfg) {
                        Ok(()) => {
                            let _ = tx.send(Ok(tick));
                        }
                        Err(reason) => {
                            let _ =
                                tx.send(Err((reason, d.clone(), Some(now_unix_ms() * 1_000_000))));
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

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
