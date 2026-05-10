//! MySQL GTID tailing (prototype): requires non-empty `@@GLOBAL.GTID_EXECUTED`; emits `[L3dgr:cdc_gtid]` lines.
//! Also emits placeholder `DataPoint` rows so the writer and metrics loop stay aligned with other live connectors.

use std::sync::mpsc::SyncSender;
use std::time::Duration;

use mysql::OptsBuilder;
use mysql::prelude::Queryable;

use crate::{DataPoint, FilterCfg, FilterState, now_unix_ms};

#[derive(Debug, Clone)]
pub struct CdcMysqlCfg {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
}

fn emit_gtid_line(gtid: &str) {
    println!(
        "[L3dgr:cdc_gtid] {}",
        serde_json::json!({ "gtid": gtid.trim() })
    );
}

fn placeholder_metrics(field_count: usize) -> Vec<f64> {
    vec![1.0_f64; field_count.max(1)]
}

pub fn run_cdc_mysql_blocking(
    cfg: &CdcMysqlCfg,
    filter_cfg: &FilterCfg,
    field_count: usize,
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
) {
    let pw = std::env::var("FORS33_SECRET_CONNECTOR__CDC_MYSQL__PASSWORD").unwrap_or_default();

    // TLS observability: when FORS33_MYSQL_TLS=1 the mysql client requests a
    // TLS handshake via its rustls-tls feature. The mysql crate uses its
    // own internal rustls config and does not expose a custom verifier hook,
    // so the per-connection leaf cert is not surfaced to user code. Trust
    // validation happens against the system root store. A future iteration
    // can rewrite this against a custom rustls connector to capture the
    // leaf bytes; for now, the plaintext path remains the default to avoid
    // forcing TLS on local dev fixtures.
    let mut builder = OptsBuilder::default()
        .ip_or_hostname(Some(cfg.host.clone()))
        .tcp_port(cfg.port)
        .db_name(Some(cfg.database.clone()))
        .user(Some(cfg.user.clone()))
        .pass(Some(pw));

    if std::env::var("FORS33_MYSQL_TLS").ok().as_deref() == Some("1") {
        let ssl = mysql::SslOpts::default();
        builder = builder.ssl_opts(Some(ssl));
    }

    let opts: mysql::Opts = builder.into();
    let mut conn = mysql::Conn::new(opts).expect("cdc_mysql connect failed");
    let gtid: Option<String> = conn
        .query_first("SELECT @@GLOBAL.GTID_EXECUTED")
        .expect("read GTID_EXECUTED");
    let g = gtid.unwrap_or_default();
    if g.trim().is_empty() {
        eprintln!(
            "[BRIDGE] cdc_mysql: @@GLOBAL.GTID_EXECUTED is empty; GTID-only CDC requires GTID mode"
        );
        return;
    }
    let mut state = FilterState::default();
    loop {
        let cur: Option<String> = conn
            .query_first("SELECT @@GLOBAL.GTID_EXECUTED")
            .unwrap_or(None);
        if let Some(ref c) = cur {
            if !c.trim().is_empty() {
                emit_gtid_line(c);
                let ts = now_unix_ms() * 1_000_000;
                let tick = DataPoint {
                    timestamp_ns: ts,
                    metrics: placeholder_metrics(field_count),
                };
                match state.check(&tick, filter_cfg) {
                    Ok(()) => {
                        let _ = tx.send(Ok(tick));
                    }
                    Err(reason) => {
                        let _ = tx.send(Err((reason, c.clone(), Some(ts))));
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}
