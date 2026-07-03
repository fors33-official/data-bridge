//! Plaintext syslog ingest: RFC 5424 then RFC 3164; TCP or UDP; listen or dial.
//! DataPoint time uses parsed header timestamp when present; otherwise wall clock.
//! Metrics are placeholder values (1.0 per field index) so N-field bounds stay contract-aligned.

use std::sync::mpsc::SyncSender;

use anyhow::{Context, Result, anyhow};
use chrono::{Datelike, NaiveDate, NaiveTime, TimeZone, Utc};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use crate::{DataPoint, FilterCfg, FilterState, now_unix_ms};

#[derive(Debug, Clone)]
pub struct SyslogCfg {
    pub format: String,
    pub transport: String,
    pub listen_address: Option<String>,
    pub connect_address: Option<String>,
}

fn placeholder_metrics(field_count: usize) -> Vec<f64> {
    vec![1.0_f64; field_count.max(1)]
}

fn rfc3339_or_similar_to_ns(s: &str) -> Option<u64> {
    let t = s.trim();
    if t.is_empty() || t == "-" {
        return None;
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(t) {
        return Some(dt.timestamp_nanos_opt().unwrap_or(0) as u64);
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%dT%H:%M:%S%.fZ") {
        let dt = Utc.from_utc_datetime(&ndt);
        return Some(dt.timestamp_nanos_opt().unwrap_or(0) as u64);
    }
    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(t, "%Y-%m-%d %H:%M:%S%.f") {
        let dt = Utc.from_utc_datetime(&ndt);
        return Some(dt.timestamp_nanos_opt().unwrap_or(0) as u64);
    }
    None
}

/// RFC 5424: HEADER fields up to structured-data; timestamp is third token after PRI+VERSION.
fn parse_rfc5424_header_time_ns(line: &str) -> Option<u64> {
    let rest = line.strip_prefix('<')?;
    let gt = rest.find('>')?;
    let after = rest[gt + 1..].trim_start();
    let mut it = after.split_whitespace();
    let _ver = it.next()?;
    let ts = it.next()?;
    rfc3339_or_similar_to_ns(ts)
}

/// RFC 3164: `<pri>MMM DD hh:mm:ss HOST TAG: msg`
fn parse_rfc3164_header_time_ns(line: &str, year: i32) -> Option<u64> {
    let rest = line.strip_prefix('<')?;
    let gt = rest.find('>')?;
    let after = rest[gt + 1..].trim_start();
    let mut it = after.split_whitespace();
    let mon = it.next()?;
    let day: u32 = it.next()?.parse().ok()?;
    let hms = it.next()?;
    let naive_time = NaiveTime::parse_from_str(hms, "%H:%M:%S").ok()?;
    let mon_num = match mon {
        "Jan" => 1u32,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    };
    let nd = NaiveDate::from_ymd_opt(year, mon_num, day)?;
    let ndt = nd.and_time(naive_time);
    let dt = Utc.from_utc_datetime(&ndt);
    Some(dt.timestamp_nanos_opt().unwrap_or(0) as u64)
}

fn line_timestamp_ns(line: &str, fmt: &str) -> u64 {
    let fmt_lc = fmt.to_ascii_lowercase();
    if fmt_lc == "rfc5424" {
        if let Some(ns) = parse_rfc5424_header_time_ns(line) {
            return ns;
        }
    } else if fmt_lc == "rfc3164" {
        let y = Utc::now().year();
        if let Some(ns) = parse_rfc3164_header_time_ns(line, y) {
            return ns;
        }
    }
    now_unix_ms() * 1_000_000
}

fn handle_one_syslog_line(
    line: &str,
    cfg: &SyslogCfg,
    filter_cfg: &FilterCfg,
    field_count: usize,
    state: &mut FilterState,
    tx: &SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
) -> Result<()> {
    let ts_ns = line_timestamp_ns(line, &cfg.format);
    let point = DataPoint {
        timestamp_ns: ts_ns,
        metrics: placeholder_metrics(field_count),
        feed: None,
    };
    match state.check(&point, filter_cfg) {
        Ok(()) => {
            if tx.send(Ok(point)).is_err() {
                eprintln!("[FORS33] FATAL: Writer channel closed. Stopping syslog connector.");
                std::process::exit(1);
            }
        }
        Err(reason) => {
            if tx
                .send(Err((reason, line.to_string(), Some(ts_ns))))
                .is_err()
            {
                eprintln!("[FORS33] FATAL: Writer channel closed. Stopping syslog connector.");
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

async fn syslog_tcp_lines<R: tokio::io::AsyncRead + Unpin>(
    reader: R,
    cfg: &SyslogCfg,
    filter_cfg: &FilterCfg,
    field_count: usize,
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
) -> Result<()> {
    let mut br = BufReader::new(reader);
    let mut line = String::new();
    let mut state = FilterState::default();
    loop {
        line.clear();
        let n = br.read_line(&mut line).await?;
        if n == 0 {
            break;
        }
        let s = line.trim_end_matches(['\r', '\n']);
        if s.is_empty() {
            continue;
        }
        handle_one_syslog_line(s, cfg, filter_cfg, field_count, &mut state, &tx)?;
    }
    Ok(())
}

pub async fn run_syslog_connector(
    cfg: &SyslogCfg,
    filter_cfg: &FilterCfg,
    field_count: usize,
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
) -> Result<()> {
    let transport = cfg.transport.to_ascii_lowercase();
    let listen = cfg
        .listen_address
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    let connect = cfg
        .connect_address
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());

    match (listen, connect) {
        (Some(bind_addr), None) if transport == "tcp" => {
            let listener = TcpListener::bind(bind_addr)
                .await
                .with_context(|| format!("syslog tcp bind {}", bind_addr))?;
            loop {
                let (sock, _) = listener.accept().await?;
                let cfg = cfg.clone();
                let fc = filter_cfg.clone();
                let txc = tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = syslog_tcp_lines(sock, &cfg, &fc, field_count, txc).await {
                        eprintln!("[BRIDGE] syslog tcp session error: {}", e);
                    }
                });
            }
        }
        (None, Some(addr)) if transport == "tcp" => {
            let stream = TcpStream::connect(addr)
                .await
                .with_context(|| format!("syslog tcp connect {}", addr))?;
            return syslog_tcp_lines(stream, cfg, filter_cfg, field_count, tx).await;
        }
        (Some(bind_addr), None) if transport == "udp" => {
            let sock = UdpSocket::bind(bind_addr)
                .await
                .with_context(|| format!("syslog udp bind {}", bind_addr))?;
            let mut buf = vec![0u8; 65_535];
            let mut state = FilterState::default();
            loop {
                let (len, _) = sock.recv_from(&mut buf).await?;
                let s = std::str::from_utf8(&buf[..len]).unwrap_or("");
                let s = s.trim_end_matches(['\r', '\n']);
                if s.is_empty() {
                    continue;
                }
                handle_one_syslog_line(s, cfg, filter_cfg, field_count, &mut state, &tx)?;
            }
        }
        (None, Some(addr)) if transport == "udp" => {
            let sock = UdpSocket::bind("0.0.0.0:0")
                .await
                .context("syslog udp ephemeral bind")?;
            sock.connect(addr)
                .await
                .with_context(|| format!("syslog udp connect {}", addr))?;
            let mut buf = vec![0u8; 65_535];
            let mut state = FilterState::default();
            loop {
                let len = sock.recv(&mut buf).await?;
                let s = std::str::from_utf8(&buf[..len]).unwrap_or("");
                let s = s.trim_end_matches(['\r', '\n']);
                if s.is_empty() {
                    continue;
                }
                handle_one_syslog_line(s, cfg, filter_cfg, field_count, &mut state, &tx)?;
            }
        }
        _ => {
            return Err(anyhow!(
                "syslog: set exactly one of listen_address or connect_address; transport must be tcp or udp"
            ));
        }
    }
    #[allow(unreachable_code)]
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_rfc3164_header_time_ns, parse_rfc5424_header_time_ns};

    #[test]
    fn rfc5424_header_timestamp_parses() {
        let line = "<34>1 2024-06-01T12:34:56.789Z host app - - test";
        let ns = parse_rfc5424_header_time_ns(line);
        assert!(ns.is_some());
    }

    #[test]
    fn rfc3164_header_timestamp_parses() {
        let line = "<34>Jan 15 10:00:00 myhost tag: hello";
        let ns = parse_rfc3164_header_time_ns(line, 2026);
        assert!(ns.is_some());
    }
}
