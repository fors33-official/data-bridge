//! Universal file connector: CSV, TSV, JSON, JSONL, Parquet.
//! Client files go straight into the bridge — no pre-processing.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::time::Instant;
use walkdir::WalkDir;

use anyhow::{Context, Result, anyhow};
#[cfg(feature = "full_engine")]
use arrow2::array::Array;
#[cfg(feature = "full_engine")]
use arrow2::io::parquet::read;
use csv::{ReaderBuilder, StringRecord, WriterBuilder};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    BridgeConfig, DataPoint, FilterState, NormalizerCfg, batch_limits, ensure_parent, now_unix_ms,
    parse_datetime_to_ns, parse_ts_to_ns,
};

const MAX_JSONL_LINE_BYTES: usize = 5_242_880; // 5 MiB

// Cooperative OS yield cadence for tight per-record loops. spawn_blocking /
// block_in_place at the engine entrypoint already isolates this work from any
// shared tokio runtime; std::thread::yield_now() here only adds OS-level
// fairness so a 2-core Docker Desktop VM does not starve sibling processes
// (UI bridge, daemon supervisor) under sustained batch load.
const YIELD_RECORDS: usize = 4096;

fn write_deadletter_jsonl_at(
    cfg: &BridgeConfig,
    dead: &mut File,
    reason: &str,
    raw_record: &str,
    timestamp_ns: u64,
) -> Result<()> {
    // Deterministic payload for SEC/non-finite evidence.
    if reason == "Non-finite metric detected" {
        let raw_json = serde_json::to_string(raw_record)?;
        write!(
            &mut *dead,
            "{{\"timestamp_ns\":{},\"reason\":\"Non-finite metric detected\",\"raw_record\":{}}}\n",
            timestamp_ns, raw_json
        )?;
        return Ok(());
    }

    let shaped = cfg.output.shape_deadletter_raw_record(raw_record);
    let obj = serde_json::json!({
        "timestamp_ns": timestamp_ns,
        "reason": reason,
        "raw_record": shaped,
    });
    serde_json::to_writer(&mut *dead, &obj)?;
    dead.write_all(b"\n")?;
    Ok(())
}

fn write_deadletter_jsonl(
    cfg: &BridgeConfig,
    dead: &mut File,
    reason: &str,
    raw_record: &str,
) -> Result<()> {
    let now_ns = now_unix_ms() * 1_000_000;
    write_deadletter_jsonl_at(cfg, dead, reason, raw_record, now_ns)
}

#[derive(Debug, Deserialize)]
pub struct FileCfg {
    pub input_path: String,
    #[serde(default = "default_format")]
    pub format: String, // csv | tsv | json | jsonl | parquet
    #[serde(default = "default_true")]
    pub has_headers: bool,
    #[serde(default = "default_mode")]
    pub mode: Option<String>, // "stream" (default) or "batch"
}

fn default_format() -> String {
    "csv".to_string()
}

fn default_true() -> bool {
    true
}

fn default_mode() -> Option<String> {
    Some("batch".to_string())
}

/// Batch mode: recursively walk directory and process all files with zero-copy streaming.
/// Processes files one at a time to respect memory limits.
fn batch_walk_and_copy(
    file_cfg: &FileCfg,
    cfg: &BridgeConfig,
    state_path: Option<&std::path::Path>,
) -> Result<()> {
    let input_path = Path::new(&file_cfg.input_path);
    let accepted_path = Path::new(&cfg.output.accepted_path);
    let dead_path = Path::new(&cfg.output.dead_letter_path);

    ensure_parent(accepted_path)?;
    ensure_parent(dead_path)?;

    // Load state for resume capability
    let mut last_processed_file_path: Option<String> = None;
    if let Some(path) = state_path {
        match crate::utils::load_state(path) {
            Ok(Some(state)) => {
                if state.status == "in_progress" {
                    last_processed_file_path = state.last_processed_file_path;
                    eprintln!(
                        "[Fors33] Resuming from previous run (last file: {:?})",
                        last_processed_file_path
                    );
                }
            }
            Ok(None) => {
                // No state file, start fresh
            }
            Err(e) => {
                eprintln!(
                    "[WARNING] State file corrupted: {}. Starting batch extraction from zero.",
                    e
                );
            }
        }
    }

    eprintln!(
        "[Fors33] Batch mode: walking directory {}",
        input_path.display()
    );
    println!("t3thr_metrics accepted=0 dropped=0 status=BATCH_START");

    let mut processed_count = 0;
    let mut error_count = 0;

    // Walk directory recursively with lexicographical sort
    let mut entries: Vec<_> = WalkDir::new(input_path)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
        .collect();

    // Sort lexicographically for O(1) cursor-based resume
    entries.sort_by(|a, b| a.path().cmp(b.path()));

    for entry in entries {
        if entry.file_type().is_file() {
            let file_path = entry.path();
            let file_path_str = file_path.to_string_lossy().to_string();

            // Skip files that have already been processed (lexicographical comparison)
            if let Some(ref last) = last_processed_file_path {
                if file_path_str <= *last {
                    eprintln!(
                        "[Fors33] Skipping already processed: {}",
                        file_path.display()
                    );
                    continue;
                }
            }

            let format = resolve_format_from_path(file_path);

            eprintln!(
                "[Fors33] Processing file: {} (format: {})",
                file_path.display(),
                format
            );

            let result = match format {
                "csv" | "tsv" => {
                    run_csv_tsv(file_cfg, cfg, format, file_path, accepted_path, dead_path)
                }
                "json" => run_json(file_cfg, cfg, file_path, accepted_path, dead_path),
                "jsonl" => run_jsonl(file_cfg, cfg, file_path, accepted_path, dead_path),
                "parquet" => run_parquet(file_cfg, cfg, file_path, accepted_path, dead_path),
                other => {
                    eprintln!("[Fors33] Skipping unsupported format: {}", other);
                    continue;
                }
            };

            match result {
                Ok(_) => {
                    processed_count += 1;
                    eprintln!("[Fors33] Successfully processed: {}", file_path.display());

                    // Update state after each successful file
                    if let Some(path) = state_path {
                        let state = crate::utils::State {
                            version: 1,
                            connector_type: "file".to_string(),
                            status: "in_progress".to_string(),
                            last_processed_file_path: Some(file_path_str.clone()),
                            cursor: None,
                        };
                        if let Err(e) = crate::utils::save_state(path, &state) {
                            eprintln!("[WARNING] Failed to save state: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error_count += 1;
                    eprintln!("[Fors33] Error processing {}: {}", file_path.display(), e);
                }
            }
        }
    }

    // Set status to completed on success
    if let Some(path) = state_path {
        let state = crate::utils::State {
            version: 1,
            connector_type: "file".to_string(),
            status: "completed".to_string(),
            last_processed_file_path,
            cursor: None,
        };
        if let Err(e) = crate::utils::save_state(path, &state) {
            eprintln!("[WARNING] Failed to save completion state: {}", e);
        }
    }

    eprintln!(
        "[Fors33] Batch mode complete: {} files processed, {} errors",
        processed_count, error_count
    );
    println!(
        "t3thr_metrics accepted={} dropped={} status=BATCH_COMPLETE",
        processed_count, error_count
    );

    if error_count > 0 {
        Err(anyhow!("Batch mode completed with {} errors", error_count))
    } else {
        Ok(())
    }
}

/// Resolve format from file path (used in batch mode for individual files).
fn resolve_format_from_path(path: &Path) -> &str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("parquet") => "parquet",
        Some("json") => "json",
        Some("jsonl") | Some("ndjson") => "jsonl",
        Some("tsv") => "tsv",
        Some("csv") => "csv",
        _ => "csv", // Default to CSV
    }
}

/// Resolve format from config or file extension.
pub(crate) fn resolve_format(cfg: &FileCfg) -> &str {
    if cfg.format != "auto" && !cfg.format.is_empty() {
        return cfg.format.as_str();
    }
    let path = Path::new(&cfg.input_path);
    match path.extension().and_then(|e| e.to_str()) {
        Some("parquet") => "parquet",
        Some("json") => "json",
        Some("jsonl") | Some("ndjson") => "jsonl",
        Some("tsv") => "tsv",
        _ => "csv",
    }
}

/// Run universal file connector.
pub fn run_file_mode(cfg: &BridgeConfig, state_path: Option<&std::path::Path>) -> Result<()> {
    let file_cfg = cfg
        .connector
        .file
        .as_ref()
        .ok_or_else(|| anyhow!("connector.file required for file mode"))?;

    // Check if batch mode is enabled
    if file_cfg.mode.as_deref() == Some("batch") {
        return batch_walk_and_copy(file_cfg, cfg, state_path);
    }

    let format = resolve_format(file_cfg);
    let input_path = Path::new(&file_cfg.input_path);
    let accepted_path = Path::new(&cfg.output.accepted_path);
    let dead_path = Path::new(&cfg.output.dead_letter_path);

    ensure_parent(accepted_path)?;
    ensure_parent(dead_path)?;

    // One-shot lifecycle marker for the daemon supervisor.
    println!("t3thr_metrics accepted=0 dropped=0 status=FILE_START");

    match format {
        "csv" | "tsv" => run_csv_tsv(file_cfg, cfg, format, input_path, accepted_path, dead_path),
        "json" => run_json(file_cfg, cfg, input_path, accepted_path, dead_path),
        "jsonl" => run_jsonl(file_cfg, cfg, input_path, accepted_path, dead_path),
        "parquet" => run_parquet(file_cfg, cfg, input_path, accepted_path, dead_path),
        other => Err(anyhow!("unsupported file format: {}", other)),
    }
}

fn run_csv_tsv(
    file_cfg: &FileCfg,
    cfg: &BridgeConfig,
    format: &str,
    input_path: &Path,
    accepted_path: &Path,
    dead_path: &Path,
) -> Result<()> {
    let delimiter = if format == "tsv" { b'\t' } else { b',' };
    let mut reader = ReaderBuilder::new()
        .has_headers(file_cfg.has_headers)
        .delimiter(delimiter)
        .from_path(input_path)
        .with_context(|| format!("failed opening input {}", input_path.display()))?;

    let headers = reader.headers().context("failed reading headers")?.clone();

    process_tick_stream(
        cfg,
        accepted_path,
        dead_path,
        reader.records().filter_map(|r| r.ok()),
        |record| parse_csv_record(&record, &headers, &cfg.normalizer),
        |record| record.iter().collect::<Vec<_>>().join("|"),
    )
}

/// N-dimensional CSV record parsing using field_map.
fn parse_csv_record(
    record: &StringRecord,
    headers: &StringRecord,
    ncfg: &NormalizerCfg,
) -> Result<DataPoint> {
    let field_count = ncfg.get_field_count();
    let field_map = ncfg.field_map.as_ref().ok_or_else(|| {
        anyhow!("field_map required (use normalize_and_validate for legacy configs)")
    })?;
    if field_count == 0 {
        return Err(anyhow!("field_count required"));
    }

    let timestamp_ns = if let Some(ts_field) = &ncfg.timestamp_field {
        let tidx = headers
            .iter()
            .position(|h| h == ts_field)
            .ok_or_else(|| anyhow!("missing timestamp field: {}", ts_field))?;
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

    let mut metrics = vec![0.0; field_count];
    for (source_field, &index) in field_map.iter() {
        if index >= field_count {
            return Err(anyhow!(
                "field_map index {} exceeds field_count {}",
                index,
                field_count
            ));
        }
        let fidx = headers
            .iter()
            .position(|h| h == source_field)
            .ok_or_else(|| anyhow!("Missing Field: {}", source_field))?;
        let value: f64 = record
            .get(fidx)
            .ok_or_else(|| anyhow!("missing cell for {}", source_field))?
            .parse()
            .with_context(|| format!("invalid numeric value for {}", source_field))?;
        metrics[index] = value;
    }

    Ok(DataPoint {
        timestamp_ns,
        metrics,
        feed: None,
    })
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

/// N-dimensional JSON parsing using field_map (legacy configs are normalized into field_map).
fn parse_json_record(obj: &Value, ncfg: &NormalizerCfg) -> Result<DataPoint> {
    let field_count = ncfg.get_field_count();
    if field_count == 0 {
        return Err(anyhow!("field_count required"));
    }
    let field_map = ncfg.field_map.as_ref().ok_or_else(|| {
        anyhow!("field_map required (use normalize_and_validate for legacy configs)")
    })?;

    let timestamp_ns = if let Some(ts_field) = &ncfg.timestamp_field {
        if let (Some(fmt), Some(Value::String(s))) =
            (&ncfg.timestamp_format, json_get_value(obj, ts_field))
        {
            parse_datetime_to_ns(s, fmt, ncfg.timestamp_date_override.as_deref())?
        } else if let Some(n) = json_get_f64(obj, ts_field) {
            let unit = ncfg.timestamp_unit.as_deref().unwrap_or("ms");
            parse_ts_to_ns(&format!("{n}"), unit, ncfg.timestamp_tick_hz)?
        } else {
            now_unix_ms() * 1_000_000
        }
    } else {
        now_unix_ms() * 1_000_000
    };

    let mut metrics = vec![0.0; field_count];
    for (source_field, &index) in field_map.iter() {
        if index >= field_count {
            return Err(anyhow!(
                "field_map index {} exceeds field_count {}",
                index,
                field_count
            ));
        }
        let value = json_get_f64(obj, source_field)
            .ok_or_else(|| anyhow!("Missing Field: {}", source_field))?;
        if !value.is_finite() {
            return Err(anyhow!("Non-finite value at {}", source_field));
        }
        metrics[index] = value;
    }

    Ok(DataPoint {
        timestamp_ns,
        metrics,
        feed: None,
    })
}

fn run_json(
    _file_cfg: &FileCfg,
    cfg: &BridgeConfig,
    input_path: &Path,
    accepted_path: &Path,
    dead_path: &Path,
) -> Result<()> {
    // Decide parsing strategy with minimal buffering:
    // - Root arrays are streamed.
    // - Wrapped objects are allowed in-memory only if file <= 500 MiB.
    const MAX_WRAPPED_JSON_BYTES: u64 = 524_288_000; // 500 MiB

    let file = File::open(input_path)
        .with_context(|| format!("failed opening {}", input_path.display()))?;
    let len = file
        .metadata()
        .with_context(|| format!("failed reading metadata for {}", input_path.display()))?
        .len();
    let mut reader = BufReader::new(file);

    // Peek first non-whitespace byte to determine root type.
    let first = loop {
        let buf = reader.fill_buf().context("peek json")?;
        if buf.is_empty() {
            return Err(anyhow!("empty JSON file"));
        }
        if let Some(idx) = buf.iter().position(|b| !b.is_ascii_whitespace()) {
            let b = buf[idx];
            // Consume up to that byte so the deserializer starts at the root token.
            reader.consume(idx);
            break b;
        }
        let n = buf.len();
        reader.consume(n);
    };

    if first == b'[' {
        // Stream root array elements.
        let de = serde_json::Deserializer::from_reader(reader);
        let iter = de.into_iter::<Value>();
        return run_json_stream_values(cfg, iter, accepted_path, dead_path);
    }

    if first == b'{' {
        if len > MAX_WRAPPED_JSON_BYTES {
            eprintln!(
                "[Fors33] FATAL: JSON file exceeds MAX_WRAPPED_JSON_BYTES ({}). Use JSONL or Parquet. path={}",
                MAX_WRAPPED_JSON_BYTES,
                input_path.display()
            );
            std::process::exit(1);
        }
        // In-memory parse for wrapped objects.
        let content = std::fs::read_to_string(input_path)
            .with_context(|| format!("failed reading {}", input_path.display()))?;
        let v: Value = serde_json::from_str(&content).context("invalid JSON")?;
        return run_json_in_memory(cfg, v, accepted_path, dead_path);
    }

    Err(anyhow!("JSON root must be array or object"))
}

fn run_json_in_memory(
    cfg: &BridgeConfig,
    v: Value,
    accepted_path: &Path,
    dead_path: &Path,
) -> Result<()> {
    match v {
        Value::Array(arr) => run_json_values(cfg, arr.into_iter(), accepted_path, dead_path),
        Value::Object(map) => {
            if let Some(rows_val) = map.get("rows").or(map.get("data")).or(map.get("records")) {
                if let Some(arr) = rows_val.as_array() {
                    return run_json_values(cfg, arr.clone().into_iter(), accepted_path, dead_path);
                }
            }
            Err(anyhow!(
                "JSON object must have 'rows', 'data', or 'records' array"
            ))
        }
        _ => Err(anyhow!("JSON root must be array or object with rows array")),
    }
}

fn run_json_stream_values<I>(
    cfg: &BridgeConfig,
    iter: I,
    accepted_path: &Path,
    dead_path: &Path,
) -> Result<()>
where
    I: Iterator<Item = std::result::Result<Value, serde_json::Error>>,
{
    // Convert streaming errors into dropped rows (dead-letter) with operator-friendly reason.
    let mut accepted_writer = WriterBuilder::new()
        .from_path(accepted_path)
        .with_context(|| format!("failed opening accepted output"))?;
    let field_count = cfg.normalizer.get_field_count();
    accepted_writer.write_record(cfg.output.get_headers(field_count))?;

    let mut dead_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dead_path)
        .with_context(|| format!("failed opening dead-letter output"))?;

    let mut state = FilterState::default();
    let mut accepted = 0usize;
    let mut dropped = 0usize;

    for (i, item) in iter.enumerate() {
        let obj = match item {
            Ok(v) => v,
            Err(e) => {
                dropped += 1;
                let reason = format!("Parse Error: {}", e);
                write_deadletter_jsonl(cfg, &mut dead_file, &reason, &format!("row_{}", i))?;
                if cfg.filter.drop_on_parse_error {
                    continue;
                }
                continue;
            }
        };
        let parsed = parse_json_record(&obj, &cfg.normalizer);
        let tick = match parsed {
            Ok(t) => t,
            Err(e) => {
                dropped += 1;
                let prefix = if cfg.filter.drop_on_parse_error {
                    "Parse Error"
                } else {
                    "Parse Warning"
                };
                let reason = format!("{}: {}", prefix, e);
                let raw = serde_json::to_string(&obj).unwrap_or_else(|_| format!("row_{}", i));
                write_deadletter_jsonl(cfg, &mut dead_file, &reason, &raw)?;
                if cfg.filter.drop_on_parse_error {
                    continue;
                }
                continue;
            }
        };

        match state.check(&tick, &cfg.filter) {
            Ok(()) => {
                accepted += 1;
                let mut row_data = vec![tick.timestamp_ns.to_string()];
                for metric in &tick.metrics {
                    row_data.push(metric.to_string());
                }
                accepted_writer.write_record(&row_data)?;
            }
            Err(reason) => {
                dropped += 1;
                let raw = serde_json::to_string(&obj).unwrap_or_else(|_| format!("row_{}", i));
                write_deadletter_jsonl_at(cfg, &mut dead_file, &reason, &raw, tick.timestamp_ns)?;
            }
        }
    }

    accepted_writer.flush()?;
    dead_file.flush()?;
    println!(
        "t3thr file (json) done | accepted={} dropped={}",
        accepted, dropped
    );

    println!(
        "t3thr_metrics accepted={} dropped={} status=FILE_EOF",
        accepted, dropped
    );
    Ok(())
}

fn run_json_values<I>(
    cfg: &BridgeConfig,
    rows: I,
    accepted_path: &Path,
    dead_path: &Path,
) -> Result<()>
where
    I: IntoIterator<Item = Value>,
{
    let mut accepted_writer = WriterBuilder::new()
        .from_path(accepted_path)
        .with_context(|| format!("failed opening accepted output"))?;
    let field_count = cfg.normalizer.get_field_count();
    accepted_writer.write_record(cfg.output.get_headers(field_count))?;

    let mut dead_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dead_path)
        .with_context(|| format!("failed opening dead-letter output"))?;

    let mut state = FilterState::default();
    let mut accepted = 0usize;
    let mut dropped = 0usize;

    for (i, obj) in rows.into_iter().enumerate() {
        let parsed = parse_json_record(&obj, &cfg.normalizer);
        let tick = match parsed {
            Ok(t) => t,
            Err(e) => {
                dropped += 1;
                let prefix = if cfg.filter.drop_on_parse_error {
                    "Parse Error"
                } else {
                    "Parse Warning"
                };
                let reason = format!("{}: {}", prefix, e);
                let raw = serde_json::to_string(&obj).unwrap_or_else(|_| format!("row_{}", i));
                write_deadletter_jsonl(cfg, &mut dead_file, &reason, &raw)?;
                if cfg.filter.drop_on_parse_error {
                    continue;
                }
                continue;
            }
        };

        match state.check(&tick, &cfg.filter) {
            Ok(()) => {
                accepted += 1;
                let mut row_data = vec![tick.timestamp_ns.to_string()];
                for metric in &tick.metrics {
                    row_data.push(metric.to_string());
                }
                accepted_writer.write_record(&row_data)?;
            }
            Err(reason) => {
                dropped += 1;
                let raw = serde_json::to_string(&obj).unwrap_or_else(|_| format!("row_{}", i));
                write_deadletter_jsonl_at(cfg, &mut dead_file, &reason, &raw, tick.timestamp_ns)?;
            }
        }
    }

    accepted_writer.flush()?;
    dead_file.flush()?;
    println!(
        "t3thr file (json) done | accepted={} dropped={}",
        accepted, dropped
    );

    println!(
        "t3thr_metrics accepted={} dropped={} status=FILE_EOF",
        accepted, dropped
    );
    Ok(())
}

fn run_jsonl(
    _file_cfg: &FileCfg,
    cfg: &BridgeConfig,
    input_path: &Path,
    accepted_path: &Path,
    dead_path: &Path,
) -> Result<()> {
    let f = File::open(input_path)
        .with_context(|| format!("failed opening {}", input_path.display()))?;
    let mut reader = BufReader::new(f);

    let mut accepted_writer = WriterBuilder::new()
        .from_path(accepted_path)
        .with_context(|| format!("failed opening accepted output"))?;
    let field_count = cfg.normalizer.get_field_count();
    accepted_writer.write_record(cfg.output.get_headers(field_count))?;

    let mut dead_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dead_path)
        .with_context(|| format!("failed opening dead-letter output"))?;

    let mut state = FilterState::default();
    let mut accepted = 0usize;
    let mut dropped = 0usize;

    let mut buf: Vec<u8> = Vec::new();
    let mut i: usize = 0;
    loop {
        if i != 0 && i % YIELD_RECORDS == 0 {
            std::thread::yield_now();
        }
        buf.clear();
        let mut total: usize = 0;
        let mut capped = false;

        // Read until newline or cap.
        loop {
            let mut chunk: Vec<u8> = Vec::new();
            let n = reader
                .read_until(b'\n', &mut chunk)
                .with_context(|| format!("read jsonl line {}", i))?;
            if n == 0 {
                // EOF
                break;
            }
            if total + chunk.len() > MAX_JSONL_LINE_BYTES {
                let remaining = MAX_JSONL_LINE_BYTES.saturating_sub(total);
                buf.extend_from_slice(&chunk[..remaining]);
                capped = true;
                // Discard the rest of this overlong line until we hit a newline to realign.
                if !chunk.contains(&b'\n') {
                    let mut discard: Vec<u8> = Vec::new();
                    loop {
                        discard.clear();
                        let dn = reader.read_until(b'\n', &mut discard)?;
                        if dn == 0 || discard.contains(&b'\n') {
                            break;
                        }
                    }
                }
                break;
            } else {
                total += chunk.len();
                buf.extend_from_slice(&chunk);
                if chunk.contains(&b'\n') {
                    break;
                }
            }
        }

        if buf.is_empty() {
            break; // EOF
        }

        // Remove trailing newline for hashing/parse fidelity (input line may or may not have one at EOF).
        if buf.last() == Some(&b'\n') {
            buf.pop();
        }

        // Skip whitespace-only lines.
        if buf.iter().all(|b| b.is_ascii_whitespace()) {
            i += 1;
            continue;
        }

        if capped {
            dropped += 1;
            let reason = format!(
                "Parse Error: JSONL line exceeded MAX_JSONL_LINE_BYTES ({})",
                MAX_JSONL_LINE_BYTES
            );
            if cfg.output.hash_raw_records {
                use sha2::{Digest, Sha256};
                let mut hasher = Sha256::new();
                hasher.update(&buf);
                let digest: [u8; 32] = hasher.finalize().into();
                let hexed = hex::encode(digest);
                write_deadletter_jsonl(cfg, &mut dead_file, &reason, &hexed)?;
            } else {
                let preview_len = buf.len().min(512);
                let preview = String::from_utf8_lossy(&buf[..preview_len]).to_string();
                let raw = format!("{preview} (truncated)");
                write_deadletter_jsonl(cfg, &mut dead_file, &reason, &raw)?;
            }
            i += 1;
            continue;
        }

        // IMPORTANT: dead-letter hashing must use the exact, unmodified payload bytes.
        // Do not pass a trimmed string to dead-letter.
        let line = String::from_utf8_lossy(&buf).to_string();
        let trimmed = line.trim();
        let obj: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                dropped += 1;
                let reason = format!("Parse Error: {}", e);
                write_deadletter_jsonl(cfg, &mut dead_file, &reason, &line)?;
                if cfg.filter.drop_on_parse_error {
                    i += 1;
                    continue;
                }
                i += 1;
                continue;
            }
        };

        let parsed = parse_json_record(&obj, &cfg.normalizer);
        let tick = match parsed {
            Ok(t) => t,
            Err(e) => {
                dropped += 1;
                let prefix = if cfg.filter.drop_on_parse_error {
                    "Parse Error"
                } else {
                    "Parse Warning"
                };
                let reason = format!("{}: {}", prefix, e);
                write_deadletter_jsonl(cfg, &mut dead_file, &reason, &line)?;
                if cfg.filter.drop_on_parse_error {
                    continue;
                }
                continue;
            }
        };

        match state.check(&tick, &cfg.filter) {
            Ok(()) => {
                accepted += 1;
                let mut row_data = vec![tick.timestamp_ns.to_string()];
                for metric in &tick.metrics {
                    row_data.push(metric.to_string());
                }
                accepted_writer.write_record(&row_data)?;
            }
            Err(reason) => {
                dropped += 1;
                write_deadletter_jsonl_at(cfg, &mut dead_file, &reason, &line, tick.timestamp_ns)?;
            }
        }
        i += 1;
    }

    accepted_writer.flush()?;
    dead_file.flush()?;
    println!(
        "t3thr file (jsonl) done | accepted={} dropped={}",
        accepted, dropped
    );

    println!(
        "t3thr_metrics accepted={} dropped={} status=FILE_EOF",
        accepted, dropped
    );
    Ok(())
}

#[cfg(not(feature = "full_engine"))]
fn run_parquet(
    _file_cfg: &FileCfg,
    _cfg: &BridgeConfig,
    _input_path: &Path,
    _accepted_path: &Path,
    _dead_path: &Path,
) -> Result<()> {
    Err(anyhow!("parquet connector requires full_engine feature"))
}

#[cfg(feature = "full_engine")]
fn run_parquet(
    _file_cfg: &FileCfg,
    cfg: &BridgeConfig,
    input_path: &Path,
    accepted_path: &Path,
    dead_path: &Path,
) -> Result<()> {
    let mut file = File::open(input_path)
        .with_context(|| format!("failed opening parquet {}", input_path.display()))?;

    let metadata = read::read_metadata(&mut file).context("read parquet metadata")?;
    let schema = read::infer_schema(&metadata).context("infer parquet schema")?;

    let row_groups = metadata.row_groups.clone();
    let chunks = read::FileReader::new(
        file,
        row_groups,
        schema.clone(),
        Some(1024 * 1024),
        None,
        None,
    );

    let field_count = cfg.normalizer.get_field_count();
    let field_map = cfg.normalizer.field_map.as_ref();
    let use_explicit_mapping = field_count > 0 && field_map.is_some();

    let mut accepted = 0usize;
    let mut dropped = 0usize;

    if !use_explicit_mapping {
        // Frictionless parquet mode: self-describing schema, emit raw rows as JSONL
        // when no explicit mapping is supplied by the daemon/UI.
        let mut accepted_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(accepted_path)
            .with_context(|| format!("failed opening accepted output"))?;
        for maybe_chunk in chunks {
            let chunk = maybe_chunk.context("read parquet chunk")?;
            let names: Vec<String> = chunk
                .arrays()
                .iter()
                .enumerate()
                .filter_map(|(i, _)| schema.fields.get(i).map(|f| f.name.clone()))
                .collect();
            let len = chunk.arrays()[0].len();
            for row in 0..len {
                if accepted != 0 && accepted % YIELD_RECORDS == 0 {
                    std::thread::yield_now();
                }
                let mut obj = serde_json::Map::new();
                for (col_idx, name) in names.iter().enumerate() {
                    let v = extract_json_value(chunk.arrays()[col_idx].as_ref(), row);
                    obj.insert(name.clone(), v);
                }
                serde_json::to_writer(&mut accepted_file, &Value::Object(obj))?;
                accepted_file.write_all(b"\n")?;
                accepted += 1;
            }
        }
        accepted_file.flush()?;
    } else {
        let field_map = field_map.expect("field_map present when explicit mapping enabled");
        if field_map.len() != field_count {
            return Err(anyhow!(
                "field_map size {} does not match field_count {}",
                field_map.len(),
                field_count
            ));
        }
        let mut accepted_writer = WriterBuilder::new()
            .from_path(accepted_path)
            .with_context(|| format!("failed opening accepted output"))?;
        let headers = cfg.output.get_headers(field_count);
        accepted_writer.write_record(headers.iter().map(String::as_str))?;

        let mut dead_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dead_path)
            .with_context(|| format!("failed opening dead-letter output"))?;

        let mut state = FilterState::with_capacity(field_count);
        let mut row_offset = 0usize;

        let ncfg = &cfg.normalizer;
        let ts_field = ncfg.timestamp_field.as_deref();

        for maybe_chunk in chunks {
            let chunk = maybe_chunk.context("read parquet chunk")?;
            let names: Vec<String> = chunk
                .arrays()
                .iter()
                .enumerate()
                .filter_map(|(i, _)| schema.fields.get(i).map(|f| f.name.clone()))
                .collect();

            let mut metric_indices: Vec<usize> = vec![0; field_count];
            for (source_field, &index) in field_map.iter() {
                if index >= field_count {
                    return Err(anyhow!(
                        "field_map index {} exceeds field_count {}",
                        index,
                        field_count
                    ));
                }
                let col_idx = names
                    .iter()
                    .position(|n| n == source_field)
                    .ok_or_else(|| anyhow!("parquet missing column: {}", source_field))?;
                metric_indices[index] = col_idx;
            }
            let ts_idx = ts_field.and_then(|f| names.iter().position(|n| n == f));
            let ts_arr = ts_idx.map(|i| chunk.arrays()[i].as_ref());

            let len = chunk.arrays()[0].len();
            for row in 0..len {
                let processed = row_offset + row;
                if processed != 0 && processed % YIELD_RECORDS == 0 {
                    std::thread::yield_now();
                }
                let mut metrics = vec![0.0; field_count];
                for (idx, &col_idx) in metric_indices.iter().enumerate() {
                    metrics[idx] = extract_f64(chunk.arrays()[col_idx].as_ref(), row)?;
                }

                let timestamp_ns =
                    if let (Some(ts_arr), Some(fmt)) = (ts_arr, &ncfg.timestamp_format) {
                        let ts_str = extract_str(ts_arr, row).unwrap_or_default();
                        parse_datetime_to_ns(&ts_str, fmt, ncfg.timestamp_date_override.as_deref())?
                    } else if let Some(ts_arr) = ts_arr {
                        if let Ok(n) = extract_f64(ts_arr, row) {
                            let unit = ncfg.timestamp_unit.as_deref().unwrap_or("ms");
                            parse_ts_to_ns(&format!("{}", n), unit, ncfg.timestamp_tick_hz)?
                        } else {
                            now_unix_ms() * 1_000_000
                        }
                    } else {
                        now_unix_ms() * 1_000_000
                    };

                let point = DataPoint {
                    timestamp_ns,
                    metrics,
                    feed: None,
                };

                let raw_record = format!("row_{}", row_offset + row);

                match state.check(&point, &cfg.filter) {
                    Ok(()) => {
                        accepted += 1;
                        let mut row_data = vec![point.timestamp_ns.to_string()];
                        for metric in &point.metrics {
                            row_data.push(metric.to_string());
                        }
                        accepted_writer.write_record(&row_data)?;
                    }
                    Err(reason) => {
                        dropped += 1;
                        write_deadletter_jsonl_at(
                            cfg,
                            &mut dead_file,
                            &reason,
                            &raw_record,
                            point.timestamp_ns,
                        )?;
                    }
                }
            }
            row_offset += len;
        }

        accepted_writer.flush()?;
        dead_file.flush()?;
    }
    println!(
        "t3thr file (parquet) done | accepted={} dropped={} | accepted_path={} dead_letter_path={}",
        accepted,
        dropped,
        accepted_path.display(),
        dead_path.display()
    );

    println!(
        "t3thr_metrics accepted={} dropped={} status=FILE_EOF",
        accepted, dropped
    );
    Ok(())
}

#[cfg(feature = "full_engine")]
fn extract_f64(array: &dyn Array, index: usize) -> Result<f64> {
    use arrow2::array::{Float32Array, Float64Array, PrimitiveArray};
    use arrow2::datatypes::DataType;
    match array.data_type() {
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("expected Float64Array"))?;
            Ok(arr.value(index))
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| anyhow!("expected Float32Array"))?;
            Ok(arr.value(index) as f64)
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .ok_or_else(|| anyhow!("expected Int64Array"))?;
            Ok(arr.value(index) as f64)
        }
        DataType::UInt64 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .ok_or_else(|| anyhow!("expected UInt64Array"))?;
            Ok(arr.value(index) as f64)
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .ok_or_else(|| anyhow!("expected Int32Array"))?;
            Ok(arr.value(index) as f64)
        }
        DataType::UInt32 => {
            let arr = array
                .as_any()
                .downcast_ref::<PrimitiveArray<u32>>()
                .ok_or_else(|| anyhow!("expected UInt32Array"))?;
            Ok(arr.value(index) as f64)
        }
        _ => Err(anyhow!("unsupported array type: {:?}", array.data_type())),
    }
}

#[cfg(feature = "full_engine")]
fn extract_json_value(array: &dyn Array, index: usize) -> Value {
    use arrow2::array::{BooleanArray, PrimitiveArray, Utf8Array};
    use arrow2::datatypes::DataType;
    if array.is_null(index) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<arrow2::array::Float64Array>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<arrow2::array::Float32Array>()
            .map(|a| Value::from(a.value(index) as f64))
            .unwrap_or(Value::Null),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<u64>>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<i32>>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<u32>>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<i16>>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<u16>>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<i8>>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<PrimitiveArray<u8>>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<Utf8Array<i64>>()
            .map(|a| Value::from(a.value(index)))
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

#[cfg(feature = "full_engine")]
fn extract_str(array: &dyn Array, index: usize) -> Option<String> {
    use arrow2::array::Utf8Array;
    use arrow2::datatypes::DataType;
    match array.data_type() {
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<Utf8Array<i32>>()?;
            Some(arr.value(index).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<Utf8Array<i64>>()?;
            Some(arr.value(index).to_string())
        }
        _ => None,
    }
}

fn process_tick_stream<I, F, G>(
    cfg: &BridgeConfig,
    accepted_path: &Path,
    dead_path: &Path,
    records: I,
    parse: F,
    raw_record: G,
) -> Result<()>
where
    I: Iterator<Item = StringRecord>,
    F: Fn(&StringRecord) -> Result<DataPoint>,
    G: Fn(&StringRecord) -> String,
{
    let mut accepted_writer = WriterBuilder::new()
        .from_path(accepted_path)
        .with_context(|| "failed opening accepted output")?;
    accepted_writer.write_record(["timestamp_ns", "price", "volume"])?;

    let mut dead_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(dead_path)
        .with_context(|| "failed opening dead-letter output")?;

    let mut state = FilterState::default();
    let mut accepted = 0usize;
    let mut dropped = 0usize;
    let mut processed = 0usize;
    let batch_started = Instant::now();

    for record in records {
        if cfg.is_batch_connector() {
            if let Some(reason) =
                cfg.execution_limits().check_writer(accepted as u64, batch_started)
            {
                batch_limits::emit_batch_complete(reason);
                break;
            }
        }
        if processed != 0 && processed % YIELD_RECORDS == 0 {
            std::thread::yield_now();
        }
        processed += 1;
        let parsed = parse(&record);
        let tick = match parsed {
            Ok(t) => t,
            Err(e) => {
                dropped += 1;
                let prefix = if cfg.filter.drop_on_parse_error {
                    "Parse Error"
                } else {
                    "Parse Warning"
                };
                let reason = format!("{}: {}", prefix, e);
                let raw = raw_record(&record);
                write_deadletter_jsonl(cfg, &mut dead_file, &reason, &raw)?;
                if cfg.filter.drop_on_parse_error {
                    continue;
                }
                continue;
            }
        };

        match state.check(&tick, &cfg.filter) {
            Ok(()) => {
                accepted += 1;
                let mut row_data = vec![tick.timestamp_ns.to_string()];
                for metric in &tick.metrics {
                    row_data.push(metric.to_string());
                }
                accepted_writer.write_record(&row_data)?;
            }
            Err(reason) => {
                dropped += 1;
                let raw = raw_record(&record);
                write_deadletter_jsonl_at(cfg, &mut dead_file, &reason, &raw, tick.timestamp_ns)?;
            }
        }
    }

    accepted_writer.flush()?;
    dead_file.flush()?;
    println!(
        "t3thr file done | accepted={} dropped={} | accepted_path={} dead_letter_path={}",
        accepted,
        dropped,
        accepted_path.display(),
        dead_path.display()
    );

    println!(
        "t3thr_metrics accepted={} dropped={} status=FILE_EOF",
        accepted, dropped
    );
    Ok(())
}
