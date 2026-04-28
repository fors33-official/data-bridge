//! gRPC connector for streaming time-series data.
//!
//! Connects to a DataStream.StreamDataPoints gRPC service and maps messages
//! to DataPoint for the filter pipeline. Uses the proto in proto/datastream.proto.
//!
//! **N-dimensional semantics**: The gRPC service must populate `msg.metrics` as a `Vec<f64>`.
//! The bridge maps these directly to `metric_0`, `metric_1`, ... via OutputCfg.headers.
//! Fields `price_path` and `volume_path` in config are deprecated and ignored.

use std::collections::HashMap;
use std::sync::mpsc::SyncSender;

use anyhow::{anyhow, Context, Result};
use tonic::transport::Endpoint;

use crate::{now_unix_ms, DataPoint, FilterCfg, FilterState};

pub mod datastream {
    tonic::include_proto!("datastream");
}

use datastream::data_stream_client::DataStreamClient;

#[derive(Debug, Clone)]
pub struct GrpcCfg {
    pub url: String,
    /// gRPC service name (e.g. "market.MarketData"). Reserved for multi-service routing; proto client is fixed.
    #[allow(dead_code)]
    pub service: String,
    /// Deprecated. Ignored. Metrics come from msg.metrics in the proto.
    #[allow(dead_code)]
    pub price_path: String,
    /// Deprecated. Ignored. Metrics come from msg.metrics in the proto.
    #[allow(dead_code)]
    pub volume_path: String,
    /// Resolved metadata (placeholders substituted in main).
    pub metadata: HashMap<String, String>,
}

/// Run gRPC connector; async, call from tokio runtime.
pub async fn run_grpc_connector(
    cfg: &GrpcCfg,
    filter_cfg: &FilterCfg,
    tx: SyncSender<Result<DataPoint, (String, String)>>,
) -> Result<()> {
    let channel = Endpoint::from_shared(cfg.url.clone())
        .context("invalid gRPC URL")?
        .connect()
        .await
        .context("failed to connect to gRPC server")?;

    let mut client = DataStreamClient::new(channel);
    let mut request = tonic::Request::new(datastream::StreamRequest {});
    for (k, v) in &cfg.metadata {
        let key = tonic::metadata::MetadataKey::from_bytes(k.as_bytes())
            .map_err(|_| anyhow!("invalid gRPC metadata key `{k}`"))?;
        let val = tonic::metadata::MetadataValue::try_from(v.as_str())
            .map_err(|e| anyhow!("invalid gRPC metadata value for `{k}`: {e}"))?;
        request.metadata_mut().insert(key, val);
    }
    let mut stream = client
        .stream_data_points(request)
        .await
        .context("failed to start StreamDataPoints")?
        .into_inner();

    // Infer field count from filter bounds config, default to 2 for legacy mode
    let field_count = filter_cfg.bounds.as_ref()
        .and_then(|bounds| {
            bounds.metrics.keys()
                .filter_map(|k| k.strip_prefix("metric_"))
                .filter_map(|s| s.split('.').next())
                .filter_map(|s| s.parse::<usize>().ok())
                .max()
                .map(|m| m + 1)
        })
        .unwrap_or(2);
    
    let mut state = FilterState::with_capacity(field_count);

    while let Some(msg) = stream
        .message()
        .await
        .context("gRPC stream error")?
    {
        let timestamp_ns = if msg.timestamp_ns != 0 {
            msg.timestamp_ns as u64
        } else {
            now_unix_ms() * 1_000_000
        };

        let metrics = msg.metrics;
        if metrics.is_empty() || metrics.iter().any(|m| !m.is_finite()) {
            continue;
        }

        let point = DataPoint { timestamp_ns, metrics };

        match state.check(&point, filter_cfg) {
            Ok(()) => {
                if tx.send(Ok(point)).is_err() {
                    eprintln!("[Fors33] FATAL: Writer channel closed. Stopping grpc connector.");
                    std::process::exit(1);
                }
            }
            Err(reason) => {
                if tx.send(Err((reason, String::new()))).is_err() {
                    eprintln!("[Fors33] FATAL: Writer channel closed. Stopping grpc connector.");
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}
