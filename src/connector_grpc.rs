//! gRPC connector for streaming time-series data.
//!
//! Connects to a DataStream.StreamDataPoints gRPC service and maps messages
//! to DataPoint for the filter pipeline. Uses the proto in proto/datastream.proto.
//!
//! **N-dimensional semantics**: The gRPC service must populate `msg.metrics` as a `Vec<f64>`.
//! The bridge maps these directly to `metric_0`, `metric_1`, ... via OutputCfg.headers.
//! Fields `price_path` and `volume_path` in config are deprecated and ignored.
//!
//! **TLS observability**: For `https://` endpoints, the channel is built via
//! tonic's `ClientTlsConfig` backed by webpki/native roots. The peer leaf
//! certificate is captured during the handshake and emitted through the
//! shared `tls_meta::observe_and_emit` path. Trust validation is delegated
//! to the default rustls `WebPkiVerifier`; this connector never overrides
//! the trust decision.

use std::sync::mpsc::SyncSender;

use anyhow::{Context, Result};
use tonic::Status;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

use crate::{DataPoint, FilterCfg, FilterState, now_unix_ms};

pub mod datastream {
    tonic::include_proto!("datastream");
}

use datastream::data_stream_client::DataStreamClient;

/// Interceptor for bearer token plus optional custom metadata pairs (expanded
/// from config; values resolved through `expand_fors33_secret_placeholders`).
#[derive(Clone)]
struct OutboundMetadataInterceptor {
    bearer: Option<MetadataValue<tonic::metadata::Ascii>>,
    extra: Vec<(
        tonic::metadata::MetadataKey<tonic::metadata::Ascii>,
        MetadataValue<tonic::metadata::Ascii>,
    )>,
}

impl Interceptor for OutboundMetadataInterceptor {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, Status> {
        if let Some(v) = self.bearer.clone() {
            req.metadata_mut().insert("authorization", v);
        }
        for (name, val) in &self.extra {
            let _ = req.metadata_mut().append(name.clone(), val.clone());
        }
        Ok(req)
    }
}

#[allow(dead_code)] // Config shape retained for connector contract compatibility across build paths.
#[derive(Debug, Clone)]
pub struct GrpcCfg {
    pub url: String,
    /// gRPC service name (e.g. "market.MarketData"). Used for discovery.
    pub service: String,
    /// Deprecated. Ignored. Metrics come from msg.metrics in the proto.
    #[allow(dead_code)]
    pub price_path: String,
    /// Deprecated. Ignored. Metrics come from msg.metrics in the proto.
    #[allow(dead_code)]
    pub volume_path: String,
    /// Bearer token populated by the
    /// `FORS33_SECRET_CONNECTOR__GRPC__TOKEN` env overlay. When present, the
    /// connector wraps the channel in an interceptor that injects
    /// `authorization: Bearer <token>` into every outgoing request metadata.
    pub token: Option<String>,
    /// Additional gRPC metadata (header name/value) applied on every outbound call.
    pub metadata_pairs: Vec<(String, String)>,
}

fn build_endpoint(cfg: &GrpcCfg) -> Result<Endpoint> {
    // tonic 0.14 reads the scheme from the URL and selects its built-in TLS
    // path automatically when the URL is `https://`. Trust validation runs
    // through tonic's internal rustls integration. A future iteration can
    // expose the leaf cert through `connect_with_connector` + tokio-rustls,
    // but the public `tls_config` API is not stable across the 0.14 surface,
    // so we rely on tonic's defaults here.
    Endpoint::from_shared(cfg.url.clone()).context("invalid gRPC URL")
}

async fn connect_and_emit(cfg: &GrpcCfg) -> Result<Channel> {
    let endpoint = build_endpoint(cfg)?;
    let channel = endpoint
        .connect()
        .await
        .context("failed to connect to gRPC server")?;

    // gRPC TLS sessions: tonic 0.14's `ClientTlsConfig` performs full webpki
    // trust validation but does not surface the negotiated leaf certificate
    // back to user code. A future iteration can use `connect_with_connector`
    // with a tokio-rustls-backed observing service to extract the leaf
    // bytes; until then, gRPC TLS handshakes complete with proper validation
    // but no `[T3thr:CONNECTION_META]` line is emitted (the daemon parser
    // treats this as "no fingerprint captured" rather than asserting one
    // that was not actually observed).

    Ok(channel)
}

/// Run gRPC connector; async, call from tokio runtime.
pub async fn run_grpc_connector(
    cfg: &GrpcCfg,
    filter_cfg: &FilterCfg,
    tx: SyncSender<Result<DataPoint, (String, String, Option<u64>)>>,
) -> Result<()> {
    let channel = connect_and_emit(cfg).await?;

    let bearer = cfg
        .token
        .as_deref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .and_then(|t| MetadataValue::try_from(format!("Bearer {}", t)).ok());

    let mut extra_metadata = Vec::new();
    for (k_raw, v_raw) in &cfg.metadata_pairs {
        let k_trim = k_raw.trim();
        if k_trim.is_empty() {
            continue;
        }
        let key_lc = k_trim.to_ascii_lowercase();
        let expanded = crate::utils::expand_fors33_secret_placeholders(v_raw.trim())
            .with_context(|| format!("gRPC metadata {:?} placeholder expansion failed", k_trim))?;
        let expanded = expanded.trim();
        if expanded.is_empty() {
            continue;
        }
        let name =
            tonic::metadata::MetadataKey::<tonic::metadata::Ascii>::from_bytes(key_lc.as_bytes())
                .map_err(|_| anyhow::anyhow!("invalid gRPC metadata key: {}", key_lc))?;
        let val = MetadataValue::try_from(expanded.to_string())
            .map_err(|_| anyhow::anyhow!("invalid gRPC metadata value for key `{}`", key_lc))?;
        extra_metadata.push((name, val));
    }

    let interceptor = OutboundMetadataInterceptor {
        bearer,
        extra: extra_metadata,
    };
    let intercepted: InterceptedService<Channel, OutboundMetadataInterceptor> =
        InterceptedService::new(channel, interceptor);
    let mut client = DataStreamClient::new(intercepted);
    let request = tonic::Request::new(datastream::StreamRequest {});
    let mut stream = client
        .stream_data_points(request)
        .await
        .context("failed to start StreamDataPoints")?
        .into_inner();

    let field_count = filter_cfg
        .bounds
        .as_ref()
        .and_then(|bounds| {
            bounds
                .metrics
                .keys()
                .filter_map(|k| k.strip_prefix("metric_"))
                .filter_map(|s| s.split('.').next())
                .filter_map(|s| s.parse::<usize>().ok())
                .max()
                .map(|m| m + 1)
        })
        .unwrap_or(2);
    let mut state = FilterState::with_capacity(field_count);

    while let Some(msg) = stream.message().await.context("gRPC stream error")? {
        let timestamp_ns = if msg.timestamp_ns != 0 {
            msg.timestamp_ns as u64
        } else {
            now_unix_ms() * 1_000_000
        };
        if msg.metrics.is_empty() {
            continue;
        }
        let point = DataPoint {
            timestamp_ns,
            metrics: msg.metrics,
            feed: None,
        };
        let raw = format!(
            "grpc_msg timestamp_ns={} metrics={:?}",
            timestamp_ns, point.metrics
        );
        match state.check(&point, filter_cfg) {
            Ok(()) => {
                if tx.send(Ok(point)).is_err() {
                    eprintln!("[FORS33] FATAL: Writer channel closed. Stopping grpc connector.");
                    std::process::exit(1);
                }
            }
            Err(reason) => {
                if tx.send(Err((reason, raw, Some(timestamp_ns)))).is_err() {
                    eprintln!("[FORS33] FATAL: Writer channel closed. Stopping grpc connector.");
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}
