//! Shared TLS observing verifier.
//!
//! Wraps the default rustls webpki verifier and, **after** a successful trust
//! verification, hands the leaf DER to `tls_meta::observe_and_emit`.
//!
//! ### Hard rule: NEVER bypass TLS validation.
//!
//! The wrapper:
//!   1. Delegates the cryptographic trust decision to `WebPkiServerVerifier`.
//!   2. Returns the verifier's result unchanged.
//!   3. Calls `tls_meta::observe_and_emit` only on `Ok(_)` outcomes.
//!
//! Returning `Ok(...)` without delegating is a **release blocker** because it
//! turns the ETL transport into a Man-in-the-Middle target.

use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error, RootCertStore, SignatureScheme};

use crate::tls_meta;

/// Default root store: prefer the system's native trust store when available
/// (matches OS policy on Linux/macOS/Windows). Fall back to the bundled
/// `webpki-roots` Mozilla bundle so the binary still works inside slim
/// distroless containers.
pub fn default_root_store() -> RootCertStore {
    let mut store = RootCertStore::empty();

    if let Ok(certs) = rustls_native_certs::load_native_certs() {
        for c in certs {
            let _ = store.add(c);
        }
    }

    if store.is_empty() {
        store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    store
}

/// Build a `rustls::ClientConfig` whose certificate verifier observes leaf
/// metadata and emits a `[T3thr:CONNECTION_META]` line through the shared
/// `utils::emit_connection_meta` path. Trust validation is fully delegated to
/// `rustls::client::WebPkiServerVerifier`.
pub fn observing_client_config() -> rustls::ClientConfig {
    let roots = default_root_store();
    let inner = WebPkiServerVerifier::builder(Arc::new(roots))
        .build()
        .expect("WebPkiServerVerifier build");
    let observer = Arc::new(ObservingVerifier { inner });
    rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(observer)
        .with_no_client_auth()
}

/// Verifier wrapper around `WebPkiServerVerifier` that emits TLS observability
/// metadata on every **successful** verification.
#[derive(Debug)]
pub struct ObservingVerifier {
    inner: Arc<WebPkiServerVerifier>,
}

impl ServerCertVerifier for ObservingVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        // 1. Crypto-validate FIRST. Any failure must propagate; metadata
        //    emission is suppressed for untrusted peers.
        let result = self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )?;

        // 2. Trusted leaf -> emit observability line. The `tls_meta` module
        //    is responsible for sanitizing every field before stderr.
        tls_meta::observe_and_emit(end_entity.as_ref());

        Ok(result)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ensure_crypto_provider() {
        // rustls 0.23 requires a process-wide CryptoProvider before any
        // ClientConfig can be built. Tests in the same binary may run before
        // the connectors install one implicitly, so we install it idempotently.
        // We pin the `ring` provider here to match the Cargo.toml feature set.
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    #[test]
    fn observing_client_config_builds() {
        ensure_crypto_provider();
        let _cfg = observing_client_config();
    }

    #[test]
    fn default_root_store_is_non_empty() {
        ensure_crypto_provider();
        let store = default_root_store();
        assert!(
            !store.is_empty(),
            "trust anchors must be present (native or webpki-roots)"
        );
    }
}
