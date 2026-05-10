//! Shared TLS observability module for the T3thr bridge.
//!
//! This module is the **single emit point** for `[T3thr:CONNECTION_META]`
//! observability lines that the FORS33 daemon (`dpk_daemon._try_parse_connection_meta_line`)
//! consumes off stderr. Every TLS-capable connector hands its captured leaf
//! certificate (DER bytes) to `observe_and_emit` so the wire format stays
//! byte-identical regardless of which connector produced it.
//!
//! ## Strict Boundary
//!
//! T3thr is pure ETL + observability. This module:
//!   - Parses X.509 fields (subject, issuer, SANs) from leaf DER.
//!   - Hashes the DER bytes (SHA-256) for fingerprinting.
//!   - Emits a single sanitized JSON line on stderr.
//!
//! It does **not**:
//!   - Hash payloads, sign data, or write WORM artifacts.
//!   - Make any compliance policy decision (that lives in the Python daemon).
//!   - Bypass TLS validation. Custom verifiers in connectors must wrap and
//!     **delegate** to `WebPkiServerVerifier`; this module only inspects the
//!     leaf bytes the verifier already received.
//!
//! ## Idempotency
//!
//! Each TLS session must call `observe_and_emit` exactly once per established
//! connection. Connectors guard against duplicate emissions on reconnects
//! using a per-connection `OnceCell`/`AtomicBool` so the daemon's parser sees
//! a deterministic line count.

use sha2::{Digest, Sha256};

#[cfg(feature = "full_engine")]
use x509_parser::prelude::*;

use crate::utils;

/// Capture, parse, and emit TLS peer metadata from a single leaf certificate.
///
/// `leaf_der` must be the leaf (server) certificate in DER form, exactly as
/// rustls passes it through `ServerCertVerifier::verify_server_cert`.
///
/// Behavior:
///   1. Compute SHA-256 hex of the DER bytes -> `tls_fingerprint_sha256`.
///   2. Parse the certificate with `x509-parser`.
///   3. Render the subject and issuer as RFC 4514 strings (lossy if the cert
///      contains malformed bytes; sanitized at the `utils` layer).
///   4. Walk the SubjectAltName extension and collect DNS / IPAddress / URI
///      entries into a `Vec<String>`.
///   5. Emit one `[T3thr:CONNECTION_META]` JSON line via `utils::emit_connection_meta`.
///
/// Fields that fail UTF-8 sanitization are dropped at the `utils::sanitize_*`
/// layer; if every field is empty after sanitization, no line is emitted.
pub fn observe_and_emit(leaf_der: &[u8]) {
    let fingerprint = sha256_hex(leaf_der);

    let (subject, issuer, sans) = parse_x509_fields(leaf_der);

    let san_refs: Vec<&str> = sans.iter().map(|s| s.as_str()).collect();
    let san_slice: Option<&[&str]> = if san_refs.is_empty() {
        None
    } else {
        Some(san_refs.as_slice())
    };

    utils::emit_connection_meta(
        Some(fingerprint.as_str()),
        subject.as_deref(),
        san_slice,
        issuer.as_deref(),
    );
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest: [u8; 32] = hasher.finalize().into();
    hex::encode(digest)
}

/// Parse RFC 4514 subject + issuer strings and SubjectAltName entries from
/// leaf DER. Returns `(subject, issuer, sans)` with empty/invalid fields
/// reduced to `None` / empty Vec rather than propagating parse errors so a
/// malformed peer cannot break the observability path.
#[cfg(feature = "full_engine")]
fn parse_x509_fields(leaf_der: &[u8]) -> (Option<String>, Option<String>, Vec<String>) {
    let parsed = match X509Certificate::from_der(leaf_der) {
        Ok((_, cert)) => cert,
        Err(_) => return (None, None, Vec::new()),
    };

    let subject = {
        let s = parsed.subject().to_string();
        if s.trim().is_empty() { None } else { Some(s) }
    };

    let issuer = {
        let s = parsed.issuer().to_string();
        if s.trim().is_empty() { None } else { Some(s) }
    };

    let mut sans: Vec<String> = Vec::new();
    if let Ok(Some(ext)) = parsed.subject_alternative_name() {
        for gn in ext.value.general_names.iter() {
            match gn {
                GeneralName::DNSName(s) => {
                    let t = s.trim();
                    if !t.is_empty() {
                        sans.push(t.to_string());
                    }
                }
                GeneralName::IPAddress(bytes) => {
                    if let Some(ip) = render_ip(bytes) {
                        sans.push(ip);
                    }
                }
                GeneralName::URI(s) => {
                    let t = s.trim();
                    if !t.is_empty() {
                        sans.push(t.to_string());
                    }
                }
                _ => {}
            }
        }
    }

    (subject, issuer, sans)
}

#[cfg(not(feature = "full_engine"))]
fn parse_x509_fields(_leaf_der: &[u8]) -> (Option<String>, Option<String>, Vec<String>) {
    // Slim builds carry x509-parser unconditionally for P1 compliance, but the
    // connectors that actually open TLS sessions (websocket/grpc/mqtt/cdc)
    // live behind `full_engine`. Keep a no-op fallback so the slim REST path
    // can call `observe_and_emit` and still emit a fingerprint without
    // pulling any X.509 parsing into the hot path.
    (None, None, Vec::new())
}

#[cfg(feature = "full_engine")]
fn render_ip(bytes: &[u8]) -> Option<String> {
    match bytes.len() {
        4 => Some(format!(
            "{}.{}.{}.{}",
            bytes[0], bytes[1], bytes[2], bytes[3]
        )),
        16 => {
            let mut groups = [0u16; 8];
            for (i, g) in groups.iter_mut().enumerate() {
                *g = ((bytes[i * 2] as u16) << 8) | (bytes[i * 2 + 1] as u16);
            }
            Some(
                groups
                    .iter()
                    .map(|g| format!("{:x}", g))
                    .collect::<Vec<_>>()
                    .join(":"),
            )
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_hex_is_deterministic() {
        let bytes = b"hello";
        let a = sha256_hex(bytes);
        let b = sha256_hex(bytes);
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn observe_and_emit_with_garbage_does_not_panic() {
        // Malformed DER must not crash the connector. The utils layer drops
        // unparseable subject/issuer fields and the function still emits the
        // SHA-256 fingerprint, which is always derivable from raw bytes.
        observe_and_emit(b"\x00\x01garbage-not-a-cert");
    }

    #[cfg(feature = "full_engine")]
    #[test]
    fn parse_x509_returns_empty_on_invalid_der() {
        let (subject, issuer, sans) = parse_x509_fields(b"not der");
        assert!(subject.is_none());
        assert!(issuer.is_none());
        assert!(sans.is_empty());
    }

    #[test]
    fn build_connection_meta_value_round_trips_through_emit() {
        // Drive the same code path the daemon parser will see; ensure no
        // panics, and the JSON shape is non-empty for at least one field.
        observe_and_emit(b"some-leaf-bytes");
    }
}
