//! `t3thr generate` subcommand: emit a frictionless TOML template for the
//! requested connector slug.
//!
//! Templates live under `t3thr_bridge/config/templates/*.toml` and are
//! embedded via `include_str!` so the generator output is auditable in the
//! source tree (no I/O at runtime, no version drift between binary and
//! template files).
//!
//! Behavior:
//!   - `--out <path>` writes the template to the given file.
//!   - omitted `--out` writes the template to stdout.
//!   - unknown connector slugs return a non-zero exit.

use std::fs;
use std::io::{self, Write};

use anyhow::{Result, anyhow};

use super::GenerateArgs;

const KRAKEN_WEBSOCKET: &str = include_str!("../../config/templates/kraken_websocket.toml");
const BINANCE_WEBSOCKET: &str = include_str!("../../config/templates/binance_websocket.toml");
const ALCHEMY_WEBSOCKET: &str = include_str!("../../config/templates/alchemy_websocket.toml");
const INFURA_WEBSOCKET: &str = include_str!("../../config/templates/infura_websocket.toml");
const KAFKA_CONSUMER: &str = include_str!("../../config/templates/kafka_consumer.toml");
const MQTT_CONSUMER: &str = include_str!("../../config/templates/mqtt_consumer.toml");
const POSTGRES_CDC: &str = include_str!("../../config/templates/postgres_cdc.toml");
const MYSQL_CDC: &str = include_str!("../../config/templates/mysql_cdc.toml");
const SYSLOG_SERVER: &str = include_str!("../../config/templates/syslog_server.toml");
const UDP_RAW_SERVER: &str = include_str!("../../config/templates/udp_raw_server.toml");
const REST_POLLING: &str = include_str!("../../config/templates/rest_polling.toml");
const GRPC_CLIENT: &str = include_str!("../../config/templates/grpc_client.toml");
const FILE_BATCH: &str = include_str!("../../config/templates/file_batch.toml");

pub fn template_for(slug: &str) -> Option<&'static str> {
    match slug {
        "kraken-websocket" => Some(KRAKEN_WEBSOCKET),
        "binance-websocket" => Some(BINANCE_WEBSOCKET),
        "alchemy-websocket" => Some(ALCHEMY_WEBSOCKET),
        "infura-websocket" => Some(INFURA_WEBSOCKET),
        "kafka-consumer" => Some(KAFKA_CONSUMER),
        "mqtt-consumer" => Some(MQTT_CONSUMER),
        "postgres-cdc" => Some(POSTGRES_CDC),
        "mysql-cdc" => Some(MYSQL_CDC),
        "syslog-server" => Some(SYSLOG_SERVER),
        "udp-raw-server" => Some(UDP_RAW_SERVER),
        "rest-polling" => Some(REST_POLLING),
        "grpc-client" => Some(GRPC_CLIENT),
        "file-batch" => Some(FILE_BATCH),
        _ => None,
    }
}

pub fn execute(args: &GenerateArgs) -> Result<()> {
    let body = template_for(args.connector.as_str())
        .ok_or_else(|| anyhow!("unknown connector: {}", args.connector))?;
    match &args.out {
        Some(path) => fs::write(path, body)
            .map_err(|e| anyhow!("failed to write template to {}: {}", path.display(), e))?,
        None => io::stdout().write_all(body.as_bytes())?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_slugs() -> &'static [&'static str] {
        &[
            "kraken-websocket",
            "binance-websocket",
            "alchemy-websocket",
            "infura-websocket",
            "kafka-consumer",
            "mqtt-consumer",
            "postgres-cdc",
            "mysql-cdc",
            "syslog-server",
            "udp-raw-server",
            "rest-polling",
            "grpc-client",
            "file-batch",
        ]
    }

    #[test]
    fn every_slug_resolves() {
        for slug in all_slugs() {
            let body = template_for(slug);
            assert!(body.is_some(), "missing template for {}", slug);
            assert!(!body.unwrap().is_empty(), "empty template for {}", slug);
        }
    }

    #[test]
    fn unknown_slug_returns_none() {
        assert!(template_for("not-a-real-connector").is_none());
        assert!(template_for("").is_none());
    }

    #[test]
    fn templates_carry_secret_placeholders_where_applicable() {
        // Only connectors whose secret-bearing fields are actually collected
        // by the extension UI/backend secret-env pipeline must carry
        // ${FORS33_SECRET_*} placeholders. Provider WS feeds (alchemy, infura,
        // kraken, binance) embed the project key directly into the URL, which
        // is regular config (not a secret env), so they are intentionally
        // exempt from this check.
        let secret_required = &[
            "kafka-consumer",
            "mqtt-consumer",
            "postgres-cdc",
            "mysql-cdc",
        ];
        for slug in secret_required {
            let body = template_for(slug).unwrap();
            assert!(
                body.contains("${FORS33_SECRET_CONNECTOR__"),
                "{} must use ${{FORS33_SECRET_CONNECTOR__*}} placeholders",
                slug
            );
        }
    }

    #[test]
    fn websocket_templates_do_not_use_secret_env_for_url() {
        // Provider URLs are not collected by the secret-env pipeline; the
        // operator pastes their project key into the URL via the UI. Make
        // sure no WS template smuggles the URL through ${FORS33_SECRET_*}.
        for slug in &[
            "kraken-websocket",
            "binance-websocket",
            "alchemy-websocket",
            "infura-websocket",
        ] {
            let body = template_for(slug).unwrap();
            assert!(
                !body.contains("${FORS33_SECRET_WEBSOCKET__"),
                "{} must not route the URL through the secret-env pipeline",
                slug
            );
        }
    }

    #[test]
    fn kafka_template_uses_sasl_username_password_placeholders() {
        let body = template_for("kafka-consumer").unwrap();
        assert!(
            body.contains("${FORS33_SECRET_CONNECTOR__MESSAGE_BUS__KAFKA__SASL_USERNAME}"),
            "kafka template missing SASL_USERNAME placeholder"
        );
        assert!(
            body.contains("${FORS33_SECRET_CONNECTOR__MESSAGE_BUS__KAFKA__SASL_PASSWORD}"),
            "kafka template missing SASL_PASSWORD placeholder"
        );
    }

    #[test]
    fn templates_have_no_obvious_plaintext_credentials() {
        for slug in all_slugs() {
            let body = template_for(slug).unwrap();
            // Trivial smoke: no quoted password literals.
            assert!(!body.contains("password = \"hunter2\""), "{}", slug);
        }
    }
}
