# T3thr bridge release notes – 0.5.0

Standalone **T3thr** release aligned with extension bridge transport behavior. Extension-agnostic: no daemon spawn, sealing, or attestation logic in this crate.

## TLS and connectors

- Shared TLS observability via `[T3thr:CONNECTION_META]` stderr contract
- rustls-centric connectors with native trust store when `native_certs` feature is enabled
- Nested Kafka/MQTT config blocks; state file locking; Tokio `block_in_place` for file ingestion

## Configuration and CLI

- **`FORS33_SECRET_*`** placeholder expansion (additive next to **`T3THR_*`** env tables)
- **`t3thr generate`** from embedded **`config/templates/`**
- Unified subcommand CLI with backward-compatible bare **`--config`**, **`--validate-only`**, **`--config-wizard`**
- Retained **`validate_config`** / **`migrate_config`** thin binaries in Docker image

## Packaging

- Published images include **`config_wizard`**, **`validate_config`**, and **`migrate_config`** under `/usr/local/bin/`
- Docker Hub **`fors33/data-bridge`** and **`ghcr.io/fors33-official/data-bridge`**
