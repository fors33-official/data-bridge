# Fors33 T3thr

[![CI](https://img.shields.io/github/actions/workflow/status/fors33-official/data-bridge/release.yml?branch=main&style=flat-square)](https://github.com/fors33-official/data-bridge/actions)
[![Release](https://img.shields.io/badge/release-0.5.0-blue?style=flat-square)](https://github.com/fors33-official/data-bridge/releases)
[![Docker Tag](https://img.shields.io/badge/docker-0.5.0%20%7C%20latest-2496ED?style=flat-square&logo=docker&logoColor=white)](https://hub.docker.com/r/fors33/data-bridge)
[![Docker Pulls](https://img.shields.io/docker/pulls/fors33/data-bridge?style=flat-square)](https://hub.docker.com/r/fors33/data-bridge)
[![License](https://img.shields.io/github/license/fors33-official/data-bridge?style=flat-square)](https://github.com/fors33-official/data-bridge/blob/main/LICENSE)

T3thr is a config-driven tool for processing time-series data from **local files** (free tier) or **live connectors** (subscription-gated), producing clean outputs for downstream analysis.

## Limitation of Liability

T3thr is a deterministic ingestion engine provided "AS IS". Fors33 is not liable for data dropped due to network latency, third-party API rate limits, or improper local hardware configuration. The operator assumes all responsibility for regulatory compliance, data retention, and hardware provisioning. For the full EULA, see `fors33.com/products/t3thr`.

## Quick start (Docker)

- **Mount your config directory** to `/app/config`
- **Default config path** inside the container is `/app/config/default.toml`

### Recommended hardened runtime flags

If you run with `--read-only`, you must also:
- mount `/app/config` as read-only
- mount a writable destination for outputs (from `BridgeConfig.output`, e.g. `/app/out`)

Example (Mac / Linux):
- `--cap-drop=ALL`
- `--security-opt no-new-privileges:true`
- `--read-only`

### Mac / Linux

```bash
docker run --rm \
  --cap-drop=ALL \
  --security-opt no-new-privileges:true \
  --read-only \
  -v "$(pwd)/config:/app/config:ro" \
  -v "$(pwd)/data:/app/out" \
  fors33/data-bridge \
  --config /app/config/default.toml
```

### Windows (PowerShell)

```powershell
docker run --rm `
  --cap-drop=ALL `
  --security-opt no-new-privileges:true `
  --read-only `
  -v ${PWD}/config:/app/config:ro `
  -v ${PWD}/data:/app/out `
  fors33/data-bridge `
  --config /app/config/default.toml
```

### GitHub Container Registry (Alternative)

You can also pull from GitHub Container Registry:

```bash
docker pull ghcr.io/fors33-official/data-bridge:0.5.0
docker pull ghcr.io/fors33-official/data-bridge:latest
```

## Live connectors (subscription required)

Live connectors (WebSocket / REST / Message Bus / gRPC / Syslog / UDP Raw / CDC) require `FORS33_LICENSE_KEY`.

```bash
docker run --rm \
  -e FORS33_LICENSE_KEY="your_key" \
  -v "$(pwd)/config:/app/config" \
  fors33/data-bridge \
  --config /app/config/default.toml
```

### CLI overview

- **Legacy style (unchanged):** `t3thr --config /app/config/default.toml` (equivalent to `t3thr run --config …`).
- **Explicit subcommands:** `t3thr run …`, **`t3thr generate --connector <slug>`** (templates under `config/templates/`), **`t3thr validate --config …`**, **`t3thr migrate …`**, **`t3thr wizard`** (same as the `config_wizard` binary).
- **Thin wrappers:** `validate_config`, `migrate_config` binaries delegate to **`t3thr`** (stable for scripting).

<details>
<summary><strong>Release notes (expand)</strong></summary>

<details open>
<summary>0.5.0 – Extension Rust bridge parity (standalone)</summary>

Summary: Rust stack matches the Fors33 Data Latch **extension bridge** (`t3thr_bridge`) for TLS observability (`[T3thr:CONNECTION_META]`), rustls-centric connectors, **`FORS33_SECRET_*`** placeholder expansion (additive next to **`T3THR_*`** env tables), nested Kafka/MQTT config, **`t3thr generate`** from embedded **`config/templates/`**, state file locking, Tokio **`block_in_place`** for file ingestion, unified subcommand CLI with **backward-compatible** bare **`--config`**, **`--validate-only`**, **`--config-wizard`**, and retained **`validate_config`** / **`migrate_config`** binaries. Published images now include **`config_wizard`**, **`validate_config`**, and **`migrate_config`** under `/usr/local/bin/`.

Technical note: **`config`** crate merges **`FORS33_SECRET`** with `__` separators after loading the file (see [`docs/bridge-release-notes-0.5.0.md`](docs/bridge-release-notes-0.5.0.md)).

</details>

<details>
<summary>0.4.0 – wizard, connectors, batch</summary>

- **Frictionless configuration:** Wizard and templates for standard providers (Kraken, Binance, Alchemy, Infura, Postgres, MySQL, and more).
- **Interactive wizard:** run **`cargo run --bin config_wizard`**, **`t3thr wizard`**, or **`t3thr --config-wizard`**.
- **Pre-configured examples:** **`config/`** directory (sample TOMLs); canonical generator templates live under **`config/templates/`** from 0.5.0 onward.
- **Connectors:** Syslog (RFC 5424/3164), UDP JSON, CDC (Postgres/MySQL), Kafka/MQTT, gRPC.
- **Batch mode:** **`mode = "batch"`**; state file **`.t3thr-state.json`** with **`--reset-state`** / **`--no-state`**.
- **Registries:** Docker Hub **`fors33/data-bridge`** and **`ghcr.io/fors33-official/data-bridge`**.

</details>

<details>
<summary>0.3.0 – secrets in the environment, not on disk</summary>

**Preferred (direct mapping):** optional **`env_*`** tables map a wire key to an **environment variable name** (`T3THR_[A-Z0-9_]+`). Values are sent as-is (put a full `Bearer …` in the env var if needed).

- `[connector.rest.env_headers]`
- `[connector.websocket.env_headers]`
- `[connector.grpc.env_metadata]`
- `[connector.message_bus.kafka.env_client_properties]` / `[connector.message_bus.mqtt.env_client_properties]` (nested; Kafka/MQTT also support literal **`client_properties`** maps)

**Deprecated path:** whole-value **`${T3THR_*}`** in literal maps still resolves with **`[DEPRECATION]`** warning.

**License key PEM:** optional **`FORS33_RUNTIME_PUBKEY_PEM`** for local Ed25519 verification before embedded issuer PEM.

**Validate:** `validate_config path/to.toml` or `t3thr validate --config …` or **`t3thr --validate-only --config …`**.

The default **slim** build supports **file**, **CSV**, and **REST**; **full_engine** adds WebSocket, message bus, gRPC, syslog, UDP raw, CDC, Parquet (the **`fors33/data-bridge`** image builds with **`full_engine`**).

</details>

</details>

## Documentation

- [`docs/bridge-release-notes-0.5.0.md`](docs/bridge-release-notes-0.5.0.md)
- `docs/QUICK_START.md`
- `pkg/README.md`
- `docs/license_backend_contract.md`

