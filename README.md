# Fors33 T3thr

![Version](https://img.shields.io/github/v/release/fors33-official/data-bridge?style=flat-square&color=black)
![Docker Pulls](https://img.shields.io/docker/pulls/fors33/data-bridge?style=flat-square&color=black)
![License](https://img.shields.io/github/license/fors33-official/data-bridge?style=flat-square&color=black)
![Build Status](https://img.shields.io/github/actions/workflow/status/fors33-official/data-bridge/release.yml?style=flat-square&color=black)
![GHCR Version](https://img.shields.io/github/v/release/fors33-official/data-bridge?label=GHCR&style=flat-square&color=black)

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
docker pull ghcr.io/fors33-official/data-bridge:0.4.0
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

### New in 0.4.0

**Frictionless Configuration:** Zero-configuration setup for standard providers. The config wizard now auto-generates complete configs for Kraken, Binance, Alchemy, Infura, PostgreSQL, MySQL, and more with pre-filled endpoints and ports.

**Pre-configured Templates:** 10 ready-to-use templates in `config/` directory:
- WebSocket: Kraken, Binance (Spot/Futures), Alchemy, Infura
- CDC: PostgreSQL (port 5432), MySQL (port 3306)
- Infrastructure: Syslog (port 514), Kafka (port 9092), MQTT (port 1883), gRPC (port 50051)

**Additional connectors:** syslog (RFC 5424/3164), UDP JSON datagrams, CDC (Postgres/MySQL).

**Batch processing mode:** Process historical data and exit on completion. Enable with `mode = "batch"` in file or REST connectors.

**State tracking:** Resume interrupted batch jobs from last position using `.t3thr-state.json`.

**CLI flags:**
- `--reset-state` - Delete state file for fresh start
- `--no-state` - Disable state tracking

### 0.3.0: secrets in the environment, not on disk

**Preferred (direct mapping):** optional tables map a wire key to an **environment variable name** (must match `T3THR_[A-Z0-9_]+`). The process reads `std::env::var`, trims, and sends the value as-is (no string concatenation—put a full `Bearer …` string in the env var if needed).

- `[connector.rest.env_headers]`
- `[connector.websocket.env_headers]`
- `[connector.grpc.env_metadata]`
- `[connector.message_bus.env_client_properties]`

**Literal-only tables** (no env indirection): `[connector.rest.headers]`, `[connector.websocket.headers]`, `[connector.grpc.metadata]`, `[connector.message_bus.client_properties]`.

**Deprecated (one release):** a whole-value template `${T3THR_*}` inside a literal table still resolves, but prints `[Fors33] [DEPRECATION]` to stderr—migrate to the matching `env_*` table.

**Placeholder rule (deprecated path only):** value must match `^\$\{([A-Z0-9_]+)\}$`; name must start with `T3THR_`; missing or empty after trim is a hard error.

**License key PEM override:** if `FORS33_RUNTIME_PUBKEY_PEM` is set and non-empty after trim, the bridge uses that Ed25519 public PEM to verify `FORS33_LICENSE_KEY` before falling back to the embedded issuer PEM.

**Validate config:** `cargo run --release --bin validate_config -- path/to/config.toml` (or `t3thr --validate-only --config …`) runs the same parse, normalization, and env binding resolution as a normal start, including the live license check when applicable.

The default **slim** binary supports **file**, **CSV**, and **REST**; **WebSocket**, **message_bus**, **gRPC**, **syslog**, **UDP raw**, **CDC** (Postgres/MySQL), and **Parquet** file input require a build with `--features full_engine` (the published `fors33/data-bridge` image uses `full_engine`).

## Documentation

- `docs/QUICK_START.md`
- `pkg/README.md`
- `docs/license_backend_contract.md`

