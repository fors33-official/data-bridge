## Docker image: Fors33 Data-bridge (main binary + compat helpers)

This directory contains packaging assets for the bridge image: primary entrypoint **`t3thr`**, plus **`config_wizard`**, **`validate_config`**, and **`migrate_config`** copied into `/usr/local/bin/` for legacy operator flows. **Free File/CSV tier** and **Pro live connectors** remain gated by `FORS33_LICENSE_KEY`.

### Build (multi-arch example)

**Docker Hub tags:** use numeric semver only (e.g. `0.5.0`), not a `v` prefix, plus `latest`.

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t fors33/data-bridge:0.5.0 \
  -t fors33/data-bridge:latest \
  -f pkg/Dockerfile \
  .
```

### Config mount contract

The container expects configs under `/app/config`. The binary defaults to `config/default.toml`, so with `WORKDIR /app` this resolves to `/app/config/default.toml`.

### Recommended hardened runtime flags (optional)

If you use `--read-only`, mount:
- `/app/config` as read-only (e.g. `:ro`)
- a writable output mount for your `BridgeConfig.output` paths (e.g. `/app/out`)

Example (Mac/Linux):
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

Example (Windows / PowerShell):
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

- **POSIX (Mac/Linux):**

```bash
docker run \
  -v "$(pwd)/config:/app/config" \
  fors33/data-bridge \
  --config /app/config/default.toml
```

- **Windows (PowerShell):**

```powershell
docker run `
  -v ${PWD}/config:/app/config `
  fors33/data-bridge `
  --config /app/config/default.toml
```

### Free tier: File / CSV

File and CSV connectors run **without any license key**:

```bash
docker run \
  -v "$(pwd)/config:/app/config" \
  fors33/data-bridge \
  --config /app/config/file_example.toml
```

### Connector maps, `T3THR_*`, and `${FORS33_SECRET_*}` (0.5.0+)

Prefer **`env_*` tables** (`[connector.rest.env_headers]`, `[connector.websocket.env_headers]`, `[connector.grpc.env_metadata]`, `[connector.message_bus.env_client_properties]`): each value is an environment variable **name** (`T3THR_*`); the resolved value is sent on the wire as-is. Literal maps (`headers`, `metadata`, `client_properties`) hold non-secret strings only; whole-value `${T3THR_*}` in literals is deprecated (still resolved one release, with stderr warning). **`${FORS33_SECRET_*}`** tokens in wire strings and nested client properties resolve from env at runtime (**additive**). Optional `FORS33_RUNTIME_PUBKEY_PEM` overrides the embedded Ed25519 public key for JWT verification.

Validate before run: `t3thr --validate-only --config /app/config/your.toml` (bare `t3thr --config …` without `run` is still accepted).

### Pro tier: Live connectors (WebSocket / REST / Message Bus / gRPC)

Live/network connectors require a valid `FORS33_LICENSE_KEY` (JWT, verified offline via EdDSA):

```bash
docker run \
  -e FORS33_LICENSE_KEY="your_key" \
  -v "$(pwd)/config:/app/config" \
  fors33/data-bridge \
  --config /app/config/live_example.toml
```

If the key is missing or invalid, the binary prints a **clinical**, non-technical message to `stderr` and exits with a non-zero code:

```text
[Fors33] ACCESS DENIED: Live Streaming Requires Active Subscription. The requested connector is restricted.
1. Purchase access at https://fors33.com/products/t3thr.
2. Set environment variable: FORS33_LICENSE_KEY="your_key"
```

