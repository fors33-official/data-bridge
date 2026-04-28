# Quick Start Guide - Fors33 T3thr

This guide helps non-technical users set up and run Fors33 T3thr in under 5 minutes.

## What is T3thr?

T3thr is a tool that:
- Reads data from files, APIs, or live streams
- Checks data quality (filters out bad/anomalous values)
- Saves clean data to output files
- Adds tamper-evident cryptographic verification

**No programming required.** Everything is configured through simple text files.

## Installation

### Windows (source build)
1. Install Rust: https://rustup.rs/
2. Open PowerShell
3. Navigate to the bridge folder:
   ```powershell
   cd path\to\bridge
   ```
4. Build the tool:
   ```powershell
   cargo build --release
   ```

This creates `target/release/t3thr.exe`

### Docker (no Rust toolchain required)

You can also run the bridge from a single Docker image.

- **POSIX (Mac/Linux), free File/CSV tier:**

```bash
docker run \
  -v "$(pwd)/config:/app/config" \
  fors33/data-bridge \
  --config /app/config/default.toml
```

- **Windows (PowerShell), free File/CSV tier:**

```powershell
docker run `
  -v ${PWD}/config:/app/config `
  fors33/data-bridge `
  --config /app/config/default.toml
```

Live connectors (WebSocket / REST / Message Bus / gRPC) require a valid `FORS33_LICENSE_KEY`. Example:

```bash
docker run \
  -e FORS33_LICENSE_KEY="your_key" \
  -v "$(pwd)/config:/app/config" \
  fors33/data-bridge \
  --config /app/config/live_example.toml
```

If the key is missing or invalid, the container exits with a clear message explaining that live streaming requires an active subscription and how to supply `FORS33_LICENSE_KEY`.

### Optional: runtime public key for licenses

If you mint JWTs with a **local** Ed25519 key pair, set `FORS33_RUNTIME_PUBKEY_PEM` to the **public** PEM so verification matches your signer. Otherwise the embedded cloud public key is used.

### Optional: connector secrets via `T3THR_*` (direct mapping)

Keep secrets out of TOML. Prefer **`env_*` tables**: e.g. `[connector.rest.env_headers]` with `Authorization = "T3THR_REST_TOKEN"` (the value is the **name** of an env var; put the real header value, including any `Bearer ` prefix, in that variable). Legacy whole-value `${T3THR_*}` in literal maps still works for one release but prints a deprecation warning.

**Check a config without running ingestion:**

```powershell
cargo run --release --bin validate_config -- config\your_config.toml
```

This delegates to `t3thr --validate-only` and exits non-zero on missing env vars or license errors.

**Live connectors beyond REST** (WebSocket, Kafka/MQTT, gRPC) and **Parquet** file input need `full_engine`:

```powershell
cargo build --release --features full_engine
cargo run --release --features full_engine -- --config config\your_config.toml
```

## Your First Pipeline

### Step 1: Run the Config Wizard

The easiest way to get started:

```powershell
cargo run --bin config_wizard
```

The wizard will ask simple questions:
- What type of data source? (CSV file, REST API, WebSocket, etc.)
- **For WebSocket: Standard Provider or Custom?** 
  - Choose from: Kraken, Binance Spot, Binance Futures, Alchemy, Infura
  - Or select "Custom" for manual configuration
- Is this live or historical data?
- How many metrics per record?
- What are the field names?
- Do you want data quality filtering?

It generates a ready-to-use config file with **pre-filled endpoints and ports** for standard providers.

### Step 2: Run Your Pipeline

```powershell
cargo run --release -- --config config/your_config.toml
```

### Step 3: Check Your Results

Look in the `out/` directory:
- `*_accepted.csv` - Clean, validated data
- `*_rejected.csv` - Bad data with rejection reasons

Each accepted row includes a `chain_hash` for tamper-evident verification.

## Pre-Configured Templates (New in 0.4.0)

Skip the wizard entirely with ready-to-use templates in `config/`:

**WebSocket Connectors:**
- `kraken_websocket.toml` - Kraken crypto exchange (pre-filled: wss://ws.kraken.com/v2)
- `binance_spot_websocket.toml` - Binance Spot markets (pre-filled: wss://stream.binance.com:9443/ws/)
- `binance_futures_websocket.toml` - Binance Futures (pre-filled: wss://fstream.binance.com/ws/)
- `alchemy_websocket.toml` - Ethereum RPC via Alchemy (set T3THR_ALCHEMY_KEY env var)
- `infura_websocket.toml` - Ethereum RPC via Infura (set T3THR_INFURA_KEY env var)

**Database CDC:**
- `postgres_cdc.toml` - PostgreSQL logical replication (pre-filled: port 5432)
- `mysql_cdc.toml` - MySQL binlog replication (pre-filled: port 3306)

**Infrastructure:**
- `syslog_server.toml` - Syslog server (pre-filled: port 514)
- `kafka_consumer.toml` - Kafka consumer (pre-filled: port 9092)
- `mqtt_consumer.toml` - MQTT consumer (pre-filled: port 1883)
- `grpc_client.toml` - gRPC client (pre-filled: port 50051)

All templates follow 12-Factor standards: secrets use environment variables, never hardcoded in config files.

## Example Use Cases

### IoT Sensor Data

**Input CSV:**
```csv
timestamp,temperature,humidity,pressure
2024-01-15 10:00:00,22.5,65.2,1013.25
2024-01-15 10:00:01,22.7,65.0,1013.30
```

**Config:** `config/v2_mqtt_example.toml`

**Run:**
```powershell
cargo run --release -- --config config/v2_mqtt_example.toml
```

### Business Inventory

**REST API polling** for inventory levels:

**Config:** `config/v2_rest_inventory.example.toml`

Edit the `url` field to point to your API endpoint.

### Custom WebSocket Stream

For any JSON WebSocket stream:

```toml
[connector.websocket]
url = "wss://your-stream.com/data"
provider = "custom"
field_paths = ["data.metric1", "data.metric2", "data.metric3"]
timestamp_path = "timestamp"
```

The bridge automatically extracts fields using simple JSONPath notation.

## Configuration Basics

All configs are TOML files with these sections:

### 1. Data Source (pick one)

**CSV File:**
```toml
[connector.csv]
input_path = "path/to/data.csv"
has_headers = true
```

**REST API:**
```toml
[connector.rest]
url = "https://api.example.com/data"
poll_interval_ms = 1000
```

**WebSocket:**
```toml
[connector.websocket]
url = "wss://stream.example.com"
provider = "custom"
field_paths = ["value1", "value2"]
```

**MQTT (IoT):**
```toml
[connector.message_bus]
provider = "mqtt"
broker = "localhost:1883"
topic = "sensors/data"
```

### 2. Field Mapping

Tell the bridge which columns contain your metrics:

```toml
[normalizer]
field_count = 3
timestamp_field = "timestamp"

[normalizer.field_map]
"temperature" = 0
"humidity" = 1
"pressure" = 2
```

### 3. Data Quality Rules (optional but recommended)

Set acceptable ranges:

```toml
[filter]
reject_nan_inf = true
replay_mode = false

[filter.bounds]
metric_0.min = -40.0
metric_0.max = 85.0
```

Detect sudden spikes:

```toml
[filter.spike_detection]
ema_alpha = 0.1
metric_0_max_delta = 10.0
```

### 4. Output

```toml
[output]
accepted_path = "out/accepted.csv"
dead_letter_path = "out/rejected.csv"
headers = ["temperature", "humidity", "pressure"]
# format = "csv" | "jsonl" (default: csv). Parquet only for file/batch modes.
```

For REST and Message Bus, use `field_paths` instead of `price_path`/`volume_path` for N-dimensional metrics. Legacy keys are normalized automatically with a deprecation warning.

## Validation & Help

**Validate your config before running:**
```powershell
cargo run --bin validate_config config/your_config.toml
```

**See all config options:**
```powershell
cargo run --release -- --explain
```

## Troubleshooting

### "Config file not found"
- Check the path is correct
- Use `--config` flag: `--config config/myfile.toml`

### "Field not found" error
- Verify field names match your CSV headers exactly
- Check `field_map` indices don't exceed `field_count`

### All rows rejected
- Check `out/*_rejected.csv` to see rejection reasons
- Adjust `filter.bounds` to match your data range
- Set `replay_mode = true` for historical data

### Output directory doesn't exist
```powershell
New-Item -ItemType Directory -Path out -Force
```

## Next Steps

1. **Read existing examples** in `config/` directory
2. **Customize for your data** - copy and modify an example config
3. **Add data quality rules** - set bounds and spike detection
4. **Automate** - schedule the bridge to run periodically

## Getting Help

- `README.md` - Full documentation
- `TERMINOLOGY.md` - Legacy field name migration guide
- `--explain` - Show all config options
- Config wizard - Interactive setup

## Working with GitHub Clone (Local Development)

If you want to contribute to or develop T3thr using the public repository:

### GitHub Clone Instructions
```bash
# Clone the public repository
git clone https://github.com/fors33-official/data-bridge.git

# Navigate to cloned directory
cd data-bridge

# Install Rust toolchain (if not already installed)
curl --proto '=https://sh.rustup.rs' | sh -s -- -y

# Build from source
cargo build --release --features full_engine

# Run locally
cargo run --release --features full_engine -- --config config/your_config.toml
```

### Local Development Setup
- **Environment Variables:** Set up `T3THR_*` variables for secrets
- **License Key:** Configure `FORS33_LICENSE_KEY` for live connectors
- **Config Management:** Copy/modify configs in cloned `config/` directory
- **Testing:** Run `cargo test` and `cargo run --bin validate_config`

### Contributing Changes
1. Make changes in your local cloned repository
2. Test thoroughly with `cargo build` and `cargo test`
3. Commit changes with clear messages
4. Push to your fork, then create Pull Request to `fors33-official/data-bridge`

### Docker vs Local Development
- **Docker Images:** Use for production/deployment (fors33/data-bridge)
- **Local Build:** Use for development, testing, and contributions
- **Both Supported:** Choose based on your workflow needs

## Key Concepts

**Data Point:** One row of data with a timestamp and metrics

**Metrics:** The numeric values you're tracking (temperature, price, count, etc.)

**Field Count:** How many metrics per record

**Filter:** Quality checks that reject bad data

**Replay Mode:** Skip timestamp checks for historical data

**Chain Hash:** Cryptographic proof of data integrity (tamper-evident)

---

**That's it!** You now have a working data pipeline with automatic quality filtering and cryptographic verification.

