# T3thr bridge release notes – 0.6.0

Standalone **T3thr** transport parity release. Extension-agnostic: no daemon spawn, sealing, or attestation logic in this crate.

## Batch execution caps

- `[execution]` table: `max_records`, `max_duration_sec`, `max_pages`
- `[FORS33] batch complete reason=…` on stderr when a cap is reached
- Batch vs stream writer disconnect behavior in REST and WebSocket connectors
- CSV and file connector record loops honor caps when `[execution] mode = "batch"`

## Multi-feed accepted outputs

- `accepted_path_by_feed` and `accepted_prefix_by_feed` for WebSocket multi-channel splits
- `channel_scoped_accepted` omits `feed` in JSONL when one file per channel
- Jsonl `write_accepted` emits `feed` when set and not channel-scoped

## WebSocket providers

- **binance_futures** and **binance_ws_api** built-in providers
- HMAC-SHA256 signing for Binance WebSocket API user-data flows
- Multi-channel Kraken subscribe; optional `feed` field on accepted JSONL rows

## Postgres CDC

- **wal2json** and **pgoutput** slot plugins via `pg_logical_slot_get_changes` / `get_binary_changes`
- Optional `resume_lsn` in config for operator-controlled resume

## Build, packaging, and release

- `native_certs` optional feature; slim default build uses webpki-roots only
- Shipped `config/default.toml` and `examples/sample_input.csv` for Docker quick start
- Runtime image: non-root `bridge` user, writable `/app/out`, bundled config permissions
- Release workflow: Cargo.toml version gate, Docker validate-before-push, Docker Hub README sync
- `TERMINOLOGY.md` migration guide for legacy field names
