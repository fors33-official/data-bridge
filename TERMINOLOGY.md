# T3thr configuration terminology

Migration guide for legacy T3thr config field names. Prefer **`t3thr migrate`** for automated updates:

```bash
t3thr migrate config/legacy.toml --output config/migrated.toml
```

## Normalizer: price/volume to N-dimensional metrics

**Legacy**

```toml
[normalizer]
price_field = "price"
volume_field = "volume"
```

**Current**

```toml
[normalizer]
field_count = 2
field_map = { price = 0, volume = 1 }
```

Or use `field_paths` on connectors where documented.

## REST connector paths

**Legacy**

```toml
[connector.rest]
price_path = "data.price"
volume_path = "data.volume"
```

**Current**

```toml
[connector.rest]
field_paths = ["data.price", "data.volume"]
```

## Message bus paths

Same pattern as REST: `price_path` / `volume_path` to `field_paths`.

## Deprecation warnings

When legacy keys are present, T3thr emits a single `[DEPRECATION]` warning at startup and normalizes values in memory. Update your TOML before the next major release line.
