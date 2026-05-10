//! `t3thr migrate` subcommand: rewrite a legacy 2-field config into the
//! current N-field schema. Behavior is preserved verbatim from the legacy
//! `src/bin/migrate_config.rs` binary; only the entry-point shape changes
//! (no interactive prompts, just `--input`/`--output`/`--dry-run` flags).

use std::fs;
use std::path::PathBuf;

use anyhow::{Result, anyhow};

use super::MigrateArgs;

pub fn execute(args: &MigrateArgs) -> Result<()> {
    println!("Migrating config: {}", args.input.display());

    let config_text =
        fs::read_to_string(&args.input).map_err(|e| anyhow!("Cannot read config file: {}", e))?;
    let mut config: toml::Value =
        toml::from_str(&config_text).map_err(|e| anyhow!("Invalid TOML syntax: {}", e))?;

    let mut changes: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    let has_legacy_fields = config
        .get("normalizer")
        .and_then(|n| n.get("price_field"))
        .is_some()
        || config
            .get("normalizer")
            .and_then(|n| n.get("volume_field"))
            .is_some();

    if !has_legacy_fields {
        println!("Config already uses N-field format. No migration needed.");
        return Ok(());
    }

    if let Some(normalizer) = config.get_mut("normalizer").and_then(|n| n.as_table_mut()) {
        let price_field = normalizer
            .get("price_field")
            .and_then(|v| v.as_str())
            .map(String::from);
        let volume_field = normalizer
            .get("volume_field")
            .and_then(|v| v.as_str())
            .map(String::from);

        if let (Some(price), Some(volume)) = (price_field, volume_field) {
            normalizer.insert("field_count".to_string(), toml::Value::Integer(2));
            changes.push("Added field_count = 2".to_string());

            let mut field_map = toml::map::Map::new();
            field_map.insert(price.clone(), toml::Value::Integer(0));
            field_map.insert(volume.clone(), toml::Value::Integer(1));
            normalizer.insert("field_map".to_string(), toml::Value::Table(field_map));
            changes.push(format!(
                "Created field_map: \"{}\" = 0, \"{}\" = 1",
                price, volume
            ));

            normalizer.remove("price_field");
            normalizer.remove("volume_field");
            changes.push("Removed price_field and volume_field".to_string());
            warnings.push(
                "Legacy price_field/volume_field removed. Original values preserved in field_map."
                    .to_string(),
            );
        }
    }

    if let Some(filter) = config.get_mut("filter").and_then(|f| f.as_table_mut()) {
        let price_min = filter.get("price_min").and_then(|v| v.as_float());
        let price_max = filter.get("price_max").and_then(|v| v.as_float());
        let volume_min = filter.get("volume_min").and_then(|v| v.as_float());
        let volume_max = filter.get("volume_max").and_then(|v| v.as_float());
        let volume_burst = filter
            .get("volume_burst_max_ratio")
            .and_then(|v| v.as_float());
        let burst_alpha = filter.get("burst_ema_alpha").and_then(|v| v.as_float());

        let mut bounds_table = toml::map::Map::new();
        let mut metric_0 = toml::map::Map::new();
        let mut metric_1 = toml::map::Map::new();

        if let Some(min) = price_min {
            metric_0.insert("min".to_string(), toml::Value::Float(min));
            changes.push(format!("Migrated price_min to metric_0.min = {}", min));
        }
        if let Some(max) = price_max {
            metric_0.insert("max".to_string(), toml::Value::Float(max));
            changes.push(format!("Migrated price_max to metric_0.max = {}", max));
        }
        if let Some(min) = volume_min {
            metric_1.insert("min".to_string(), toml::Value::Float(min));
            changes.push(format!("Migrated volume_min to metric_1.min = {}", min));
        }
        if let Some(max) = volume_max {
            metric_1.insert("max".to_string(), toml::Value::Float(max));
            changes.push(format!("Migrated volume_max to metric_1.max = {}", max));
        }

        if !metric_0.is_empty() {
            bounds_table.insert("metric_0".to_string(), toml::Value::Table(metric_0));
        }
        if !metric_1.is_empty() {
            bounds_table.insert("metric_1".to_string(), toml::Value::Table(metric_1));
        }
        if !bounds_table.is_empty() {
            filter.insert("bounds".to_string(), toml::Value::Table(bounds_table));
        }

        filter.remove("price_min");
        filter.remove("price_max");
        filter.remove("volume_min");
        filter.remove("volume_max");

        if volume_burst.is_some() || burst_alpha.is_some() {
            let mut spike_table = toml::map::Map::new();
            if let Some(alpha) = burst_alpha {
                spike_table.insert("ema_alpha".to_string(), toml::Value::Float(alpha));
                changes.push(format!(
                    "Migrated burst_ema_alpha to spike_detection.ema_alpha = {}",
                    alpha
                ));
            }
            if let Some(burst) = volume_burst {
                spike_table.insert(
                    "metric_1_max_delta".to_string(),
                    toml::Value::Float(burst * 100.0),
                );
                changes.push(
                    "Migrated volume_burst_max_ratio to spike_detection.metric_1_max_delta"
                        .to_string(),
                );
                warnings.push("Note: volume_burst_max_ratio converted to spike_detection.metric_1_max_delta. You may need to adjust the threshold.".to_string());
            }
            filter.insert(
                "spike_detection".to_string(),
                toml::Value::Table(spike_table),
            );
            filter.remove("volume_burst_max_ratio");
            filter.remove("burst_ema_alpha");
        }
    }

    if let Some(output) = config.get_mut("output").and_then(|o| o.as_table_mut()) {
        if !output.contains_key("headers") {
            let headers = vec![
                toml::Value::String("price".to_string()),
                toml::Value::String("volume".to_string()),
            ];
            output.insert("headers".to_string(), toml::Value::Array(headers));
            changes.push("Added default headers for legacy 2-field mode".to_string());
        }
    }

    println!("\n=== Migration Summary ===\n");
    if changes.is_empty() {
        println!("No changes needed.");
        return Ok(());
    }
    println!("Changes ({}):", changes.len());
    for (i, c) in changes.iter().enumerate() {
        println!("  {}. {}", i + 1, c);
    }
    if !warnings.is_empty() {
        println!("\nWarnings ({}):", warnings.len());
        for (i, w) in warnings.iter().enumerate() {
            println!("  {}. {}", i + 1, w);
        }
    }

    let migrated_text =
        toml::to_string_pretty(&config).map_err(|e| anyhow!("Failed to serialize TOML: {}", e))?;

    if args.dry_run {
        println!("\n=== Migrated Config (dry run) ===\n");
        println!("{}", migrated_text);
        println!("\nRun without --dry-run to save changes.");
        return Ok(());
    }

    let output_path = args.output.clone().unwrap_or_else(|| {
        let mut path: PathBuf = args.input.clone();
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "config".to_string());
        path.set_file_name(format!("{}_migrated.toml", stem));
        path
    });

    fs::write(&output_path, migrated_text)
        .map_err(|e| anyhow!("Failed to write output file: {}", e))?;
    println!("\nMigrated config saved to: {}", output_path.display());
    Ok(())
}
