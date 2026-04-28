//! Interactive config wizard for Fors33 T3thr
//!
//! Guides users through creating a valid configuration file. Secrets are never
//! written to disk: the wizard only asks for **environment variable names**
//! (must start with `T3THR_`) and writes them into `env_*` tables (direct mapping, no `${…}`).

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;

fn main() -> io::Result<()> {
    println!("\n=== Fors33 T3thr Configuration Wizard ===\n");
    println!("This wizard will help you create a configuration file for your data pipeline.\n");
    println!("For live connectors, secret values are **not** stored in the file.\n");
    println!("You will name environment variables (prefix T3THR_) and the config will use ${{VAR}} placeholders.\n");

    // 1. Data source type
    println!("1. What type of data source do you have?");
    println!("   a) CSV file (historical data, logs)");
    println!("   b) REST API (polling an HTTP endpoint)");
    println!("   c) WebSocket (live streaming data)");
    println!("   d) MQTT (IoT sensor data)");
    println!("   e) Kafka (message queue)");
    println!("   f) gRPC (streaming RPC)");
    let source_type = prompt_choice("Enter choice (a-f)", &['a', 'b', 'c', 'd', 'e', 'f'])?;

    // 1b. WebSocket provider selection (if applicable)
    let ws_provider = if source_type == 'c' {
        println!("\n1b. WebSocket provider selection:");
        println!("   a) Standard Provider (Kraken, Binance, Alchemy, Infura)");
        println!("   b) Custom Provider (manual configuration)");
        let provider_choice = prompt_choice("Enter choice (a-b)", &['a', 'b'])?;
        if provider_choice == 'a' {
            println!("\n   Select standard provider:");
            println!("   a) Kraken (crypto exchange)");
            println!("   b) Binance Spot (crypto exchange)");
            println!("   c) Binance Futures (crypto exchange)");
            println!("   d) Alchemy (Ethereum RPC)");
            println!("   e) Infura (Ethereum RPC)");
            let provider = prompt_choice("Enter choice (a-e)", &['a', 'b', 'c', 'd', 'e'])?;
            Some(match provider {
                'a' => "kraken",
                'b' => "binance_spot",
                'c' => "binance_futures",
                'd' => "alchemy",
                'e' => "infura",
                _ => "custom",
            })
        } else {
            None
        }
    } else {
        None
    };

    // 2. Live vs historical
    let is_live = if source_type == 'a' {
        println!("\n2. Is this historical data (replay) or live data?");
        println!("   a) Historical (audit logs, batch processing)");
        println!("   b) Live (real-time monitoring)");
        let mode = prompt_choice("Enter choice (a-b)", &['a', 'b'])?;
        mode == 'b'
    } else {
        true // REST/WebSocket/MQTT/Kafka/gRPC are always live from the connector's perspective
    };

    // 3. Data domain (guides examples only)
    println!("\n3. What domain does your data represent?");
    println!("   a) IoT/Sensors (temperature, humidity, pressure)");
    println!("   b) Business metrics (sales, inventory, costs)");
    println!("   c) API monitoring (latency, error rates, throughput)");
    println!("   d) Research/Science (experimental measurements)");
    println!("   e) Gaming (player stats, server metrics)");
    println!("   f) Finance/Trading (market data)");
    println!("   g) Other/Custom");
    let _domain = prompt_choice("Enter choice (a-g)", &['a', 'b', 'c', 'd', 'e', 'f', 'g'])?;

    // 4. Number of metrics
    println!("\n4. How many numeric values (metrics) does each record have?");
    println!("   Examples:");
    println!("   - Temperature + Humidity = 2 metrics");
    println!("   - Price + Volume + Spread = 3 metrics");
    let field_count: usize = prompt_number("Enter number of metrics", 1, 10)?;

    // 5. Field names and mappings
    let mut field_map = HashMap::new();
    let mut field_names = Vec::new();

    println!("\n5. Enter the names of your metric fields:");
    for i in 0..field_count {
        let name = prompt_string(&format!("   Metric {} name", i))?;
        field_names.push(name.clone());
        field_map.insert(name, i);
    }

    // 6. Timestamp field
    println!("\n6. Timestamp configuration:");
    let has_timestamp = prompt_yes_no("Does your data have a timestamp field?")?;
    let (timestamp_field, timestamp_format) = if has_timestamp {
        let ts_field = prompt_string("   Timestamp field name")?;
        println!("   Common formats:");
        println!("   a) ISO datetime (2024-01-15 10:00:00)");
        println!("   b) Unix milliseconds (1705315200000)");
        println!("   c) Unix seconds (1705315200)");
        let ts_type = prompt_choice("   Format type (a-c)", &['a', 'b', 'c'])?;
        let format = match ts_type {
            'a' => "%Y-%m-%d %H:%M:%S".to_string(),
            'b' => "ms".to_string(),
            'c' => "s".to_string(),
            _ => "%Y-%m-%d %H:%M:%S".to_string(),
        };
        (Some(ts_field), Some(format))
    } else {
        println!("   Using current system time for each record.");
        (None, None)
    };

    // 7. Data quality filters
    println!("\n7. Data quality filtering:");
    let filter_bounds = prompt_yes_no("Do you want to set min/max bounds for your metrics?")?;
    let mut bounds = Vec::new();
    if filter_bounds {
        for (i, name) in field_names.iter().enumerate() {
            println!("   Metric {}: {}", i, name);
            let min = prompt_float("     Minimum value (or press Enter for no limit)")?;
            let max = prompt_float("     Maximum value (or press Enter for no limit)")?;
            bounds.push((i, min, max));
        }
    }

    let spike_detection = prompt_yes_no("\nEnable spike/anomaly detection?")?;

    // 7b. Credential environment variable names (never plaintext secrets)
    let cred = collect_credential_placeholders(source_type)?;

    // 8. Output configuration
    println!("\n8. Output configuration:");
    let output_name = prompt_string("Output file prefix (e.g., 'sensor_data', 'api_metrics')")?;

    // 9. Generate config
    let config_name = match source_type {
        'a' => format!("{}_csv.toml", output_name),
        'b' => format!("{}_rest.toml", output_name),
        'c' => format!("{}_websocket.toml", output_name),
        'd' => format!("{}_mqtt.toml", output_name),
        'e' => format!("{}_kafka.toml", output_name),
        'f' => format!("{}_grpc.toml", output_name),
        _ => format!("{}.toml", output_name),
    };

    println!("\n=== Generated Configuration ===\n");

    let config_content = generate_config(
        source_type,
        ws_provider.as_deref(),
        &output_name,
        field_count,
        &field_map,
        &field_names,
        timestamp_field.as_deref(),
        timestamp_format.as_deref(),
        &bounds,
        spike_detection,
        !is_live, // replay_mode = !is_live
        &cred,
    );

    println!("{}", config_content);

    println!("\n=== Next Steps ===");
    println!("1. Save this config to: config/{}", config_name);
    println!("2. Set the referenced T3THR_* variables in your process or container environment.");
    println!("3. Validate: cargo run --release --bin validate_config -- config/{}", config_name);
    println!("4. Run: cargo run --release --features full_engine -- --config config/{}", config_name);

    let save = prompt_yes_no("\nSave this config now?")?;
    if save {
        let config_path = PathBuf::from("config").join(&config_name);
        std::fs::write(&config_path, config_content)?;
        println!("✓ Saved to {}", config_path.display());
    }

    Ok(())
}

struct CredFragments {
    rest_headers: String,
    ws_headers: String,
    grpc_metadata: String,
    kafka_client_props: String,
    mqtt_client_props: String,
}

impl Default for CredFragments {
    fn default() -> Self {
        Self {
            rest_headers: String::new(),
            ws_headers: String::new(),
            grpc_metadata: String::new(),
            kafka_client_props: String::new(),
            mqtt_client_props: String::new(),
        }
    }
}

fn collect_credential_placeholders(source_type: char) -> io::Result<CredFragments> {
    let mut c = CredFragments::default();
    match source_type {
        'b' => {
            if prompt_yes_no(
                "\nWill the REST connector send a secret in an HTTP header (e.g. Authorization, x-api-key)?",
            )? {
                let var = prompt_t3thr_env_key(
                    "Define the environment variable name for the REST header value (e.g. T3THR_REST_TOKEN). \
                     Put the full header value in that variable at runtime (for example \"Bearer <token>\" if you use Bearer auth):",
                )?;
                let header_name = prompt_string(
                    "HTTP header name to send (press Enter for Authorization):",
                )?;
                let h = if header_name.trim().is_empty() {
                    "Authorization".to_string()
                } else {
                    header_name.trim().to_string()
                };
                c.rest_headers = format!(
                    "\n[connector.rest.env_headers]\n{} = \"{}\"\n",
                    toml_key(&h),
                    var
                );
            }
        }
        'c' => {
            if prompt_yes_no(
                "\nWill the WebSocket handshake include a secret header (e.g. Authorization)?",
            )? {
                let var = prompt_t3thr_env_key(
                    "Define the environment variable name for the WebSocket handshake header value (e.g. T3THR_WEBSOCKET_TOKEN):",
                )?;
                let header_name = prompt_string(
                    "Handshake header name (press Enter for Authorization):",
                )?;
                let h = if header_name.trim().is_empty() {
                    "Authorization".to_string()
                } else {
                    header_name.trim().to_string()
                };
                c.ws_headers = format!(
                    "\n[connector.websocket.env_headers]\n{} = \"{}\"\n",
                    toml_key(&h),
                    var
                );
            }
        }
        'e' => {
            if prompt_yes_no(
                "\nWill Kafka use SASL (e.g. PLAIN) with a password supplied from the environment?",
            )? {
                let var = prompt_t3thr_env_key(
                    "Define the environment variable name for the Kafka SASL password (e.g. T3THR_KAFKA_SASL_PASSWORD):",
                )?;
                c.kafka_client_props = format!(
                    "\n[connector.message_bus.client_properties]\n\
                     \"security.protocol\" = \"SASL_SSL\"\n\
                     \"sasl.mechanism\" = \"PLAIN\"\n\
                     \"sasl.username\" = \"replace_with_public_username_or_literal\"\n\
                     [connector.message_bus.env_client_properties]\n\
                     \"sasl.password\" = \"{}\"\n",
                    var
                );
            }
        }
        'd' => {
            if prompt_yes_no("\nDoes MQTT authentication use a password from the environment?")? {
                let var = prompt_t3thr_env_key(
                    "Define the environment variable name for the MQTT password (e.g. T3THR_MQTT_PASSWORD):",
                )?;
                c.mqtt_client_props = format!(
                    "\n[connector.message_bus.env_client_properties]\n\
                     \"password\" = \"{}\"\n",
                    var
                );
            }
        }
        'f' => {
            if prompt_yes_no(
                "\nWill gRPC calls attach a secret metadata value (e.g. authorization bearer or API key)?",
            )? {
                let meta_key = prompt_string(
                    "gRPC metadata key to send (press Enter for authorization):",
                )?;
                let k = if meta_key.trim().is_empty() {
                    "authorization".to_string()
                } else {
                    meta_key.trim().to_string()
                };
                let var = prompt_t3thr_env_key(
                    "Define the environment variable name for that metadata value (e.g. T3THR_GRPC_METADATA_SECRET):",
                )?;
                c.grpc_metadata = format!(
                    "\n[connector.grpc.env_metadata]\n{} = \"{}\"\n",
                    toml_key(&k),
                    var
                );
            }
        }
        _ => {}
    }
    Ok(c)
}

/// Quote TOML keys if needed.
fn toml_key(s: &str) -> String {
    if s.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        s.to_string()
    } else {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    }
}

fn is_valid_t3thr_env_name(s: &str) -> bool {
    let Some(rest) = s.strip_prefix("T3THR_") else {
        return false;
    };
    if rest.is_empty() {
        return false;
    }
    rest.chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

fn prompt_t3thr_env_key(prompt: &str) -> io::Result<String> {
    loop {
        print!("{} ", prompt);
        println!("(Must match ^T3THR_[A-Z0-9_]+$):");
        print!("> ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let s = input.trim().to_string();
        if is_valid_t3thr_env_name(&s) {
            return Ok(s);
        }
        println!("Invalid name. Use only A-Z, 0-9, and underscore after the T3THR_ prefix (e.g. T3THR_REST_TOKEN).");
    }
}

fn prompt_choice(prompt: &str, choices: &[char]) -> io::Result<char> {
    loop {
        print!("{}: ", prompt);
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();
        if input.len() == 1 {
            let ch = input.chars().next().unwrap();
            if choices.contains(&ch) {
                return Ok(ch);
            }
        }
        println!("Invalid choice. Please enter one of: {:?}", choices);
    }
}

fn prompt_yes_no(prompt: &str) -> io::Result<bool> {
    print!("{} (y/n): ", prompt);
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_lowercase().starts_with('y'))
}

fn prompt_string(prompt: &str) -> io::Result<String> {
    print!("{} ", prompt);
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

fn prompt_number(prompt: &str, min: usize, max: usize) -> io::Result<usize> {
    loop {
        print!("{} ({}-{}): ", prompt, min, max);
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if let Ok(num) = input.trim().parse::<usize>() {
            if num >= min && num <= max {
                return Ok(num);
            }
        }
        println!("Please enter a number between {} and {}", min, max);
    }
}

fn prompt_float(prompt: &str) -> io::Result<Option<f64>> {
    print!("{}: ", prompt);
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();
    if input.is_empty() {
        Ok(None)
    } else if let Ok(num) = input.parse::<f64>() {
        Ok(Some(num))
    } else {
        println!("Invalid number, using no limit");
        Ok(None)
    }
}

fn json_array_string(paths: &[String]) -> String {
    let inner: Vec<String> = paths
        .iter()
        .map(|p| serde_json::to_string(p).unwrap_or_else(|_| format!("\"{}\"", p)))
        .collect();
    format!("[{}]", inner.join(", "))
}

fn generate_config(
    source_type: char,
    ws_provider: Option<&str>,
    output_name: &str,
    field_count: usize,
    field_map: &HashMap<String, usize>,
    field_names: &[String],
    timestamp_field: Option<&str>,
    timestamp_format: Option<&str>,
    bounds: &[(usize, Option<f64>, Option<f64>)],
    spike_detection: bool,
    replay_mode: bool,
    cred: &CredFragments,
) -> String {
    let mut config = String::new();

    // Connector section
    match source_type {
        'a' => {
            config.push_str("[connector.csv]\n");
            config.push_str("input_path = \"path/to/your_data.csv\"\n");
            config.push_str("has_headers = true\n");
        }
        'b' => {
            let fp = json_array_string(field_names);
            config.push_str("[connector.rest]\n");
            config.push_str("url = \"https://api.example.com/data\"\n");
            config.push_str("poll_interval_ms = 1000\n");
            config.push_str(&format!("field_paths = {}\n", fp));
            config.push_str("# Legacy fallback: if field_paths is omitted, defaults to [\"price\", \"volume\"]\n");
            config.push_str(&cred.rest_headers);
        }
        'c' => {
            config.push_str("[connector.websocket]\n");
            match ws_provider {
                Some("kraken") => {
                    config.push_str("url = \"wss://ws.kraken.com/v2\"\n");
                    config.push_str("provider = \"kraken\"\n");
                    // Kraken has hardcoded parsing, no field_paths needed
                    config.push_str("# Kraken uses built-in parsing - field_paths not required\n");
                }
                Some("binance_spot") => {
                    config.push_str("url = \"wss://stream.binance.com:9443/ws/\"\n");
                    config.push_str("provider = \"binance\"\n");
                    config.push_str("stream = \"btcusdt@trade\"  # Replace with your symbol\n");
                    config.push_str("# Binance uses built-in parsing - field_paths not required\n");
                }
                Some("binance_futures") => {
                    config.push_str("url = \"wss://fstream.binance.com/ws/\"\n");
                    config.push_str("provider = \"binance\"\n");
                    config.push_str("stream = \"btcusdt@trade\"  # Replace with your symbol\n");
                    config.push_str("# Binance Futures uses built-in parsing - field_paths not required\n");
                }
                Some("alchemy") => {
                    config.push_str("url = \"wss://eth-mainnet.g.alchemy.com/v2/${T3THR_ALCHEMY_KEY}\"\n");
                    config.push_str("provider = \"alchemy\"\n");
                    config.push_str("# Set T3THR_ALCHEMY_KEY environment variable with your Alchemy API key\n");
                    let fp = json_array_string(field_names);
                    config.push_str(&format!("field_paths = {}\n", fp));
                }
                Some("infura") => {
                    config.push_str("url = \"wss://mainnet.infura.io/ws/v3/${T3THR_INFURA_KEY}\"\n");
                    config.push_str("provider = \"infura\"\n");
                    config.push_str("# Set T3THR_INFURA_KEY environment variable with your Infura API key\n");
                    let fp = json_array_string(field_names);
                    config.push_str(&format!("field_paths = {}\n", fp));
                }
                _ => {
                    // Custom provider
                    config.push_str("url = \"wss://stream.example.com\"\n");
                    config.push_str("provider = \"custom\"\n");
                    let fp = json_array_string(field_names);
                    config.push_str(&format!("field_paths = {}\n", fp));
                }
            }
            config.push_str(&cred.ws_headers);
        }
        'd' => {
            let fp = json_array_string(field_names);
            config.push_str("[connector.message_bus]\n");
            config.push_str("provider = \"mqtt\"\n");
            config.push_str("broker = \"localhost:1883\"  # Default MQTT port\n");
            config.push_str("topic = \"sensors/data\"\n");
            config.push_str(&format!("field_paths = {}\n", fp));
            config.push_str("# Legacy fallback: if field_paths is omitted, defaults to [\"price\", \"volume\"]\n");
            config.push_str(&cred.mqtt_client_props);
        }
        'e' => {
            let fp = json_array_string(field_names);
            config.push_str("[connector.message_bus]\n");
            config.push_str("provider = \"kafka\"\n");
            config.push_str("bootstrap_servers = \"localhost:9092\"  # Default Kafka port\n");
            config.push_str("topic = \"data-stream\"\n");
            config.push_str("group_id = \"data_bridge\"\n");
            config.push_str(&format!("field_paths = {}\n", fp));
            config.push_str("# Legacy fallback: if field_paths is omitted, defaults to [\"price\", \"volume\"]\n");
            config.push_str(&cred.kafka_client_props);
        }
        'f' => {
            config.push_str("[connector.grpc]\n");
            config.push_str("url = \"https://grpc.example.com:50051\"  # Default gRPC port\n");
            config.push_str("service = \"datastream.DataStream\"\n");
            config.push_str("# Metrics come from the gRPC stream (proto); align [output.headers] with server metric order.\n");
            config.push_str(&cred.grpc_metadata);
        }
        _ => {}
    }

    config.push_str("\n[normalizer]\n");
    config.push_str(&format!("field_count = {}\n", field_count));

    if let Some(ts_field) = timestamp_field {
        config.push_str(&format!("timestamp_field = \"{}\"\n", ts_field));
        if let Some(ts_format) = timestamp_format {
            if ts_format != "ms" && ts_format != "s" {
                config.push_str(&format!("timestamp_format = \"{}\"\n", ts_format));
            } else {
                config.push_str(&format!("timestamp_unit = \"{}\"\n", ts_format));
            }
        }
    }

    config.push_str("\n[normalizer.field_map]\n");
    for (name, idx) in field_map {
        config.push_str(&format!("\"{}\" = {}\n", name, idx));
    }

    config.push_str("\n[filter]\n");
    config.push_str("reject_nan_inf = true\n");
    config.push_str(&format!("replay_mode = {}\n", replay_mode));
    config.push_str("drop_on_parse_error = true\n");
    config.push_str("fail_fast = true\n");
    config.push_str("future_tolerance_ms = 60000\n");
    config.push_str("stale_tolerance_ms = 300000\n");

    if !bounds.is_empty() {
        config.push_str("\n[filter.bounds]\n");
        for &(idx, min, max) in bounds {
            if let Some(min_val) = min {
                config.push_str(&format!("metric_{}.min = {}\n", idx, min_val));
            }
            if let Some(max_val) = max {
                config.push_str(&format!("metric_{}.max = {}\n", idx, max_val));
            }
        }
    }

    if spike_detection {
        config.push_str("\n[filter.spike_detection]\n");
        config.push_str("ema_alpha = 0.1\n");
        for i in 0..field_count {
            config.push_str(&format!(
                "metric_{}_max_delta = 100.0  # Adjust threshold\n",
                i
            ));
        }
    }

    config.push_str("\n[output]\n");
    config.push_str(&format!("accepted_path = \"out/{}_accepted.csv\"\n", output_name));
    config.push_str(&format!(
        "dead_letter_path = \"out/{}_dead.csv\"\n",
        output_name
    ));

    config.push_str("\n[output.headers]\n");
    config.push_str("headers = [");
    for (i, name) in field_names.iter().enumerate() {
        if i > 0 {
            config.push_str(", ");
        }
        config.push_str(&format!("\"{}\"", name));
    }
    config.push_str("]\n");

    config
}
