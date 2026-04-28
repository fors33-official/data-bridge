//! CDC Postgres connector using logical replication.
//! Streams changes from a PostgreSQL publication using logical decoding.

use std::sync::mpsc::SyncSender;

use anyhow::{anyhow, Result};
use serde::Deserialize;
use tokio_postgres::Config;
use tokio_postgres::types::PgLsn;

use crate::{DataPoint, FilterCfg, FilterState};

/// Environment variable name for password (optional, env_password takes precedence)
pub const ENV_PASSWORD_SUFFIX: &str = "T3THR_POSTGRES_PASSWORD";

#[derive(Debug, Deserialize, Clone)]
pub struct CdcPostgresCfg {
    pub host: String,
    #[serde(default)]
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: Option<String>, // Plaintext (not recommended)
    pub env_password: Option<String>, // Environment variable name (e.g., "T3THR_POSTGRES_PASSWORD")
    pub publication: String,
    pub slot_name: String,
}

impl CdcPostgresCfg {
    /// Get password from environment or config
    pub fn resolve_password(&self) -> Result<String> {
        // First try env_password if specified
        if let Some(env_var) = &self.env_password {
            if !env_var.starts_with("T3THR_") {
                return Err(anyhow!(
                    "environment variable name must start with T3THR_ (got `{}`)",
                    env_var
                ));
            }
            let password = std::env::var(env_var)
                .map_err(|_| anyhow!("environment variable `{}` not set", env_var))?;
            if password.trim().is_empty() {
                return Err(anyhow!(
                    "environment variable `{}` is empty after trim",
                    env_var
                ));
            }
            return Ok(password.trim().to_string());
        }

        // Fall back to plaintext password
        if let Some(password) = &self.password {
            if password.trim().is_empty() {
                return Err(anyhow!("password cannot be empty"));
            }
            return Ok(password.trim().to_string());
        }

        // Try default environment variable
        if let Ok(password) = std::env::var(ENV_PASSWORD_SUFFIX) {
            if !password.trim().is_empty() {
                return Ok(password.trim().to_string());
            }
        }

        Err(anyhow!(
            "no password provided. Set env_password, password, or {} environment variable",
            ENV_PASSWORD_SUFFIX
        ))
    }

    /// Build connection config
    pub fn build_config(&self) -> Result<Config> {
        let password = self.resolve_password()?;

        let mut config = Config::new();
        config.host(&self.host);
        if self.port > 0 {
            config.port(self.port);
        }
        config.dbname(&self.database);
        config.user(&self.username);
        config.password(&password);

        // Note: tokio-postgres replication configuration requires specific API
        // Full implementation would set replication mode here
        Ok(config)
    }
}

pub async fn run_cdc_postgres_mode(
    cfg: &CdcPostgresCfg,
    _tx: SyncSender<Result<DataPoint, (String, String)>>,
    _filter_cfg: &FilterCfg,
) -> Result<()> {
    let config = cfg.build_config()?;

    eprintln!(
        "[Fors33] CDC Postgres connecting to {}:{}/{}",
        cfg.host,
        if cfg.port > 0 { cfg.port } else { 5432 },
        cfg.database
    );

    // Connect to database
    let (client, connection) = config.connect(tokio_postgres::NoTls).await?;

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("[Fors33] CDC Postgres connection error: {}", e);
        }
    });

    // Verify publication exists
    let pub_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&cfg.publication],
        )
        .await
        .map(|row| row.get(0))?;

    if !pub_exists {
        return Err(anyhow!(
            "publication '{}' does not exist. Create it with: CREATE PUBLICATION {} FOR ALL TABLES;",
            cfg.publication,
            cfg.publication
        ));
    }

    // Create replication slot if it doesn't exist
    let slot_exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&cfg.slot_name],
        )
        .await
        .map(|row| row.get(0))?;

    let start_lsn = if slot_exists {
        // Get confirmed flush LSN from existing slot
        let confirmed_lsn: Option<PgLsn> = client
            .query_one(
                "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1",
                &[&cfg.slot_name],
            )
            .await
            .ok()
            .map(|row| row.get(0));
        confirmed_lsn
    } else {
        // Create new slot
        eprintln!("[Fors33] Creating replication slot: {}", cfg.slot_name);
        let slot_result: (PgLsn, String) = client
            .query_one(
                &format!(
                    "SELECT * FROM pg_create_logical_replication_slot('{}', 'pgoutput')",
                    cfg.slot_name
                ),
                &[],
            )
            .await
            .map(|row| (row.get(0), row.get(1)))?;
        Some(slot_result.0)
    };

    // Start logical replication
    let slot_name = cfg.slot_name.clone();
    let start_lsn_str = start_lsn
        .map(|lsn| lsn.to_string())
        .unwrap_or_else(|| "0/0".to_string());

    eprintln!(
        "[Fors33] Starting logical replication from slot {} at LSN {}",
        slot_name, start_lsn_str
    );

    let query = format!(
        "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
        slot_name, start_lsn_str, cfg.publication
    );

    // Create replication stream
    let _stream = client
        .copy_out(&query)
        .await
        .map_err(|e| anyhow!("failed to start replication: {}", e))?;

    let _filter_state = FilterState::with_capacity(2); // CDC typically produces 2 metrics (operation type + value)

    // Process replication stream
    // Note: LogicalReplicationStream would need proper parsing of pgoutput protocol
    // For now, implement a simplified version that processes raw changes

    // In a full implementation, we would:
    // 1. Parse the pgoutput protocol messages
    // 2. Extract relation info, tuple data, etc.
    // 3. Convert to DataPoint format
    // 4. Send to channel

    // Simplified implementation: poll for changes
    loop {
        // This is a placeholder for the actual replication stream processing
        // The real implementation would use LogicalReplicationStream from tokio-postgres

        // For now, emit a log message every 30 seconds to indicate activity
        // In production, this would be replaced with actual WAL processing
        eprintln!("[Fors33] CDC Postgres replication active (simplified mode)");
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

        // TODO: Implement full pgoutput protocol parsing
        // This requires handling:
        // - Begin/Commit messages
        // - Relation messages (table schema)
        // - Insert/Update/Delete messages with tuple data
        // - Keepalive messages
    }
}
