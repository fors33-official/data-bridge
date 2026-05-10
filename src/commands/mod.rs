//! Subcommand dispatch for the unified `t3thr` Swiss-Army CLI.
//!
//! Top-level shape:
//!
//! ```text
//! t3thr run      --config <path>   [--explain] [--reset-state] [--no-state]
//! t3thr generate --connector <slug> [--out <path>]
//! t3thr validate --config <path>
//! t3thr migrate  <input> [--output <path>] [--dry-run]
//! ```
//!
//! Backward compatibility: a bare `t3thr --config X` invocation is rewritten
//! before clap parsing into `t3thr run --config X` by `dispatch_argv`. Every
//! existing CI script and Dockerfile that spawns `t3thr` with raw flags keeps
//! working; new tooling should use explicit subcommands.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

pub mod generate;
pub mod migrate;
pub mod run;
pub mod validate;
pub mod wizard;

#[derive(Debug, Parser)]
#[command(name = "t3thr")]
#[command(about = "FORS33 T3THR - Config-driven time-series processor")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a configured pipeline (default behavior).
    Run(RunArgs),
    /// Emit a frictionless TOML template for a connector type.
    Generate(GenerateArgs),
    /// Validate a config file without executing it.
    Validate(ValidateArgs),
    /// Migrate a legacy config to the current schema.
    Migrate(MigrateArgs),
    /// Interactive configuration wizard (`config_wizard` binary).
    Wizard,
}

#[derive(Debug, Parser, Clone)]
pub struct RunArgs {
    /// Path to TOML config
    #[arg(long, default_value = "config/default.toml")]
    pub config: PathBuf,

    /// Explain config options and exit (prints documentation for config file)
    #[arg(long)]
    pub explain: bool,

    /// Reset state file and start fresh (for batch mode)
    #[arg(long)]
    pub reset_state: bool,

    /// Disable state tracking (for batch mode)
    #[arg(long)]
    pub no_state: bool,

    /// Parse config, resolve env bindings, validate license when applicable, and exit without running.
    #[arg(long)]
    pub validate_only: bool,

    /// Launch the interactive config wizard (same as the `config_wizard` binary).
    #[arg(long)]
    pub config_wizard: bool,
}

#[derive(Debug, Parser, Clone)]
pub struct GenerateArgs {
    /// Connector slug to scaffold (e.g. kraken-websocket, postgres-cdc).
    #[arg(long)]
    pub connector: String,
    /// Optional output path; defaults to stdout when omitted.
    #[arg(long)]
    pub out: Option<PathBuf>,
}

#[derive(Debug, Parser, Clone)]
pub struct ValidateArgs {
    /// Path to TOML config to validate.
    #[arg(long)]
    pub config: PathBuf,
}

#[derive(Debug, Parser, Clone)]
pub struct MigrateArgs {
    /// Path to legacy config file.
    pub input: PathBuf,
    /// Path for migrated config (optional, defaults to <input>_migrated.toml).
    #[arg(short, long)]
    pub output: Option<PathBuf>,
    /// Show changes without writing.
    #[arg(long)]
    pub dry_run: bool,
}

/// Pre-parse `argv` and inject `"run"` at index 1 when the user supplied
/// only legacy flags (i.e. the second token starts with `-` or `--`). This
/// preserves backward compatibility for `t3thr --config X` without relying
/// on clap's brittle "default subcommand" macro behavior.
pub fn argv_with_default_run<I, S>(argv: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut v: Vec<String> = argv.into_iter().map(Into::into).collect();
    if v.len() < 2 {
        return v;
    }
    let known: &[&str] = &[
        "run",
        "generate",
        "validate",
        "migrate",
        "wizard",
        "help",
        "--help",
        "-h",
        "--version",
        "-V",
    ];
    let arg1 = v[1].as_str();
    if known.iter().any(|k| *k == arg1) {
        return v;
    }
    if arg1.starts_with('-') {
        v.insert(1, "run".to_string());
    }
    v
}

/// Dispatch the parsed CLI to the matching subcommand handler.
pub fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Run(args) => run::execute(&args),
        Command::Generate(args) => generate::execute(&args),
        Command::Validate(args) => validate::execute(&args),
        Command::Migrate(args) => migrate::execute(&args),
        Command::Wizard => wizard::execute(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argv_inject_when_first_arg_is_flag() {
        let v = argv_with_default_run(["t3thr", "--config", "x.toml"]);
        assert_eq!(v, vec!["t3thr", "run", "--config", "x.toml"]);
    }

    #[test]
    fn argv_no_inject_when_first_arg_is_subcommand() {
        let v = argv_with_default_run(["t3thr", "generate", "--connector", "kraken-websocket"]);
        assert_eq!(
            v,
            vec!["t3thr", "generate", "--connector", "kraken-websocket"]
        );
    }

    #[test]
    fn argv_no_inject_when_first_arg_is_run() {
        let v = argv_with_default_run(["t3thr", "run", "--config", "x.toml"]);
        assert_eq!(v, vec!["t3thr", "run", "--config", "x.toml"]);
    }

    #[test]
    fn argv_no_inject_for_help_and_version() {
        let h = argv_with_default_run(["t3thr", "--help"]);
        assert_eq!(h, vec!["t3thr", "--help"]);
        let v = argv_with_default_run(["t3thr", "-V"]);
        assert_eq!(v, vec!["t3thr", "-V"]);
    }

    #[test]
    fn argv_unchanged_when_no_args() {
        let v = argv_with_default_run(["t3thr"]);
        assert_eq!(v, vec!["t3thr"]);
    }

    #[test]
    fn explicit_run_and_legacy_form_produce_same_run_args() {
        let injected = argv_with_default_run(["t3thr", "--config", "x.toml"]);
        let cli_legacy = Cli::parse_from(&injected);
        let cli_explicit = Cli::parse_from(["t3thr", "run", "--config", "x.toml"]);
        match (cli_legacy.command, cli_explicit.command) {
            (Command::Run(a), Command::Run(b)) => {
                assert_eq!(a.config, b.config);
                assert_eq!(a.explain, b.explain);
                assert_eq!(a.reset_state, b.reset_state);
                assert_eq!(a.no_state, b.no_state);
                assert_eq!(a.validate_only, b.validate_only);
                assert_eq!(a.config_wizard, b.config_wizard);
            }
            _ => panic!("both forms should route to Command::Run"),
        }
    }

    #[test]
    fn explicit_subcommands_route_correctly() {
        let g = Cli::parse_from(["t3thr", "generate", "--connector", "kafka-consumer"]);
        assert!(matches!(g.command, Command::Generate(_)));
        let v = Cli::parse_from(["t3thr", "validate", "--config", "x.toml"]);
        assert!(matches!(v.command, Command::Validate(_)));
        let m = Cli::parse_from(["t3thr", "migrate", "in.toml"]);
        assert!(matches!(m.command, Command::Migrate(_)));
        let w = Cli::parse_from(["t3thr", "wizard"]);
        assert!(matches!(w.command, Command::Wizard));
    }
}
