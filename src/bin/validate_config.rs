//! Validates a T3thr config by delegating to the `t3thr` binary `--validate-only`
//! (same TOML parse, normalization, literal placeholders + `env_*` resolution, and live license checks).

use std::path::PathBuf;
use std::process::{Command, Stdio};

use clap::Parser;

#[derive(Parser)]
#[command(name = "validate_config")]
#[command(about = "Validate T3thr configuration (delegates to t3thr --validate-only)")]
struct Cli {
    /// Path to config file to validate
    config: PathBuf,
}

fn main() {
    let cli = Cli::parse();

    let exe = option_env!("CARGO_BIN_EXE_t3thr").map(PathBuf::from).unwrap_or_else(|| {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("target");
        p.push(if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        });
        p.push(if cfg!(windows) {
            "t3thr.exe"
        } else {
            "t3thr"
        });
        p
    });

    if !exe.exists() {
        eprintln!(
            "validate_config: t3thr executable not found at {}.\nBuild first: cargo build [--release] [--features full_engine]",
            exe.display()
        );
        std::process::exit(2);
    }

    let status = Command::new(&exe)
        .arg("--config")
        .arg(&cli.config)
        .arg("--validate-only")
        .stdin(Stdio::null())
        .status();

    match status {
        Ok(s) if s.success() => {
            // `t3thr --validate-only` already prints the outcome on stdout.
            std::process::exit(0);
        }
        Ok(s) => {
            std::process::exit(s.code().unwrap_or(1));
        }
        Err(e) => {
            eprintln!("validate_config: failed to spawn t3thr: {e}");
            std::process::exit(3);
        }
    }
}
