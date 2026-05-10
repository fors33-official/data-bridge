//! Migrate legacy configs by delegating to `t3thr migrate` (same behavior as the subcommand).

use std::path::PathBuf;
use std::process::{Command, Stdio};

use clap::Parser;

#[derive(Parser)]
#[command(name = "migrate_config")]
#[command(about = "Migrate legacy Data Bridge configs (delegates to: t3thr migrate)")]
struct Cli {
    /// Path to legacy config file
    input: PathBuf,
    #[arg(short, long)]
    output: Option<PathBuf>,
    #[arg(long)]
    dry_run: bool,
}

fn t3thr_exe() -> PathBuf {
    option_env!("CARGO_BIN_EXE_t3thr")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            p.push("target");
            p.push(if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            });
            p.push(if cfg!(windows) { "t3thr.exe" } else { "t3thr" });
            p
        })
}

fn main() {
    let cli = Cli::parse();
    let exe = t3thr_exe();
    if !exe.exists() {
        eprintln!(
            "migrate_config: t3thr not found at {}. Build: cargo build [--release] [--features full_engine]",
            exe.display()
        );
        std::process::exit(2);
    }

    let mut cmd = Command::new(&exe);
    cmd.arg("migrate").arg(&cli.input);
    if let Some(ref o) = cli.output {
        cmd.arg("--output").arg(o);
    }
    if cli.dry_run {
        cmd.arg("--dry-run");
    }
    cmd.stdin(Stdio::null());

    match cmd.status() {
        Ok(s) if s.success() => std::process::exit(0),
        Ok(s) => std::process::exit(s.code().unwrap_or(1)),
        Err(e) => {
            eprintln!("migrate_config: failed to spawn t3thr: {e}");
            std::process::exit(3);
        }
    }
}
