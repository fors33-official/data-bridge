//! Launch the interactive `config_wizard` binary shipped alongside `t3thr`.

use std::process::Command;

use anyhow::{Context, Result};

pub fn execute() -> Result<()> {
    let mut wizard = std::env::current_exe().context("failed to read current executable path")?;
    let name = if cfg!(windows) {
        "config_wizard.exe"
    } else {
        "config_wizard"
    };
    wizard.set_file_name(name);
    if !wizard.exists() {
        anyhow::bail!(
            "config_wizard not found next to this binary ({}). Build with: cargo build --release --bins [--features full_engine]",
            wizard.display()
        );
    }
    let status = Command::new(&wizard)
        .status()
        .with_context(|| format!("failed to spawn {}", wizard.display()))?;
    std::process::exit(status.code().unwrap_or(1));
}
