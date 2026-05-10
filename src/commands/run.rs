//! `t3thr run` subcommand: dispatches to the existing pipeline runner that
//! lives at the crate root (`crate::execute_run`). The legacy `t3thr --config X`
//! form rewrites argv to this subcommand before clap parsing, so the runner
//! body stays untouched.

use anyhow::Result;

use super::RunArgs;

pub fn execute(args: &RunArgs) -> Result<()> {
    crate::execute_run(args)
}
