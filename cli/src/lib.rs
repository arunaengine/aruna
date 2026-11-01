//! Library behind the aruna-cli binary, so tests can run commands in process.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod args;
mod client;
mod commands;
mod config;
mod error;
mod output;
mod remote;
mod rocrate;
mod sync;
mod transfer;

use clap::Parser;

pub use args::Cli;
pub use error::CliError;

/// Runs one parsed command line and writes its results to `out`.
pub async fn run(cli: Cli, out: &mut dyn std::io::Write) -> Result<(), CliError> {
    commands::dispatch(cli, out).await
}

/// Parses `args`, whose first item is the program name, and runs them like the binary does.
pub async fn run_args<I, T>(args: I, out: &mut dyn std::io::Write) -> Result<(), CliError>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let cli = Cli::try_parse_from(args).map_err(|error| CliError::usage(error.to_string()))?;
    run(cli, out).await
}
