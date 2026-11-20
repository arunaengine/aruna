//! Library behind the aruna-cli binary, so tests can run commands in process.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod args;
mod client;
mod commands;
mod config;
pub mod error;
mod output;
mod remote;
mod rocrate;
mod sync;
mod transfer;

use clap::Parser;

use args::Cli;
use error::CliError;

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

/// Records `upload_id` in `config_dir` as the multipart upload a `cp` of `path` to `bucket` and
/// `key` at `endpoint` started and did not finish, under the record lock a `cp` takes. Tests use
/// it to stand in for an interrupted first run.
#[cfg(feature = "test-hooks")]
pub async fn keep_upload(
    config_dir: &std::path::Path,
    target: (&str, &str, &str),
    (path, part_size): (&std::path::Path, u64),
    upload_id: &str,
) -> Result<(), CliError> {
    transfer::keep_upload(config_dir, target, (path, part_size), upload_id).await
}
