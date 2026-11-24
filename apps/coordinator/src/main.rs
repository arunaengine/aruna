//! Starts the coordinator on standard input and output, the channel its node opened.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::os::unix::fs::DirBuilderExt;
use std::process::ExitCode;

use aruna_coordinator::{ServeOptions, serve_streams};

fn main() -> ExitCode {
    // Standard output is the channel, so diagnostics go to standard error.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();
    let folder = std::env::temp_dir().join(format!("aruna-coordinator-{}", std::process::id()));
    if let Err(error) = std::fs::DirBuilder::new().mode(0o700).create(&folder) {
        tracing::error!(%error, "cannot create the private folder");
        return ExitCode::FAILURE;
    }
    let secrets_file = folder.join("secrets.json");
    // SAFETY: no other thread runs yet; the runtime starts below. uwe reads the Nextflow secret
    // store only from the process environment.
    unsafe { std::env::set_var("NXF_SECRETS_FILE", &secrets_file) };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build();
    let served = match runtime {
        Ok(runtime) => runtime.block_on(serve_streams(
            tokio::io::stdin(),
            tokio::io::stdout(),
            ServeOptions {
                secrets_file: Some(secrets_file),
            },
        )),
        Err(error) => {
            tracing::error!(%error, "cannot start the runtime");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = std::fs::remove_dir_all(&folder) {
        tracing::warn!(%error, "cannot remove the private folder");
    }
    match served {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "the session failed");
            ExitCode::FAILURE
        }
    }
}
