//! Process entry: installs the crypto provider, dispatches helpers and runs the node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

#![allow(clippy::result_large_err)]
// The tracked recovery child overflows the default query depth in a fresh build.
#![recursion_limit = "256"]

#[cfg(test)]
use aruna::application::ProcessOutcome;
use aruna::application::run_node;
use aruna::default_env;
use aruna::telemetry::{init_tracing, shutdown_tracing};
use tracing::warn;

/// Process entry. Prerequisites and helper dispatch happen before the node
/// runtime exists, so a helper exit never starts the node.
fn main() {
    // Both ring and aws-lc-rs are in the graph; rustls needs one picked before any TLS init.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("install rustls ring crypto provider");
    if let Some(code) = aruna_compute::dispatch_helper() {
        std::process::exit(code);
    }
    if std::env::args().nth(1).as_deref() == Some("--git-hook")
        || std::env::args_os().next().is_some_and(|path| {
            std::path::Path::new(&path)
                .file_name()
                .is_some_and(|name| name == "pre-receive")
        })
    {
        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .and_then(|runtime| runtime.block_on(aruna_operations::git::hook::validate()));
        if let Err(error) = &result {
            // Git relays hook stderr to the pushing client as its rejection reason.
            eprintln!("aruna: {error}");
        }
        std::process::exit(if result.is_ok() { 0 } else { 1 });
    }
    run_runtime();
}

/// Synchronous runtime entry: creates the Tokio runtime once on the node path
/// and maps the node outcome to its exit behavior.
#[tokio::main]
async fn run_runtime() {
    dotenv_optional(dotenvy::dotenv()).expect("Failed to load .env file");
    init_tracing();

    if let Err(error) = report_default_env() {
        eprintln!("{error}");
        shutdown_tracing();
        std::process::exit(1);
    }

    let outcome = run_node().await;
    shutdown_tracing();

    match outcome {
        Ok(outcome) => {
            if let Some(failure) = outcome.failure() {
                eprintln!("{failure}");
            }
            if let Some(code) = outcome.exit_code() {
                std::process::exit(code);
            }
        }
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

/// A device is started by its desktop app with the environment already set, so
/// a missing `.env` is a valid profile. A malformed one is still an error.
fn dotenv_optional(
    loaded: Result<std::path::PathBuf, dotenvy::Error>,
) -> Result<(), dotenvy::Error> {
    match loaded {
        Ok(_) => Ok(()),
        Err(error) if error.not_found() => Ok(()),
        Err(error) => Err(error),
    }
}

/// Refuses the start while a published demonstration key is in the environment,
/// unless the operator opted in; each admitted key is then named in a warning.
fn report_default_env() -> Result<(), default_env::DefaultEnvError> {
    let keys = default_env::guard(|key| std::env::var(key).ok(), std::env::args())?;
    for key in keys {
        warn!(
            key,
            "Serving with a published demonstration key, admitted by {}",
            default_env::OVERRIDE_FLAG
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotenv_allows_missing() {
        // No .env is a device profile; a malformed one must still fail startup.
        assert!(dotenv_optional(Ok(std::path::PathBuf::from(".env"))).is_ok());
        assert!(
            dotenv_optional(Err(dotenvy::Error::Io(std::io::Error::from(
                std::io::ErrorKind::NotFound
            ))))
            .is_ok()
        );
        assert!(dotenv_optional(Err(dotenvy::Error::LineParse("KEY".into(), 1))).is_err());
    }

    // The outcome mapping is unit-tested next to `ProcessOutcome`; this guards
    // that the process entry maps the wipe outcomes to their documented codes.
    #[test]
    fn wipe_codes_distinct() {
        assert_ne!(
            ProcessOutcome::WipeComplete.exit_code(),
            ProcessOutcome::WipeIncomplete.exit_code()
        );
        assert!(ProcessOutcome::WipeIncomplete.exit_code().is_some());
    }
}
