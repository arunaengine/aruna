//! Starts the coordinator on standard input and output, or on `ARUNA_SESSION_SOCKET` inside a
//! container session; with `session-client` it bridges stdio to that socket for `open_session`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use aruna_coordinator::{ServeOptions, serve_streams};
use tokio::net::{UnixListener, UnixStream};

const SESSION_SOCKET_ENV: &str = "ARUNA_SESSION_SOCKET";
const DEFAULT_SESSION_SOCKET: &str = "/tmp/.aruna/session.sock";

fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("session-client") {
        return run_session_client();
    }
    // Standard output is the channel in stdio mode, so diagnostics go to standard error.
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
    let socket_path = std::env::var_os(SESSION_SOCKET_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build();
    let served = match runtime {
        Ok(runtime) => runtime.block_on(async move {
            let options = ServeOptions {
                secrets_file: Some(secrets_file),
            };
            match socket_path {
                Some(path) => serve_socket(&path, options).await,
                None => serve_streams(tokio::io::stdin(), tokio::io::stdout(), options)
                    .await
                    .map_err(|error| error.to_string()),
            }
        }),
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

async fn serve_socket(path: &Path, options: ServeOptions) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| format!("session socket dir: {error}"))?;
    }
    let _ = std::fs::remove_file(path);
    let listener =
        UnixListener::bind(path).map_err(|error| format!("bind session socket: {error}"))?;
    let (stream, _) = listener
        .accept()
        .await
        .map_err(|error| format!("accept session socket: {error}"))?;
    let (reader, writer) = stream.into_split();
    let result = serve_streams(reader, writer, options)
        .await
        .map_err(|error| error.to_string());
    let _ = std::fs::remove_file(path);
    result
}

fn run_session_client() -> ExitCode {
    let socket = std::env::var_os(SESSION_SOCKET_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SESSION_SOCKET));
    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return ExitCode::FAILURE;
    };
    let ok = runtime.block_on(async move {
        let deadline = Instant::now() + Duration::from_secs(5);
        let stream = loop {
            match UnixStream::connect(&socket).await {
                Ok(stream) => break stream,
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => {
                    eprintln!("session-client connect {}: {error}", socket.display());
                    return false;
                }
            }
        };
        let (mut sock_read, mut sock_write) = stream.into_split();
        let mut stdin = tokio::io::stdin();
        let mut stdout = tokio::io::stdout();
        let to_sock = tokio::io::copy(&mut stdin, &mut sock_write);
        let from_sock = tokio::io::copy(&mut sock_read, &mut stdout);
        tokio::select! {
            result = to_sock => result.is_ok(),
            result = from_sock => result.is_ok(),
        }
    });
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
