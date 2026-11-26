//! The workflow coordinator of an Aruna node: greets the node, then answers preflight or report
//! requests or runs one workflow, over one framed byte channel.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod backend;
pub mod changes;
pub mod composed;
pub mod preflight;
pub mod protocol;
pub mod run_store;
pub mod runner;
pub mod source;
pub mod storage;

use std::path::PathBuf;

use aruna_workflow_contract::message::RunFinishedPayload;
use aruna_workflow_contract::{
    ContractError, CoordinatorMode, CoordinatorToHost, FrameError, HostToCoordinator,
    PROTOCOL_VERSION, PreflightKind, PreflightResult,
};
use serde_json::json;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::task::{JoinError, JoinHandle, JoinSet};

use crate::protocol::{Inbound, RpcClient};

/// The uwe-rs revision this coordinator links, as `Cargo.toml` pins it.
pub const UWE_REVISION: &str = "a278b311c1bbc367d4b10381f95b025acf901764";
/// What this coordinator can do, as `HelloAck` lists it.
pub const CAPABILITIES: [&str; 5] = ["preflight", "report", "run", "composed", "step_settings"];

/// What the process gives the session besides the channel.
#[derive(Clone, Debug, Default)]
pub struct ServeOptions {
    /// The Nextflow secret store that `NXF_SECRETS_FILE` names for the process; a run with
    /// secrets writes its placeholders there.
    pub secrets_file: Option<PathBuf>,
}

/// Why a session ended without its work done.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error("the host closed the channel before its greeting")]
    NoGreeting,
    #[error("the first frame of the host was no greeting")]
    UnexpectedFirst,
    #[error(transparent)]
    Version(ContractError),
    #[error("the channel failed: {0}")]
    Frame(#[from] FrameError),
}

/// Serves one session over a channel: the greeting, then the requests of its mode, until the
/// host shuts it down or closes the channel. Tests drive it over in-memory pipes.
pub async fn serve_streams<R, W>(
    reader: R,
    writer: W,
    options: ServeOptions,
) -> Result<(), ServeError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (rpc, writer, mut inbound) = protocol::start(reader, writer);
    let served = session(&rpc, &mut inbound, options).await;
    rpc.close_calls("the session ended");
    let closed = writer.close().await;
    served?;
    closed?;
    Ok(())
}

async fn session(
    rpc: &RpcClient,
    inbound: &mut tokio::sync::mpsc::Receiver<Inbound>,
    options: ServeOptions,
) -> Result<(), ServeError> {
    let (run_id, mode) = match inbound.recv().await {
        Some(Inbound::Message(message)) => match *message {
            HostToCoordinator::Hello {
                protocol_version,
                run_id,
                mode,
                ..
            } => {
                rpc.send(&CoordinatorToHost::HelloAck {
                    protocol_version: PROTOCOL_VERSION,
                    uwe_revision: UWE_REVISION.into(),
                    capabilities: CAPABILITIES.map(str::to_owned).to_vec(),
                })?;
                if protocol_version != PROTOCOL_VERSION {
                    return Err(ServeError::Version(ContractError::Version {
                        expected: PROTOCOL_VERSION,
                        found: protocol_version,
                    }));
                }
                (run_id, mode)
            }
            _ => return Err(ServeError::UnexpectedFirst),
        },
        Some(Inbound::Closed(Some(error))) => return Err(error.into()),
        Some(Inbound::Closed(None)) | None => return Err(ServeError::NoGreeting),
    };
    let cancel = rpc.cancel_token();
    let mut requests: JoinSet<()> = JoinSet::new();
    let mut run: Option<JoinHandle<RunFinishedPayload>> = None;
    let mut replay_pages = Vec::new();
    let mut started = false;
    let mut draining = false;
    let mut open = true;
    let mut failure = None;
    loop {
        if (draining || !open) && requests.is_empty() && run.is_none() {
            break;
        }
        tokio::select! {
            received = inbound.recv(), if open => {
                let message = match received {
                    Some(Inbound::Message(message)) => *message,
                    closed => {
                        let error = match closed {
                            Some(Inbound::Closed(error)) => error,
                            _ => None,
                        };
                        // No answer can come any more, so the work in flight stops.
                        open = false;
                        cancel.cancel();
                        rpc.close_calls("the host closed the channel");
                        failure = failure.or(error);
                        continue;
                    }
                };
                match message {
                    HostToCoordinator::PreflightRequest { request_id, kind } => {
                        if let Err(refused) = admit(mode, &kind) {
                            respond(rpc, request_id, Err(refused));
                            continue;
                        }
                        let staged = match mode {
                            CoordinatorMode::Report => std::mem::take(&mut replay_pages),
                            _ => Vec::new(),
                        };
                        let (rpc, cancel) = (rpc.clone(), cancel.child_token());
                        requests.spawn(async move {
                            let result = preflight::answer_with(kind, staged, cancel).await;
                            respond(&rpc, request_id, result);
                        });
                    }
                    HostToCoordinator::RunReplayPages { pages }
                        if (mode == CoordinatorMode::Run && !started)
                            || mode == CoordinatorMode::Report =>
                    {
                        replay_pages.extend(pages);
                    }
                    HostToCoordinator::RunReplayPages { .. } => {
                        tracing::warn!(?mode, started, "replay pages this session does not take");
                    }
                    HostToCoordinator::RunStart(mut payload)
                        if mode == CoordinatorMode::Run && !started =>
                    {
                        started = true;
                        if !replay_pages.is_empty() {
                            let mut combined = std::mem::take(&mut replay_pages);
                            combined.extend(payload.prior_log_pages);
                            payload.prior_log_pages = combined;
                        }
                        let (rpc, run_id) = (rpc.clone(), run_id.clone());
                        let secrets = options.secrets_file.clone();
                        run = Some(tokio::spawn(async move {
                            runner::run_workflow(rpc, run_id, payload, secrets.as_deref()).await
                        }));
                    }
                    HostToCoordinator::RunStart(_) => {
                        tracing::warn!(?mode, started, "a run start this session does not take");
                    }
                    HostToCoordinator::RpcResponse { call_id, result } => rpc.answer(call_id, result),
                    HostToCoordinator::Cancel { reason } => {
                        tracing::info!(%reason, "the host cancelled the work");
                        cancel.cancel();
                    }
                    HostToCoordinator::Shutdown => draining = true,
                    HostToCoordinator::Hello { .. } => tracing::warn!("a second greeting"),
                }
            }
            Some(joined) = requests.join_next(), if !requests.is_empty() => {
                if let Err(error) = joined {
                    tracing::error!(%error, "a request worker failed");
                }
            }
            finished = joined_run(&mut run), if run.is_some() => {
                run = None;
                let payload = finished.unwrap_or_else(|error| RunFinishedPayload {
                    success: false,
                    error: Some(format!("the run worker failed: {error}")),
                    stopped: cancel.is_cancelled(),
                    final_summary_json: json!({}),
                });
                send_finished(rpc, payload);
                // The run is the whole work of a run session.
                draining = true;
            }
        }
    }
    match failure {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

/// Whether the session's mode takes a request: preflight sessions take checks, report sessions
/// take reports over run logs, and run sessions take neither.
fn admit(mode: CoordinatorMode, kind: &PreflightKind) -> Result<(), ContractError> {
    let report = preflight::is_report(kind);
    match (mode, report) {
        (CoordinatorMode::Preflight, false) | (CoordinatorMode::Report, true) => Ok(()),
        (mode, _) => Err(ContractError::invalid(format!(
            "a coordinator in {mode:?} mode does not answer {}",
            kind.name()
        ))),
    }
}

fn respond(rpc: &RpcClient, request_id: u64, result: Result<PreflightResult, ContractError>) {
    let sent = rpc.send(&CoordinatorToHost::PreflightResponse { request_id, result });
    if let Err(error) = sent {
        let refused = ContractError::invalid(format!("the result does not fit a frame: {error}"));
        let result = Err(refused);
        if let Err(error) = rpc.send(&CoordinatorToHost::PreflightResponse { request_id, result }) {
            tracing::error!(%error, request_id, "no answer could be sent");
        }
    }
}

fn send_finished(rpc: &RpcClient, payload: RunFinishedPayload) {
    let stopped = payload.stopped;
    let success = payload.success;
    let error = payload.error.clone();
    if let Err(problem) = rpc.send(&CoordinatorToHost::RunFinished(payload)) {
        tracing::error!(%problem, "the summary of the run does not fit a frame");
        let reduced = RunFinishedPayload {
            success,
            error,
            stopped,
            final_summary_json: json!({}),
        };
        if let Err(problem) = rpc.send(&CoordinatorToHost::RunFinished(reduced)) {
            tracing::error!(%problem, "the end of the run could not be sent");
        }
    }
}

async fn joined_run(
    run: &mut Option<JoinHandle<RunFinishedPayload>>,
) -> Result<RunFinishedPayload, JoinError> {
    match run {
        Some(handle) => handle.await,
        None => std::future::pending().await,
    }
}
