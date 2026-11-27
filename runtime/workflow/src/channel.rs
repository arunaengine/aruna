//! One framed connection to a coordinator attempt: the greeting, single messages both ways and
//! one preflight exchange, with the length-prefixed frames of the contract.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::pin::Pin;

use aruna_core::workflow::WorkflowError;
use aruna_workflow_contract::{
    ContractError, CoordinatorMode, CoordinatorToHost, FrameError, HostRpcResult,
    HostToCoordinator, PROTOCOL_VERSION, PreflightKind, PreflightResult, decode_frame_len,
    decode_message, encode_message,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::transport::{Attempt, crashed};

/// The reading half of a channel.
pub struct FrameReader {
    reader: Pin<Box<dyn AsyncRead + Send>>,
}

/// The writing half of a channel.
pub struct FrameWriter {
    writer: Pin<Box<dyn AsyncWrite + Send>>,
}

/// A greeted or fresh channel to one coordinator; dropping it ends the attempt.
pub struct Connection {
    reader: FrameReader,
    writer: FrameWriter,
    guard: Box<dyn Send>,
}

impl FrameWriter {
    pub async fn send(&mut self, message: &HostToCoordinator) -> Result<(), WorkflowError> {
        let frame = encode_message(message).map_err(frame_error)?;
        self.writer.write_all(&frame).await.map_err(io_error)?;
        self.writer.flush().await.map_err(io_error)
    }
}

impl FrameReader {
    /// The next message; `None` when the coordinator closed the channel between frames.
    pub async fn receive(&mut self) -> Result<Option<CoordinatorToHost>, WorkflowError> {
        let mut prefix = [0u8; 4];
        if self.reader.read(&mut prefix[..1]).await.map_err(io_error)? == 0 {
            return Ok(None);
        }
        self.reader
            .read_exact(&mut prefix[1..])
            .await
            .map_err(|_| frame_error(FrameError::Truncated))?;
        let mut payload = vec![0u8; decode_frame_len(prefix).map_err(frame_error)?];
        self.reader
            .read_exact(&mut payload)
            .await
            .map_err(|_| frame_error(FrameError::Truncated))?;
        decode_message(&payload).map(Some).map_err(frame_error)
    }
}

impl Connection {
    pub fn new(attempt: Attempt) -> Self {
        Self {
            reader: FrameReader {
                reader: attempt.reader,
            },
            writer: FrameWriter {
                writer: attempt.writer,
            },
            guard: attempt.guard,
        }
    }

    /// The halves and the guard, so a session reads and writes at the same time.
    pub fn into_parts(self) -> (FrameReader, FrameWriter, Box<dyn Send>) {
        (self.reader, self.writer, self.guard)
    }

    pub async fn send(&mut self, message: &HostToCoordinator) -> Result<(), WorkflowError> {
        self.writer.send(message).await
    }

    pub async fn receive(&mut self) -> Result<Option<CoordinatorToHost>, WorkflowError> {
        self.reader.receive().await
    }

    /// Sends the greeting and waits for the coordinator's, which must speak this protocol.
    pub async fn greet(
        &mut self,
        mode: CoordinatorMode,
        run_id: &str,
        claim_token_digest: &str,
    ) -> Result<(), WorkflowError> {
        self.send(&HostToCoordinator::Hello {
            protocol_version: PROTOCOL_VERSION,
            run_id: run_id.to_owned(),
            claim_token_digest: claim_token_digest.to_owned(),
            mode,
        })
        .await?;
        match self.receive().await? {
            Some(CoordinatorToHost::HelloAck {
                protocol_version, ..
            }) if protocol_version == PROTOCOL_VERSION => Ok(()),
            Some(CoordinatorToHost::HelloAck {
                protocol_version, ..
            }) => Err(WorkflowError::Refused(ContractError::Version {
                expected: PROTOCOL_VERSION,
                found: protocol_version,
            })),
            Some(_) => Err(crashed(
                "the coordinator answered the greeting with another message".into(),
            )),
            None => Err(crashed("the coordinator ended before its greeting".into())),
        }
    }

    /// Asks one preflight request and waits for its answer. A preflight coordinator reaches no
    /// host data, so its calls are refused; its live events are dropped.
    pub async fn preflight(
        &mut self,
        request_id: u64,
        kind: PreflightKind,
    ) -> Result<Result<PreflightResult, ContractError>, WorkflowError> {
        self.send(&HostToCoordinator::PreflightRequest { request_id, kind })
            .await?;
        loop {
            match self.receive().await? {
                Some(CoordinatorToHost::PreflightResponse {
                    request_id: answered,
                    result,
                }) if answered == request_id => return Ok(result),
                Some(CoordinatorToHost::RpcRequest { call_id, .. }) => {
                    let refused = ContractError::Denied {
                        message: "a preflight coordinator reaches no host data".into(),
                    };
                    let result = HostRpcResult::Failed(refused);
                    self.send(&HostToCoordinator::RpcResponse { call_id, result })
                        .await?;
                }
                Some(CoordinatorToHost::LiveEvent { .. }) => {}
                Some(_) => {
                    return Err(crashed("the coordinator sent an unexpected message".into()));
                }
                None => return Err(crashed("the coordinator ended its channel".into())),
            }
        }
    }
}

fn io_error(error: std::io::Error) -> WorkflowError {
    crashed(format!("the channel failed: {error}"))
}

fn frame_error(error: FrameError) -> WorkflowError {
    crashed(format!("a frame of the channel: {error}"))
}
