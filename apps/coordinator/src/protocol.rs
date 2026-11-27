//! The framed channel of one coordinator attempt: a reader and a writer task, calls to the host
//! routed back by call id, live events with their own sequence, and the cancel token of the run.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use aruna_workflow_contract::{
    ContractError, CoordinatorToHost, FrameError, HostRpcRequest, HostRpcResult, HostToCoordinator,
    LiveWorkflowEvent, decode_frame_len, decode_message, encode_message,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use workflow_graph::CancellationToken;

/// What the reader task hands to the session loop.
pub(crate) enum Inbound {
    Message(Box<HostToCoordinator>),
    /// The host closed the channel, cleanly when `None`.
    Closed(Option<FrameError>),
}

/// What the writer task takes: an encoded frame, or the end of the channel after the queued ones.
enum Outbound {
    Frame(Vec<u8>),
    Close,
}

/// Where the answer to a call goes: an async caller, or a thread that blocks for it.
enum Waiter {
    Async(oneshot::Sender<Result<HostRpcResult, ContractError>>),
    Blocking(std::sync::mpsc::Sender<Result<HostRpcResult, ContractError>>),
}

struct Shared {
    outbound: mpsc::UnboundedSender<Outbound>,
    pending: Mutex<Pending>,
    next_call: AtomicU64,
    next_event: AtomicU64,
    cancel: CancellationToken,
}

/// The calls that wait for an answer, or why no answer can come any more.
#[derive(Default)]
struct Pending {
    waiters: HashMap<u64, Waiter>,
    closed: Option<String>,
}

/// Sends calls and events to the host; clones share the channel.
#[derive(Clone)]
pub struct RpcClient {
    shared: Arc<Shared>,
}

/// The writer task of a channel; `close` lets it write what is queued and end.
pub(crate) struct Writer {
    task: tokio::task::JoinHandle<Result<(), FrameError>>,
    client: RpcClient,
}

/// Starts the reader and writer tasks of a channel.
pub(crate) fn start<R, W>(reader: R, writer: W) -> (RpcClient, Writer, mpsc::Receiver<Inbound>)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (outbound, queued) = mpsc::unbounded_channel();
    let client = RpcClient {
        shared: Arc::new(Shared {
            outbound,
            pending: Mutex::new(Pending::default()),
            next_call: AtomicU64::new(1),
            next_event: AtomicU64::new(1),
            cancel: CancellationToken::new(),
        }),
    };
    // A small queue, so that a host that stops reading holds back the reader too.
    let (inbound, received) = mpsc::channel(16);
    tokio::spawn(read_loop(reader, inbound));
    let task = tokio::spawn(write_loop(writer, queued));
    let writer = Writer {
        task,
        client: client.clone(),
    };
    (client, writer, received)
}

impl Writer {
    /// Writes the queued messages, closes the stream and waits for it.
    pub(crate) async fn close(self) -> Result<(), FrameError> {
        let _ = self.client.shared.outbound.send(Outbound::Close);
        match self.task.await {
            Ok(result) => result,
            Err(error) => Err(FrameError::Io(std::io::Error::other(error))),
        }
    }
}

impl RpcClient {
    /// The token that a `Cancel` of the host cancels.
    pub fn cancel_token(&self) -> CancellationToken {
        self.shared.cancel.clone()
    }

    /// Queues a message; it fails only when the message does not fit into one frame. A closed
    /// writer means the host is gone, and the session loop ends on its own then.
    pub(crate) fn send(&self, message: &CoordinatorToHost) -> Result<(), FrameError> {
        let frame = encode_message(message)?;
        let _ = self.shared.outbound.send(Outbound::Frame(frame));
        Ok(())
    }

    /// Sends a live event with the next event number; one too large for a frame is dropped.
    pub fn emit(&self, event: LiveWorkflowEvent) {
        let seq = self.shared.next_event.fetch_add(1, Ordering::SeqCst);
        if let Err(error) = self.send(&CoordinatorToHost::LiveEvent { seq, event }) {
            tracing::warn!(%error, seq, "a live event was dropped");
        }
    }

    /// Calls the host and waits for its answer; a `Failed` answer becomes the error.
    pub async fn call(&self, request: HostRpcRequest) -> Result<HostRpcResult, ContractError> {
        let (sender, receiver) = oneshot::channel();
        self.register(Waiter::Async(sender), request)?;
        let answer = receiver.await.map_err(|_| closed_error(None))?;
        failed_answer(answer)
    }

    /// `call` for a thread that may block, like uwe's worker thread, inside a runtime or not.
    pub fn call_blocking(&self, request: HostRpcRequest) -> Result<HostRpcResult, ContractError> {
        let (sender, receiver) = std::sync::mpsc::channel();
        self.register(Waiter::Blocking(sender), request)?;
        let answer = receiver.recv().map_err(|_| closed_error(None))?;
        failed_answer(answer)
    }

    fn register(&self, waiter: Waiter, request: HostRpcRequest) -> Result<(), ContractError> {
        let call_id = self.shared.next_call.fetch_add(1, Ordering::SeqCst);
        {
            let mut pending = self.pending();
            if let Some(reason) = &pending.closed {
                return Err(closed_error(Some(reason)));
            }
            pending.waiters.insert(call_id, waiter);
        }
        let sent = self.send(&CoordinatorToHost::RpcRequest { call_id, request });
        if let Err(error) = sent {
            self.pending().waiters.remove(&call_id);
            return Err(ContractError::invalid(format!(
                "the call cannot be sent: {error}"
            )));
        }
        Ok(())
    }

    /// Hands an answer of the host to its caller; an answer nobody waits for is dropped.
    pub(crate) fn answer(&self, call_id: u64, result: HostRpcResult) {
        let waiter = self.pending().waiters.remove(&call_id);
        match waiter {
            Some(Waiter::Async(sender)) => drop(sender.send(Ok(result))),
            Some(Waiter::Blocking(sender)) => drop(sender.send(Ok(result))),
            None => tracing::debug!(call_id, "an answer to no waiting call"),
        }
    }

    /// Fails every waiting call and every later one, as no answer can come any more.
    pub(crate) fn close_calls(&self, reason: &str) {
        let waiters = {
            let mut pending = self.pending();
            pending.closed = Some(reason.to_owned());
            std::mem::take(&mut pending.waiters)
        };
        for waiter in waiters.into_values() {
            let error = Err(closed_error(Some(reason)));
            match waiter {
                Waiter::Async(sender) => drop(sender.send(error)),
                Waiter::Blocking(sender) => drop(sender.send(error)),
            }
        }
    }

    fn pending(&self) -> std::sync::MutexGuard<'_, Pending> {
        self.shared
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

fn failed_answer(
    answer: Result<HostRpcResult, ContractError>,
) -> Result<HostRpcResult, ContractError> {
    match answer? {
        HostRpcResult::Failed(error) => Err(error),
        result => Ok(result),
    }
}

fn closed_error(reason: Option<&str>) -> ContractError {
    let reason = reason.unwrap_or("the channel closed");
    ContractError::internal(format!("the host gives no answers any more: {reason}"))
}

/// Reads one message; `None` when the stream ended cleanly between frames.
pub(crate) async fn read_message<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Option<HostToCoordinator>, FrameError> {
    let mut prefix = [0u8; 4];
    let mut filled = 0;
    while filled < prefix.len() {
        match reader.read(&mut prefix[filled..]).await? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(FrameError::Truncated),
            read => filled += read,
        }
    }
    let size = decode_frame_len(prefix)?;
    let mut payload = vec![0u8; size];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::UnexpectedEof => FrameError::Truncated,
            _ => FrameError::Io(error),
        })?;
    decode_message(&payload).map(Some)
}

async fn read_loop<R: AsyncRead + Unpin>(mut reader: R, inbound: mpsc::Sender<Inbound>) {
    loop {
        let read = read_message(&mut reader).await;
        let next = match read {
            Ok(Some(message)) => Inbound::Message(Box::new(message)),
            Ok(None) => Inbound::Closed(None),
            Err(error) => Inbound::Closed(Some(error)),
        };
        let last = matches!(next, Inbound::Closed(_));
        if inbound.send(next).await.is_err() || last {
            return;
        }
    }
}

async fn write_loop<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut queued: mpsc::UnboundedReceiver<Outbound>,
) -> Result<(), FrameError> {
    while let Some(Outbound::Frame(frame)) = queued.recv().await {
        writer.write_all(&frame).await?;
        writer.flush().await?;
    }
    writer.shutdown().await?;
    Ok(())
}
