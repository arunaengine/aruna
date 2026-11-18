//! Sends one job-control request to its owner and frames requests and answers on the stream.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::events::JobControlEvent;
use aruna_core::jobs::{JobRequest, JobResponse};
use aruna_core::stream::BackendStream;
use futures::{Stream, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tokio_util::io::ReaderStream;
use tracing::warn;

use crate::NetHandle;
use crate::streams::{BiStream, RecvStream, SendStream};

/// Bound of every frame read or write and of each artifact chunk on a job stream.
pub const JOB_IO_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

/// The owner's answer and, for a ranged artifact that is ready, the stream carrying its bytes.
#[derive(Debug)]
pub struct JobReply {
    pub response: JobResponse,
    pub body: Option<BiStream>,
}

/// Runs a job-control effect: an unreachable owner is reported so the routing
/// operation can map it to `Unavailable` (503). A ranged artifact answers with its bytes.
pub(crate) async fn exchange(
    net: &NetHandle,
    owner: NodeId,
    request: JobRequest,
) -> JobControlEvent {
    match send_request(net, owner, request).await {
        Ok(JobReply {
            response,
            body: Some(stream),
        }) => JobControlEvent::Streamed {
            response: Box::new(response),
            body: BackendStream::new(ReaderStream::new(stream.into_recv())),
        },
        Ok(reply) => JobControlEvent::Response(Box::new(reply.response)),
        Err(error) => JobControlEvent::Unavailable(format!("job owner unavailable: {error}")),
    }
}

/// Opens a job-control stream to `owner`, writes the request and reads the answer.
pub async fn send_request(
    net: &NetHandle,
    owner: NodeId,
    request: JobRequest,
) -> Result<JobReply, String> {
    let expects_body = matches!(request, JobRequest::Artifact { range: Some(_), .. });
    let mut stream = net
        .open_stream(owner, Alpn::JobControl)
        .await
        .map_err(|error| error.to_string())?;
    timeout(JOB_IO_TIMEOUT, write_frame(&mut stream.0, &request))
        .await
        .map_err(|_| "job request timed out".to_string())??;
    stream.0.finish().map_err(|error| error.to_string())?;
    let response = timeout(JOB_IO_TIMEOUT, read_frame(&mut stream.1))
        .await
        .map_err(|_| "job response timed out".to_string())??;
    let body = if expects_body && matches!(response, JobResponse::ArtifactReady { .. }) {
        Some(stream)
    } else {
        stream.1.stop(0u32.into()).ok();
        None
    };
    Ok(JobReply { response, body })
}

/// Reads the request a peer opened a job-control stream with.
pub async fn read_request(stream: &mut BiStream, peer: NodeId) -> Option<JobRequest> {
    match timeout(JOB_IO_TIMEOUT, read_frame::<JobRequest>(&mut stream.1)).await {
        Ok(Ok(request)) => Some(request),
        Ok(Err(error)) => {
            warn!(%peer, %error, "Failed to read job-control request");
            None
        }
        Err(_) => {
            warn!(%peer, "Timed out reading job-control request");
            None
        }
    }
}

/// Writes the answer and its artifact bytes. Each source poll is bounded, so a stalled artifact
/// reader releases its blob-read and inbound-handler permits instead of pinning them.
pub async fn write_answer<B, C, E>(
    mut stream: BiStream,
    peer: NodeId,
    response: &JobResponse,
    body: Option<B>,
) where
    B: Stream<Item = Result<C, E>> + Unpin,
    C: AsRef<[u8]>,
    E: std::fmt::Display,
{
    let written = timeout(JOB_IO_TIMEOUT, write_frame(&mut stream.0, response)).await;
    if !matches!(written, Ok(Ok(()))) {
        return;
    }
    if let Some(mut body) = body {
        loop {
            let chunk = match timeout(JOB_IO_TIMEOUT, body.next()).await {
                Ok(Some(Ok(chunk))) => chunk,
                Ok(None) => break,
                Ok(Some(Err(error))) => {
                    warn!(%peer, %error, "Failed to read job artifact");
                    stream.0.reset(1u32.into()).ok();
                    return;
                }
                Err(_) => {
                    warn!(%peer, "Timed out reading job artifact source");
                    stream.0.reset(1u32.into()).ok();
                    return;
                }
            };
            let sent = timeout(JOB_IO_TIMEOUT, stream.0.write_all(chunk.as_ref())).await;
            if !matches!(sent, Ok(Ok(()))) {
                return;
            }
        }
    }
    stream.0.finish().ok();
}

pub async fn write_frame<T: Serialize>(send: &mut SendStream, value: &T) -> Result<(), String> {
    let bytes = postcard::to_allocvec(value).map_err(|error| error.to_string())?;
    if bytes.len() > MAX_FRAME_SIZE {
        return Err("job-control frame exceeds maximum size".to_string());
    }
    send.write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|error| error.to_string())?;
    send.write_all(&bytes)
        .await
        .map_err(|error| error.to_string())?;
    send.flush().await.map_err(|error| error.to_string())
}

pub async fn read_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T, String> {
    let mut length = [0u8; 4];
    recv.read_exact(&mut length)
        .await
        .map_err(|error| error.to_string())?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_FRAME_SIZE {
        return Err("job-control frame exceeds maximum size".to_string());
    }
    let mut bytes = vec![0u8; length];
    recv.read_exact(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    postcard::from_bytes(&bytes).map_err(|error| error.to_string())
}
