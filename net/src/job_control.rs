//! Sends one job-control request to its owner and frames requests and answers on the stream.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::events::JobControlEvent;
use aruna_core::jobs::{JobRequest, JobResponse};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;

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
/// operation can map it to `Unavailable` (503). The artifact body path stays out of band.
pub(crate) async fn exchange(
    net: &NetHandle,
    owner: NodeId,
    request: JobRequest,
) -> JobControlEvent {
    match send_request(net, owner, request).await {
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
