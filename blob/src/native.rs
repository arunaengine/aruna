//! Sends native reference requests to another node and reads its answer frame.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::alpn::Alpn;
use aruna_core::errors::StagingSourceError;
use aruna_core::events::StagingSourceEvent;
use aruna_core::native_reference::{
    MAX_HEADER_SIZE, NativeReferenceRequest, NativeReferenceResponse, relationship_request,
};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
use aruna_net::NetHandle;
use aruna_net::streams::{BiStream, RecvStream, SendStream};
use futures::StreamExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tokio_util::io::ReaderStream;
use tracing::warn;

/// Bound on each frame and body write of the protocol.
pub const NATIVE_IO_TIMEOUT: Duration = Duration::from_secs(30);

/// The metadata of a relationship's native source.
pub(crate) async fn head(
    net: &NetHandle,
    access: ResolvedSourceAccess,
) -> Result<StagingSourceEvent, StagingSourceError> {
    let (origin, request) = relationship_request(&access, None, true)?;
    let (response, mut stream) = send_request(net, origin, request).await?;
    stream.0.finish().ok();
    stream.1.stop(0u32.into()).ok();
    match response {
        NativeReferenceResponse::Content(metadata)
        | NativeReferenceResponse::Described { metadata, .. } => {
            Ok(StagingSourceEvent::HeadResult { metadata })
        }
        NativeReferenceResponse::Rejected(reject) => Err(reject.into()),
    }
}

/// The metadata and bytes of a relationship's native source.
pub(crate) async fn read(
    net: &NetHandle,
    access: ResolvedSourceAccess,
    range: Option<std::ops::Range<u64>>,
) -> Result<StagingSourceEvent, StagingSourceError> {
    let (origin, request) = relationship_request(&access, range, false)?;
    let (response, stream) = send_request(net, origin, request).await?;
    match response {
        NativeReferenceResponse::Content(metadata)
        | NativeReferenceResponse::Described { metadata, .. } => {
            Ok(StagingSourceEvent::ReadResult {
                metadata,
                stream: body_stream(stream),
            })
        }
        NativeReferenceResponse::Rejected(reject) => Err(reject.into()),
    }
}

/// The body that follows an accepted answer, as a byte stream.
pub fn body_stream(stream: BiStream) -> BackendStream<Result<bytes::Bytes, StreamError>> {
    BackendStream::new(ReaderStream::new(stream.into_recv()))
}

/// Sends one request to `origin` and reads its answer; the stream then carries any body.
pub async fn send_request(
    net: &NetHandle,
    origin: NodeId,
    request: NativeReferenceRequest,
) -> Result<(NativeReferenceResponse, BiStream), StagingSourceError> {
    let mut stream = net
        .open_stream(origin, Alpn::NativeReference)
        .await
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
    timeout(NATIVE_IO_TIMEOUT, write_frame(&mut stream.0, &request))
        .await
        .map_err(|_| StagingSourceError::ReadError("native request timed out".to_string()))?
        .map_err(StagingSourceError::ReadError)?;
    stream
        .0
        .finish()
        .map_err(|error| StagingSourceError::ReadError(error.to_string()))?;
    let response = timeout(
        NATIVE_IO_TIMEOUT,
        read_frame::<NativeReferenceResponse>(&mut stream.1),
    )
    .await
    .map_err(|_| StagingSourceError::ReadError("native response timed out".to_string()))?
    .map_err(StagingSourceError::ReadError)?;
    Ok((response, stream))
}

/// The request a peer sent on its native reference stream; a failure is only logged.
pub async fn read_request(stream: &mut BiStream, peer: NodeId) -> Option<NativeReferenceRequest> {
    match timeout(NATIVE_IO_TIMEOUT, read_frame(&mut stream.1)).await {
        Ok(Ok(request)) => Some(request),
        Ok(Err(error)) => {
            warn!(%peer, %error, "Failed to read native reference request");
            None
        }
        Err(_) => {
            warn!(%peer, "Timed out reading native reference request");
            None
        }
    }
}

/// Writes `response` and any `body`, then finishes the stream; a failed write ends the answer.
pub async fn write_answer(
    mut stream: BiStream,
    peer: NodeId,
    response: &NativeReferenceResponse,
    body: Option<BackendStream<Result<bytes::Bytes, StreamError>>>,
) {
    let written = timeout(NATIVE_IO_TIMEOUT, write_frame(&mut stream.0, response));
    if !matches!(written.await, Ok(Ok(()))) {
        return;
    }
    if let Some(mut body) = body {
        while let Some(chunk) = body.next().await {
            let chunk = match chunk {
                Ok(chunk) => chunk,
                Err(error) => {
                    warn!(%peer, %error, "Failed to read native reference source");
                    stream.0.reset(1u32.into()).ok();
                    return;
                }
            };
            let written = timeout(NATIVE_IO_TIMEOUT, stream.0.write_all(&chunk));
            if !matches!(written.await, Ok(Ok(()))) {
                return;
            }
        }
    }
    stream.0.finish().ok();
}

pub async fn write_frame<T: Serialize>(send: &mut SendStream, value: &T) -> Result<(), String> {
    let bytes = postcard::to_allocvec(value).map_err(|error| error.to_string())?;
    if bytes.len() > MAX_HEADER_SIZE {
        return Err("native reference header is too large".to_string());
    }
    send.write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|error| error.to_string())?;
    send.write_all(&bytes)
        .await
        .map_err(|error| error.to_string())
}

pub async fn read_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T, String> {
    let mut length = [0u8; 4];
    recv.read_exact(&mut length)
        .await
        .map_err(|error| error.to_string())?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_HEADER_SIZE {
        return Err("native reference header is too large".to_string());
    }
    let mut bytes = vec![0u8; length];
    recv.read_exact(&mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    postcard::from_bytes(&bytes).map_err(|error| error.to_string())
}
