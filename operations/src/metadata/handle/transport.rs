//! Frames metadata messages and bodies on the streams this node serves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::metadata::MetadataError;
use aruna_net::streams::{BiStream, RecvStream};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, timeout_at};

use super::frames::{read_message_budget, write_message};
use super::{METADATA_CHUNK_SIZE, METADATA_IO_TIMEOUT};
use crate::metadata::protocol::MetadataTransportMessage;

pub(super) async fn write_transport_message(
    stream: &mut BiStream,
    message: &MetadataTransportMessage,
) -> Result<(), MetadataError> {
    let result: Result<Result<(), String>, tokio::time::error::Elapsed> =
        timeout(METADATA_IO_TIMEOUT, write_message(stream, message)).await;
    result
        .map_err(|_| MetadataError::Backend("timed out writing metadata message".to_string()))?
        .map_err(MetadataError::Backend)
}

pub(super) async fn read_budget<R>(
    reader: &mut R,
    budget: &Arc<tokio::sync::Semaphore>,
) -> Result<(MetadataTransportMessage, tokio::sync::OwnedSemaphorePermit), MetadataError>
where
    R: AsyncRead + Unpin + ?Sized,
{
    timeout(
        METADATA_IO_TIMEOUT,
        read_message_budget(reader, super::super::protocol::MAX_MESSAGE_SIZE, budget),
    )
    .await
    .map_err(|_| MetadataError::Backend("timed out waiting for metadata message".to_string()))?
    .map_err(MetadataError::Backend)
}

pub(super) async fn write_stream_body(
    stream: &mut BiStream,
    bytes: &[u8],
) -> Result<(), MetadataError> {
    timeout(METADATA_IO_TIMEOUT, async {
        for chunk in bytes.chunks(METADATA_CHUNK_SIZE) {
            stream
                .0
                .write_all(chunk)
                .await
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
        }
        Ok::<(), MetadataError>(())
    })
    .await
    .map_err(|_| MetadataError::Backend("timed out writing metadata body".to_string()))?
}

pub(super) async fn close_stream(stream: &mut BiStream) {
    let _ = stream.0.finish();
    let _ = stream.1.stop(0u32.into());
}

pub(super) fn close_stream_at(stream: &mut BiStream, deadline: tokio::time::Instant) {
    if tokio::time::Instant::now() < deadline {
        let _ = stream.0.finish();
    }
    let _ = stream.1.stop(0u32.into());
}

pub(super) async fn write_message_at(
    stream: &mut BiStream,
    message: &MetadataTransportMessage,
    deadline: tokio::time::Instant,
) -> Result<(), MetadataError> {
    timeout_at(deadline, write_message(stream, message))
        .await
        .map_err(|_| MetadataError::Backend("timed out writing metadata message".to_string()))?
        .map_err(MetadataError::Backend)
}

pub(super) async fn write_body_at(
    stream: &mut BiStream,
    bytes: &[u8],
    deadline: tokio::time::Instant,
) -> Result<(), MetadataError> {
    timeout_at(deadline, async {
        for chunk in bytes.chunks(METADATA_CHUNK_SIZE) {
            stream
                .0
                .write_all(chunk)
                .await
                .map_err(|error| MetadataError::Backend(error.to_string()))?;
        }
        Ok::<(), MetadataError>(())
    })
    .await
    .map_err(|_| MetadataError::Backend("timed out writing metadata body".to_string()))?
}

pub(super) async fn drain_stream_at(
    reader: &mut RecvStream,
    deadline: tokio::time::Instant,
) -> Result<(), MetadataError> {
    timeout_at(deadline, reader.read_to_end(1))
        .await
        .map_err(|_| {
            MetadataError::Backend("timed out draining metadata request stream".to_string())
        })?
        .map(|_| ())
        .map_err(|error| MetadataError::Backend(error.to_string()))
}

pub(super) async fn drain_request_stream(stream: &mut BiStream) -> Result<(), MetadataError> {
    timeout(METADATA_IO_TIMEOUT, stream.1.read_to_end(1))
        .await
        .map_err(|_| {
            MetadataError::Backend("timed out draining metadata request stream".to_string())
        })?
        .map(|_| ())
        .map_err(|error| MetadataError::Backend(error.to_string()))
}
