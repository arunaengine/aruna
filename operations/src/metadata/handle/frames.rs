//! Writes and reads length-prefixed metadata protocol frames on a peer stream.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_net::streams::BiStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::metadata::protocol::{
    AUDIT_FRAME, MAX_MESSAGE_SIZE, MetadataTransportMessage, POSTCARD_VARIANT_BYTES,
    STANDARD_FRAME, audit_frame_cap, decode_message, encode_message, frame_class, is_audit_variant,
    parse_variant,
};

pub(crate) async fn write_message(
    stream: &mut BiStream,
    message: &MetadataTransportMessage,
) -> Result<(), String> {
    let bytes = encode_message(message)?;
    write_encoded_message(stream, frame_class(message), &bytes).await
}

pub(crate) async fn write_encoded_message(
    stream: &mut BiStream,
    class: u8,
    bytes: &[u8],
) -> Result<(), String> {
    if class != STANDARD_FRAME && class != AUDIT_FRAME {
        return Err("invalid metadata frame class".to_string());
    }
    stream
        .0
        .write_all(&[class])
        .await
        .map_err(|err| err.to_string())?;
    stream
        .0
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .map_err(|err| err.to_string())?;
    stream
        .0
        .write_all(bytes)
        .await
        .map_err(|err| err.to_string())?;
    stream.0.flush().await.map_err(|err| err.to_string())?;
    Ok(())
}

pub async fn read_message(stream: &mut BiStream) -> Result<MetadataTransportMessage, String> {
    read_message_cap(&mut stream.1, MAX_MESSAGE_SIZE).await
}

pub(crate) async fn read_message_budget<R>(
    reader: &mut R,
    max_size: usize,
    byte_budget: &Arc<Semaphore>,
) -> Result<(MetadataTransportMessage, OwnedSemaphorePermit), String>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let (message, permit) = read_message_inner(reader, max_size, Some(byte_budget)).await?;
    permit
        .ok_or_else(|| "metadata inbound frame budget missing".to_string())
        .map(|permit| (message, permit))
}

pub(crate) async fn read_message_cap<R>(
    reader: &mut R,
    max_size: usize,
) -> Result<MetadataTransportMessage, String>
where
    R: AsyncRead + Unpin + ?Sized,
{
    read_message_inner(reader, max_size, None)
        .await
        .map(|(message, _)| message)
}

async fn read_message_inner<R>(
    reader: &mut R,
    max_size: usize,
    byte_budget: Option<&Arc<Semaphore>>,
) -> Result<(MetadataTransportMessage, Option<OwnedSemaphorePermit>), String>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut class_buf = [0u8; 1];
    reader
        .read_exact(&mut class_buf)
        .await
        .map_err(|err| err.to_string())?;
    let class = class_buf[0];
    if class != STANDARD_FRAME && class != AUDIT_FRAME {
        return Err("invalid metadata frame class".to_string());
    }

    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| err.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_size.min(MAX_MESSAGE_SIZE) {
        return Err("metadata frame exceeds maximum size".to_string());
    }
    if class == AUDIT_FRAME && len > audit_frame_cap() {
        return Err("metadata audit frame exceeds maximum size".to_string());
    }

    let permit = match byte_budget {
        Some(byte_budget) => {
            let permits = u32::try_from(len)
                .map_err(|_| "metadata frame length is unsupported".to_string())?;
            Some(
                byte_budget
                    .clone()
                    .try_acquire_many_owned(permits)
                    .map_err(|_| "metadata inbound frame budget unavailable".to_string())?,
            )
        }
        None => None,
    };

    let mut variant = [0u8; POSTCARD_VARIANT_BYTES];
    let variant_len = len.min(POSTCARD_VARIANT_BYTES);
    let mut prefix_len = 0;
    if variant_len > 0 {
        reader
            .read_exact(&mut variant[..1])
            .await
            .map_err(|err| err.to_string())?;
        prefix_len = 1;
        while prefix_len < variant_len && variant[prefix_len - 1] & 0x80 != 0 {
            reader
                .read_exact(&mut variant[prefix_len..prefix_len + 1])
                .await
                .map_err(|err| err.to_string())?;
            prefix_len += 1;
        }
        let variant = parse_variant(&variant[..prefix_len])
            .map_err(|_| "metadata frame variant is invalid".to_string())?;
        if len > audit_frame_cap() && variant.is_some_and(is_audit_variant) {
            return Err("metadata audit frame exceeds maximum size".to_string());
        }
    }

    let mut bytes = vec![0u8; len];
    bytes[..prefix_len].copy_from_slice(&variant[..prefix_len]);
    reader
        .read_exact(&mut bytes[prefix_len..])
        .await
        .map_err(|err| err.to_string())?;
    let message = decode_message(&bytes).map_err(|err| err.to_string())?;
    let decoded_class = frame_class(&message);
    if decoded_class != class {
        return Err("metadata frame class does not match message".to_string());
    }
    if decoded_class == AUDIT_FRAME && len > audit_frame_cap() {
        return Err("metadata audit frame exceeds maximum size".to_string());
    }
    Ok((message, permit))
}
