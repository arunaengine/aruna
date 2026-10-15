//! Sends the parts of compressed uploads to the provider as frames and ends them with the seek
//! table. A part below 5 MiB once framed is padded unless it is the last one.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::frames::read_range;
use crate::codec::{FRAME_SIZE, FrameEncoder, FrameWriter, PART_MIN, padding, padding_len};
use crate::s3::NativeMultipart;
use aruna_core::errors::BlobError;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::format::{FrameLayout, PartFrame, PartLayout, StoredLayout};
use aruna_core::structs::storage::multipart::{BackendUpload, MultipartPart};
use bytes::Bytes;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt, stream};
use tokio::time::timeout;

/// Largest part framed in memory to learn its stored size; a larger one waits in a blob.
const BUFFER_MAX: u64 = 64 << 20;
/// The provider part that takes the seek table when the last listed part was streamed.
const TABLE_PART: u16 = 10_000;

/// The zstd level of a compressed provider upload; `None` for an upload of raw parts.
pub(super) fn part_level(upload: &BackendUpload) -> Option<u8> {
    match &upload.location.format.layout {
        StoredLayout::FramedPart(layout) => Some(layout.level),
        _ => None,
    }
}

/// Whether a compressed part streams into the provider now. A part below 5 MiB and part 10000
/// can only be last, so they wait in a blob for the seek table, as do parts too large to buffer.
pub(super) fn streams_now(part_number: u16, size: Option<u64>) -> bool {
    part_number < TABLE_PART && size.is_some_and(|size| (PART_MIN..=BUFFER_MAX).contains(&size))
}

impl BlobHandler {
    /// Frames one part in memory, pads it to the part minimum and sends it as one provider part.
    pub(super) async fn upload_framed(
        &self,
        upload: &BackendUpload,
        part_number: u16,
        size: u64,
        level: u8,
        mut body: BackendStream<Result<Bytes, StreamError>>,
    ) -> Result<(String, PartLayout), BlobError> {
        let _share = self.reserve_pithos(size + size / 1024 + FRAME_SIZE).await?;
        let mut encoder = FrameEncoder::new(level);
        let mut pieces = Vec::new();
        let mut received = 0u64;
        while let Some(chunk) = body.0.next().await {
            let bytes = chunk.map_err(|error| BlobError::StreamFailed(error.to_string()))?;
            received += bytes.len() as u64;
            if received > size {
                break;
            }
            pieces.extend(encoder.push(&bytes).await?);
        }
        if received != size {
            return Err(BlobError::StreamFailed(
                "body size did not match Content-Length".to_string(),
            ));
        }
        let (last, frames) = encoder.finish_part().await?;
        pieces.extend(last);
        let stored: u64 = pieces.iter().map(|piece| piece.len() as u64).sum();
        let pad = padding_len(stored);
        if pad > 0 {
            pieces.push(padding(pad));
        }
        let native = self.native_upload(&upload.location)?;
        let path = upload.location.get_storage_path()?;
        let body = BackendStream(Box::pin(stream::iter(pieces.into_iter().map(Ok))));
        let length = stored + u64::from(pad);
        let etag = native
            .upload_part(&path, &upload.upload_id, part_number, length, body)
            .await?;
        let layout = PartLayout {
            level,
            frames,
            padding: pad,
        };
        Ok((etag, layout))
    }

    /// Completes a compressed upload. Staged parts are framed for their entries, then framed
    /// again while they are sent; the seek table goes last.
    pub(super) async fn finish_framed(
        &self,
        upload: &BackendUpload,
        parts: &[MultipartPart],
        level: u8,
    ) -> Result<FrameLayout, BlobError> {
        let Some(last) = parts.last() else {
            return Err(BlobError::WriteError(
                "the upload lists no part".to_string(),
            ));
        };
        let mut writer = FrameWriter::new(level);
        let mut etags = Vec::with_capacity(parts.len() + 1);
        let mut staged = Vec::new();
        for part in parts {
            let number = part.part_number;
            match (&part.location.format.layout, part.location.partial) {
                (StoredLayout::FramedPart(layout), true) => {
                    let etag = part.backend_etag.clone().ok_or_else(|| {
                        BlobError::WriteError(format!("part {number} has no provider ETag"))
                    })?;
                    for frame in &layout.frames {
                        writer.entry(frame.clone())?;
                    }
                    if layout.padding > 0 {
                        writer.pad(layout.padding)?;
                    }
                    etags.push((number, etag));
                }
                (StoredLayout::Raw, false) => {
                    let frames = self.frame_blob(&part.location, level, None).await?;
                    let stored = frames.iter().map(|frame| u64::from(frame.stored)).sum();
                    let pad = match std::ptr::eq(part, last) {
                        true => 0,
                        false => padding_len(stored),
                    };
                    for frame in &frames {
                        writer.entry(frame.clone())?;
                    }
                    if pad > 0 {
                        writer.pad(pad)?;
                    }
                    staged.push((etags.len(), part, frames, pad));
                    etags.push((number, String::new()));
                }
                _ => {
                    return Err(BlobError::WriteError(format!(
                        "part {number} holds no frames of this upload"
                    )));
                }
            }
        }
        let (table, layout) = writer.finish()?;
        let table = Bytes::from(table);
        let native = self.native_upload(&upload.location)?;
        let path = upload.location.get_storage_path()?;
        let sent = async {
            for (slot, part, frames, pad) in &staged {
                let mut tail = Vec::new();
                if *pad > 0 {
                    tail.push(padding(*pad));
                }
                if std::ptr::eq(*part, last) {
                    tail.push(table.clone());
                }
                let send = self.send_staged(&native, &path, upload, part, level, frames, tail);
                etags[*slot].1 = send.await?;
            }
            if last.location.partial {
                let body = BackendStream(Box::pin(stream::iter([Ok(table.clone())])));
                let length = table.len() as u64;
                let etag = native
                    .upload_part(&path, &upload.upload_id, TABLE_PART, length, body)
                    .await?;
                etags.push((TABLE_PART, etag));
            }
            native.complete(&path, &upload.upload_id, &etags).await
        }
        .await;
        // A completion whose answer was lost leaves the object; its seek table names every frame.
        if let Err(error) = sent
            && !self
                .holds_table(&upload.location, &path, &layout, &table)
                .await
        {
            return Err(error);
        }
        Ok(layout)
    }

    /// Sends a staged part framed again, then `tail`, and checks it framed as before.
    #[allow(clippy::too_many_arguments)]
    async fn send_staged(
        &self,
        native: &NativeMultipart,
        path: &str,
        upload: &BackendUpload,
        part: &MultipartPart,
        level: u8,
        frames: &[PartFrame],
        tail: Vec<Bytes>,
    ) -> Result<String, BlobError> {
        let stored = frames
            .iter()
            .map(|frame| u64::from(frame.stored))
            .sum::<u64>()
            + tail.iter().map(|piece| piece.len() as u64).sum::<u64>();
        let (sink, receiver) = mpsc::channel(4);
        let body = receiver.chain(stream::iter(tail.into_iter().map(Ok)));
        let send = native.upload_part(
            path,
            &upload.upload_id,
            part.part_number,
            stored,
            BackendStream(Box::pin(body)),
        );
        let (framed, sent) = tokio::join!(self.frame_blob(&part.location, level, Some(sink)), send);
        if framed? != frames {
            return Err(BlobError::IntegrityCheckFailed(
                "a staged part framed differently".to_string(),
            ));
        }
        sent
    }

    /// Frames the raw bytes of a staged part and sends the stored bytes to `sink` when given.
    async fn frame_blob(
        &self,
        location: &BackendLocation,
        level: u8,
        mut sink: Option<mpsc::Sender<Result<Bytes, StreamError>>>,
    ) -> Result<Vec<PartFrame>, BlobError> {
        let read = |error: opendal::Error| BlobError::ReadError(error.to_string());
        let late = || BlobError::ReadError("timed out reading a staged part".to_string());
        let stopped = |_| BlobError::WriteError("the part upload stopped".to_string());
        let operator = self.operator_from_location(location)?;
        let path = location.get_storage_path()?;
        let reader = timeout(self.io_timeout(), operator.reader(&path))
            .await
            .map_err(|_| late())?
            .map_err(read)?;
        let mut stream = timeout(self.io_timeout(), reader.into_bytes_stream(..))
            .await
            .map_err(|_| late())?
            .map_err(read)?;
        let mut encoder = FrameEncoder::new(level);
        while let Some(chunk) = timeout(self.transfer_idle_timeout(), stream.next())
            .await
            .map_err(|_| late())?
        {
            let chunk = chunk.map_err(|error| BlobError::ReadError(error.to_string()))?;
            for piece in encoder.push(&chunk).await? {
                if let Some(sink) = sink.as_mut() {
                    sink.send(Ok(piece)).await.map_err(stopped)?;
                }
            }
        }
        let (last, frames) = encoder.finish_part().await?;
        if let (Some(sink), Some(last)) = (sink.as_mut(), last) {
            sink.send(Ok(last)).await.map_err(stopped)?;
        }
        let original: u64 = frames.iter().map(|frame| u64::from(frame.original)).sum();
        if original != location.blob_size {
            return Err(BlobError::IntegrityCheckFailed(
                "a staged part has the wrong size".to_string(),
            ));
        }
        Ok(frames)
    }

    /// Whether the object at `path` is this completion: its size and seek table match.
    async fn holds_table(
        &self,
        location: &BackendLocation,
        path: &str,
        layout: &FrameLayout,
        table: &Bytes,
    ) -> bool {
        let Ok(operator) = self.operator_from_location(location) else {
            return false;
        };
        match timeout(self.io_timeout(), operator.stat(path)).await {
            Ok(Ok(metadata)) if metadata.content_length() == layout.stored_size => {}
            _ => return false,
        }
        let range = layout.stored_size - table.len() as u64..layout.stored_size;
        let idle = self.transfer_idle_timeout();
        read_range(&operator, path, range, idle)
            .await
            .is_ok_and(|tail| tail == *table)
    }
}
