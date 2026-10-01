//! Reads framed copies: checks the seek table, then decodes only the frames needed.
//! Also gives bao transfers random access to the original bytes of any copy.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use crate::bao_tree::OpenDalReader;
use crate::codec::{self, FRAME_SIZE, FrameIndex};
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::BackendStream;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::format::{FrameLayout, StoredLayout};
use bytes::{Bytes, BytesMut};
use futures::stream;
use iroh_io::AsyncSliceReader;
use opendal::Operator;
use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// Random access to the original bytes of a framed copy. Keeps the last decoded
/// frame, so sequential reads decode each frame once.
pub(super) struct FrameReader {
    operator: Operator,
    path: String,
    index: Arc<FrameIndex>,
    size: u64,
    decoded: Option<(u64, Bytes)>,
    idle: Duration,
}

/// Walks the frames of one range; `expected` holds the hash a full read must match.
struct FrameCursor {
    reader: FrameReader,
    position: u64,
    end: u64,
    expected: Option<(blake3::Hasher, [u8; 32])>,
}

/// Reads one byte range on its own task, so the stream that awaits it stays `Sync`.
async fn read_range(
    operator: &Operator,
    path: &str,
    range: Range<u64>,
    idle: Duration,
) -> Result<Bytes, BlobError> {
    if range.is_empty() {
        return Ok(Bytes::new());
    }
    let (operator, path) = (operator.clone(), path.to_string());
    let mut task = tokio::spawn(async move { operator.read_with(&path).range(range).await });
    let buffer = match timeout(idle, &mut task).await {
        Ok(joined) => joined
            .map_err(|error| BlobError::ReadError(error.to_string()))?
            .map_err(|error| BlobError::ReadError(error.to_string()))?,
        Err(_) => {
            task.abort();
            return Err(BlobError::ReadError("frame read idle timeout".to_string()));
        }
    };
    Ok(buffer.to_bytes())
}

impl FrameReader {
    /// The original bytes of one frame.
    async fn frame(&mut self, frame: u64) -> Result<Bytes, BlobError> {
        if let Some((cached, bytes)) = &self.decoded
            && *cached == frame
        {
            return Ok(bytes.clone());
        }
        let range = self.index.frame_range(frame);
        let stored = read_range(&self.operator, &self.path, range, self.idle).await?;
        let length = codec::frame_len(self.size, frame);
        let decoded = tokio::task::spawn_blocking(move || codec::decode_frame(length, stored))
            .await
            .map_err(|error| BlobError::ReadError(error.to_string()))??;
        self.decoded = Some((frame, decoded.clone()));
        Ok(decoded)
    }

    /// Original bytes from `start`, up to `end` and the end of that frame.
    async fn piece(&mut self, start: u64, end: u64) -> Result<Bytes, BlobError> {
        let frame = start / FRAME_SIZE;
        let decoded = self.frame(frame).await?;
        let frame_start = frame * FRAME_SIZE;
        let to = (end - frame_start).min(decoded.len() as u64);
        Ok(decoded.slice((start - frame_start) as usize..to as usize))
    }
}

impl AsyncSliceReader for FrameReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let len = len.min(self.size.saturating_sub(offset) as usize);
        self.read_exact_at(offset, len).await
    }

    async fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let end = offset
            .checked_add(len as u64)
            .filter(|end| *end <= self.size)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let mut out = BytesMut::with_capacity(len);
        let mut position = offset;
        while position < end {
            let piece = self.piece(position, end).await.map_err(io::Error::other)?;
            position += piece.len() as u64;
            out.extend_from_slice(&piece);
        }
        Ok(out.freeze())
    }

    async fn size(&mut self) -> io::Result<u64> {
        Ok(self.size)
    }
}

/// Source of a bao transfer: the stored bytes of a raw copy, or decoded frames.
pub(super) enum SliceReader {
    Raw(OpenDalReader),
    Framed(FrameReader),
}

impl AsyncSliceReader for SliceReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        match self {
            Self::Raw(reader) => reader.read_at(offset, len).await,
            Self::Framed(reader) => reader.read_at(offset, len).await,
        }
    }

    async fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        match self {
            Self::Raw(reader) => reader.read_exact_at(offset, len).await,
            Self::Framed(reader) => reader.read_exact_at(offset, len).await,
        }
    }

    async fn size(&mut self) -> io::Result<u64> {
        match self {
            Self::Raw(reader) => reader.size().await,
            Self::Framed(reader) => reader.size().await,
        }
    }
}

impl FrameCursor {
    async fn next(&mut self) -> Result<Option<Bytes>, BlobError> {
        if self.position >= self.end {
            if let Some((hasher, expected)) = self.expected.take()
                && hasher.finalize().as_bytes() != &expected
            {
                return Err(BlobError::IntegrityCheckFailed(
                    "blake3 hash mismatch".to_string(),
                ));
            }
            return Ok(None);
        }
        let piece = self.reader.piece(self.position, self.end).await?;
        if let Some((hasher, _)) = self.expected.as_mut() {
            hasher.update(&piece);
        }
        self.position += piece.len() as u64;
        Ok(Some(piece))
    }
}

impl BlobHandler {
    /// Streams the original bytes in `range` of a framed copy. A full read also
    /// checks the BLAKE3 of the whole object at its end.
    pub(super) async fn read_frames(
        &self,
        location: &BackendLocation,
        layout: &FrameLayout,
        range: Range<u64>,
    ) -> BlobEvent {
        match self.open_frames(location, layout, range).await {
            Ok(cursor) => {
                let stream_size = cursor.end - cursor.position;
                BlobEvent::ReadFinished {
                    blob: BackendStream::new(stream::try_unfold(cursor, |mut cursor| async move {
                        Ok::<_, BlobError>(cursor.next().await?.map(|bytes| (bytes, cursor)))
                    })),
                    stream_size,
                }
            }
            Err(error) => BlobEvent::Error(error),
        }
    }

    async fn open_frames(
        &self,
        location: &BackendLocation,
        layout: &FrameLayout,
        range: Range<u64>,
    ) -> Result<FrameCursor, BlobError> {
        let size = location.blob_size;
        if range.start > range.end || range.end > size {
            return Err(BlobError::ReadError(
                "range is outside the blob".to_string(),
            ));
        }
        let expected = match (range.start, range.end) {
            (0, end) if end == size => {
                let hash = location.get_blake3().ok_or_else(|| {
                    BlobError::IntegrityCheckFailed("missing stored blake3 hash".to_string())
                })?;
                let hash: [u8; 32] = hash.try_into().map_err(|_| {
                    BlobError::IntegrityCheckFailed("invalid stored blake3 hash".to_string())
                })?;
                Some((blake3::Hasher::new(), hash))
            }
            _ => None,
        };
        Ok(FrameCursor {
            reader: self.frame_reader(location, layout).await?,
            position: range.start,
            end: range.end,
            expected,
        })
    }

    /// Opens a framed copy with its seek table.
    pub(super) async fn frame_reader(
        &self,
        location: &BackendLocation,
        layout: &FrameLayout,
    ) -> Result<FrameReader, BlobError> {
        let size = location.blob_size;
        let operator = self.operator_from_location(location)?;
        let path = location.get_storage_path()?;
        let idle = self.transfer_idle_timeout();
        let index = self
            .frame_index(&operator, &path, size, layout, idle)
            .await?;
        Ok(FrameReader {
            operator,
            path,
            index,
            size,
            decoded: None,
            idle,
        })
    }

    /// The seek table, read whole and checked against the record.
    async fn frame_index(
        &self,
        operator: &Operator,
        path: &str,
        size: u64,
        layout: &FrameLayout,
        idle: Duration,
    ) -> Result<Arc<FrameIndex>, BlobError> {
        let table = codec::table_range(size, layout)?;
        let table = read_range(operator, path, table, idle).await?;
        Ok(Arc::new(FrameIndex::parse(size, layout, &table)?))
    }

    /// Reader over the original bytes of any copy, for bao transfers.
    pub(super) async fn slice_reader(
        &self,
        location: &BackendLocation,
    ) -> Result<SliceReader, BlobError> {
        if let StoredLayout::Frames(layout) = &location.format.layout {
            let reader = self.frame_reader(location, layout).await?;
            return Ok(SliceReader::Framed(reader));
        }
        let operator = self.operator_from_location(location)?;
        let path = location.get_storage_path()?;
        let idle = self.transfer_idle_timeout();
        OpenDalReader::new(&operator, &path, location.blob_size, idle)
            .await
            .map(SliceReader::Raw)
            .map_err(|error| BlobError::OperatorCreationFailed(error.to_string()))
    }
}
