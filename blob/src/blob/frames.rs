//! Reads framed copies: checks the index and each frame, then decodes only the frames a range needs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use crate::codec::{self, FRAME_SIZE, FrameEntry, FrameIndex};
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::BackendStream;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::format::FrameLayout;
use bytes::Bytes;
use futures::stream;
use opendal::Operator;
use std::ops::Range;
use std::time::Duration;
use tokio::time::timeout;

/// One loaded group: its number, checked entries and the stored offset of each frame.
struct LoadedGroup {
    group: u64,
    entries: Vec<FrameEntry>,
    offsets: Vec<u64>,
}

/// Walks the frames of one range; `expected` holds the hash a full read must match.
struct FrameCursor {
    operator: Operator,
    path: String,
    index: FrameIndex,
    size: u64,
    position: u64,
    end: u64,
    loaded: Option<LoadedGroup>,
    expected: Option<(blake3::Hasher, [u8; 32])>,
    idle: Duration,
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

impl FrameCursor {
    async fn load_group(&mut self, group: u64) -> Result<(), BlobError> {
        if self
            .loaded
            .as_ref()
            .is_some_and(|loaded| loaded.group == group)
        {
            return Ok(());
        }
        let bytes = read_range(
            &self.operator,
            &self.path,
            self.index.entries_range(group),
            self.idle,
        )
        .await?;
        let entries = self.index.entries(group, &bytes)?;
        let mut offset = self.index.group_start(group);
        let offsets = entries
            .iter()
            .map(|entry| {
                let start = offset;
                offset += entry.stored_len();
                start
            })
            .collect();
        self.loaded = Some(LoadedGroup {
            group,
            entries,
            offsets,
        });
        Ok(())
    }

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
        let frame = self.position / FRAME_SIZE;
        let (group, slot) = codec::group_of(frame);
        self.load_group(group).await?;
        let loaded = self
            .loaded
            .as_ref()
            .ok_or_else(|| BlobError::ReadError("frame group is not loaded".to_string()))?;
        let entry = loaded.entries[slot].clone();
        let start = loaded.offsets[slot];
        let stored = read_range(
            &self.operator,
            &self.path,
            start..start + entry.stored_len(),
            self.idle,
        )
        .await?;
        let length = codec::frame_len(self.size, frame);
        let decoded =
            tokio::task::spawn_blocking(move || codec::decode_frame(&entry, length, stored))
                .await
                .map_err(|error| BlobError::ReadError(error.to_string()))??;
        let frame_start = frame * FRAME_SIZE;
        let from = (self.position - frame_start) as usize;
        let to = (self.end - frame_start).min(length) as usize;
        let piece = decoded.slice(from..to);
        if let Some((hasher, _)) = self.expected.as_mut() {
            hasher.update(&piece);
        }
        self.position = frame_start + to as u64;
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
        let operator = self.operator_from_location(location)?;
        let path = location.get_storage_path()?;
        let idle = self.transfer_idle_timeout();
        let tail = codec::tail_range(size, layout)?;
        let tail = read_range(&operator, &path, tail, idle).await?;
        let index = FrameIndex::parse(size, layout, &tail)?;
        Ok(FrameCursor {
            operator,
            path,
            index,
            size,
            position: range.start,
            end: range.end,
            loaded: None,
            expected,
            idle,
        })
    }
}
