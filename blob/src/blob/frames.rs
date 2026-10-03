//! Reads framed copies: checks the seek table, then decodes only the frames needed.
//! Also gives bao transfers random access to the original bytes of any copy.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use crate::bao_tree::OpenDalReader;
use crate::codec::{self, FrameIndex};
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::BackendStream;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::format::{FrameLayout, StoredLayout};
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, stream};
use iroh_io::AsyncSliceReader;
use lru::LruCache;
use opendal::Operator;
use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout;

/// Upper bound for the parsed seek tables kept in memory, about 1.3 TiB of framed data.
const INDEX_CACHE_BYTES: usize = 64 << 20;
/// Stored bytes one backend request fetches for consecutive frames, and their decoded bytes.
const FETCH_BYTES: u64 = 8 << 20;
/// Frames of one batch that decode on the blocking pool at the same time.
const DECODE_TASKS: usize = 4;

/// Parsed seek tables keyed by their hash. Stored objects never change, so entries stay valid.
#[derive(Debug)]
pub(super) struct IndexCache {
    entries: LruCache<[u8; 32], Arc<FrameIndex>>,
    bytes: usize,
}

impl IndexCache {
    pub(super) fn new() -> Self {
        Self {
            entries: LruCache::unbounded(),
            bytes: 0,
        }
    }

    fn get(&mut self, hash: &[u8; 32]) -> Option<Arc<FrameIndex>> {
        self.entries.get(hash).cloned()
    }

    fn insert(&mut self, hash: [u8; 32], index: Arc<FrameIndex>) {
        let memory = index.memory();
        if memory > INDEX_CACHE_BYTES {
            return;
        }
        if let Some((_, old)) = self.entries.push(hash, index) {
            self.bytes -= old.memory();
        }
        self.bytes += memory;
        while self.bytes > INDEX_CACHE_BYTES {
            let Some((_, old)) = self.entries.pop_lru() else {
                break;
            };
            self.bytes -= old.memory();
        }
    }
}

/// A running fetch of stored frame bytes.
type Fetch = JoinHandle<Result<Bytes, BlobError>>;

/// Random access to the original bytes of a framed copy. Fetches the stored bytes of
/// consecutive frames in one request, decodes a few in parallel, and fetches the next batch
/// of a longer read while the current one is decoded.
pub(super) struct FrameReader {
    operator: Operator,
    path: String,
    index: Arc<FrameIndex>,
    size: u64,
    /// The first frame of the decoded batch, and its frames.
    decoded: Option<(u64, Vec<Bytes>)>,
    ahead: Option<(Range<u64>, Fetch)>,
    idle: Duration,
}

impl Drop for FrameReader {
    fn drop(&mut self) {
        if let Some((_, task)) = self.ahead.take() {
            task.abort();
        }
    }
}

/// Walks the frames of one range.
struct FrameCursor {
    reader: FrameReader,
    position: u64,
    end: u64,
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
    /// Starts fetching the stored bytes of `frames` on its own task.
    fn fetch(&self, frames: Range<u64>) -> (Range<u64>, Fetch) {
        let range = self.index.frames_range(&frames);
        let (operator, path, idle) = (self.operator.clone(), self.path.clone(), self.idle);
        let task = tokio::spawn(async move {
            let expected = range.end - range.start;
            let bytes = read_range(&operator, &path, range, idle).await?;
            match bytes.len() as u64 == expected {
                true => Ok(bytes),
                false => Err(BlobError::ReadError("short frame read".to_string())),
            }
        });
        (frames, task)
    }

    /// The original bytes of one frame; `last` is the last frame the caller still needs.
    async fn frame(&mut self, frame: u64, last: u64) -> Result<Bytes, BlobError> {
        if let Some((first, frames)) = &self.decoded
            && let Some(bytes) = frame
                .checked_sub(*first)
                .and_then(|at| frames.get(at as usize))
        {
            return Ok(bytes.clone());
        }
        self.decoded = None;
        let (frames, task) = match self.ahead.take() {
            Some((frames, task)) if frames.start == frame => (frames, task),
            other => {
                if let Some((_, task)) = other {
                    task.abort();
                }
                self.fetch(self.index.fetch_frames(frame, last, FETCH_BYTES))
            }
        };
        let stored = task
            .await
            .map_err(|error| BlobError::ReadError(error.to_string()))??;
        if frames.end <= last {
            let next = self.index.fetch_frames(frames.end, last, FETCH_BYTES);
            self.ahead = Some(self.fetch(next));
        }
        let decoded = self.decode(&frames, stored).await?;
        let bytes = decoded[0].clone();
        self.decoded = Some((frames.start, decoded));
        Ok(bytes)
    }

    /// Checks and decodes each frame of one fetched batch on the blocking pool,
    /// at most `DECODE_TASKS` at a time.
    async fn decode(&self, frames: &Range<u64>, stored: Bytes) -> Result<Vec<Bytes>, BlobError> {
        let index = self.index.clone();
        let base = index.frames_range(frames).start;
        let tasks = frames.clone().map(move |frame| {
            let range = index.frame_range(frame);
            let bytes = stored.slice((range.start - base) as usize..(range.end - base) as usize);
            let (length, digest) = (index.original_len(frame), *index.digest(frame));
            tokio::task::spawn_blocking(move || codec::decode_frame(length, &digest, bytes))
        });
        let mut tasks = stream::iter(tasks).buffered(DECODE_TASKS);
        let mut decoded = Vec::new();
        while let Some(task) = tasks.next().await {
            decoded.push(task.map_err(|error| BlobError::ReadError(error.to_string()))??);
        }
        Ok(decoded)
    }

    /// Original bytes from `start`, up to `end` and the end of that frame.
    async fn piece(&mut self, start: u64, end: u64) -> Result<Bytes, BlobError> {
        let frame = self.index.frame_at(start);
        let decoded = self.frame(frame, self.index.frame_at(end - 1)).await?;
        let frame_start = self.index.original_range(frame).start;
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
            return Ok(None);
        }
        let piece = self.reader.piece(self.position, self.end).await?;
        self.position += piece.len() as u64;
        Ok(Some(piece))
    }
}

impl BlobHandler {
    /// Streams the original bytes in `range` of a framed copy. Every frame is checked
    /// against its digest in the authenticated seek table before any of its bytes are returned,
    /// so a full read needs no separate check of the whole object's BLAKE3.
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
        Ok(FrameCursor {
            reader: self.frame_reader(location, layout).await?,
            position: range.start,
            end: range.end,
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
            ahead: None,
            idle,
        })
    }

    /// The seek table from the cache, or read whole and checked against the record.
    async fn frame_index(
        &self,
        operator: &Operator,
        path: &str,
        size: u64,
        layout: &FrameLayout,
        idle: Duration,
    ) -> Result<Arc<FrameIndex>, BlobError> {
        let cached =
            (self.frame_indexes.lock().ok()).and_then(|mut cache| cache.get(&layout.index_hash));
        if let Some(index) = cached.filter(|index| index.size() == size) {
            return Ok(index);
        }
        let table = codec::table_range(size, layout)?;
        let table = read_range(operator, path, table, idle).await?;
        let index = Arc::new(FrameIndex::parse(size, layout, &table)?);
        if let Ok(mut cache) = self.frame_indexes.lock() {
            cache.insert(layout.index_hash, index.clone());
        }
        Ok(index)
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
