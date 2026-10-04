//! Writes and reads Pithos copies of encrypted objects. The backend serves byte ranges to the
//! Pithos reader, and sealing, decryption and decoding run on the blocking pool.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::frames::read_range;
use aruna_core::errors::BlobError;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::storage::format::{Compression, PithosLayout};
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use opendal::Operator;
use pithos_lib::archive::{
    AccessKeys, ArchivePath, AsyncArchive, BlockingHook, CdcConfig, Chunking, Composition,
    EntryMetadata, OpenOptions, Piece, PieceEncoder, ProcessingOptions, compose,
};
use pithos_lib::crypto::PublicKey;
use pithos_lib::error::PithosError;
use pithos_lib::source::{AsyncArchiveSource, SourceError};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

/// Path of the one file in the archive of an object.
pub const OBJECT_PATH: &str = "object";

const MIB: usize = 1 << 20;
/// Plaintext gathered before each call into the encoder, so small chunks share one pool task.
const BATCH: usize = MIB;
/// Zstd levels that the Pithos compression levels 1 to 7 stand for.
const PITHOS_ZSTD: [u8; 7] = [1, 4, 8, 11, 15, 18, 22];

/// What a Pithos write stored: the original size, its plaintext BLAKE3 and the copy's record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PithosWrite {
    pub size: u64,
    pub content_hash: [u8; 32],
    pub layout: PithosLayout,
}

/// One stored archive. Stored objects never change, so reads need no pinned revision.
struct StoredArchive {
    operator: Operator,
    path: String,
    idle: Duration,
}

impl AsyncArchiveSource for StoredArchive {
    async fn len(&self) -> Result<u64, SourceError> {
        let metadata = self.operator.stat(&self.path).await;
        metadata
            .map(|metadata| metadata.content_length())
            .map_err(|error| SourceError::Remote {
                offset: 0,
                message: error.to_string(),
            })
    }

    async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, SourceError> {
        let end = offset.checked_add(len).ok_or(SourceError::RangeOverflow {
            offset,
            length: usize::try_from(len).unwrap_or(usize::MAX),
        })?;
        let bytes = read_range(&self.operator, &self.path, offset..end, self.idle).await;
        bytes
            .map(|bytes| bytes.to_vec())
            .map_err(|error| SourceError::Remote {
                offset,
                message: error.to_string(),
            })
    }
}

/// Runs the CPU work of the Pithos reader on Tokio's blocking pool.
struct TokioBlocking;

impl BlockingHook for TokioBlocking {
    async fn spawn_blocking<F, T>(&self, task: F) -> T
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        match tokio::task::spawn_blocking(task).await {
            Ok(value) => value,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(error) => panic!("Pithos blocking task stopped: {error}"),
        }
    }
}

/// Streams the original bytes of `range` from the Pithos copy at `path`.
///
/// Opening fails before anything is decrypted when the archive does not match `layout`. `keys`
/// must open a grant of the object; every block is checked against its BLAKE3 when it is read.
pub async fn read(
    operator: Operator,
    path: String,
    layout: &PithosLayout,
    keys: AccessKeys,
    range: Range<u64>,
    idle: Duration,
) -> Result<impl Stream<Item = Result<Bytes, BlobError>> + Send + 'static, BlobError> {
    let source = StoredArchive {
        operator,
        path,
        idle,
    };
    let options = OpenOptions::default()
        .with_access_keys(keys)
        .with_expected_metadata_digest(layout.metadata_digest);
    let archive =
        AsyncArchive::open_with_hook(source, options, Some(layout.stored_size), TokioBlocking)
            .await
            .map_err(blob_error)?;
    let stream = Arc::new(archive)
        .read_range_owned(OBJECT_PATH, range)
        .map_err(blob_error)?;
    Ok(stream.map(|chunk| chunk.map(Bytes::from).map_err(blob_error)))
}

impl BlobHandler {
    /// Writes `blob` at `path` as a Pithos archive with one file, granted to `bucket`.
    ///
    /// Sealed blocks reach the backend as they are produced; a failed write removes the partial
    /// object. The content hash is the BLAKE3 of the original bytes.
    pub async fn write_pithos(
        &self,
        operator: &Operator,
        path: &str,
        bucket: PublicKey,
        compression: Compression,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> Result<PithosWrite, BlobError> {
        let encoder = piece_encoder(bucket, compression).map_err(write_error)?;
        let (mut writer, mut abandoned) = (None, false);
        let written = self
            .write_archive(operator, path, &mut writer, &mut abandoned, encoder, blob)
            .await;
        if written.is_err() {
            self.clean_partial(writer.as_mut(), abandoned, Some(operator), Some(path), None)
                .await?;
        }
        written
    }

    /// Writes the header, the sealed blocks and the directory. `writer` keeps the open writer
    /// for cleanup; `abandoned` marks a writer whose call a timeout cut off.
    async fn write_archive(
        &self,
        operator: &Operator,
        path: &str,
        writer: &mut Option<opendal::Writer>,
        abandoned: &mut bool,
        mut encoder: PieceEncoder,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> Result<PithosWrite, BlobError> {
        let header = compose_object(&[]).map_err(write_error)?.header();
        let writer = match timeout(self.io_timeout(), operator.writer(path)).await {
            Ok(Ok(opened)) => writer.insert(opened),
            Ok(Err(error)) => return Err(BlobError::OperatorCreationFailed(error.to_string())),
            Err(_) => return Err(deadline_expired()),
        };
        let idle = self.transfer_idle_timeout();
        settle(abandoned, idle, writer.write(header.to_vec())).await?;
        let (mut size, mut stored) = (0u64, header.len() as u64);
        let mut batch = BytesMut::new();
        loop {
            let chunk = match timeout(idle, blob.next()).await {
                Ok(Some(chunk)) => {
                    chunk.map_err(|error| BlobError::StreamFailed(error.to_string()))?
                }
                Ok(None) => break,
                Err(_) => return Err(deadline_expired()),
            };
            size = size
                .checked_add(chunk.len() as u64)
                .ok_or(BlobError::SizeLimitExceeded { limit: u64::MAX })?;
            batch.extend_from_slice(&chunk);
            if batch.len() < BATCH {
                continue;
            }
            let plain = batch.split().freeze();
            let sealed = TokioBlocking.spawn_blocking(move || {
                let mut encoder = encoder;
                encoder.write(&plain).map(|blocks| (encoder, blocks))
            });
            let blocks;
            (encoder, blocks) = sealed.await.map_err(write_error)?;
            stored += blocks.len() as u64;
            if !blocks.is_empty() {
                settle(abandoned, idle, writer.write(blocks)).await?;
            }
        }
        let plain = batch.freeze();
        let sealed = TokioBlocking.spawn_blocking(move || finish_piece(encoder, &plain));
        let (blocks, piece, composition) = sealed.await.map_err(write_error)?;
        stored += blocks.len() as u64;
        if !blocks.is_empty() {
            settle(abandoned, idle, writer.write(blocks)).await?;
        }
        let start = header.len() as u64;
        let consistent = composition.header() == header
            && composition.piece_offsets() == [start]
            && stored == start + piece.stored_len()
            && piece.original_size() == size;
        let (true, Some(content_hash)) = (consistent, composition.content_hash()) else {
            let message = "the Pithos archive does not match the written bytes";
            return Err(BlobError::WriteError(message.to_string()));
        };
        let directory = composition.directory().to_vec();
        settle(abandoned, idle, writer.write(directory)).await?;
        settle(abandoned, self.io_timeout(), writer.close()).await?;
        let layout = PithosLayout {
            stored_size: composition.archive_len(),
            metadata_digest: composition.metadata_digest(),
        };
        Ok(PithosWrite {
            size,
            content_hash,
            layout,
        })
    }
}

/// One piece of FastCDC blocks from 1 to 16 MiB with content-derived keys, granted to `bucket`.
fn piece_encoder(bucket: PublicKey, compression: Compression) -> Result<PieceEncoder, PithosError> {
    let blocks = CdcConfig::new(MIB, 4 * MIB, 16 * MIB)?;
    let processing = ProcessingOptions::new(true, pithos_level(compression))?;
    PieceEncoder::new(1, vec![bucket], processing)?
        .with_chunking(Chunking::ContentDefined(blocks))?
        .with_content_hash(true)
}

/// Off is level 0; zstd takes the Pithos level with the nearest zstd level, the lower on a tie.
fn pithos_level(compression: Compression) -> u8 {
    let Compression::Zstd { level } = compression else {
        return 0;
    };
    let nearest = PITHOS_ZSTD
        .iter()
        .enumerate()
        .min_by_key(|(_, zstd)| zstd.abs_diff(level));
    nearest.map_or(0, |(index, _)| index as u8 + 1)
}

/// Seals the last bytes and composes the archive around the finished piece.
fn finish_piece(
    mut encoder: PieceEncoder,
    plain: &[u8],
) -> Result<(Vec<u8>, Piece, Composition), PithosError> {
    let mut blocks = encoder.write(plain)?;
    blocks.extend(encoder.flush()?);
    let piece = encoder.finish()?;
    let composition = compose_object(std::slice::from_ref(&piece))?;
    Ok((blocks, piece, composition))
}

/// Times and permissions of an object live in Aruna's records, so the entry keeps fixed values.
fn compose_object(pieces: &[Piece]) -> Result<Composition, PithosError> {
    let path = ArchivePath::new(OBJECT_PATH)?;
    compose(path, EntryMetadata::new(0, 0, 0o644), pieces)
}

/// Runs one writer call within `limit`. A call cut off by the limit must not be polled again,
/// so it marks the writer abandoned.
async fn settle<T>(
    abandoned: &mut bool,
    limit: Duration,
    call: impl Future<Output = opendal::Result<T>>,
) -> Result<T, BlobError> {
    match timeout(limit, call).await {
        Ok(result) => result.map_err(|error| BlobError::WriteError(error.to_string())),
        Err(_) => {
            *abandoned = true;
            Err(deadline_expired())
        }
    }
}

fn deadline_expired() -> BlobError {
    BlobError::WriteError("blob write deadline expired".to_string())
}

fn write_error(error: PithosError) -> BlobError {
    BlobError::WriteError(error.to_string())
}

/// A Pithos copy is only read with its bucket key, never as raw bytes.
pub(super) fn needs_bucket_key() -> BlobError {
    BlobError::ReadError("an encrypted copy is read with its bucket key".to_string())
}

/// Backend, key and range problems are read failures; a changed or damaged archive is an
/// integrity failure.
fn blob_error(error: PithosError) -> BlobError {
    match error {
        PithosError::Io(_)
        | PithosError::Source(_)
        | PithosError::ContentUnavailable
        | PithosError::InvalidReadRange { .. }
        | PithosError::FileNotFound(_) => BlobError::ReadError(error.to_string()),
        error => BlobError::IntegrityCheckFailed(error.to_string()),
    }
}
