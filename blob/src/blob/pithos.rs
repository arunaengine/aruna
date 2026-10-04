//! Writes and reads Pithos copies of encrypted objects. The backend serves byte ranges to the
//! Pithos reader, and sealing, decryption and decoding run on the blocking pool.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::frames::read_range;
use aruna_core::errors::BlobError;
use aruna_core::structs::storage::encryption::{BlockCipher, BlockKeys, SealPlan};
use aruna_core::structs::storage::format::{Compression, PithosLayout};
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use opendal::Operator;
use pithos_lib::archive::{
    AccessKeys, ArchivePath, AsyncArchive, BlockKeyMode, BlockingHook, CdcConfig, Chunking,
    Composition, EntryMetadata, OpenOptions, PayloadCipher, Piece, PieceEncoder, ProcessingOptions,
    compose,
};
use pithos_lib::crypto::PublicKey;
use pithos_lib::error::PithosError;
use pithos_lib::source::{AsyncArchiveSource, SourceError};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

/// Path of the one file in the archive of an object.
pub const OBJECT_PATH: &str = "object";

const MIB: usize = 1 << 20;
/// Plaintext gathered before each call into the encoder, so small chunks share one pool task.
const BATCH: usize = MIB;
/// Zstd levels that the Pithos compression levels 1 to 7 stand for.
const PITHOS_ZSTD: [u8; 7] = [1, 4, 8, 11, 15, 18, 22];

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

/// Seals original bytes into one Pithos archive while a write streams them to the backend.
/// It yields the header, the sealed blocks and the directory in order.
pub(super) struct ArchiveEncoder {
    /// Lent to the blocking pool while a batch is sealed; a failed seal ends the write.
    encoder: Option<PieceEncoder>,
    batch: BytesMut,
    size: u64,
    stored: u64,
    header: [u8; 6],
}

impl ArchiveEncoder {
    /// One piece of FastCDC blocks from 1 to 16 MiB, granted to the key of `plan` with its
    /// cipher and key mode. Content hashing stays on in both key modes.
    pub(super) fn new(plan: &SealPlan, compression: Compression) -> Result<Self, BlobError> {
        let bucket = PublicKey::from_raw(plan.public_key)
            .map_err(|error| BlobError::WriteError(error.to_string()))?;
        let blocks = CdcConfig::new(MIB, 4 * MIB, 16 * MIB).map_err(write_error)?;
        let processing = ProcessingOptions::new(true, pithos_level(compression))
            .and_then(|options| options.with_cipher(cipher(plan.cipher)))
            .and_then(|options| options.with_key_mode(key_mode(plan.block_keys)))
            .map_err(write_error)?;
        let encoder = PieceEncoder::new(1, vec![bucket], processing)
            .and_then(|encoder| encoder.with_chunking(Chunking::ContentDefined(blocks)))
            .and_then(|encoder| encoder.with_content_hash(true))
            .map_err(write_error)?;
        let header = compose_object(&[]).map_err(write_error)?.header();
        Ok(Self {
            encoder: Some(encoder),
            batch: BytesMut::new(),
            size: 0,
            stored: header.len() as u64,
            header,
        })
    }

    /// The archive header, written before any block.
    pub(super) fn header(&self) -> Bytes {
        Bytes::copy_from_slice(&self.header)
    }

    /// Takes `bytes` in slices of at most one batch, sealing each full batch on the blocking pool.
    pub(super) async fn push(&mut self, mut bytes: &[u8]) -> Result<Vec<Bytes>, BlobError> {
        let mut out = Vec::new();
        while !bytes.is_empty() {
            let take = (BATCH - self.batch.len()).min(bytes.len());
            self.batch.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.batch.len() == BATCH {
                let plain = self.batch.split().freeze();
                out.extend(self.seal(plain).await?);
            }
        }
        Ok(out)
    }

    async fn seal(&mut self, plain: Bytes) -> Result<Option<Bytes>, BlobError> {
        self.count(plain.len())?;
        let mut encoder = self.encoder.take().ok_or_else(sealing_failed)?;
        let sealed = TokioBlocking.spawn_blocking(move || {
            let blocks = encoder.write(&plain);
            (encoder, blocks)
        });
        let (encoder, blocks) = sealed.await;
        let blocks = blocks.map_err(write_error)?;
        self.encoder = Some(encoder);
        self.stored += blocks.len() as u64;
        Ok((!blocks.is_empty()).then(|| Bytes::from(blocks)))
    }

    /// Seals the last bytes and returns the closing bytes, the layout and the content hash.
    pub(super) async fn finish(
        mut self,
    ) -> Result<(Vec<Bytes>, PithosLayout, [u8; 32]), BlobError> {
        let plain = std::mem::take(&mut self.batch).freeze();
        self.count(plain.len())?;
        let encoder = self.encoder.take().ok_or_else(sealing_failed)?;
        let sealed = TokioBlocking.spawn_blocking(move || finish_piece(encoder, &plain));
        let (blocks, piece, composition) = sealed.await.map_err(write_error)?;
        let start = self.header.len() as u64;
        let stored = self.stored + blocks.len() as u64;
        let directory = composition.directory().to_vec();
        let consistent = composition.header() == self.header
            && composition.piece_offsets() == [start]
            && stored == start + piece.stored_len()
            && piece.original_size() == self.size
            && composition.archive_len() == stored + directory.len() as u64;
        let (true, Some(content_hash)) = (consistent, composition.content_hash()) else {
            let message = "the Pithos archive does not match the written bytes";
            return Err(BlobError::WriteError(message.to_string()));
        };
        let layout = PithosLayout {
            stored_size: composition.archive_len(),
            metadata_digest: composition.metadata_digest(),
        };
        let mut out = Vec::new();
        if !blocks.is_empty() {
            out.push(Bytes::from(blocks));
        }
        out.push(Bytes::from(directory));
        Ok((out, layout, content_hash))
    }

    fn count(&mut self, len: usize) -> Result<(), BlobError> {
        self.size = self
            .size
            .checked_add(len as u64)
            .ok_or(BlobError::SizeLimitExceeded { limit: u64::MAX })?;
        Ok(())
    }
}

fn sealing_failed() -> BlobError {
    BlobError::WriteError("an earlier Pithos batch failed".to_string())
}

fn cipher(cipher: BlockCipher) -> PayloadCipher {
    match cipher {
        BlockCipher::ChaCha20Poly1305 => PayloadCipher::ChaCha20Poly1305,
        BlockCipher::Aes256Gcm => PayloadCipher::Aes256Gcm,
    }
}

fn key_mode(keys: BlockKeys) -> BlockKeyMode {
    match keys {
        BlockKeys::ContentDerived => BlockKeyMode::ContentDerived,
        BlockKeys::Unique => BlockKeyMode::Unique,
    }
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
