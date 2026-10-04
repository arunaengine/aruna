//! Writes and reads Pithos copies of encrypted objects. The backend serves byte ranges to the
//! Pithos reader, and sealing, decryption and decoding run on the blocking pool.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::frames::read_range;
use super::unlock::LeaseGuard;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::storage::blob::{ArchiveKey, BackendLocation};
use aruna_core::structs::storage::encryption::{
    BlockCipher, BlockKeys, BucketKeyError, ReadLease, SealPlan,
};
use aruna_core::structs::storage::format::{Compression, PithosLayout, StoredLayout};
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};
use opendal::Operator;
use pithos_lib::archive::{
    AccessKeys, ArchivePath, AsyncArchive, BlockKeyMode, BlockingHook, CdcConfig, Chunking,
    Composition, EntryKind, EntryMetadata, NoExternalBlocks, OpenLimits, OpenOptions,
    PayloadCipher, Piece, PieceEncoder, ProcessingOptions, compose,
};
use pithos_lib::crypto::{PrivateKey, PublicKey};
use pithos_lib::error::PithosError;
use pithos_lib::source::{AsyncArchiveSource, SourceError};
use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use zeroize::Zeroizing;

/// Path of the one file in the archive of an object.
pub const OBJECT_PATH: &str = "object";

const MIB: usize = 1 << 20;
/// Plaintext gathered before each call into the encoder, so small chunks share one pool task.
const BATCH: usize = MIB;
/// Zstd levels that the Pithos compression levels 1 to 7 stand for.
const PITHOS_ZSTD: [u8; 7] = [1, 4, 8, 11, 15, 18, 22];
/// Largest original object a Pithos write accepts: 5 TiB.
pub const MAX_SIZE: u64 = 5 << 40;
/// Largest decoded block: the FastCDC maximum of single uploads.
const MAX_BLOCK: u64 = 16 << 20;

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

/// Limits of Aruna archives: one 5 TiB file in blocks of at least 1 MiB, up to 10,000 pieces,
/// and no parent directories or references.
pub(super) fn limits() -> OpenLimits {
    let descriptors = 5_252_881;
    OpenLimits {
        max_directory_bytes: 1 << 30,
        max_total_directory_bytes: 1 << 30,
        max_parent_directories: 0,
        max_references: 0,
        max_descriptors: descriptors,
        max_accessible_block_references: descriptors,
        max_opaque_metadata_bytes: 512 << 20,
        // The zstd bound of the largest block, plus nonce and tag.
        max_stored_block_bytes: zstd::zstd_safe::compress_bound(MAX_BLOCK as usize) as u64 + 28,
        max_decoded_block_bytes: MAX_BLOCK,
        ..OpenLimits::default()
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
    let archive = open(operator, path, layout, keys, idle).await?;
    let stream = archive
        .read_range_owned(OBJECT_PATH, range)
        .map_err(blob_error)?;
    Ok(stream.map(|chunk| chunk.map(Bytes::from).map_err(blob_error)))
}

/// An opened Aruna archive whose single file reads with the granted keys.
type OpenArchive = Arc<AsyncArchive<StoredArchive, NoExternalBlocks, TokioBlocking>>;

/// Opens the copy at `path` within the Aruna limits and checks its single-file shape.
async fn open(
    operator: Operator,
    path: String,
    layout: &PithosLayout,
    keys: AccessKeys,
    idle: Duration,
) -> Result<OpenArchive, BlobError> {
    let source = StoredArchive {
        operator,
        path,
        idle,
    };
    let options = OpenOptions::default()
        .with_limits(limits())
        .with_access_keys(keys)
        .with_expected_metadata_digest(layout.metadata_digest);
    let archive =
        AsyncArchive::open_with_hook(source, options, Some(layout.stored_size), TokioBlocking)
            .await
            .map_err(blob_error)?;
    let single = {
        let mut entries = archive.entries();
        match (entries.next(), entries.next()) {
            (Some(entry), None) => {
                entry.path == OBJECT_PATH
                    && matches!(entry.kind, EntryKind::File { .. })
                    && entry.references.is_empty()
            }
            _ => false,
        }
    };
    if !single {
        let message = "a Pithos copy must hold exactly one file named object";
        return Err(BlobError::IntegrityCheckFailed(message.to_string()));
    }
    Ok(Arc::new(archive))
}

/// Plaintext of a sealed copy by offset, for an authorized remote read. It keeps the lease,
/// so the key and the archive stay in use until the transfer ends.
pub(super) struct SealedReader {
    archive: OpenArchive,
    size: u64,
    _lease: ReadLease,
}

impl iroh_io::AsyncSliceReader for SealedReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        let len = len.min(self.size.saturating_sub(offset) as usize);
        self.read_exact_at(offset, len).await
    }

    async fn read_exact_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        let end = offset
            .checked_add(len as u64)
            .filter(|end| *end <= self.size)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?;
        let mut stream = Arc::clone(&self.archive)
            .read_range_owned(OBJECT_PATH, offset..end)
            .map_err(std::io::Error::other)?;
        let mut out = BytesMut::with_capacity(len);
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk.map_err(std::io::Error::other)?);
        }
        Ok(out.freeze())
    }

    async fn size(&mut self) -> std::io::Result<u64> {
        Ok(self.size)
    }
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

    /// Takes `bytes` in slices of at most one batch, sealing each full batch on the blocking
    /// pool. The size cap is checked before each batch is sealed.
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
            .filter(|size| *size <= MAX_SIZE)
            .ok_or(BlobError::SizeLimitExceeded { limit: MAX_SIZE })?;
        Ok(())
    }
}

fn sealing_failed() -> BlobError {
    BlobError::WriteError("an earlier Pithos batch failed".to_string())
}

/// The plaintext of one leased read. It keeps the lease until it ends and is polled only through
/// `&mut self`, so it may be shared. A whole read checks the recorded BLAKE3 at its end.
struct LeasedRead<S> {
    stream: Mutex<Pin<Box<S>>>,
    hasher: blake3::Hasher,
    expected: Option<Vec<u8>>,
    _lease: ReadLease,
}

impl<S: Stream<Item = Result<Bytes, BlobError>>> Stream for LeasedRead<S> {
    type Item = Result<Bytes, BlobError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Ok(stream) = this.stream.get_mut() else {
            return Poll::Ready(None);
        };
        match stream.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.hasher.update(&chunk);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(None) => match this.expected.take() {
                Some(hash) if this.hasher.finalize().as_bytes()[..] != hash[..] => {
                    let message = "blake3 hash mismatch".to_string();
                    Poll::Ready(Some(Err(BlobError::IntegrityCheckFailed(message))))
                }
                _ => Poll::Ready(None),
            },
            other => other,
        }
    }
}

impl BlobHandler {
    /// Streams `range`, or the whole object, of a Pithos copy under `lease`, which must be
    /// admitted for this copy's key and archive. The stream keeps the lease until it ends; a
    /// whole read is also checked against the recorded BLAKE3.
    pub async fn read_sealed(
        &self,
        location: BackendLocation,
        range: Option<Range<u64>>,
        lease: ReadLease,
    ) -> BlobEvent {
        match self.sealed_stream(&location, range, lease).await {
            Ok((blob, stream_size)) => BlobEvent::ReadFinished { blob, stream_size },
            Err(error) => BlobEvent::Error(error),
        }
    }

    async fn sealed_stream(
        &self,
        location: &BackendLocation,
        range: Option<Range<u64>>,
        lease: ReadLease,
    ) -> Result<(BackendStream<Result<Bytes, StreamError>>, u64), BlobError> {
        let StoredLayout::Pithos(layout) = &location.format.layout else {
            return Err(BlobError::ReadError("not a Pithos copy".to_string()));
        };
        let keys = self.sealed_keys(location, &lease)?;
        let expected = match range {
            Some(_) => None,
            None => Some(location.get_blake3().map(<[u8]>::to_vec).ok_or_else(|| {
                BlobError::IntegrityCheckFailed("missing stored blake3 hash".to_string())
            })?),
        };
        let range = range.unwrap_or(0..location.blob_size);
        let operator = self.operator_from_location(location)?;
        let path = location.get_storage_path()?;
        let idle = self.transfer_idle_timeout();
        let size = range.end.saturating_sub(range.start);
        let stream = Box::pin(read(operator, path, layout, keys, range, idle).await?);
        let blob = LeasedRead {
            stream: Mutex::new(stream),
            hasher: blake3::Hasher::new(),
            expected,
            _lease: lease,
        };
        Ok((BackendStream::new(blob), size))
    }

    /// A slice reader over the plaintext of the sealed copy at `location`, under `lease`.
    pub(super) async fn sealed_reader(
        &self,
        location: &BackendLocation,
        lease: ReadLease,
    ) -> Result<SealedReader, BlobError> {
        let StoredLayout::Pithos(layout) = &location.format.layout else {
            return Err(BlobError::ReadError("not a Pithos copy".to_string()));
        };
        let keys = self.sealed_keys(location, &lease)?;
        let operator = self.operator_from_location(location)?;
        let path = location.get_storage_path()?;
        let archive = open(operator, path, layout, keys, self.transfer_idle_timeout()).await?;
        Ok(SealedReader {
            archive,
            size: location.blob_size,
            _lease: lease,
        })
    }

    /// The key of a sealed copy, only through a lease admitted for exactly this archive. A lock
    /// after admission does not stop it: the lease keeps its own key.
    pub(super) fn sealed_keys(
        &self,
        location: &BackendLocation,
        lease: &ReadLease,
    ) -> Result<AccessKeys, BlobError> {
        let key = location.format.bucket_key().ok_or_else(needs_bucket_key)?;
        if lease.key != key || lease.archive != ArchiveKey::of(location) {
            return Err(BucketKeyError::Locked(key.bucket_id).into());
        }
        let secret = LeaseGuard::secret(lease).ok_or(BucketKeyError::Locked(key.bucket_id))?;
        let bytes: &[u8; 32] = secret
            .bytes()
            .expose()
            .try_into()
            .map_err(|_| BlobError::from(BucketKeyError::WrongKey))?;
        let raw = Zeroizing::new(*bytes);
        Ok(AccessKeys::new().with_key(PrivateKey::from_raw(raw)))
    }
}

pub(super) fn cipher(cipher: BlockCipher) -> PayloadCipher {
    match cipher {
        BlockCipher::ChaCha20Poly1305 => PayloadCipher::ChaCha20Poly1305,
        BlockCipher::Aes256Gcm => PayloadCipher::Aes256Gcm,
    }
}

pub(super) fn key_mode(keys: BlockKeys) -> BlockKeyMode {
    match keys {
        BlockKeys::ContentDerived => BlockKeyMode::ContentDerived,
        BlockKeys::Unique => BlockKeyMode::Unique,
    }
}

/// Off is level 0; zstd takes the Pithos level with the nearest zstd level, the lower on a tie.
pub(super) fn pithos_level(compression: Compression) -> u8 {
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

pub(super) fn write_error(error: PithosError) -> BlobError {
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

#[cfg(test)]
mod tests {
    use super::{ArchiveEncoder, BATCH, MAX_SIZE};
    use aruna_core::errors::BlobError;
    use aruna_core::structs::storage::encryption::{BucketKeyRef, SealPlan};
    use aruna_core::structs::storage::format::Compression;
    use pithos_lib::crypto::PrivateKey;

    fn encoder() -> ArchiveEncoder {
        let plan = SealPlan {
            key: BucketKeyRef::new(ulid::Ulid::from_bytes([1; 16]), 1),
            public_key: *PrivateKey::generate().public_key().as_bytes(),
            cipher: Default::default(),
            block_keys: Default::default(),
            storage_generation: 1,
        };
        ArchiveEncoder::new(&plan, Compression::Off).unwrap()
    }

    #[tokio::test]
    async fn batches_stay_bounded() {
        let mut encoder = encoder();
        encoder.push(&vec![7; 3 * BATCH + 10]).await.unwrap();
        assert_eq!(encoder.batch.len(), 10);
        assert_eq!(encoder.size, 3 * BATCH as u64);
    }

    #[tokio::test]
    async fn caps_original_size() {
        let mut encoder = encoder();
        encoder.size = MAX_SIZE - 5;
        let refused = encoder.push(&vec![7; BATCH]).await;
        assert_eq!(
            refused.unwrap_err(),
            BlobError::SizeLimitExceeded { limit: MAX_SIZE }
        );
    }
}
