//! Seals the parts of encrypted multipart uploads as Pithos pieces and composes the stored pieces
//! into one archive. Composition opens no key, so it also works while the bucket is locked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::backend::build_part_path;
use super::io::compose_chunk;
use super::pithos::OBJECT_PATH;
use crate::hash::Hasher;
use aruna_core::UserId;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::storage::blob::{BackendLocation, ResolvedBackend};
use aruna_core::structs::storage::encryption::{BlockCipher, BlockKeys};
use aruna_core::structs::storage::format::{Compression, PithosLayout, StoredFormat};
use aruna_core::structs::storage::multipart::{
    MultipartPart, MultipartPartKey, PartPiece, UploadEncryption,
};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use opendal::{Operator, Writer};
use pithos_lib::archive::{
    ArchivePath, BlockKeyMode, Chunking, Composition, EntryMetadata, PayloadCipher, Piece,
    PieceEncoder, ProcessingOptions, compose,
};
use pithos_lib::crypto::PublicKey;
use pithos_lib::error::PithosError;
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::time::timeout;
use ulid::Ulid;

const MIB: usize = 1 << 20;
/// Multipart parts use fixed blocks, so equal parts line up with the whole file.
const PART_BLOCK: usize = 4 * MIB;
/// Zstd levels that the Pithos compression levels 1 to 7 stand for.
const PITHOS_ZSTD: [u8; 7] = [1, 4, 8, 11, 15, 18, 22];

impl BlobHandler {
    /// Writes one part of an encrypted upload as a Pithos piece keyed by its part number.
    ///
    /// The piece records a content tree at `content_offset` when one is given. The location
    /// carries the size and checksums of the original bytes; the piece record holds no key.
    pub async fn seal_piece(
        &self,
        part: MultipartPartKey,
        resolved: ResolvedBackend,
        created_by: UserId,
        content_offset: Option<u64>,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let Some(plan) = resolved.encryption else {
            return BlobEvent::Error(BlobError::WriteError("a piece needs a seal plan".into()));
        };
        let upload = UploadEncryption {
            plan,
            compression: resolved.compression,
        };
        let encoder = match piece_encoder(&upload, part.part_number, content_offset) {
            Ok(encoder) => encoder,
            Err(error) => return BlobEvent::Error(error),
        };
        let root = match self.registry.config_for(&resolved.backend) {
            Ok(config) => config.root.clone(),
            Err(error) => return BlobEvent::Error(error),
        };
        let multipart_bucket = match self.multipart_bucket(&resolved.backend) {
            Ok(bucket) => bucket,
            Err(error) => return BlobEvent::Error(error),
        };
        let ulid = Ulid::generate();
        let location = BackendLocation {
            backend: resolved.backend.clone(),
            storage_class: resolved.storage_class.clone(),
            root,
            storage_bucket: multipart_bucket.clone(),
            backend_path: build_part_path(part.upload_id, part.part_number, ulid),
            ulid,
            format: StoredFormat::default(),
            created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        };
        let Some(mut reservation) = self.hold_reservation(location.ulid) else {
            let message = "too many active blob reservations".to_string();
            return BlobEvent::Error(BlobError::WriteError(message));
        };
        if let Err(error) = self.finalize_reservation(&location).await {
            return BlobEvent::Error(error);
        }
        let operator =
            self.registry
                .bucket_operator(&resolved.backend, &multipart_bucket, &self.egress);
        let written = match operator {
            Ok(operator) => {
                self.seal_part(location.clone(), operator, encoder, blob)
                    .await
            }
            Err(error) => Err(error),
        };
        match written {
            Ok((location, piece)) => {
                reservation.retain();
                let piece = PartPiece {
                    record: piece.to_bytes(),
                    stored_len: piece.stored_len(),
                    content_offset,
                };
                BlobEvent::PieceWritten { location, piece }
            }
            Err(error @ BlobError::WriteCleanup { .. }) => {
                reservation.retain();
                BlobEvent::Error(error)
            }
            Err(error) => match self.release_reservation(&location).await {
                Ok(()) => BlobEvent::Error(error),
                Err(cleanup) => {
                    reservation.retain();
                    BlobEvent::Error(BlobError::WriteCleanup {
                        location,
                        message: format!("{error}; {cleanup}"),
                    })
                }
            },
        }
    }

    /// Streams sealed blocks to the part path; a failed write removes the partial object.
    async fn seal_part(
        &self,
        mut location: BackendLocation,
        operator: Operator,
        encoder: PieceEncoder,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> Result<(BackendLocation, Piece), BlobError> {
        let path = location.get_storage_path()?;
        let (mut writer, mut abandoned) = (None, false);
        let mut hasher = Hasher::new();
        let sealed = self
            .seal_blocks(
                &operator,
                &path,
                &mut writer,
                &mut abandoned,
                encoder,
                blob,
                &mut hasher,
            )
            .await;
        match sealed {
            Ok((size, piece)) => {
                location.blob_size = size;
                location.hashes = hasher.to_map();
                Ok((location, piece))
            }
            Err(error) => {
                let cleaned = self
                    .clean_partial(
                        writer.as_mut(),
                        abandoned,
                        Some(&operator),
                        Some(&path),
                        None,
                    )
                    .await;
                match cleaned {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(BlobError::WriteCleanup {
                        location,
                        message: format!("{error}; {cleanup}"),
                    }),
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn seal_blocks(
        &self,
        operator: &Operator,
        path: &str,
        writer: &mut Option<Writer>,
        abandoned: &mut bool,
        mut encoder: PieceEncoder,
        mut blob: BackendStream<Result<Bytes, StreamError>>,
        hasher: &mut Hasher,
    ) -> Result<(u64, Piece), BlobError> {
        let writer = match timeout(self.io_timeout(), operator.writer(path)).await {
            Ok(Ok(opened)) => writer.insert(opened),
            Ok(Err(error)) => return Err(BlobError::OperatorCreationFailed(error.to_string())),
            Err(_) => return Err(deadline_expired()),
        };
        let idle = self.transfer_idle_timeout();
        let (mut size, mut stored) = (0u64, 0u64);
        let mut batch = BytesMut::new();
        loop {
            let chunk = match timeout(idle, blob.next()).await {
                Ok(Some(chunk)) => {
                    chunk.map_err(|error| BlobError::StreamFailed(error.to_string()))?
                }
                Ok(None) => break,
                Err(_) => return Err(deadline_expired()),
            };
            hasher.update(&chunk);
            size = size
                .checked_add(chunk.len() as u64)
                .ok_or(BlobError::SizeLimitExceeded { limit: u64::MAX })?;
            batch.extend_from_slice(&chunk);
            if batch.len() < PART_BLOCK {
                continue;
            }
            let plain = batch.split().freeze();
            let blocks;
            (encoder, blocks) = blocking(move || {
                let mut encoder = encoder;
                encoder.write(&plain).map(|blocks| (encoder, blocks))
            })
            .await?;
            stored += blocks.len() as u64;
            if !blocks.is_empty() {
                settle(abandoned, idle, writer.write(blocks)).await?;
            }
        }
        let plain = batch.freeze();
        let (blocks, piece) = blocking(move || {
            let mut blocks = encoder.write(&plain)?;
            blocks.extend(encoder.flush()?);
            Ok((blocks, encoder.finish()?))
        })
        .await?;
        stored += blocks.len() as u64;
        if !blocks.is_empty() {
            settle(abandoned, idle, writer.write(blocks)).await?;
        }
        if stored != piece.stored_len() || size != piece.original_size() {
            let message = "the Pithos piece does not match the written bytes";
            return Err(BlobError::WriteError(message.to_string()));
        }
        settle(abandoned, self.io_timeout(), writer.close()).await?;
        Ok((size, piece))
    }

    /// Writes the header, the stored bytes of each part in order and the directory as one
    /// archive at a new object path. Every stored length and offset is checked on the way.
    ///
    /// The location names the key of `upload`; its BLAKE3 is set only when the recorded content
    /// trees line up with the final offsets.
    pub async fn compose_pieces(
        &self,
        request_bucket: &str,
        request_key: &str,
        resolved: ResolvedBackend,
        created_by: UserId,
        parts: Vec<MultipartPart>,
    ) -> BlobEvent {
        let Some(plan) = resolved.encryption else {
            return BlobEvent::Error(BlobError::WriteError("pieces need a seal plan".into()));
        };
        let composition = match compose_parts(&parts) {
            Ok(composition) => composition,
            Err(error) => return BlobEvent::Error(error),
        };
        let root = match self.registry.config_for(&resolved.backend) {
            Ok(config) => config.root.clone(),
            Err(error) => return BlobEvent::Error(error),
        };
        let ulid = Ulid::generate();
        let backend_path =
            match super::backend::build_backend_path(request_bucket, request_key, ulid) {
                Ok(path) => path,
                Err(error) => return BlobEvent::Error(BlobError::ConversionError(error)),
            };
        let layout = PithosLayout {
            stored_size: composition.archive_len(),
            metadata_digest: composition.metadata_digest(),
        };
        let mut hashes = HashMap::new();
        if let Some(hash) = composition.content_hash() {
            hashes.insert(HASH_BLAKE3.to_string(), hash.to_vec());
        }
        let template = BackendLocation {
            backend: resolved.backend.clone(),
            storage_class: resolved.storage_class.clone(),
            root,
            storage_bucket: String::new(),
            backend_path,
            ulid,
            format: StoredFormat::pithos(layout, plan.key),
            created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: parts.iter().map(|part| part.location.blob_size).sum(),
            hashes,
        };
        let Some(mut reservation) = self.hold_reservation(template.ulid) else {
            let message = "too many active blob reservations".to_string();
            return BlobEvent::Error(BlobError::WriteError(message));
        };
        let location = match self.reserve_bucket(&resolved.backend, &template).await {
            Ok(location) => location,
            Err(error) => return BlobEvent::Error(error),
        };
        let written = self.write_composed(&location, &composition, &parts).await;
        match written {
            Ok(()) => {
                reservation.retain();
                match self.finalize_reservation(&location).await {
                    Ok(()) => BlobEvent::WriteFinished { location },
                    Err(error) => BlobEvent::Error(BlobError::WriteCleanup {
                        location,
                        message: error.to_string(),
                    }),
                }
            }
            Err(error @ BlobError::WriteCleanup { .. }) => {
                reservation.retain();
                BlobEvent::Error(error)
            }
            Err(error) => {
                _ = self.release_reservation(&location).await;
                BlobEvent::Error(error)
            }
        }
    }

    async fn write_composed(
        &self,
        location: &BackendLocation,
        composition: &Composition,
        parts: &[MultipartPart],
    ) -> Result<(), BlobError> {
        let operator = self.operator_from_location(location)?;
        let path = location.get_storage_path()?;
        let backend = self.registry.config_for(&location.backend)?.backend_type;
        let chunk = compose_chunk(&backend, composition.archive_len(), true);
        let opened = match chunk {
            Some(chunk) => {
                let writer = operator.writer_with(&path).chunk(chunk).into_future();
                timeout(self.io_timeout(), writer).await
            }
            None => timeout(self.io_timeout(), operator.writer(&path)).await,
        };
        let mut writer = match opened {
            Ok(Ok(writer)) => writer,
            Ok(Err(error)) => return Err(BlobError::OperatorCreationFailed(error.to_string())),
            Err(_) => return Err(deadline_expired()),
        };
        let mut abandoned = false;
        let written = self
            .copy_pieces(&mut writer, &mut abandoned, composition, parts)
            .await;
        let Err(error) = written else {
            return Ok(());
        };
        let cleaned = self
            .clean_partial(
                Some(&mut writer),
                abandoned,
                Some(&operator),
                Some(&path),
                Some(location),
            )
            .await;
        match cleaned {
            Ok(()) => Err(error),
            Err(cleanup) => Err(BlobError::WriteCleanup {
                location: location.clone(),
                message: format!("{error}; {cleanup}"),
            }),
        }
    }

    async fn copy_pieces(
        &self,
        writer: &mut Writer,
        abandoned: &mut bool,
        composition: &Composition,
        parts: &[MultipartPart],
    ) -> Result<(), BlobError> {
        let idle = self.transfer_idle_timeout();
        let header = composition.header();
        settle(abandoned, idle, writer.write(header.to_vec())).await?;
        let mut offset = header.len() as u64;
        for (part, start) in parts.iter().zip(composition.piece_offsets()) {
            let expected = part.piece.as_ref().map(|piece| piece.stored_len);
            if offset != *start || expected.is_none() {
                return Err(mismatch());
            }
            let copied = self.copy_part(writer, abandoned, &part.location).await?;
            if Some(copied) != expected {
                return Err(mismatch());
            }
            offset = offset.checked_add(copied).ok_or_else(mismatch)?;
        }
        let directory = composition.directory();
        let end = offset.checked_add(directory.len() as u64);
        if end != Some(composition.archive_len()) {
            return Err(mismatch());
        }
        settle(abandoned, idle, writer.write(directory.to_vec())).await?;
        settle(abandoned, idle, writer.close()).await?;
        Ok(())
    }

    /// Copies the stored bytes of one part unchanged and returns their length.
    async fn copy_part(
        &self,
        writer: &mut Writer,
        abandoned: &mut bool,
        part: &BackendLocation,
    ) -> Result<u64, BlobError> {
        let operator = self.operator_from_location(part)?;
        let path = part.get_storage_path()?;
        let read_error = |error: opendal::Error| BlobError::ReadError(error.to_string());
        let opening = async {
            let reader = operator.reader(&path).await.map_err(read_error)?;
            reader.into_bytes_stream(..).await.map_err(read_error)
        };
        let reader = timeout(self.io_timeout(), opening)
            .await
            .map_err(|_| BlobError::ReadError("timed out opening a part".to_string()))??;
        let mut reader = BackendStream::new(reader);
        let idle = self.transfer_idle_timeout();
        let mut copied = 0u64;
        loop {
            let next = timeout(idle, reader.next())
                .await
                .map_err(|_| BlobError::ReadError("part reader idle timeout".to_string()))?;
            let Some(bytes) = next else {
                return Ok(copied);
            };
            let bytes = bytes.map_err(|error| BlobError::ReadError(error.to_string()))?;
            copied += bytes.len() as u64;
            settle(abandoned, idle, writer.write(bytes)).await?;
        }
    }
}

/// Fixed 4 MiB blocks with the cipher, key mode and compression of `upload`, keyed by the part
/// number. Content hashing stays on in both key modes when an offset is known.
fn piece_encoder(
    upload: &UploadEncryption,
    part_number: u16,
    content_offset: Option<u64>,
) -> Result<PieceEncoder, BlobError> {
    let plan = &upload.plan;
    let bucket = PublicKey::from_raw(plan.public_key)
        .map_err(|error| BlobError::WriteError(error.to_string()))?;
    let processing = ProcessingOptions::new(true, pithos_level(upload.compression))
        .and_then(|options| options.with_cipher(cipher(plan.cipher)))
        .and_then(|options| options.with_key_mode(key_mode(plan.block_keys)))
        .map_err(write_error)?;
    let encoder = PieceEncoder::new(u64::from(part_number), vec![bucket], processing)
        .and_then(|encoder| encoder.with_chunking(Chunking::Fixed(PART_BLOCK)))
        .and_then(|encoder| encoder.with_content_hash(content_offset.is_some()));
    match content_offset {
        Some(offset) => encoder.and_then(|encoder| encoder.with_content_offset(offset)),
        None => encoder,
    }
    .map_err(write_error)
}

/// The composition of the saved pieces in part order. Each record must name its part number
/// and agree with the stored part's sizes.
fn compose_parts(parts: &[MultipartPart]) -> Result<Composition, BlobError> {
    let mut pieces = Vec::with_capacity(parts.len());
    for part in parts {
        let record = part.piece.as_ref().ok_or_else(mismatch)?;
        let piece = Piece::from_bytes(&record.record).map_err(write_error)?;
        let consistent = piece.key_id() == u64::from(part.part_number)
            && piece.stored_len() == record.stored_len
            && piece.original_size() == part.location.blob_size;
        if !consistent {
            return Err(mismatch());
        }
        pieces.push(piece);
    }
    let path = ArchivePath::new(OBJECT_PATH).map_err(write_error)?;
    compose(path, EntryMetadata::new(0, 0, 0o644), &pieces).map_err(write_error)
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

/// Runs sealing on the blocking pool.
async fn blocking<T: Send + 'static>(
    task: impl FnOnce() -> Result<T, PithosError> + Send + 'static,
) -> Result<T, BlobError> {
    match tokio::task::spawn_blocking(task).await {
        Ok(result) => result.map_err(write_error),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(BlobError::WriteError(error.to_string())),
    }
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

fn mismatch() -> BlobError {
    BlobError::IntegrityCheckFailed("a stored part does not match its piece record".to_string())
}

fn write_error(error: PithosError) -> BlobError {
    BlobError::WriteError(error.to_string())
}
