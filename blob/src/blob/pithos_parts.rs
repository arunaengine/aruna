//! Seals the parts of encrypted multipart uploads as Pithos pieces.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::backend::build_part_path;
use crate::hash::Hasher;
use aruna_core::UserId;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::storage::blob::{BackendLocation, ResolvedBackend};
use aruna_core::structs::storage::encryption::{BlockCipher, BlockKeys};
use aruna_core::structs::storage::format::{Compression, StoredFormat};
use aruna_core::structs::storage::multipart::{MultipartPartKey, PartPiece, UploadEncryption};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use opendal::{Operator, Writer};
use pithos_lib::archive::{
    BlockKeyMode, Chunking, PayloadCipher, Piece, PieceEncoder, ProcessingOptions,
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

fn write_error(error: PithosError) -> BlobError {
    BlobError::WriteError(error.to_string())
}
