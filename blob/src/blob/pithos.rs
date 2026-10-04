//! Reads Pithos copies of encrypted objects. The backend serves byte ranges to the Pithos
//! reader, and decryption and decoding run on the blocking pool.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::frames::read_range;
use aruna_core::errors::BlobError;
use aruna_core::structs::storage::format::PithosLayout;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use opendal::Operator;
use pithos_lib::archive::{AccessKeys, AsyncArchive, BlockingHook, OpenOptions};
use pithos_lib::error::PithosError;
use pithos_lib::source::{AsyncArchiveSource, SourceError};
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

/// Path of the one file in the archive of an object.
pub const OBJECT_PATH: &str = "object";

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
