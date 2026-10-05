//! Hashes the verified plaintext of a sealed copy inside the adapter, for pending content.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use crate::hash::Hasher;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::blob::BackendLocation;
use aruna_core::structs::storage::encryption::ReadLease;
use aruna_core::structs::storage::format::StoredLayout;
use futures::StreamExt;

impl BlobHandler {
    /// Reads every block of `location` through `lease`; each block is checked on read, so the
    /// hashes describe verified plaintext. A size other than the recorded one is an error.
    pub(super) async fn hash_archive(
        &self,
        location: BackendLocation,
        lease: ReadLease,
    ) -> BlobEvent {
        let result = Box::pin(self.hash_sealed(&location, &lease)).await;
        // The lease keeps the key and the archive until the last block is read.
        drop(lease);
        result.unwrap_or_else(BlobEvent::Error)
    }

    async fn hash_sealed(
        &self,
        location: &BackendLocation,
        lease: &ReadLease,
    ) -> Result<BlobEvent, BlobError> {
        let StoredLayout::Pithos(_) = &location.format.layout else {
            return Err(BlobError::ReadError("only sealed copies hash here".into()));
        };
        let keys = self.lease_keys(location, Some(lease))?;
        let range = 0..location.blob_size;
        let stream = self.read_archive(location, keys, range).await?;
        let mut stream = Box::pin(stream);
        let mut hasher = Hasher::new();
        let mut size = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            size = size.saturating_add(chunk.len() as u64);
            hasher.update(&chunk);
        }
        if size != location.blob_size {
            return Err(BlobError::ReadError(format!(
                "sealed copy holds {size} bytes, recorded {}",
                location.blob_size
            )));
        }
        Ok(BlobEvent::ArchiveHashed {
            hashes: hasher.to_map(),
            size,
        })
    }
}
