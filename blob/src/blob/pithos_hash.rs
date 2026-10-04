//! Hashes the verified plaintext of a sealed copy inside the adapter, for pending content.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use crate::hash::Hasher;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::blob::{ArchiveKey, BackendLocation};
use aruna_core::structs::storage::encryption::{BucketKeyError, ReadLease};
use aruna_core::structs::storage::format::StoredLayout;
use futures::StreamExt;
use pithos_lib::archive::AccessKeys;
use pithos_lib::crypto::PrivateKey;
use std::time::Instant;
use zeroize::Zeroizing;

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
        let StoredLayout::Pithos(layout) = &location.format.layout else {
            return Err(BlobError::ReadError("only sealed copies hash here".into()));
        };
        let key = location
            .format
            .bucket_key()
            .ok_or_else(super::pithos::needs_bucket_key)?;
        if lease.key != key || lease.archive != ArchiveKey::of(location) {
            return Err(BucketKeyError::Locked(key.bucket_id).into());
        }
        let (secret, _) = match self.unlocks.lock() {
            Ok(mut registry) => registry.unlocked_key(key, Instant::now())?,
            Err(_) => return Err(BucketKeyError::Locked(key.bucket_id).into()),
        };
        let bytes = secret.bytes().expose();
        if bytes.len() != 32 {
            return Err(BucketKeyError::WrongKey.into());
        }
        let mut raw = Zeroizing::new([0u8; 32]);
        raw.copy_from_slice(bytes);
        let keys = AccessKeys::new().with_key(PrivateKey::from_raw(raw));
        let operator = self.operator_from_location(location)?;
        let path = location.get_storage_path()?;
        let idle = self.transfer_idle_timeout();
        let range = 0..location.blob_size;
        let stream = super::pithos::read(operator, path, layout, keys, range, idle).await?;
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
