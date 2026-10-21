//! Lists and hashes objects on a backend that Aruna did not write, for backend imports.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::io::backend_version;
use crate::hash::Hasher;
use aruna_core::UserId;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::structs::storage::backend_import::ExternalObject;
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, ResolvedBackend};
use aruna_core::structs::storage::format::StoredFormat;
use futures::StreamExt;
use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use ulid::Ulid;

fn list_error(error: impl ToString) -> BlobError {
    BlobError::ListError(error.to_string())
}

impl BlobHandler {
    pub(super) async fn list_external(
        &self,
        backend: &BackendRef,
        prefix: &str,
        start_after: Option<String>,
        limit: usize,
    ) -> BlobEvent {
        self.external_page(backend, prefix, start_after.as_deref(), limit.max(1))
            .await
            .unwrap_or_else(BlobEvent::Error)
    }

    /// A backend that skips to `start_after` itself lists in key order. Any other is
    /// read in full and only the smallest keys after `start_after` are kept.
    async fn external_page(
        &self,
        backend: &BackendRef,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<BlobEvent, BlobError> {
        // A storage without a bucket lists one bucket; its keys keep the bucket in front.
        let (bucket, prefix) = match self.registry.splits(backend) {
            true => match prefix.split_once('/') {
                Some((bucket, rest)) if !bucket.is_empty() => (bucket, rest),
                _ => return Err(list_error("the folder names no bucket")),
            },
            false => ("", prefix),
        };
        let scope = match bucket {
            "" => String::new(),
            bucket => format!("{bucket}/"),
        };
        let start_after = start_after.map(|after| after.strip_prefix(&scope).unwrap_or(after));
        let operator = self
            .registry
            .bucket_operator(backend, bucket, &self.egress)?;
        let ordered = operator.info().capability().list_with_start_after;
        let mut request = operator.lister_with(prefix).recursive(true);
        if ordered && let Some(start_after) = start_after {
            request = request.start_after(start_after);
        }
        let mut lister = timeout(self.io_timeout(), request)
            .await
            .map_err(|_| list_error("timed out opening the listing"))?
            .map_err(list_error)?;
        let mut page = BTreeMap::new();
        while let Some(entry) = timeout(self.io_timeout(), lister.next())
            .await
            .map_err(|_| list_error("timed out reading the listing"))?
        {
            let entry = entry.map_err(list_error)?;
            let metadata = entry.metadata();
            if !metadata.is_file() || start_after.is_some_and(|after| entry.path() <= after) {
                continue;
            }
            let modified_ms = metadata
                .last_modified()
                .map(SystemTime::from)
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|since| since.as_millis() as u64);
            let object = ExternalObject {
                key: format!("{scope}{}", entry.path()),
                size: metadata.content_length(),
                etag: metadata.etag().map(str::to_string),
                modified_ms,
            };
            page.insert(object.key.clone(), object);
            if page.len() > limit {
                page.pop_last();
            }
            if ordered && page.len() == limit {
                break;
            }
        }
        let next = match page.len() == limit {
            true => page.keys().next_back().cloned(),
            false => None,
        };
        Ok(BlobEvent::ExternalListed {
            objects: page.into_values().collect(),
            next,
        })
    }

    pub(super) async fn hash_external(
        &self,
        resolved: ResolvedBackend,
        path: String,
        created_by: UserId,
    ) -> BlobEvent {
        self.external_hash(resolved, path, created_by)
            .await
            .unwrap_or_else(BlobEvent::Error)
    }

    /// The location names the exact key with an empty storage bucket, which marks it as
    /// external; its hashes describe the bytes as they were read.
    async fn external_hash(
        &self,
        resolved: ResolvedBackend,
        path: String,
        created_by: UserId,
    ) -> Result<BlobEvent, BlobError> {
        let root = self.registry.config_for(&resolved.backend)?.root;
        let location = BackendLocation {
            backend: resolved.backend,
            storage_class: resolved.storage_class,
            root,
            storage_bucket: String::new(),
            backend_path: path,
            ulid: Ulid::generate(),
            format: StoredFormat::default(),
            created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        };
        let operator = self.operator_from_location(&location)?;
        let storage_path = self.object_path(&location)?;
        let read_error = |error: &str| BlobError::ReadError(error.to_string());
        let open_error = |error: opendal::Error| match error.kind() {
            opendal::ErrorKind::NotFound => BlobError::Unreadable(error.to_string()),
            _ => read_error(&error.to_string()),
        };
        // OpenDAL trims and drops empty parts, so `raw//a.txt` would read `raw/a.txt`.
        if opendal::raw::normalize_path(&storage_path) != storage_path {
            let message = "the backend cannot read this key exactly".to_string();
            return Err(BlobError::Unreadable(message));
        }
        // The stat names the version that is read, so a later overwrite cannot change it.
        let metadata = timeout(self.io_timeout(), operator.stat(&storage_path))
            .await
            .map_err(|_| read_error("timed out reading the object"))?
            .map_err(open_error)?;
        let version = backend_version(&operator, &metadata);
        let etag = metadata.etag().map(str::to_string);
        // Without a version the read names the stated ETag where the storage can, so the hash
        // and the ETag belong to the same bytes.
        let reader = operator.reader_with(&storage_path);
        let capability = operator.info().capability();
        let opened = match (&version, &etag) {
            (Some(version), _) if capability.read_with_version => reader.version(version),
            (None, Some(etag)) if capability.read_with_if_match => reader.if_match(etag),
            _ => reader,
        };
        let reader = timeout(self.io_timeout(), opened)
            .await
            .map_err(|_| read_error("timed out opening the object"))?
            .map_err(open_error)?;
        let stream = timeout(self.io_timeout(), reader.into_bytes_stream(..))
            .await
            .map_err(|_| read_error("timed out starting the object read"))?
            .map_err(|error| read_error(&error.to_string()))?;
        let mut stream = Box::pin(stream);
        let mut hasher = Hasher::new();
        let mut size = 0u64;
        while let Some(chunk) = timeout(self.transfer_idle_timeout(), stream.next())
            .await
            .map_err(|_| read_error("object read idle timeout"))?
        {
            let chunk = chunk.map_err(|error| read_error(&error.to_string()))?;
            size = size.saturating_add(chunk.len() as u64);
            hasher.update(&chunk);
        }
        Ok(BlobEvent::ExternalHashed {
            location: BackendLocation {
                blob_size: size,
                hashes: hasher.to_map(),
                ..location
            },
            version,
            etag,
        })
    }
}
