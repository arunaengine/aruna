//! Lists and hashes objects on a backend that Aruna did not write, for backend imports.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::io::backend_version;
use crate::hash::Hasher;
use crate::s3::RESERVATION;
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

/// Whether the object is the empty reservation a write holds its path with, not a file.
fn reservation(metadata: &opendal::Metadata) -> bool {
    let marked = metadata.user_metadata();
    metadata.content_length() == 0 && marked.is_some_and(|marked| marked.get(RESERVATION).is_some())
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
            // A listing tells no user metadata, so an empty file is looked at for the marker.
            if metadata.content_length() == 0 {
                let stat = timeout(self.io_timeout(), operator.stat(entry.path())).await;
                let stat = stat.map_err(|_| list_error("timed out reading an empty file"))?;
                match stat {
                    Ok(found) if reservation(&found) => continue,
                    Ok(_) => {}
                    Err(error) if error.kind() == opendal::ErrorKind::NotFound => continue,
                    Err(error) => return Err(list_error(error)),
                }
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

    /// The location of the exact key with an empty storage bucket, which marks it as external;
    /// it has no size or hashes yet.
    fn external_location(
        &self,
        resolved: ResolvedBackend,
        path: String,
        created_by: UserId,
    ) -> Result<BackendLocation, BlobError> {
        let root = self.registry.config_for(&resolved.backend)?.root;
        Ok(BackendLocation {
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
        })
    }

    pub(super) async fn stat_external(
        &self,
        resolved: ResolvedBackend,
        path: String,
        created_by: UserId,
    ) -> BlobEvent {
        self.external_stat(resolved, path, created_by)
            .await
            .unwrap_or_else(BlobEvent::Error)
    }

    /// The size, ETag and version the provider reports for the file, without reading it. A
    /// write's empty reservation counts as no file.
    async fn external_stat(
        &self,
        resolved: ResolvedBackend,
        path: String,
        created_by: UserId,
    ) -> Result<BlobEvent, BlobError> {
        let mut location = self.external_location(resolved, path, created_by)?;
        let operator = self.operator_from_location(&location)?;
        let storage_path = self.object_path(&location)?;
        // OpenDAL trims and drops empty parts, so `raw//a.txt` would stat `raw/a.txt`.
        if opendal::raw::normalize_path(&storage_path) != storage_path {
            let message = "the backend cannot read this key exactly".to_string();
            return Err(BlobError::Unreadable(message));
        }
        let stat = timeout(self.io_timeout(), operator.stat(&storage_path)).await;
        let metadata = match stat {
            Ok(Ok(metadata)) if !reservation(&metadata) => metadata,
            Ok(Ok(_)) => {
                return Ok(BlobEvent::ExternalStat {
                    location: None,
                    version: None,
                    etag: None,
                });
            }
            Ok(Err(error)) if error.kind() == opendal::ErrorKind::NotFound => {
                return Ok(BlobEvent::ExternalStat {
                    location: None,
                    version: None,
                    etag: None,
                });
            }
            Ok(Err(error)) => return Err(BlobError::ReadError(error.to_string())),
            Err(_) => return Err(BlobError::ReadError("timed out reading the object".into())),
        };
        location.blob_size = metadata.content_length();
        Ok(BlobEvent::ExternalStat {
            version: backend_version(&operator, &metadata),
            etag: metadata.etag().map(str::to_string),
            location: Some(location),
        })
    }

    /// The location names the exact key with an empty storage bucket, which marks it as
    /// external; its hashes describe the bytes as they were read. A write's empty reservation
    /// is no file and unreadable.
    async fn external_hash(
        &self,
        resolved: ResolvedBackend,
        path: String,
        created_by: UserId,
    ) -> Result<BlobEvent, BlobError> {
        let location = self.external_location(resolved, path, created_by)?;
        let operator = self.operator_from_location(&location)?;
        let storage_path = self.object_path(&location)?;
        let read_error = |error: &str| BlobError::ReadError(error.to_string());
        let reserved = || BlobError::Unreadable("the path holds a write's reservation".into());
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
        if reservation(&metadata) {
            return Err(reserved());
        }
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
        let mut stream = timeout(self.io_timeout(), reader.into_bytes_stream(..))
            .await
            .map_err(|_| read_error("timed out starting the object read"))?
            .map_err(|error| read_error(&error.to_string()))?;
        // The ETag is the one of the response hashed; a storage that reports none has none.
        let (etag, marked) = match timeout(self.io_timeout(), stream.metadata()).await {
            Ok(Ok(read)) => (read.etag().map(str::to_string), reservation(&read)),
            Ok(Err(error)) if error.kind() == opendal::ErrorKind::Unsupported => (None, false),
            Ok(Err(error)) => return Err(read_error(&error.to_string())),
            Err(_) => return Err(read_error("timed out opening the object read")),
        };
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
        // An empty file shares a reservation's ETag, so the read may find one the stat did not.
        if marked && size == 0 {
            return Err(reserved());
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
