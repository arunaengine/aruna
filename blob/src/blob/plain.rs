//! Writes objects to fixed paths of a tenant's folders for backend imports.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::backend::{key_fits, split_bucket};
use super::io::{backend_version, compose_chunk};
use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::handle::Handle as _;
use aruna_core::keyspaces::BACKEND_VERSION_KEYSPACE;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::storage::backend_import::{PlainPath, tagged_path, version_key};
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, ResolvedBackend};
use aruna_core::structs::storage::format::{Compression, StoredFormat, StoredLayout};
use bytes::Bytes;
use opendal::{ErrorKind, Operator};
use std::collections::HashMap;
use std::time::SystemTime;
use tokio::time::timeout;

/// Without conditional writes a stat proves nothing, so a write that must find its path
/// free goes to its tagged path, chosen before any byte is read.
pub(super) fn conditional(
    operator: &Operator,
    location: &mut BackendLocation,
    plain: &PlainPath,
) -> bool {
    if operator.info().capability().write_with_if_not_exists || !plain.if_not_exists {
        return plain.if_not_exists;
    }
    if !plain.path.ends_with(&format!("~{}", plain.ulid)) {
        location.backend_path = tagged_path(&plain.path, plain.ulid);
    }
    false
}

impl BlobHandler {
    /// The location of `path` on `backend`; the empty storage bucket marks a tenant path.
    fn plain_location(
        &self,
        backend: BackendRef,
        storage_class: Option<String>,
        plain: &PlainPath,
        created_by: UserId,
    ) -> Result<BackendLocation, BlobError> {
        let root = self.registry.config_for(&backend)?.root.clone();
        Ok(BackendLocation {
            backend,
            storage_class,
            root,
            storage_bucket: String::new(),
            backend_path: plain.path.clone(),
            ulid: plain.ulid,
            format: StoredFormat {
                layout: StoredLayout::Plain(plain.ulid),
                ..StoredFormat::default()
            },
            created_by,
            created_at: SystemTime::now(),
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        })
    }

    /// No cleanup marker is written: nothing ever deletes bytes at a tenant path.
    pub(super) async fn write_plain(
        &self,
        resolved: ResolvedBackend,
        plain: PlainPath,
        created_by: UserId,
        blob: BackendStream<Result<Bytes, StreamError>>,
    ) -> BlobEvent {
        let class = resolved.storage_class.clone();
        let mut location = match self.plain_location(resolved.backend, class, &plain, created_by) {
            Ok(location) => location,
            Err(error) => return BlobEvent::Error(error),
        };
        let operator = match self.operator_from_location(&location) {
            Ok(operator) => operator,
            Err(error) => return BlobEvent::Error(error),
        };
        let conditional = conditional(&operator, &mut location, &plain);
        Box::pin(self.write_tenant(location, operator, blob, conditional)).await
    }

    pub(super) async fn compose_plain(
        &self,
        resolved: ResolvedBackend,
        plain: PlainPath,
        created_by: UserId,
        parts: Vec<BackendLocation>,
    ) -> BlobEvent {
        let class = resolved.storage_class.clone();
        let mut location = match self.plain_location(resolved.backend, class, &plain, created_by) {
            Ok(location) => location,
            Err(error) => return BlobEvent::Error(error),
        };
        let (operator, backend_type) = match self
            .operator_from_location(&location)
            .and_then(|operator| Ok((operator, self.registry.config_for(&location.backend)?)))
        {
            Ok((operator, config)) => (operator, config.backend_type),
            Err(error) => return BlobEvent::Error(error),
        };
        let total = parts.iter().map(|part| part.blob_size).sum();
        let chunk = compose_chunk(&backend_type, total, false);
        let conditional = conditional(&operator, &mut location, &plain);
        let composed = self.compose_parts(
            location,
            operator,
            parts,
            chunk,
            Compression::Off,
            conditional,
        );
        Box::pin(composed).await
    }

    pub(super) async fn stat_plain(&self, backend: BackendRef, path: String) -> BlobEvent {
        let (bucket, path) = match self.registry.splits(&backend) {
            true => match split_bucket(&path) {
                Ok(parts) => parts,
                Err(error) => return BlobEvent::Error(error),
            },
            false => (String::new(), path),
        };
        // A key the provider would refuse is answered before any request.
        let fits = self
            .registry
            .config_for(&backend)
            .and_then(|config| key_fits(&config.root, &path));
        if let Err(error) = fits {
            return BlobEvent::Error(error);
        }
        let operator = match self
            .registry
            .bucket_operator(&backend, &bucket, &self.egress)
        {
            Ok(operator) => operator,
            Err(error) => return BlobEvent::Error(error),
        };
        match timeout(self.io_timeout(), operator.stat(&path)).await {
            Ok(Ok(metadata)) => BlobEvent::PlainStat {
                exists: true,
                version: backend_version(&operator, &metadata),
            },
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => BlobEvent::PlainStat {
                exists: false,
                version: None,
            },
            Ok(Err(error)) => BlobEvent::Error(BlobError::ReadError(error.to_string())),
            Err(_) => BlobEvent::Error(BlobError::ReadError("timed out reading the path".into())),
        }
    }

    /// Whether the backend still has the object of `location`, at the backend version reads use.
    pub(super) async fn stat_copy(&self, location: BackendLocation) -> BlobEvent {
        let (operator, path) = match self
            .operator_from_location(&location)
            .and_then(|operator| Ok((operator, self.object_path(&location)?)))
        {
            Ok(found) => found,
            Err(error) => return BlobEvent::Error(error),
        };
        let version = match self.read_version(&location).await {
            Ok(version) => version,
            Err(error) => return BlobEvent::Error(error),
        };
        let stat = operator.stat_with(&path);
        let stat = match version.filter(|_| operator.info().capability().stat_with_version) {
            Some(version) => stat.version(&version),
            None => stat,
        };
        match timeout(self.io_timeout(), stat.into_future()).await {
            Ok(Ok(_)) => BlobEvent::CopyStat { exists: true },
            Ok(Err(error)) if error.kind() == ErrorKind::NotFound => {
                BlobEvent::CopyStat { exists: false }
            }
            Ok(Err(error)) => BlobEvent::Error(BlobError::ReadError(error.to_string())),
            Err(_) => BlobEvent::Error(BlobError::ReadError("timed out reading the path".into())),
        }
    }

    /// The backend version a write recorded for a tenant path; `None` reads the newest bytes.
    pub(super) async fn read_version(
        &self,
        location: &BackendLocation,
    ) -> Result<Option<String>, BlobError> {
        if !location.is_external() {
            return Ok(None);
        }
        let read = StorageEffect::Read {
            key_space: BACKEND_VERSION_KEYSPACE.to_string(),
            key: version_key(location.ulid),
            txn_id: None,
        };
        match self.storage.send_effect(Effect::Storage(read)).await {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value
                .map(|value| String::from_utf8(value.to_vec()))
                .transpose()
                .map_err(|error| BlobError::ReadError(error.to_string())),
            other => Err(BlobError::ReadError(format!(
                "failed to read the backend version: {other:?}"
            ))),
        }
    }
}
