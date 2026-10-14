//! Writes objects to fixed paths of a tenant's folders for backend imports.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use super::io::compose_chunk;
use aruna_core::UserId;
use aruna_core::errors::BlobError;
use aruna_core::events::BlobEvent;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::storage::backend_import::PlainPath;
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef, ResolvedBackend};
use aruna_core::structs::storage::format::{Compression, StoredFormat};
use bytes::Bytes;
use std::collections::HashMap;
use std::time::SystemTime;
use tokio::time::timeout;
use ulid::Ulid;

impl BlobHandler {
    /// The location of `path` on `backend`; the empty storage bucket marks a tenant path.
    fn plain_location(
        &self,
        backend: BackendRef,
        storage_class: Option<String>,
        plain: PlainPath,
        created_by: UserId,
    ) -> Result<BackendLocation, BlobError> {
        let root = self.registry.config_for(&backend)?.root.clone();
        Ok(BackendLocation {
            backend,
            storage_class,
            root,
            storage_bucket: String::new(),
            backend_path: plain.path,
            ulid: plain.ulid,
            format: StoredFormat::default(),
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
        size: Option<u64>,
    ) -> BlobEvent {
        let class = resolved.storage_class.clone();
        let location = match self.plain_location(resolved.backend, class, plain, created_by) {
            Ok(location) => location,
            Err(error) => return BlobEvent::Error(error),
        };
        let operator = match self.operator_from_location(&location) {
            Ok(operator) => operator,
            Err(error) => return BlobEvent::Error(error),
        };
        let written = self.write_encoded(
            location,
            operator,
            blob,
            Compression::Off,
            (None, None, None),
            size,
        );
        Box::pin(written).await
    }

    pub(super) async fn compose_plain(
        &self,
        resolved: ResolvedBackend,
        plain: PlainPath,
        created_by: UserId,
        parts: Vec<BackendLocation>,
    ) -> BlobEvent {
        let class = resolved.storage_class.clone();
        let location = match self.plain_location(resolved.backend, class, plain, created_by) {
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
        let composed = self.compose_parts(location, operator, parts, chunk, Compression::Off);
        Box::pin(composed).await
    }

    /// Copies the raw bytes at `source` to `path`; the caller compares the hashes it gets back.
    pub(super) async fn copy_aside(&self, source: BackendLocation, path: String) -> BlobEvent {
        let read_error = |error: &str| BlobEvent::Error(BlobError::ReadError(error.to_string()));
        let plain = PlainPath {
            path,
            ulid: Ulid::generate(),
        };
        let class = source.storage_class.clone();
        let target =
            match self.plain_location(source.backend.clone(), class, plain, source.created_by) {
                Ok(target) => target,
                Err(error) => return BlobEvent::Error(error),
            };
        let (operator, source_path) = match self
            .operator_from_location(&source)
            .and_then(|operator| Ok((operator, source.get_storage_path()?)))
        {
            Ok(opened) => opened,
            Err(error) => return BlobEvent::Error(error),
        };
        let reader = match timeout(self.io_timeout(), operator.reader(&source_path)).await {
            Ok(Ok(reader)) => reader,
            Ok(Err(error)) => return read_error(&error.to_string()),
            Err(_) => return read_error("timed out opening the replaced object"),
        };
        let stream = match timeout(self.io_timeout(), reader.into_bytes_stream(..)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(error)) => return read_error(&error.to_string()),
            Err(_) => return read_error("timed out reading the replaced object"),
        };
        let target_operator = match self.operator_from_location(&target) {
            Ok(operator) => operator,
            Err(error) => return BlobEvent::Error(error),
        };
        let written = self.write_encoded(
            target,
            target_operator,
            BackendStream::new(stream),
            Compression::Off,
            (None, None, None),
            Some(source.blob_size),
        );
        Box::pin(written).await
    }

    /// `false` while another write holds the path.
    pub(super) fn claim_plain(&self, backend: BackendRef, path: String, id: Ulid) -> bool {
        let Ok(mut claims) = self.plain_claims.lock() else {
            return false;
        };
        match claims.get(&(backend.clone(), path.clone())) {
            Some(holder) => *holder == id,
            None => {
                claims.insert((backend, path), id);
                true
            }
        }
    }

    pub(super) fn release_plain(&self, id: Ulid) {
        if let Ok(mut claims) = self.plain_claims.lock() {
            claims.retain(|_, holder| *holder != id);
        }
    }
}
