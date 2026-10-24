//! Picks and reserves backend buckets and hidden blob keys, and tracks bucket load stats.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::BlobHandler;
use crate::error::BlobLibError;
use crate::s3::make_bucket;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    BLOB_CLEANUP_KEYSPACE, BUCKET_FREE_INDEX, BUCKET_STATS_DB, HIDDEN_RESERVATION_KEYSPACE,
};
use aruna_core::structs::storage::blob::{
    Backend, BackendConfig, BackendLocation, BackendRef, BlobCleanupWork, HIDDEN_BLOB_PREFIX,
    HiddenBlobKey, MULTIPART_PART_PREFIX, ensure_confined_path,
};
use aruna_core::structs::storage::group_backend::{DEFAULT_BUCKET, MAX_KEY_LEN, PARTS_PREFIX};
use aruna_core::types::TxnId;
use aruna_storage::storage::TransactionOwner;
use byteview::ByteView;
use opendal::Operator;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use ulid::Ulid;

impl From<BlobError> for BlobLibError {
    fn from(error: BlobError) -> Self {
        BlobLibError::IoError(std::io::Error::other(error.to_string()))
    }
}

const BUCKET_STATS_RETRIES: u32 = 32;
const BUCKET_STATS_BACKOFF: Duration = Duration::from_millis(1);
const STATS_BACKOFF_CAP: Duration = Duration::from_millis(50);
// A fresh bucket is private to the reserving writer, so a second round only
// happens when another writer filled the bucket we picked.
const BUCKET_RESERVE_ROUNDS: usize = 8;
const ACTIVE_RESERVATION_LIMIT: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LoadUpdate {
    Applied,
    Conflict,
    Full,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReleaseUpdate {
    Applied,
    Missing,
    Conflict,
    Unknown,
}

#[derive(Debug)]
pub(super) struct ReservationGuard {
    active: Arc<StdMutex<std::collections::HashSet<Ulid>>>,
    id: Ulid,
    remove_on_drop: bool,
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if self.remove_on_drop
            && let Ok(mut active) = self.active.lock()
        {
            active.remove(&self.id);
        }
    }
}

impl ReservationGuard {
    pub(super) fn retain(&mut self) {
        self.remove_on_drop = false;
    }
}

// Ulid randomness spreads retry storms without pulling in an rng dependency.
fn conflict_backoff(attempt: u32) -> Duration {
    let base = BUCKET_STATS_BACKOFF
        .saturating_mul(1u32 << attempt.min(6))
        .min(STATS_BACKOFF_CAP);
    let micros = base.as_micros() as u64;
    let spread = u64::from(Ulid::generate().to_bytes()[15]);
    Duration::from_micros(micros / 2 + micros * spread / 510)
}

fn reservation_key(key: &HiddenBlobKey) -> Result<ByteView, BlobError> {
    postcard::to_allocvec(key)
        .map(ByteView::from)
        .map_err(|error| BlobError::ConversionError(error.into()))
}

pub(super) fn intent_key(location: &BackendLocation) -> ByteView {
    ByteView::from(location.ulid.to_bytes().to_vec())
}

pub(super) fn intent_value(location: &BackendLocation) -> Result<ByteView, BlobError> {
    BlobCleanupWork::ReconcileReservation {
        location: location.clone(),
    }
    .to_bytes()
    .map(ByteView::from)
    .map_err(BlobError::ConversionError)
}

impl BlobHandler {
    pub(super) async fn ensure_multipart_bucket(&self) -> Result<(), BlobError> {
        for (_, backend) in self.registry.entries() {
            if backend.config.backend_type != Backend::S3 {
                continue;
            }
            let Some(bucket) = backend.config.multipart_bucket.as_deref() else {
                continue;
            };
            make_bucket(bucket, &backend.config.service_config).await?;
        }
        Ok(())
    }

    pub(super) fn multipart_bucket(&self, backend: &BackendRef) -> Result<String, BlobError> {
        self.registry
            .config_for(backend)?
            .multipart_bucket
            .clone()
            .ok_or_else(|| {
                BlobError::OperatorCreationFailed("multipart bucket not configured".to_string())
            })
    }

    pub(super) async fn eval_backend_bucket(
        &self,
        backend: &BackendRef,
    ) -> Result<String, BlobError> {
        self.select_backend_bucket(backend, &[]).await
    }

    async fn select_backend_bucket(
        &self,
        backend: &BackendRef,
        skip: &[String],
    ) -> Result<String, BlobError> {
        let config = self.registry.config_for(backend)?.clone();
        if let Some(bucket) = fixed_bucket(&config)? {
            return Ok(bucket);
        }

        // With a limit the free index names the emptiest bucket below it; one seek either way.
        let qualifier = stats_prefix(backend, None);
        let found = match config.max_bucket_size {
            Some(_) => {
                let name_at = qualifier.len() + 8;
                self.first_bucket(BUCKET_FREE_INDEX, qualifier, name_at, skip)
                    .await?
            }
            None => {
                let prefix = stats_prefix(backend, config.bucket_prefix.as_deref());
                self.first_bucket(BUCKET_STATS_DB, prefix, qualifier.len(), skip)
                    .await?
            }
        };
        if let Some(bucket) = found {
            return Ok(bucket);
        }

        let bucket_name = generate_bucket_name(config.bucket_prefix.as_deref());

        if Backend::S3 == config.backend_type {
            make_bucket(&bucket_name, &config.service_config).await?;
        }

        self.write_bucket_load(backend, &bucket_name, 0).await?;
        Ok(bucket_name)
    }

    // Reserves a slot and records the physical object before any bytes exist.
    pub(super) async fn reserve_bucket(
        &self,
        backend: &BackendRef,
        template: &BackendLocation,
    ) -> Result<BackendLocation, BlobError> {
        let config = self.registry.config_for(backend)?.clone();
        let Some(max_bucket_size) = config.max_bucket_size else {
            let bucket = Box::pin(self.eval_backend_bucket(backend)).await?;
            let location = location_bucket(template, &bucket);
            self.reserve_load(backend, &bucket, None, Some(&location))
                .await?;
            return Ok(location);
        };
        if let Some(bucket) = config.service_config.get("bucket") {
            let location = location_bucket(template, bucket);
            self.reserve_load(backend, bucket, None, Some(&location))
                .await?;
            return Ok(location);
        }

        let mut full = Vec::new();
        for _ in 0..BUCKET_RESERVE_ROUNDS {
            let bucket = Box::pin(self.select_backend_bucket(backend, &full)).await?;
            let location = location_bucket(template, &bucket);
            if self
                .reserve_load(backend, &bucket, Some(max_bucket_size), Some(&location))
                .await?
            {
                return Ok(location);
            }
            full.push(bucket);
        }
        Err(BlobError::WriteError(
            "no backend bucket had free capacity".to_string(),
        ))
    }

    pub(super) fn hold_reservation(&self, id: Ulid) -> Option<ReservationGuard> {
        if let Ok(mut active) = self.reservation_active.lock() {
            if active.len() >= ACTIVE_RESERVATION_LIMIT && !active.contains(&id) {
                return None;
            }
            active.insert(id);
        } else {
            return None;
        }
        Some(ReservationGuard {
            active: Arc::clone(&self.reservation_active),
            id,
            remove_on_drop: true,
        })
    }

    pub(super) fn clear_active(&self, id: Ulid) {
        if let Ok(mut active) = self.reservation_active.lock() {
            active.remove(&id);
        }
    }

    pub(super) fn reservation_active(&self, id: Ulid) -> bool {
        self.reservation_active
            .lock()
            .is_ok_and(|active| active.contains(&id))
    }

    pub(super) async fn reserve_hidden_key(
        &self,
        backend: &BackendRef,
        root: &str,
        backend_path: &str,
        key_slot: Arc<StdMutex<Option<HiddenBlobKey>>>,
    ) -> Result<HiddenBlobKey, BlobError> {
        let config = self.registry.config_for(backend)?.clone();
        let rounds = if config.max_bucket_size.is_some() {
            BUCKET_RESERVE_ROUNDS
        } else {
            1
        };
        let mut full = Vec::new();
        for _ in 0..rounds {
            let bucket = Box::pin(self.select_backend_bucket(backend, &full)).await?;
            let key = HiddenBlobKey::new(
                backend.clone(),
                root.to_string(),
                bucket.clone(),
                backend_path.to_string(),
            )
            .map_err(BlobError::ConversionError)?;
            *key_slot.lock().map_err(|_| {
                BlobError::ReadError("hidden bucket reservation state is poisoned".to_string())
            })? = Some(key.clone());
            let outcome = match self
                .try_reserve_key(backend, &bucket, &key, config.max_bucket_size)
                .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    if let Err(cleanup) = self.release_hidden_key(&key).await {
                        return Err(BlobError::ReadError(format!(
                            "{error}; reservation cleanup failed: {cleanup}"
                        )));
                    }
                    return Err(error);
                }
            };
            match outcome {
                LoadUpdate::Applied => return Ok(key),
                LoadUpdate::Full => full.push(bucket),
                LoadUpdate::Conflict => {
                    return Err(BlobError::ReadError(
                        "hidden bucket reservation kept conflicting".to_string(),
                    ));
                }
            }
        }
        Err(BlobError::WriteError(
            "no backend bucket had free capacity".to_string(),
        ))
    }

    pub(super) async fn release_hidden_key(&self, key: &HiddenBlobKey) -> Result<(), BlobError> {
        if !self.stats_managed(&key.backend, &key.storage_bucket) {
            return Ok(());
        }
        let marker = reservation_key(key)?;
        let mut last_error = None;
        for attempt in 0..BUCKET_STATS_RETRIES {
            match self.release_key_txn(key, &marker).await {
                Ok(LoadUpdate::Applied) => return Ok(()),
                Ok(LoadUpdate::Conflict) if attempt + 1 < BUCKET_STATS_RETRIES => {
                    tokio::time::sleep(conflict_backoff(attempt)).await;
                }
                Ok(LoadUpdate::Conflict) => break,
                Ok(LoadUpdate::Full) => unreachable!("release cannot fill a bucket"),
                Err(error) if attempt + 1 < BUCKET_STATS_RETRIES => {
                    last_error = Some(error);
                    tokio::time::sleep(conflict_backoff(attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(error) = last_error {
            return Err(error);
        }
        Err(BlobError::ReadError(
            "hidden bucket release kept conflicting".to_string(),
        ))
    }

    async fn try_reserve_key(
        &self,
        backend: &BackendRef,
        bucket: &str,
        key: &HiddenBlobKey,
        capacity: Option<u64>,
    ) -> Result<LoadUpdate, BlobError> {
        if !self.stats_managed(backend, bucket) {
            return Ok(LoadUpdate::Applied);
        }
        let marker = reservation_key(key)?;
        let mut last_error = None;
        for attempt in 0..BUCKET_STATS_RETRIES {
            match self
                .reserve_key_txn(backend, bucket, &marker, capacity)
                .await
            {
                Ok(LoadUpdate::Conflict) if attempt + 1 < BUCKET_STATS_RETRIES => {
                    tokio::time::sleep(conflict_backoff(attempt)).await;
                }
                Ok(outcome) => return Ok(outcome),
                Err(error) if attempt + 1 < BUCKET_STATS_RETRIES => {
                    last_error = Some(error);
                    tokio::time::sleep(conflict_backoff(attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(error) = last_error {
            return Err(error);
        }
        Ok(LoadUpdate::Conflict)
    }

    async fn reserve_key_txn(
        &self,
        backend: &BackendRef,
        bucket: &str,
        marker: &ByteView,
        capacity: Option<u64>,
    ) -> Result<LoadUpdate, BlobError> {
        let mut owner = self
            .storage
            .start_transaction(false)
            .await
            .map_err(|error| {
                BlobError::ReadError(format!(
                    "failed to start hidden bucket transaction: {error}"
                ))
            })?;
        let Some(txn_id) = owner.id() else {
            return Err(BlobError::ReadError(
                "hidden bucket transaction owner missing transaction".to_string(),
            ));
        };
        let marker_exists = match self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: HIDDEN_RESERVATION_KEYSPACE.to_string(),
                key: marker.clone(),
                txn_id: Some(txn_id),
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            _ => {
                self.abort_stats_txn(&mut owner).await;
                return Err(BlobError::ReadError(
                    "failed to read hidden bucket reservation".to_string(),
                ));
            }
        };
        if marker_exists {
            self.abort_stats_txn(&mut owner).await;
            return Ok(LoadUpdate::Applied);
        }
        let load = match self.bucket_load_txn(backend, bucket, txn_id).await {
            Ok(load) => load,
            Err(error) => {
                self.abort_stats_txn(&mut owner).await;
                return Err(error);
            }
        };
        if capacity.is_some_and(|capacity| load >= capacity) {
            self.abort_stats_txn(&mut owner).await;
            return Ok(LoadUpdate::Full);
        }
        let adjusted = load.saturating_add(1);
        let stats_key = stats_key(backend, bucket).into();
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: BUCKET_STATS_DB.to_string(),
                key: stats_key,
                value: adjusted.to_le_bytes().to_vec().into(),
                txn_id: Some(txn_id),
            }))
            .await;
        if !matches!(event, Event::Storage(StorageEvent::WriteResult { .. })) {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to reserve hidden bucket capacity".to_string(),
            ));
        }
        if !self
            .move_free_row(backend, bucket, (load, adjusted), txn_id)
            .await
        {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to update the bucket free index".to_string(),
            ));
        }
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: HIDDEN_RESERVATION_KEYSPACE.to_string(),
                key: marker.clone(),
                value: ByteView::from(vec![1]),
                txn_id: Some(txn_id),
            }))
            .await;
        if !matches!(event, Event::Storage(StorageEvent::WriteResult { .. })) {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to persist hidden bucket reservation".to_string(),
            ));
        }
        match self
            .storage
            .send_effect(Effect::Storage(StorageEffect::CommitTransaction { txn_id }))
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed })
                if committed == txn_id =>
            {
                owner.finish();
                Ok(LoadUpdate::Applied)
            }
            Event::Storage(StorageEvent::Error { error }) => match error {
                StorageError::TransactionConflict => {
                    owner.finish();
                    Ok(LoadUpdate::Conflict)
                }
                StorageError::TransactionNotFound => {
                    owner.finish();
                    Err(BlobError::ReadError(
                        "hidden bucket transaction was not found".to_string(),
                    ))
                }
                StorageError::QueueFull => Err(BlobError::ReadError(
                    "failed to reserve hidden bucket: queue full".to_string(),
                )),
                error => {
                    owner.unknown();
                    Err(BlobError::ReadError(format!(
                        "failed to reserve hidden bucket: {error}"
                    )))
                }
            },
            other => {
                owner.unknown();
                Err(BlobError::ReadError(format!(
                    "unexpected hidden bucket reservation event: {other:?}"
                )))
            }
        }
    }

    async fn release_key_txn(
        &self,
        key: &HiddenBlobKey,
        marker: &ByteView,
    ) -> Result<LoadUpdate, BlobError> {
        let mut owner = self
            .storage
            .start_transaction(false)
            .await
            .map_err(|error| {
                BlobError::ReadError(format!("failed to start hidden bucket release: {error}"))
            })?;
        let Some(txn_id) = owner.id() else {
            return Err(BlobError::ReadError(
                "hidden bucket release owner missing transaction".to_string(),
            ));
        };
        let marker_exists = match self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: HIDDEN_RESERVATION_KEYSPACE.to_string(),
                key: marker.clone(),
                txn_id: Some(txn_id),
            }))
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value.is_some(),
            _ => {
                self.abort_stats_txn(&mut owner).await;
                return Err(BlobError::ReadError(
                    "failed to read hidden bucket release".to_string(),
                ));
            }
        };
        if !marker_exists {
            self.abort_stats_txn(&mut owner).await;
            return Ok(LoadUpdate::Applied);
        }
        let load = match self
            .bucket_load_txn(&key.backend, &key.storage_bucket, txn_id)
            .await
        {
            Ok(load) => load,
            Err(error) => {
                self.abort_stats_txn(&mut owner).await;
                return Err(error);
            }
        };
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: BUCKET_STATS_DB.to_string(),
                key: stats_key(&key.backend, &key.storage_bucket).into(),
                value: load.saturating_sub(1).to_le_bytes().to_vec().into(),
                txn_id: Some(txn_id),
            }))
            .await;
        if !matches!(event, Event::Storage(StorageEvent::WriteResult { .. })) {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to release hidden bucket capacity".to_string(),
            ));
        }
        if !self
            .move_free_row(
                &key.backend,
                &key.storage_bucket,
                (load, load.saturating_sub(1)),
                txn_id,
            )
            .await
        {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to update the bucket free index".to_string(),
            ));
        }
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Delete {
                key_space: HIDDEN_RESERVATION_KEYSPACE.to_string(),
                key: marker.clone(),
                txn_id: Some(txn_id),
            }))
            .await;
        if !matches!(event, Event::Storage(StorageEvent::DeleteResult { .. })) {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to delete hidden bucket reservation".to_string(),
            ));
        }
        match self
            .storage
            .send_effect(Effect::Storage(StorageEffect::CommitTransaction { txn_id }))
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed })
                if committed == txn_id =>
            {
                owner.finish();
                Ok(LoadUpdate::Applied)
            }
            Event::Storage(StorageEvent::Error { error }) => match error {
                StorageError::TransactionConflict => {
                    owner.finish();
                    Ok(LoadUpdate::Conflict)
                }
                StorageError::TransactionNotFound => {
                    owner.finish();
                    Err(BlobError::ReadError(
                        "hidden bucket release transaction was not found".to_string(),
                    ))
                }
                StorageError::QueueFull => Err(BlobError::ReadError(
                    "failed to release hidden bucket: queue full".to_string(),
                )),
                error => {
                    owner.unknown();
                    Err(BlobError::ReadError(format!(
                        "failed to release hidden bucket: {error}"
                    )))
                }
            },
            other => {
                owner.unknown();
                Err(BlobError::ReadError(format!(
                    "unexpected hidden bucket release event: {other:?}"
                )))
            }
        }
    }

    /// The first bucket name under `prefix` that `skip` does not hold, read with one seek.
    async fn first_bucket(
        &self,
        key_space: &str,
        prefix: Vec<u8>,
        name_at: usize,
        skip: &[String],
    ) -> Result<Option<String>, BlobError> {
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Iter {
                key_space: key_space.to_string(),
                prefix: Some(prefix.into()),
                start: None,
                limit: skip.len().saturating_add(1),
                txn_id: None,
            }))
            .await;
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return Err(BlobError::ReadError(
                "unexpected storage event while reading bucket stats".to_string(),
            ));
        };
        for (key, _) in values {
            let name = key.as_ref().get(name_at..).unwrap_or_default().to_vec();
            let name = String::from_utf8(name).map_err(ConversionError::from)?;
            if !skip.contains(&name) {
                return Ok(Some(name));
            }
        }
        Ok(None)
    }

    /// The object limit of a bucket the free index tracks; `None` for pinned, group and
    /// multipart buckets and buckets outside the configured prefix.
    fn indexed_limit(&self, backend: &BackendRef, bucket: &str) -> Option<u64> {
        if !self.stats_managed(backend, bucket) {
            return None;
        }
        let config = self.registry.config_for(backend).ok()?;
        if fixed_bucket(&config).ok()?.is_some()
            || !bucket.starts_with(config.bucket_prefix.as_deref().unwrap_or_default())
        {
            return None;
        }
        config.max_bucket_size
    }

    /// Moves a bucket's free index row from load `old` to `new` in the stats transaction; a
    /// bucket at its limit leaves the index.
    async fn move_free_row(
        &self,
        backend: &BackendRef,
        bucket: &str,
        (old, new): (u64, u64),
        txn_id: TxnId,
    ) -> bool {
        let Some(limit) = self.indexed_limit(backend, bucket) else {
            return true;
        };
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Delete {
                key_space: BUCKET_FREE_INDEX.to_string(),
                key: free_key(backend, old, bucket).into(),
                txn_id: Some(txn_id),
            }))
            .await;
        if !matches!(event, Event::Storage(StorageEvent::DeleteResult { .. })) {
            return false;
        }
        if new >= limit {
            return true;
        }
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: BUCKET_FREE_INDEX.to_string(),
                key: free_key(backend, new, bucket).into(),
                value: ByteView::from(Vec::new()),
                txn_id: Some(txn_id),
            }))
            .await;
        matches!(event, Event::Storage(StorageEvent::WriteResult { .. }))
    }

    pub(super) async fn write_bucket_load(
        &self,
        backend: &BackendRef,
        bucket: &str,
        load: u64,
    ) -> Result<(), BlobError> {
        let mut writes = vec![(
            BUCKET_STATS_DB.to_string(),
            stats_key(backend, bucket).into(),
            load.to_le_bytes().to_vec().into(),
        )];
        if self
            .indexed_limit(backend, bucket)
            .is_some_and(|limit| load < limit)
        {
            let key = free_key(backend, load, bucket).into();
            writes.push((
                BUCKET_FREE_INDEX.to_string(),
                key,
                ByteView::from(Vec::new()),
            ));
        }
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::BatchWrite {
                writes,
                txn_id: None,
            }))
            .await;

        match event {
            Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
            Event::Storage(StorageEvent::Error { error }) => Err(BlobError::ReadError(format!(
                "failed to write bucket stats: {error}"
            ))),
            _ => Err(BlobError::ReadError(
                "unexpected storage event while writing bucket stats".to_string(),
            )),
        }
    }

    pub(super) fn stats_managed(&self, backend: &BackendRef, bucket: &str) -> bool {
        // Group storages keep no bucket stats: Aruna writes to one bucket or a tenant path.
        if matches!(backend, BackendRef::Group(_)) {
            return false;
        }
        self.registry
            .config_for(backend)
            .ok()
            .and_then(|config| config.multipart_bucket)
            .as_deref()
            != Some(bucket)
    }

    pub(super) async fn release_reservation(
        &self,
        location: &BackendLocation,
    ) -> Result<(), BlobError> {
        if !self.stats_managed(&location.backend, &location.storage_bucket) {
            return self.clear_marker(location).await;
        }
        let mut last_error = None;
        for attempt in 0..BUCKET_STATS_RETRIES {
            match self.release_marker(location).await {
                Ok(ReleaseUpdate::Applied) => return Ok(()),
                Ok(ReleaseUpdate::Missing) => {
                    self.clear_active(location.ulid);
                    return Ok(());
                }
                Ok(ReleaseUpdate::Conflict) if attempt + 1 < BUCKET_STATS_RETRIES => {
                    tokio::time::sleep(conflict_backoff(attempt)).await;
                }
                Ok(ReleaseUpdate::Conflict) => break,
                Ok(ReleaseUpdate::Unknown) => {
                    return Err(BlobError::ReadError(
                        "bucket reservation release outcome is unknown".to_string(),
                    ));
                }
                Err(error) if attempt + 1 < BUCKET_STATS_RETRIES => {
                    last_error = Some(error);
                    tokio::time::sleep(conflict_backoff(attempt)).await;
                }
                Err(error) => return Err(error),
            }
        }
        last_error.map_or_else(
            || {
                Err(BlobError::ReadError(
                    "bucket reservation release kept conflicting".to_string(),
                ))
            },
            Err,
        )
    }

    pub(super) async fn clear_marker(&self, location: &BackendLocation) -> Result<(), BlobError> {
        let mut owner = self
            .storage
            .start_transaction(false)
            .await
            .map_err(|error| {
                BlobError::ReadError(format!(
                    "failed to start bucket reservation cleanup: {error}"
                ))
            })?;
        let Some(txn_id) = owner.id() else {
            return Err(BlobError::ReadError(
                "bucket reservation cleanup owner missing transaction".to_string(),
            ));
        };
        let marker = match self.marker_exists(location, txn_id).await {
            Ok(marker) => marker,
            Err(error) => {
                self.abort_stats_txn(&mut owner).await;
                return Err(error);
            }
        };
        if !marker {
            self.abort_stats_txn(&mut owner).await;
            self.clear_active(location.ulid);
            return Ok(());
        }
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Delete {
                key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
                key: intent_key(location),
                txn_id: Some(txn_id),
            }))
            .await;
        if !matches!(event, Event::Storage(StorageEvent::DeleteResult { .. })) {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to clear bucket reservation".to_string(),
            ));
        }
        match self
            .storage
            .send_effect(Effect::Storage(StorageEffect::CommitTransaction { txn_id }))
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed })
                if committed == txn_id =>
            {
                owner.finish();
                self.clear_active(location.ulid);
                Ok(())
            }
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }) => {
                owner.finish();
                Err(BlobError::ReadError(
                    "bucket reservation cleanup conflicted".to_string(),
                ))
            }
            Event::Storage(StorageEvent::Error { error }) => {
                owner.unknown();
                Err(BlobError::ReadError(format!(
                    "failed to clear bucket reservation: {error}"
                )))
            }
            other => {
                owner.unknown();
                Err(BlobError::ReadError(format!(
                    "unexpected bucket reservation cleanup event: {other:?}"
                )))
            }
        }
    }

    async fn release_marker(&self, location: &BackendLocation) -> Result<ReleaseUpdate, BlobError> {
        let mut owner = self
            .storage
            .start_transaction(false)
            .await
            .map_err(|error| {
                BlobError::ReadError(format!(
                    "failed to start bucket reservation release: {error}"
                ))
            })?;
        let Some(txn_id) = owner.id() else {
            return Err(BlobError::ReadError(
                "bucket reservation release owner missing transaction".to_string(),
            ));
        };
        if !self.marker_exists(location, txn_id).await? {
            self.abort_stats_txn(&mut owner).await;
            return Ok(ReleaseUpdate::Missing);
        }
        let load = match self
            .bucket_load_txn(&location.backend, &location.storage_bucket, txn_id)
            .await
        {
            Ok(load) => load,
            Err(error) => {
                self.abort_stats_txn(&mut owner).await;
                return Err(error);
            }
        };
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: BUCKET_STATS_DB.to_string(),
                key: stats_key(&location.backend, &location.storage_bucket).into(),
                value: load.saturating_sub(1).to_le_bytes().to_vec().into(),
                txn_id: Some(txn_id),
            }))
            .await;
        if !matches!(event, Event::Storage(StorageEvent::WriteResult { .. })) {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to release bucket capacity".to_string(),
            ));
        }
        if !self
            .move_free_row(
                &location.backend,
                &location.storage_bucket,
                (load, load.saturating_sub(1)),
                txn_id,
            )
            .await
        {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to update the bucket free index".to_string(),
            ));
        }
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Delete {
                key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
                key: intent_key(location),
                txn_id: Some(txn_id),
            }))
            .await;
        if !matches!(event, Event::Storage(StorageEvent::DeleteResult { .. })) {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to delete bucket reservation".to_string(),
            ));
        }
        match self
            .storage
            .send_effect(Effect::Storage(StorageEffect::CommitTransaction { txn_id }))
            .await
        {
            Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed })
                if committed == txn_id =>
            {
                owner.finish();
                self.clear_active(location.ulid);
                Ok(ReleaseUpdate::Applied)
            }
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionConflict,
            }) => {
                owner.finish();
                Ok(ReleaseUpdate::Conflict)
            }
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            }) => {
                owner.finish();
                Err(BlobError::ReadError(
                    "bucket reservation release transaction was not found".to_string(),
                ))
            }
            Event::Storage(StorageEvent::Error { error: _ }) => {
                owner.unknown();
                Ok(ReleaseUpdate::Unknown)
            }
            other => {
                owner.unknown();
                tracing::warn!(event = ?other, "bucket reservation release outcome is unknown");
                Ok(ReleaseUpdate::Unknown)
            }
        }
    }

    async fn marker_exists(
        &self,
        location: &BackendLocation,
        txn_id: TxnId,
    ) -> Result<bool, BlobError> {
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
                key: intent_key(location),
                txn_id: Some(txn_id),
            }))
            .await;
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_some()),
            _ => Err(BlobError::ReadError(
                "failed to read bucket reservation".to_string(),
            )),
        }
    }

    /// `None` when no row is left; `Some(false)` when a newer row reuses the key.
    pub(super) async fn marker_matches(
        &self,
        location: &BackendLocation,
    ) -> Result<Option<bool>, BlobError> {
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
                key: intent_key(location),
                txn_id: None,
            }))
            .await;
        match event {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.map(|value| {
                matches!(
                    BlobCleanupWork::from_bytes(value.as_ref()),
                    Ok(BlobCleanupWork::ReconcileReservation { location: marked })
                        if marked.same_object(location)
                )
            })),
            _ => Err(BlobError::ReadError(
                "failed to read bucket reservation".to_string(),
            )),
        }
    }

    async fn reserve_load(
        &self,
        backend: &BackendRef,
        bucket: &str,
        capacity: Option<u64>,
        location: Option<&BackendLocation>,
    ) -> Result<bool, BlobError> {
        match self
            .retry_load_update(backend, bucket, 1, capacity, location)
            .await?
        {
            LoadUpdate::Applied => Ok(true),
            LoadUpdate::Full => Ok(false),
            LoadUpdate::Conflict => Err(BlobError::ReadError(format!(
                "bucket stats update for {bucket} kept conflicting"
            ))),
        }
    }

    async fn retry_load_update(
        &self,
        backend: &BackendRef,
        bucket: &str,
        delta: i64,
        capacity: Option<u64>,
        location: Option<&BackendLocation>,
    ) -> Result<LoadUpdate, BlobError> {
        if !self.stats_managed(backend, bucket) {
            if let Some(location) = location {
                self.finalize_reservation(location).await?;
            }
            return Ok(LoadUpdate::Applied);
        }

        for attempt in 0..BUCKET_STATS_RETRIES {
            match self
                .try_adjust_load(backend, bucket, delta, capacity, location)
                .await?
            {
                LoadUpdate::Conflict => {}
                outcome => return Ok(outcome),
            }
            if attempt + 1 < BUCKET_STATS_RETRIES {
                tokio::time::sleep(conflict_backoff(attempt)).await;
            }
        }
        Ok(LoadUpdate::Conflict)
    }

    async fn try_adjust_load(
        &self,
        backend: &BackendRef,
        bucket: &str,
        delta: i64,
        capacity: Option<u64>,
        location: Option<&BackendLocation>,
    ) -> Result<LoadUpdate, BlobError> {
        let mut owner = self
            .storage
            .start_transaction(false)
            .await
            .map_err(|error| {
                BlobError::ReadError(format!("failed to start bucket stats transaction: {error}"))
            })?;
        let Some(txn_id) = owner.id() else {
            return Err(BlobError::ReadError(
                "bucket stats transaction owner missing transaction".to_string(),
            ));
        };

        let load = match self.bucket_load_txn(backend, bucket, txn_id).await {
            Ok(load) => load,
            Err(error) => {
                self.abort_stats_txn(&mut owner).await;
                return Err(error);
            }
        };
        if delta > 0
            && let Some(capacity) = capacity
            && load >= capacity
        {
            self.abort_stats_txn(&mut owner).await;
            return Ok(LoadUpdate::Full);
        }
        let adjusted = if delta >= 0 {
            load.saturating_add(delta.unsigned_abs())
        } else {
            load.saturating_sub(delta.unsigned_abs())
        };

        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: BUCKET_STATS_DB.to_string(),
                key: stats_key(backend, bucket).into(),
                value: adjusted.to_le_bytes().to_vec().into(),
                txn_id: Some(txn_id),
            }))
            .await;
        if !matches!(event, Event::Storage(StorageEvent::WriteResult { .. })) {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to write bucket stats".to_string(),
            ));
        }
        if !self
            .move_free_row(backend, bucket, (load, adjusted), txn_id)
            .await
        {
            self.abort_stats_txn(&mut owner).await;
            return Err(BlobError::ReadError(
                "failed to update the bucket free index".to_string(),
            ));
        }

        if let Some(location) = location {
            let value = match intent_value(location) {
                Ok(value) => value,
                Err(error) => {
                    self.abort_stats_txn(&mut owner).await;
                    return Err(error);
                }
            };
            let event = self
                .storage
                .send_effect(Effect::Storage(StorageEffect::Write {
                    key_space: BLOB_CLEANUP_KEYSPACE.to_string(),
                    key: intent_key(location),
                    value,
                    txn_id: Some(txn_id),
                }))
                .await;
            if !matches!(event, Event::Storage(StorageEvent::WriteResult { .. })) {
                self.abort_stats_txn(&mut owner).await;
                return Err(BlobError::ReadError(
                    "failed to persist bucket reservation".to_string(),
                ));
            }
        }

        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::CommitTransaction { txn_id }))
            .await;
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { txn_id: committed })
                if committed == txn_id =>
            {
                owner.finish();
                Ok(LoadUpdate::Applied)
            }
            Event::Storage(StorageEvent::Error { error }) => match error {
                StorageError::TransactionConflict => {
                    owner.finish();
                    Ok(LoadUpdate::Conflict)
                }
                StorageError::TransactionNotFound => {
                    owner.finish();
                    Err(BlobError::ReadError(
                        "bucket stats transaction was not found".to_string(),
                    ))
                }
                StorageError::QueueFull => Err(BlobError::ReadError(
                    "failed to commit bucket stats: queue full".to_string(),
                )),
                error => {
                    owner.unknown();
                    Err(BlobError::ReadError(format!(
                        "failed to commit bucket stats: {error}"
                    )))
                }
            },
            other => {
                owner.unknown();
                Err(BlobError::ReadError(format!(
                    "unexpected storage event while committing bucket stats: {other:?}"
                )))
            }
        }
    }

    async fn bucket_load_txn(
        &self,
        backend: &BackendRef,
        bucket: &str,
        txn_id: TxnId,
    ) -> Result<u64, BlobError> {
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::Read {
                key_space: BUCKET_STATS_DB.to_string(),
                key: stats_key(backend, bucket).into(),
                txn_id: Some(txn_id),
            }))
            .await;
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return Err(BlobError::ReadError(
                "failed to read bucket stats".to_string(),
            ));
        };
        match value {
            Some(value) => Ok(u64::from_le_bytes(
                value.as_ref().try_into().map_err(ConversionError::from)?,
            )),
            None => Ok(0),
        }
    }

    async fn abort_stats_txn(&self, owner: &mut TransactionOwner) {
        let Some(txn_id) = owner.id() else {
            return;
        };
        let event = self
            .storage
            .send_effect(Effect::Storage(StorageEffect::AbortTransaction { txn_id }))
            .await;
        match event {
            Event::Storage(StorageEvent::TransactionAborted { txn_id: aborted })
                if aborted == txn_id =>
            {
                owner.finish()
            }
            Event::Storage(StorageEvent::Error {
                error: StorageError::TransactionNotFound,
            }) => owner.finish(),
            other => {
                tracing::warn!(%txn_id, event = ?other, "failed to abort bucket stats transaction");
            }
        }
    }

    pub(super) fn operator_from_location(
        &self,
        location: &BackendLocation,
    ) -> Result<Operator, BlobError> {
        let bucket = if self.registry.splits(&location.backend) {
            split_location(location)?.0
        } else {
            location.storage_bucket.clone()
        };
        self.registry
            .operator_for(&location.backend, &location.root, &bucket, &self.egress)
    }

    /// The path of `location` below its operator's root. A group storage refuses keys its
    /// provider would refuse, before any byte is written.
    pub(super) fn object_path(&self, location: &BackendLocation) -> Result<String, BlobError> {
        object_key(location, self.registry.splits(&location.backend))
    }

    pub(super) fn operator_from_hidden(&self, key: &HiddenBlobKey) -> Result<Operator, BlobError> {
        self.registry
            .operator_for(&key.backend, &key.root, &key.storage_bucket, &self.egress)
    }

    pub(super) async fn hidden_bucket_after(
        &self,
        backend: &BackendRef,
        start_after: Option<&str>,
    ) -> Result<Option<String>, BlobError> {
        let config = self.registry.config_for(backend)?;
        if let Some(bucket) = config.service_config.get("bucket") {
            return Ok((start_after.is_none()).then(|| bucket.clone()));
        }
        let prefix = stats_prefix(backend, config.bucket_prefix.as_deref());
        let start = start_after.map(|bucket| IterStart::After(stats_key(backend, bucket).into()));
        let event = tokio::time::timeout(
            self.io_timeout(),
            self.storage
                .send_effect(Effect::Storage(StorageEffect::Iter {
                    key_space: BUCKET_STATS_DB.to_string(),
                    prefix: Some(prefix.clone().into()),
                    start,
                    limit: 1,
                    txn_id: None,
                })),
        )
        .await
        .map_err(|_| BlobError::ReadError("timed out reading hidden bucket stats".to_string()))?;
        let Event::Storage(StorageEvent::IterResult { values, .. }) = event else {
            return Err(BlobError::ReadError(
                "unexpected hidden bucket iteration event".to_string(),
            ));
        };
        // Slice past the backend discriminator only: the stored name keeps its
        // configured bucket prefix, and truncating it breaks backend listings.
        let name_offset = stats_prefix(backend, None).len();
        values
            .first()
            .map(|(key, _)| {
                String::from_utf8(key.as_ref().get(name_offset..).unwrap_or_default().to_vec())
                    .map_err(ConversionError::from)
            })
            .transpose()
            .map_err(BlobError::ConversionError)
    }
}

/// The free index row of a bucket at `load`; big-endian loads sort the emptiest bucket first.
fn free_key(backend: &BackendRef, load: u64, bucket: &str) -> Vec<u8> {
    let mut key = stats_prefix(backend, None);
    key.extend_from_slice(&load.to_be_bytes());
    key.extend_from_slice(bucket.as_bytes());
    key
}

/// Bucket stats are per backend: the key is the backend discriminator, a NUL
/// separator, then the bucket name.
fn stats_key(backend: &BackendRef, bucket: &str) -> Vec<u8> {
    let mut key = stats_prefix(backend, None);
    key.extend_from_slice(bucket.as_bytes());
    key
}

fn stats_prefix(backend: &BackendRef, bucket_prefix: Option<&str>) -> Vec<u8> {
    let mut prefix = backend.key_bytes();
    prefix.push(0);
    if let Some(bucket_prefix) = bucket_prefix {
        prefix.extend_from_slice(bucket_prefix.as_bytes());
    }
    prefix
}

/// The one bucket a group storage writes Aruna's layout to: its container or its default
/// bucket, never a linked bucket. Node backends answer `None` unless their config pins one.
fn fixed_bucket(config: &BackendConfig) -> Result<Option<String>, BlobError> {
    if let Some(bucket) = config.service_config.get("bucket") {
        return Ok(Some(bucket.clone()));
    }
    if !matches!(config.backend_type, Backend::Group(_)) {
        return Ok(None);
    }
    let default = config.service_config.get(DEFAULT_BUCKET);
    default.cloned().map(Some).ok_or_else(|| {
        BlobError::WriteError("this storage has no default bucket for Aruna's files".to_string())
    })
}

/// The path of `location` below its root; `split` drops the bucket in front.
pub(super) fn object_key(location: &BackendLocation, split: bool) -> Result<String, BlobError> {
    let path = location.get_storage_path()?;
    let path = match split {
        true => split_location(location)?.1,
        false => path,
    };
    if matches!(location.backend, BackendRef::Group(_)) {
        key_fits(&location.root, &path)?;
    }
    Ok(path)
}

/// Refuses a group storage key over the provider limit, counted with the root in front.
pub(super) fn key_fits(root: &str, path: &str) -> Result<(), BlobError> {
    let root = root.trim_matches('/');
    let length = path.len() + if root.is_empty() { 0 } else { root.len() + 1 };
    match length > MAX_KEY_LEN {
        true => Err(BlobError::KeyTooLong),
        false => Ok(()),
    }
}

/// The bucket and key of a location on a storage that names the bucket in each path: the
/// storage bucket of Aruna's layout, or the first folder of a tenant path.
pub(super) fn split_location(location: &BackendLocation) -> Result<(String, String), BlobError> {
    if !location.storage_bucket.is_empty() {
        return Ok((
            location.storage_bucket.clone(),
            location.backend_path.clone(),
        ));
    }
    split_bucket(&location.backend_path)
}

/// Splits `bucket/key`; both parts must be present.
pub(super) fn split_bucket(path: &str) -> Result<(String, String), BlobError> {
    match path.split_once('/') {
        Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => {
            Ok((bucket.to_string(), key.to_string()))
        }
        _ => Err(BlobError::UnknownBackend(format!(
            "the storage path `{path}` names no bucket"
        ))),
    }
}

fn location_bucket(template: &BackendLocation, bucket: &str) -> BackendLocation {
    let mut location = template.clone();
    location.storage_bucket = bucket.to_string();
    location
}

pub(super) fn generate_bucket_name(prefix: Option<&str>) -> String {
    let prefix = prefix.unwrap_or("aruna-");
    format!("{}{}", prefix, Ulid::generate().to_string().to_lowercase())
}

pub(super) fn build_backend_path(
    bucket: &str,
    key: &str,
    ulid: Ulid,
) -> Result<String, ConversionError> {
    let path = PathBuf::from(bucket).join(format!("{}_{}", key, ulid));
    ensure_confined_path(&path)?;
    let first = path.components().find_map(|component| match component {
        std::path::Component::Normal(part) => part.to_str(),
        _ => None,
    });
    if first == Some(HIDDEN_BLOB_PREFIX) || first == Some(MULTIPART_PART_PREFIX) {
        return Err(ConversionError::UnsafePath(
            "bucket collides with a reserved backend namespace".to_string(),
        ));
    }
    path.into_os_string()
        .into_string()
        .map_err(|_| ConversionError::OsStringError)
}

pub(super) fn build_hidden_path(
    namespace: Ulid,
    name: &str,
    ulid: Ulid,
) -> Result<String, ConversionError> {
    if name.is_empty() {
        return Err(ConversionError::UnsafePath(
            "hidden blob name must not be empty".to_string(),
        ));
    }
    let path = PathBuf::from(HIDDEN_BLOB_PREFIX)
        .join(namespace.to_string())
        .join(format!("{name}_{ulid}"));
    ensure_confined_path(&path)?;
    path.into_os_string()
        .into_string()
        .map_err(|_| ConversionError::OsStringError)
}

/// A part path; on a storage without a bucket the parts wait in a tenant bucket, so their
/// folder is named as Aruna's own.
pub(super) fn build_part_path(
    split: bool,
    upload_id: Ulid,
    part_number: u16,
    ulid: Ulid,
) -> String {
    let prefix = if split {
        PARTS_PREFIX
    } else {
        MULTIPART_PART_PREFIX
    };
    PathBuf::from(prefix)
        .join(upload_id.to_string())
        .join(format!("{:05}_{}", part_number, ulid))
        .into_os_string()
        .into_string()
        .expect("multipart part path must be valid utf-8")
}

pub(super) fn rebuild_backend_path(
    original_path: &str,
    ulid: Ulid,
) -> Result<String, ConversionError> {
    let original = PathBuf::from(original_path);
    let parent = original.parent().map(PathBuf::from).unwrap_or_default();
    let file_name = original
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(ConversionError::OsStringError)?;
    let base_name = file_name
        .rsplit_once('_')
        .map_or(file_name, |(base, _)| base);

    let path = parent.join(format!("{}_{}", base_name, ulid));
    ensure_confined_path(&path)?;
    path.into_os_string()
        .into_string()
        .map_err(|_| ConversionError::OsStringError)
}

#[cfg(test)]
mod split_tests {
    use super::{fixed_bucket, object_key, split_bucket};
    use aruna_core::errors::BlobError;
    use aruna_core::structs::storage::blob::{Backend, BackendConfig, BlobTimeoutConfig};
    use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
    use aruna_core::structs::storage::format::StoredFormat;
    use aruna_core::structs::storage::group_backend::GroupBackendKind;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn location(storage_bucket: &str, backend_path: &str) -> BackendLocation {
        BackendLocation {
            backend: BackendRef::Group(Ulid::nil()),
            storage_class: None,
            root: String::new(),
            storage_bucket: storage_bucket.to_string(),
            backend_path: backend_path.to_string(),
            ulid: Ulid::nil(),
            format: StoredFormat::default(),
            created_by: aruna_core::UserId::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 0,
            hashes: HashMap::new(),
        }
    }

    #[test]
    fn splits_storage_paths() {
        // Aruna's layout on a default bucket keeps no second bucket folder.
        let layout = location("lab-default", "seq/a.csv_01");
        assert_eq!(object_key(&layout, true).unwrap(), "seq/a.csv_01");
        assert_eq!(
            object_key(&layout, false).unwrap(),
            "lab-default/seq/a.csv_01"
        );
        // A tenant path names its bucket in the first folder.
        let tenant = location("", "lab-raw/2024/a.csv");
        assert_eq!(object_key(&tenant, true).unwrap(), "2024/a.csv");
        assert_eq!(object_key(&tenant, false).unwrap(), "lab-raw/2024/a.csv");
        assert!(object_key(&location("", "a.csv"), true).is_err());
        assert!(split_bucket("/a.csv").is_err());
    }

    #[test]
    fn layout_skips_parts() {
        // Parts may wait in a linked bucket; Aruna's own layout never goes there.
        let config = |public: &[(&str, &str)]| BackendConfig {
            backend_type: Backend::Group(GroupBackendKind::S3),
            root: String::new(),
            service_config: public
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            bucket_prefix: None,
            max_bucket_size: None,
            multipart_bucket: Some("lab-raw".to_string()),
            timeouts: BlobTimeoutConfig::default(),
        };
        assert!(fixed_bucket(&config(&[])).is_err());
        let chosen = fixed_bucket(&config(&[("default_bucket", "lab-default")])).unwrap();
        assert_eq!(chosen.as_deref(), Some("lab-default"));
    }

    #[test]
    fn refuses_long_keys() {
        let name = "a".repeat(1000);
        let mut fits = location("", &format!("lab-raw/{name}"));
        assert!(object_key(&fits, true).is_ok());
        fits.root = "r".repeat(30);
        assert_eq!(object_key(&fits, true), Err(BlobError::KeyTooLong));
        // Node backends keep their own limits.
        fits.backend = BackendRef::node_default();
        assert!(object_key(&fits, false).is_ok());
    }
}
