//! Checks a bucket's current versions for stored copies that cannot be served and records them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{BUCKET_PAGE, MIGRATION_CONTINUE, MIGRATION_PAGE};
use crate::blob::records::{blob_location_read, read_version_effect};
use crate::driver::DriverContext;
use crate::groups::backends::backend_key;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError, StorageError};
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BROKEN_VERSION_KEYSPACE, CHECK_QUEUE_KEYSPACE, STORAGE_BACKEND_KEYSPACE,
    STORAGE_CHECK_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, BlobHeadKey, BlobVersion, BlobVersionState, CurrentVersionPointer,
    VersionKey,
};
use aruna_core::structs::storage::broken::{BrokenReason, BrokenVersion, StorageCheck};
use aruna_core::structs::storage::group_backend::GroupStorage;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use std::time::Duration;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CheckState {
    Init,
    ReadVersion,
    ReadLocation,
    ReadStorage,
    Stat,
    Record,
    Finish,
}

/// What a check found for one version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckOutcome {
    Healthy,
    Broken(BrokenReason),
    /// Nothing stored to check: no version, no copy, or an unfinished copy.
    Skipped,
}

#[derive(Debug, Error, PartialEq)]
pub enum CheckVersionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error("version check did not finish")]
    NotFinished,
    #[error("unexpected event in state {state:?}: {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        received: Box<Event>,
    },
}

/// Checks one version's copy: its location row, its group storage and one existence probe.
/// A broken version is recorded with the version record it saw; any other answer clears it.
#[derive(Debug, PartialEq)]
pub struct CheckVersionOperation {
    version_key: VersionKey,
    now_ms: u64,
    state: CheckState,
    version: Option<BlobVersion>,
    location: Option<BackendLocation>,
    output: Option<Result<CheckOutcome, CheckVersionError>>,
}

impl CheckVersionOperation {
    pub fn new(version_key: VersionKey, now_ms: u64) -> Self {
        Self {
            version_key,
            now_ms,
            state: CheckState::Init,
            version: None,
            location: None,
            output: None,
        }
    }

    fn fail(&mut self, error: CheckVersionError) -> Effects {
        self.state = CheckState::Finish;
        self.output = Some(Err(error));
        smallvec![]
    }

    fn unexpected(&mut self, state: &'static str, received: Event) -> Effects {
        let received = Box::new(received);
        self.fail(CheckVersionError::InvalidStateEvent { state, received })
    }

    /// Writes or clears the broken row, then ends with `outcome`.
    fn record(&mut self, outcome: CheckOutcome) -> Effects {
        let key = match self.version_key.to_bytes() {
            Ok(key) => Key::from(key),
            Err(error) => return self.fail(error.into()),
        };
        let effect = match (outcome, self.version.clone()) {
            (CheckOutcome::Broken(reason), Some(version)) => {
                let row = BrokenVersion {
                    reason,
                    version,
                    found_at_ms: self.now_ms,
                };
                match row.to_bytes() {
                    Ok(value) => StorageEffect::Write {
                        key_space: BROKEN_VERSION_KEYSPACE.to_string(),
                        key,
                        value: value.into(),
                        txn_id: None,
                    },
                    Err(error) => return self.fail(error.into()),
                }
            }
            _ => StorageEffect::Delete {
                key_space: BROKEN_VERSION_KEYSPACE.to_string(),
                key,
                txn_id: None,
            },
        };
        self.output = Some(Ok(outcome));
        self.state = CheckState::Record;
        smallvec![Effect::Storage(effect)]
    }

    fn handle_version(&mut self, value: Option<&[u8]>) -> Effects {
        let version = match value.map(BlobVersion::from_bytes).transpose() {
            Ok(version) => version,
            Err(error) => return self.fail(error.into()),
        };
        let key = version.as_ref().and_then(|version| match &version.state {
            BlobVersionState::Materialized { .. } => version.location_key(),
            _ => None,
        });
        self.version = version;
        let Some(key) = key else {
            return self.record(CheckOutcome::Skipped);
        };
        self.state = CheckState::ReadLocation;
        smallvec![blob_location_read(&key, None)]
    }

    fn handle_location(&mut self, value: Option<&[u8]>) -> Effects {
        let location = match value.map(BackendLocation::from_bytes).transpose() {
            Ok(Some(location)) => location,
            Ok(None) => return self.record(CheckOutcome::Broken(BrokenReason::MissingRecord)),
            Err(error) => return self.fail(error.into()),
        };
        if location.staging || location.partial {
            return self.record(CheckOutcome::Skipped);
        }
        let storage = match &location.backend {
            BackendRef::Group(backend_id) => Some(backend_key(*backend_id)),
            BackendRef::Node(_) => None,
        };
        self.location = Some(location);
        let Some(key) = storage else {
            return self.stat();
        };
        self.state = CheckState::ReadStorage;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: STORAGE_BACKEND_KEYSPACE.to_string(),
            key,
            txn_id: None,
        })]
    }

    fn handle_storage(&mut self, value: Option<&[u8]>) -> Effects {
        match value.map(GroupStorage::from_bytes).transpose() {
            Ok(Some(storage)) if !storage.disabled => self.stat(),
            Ok(_) => self.record(CheckOutcome::Broken(BrokenReason::StorageRemoved)),
            Err(error) => self.fail(error.into()),
        }
    }

    fn stat(&mut self) -> Effects {
        let Some(location) = self.location.clone() else {
            return self.fail(CheckVersionError::NotFinished);
        };
        self.state = CheckState::Stat;
        smallvec![Effect::Blob(BlobEffect::StatCopy { location })]
    }
}

impl Operation for CheckVersionOperation {
    type Output = CheckOutcome;
    type Error = CheckVersionError;

    fn start(&mut self) -> Effects {
        match read_version_effect(&self.version_key, None) {
            Ok(effect) => {
                self.state = CheckState::ReadVersion;
                smallvec![effect]
            }
            Err(error) => self.fail(error.into()),
        }
    }

    fn step(&mut self, event: Event) -> Effects {
        if let Event::Storage(StorageEvent::Error { error }) = event {
            return self.fail(error.into());
        }
        match (self.state, event) {
            (CheckState::ReadVersion, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.handle_version(value.as_deref())
            }
            (CheckState::ReadLocation, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.handle_location(value.as_deref())
            }
            (CheckState::ReadStorage, Event::Storage(StorageEvent::ReadResult { value, .. })) => {
                self.handle_storage(value.as_deref())
            }
            (CheckState::Stat, Event::Blob(BlobEvent::CopyStat { exists })) => match exists {
                true => self.record(CheckOutcome::Healthy),
                false => self.record(CheckOutcome::Broken(BrokenReason::MissingBlob)),
            },
            (CheckState::Stat, Event::Blob(BlobEvent::Error(error))) => self.fail(error.into()),
            (
                CheckState::Record,
                Event::Storage(
                    StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. },
                ),
            ) => {
                self.state = CheckState::Finish;
                smallvec![]
            }
            (CheckState::Init, _) => self.start(),
            (_, event) => self.unexpected("Check", event),
        }
    }

    fn is_complete(&self) -> bool {
        self.state == CheckState::Finish
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.state {
            CheckState::Finish => self.output.unwrap_or(Err(CheckVersionError::NotFinished)),
            _ => Err(CheckVersionError::NotFinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

/// Advances the unfinished checks at the head of the queue by one page each. Returns when
/// the task must run again.
pub async fn process_checks(context: &DriverContext) -> Result<Option<Duration>, String> {
    let (rows, more) = crate::jobs::store::iter_prefix_page(
        &context.storage_handle,
        CHECK_QUEUE_KEYSPACE,
        None,
        None,
        BUCKET_PAGE,
        None,
    )
    .await?;
    let mut next = more.map(|_| MIGRATION_CONTINUE);
    for (key, _) in rows {
        let bucket = String::from_utf8(key.to_vec()).map_err(|error| error.to_string())?;
        let Some(record) = read_check(context, &bucket).await? else {
            continue;
        };
        if record.finished_at_ms.is_none() && check_page(context, &bucket, record).await? {
            next = Some(MIGRATION_CONTINUE);
        }
    }
    Ok(next)
}

pub async fn read_check(
    context: &DriverContext,
    bucket: &str,
) -> Result<Option<StorageCheck>, String> {
    let read = StorageEffect::Read {
        key_space: STORAGE_CHECK_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| StorageCheck::from_bytes(value.as_ref()))
            .transpose()
            .map_err(|error| error.to_string()),
        other => Err(format!("could not read check progress: {other:?}")),
    }
}

/// Checks one page of current versions and stores the progress. Returns whether more remain.
async fn check_page(
    context: &DriverContext,
    bucket: &str,
    mut record: StorageCheck,
) -> Result<bool, String> {
    let prefix = BlobHeadKey::bucket_prefix(bucket).map_err(|error| error.to_string())?;
    let (heads, next) = crate::jobs::store::iter_prefix_page(
        &context.storage_handle,
        BLOB_HEAD_KEYSPACE,
        Some(prefix.into()),
        record.cursor.clone().map(Into::into),
        MIGRATION_PAGE,
        None,
    )
    .await?;
    let now_ms = crate::effect_adapters::routing::now_ms();
    for (key, value) in &heads {
        record.cursor = Some(key.to_vec());
        let head = BlobHeadKey::from_bytes(key.as_ref()).map_err(|error| error.to_string())?;
        let pointer = CurrentVersionPointer::from_bytes(value.as_ref());
        let pointer = pointer.map_err(|error| error.to_string())?;
        let version_key = VersionKey::new(head.bucket, head.key, pointer.version_id);
        let operation = CheckVersionOperation::new(version_key, now_ms);
        match crate::driver::drive(operation, context).await {
            Ok(CheckOutcome::Broken(_)) => record.broken += 1,
            Ok(_) => {}
            Err(error) => {
                record.failed += 1;
                tracing::warn!(bucket, %error, "Failed to check a version");
            }
        }
        record.checked += 1;
    }
    if next.is_none() {
        record.finished_at_ms = Some(now_ms);
    }
    store_check(context, bucket, &record).await?;
    Ok(next.is_some())
}

/// Stores the progress unless a newer check replaced it; a finished check leaves the queue.
async fn store_check(
    context: &DriverContext,
    bucket: &str,
    record: &StorageCheck,
) -> Result<(), String> {
    let storage = &context.storage_handle;
    let txn_id = match storage
        .send_storage_effect(StorageEffect::StartTransaction { read: false })
        .await
    {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        other => return Err(format!("could not start a progress transaction: {other:?}")),
    };
    let key: Key = bucket.as_bytes().to_vec().into();
    let read = StorageEffect::Read {
        key_space: STORAGE_CHECK_KEYSPACE.to_string(),
        key: key.clone(),
        txn_id: Some(txn_id),
    };
    let current = match storage.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .map(|value| StorageCheck::from_bytes(value.as_ref()))
            .transpose()
            .map_err(|error| error.to_string()),
        other => Err(format!("could not read check progress: {other:?}")),
    };
    let same = matches!(&current, Ok(Some(stored)) if stored.started_at_ms == record.started_at_ms);
    let mut writes = Vec::new();
    if same {
        let value = record.to_bytes().map_err(|error| error.to_string())?;
        writes.push((
            STORAGE_CHECK_KEYSPACE.to_string(),
            key.clone(),
            value.into(),
        ));
    }
    let batch = StorageEffect::BatchWrite {
        writes,
        txn_id: Some(txn_id),
    };
    let written = match (current, storage.send_storage_effect(batch).await) {
        (Err(error), _) => Err(error),
        (_, Event::Storage(StorageEvent::BatchWriteResult { .. })) => Ok(()),
        (_, other) => Err(format!("could not write check progress: {other:?}")),
    };
    let finished = written.is_ok() && same && record.finished_at_ms.is_some();
    let left = match finished {
        true => {
            let delete = StorageEffect::Delete {
                key_space: CHECK_QUEUE_KEYSPACE.to_string(),
                key,
                txn_id: Some(txn_id),
            };
            match storage.send_storage_effect(delete).await {
                Event::Storage(StorageEvent::DeleteResult { .. }) => Ok(()),
                other => Err(format!("could not leave the check queue: {other:?}")),
            }
        }
        false => written,
    };
    if let Err(error) = left {
        storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
        return Err(error);
    }
    match storage
        .send_storage_effect(StorageEffect::CommitTransaction { txn_id })
        .await
    {
        Event::Storage(StorageEvent::TransactionCommitted { .. }) => Ok(()),
        other => Err(format!("check progress was not stored: {other:?}")),
    }
}

/// One page of a bucket's broken versions after `after`, and the key the next page starts after.
pub async fn list_broken(
    context: &DriverContext,
    bucket: &str,
    after: Option<Key>,
    limit: usize,
) -> Result<(Vec<(VersionKey, BrokenVersion)>, Option<Key>), String> {
    let prefix = VersionKey::bucket_prefix(bucket).map_err(|error| error.to_string())?;
    let (rows, next) = crate::jobs::store::iter_prefix_page(
        &context.storage_handle,
        BROKEN_VERSION_KEYSPACE,
        Some(prefix.into()),
        after,
        limit,
        None,
    )
    .await?;
    let mut broken = Vec::with_capacity(rows.len());
    for (key, value) in rows {
        let key = VersionKey::from_bytes(key.as_ref()).map_err(|error| error.to_string())?;
        let row = BrokenVersion::from_bytes(value.as_ref()).map_err(|error| error.to_string())?;
        broken.push((key, row));
    }
    Ok((broken, next))
}

/// What one cleanup request did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Cleaned {
    pub deleted: u64,
    /// Recorded versions that changed since the check, or whose delete failed; their rows stay.
    pub kept: u64,
    /// Recorded versions the caller may not delete; their rows stay.
    pub denied: u64,
    /// The key the next request starts after; `None` after the last page.
    pub next: Option<Key>,
    /// The scan budget ran out before one row the caller may delete was found.
    pub exhausted: bool,
}

/// Deletes up to `limit` recorded broken versions after `after`, authorized for `auth` per path,
/// each only while its record is still the recorded one. At most `scan` rows are read;
/// denied rows do not count toward `limit` and never name `next`.
pub async fn clean_broken(
    context: &DriverContext,
    template: &crate::s3::object::delete::DeleteObjectInput,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    after: Option<Key>,
    limit: usize,
    scan: usize,
) -> Result<Cleaned, String> {
    let (mut after, mut handled, mut examined) = (after, 0, 0);
    let mut last = None;
    let mut cleaned = Cleaned::default();
    loop {
        if examined == scan {
            cleaned.exhausted = handled == 0;
            cleaned.next = last;
            return Ok(cleaned);
        }
        let page = limit.min(scan - examined);
        let (rows, next) = list_broken(context, &template.bucket, after.take(), page).await?;
        for (key, row) in rows {
            if handled == limit {
                cleaned.next = last;
                return Ok(cleaned);
            }
            examined += 1;
            if clean_one(context, template, auth, &key, row, &mut cleaned).await? {
                handled += 1;
                last = Some(key.to_bytes().map_err(|error| error.to_string())?.into());
            }
        }
        match next {
            Some(next) => after = Some(next),
            None => return Ok(cleaned),
        }
    }
}

/// Cleans one recorded version; `false` when the caller may not delete it.
async fn clean_one(
    context: &DriverContext,
    template: &crate::s3::object::delete::DeleteObjectInput,
    auth: &aruna_core::structs::identity::auth::AuthContext,
    key: &VersionKey,
    row: BrokenVersion,
    cleaned: &mut Cleaned,
) -> Result<bool, String> {
    use crate::auth::request_authorization::{AuthorizeError, authorize};
    use crate::auth::request_policy::PolicyRequestExtras;
    use crate::s3::object::delete::{DeleteObjectError, DeleteObjectInput, DeleteObjectOperation};
    use aruna_core::structs::identity::auth::Permission;
    let path = aruna_core::structs::storage::blob::object_permission_path(
        template.realm_id,
        template.group_id,
        template.node_id,
        &key.bucket,
        &key.key,
    );
    let extras = PolicyRequestExtras::rest();
    match authorize(
        context,
        template.realm_id,
        auth,
        &path,
        &Permission::WRITE,
        extras,
    )
    .await
    {
        Ok(()) => {}
        Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
            cleaned.denied += 1;
            return Ok(false);
        }
        Err(error) => return Err(error.to_string()),
    }
    let input = DeleteObjectInput {
        bucket: key.bucket.clone(),
        key: key.key.clone(),
        version_id: Some(key.version_id),
        group_id: template.group_id,
        realm_id: template.realm_id,
        node_id: template.node_id,
        deleted_by: template.deleted_by,
    };
    let operation = DeleteObjectOperation::new(input)
        .with_restrictions(auth.path_restrictions.clone())
        .with_expected(row.version);
    // A version deleted meanwhile only leaves its row behind.
    let deleted = match crate::driver::drive(operation, context).await {
        Ok(_) => true,
        Err(DeleteObjectError::NoSuchVersion) => false,
        Err(error) => {
            if error != DeleteObjectError::VersionChanged {
                tracing::warn!(bucket = %key.bucket, %error, "Failed to delete a broken version");
            }
            cleaned.kept += 1;
            return Ok(true);
        }
    };
    let delete = StorageEffect::Delete {
        key_space: BROKEN_VERSION_KEYSPACE.to_string(),
        key: key.to_bytes().map_err(|error| error.to_string())?.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(delete).await {
        Event::Storage(StorageEvent::DeleteResult { .. }) => cleaned.deleted += u64::from(deleted),
        other => return Err(format!("could not clear a broken version: {other:?}")),
    }
    Ok(true)
}

/// Whether `key` names the current version of its object.
pub async fn is_current(context: &DriverContext, key: &VersionKey) -> Result<bool, String> {
    let head = BlobHeadKey::new(key.bucket.clone(), key.key.clone());
    let read = StorageEffect::Read {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key: head.to_bytes().map_err(|error| error.to_string())?.into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => match value {
            Some(value) => CurrentVersionPointer::from_bytes(value.as_ref())
                .map(|pointer| pointer.version_id == key.version_id)
                .map_err(|error| error.to_string()),
            None => Ok(false),
        },
        other => Err(format!("could not read a head: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::storage::format::{EncodingClass, StoredFormat};
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn read(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: value.map(Into::into),
        })
    }

    fn version(backend: BackendRef) -> BlobVersion {
        let created = SystemTime::UNIX_EPOCH;
        BlobVersion::materialized(
            [4; 32],
            backend,
            EncodingClass::Raw,
            created,
            Default::default(),
            None,
        )
    }

    fn location(backend: BackendRef) -> BackendLocation {
        BackendLocation {
            backend,
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "store".to_string(),
            backend_path: "b/k".to_string(),
            ulid: Ulid::from_bytes([3; 16]),
            format: StoredFormat::default(),
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 4,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), vec![4u8; 32])]),
        }
    }

    /// Drives a check of a version on `backend` up to the answer after its location read.
    fn located(backend: BackendRef) -> (CheckVersionOperation, Effects) {
        let key = VersionKey::new("b", "k", Ulid::from_bytes([2; 16]));
        let mut operation = CheckVersionOperation::new(key, 9);
        operation.start();
        operation.step(read(Some(version(backend.clone()).to_bytes().unwrap())));
        let effects = operation.step(read(Some(location(backend).to_bytes().unwrap())));
        (operation, effects)
    }

    /// Answers the broken row write or delete and returns the outcome and the effect.
    fn recorded(mut operation: CheckVersionOperation, effects: Effects) -> (CheckOutcome, bool) {
        let [Effect::Storage(effect)] = effects.as_slice() else {
            panic!("expected the broken row effect, got {effects:?}")
        };
        let written = matches!(effect, StorageEffect::Write { .. });
        let answer = match written {
            true => StorageEvent::WriteResult {
                key: Vec::new().into(),
            },
            false => StorageEvent::DeleteResult {
                key: Vec::new().into(),
            },
        };
        operation.step(Event::Storage(answer));
        (operation.finalize().unwrap(), written)
    }

    #[test]
    fn missing_blob_recorded() {
        let (mut operation, effects) = located(BackendRef::node_default());
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::StatCopy { .. })]
        ));
        let effects = operation.step(Event::Blob(BlobEvent::CopyStat { exists: false }));
        let outcome = recorded(operation, effects);
        assert_eq!(
            outcome,
            (CheckOutcome::Broken(BrokenReason::MissingBlob), true)
        );
    }

    #[test]
    fn healthy_copy_cleared() {
        let (mut operation, _) = located(BackendRef::node_default());
        let effects = operation.step(Event::Blob(BlobEvent::CopyStat { exists: true }));
        assert_eq!(recorded(operation, effects), (CheckOutcome::Healthy, false));
    }

    #[test]
    fn removed_storage_recorded() {
        // A removed group storage is broken without asking the storage.
        let (mut operation, effects) = located(BackendRef::Group(Ulid::from_bytes([5; 16])));
        let [Effect::Storage(StorageEffect::Read { key_space, .. })] = effects.as_slice() else {
            panic!("expected the storage record read, got {effects:?}")
        };
        assert_eq!(key_space, STORAGE_BACKEND_KEYSPACE);
        let effects = operation.step(read(None));
        let outcome = recorded(operation, effects);
        assert_eq!(
            outcome,
            (CheckOutcome::Broken(BrokenReason::StorageRemoved), true)
        );
    }

    #[test]
    fn missing_record_recorded() {
        let key = VersionKey::new("b", "k", Ulid::from_bytes([2; 16]));
        let mut operation = CheckVersionOperation::new(key, 9);
        operation.start();
        operation.step(read(Some(
            version(BackendRef::node_default()).to_bytes().unwrap(),
        )));
        let effects = operation.step(read(None));
        let outcome = recorded(operation, effects);
        assert_eq!(
            outcome,
            (CheckOutcome::Broken(BrokenReason::MissingRecord), true)
        );
    }
}
