//! Releases the file a rename replaced in a linked folder, once nothing else names it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::reclaim::{ReclaimBlobOperation, ReclaimVerdict};
use crate::blob::records::{iter_index_effect, owners_scan_effect};
use crate::driver::{DriverContext, drive};
use crate::groups::backends::parse_read;
use crate::jobs::store::iter_prefix_page;
use aruna_core::effects::{BlobEffect, Effect, IterStart, StorageEffect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_IMPORT_KEYSPACE, BACKEND_VERSION_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, IMPORTED_OBJECT_KEYSPACE,
};
use aruna_core::structs::storage::backend_import::{
    BackendImport, ImportedObject, object_key, version_key,
};
use aruna_core::structs::storage::blob::{
    ArchiveKey, BackendLocation, BackendRef, BlobLocationKey, BlobVersion, HashIndex, VersionKey,
};
use aruna_core::structs::storage::cleanup::ReclaimCandidateKey;
use aruna_core::types::Key;
use tracing::warn;
use ulid::Ulid;

const RULE_PAGE: usize = 128;

/// Deletes the old file of a rename at its recorded backend `version`, then the import row
/// that named it as `version_id`. Any other reference to the copy keeps both; `false` retries.
pub(crate) async fn release_renamed(
    context: &DriverContext,
    location: BackendLocation,
    import_id: Ulid,
    version_id: Ulid,
    version: String,
) -> bool {
    let key = object_key(import_id, &location.backend_path);
    let observed = match read_value(context, IMPORTED_OBJECT_KEYSPACE, key.clone()).await {
        Ok(observed) => observed,
        Err(error) => {
            warn!(%error, "Renamed file row could not be read");
            return false;
        }
    };
    let row = observed
        .as_deref()
        .map(ImportedObject::from_bytes)
        .transpose();
    match row {
        // Another write took the path, so the file there is not the renamed one.
        Ok(Some(row)) if row.version_id != version_id => return true,
        Ok(_) => {}
        Err(error) => {
            warn!(%error, "Renamed file row could not be decoded");
            return false;
        }
    }
    match referenced(context, &location, (import_id, &version)).await {
        Ok(true) => return true,
        Ok(false) => {}
        Err(error) => {
            warn!(%error, "References of a renamed file could not be read");
            return false;
        }
    }
    // A stored location of the file, also one waiting for reclaim, may still be reused; one
    // of another backend version at the path keeps the path's row.
    let shared = match pinned(context, &location, &version).await {
        Ok(Some(shared)) => shared,
        Ok(None) => return false,
        Err(error) => {
            warn!(%error, "Locations of a renamed file could not be read");
            return false;
        }
    };
    // The rename's own old location goes first, whatever the storage's cleanup strategy, and
    // the rename and that retirement are on disk before the file is deleted.
    if !retire_source(context, &location).await || !synced(context).await {
        return false;
    }
    let Some(blob_handle) = context.blob_handle.as_ref() else {
        return false;
    };
    let effect = BlobEffect::DeletePlain { location, version };
    match blob_handle.send_blob_effect(effect).await {
        Event::Blob(BlobEvent::DeleteFinished) => {}
        // A storage that cannot delete this exact version keeps the file and its row.
        Event::Blob(BlobEvent::Error(BlobError::CleanupUnsupported)) => return true,
        event => {
            warn!(?event, "Renamed file could not be deleted");
            return false;
        }
    }
    match observed {
        Some(observed) if !shared => drop_row(context, key, observed).await,
        _ => true,
    }
}

/// The backend version a location row's file was written as, if one is recorded.
async fn provider(
    context: &DriverContext,
    location: &BackendLocation,
) -> Result<Option<String>, String> {
    let value = read_value(
        context,
        BACKEND_VERSION_KEYSPACE,
        version_key(location.ulid),
    )
    .await?;
    let value = value.map(String::from_utf8).transpose();
    value.map_err(|error| error.to_string())
}

/// Whether `other` names other bytes at the same path: another known backend version.
async fn other_version(
    context: &DriverContext,
    other: &BackendLocation,
    version: &str,
) -> Result<bool, String> {
    Ok(provider(context, other)
        .await?
        .is_some_and(|other| other != version))
}

/// Whether a version, an archive owner or another rule's row still names the copy.
async fn referenced(
    context: &DriverContext,
    location: &BackendLocation,
    (import_id, provider): (Ulid, &str),
) -> Result<bool, String> {
    let wanted = location.location_key().map_err(|error| error.to_string())?;
    let mut start = None;
    loop {
        let effect = iter_index_effect(&wanted.blake3_hash, start.take(), None)
            .map_err(|error| error.to_string())?;
        let effect = storage(effect)?;
        let (aliases, next) = match send(context, effect).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            other => return Err(format!("could not read hash aliases: {other:?}")),
        };
        for (key, _) in aliases {
            let alias = HashIndex::from_bytes(key.as_ref()).map_err(|error| error.to_string())?;
            let version = VersionKey::new(&alias.bucket, &alias.key, alias.version_id);
            let version = version.to_bytes().map_err(|error| error.to_string())?;
            // A missing indexed version proves nothing, so the copy stays.
            let Some(value) = read_value(context, BLOB_VERSIONS_KEYSPACE, version.into()).await?
            else {
                return Ok(true);
            };
            let version = BlobVersion::from_bytes(&value).map_err(|error| error.to_string())?;
            let Some(other) = version.location_key() else {
                continue;
            };
            if other == wanted
                || (other.backend == wanted.backend
                    && same_file(context, &other, (location, provider)).await?)
            {
                return Ok(true);
            }
        }
        match next {
            Some(next) => start = Some(next),
            None => break,
        }
    }
    let owners = storage(owners_scan_effect(&ArchiveKey::of(location), None))?;
    match send(context, owners).await {
        Event::Storage(StorageEvent::IterResult { values, .. }) if values.is_empty() => {}
        Event::Storage(StorageEvent::IterResult { .. }) => return Ok(true),
        other => return Err(format!("could not read archive owners: {other:?}")),
    }
    other_rows(context, location, import_id).await
}

/// Whether the storage confirmed that every committed transaction is on disk.
async fn synced(context: &DriverContext) -> bool {
    match send(context, StorageEffect::SyncAll).await {
        Event::Storage(StorageEvent::SyncAllFinished) => true,
        event => {
            warn!(
                ?event,
                "Storage did not sync before a renamed file is deleted"
            );
            false
        }
    }
}

/// Drops the old location row of the rename in a reclaim transaction that proves no version
/// reads it, and debits its bytes; `false` while something still reads it.
async fn retire_source(context: &DriverContext, location: &BackendLocation) -> bool {
    let Ok(key) = location.location_key() else {
        return false;
    };
    let candidate = ReclaimCandidateKey::new(key.backend, key.encoding, key.blake3_hash);
    let now = std::time::SystemTime::now();
    let operation = ReclaimBlobOperation::new(candidate, now, now).forced();
    match drive(operation, context).await {
        Ok(ReclaimVerdict::Freed { .. } | ReclaimVerdict::Dropped) => true,
        Ok(ReclaimVerdict::Pinned | ReclaimVerdict::NotDue) => false,
        Err(error) => {
            warn!(%error, "Old location of a renamed file could not be retired");
            false
        }
    }
}

/// `None` while another location row of these bytes on the same storage reads the same file:
/// reclaim drops such rows without touching the file, so the release waits for them. Else
/// whether a row reads another backend version at the same path, which keeps the path in use.
async fn pinned(
    context: &DriverContext,
    location: &BackendLocation,
    version: &str,
) -> Result<Option<bool>, String> {
    let mut shared = false;
    let wanted = location.location_key().map_err(|error| error.to_string())?;
    let mut start = None;
    loop {
        let effect = StorageEffect::Iter {
            key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
            prefix: Some(wanted.blake3_hash.to_vec().into()),
            start: start.take().map(IterStart::After),
            limit: RULE_PAGE,
            txn_id: None,
        };
        let (rows, next) = match send(context, effect).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            other => return Err(format!("could not read locations: {other:?}")),
        };
        for (key, value) in rows {
            let row = BackendLocation::from_bytes(&value).map_err(|error| error.to_string())?;
            let own = key.as_ref() == wanted.to_bytes().as_slice();
            if !own && row.backend == location.backend && same_path(&row, location) {
                match other_version(context, &row, version).await? {
                    true => shared = true,
                    false => return Ok(None),
                }
            }
        }
        match next {
            Some(next) => start = Some(next),
            None => return Ok(Some(shared)),
        }
    }
}

fn same_path(left: &BackendLocation, right: &BackendLocation) -> bool {
    (&left.root, &left.storage_bucket, &left.backend_path)
        == (&right.root, &right.storage_bucket, &right.backend_path)
}

/// Whether another copy of the same bytes, such as a literal-name import, reads the same file:
/// the same path and no other known backend version.
async fn same_file(
    context: &DriverContext,
    key: &BlobLocationKey,
    (location, version): (&BackendLocation, &str),
) -> Result<bool, String> {
    let Some(value) = read_value(context, BLOB_LOCATIONS_KEYSPACE, key.to_bytes().into()).await?
    else {
        return Ok(false);
    };
    let other = BackendLocation::from_bytes(&value).map_err(|error| error.to_string())?;
    Ok(same_path(&other, location) && !other_version(context, &other, version).await?)
}

/// Whether another rule on the same storage, such as one sharing the folder, has a row there.
async fn other_rows(
    context: &DriverContext,
    location: &BackendLocation,
    import_id: Ulid,
) -> Result<bool, String> {
    let mut after = None;
    loop {
        let storage = &context.storage_handle;
        let page = iter_prefix_page(
            storage,
            BACKEND_IMPORT_KEYSPACE,
            None,
            after,
            RULE_PAGE,
            None,
        );
        let (rules, next) = page.await?;
        for (_, value) in rules {
            let rule = BackendImport::from_bytes(&value).map_err(|error| error.to_string())?;
            let on = |backend: Ulid| BackendRef::Group(backend) == location.backend;
            if rule.import_id == import_id || !(on(rule.backend_id) || on(rule.write_backend())) {
                continue;
            }
            let key = object_key(rule.import_id, &location.backend_path);
            if read_value(context, IMPORTED_OBJECT_KEYSPACE, key)
                .await?
                .is_some()
            {
                return Ok(true);
            }
        }
        match next {
            Some(next) => after = Some(next),
            None => return Ok(false),
        }
    }
}

/// Deletes the row only if it is still the one read before the file went; an import may have
/// committed a replacement meanwhile.
async fn drop_row(context: &DriverContext, key: Key, observed: Vec<u8>) -> bool {
    let txn_id = match send(context, StorageEffect::StartTransaction { read: false }).await {
        Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
        event => {
            warn!(?event, "Renamed file row could not be locked");
            return false;
        }
    };
    let read = StorageEffect::Read {
        key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
        key: key.clone(),
        txn_id: Some(txn_id),
    };
    let current = match send(context, read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
        event => {
            warn!(?event, "Renamed file row could not be read again");
            send(context, StorageEffect::AbortTransaction { txn_id }).await;
            return false;
        }
    };
    if current.as_deref() != Some(observed.as_slice()) {
        send(context, StorageEffect::AbortTransaction { txn_id }).await;
        return true;
    }
    let delete = StorageEffect::Delete {
        key_space: IMPORTED_OBJECT_KEYSPACE.to_string(),
        key,
        txn_id: Some(txn_id),
    };
    if !matches!(
        send(context, delete).await,
        Event::Storage(StorageEvent::DeleteResult { .. })
    ) {
        send(context, StorageEffect::AbortTransaction { txn_id }).await;
        return false;
    }
    let commit = StorageEffect::CommitTransaction { txn_id };
    matches!(
        send(context, commit).await,
        Event::Storage(StorageEvent::TransactionCommitted { .. })
    )
}

async fn send(context: &DriverContext, effect: StorageEffect) -> Event {
    context.storage_handle.send_storage_effect(effect).await
}

/// The storage effect a records helper built.
fn storage(effect: Effect) -> Result<StorageEffect, String> {
    match effect {
        Effect::Storage(effect) => Ok(effect),
        other => Err(format!("not a storage effect: {other:?}")),
    }
}

async fn read_value(
    context: &DriverContext,
    key_space: &str,
    key: Key,
) -> Result<Option<Vec<u8>>, String> {
    let read = StorageEffect::Read {
        key_space: key_space.to_string(),
        key,
        txn_id: None,
    };
    let event = send(context, read).await;
    parse_read(event, |bytes| Ok(bytes.to_vec())).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::keyspaces::PATHS_INDEX_KEYSPACE;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::format::{EncodingClass, StoredFormat, StoredLayout};
    use aruna_storage::storage::FjallStorage;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use tempfile::{TempDir, tempdir};

    const IMPORT: Ulid = Ulid::from_parts(1, 1);
    const VERSION: Ulid = Ulid::from_parts(2, 2);

    fn setup() -> (TempDir, DriverContext) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        (dir, context)
    }

    fn old() -> BackendLocation {
        let ulid = Ulid::from_parts(3, 3);
        BackendLocation {
            backend: BackendRef::Group(Ulid::from_parts(4, 4)),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: format!("raw/{VERSION}_a.csv"),
            ulid,
            format: StoredFormat {
                layout: StoredLayout::Plain(ulid),
                ..StoredFormat::default()
            },
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 3,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), vec![7u8; 32])]),
        }
    }

    async fn put(context: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
        let write = StorageEffect::Write {
            key_space: key_space.to_string(),
            key,
            value: value.into(),
            txn_id: None,
        };
        let event = send(context, write).await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn row(context: &DriverContext, import_id: Ulid) -> Option<Vec<u8>> {
        let key = object_key(import_id, &old().backend_path);
        read_value(context, IMPORTED_OBJECT_KEYSPACE, key)
            .await
            .unwrap()
    }

    async fn renamed_row(context: &DriverContext, import_id: Ulid) {
        let row = ImportedObject::written(VERSION, "in/a.csv".to_string(), 3, None);
        let key = object_key(import_id, &old().backend_path);
        put(
            context,
            IMPORTED_OBJECT_KEYSPACE,
            key,
            row.to_bytes().unwrap(),
        )
        .await;
    }

    async fn release(context: &DriverContext) -> bool {
        release_renamed(context, old(), IMPORT, VERSION, "v1".to_string()).await
    }

    /// A version of the same bytes on the old file's storage, stored with `encoding`.
    async fn alias_version(context: &DriverContext, encoding: EncodingClass) {
        let successor = VersionKey::new("b", "in/a.csv", Ulid::from_parts(5, 5));
        let location = old();
        let version = BlobVersion::materialized(
            [7u8; 32],
            location.backend.clone(),
            encoding,
            SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
        );
        let key = successor.to_bytes().unwrap().into();
        put(
            context,
            BLOB_VERSIONS_KEYSPACE,
            key,
            version.to_bytes().unwrap(),
        )
        .await;
        let node = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let alias = HashIndex::new(
            [7u8; 32],
            successor.version_id,
            RealmId::from_bytes([1u8; 32]),
            Ulid::from_parts(6, 6),
            node,
            "b",
            "in/a.csv",
        );
        let alias = alias.to_bytes().unwrap().into();
        put(context, PATHS_INDEX_KEYSPACE, alias, Vec::new()).await;
    }

    #[tokio::test]
    async fn keeps_shared_copy() {
        // A successor version that still names the old copy keeps the file and its row.
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        let encoding = old().format.encoding();
        assert!(matches!(encoding, EncodingClass::Plain { .. }));
        alias_version(&context, encoding).await;
        assert!(release(&context).await);
        assert!(row(&context, IMPORT).await.is_some());
    }

    #[tokio::test]
    async fn keeps_literal_alias() {
        // A literal-name import reads the same file through its own raw location.
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        alias_version(&context, EncodingClass::Raw).await;
        let mut raw = old();
        raw.format = StoredFormat::default();
        let key = raw.location_key().unwrap().to_bytes().into();
        put(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            key,
            raw.to_bytes().unwrap(),
        )
        .await;
        assert!(release(&context).await);
        assert!(row(&context, IMPORT).await.is_some());

        // A raw copy elsewhere on the storage does not keep the file.
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        alias_version(&context, EncodingClass::Raw).await;
        raw.backend_path = "raw/a.csv".to_string();
        let key = raw.location_key().unwrap().to_bytes().into();
        put(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            key,
            raw.to_bytes().unwrap(),
        )
        .await;
        assert!(!release(&context).await);
    }

    #[tokio::test]
    async fn keeps_shared_file() {
        // Another rule on the same storage, as one sharing the folder, still lists the file.
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        let other = Ulid::from_parts(8, 8);
        let rule = BackendImport {
            import_id: other,
            group_id: Ulid::from_parts(6, 6),
            backend_id: Ulid::from_parts(4, 4),
            backend_prefix: "raw/".to_string(),
            bucket: "c".to_string(),
            key_prefix: String::new(),
            schedule: None,
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            last_job: None,
            last_started_ms: None,
            write_backend_id: None,
            write_prefix: None,
            write_pattern: "{dir}{name}".to_string(),
        };
        put(
            &context,
            BACKEND_IMPORT_KEYSPACE,
            rule.key(),
            rule.to_bytes().unwrap(),
        )
        .await;
        renamed_row(&context, other).await;
        assert!(release(&context).await);
        assert!(row(&context, IMPORT).await.is_some());

        // Without any reference the delete waits for the blob storage.
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        assert!(!release(&context).await);
        assert!(row(&context, IMPORT).await.is_some());
    }

    #[tokio::test]
    async fn waits_for_locations() {
        // A stored location of the file, referenced or not, keeps the release waiting.
        let (_dir, context) = setup();
        assert_eq!(pinned(&context, &old(), "v1").await.unwrap(), Some(false));
        // The rename's own old location is retired by the release, so it does not pin.
        let own = old().location_key().unwrap().to_bytes().into();
        put(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            own,
            old().to_bytes().unwrap(),
        )
        .await;
        assert_eq!(pinned(&context, &old(), "v1").await.unwrap(), Some(false));
        let mut raw = old();
        raw.format = StoredFormat::default();
        raw.backend_path = "raw/elsewhere.csv".to_string();
        let key = raw.location_key().unwrap().to_bytes().into();
        put(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            key,
            raw.to_bytes().unwrap(),
        )
        .await;
        assert_eq!(pinned(&context, &old(), "v1").await.unwrap(), Some(false));
        raw.backend_path = old().backend_path;
        let key = raw.location_key().unwrap().to_bytes().into();
        put(
            &context,
            BLOB_LOCATIONS_KEYSPACE,
            key,
            raw.to_bytes().unwrap(),
        )
        .await;
        assert_eq!(pinned(&context, &old(), "v1").await.unwrap(), None);
        renamed_row(&context, IMPORT).await;
        assert!(!release(&context).await);
        assert!(row(&context, IMPORT).await.is_some());
        // A row of another backend version at the path reads other bytes: no pin, but the
        // path stays in use.
        let other = version_key(raw.ulid).to_vec();
        let value = b"v0".to_vec().into();
        put(&context, BACKEND_VERSION_KEYSPACE, other.into(), value).await;
        assert_eq!(pinned(&context, &old(), "v1").await.unwrap(), Some(true));
    }

    #[tokio::test]
    async fn syncs_before_delete() {
        // The node storage answers the sync the release waits for before it deletes the file.
        let (_dir, context) = setup();
        assert!(synced(&context).await);
    }

    #[tokio::test]
    async fn drops_unchanged_row() {
        let (_dir, context) = setup();
        renamed_row(&context, IMPORT).await;
        let key = object_key(IMPORT, &old().backend_path);
        // An import replaced the row after it was read, so the new row stays.
        assert!(drop_row(&context, key.clone(), b"older".to_vec()).await);
        assert!(row(&context, IMPORT).await.is_some());
        let observed = row(&context, IMPORT).await.unwrap();
        assert!(drop_row(&context, key, observed).await);
        assert!(row(&context, IMPORT).await.is_none());
    }
}
