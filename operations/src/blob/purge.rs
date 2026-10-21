//! Finds the linked-storage files of purged versions, keeps them planned while the purge runs
//! and hands them to the release of renamed files once the versions are gone.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::blob::records::iter_hash_page;
use crate::blob::unify::{rules_on, second_copies};
use crate::driver::DriverContext;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BACKEND_VERSION_KEYSPACE, BLOB_CLEANUP_KEYSPACE, BLOB_LOCATIONS_KEYSPACE,
    BLOB_VERSIONS_KEYSPACE, IMPORTED_OBJECT_KEYSPACE, PURGE_FILES_KEYSPACE,
};
use aruna_core::structs::storage::backend_import::{
    BackendImport, ImportedObject, object_key, version_key,
};
use aruna_core::structs::storage::blob::{
    BackendLocation, BlobCleanupWork, BlobVersion, HashIndex, VersionKey,
};
use aruna_core::structs::storage::storage_purge::StoragePurgeScope;
use aruna_core::types::{GroupId, Key, Value};
use std::collections::HashMap;
use ulid::Ulid;

const ALIAS_PAGE: usize = 256;

/// One linked-storage file of a purged version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageFile {
    pub key: String,
    pub version_id: Ulid,
    pub location: BackendLocation,
    /// The backend version the file was written as; only that version is deleted.
    pub version: Option<String>,
    /// The rule that records the file and the version its row names.
    pub owner: Option<(Ulid, Ulid)>,
    /// Why the file would stay after the purge, as far as a preview can tell.
    pub kept: Option<String>,
    /// The key index row of a second copy no version reads, which goes with the versions.
    pub index: Option<Key>,
}

impl StorageFile {
    /// The release of this file once purge `job_id` ended, which deletes it only once nothing
    /// reads it.
    fn work(&self, job_id: Ulid) -> Option<BlobCleanupWork> {
        let (import_id, version_id) = self.owner.unwrap_or((Ulid::nil(), self.version_id));
        Some(BlobCleanupWork::ReleasePlanned {
            job_id,
            location: self.location.clone(),
            import_id,
            version_id,
            version: self.version.clone()?,
        })
    }
}

async fn batch(
    context: &DriverContext,
    reads: Vec<(String, Key)>,
) -> Result<Vec<Option<Value>>, String> {
    if reads.is_empty() {
        return Ok(Vec::new());
    }
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::BatchReadResult { values }) => {
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }
        other => Err(format!("could not read purged versions: {other:?}")),
    }
}

/// The linked-storage files the `versions` of `bucket` read, one entry per file. With `pins`,
/// the purge scope, each file says why it would stay.
pub async fn storage_files(
    context: &DriverContext,
    (bucket, group_id): (&str, GroupId),
    versions: &[(String, Ulid)],
    pins: Option<&StoragePurgeScope>,
) -> Result<Vec<StorageFile>, String> {
    let mut reads = Vec::with_capacity(versions.len());
    for (key, id) in versions {
        let version = VersionKey::new(bucket, key, *id);
        let version = version.to_bytes().map_err(|error| error.to_string())?;
        reads.push((BLOB_VERSIONS_KEYSPACE.to_string(), version.into()));
    }
    let mut named = Vec::new();
    for ((key, id), value) in versions.iter().zip(batch(context, reads).await?) {
        let Some(value) = value else {
            continue;
        };
        let version = BlobVersion::from_bytes(&value).map_err(|error| error.to_string())?;
        if let Some(location) = version.location_key() {
            named.push((key.clone(), *id, location));
        }
    }
    let reads = named
        .iter()
        .map(|(.., location)| {
            (
                BLOB_LOCATIONS_KEYSPACE.to_string(),
                location.to_bytes().into(),
            )
        })
        .collect();
    let mut files: Vec<StorageFile> = Vec::new();
    for ((key, id, _), value) in named.into_iter().zip(batch(context, reads).await?) {
        let Some(value) = value else {
            continue;
        };
        let location = BackendLocation::from_bytes(&value).map_err(|error| error.to_string())?;
        let linked = location.is_external() && !location.staging && !location.partial;
        if !linked || files.iter().any(|file| file.location.ulid == location.ulid) {
            continue;
        }
        files.push(StorageFile {
            key,
            version_id: id,
            location,
            version: None,
            owner: None,
            kept: None,
            index: None,
        });
    }
    // A second copy an import registered for a purged version goes with that version.
    let routing = crate::blob::migration::move_routing(context, bucket).await?;
    let mut keys: std::collections::BTreeMap<&str, Vec<Ulid>> = Default::default();
    for (key, id) in versions {
        keys.entry(key.as_str()).or_default().push(*id);
    }
    for (key, ids) in keys {
        let twins = second_copies(context, routing.as_ref(), (bucket, key), &ids).await?;
        for (location, index, version_id) in twins {
            if files.iter().any(|file| file.location.ulid == location.ulid) {
                continue;
            }
            files.push(StorageFile {
                key: key.to_string(),
                version_id,
                location,
                version: None,
                owner: None,
                kept: None,
                index: Some(index),
            });
        }
    }
    let mut rules: HashMap<Vec<u8>, Vec<BackendImport>> = HashMap::new();
    for file in &files {
        let backend = file.location.backend.key_bytes();
        if let std::collections::hash_map::Entry::Vacant(entry) = rules.entry(backend) {
            entry.insert(rules_on(context, group_id, &file.location.backend).await?);
        }
    }
    for file in &mut files {
        let rules = &rules[&file.location.backend.key_bytes()];
        let mut reads = vec![(
            BACKEND_VERSION_KEYSPACE.to_string(),
            version_key(file.location.ulid),
        )];
        for rule in rules {
            let row = object_key(rule.import_id, &file.location.backend_path);
            reads.push((IMPORTED_OBJECT_KEYSPACE.to_string(), row));
        }
        let mut values = batch(context, reads).await?.into_iter();
        let version = values.next().flatten();
        let version = version.map(|value| String::from_utf8(value.to_vec()));
        let version = version.transpose().map_err(|error| error.to_string())?;
        // "null" names whatever the path holds later, never the file's bytes.
        file.version = version.filter(|version| version != "null");
        let mut owners = Vec::new();
        for (rule, row) in rules.iter().zip(values) {
            let row = row.as_deref().map(ImportedObject::from_bytes).transpose();
            if let Some(row) = row.map_err(|error| error.to_string())? {
                owners.push((rule.import_id, row));
            }
        }
        let own = owners.iter().position(|(_, row)| {
            row.key.as_deref() == Some(file.key.as_str()) || row.version_id == file.version_id
        });
        file.owner = own.map(|at| (owners[at].0, owners[at].1.version_id));
        let Some(scope) = pins else {
            continue;
        };
        file.kept = if file.version.is_none() {
            Some("the storage keeps no exact version of the file".to_string())
        } else if owners.len() > usize::from(own.is_some()) {
            Some("another link records the file".to_string())
        } else if read_elsewhere(context, &file.location, scope).await? {
            Some("a version outside the deleted scope reads the file".to_string())
        } else {
            None
        };
    }
    Ok(files)
}

/// Whether a version of a key outside `scope` reads the file of `location`.
async fn read_elsewhere(
    context: &DriverContext,
    location: &BackendLocation,
    scope: &StoragePurgeScope,
) -> Result<bool, String> {
    let wanted = location.location_key().map_err(|error| error.to_string())?;
    let mut start = None;
    loop {
        let effect = iter_hash_page(&wanted.blake3_hash, start.take(), None, ALIAS_PAGE);
        let Effect::Storage(effect) = effect.map_err(|error| error.to_string())? else {
            return Err("not a storage effect".to_string());
        };
        let (aliases, next) = match context.storage_handle.send_storage_effect(effect).await {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            other => return Err(format!("could not read hash aliases: {other:?}")),
        };
        for (key, _) in aliases {
            let alias = HashIndex::from_bytes(&key).map_err(|error| error.to_string())?;
            if scope.matches_key(&alias.bucket, &alias.key) {
                continue;
            }
            let version = VersionKey::new(&alias.bucket, &alias.key, alias.version_id);
            let version = version.to_bytes().map_err(|error| error.to_string())?;
            let read = vec![(BLOB_VERSIONS_KEYSPACE.to_string(), version.into())];
            let Some(Some(value)) = batch(context, read).await?.pop() else {
                continue;
            };
            let version = BlobVersion::from_bytes(&value).map_err(|error| error.to_string())?;
            if version.location_key() == Some(wanted.clone()) {
                return Ok(true);
            }
        }
        match next {
            Some(next) => start = Some(next),
            None => return Ok(false),
        }
    }
}

/// Versions a deletion preview counts at most before its totals become lower bounds.
const TOTAL_VERSIONS: u64 = 1_000_000;
const LIST_PAGE: usize = 1000;

/// What a purge of a scope removes: keys and versions in Aruna, files in linked storage.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScopeTotals {
    pub keys: u64,
    pub versions: u64,
    /// Bytes of the versions' content in Aruna, each version counted.
    pub bytes: u64,
    pub storage_files: u64,
    pub storage_bytes: u64,
    /// `false` when the count stopped at its limit; the numbers are then lower bounds.
    pub complete: bool,
}

/// Counts what a purge of `scope` removes, from the version listing alone.
pub async fn scope_totals(
    context: &DriverContext,
    scope: &StoragePurgeScope,
    readable: &(dyn Fn(&str) -> bool + Sync),
) -> Result<ScopeTotals, String> {
    use crate::s3::object::versions::{ListVersionsInput, ListVersionsItem, ListVersionsOperation};
    let mut totals = ScopeTotals::default();
    let mut seen = std::collections::HashSet::new();
    let (mut key_marker, mut version_marker) = (None, None);
    let mut last_key = None::<String>;
    let mut scanned = 0u64;
    let routing = crate::blob::migration::move_routing(context, scope.bucket()).await?;
    let routing = routing.as_ref();
    // The versions of the key passed over last, whose second copies count once it ends.
    let mut ids = Vec::new();
    loop {
        let input = ListVersionsInput {
            bucket: scope.bucket().to_string(),
            prefix: scope.list_prefix().map(str::to_string),
            delimiter: None,
            key_marker: key_marker.take(),
            version_id_marker: version_marker.take(),
            max_keys: Some(LIST_PAGE),
        };
        let page = crate::driver::drive(ListVersionsOperation::new(input), context)
            .await
            .map_err(|error| error.to_string())?;
        for item in &page.items {
            let (key, id, location) = match item {
                ListVersionsItem::Version {
                    key,
                    version_id,
                    location,
                    ..
                } => (key, *version_id, location.as_ref()),
                ListVersionsItem::DeleteMarker {
                    key, version_id, ..
                } => (key, *version_id, None),
            };
            // The bound counts every version passed over, also the hidden ones.
            scanned += 1;
            if !scope.matches_key(scope.bucket(), key) || !readable(key) {
                continue;
            }
            totals.versions += 1;
            if last_key.as_deref() != Some(key.as_str()) {
                let ended = (last_key.as_deref(), std::mem::take(&mut ids));
                count_copies(
                    context,
                    (scope.bucket(), routing),
                    ended,
                    (&mut totals, &mut seen),
                )
                .await?;
                totals.keys += 1;
                last_key = Some(key.clone());
            }
            ids.push(id);
            let Some(location) = location else {
                continue;
            };
            totals.bytes += location.blob_size;
            let linked = location.is_external() && !location.staging && !location.partial;
            if linked && seen.insert(location.ulid) {
                totals.storage_files += 1;
                totals.storage_bytes += location.blob_size;
            }
        }
        if scanned >= TOTAL_VERSIONS && page.is_truncated {
            let ended = (last_key.as_deref(), ids);
            count_copies(
                context,
                (scope.bucket(), routing),
                ended,
                (&mut totals, &mut seen),
            )
            .await?;
            return Ok(totals);
        }
        match (page.is_truncated, page.next_key_marker) {
            (true, Some(next)) => {
                (key_marker, version_marker) = (Some(next), page.next_version_marker)
            }
            _ => {
                let ended = (last_key.as_deref(), ids);
                count_copies(
                    context,
                    (scope.bucket(), routing),
                    ended,
                    (&mut totals, &mut seen),
                )
                .await?;
                totals.complete = true;
                return Ok(totals);
            }
        }
    }
}

/// Counts the second copies an import registered for the versions `ids` of `key`.
async fn count_copies(
    context: &DriverContext,
    (bucket, routing): (
        &str,
        Option<&aruna_core::structs::storage::routing::RoutingSnapshot>,
    ),
    (key, ids): (Option<&str>, Vec<Ulid>),
    (totals, seen): (&mut ScopeTotals, &mut std::collections::HashSet<Ulid>),
) -> Result<(), String> {
    let Some(key) = key.filter(|_| !ids.is_empty()) else {
        return Ok(());
    };
    for (file, ..) in second_copies(context, routing, (bucket, key), &ids).await? {
        if seen.insert(file.ulid) {
            totals.storage_files += 1;
            totals.storage_bytes += file.blob_size;
        }
    }
    Ok(())
}

fn plan_key(job_id: Ulid, location: Ulid) -> Key {
    [&job_id.to_bytes()[..], b"f", &location.to_bytes()[..]]
        .concat()
        .into()
}

fn pause_key(job_id: Ulid) -> Key {
    [&job_id.to_bytes()[..], b"p"].concat().into()
}

/// Plans the release of `files` in the cleanup queue before their versions go; the queue
/// releases them once the purge has ended, whichever way. A file without a backend version
/// is not planned.
pub async fn plan_files(
    context: &DriverContext,
    job_id: Ulid,
    files: &[StorageFile],
) -> Result<(), String> {
    let mut writes = Vec::new();
    for file in files {
        let Some(work) = file.work(job_id) else {
            continue;
        };
        let value = work.to_bytes().map_err(|error| error.to_string())?;
        let key = plan_key(job_id, file.location.ulid);
        writes.push((BLOB_CLEANUP_KEYSPACE.to_string(), key, value.into()));
    }
    if writes.is_empty() {
        return Ok(());
    }
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(write).await {
        Event::Storage(StorageEvent::BatchWriteResult { .. }) => Ok(()),
        other => Err(format!("could not plan the file releases: {other:?}")),
    }
}

/// Pauses or resumes the purge `job_id` between its batches.
pub async fn set_paused(context: &DriverContext, job_id: Ulid, paused: bool) -> Result<(), String> {
    let key = pause_key(job_id);
    let effect = match paused {
        true => StorageEffect::Write {
            key_space: PURGE_FILES_KEYSPACE.to_string(),
            key,
            value: Vec::new().into(),
            txn_id: None,
        },
        false => StorageEffect::Delete {
            key_space: PURGE_FILES_KEYSPACE.to_string(),
            key,
            txn_id: None,
        },
    };
    match context.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. }) => {
            Ok(())
        }
        other => Err(format!("could not change the purge pause: {other:?}")),
    }
}

/// Whether the purge `job_id` is paused.
pub async fn is_paused(context: &DriverContext, job_id: Ulid) -> Result<bool, String> {
    let read = vec![(PURGE_FILES_KEYSPACE.to_string(), pause_key(job_id))];
    Ok(batch(context, read).await?.pop().flatten().is_some())
}

#[cfg(test)]
#[path = "purge_tests.rs"]
mod tests;
