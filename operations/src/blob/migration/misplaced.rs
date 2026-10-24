//! Counts the versions of a bucket that a storage move would move now, and the linked files
//! it would rename.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{encrypting, move_routing, place, read_earlier, renames, source_key, stays};
use crate::blob::records::blob_location_read;
use crate::driver::DriverContext;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BROKEN_VERSION_KEYSPACE,
    IMPORTED_OBJECT_KEYSPACE, STORAGE_BACKEND_KEYSPACE,
};
use aruna_core::structs::storage::backend_import::{
    BackendImport, ImportedObject, copy_provable, object_key, object_prefix,
};
use aruna_core::structs::storage::blob::{BackendLocation, BlobVersion, VersionKey};
use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::types::Key;

/// Versions under the prefix one count reads at most, so a request stays bounded.
const COUNT_LIMIT: usize = 10_000;
/// Version rows of the bucket one count passes over at most, under the prefix or not.
const SCAN_LIMIT: usize = 200_000;
const COUNT_PAGE: usize = 256;
/// Renames one count lists by name; `renamed` counts all of them.
const RENAME_LIST: usize = 100;

/// Versions under a prefix that are not where the rules place a new write of their key.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Misplaced {
    pub count: u64,
    /// `false` when the count stopped at its limit and more versions exist.
    pub complete: bool,
    /// Linked files of an earlier pattern among them, which a move gives their current name.
    pub renamed: u64,
    /// The first of them, as the old and the new path.
    pub renames: Vec<(String, String)>,
    /// Renames of keys the caller may not use; they are neither counted nor listed.
    pub withheld: u64,
}

/// Counts what a move of `prefix` would move, with the move's own skips: an encrypted bucket
/// moves nothing; linked-folder files, unfinished copies and recorded broken versions stay.
/// With `renaming`, linked files of earlier patterns whose key it allows count as renames.
pub async fn count_misplaced(
    context: &DriverContext,
    bucket: &str,
    prefix: &str,
    renaming: Option<&(dyn Fn(&str) -> bool + Sync)>,
) -> Result<Misplaced, String> {
    let mut misplaced = Misplaced {
        complete: true,
        ..Misplaced::default()
    };
    if encrypting(context, &bucket.as_bytes().to_vec().into()).await? {
        return Ok(misplaced);
    }
    let Some(routing) = move_routing(context, bucket).await? else {
        return Ok(misplaced);
    };
    // Only a move asked to rename gives linked files their current name.
    let earlier = match renaming {
        Some(_) => read_earlier(context, &routing).await?,
        None => super::Earlier::new(),
    };
    let scan = VersionKey::bucket_prefix(bucket).map_err(|error| error.to_string())?;
    let (mut after, mut seen, mut scanned) = (None::<Key>, 0, 0);
    loop {
        let (versions, next) = crate::jobs::store::iter_prefix_page(
            &context.storage_handle,
            BLOB_VERSIONS_KEYSPACE,
            Some(scan.clone().into()),
            after.take(),
            COUNT_PAGE,
            None,
        )
        .await?;
        for (key, value) in versions {
            if seen >= COUNT_LIMIT || scanned >= SCAN_LIMIT {
                misplaced.complete = false;
                return Ok(misplaced);
            }
            scanned += 1;
            let version_key = VersionKey::from_bytes(key.as_ref()).map_err(|e| e.to_string())?;
            if !version_key.key.starts_with(prefix) {
                continue;
            }
            seen += 1;
            let version = BlobVersion::from_bytes(value.as_ref()).map_err(|e| e.to_string())?;
            // A key the rules cannot place now fails its move; it is not counted.
            let Ok((resolved, plain)) = place(&routing, &version_key) else {
                continue;
            };
            let target = (resolved.backend.clone(), resolved.compression.into());
            let wanted = |_: &_| target.clone();
            let Some(source) = source_key(&version, wanted, plain.is_some()) else {
                continue;
            };
            if read(context, BROKEN_VERSION_KEYSPACE, key.clone())
                .await?
                .is_some()
            {
                continue;
            }
            let Some(location) = read_location(context, &source).await? else {
                continue;
            };
            let renamed = plain
                .as_ref()
                .filter(|plain| renames(&routing, &earlier, plain, &location, &version_key));
            if let Some(plain) = renamed {
                if !renaming.is_some_and(|allow| allow(&version_key.key)) {
                    misplaced.withheld += 1;
                    continue;
                }
                misplaced.renamed += 1;
                if misplaced.renames.len() < RENAME_LIST {
                    let paths = (location.backend_path.clone(), plain.path.clone());
                    misplaced.renames.push(paths);
                }
                misplaced.count += 1;
                continue;
            }
            if stays(&location, true) {
                continue;
            }
            // A copy in place with its linked file recorded for this version is done.
            if let Some(plain) =
                plain.filter(|_| (source.backend.clone(), source.encoding) == target)
            {
                let row = object_key(plain.import_id, &plain.path);
                let row = read(context, IMPORTED_OBJECT_KEYSPACE, row).await?;
                let row = row.map(|row| ImportedObject::from_bytes(&row)).transpose();
                let row = row.map_err(|error| error.to_string())?;
                if row.is_some_and(|row| row.version_id == version_key.version_id) {
                    continue;
                }
            }
            misplaced.count += 1;
        }
        match next {
            Some(next) if seen < COUNT_LIMIT && scanned < SCAN_LIMIT => after = Some(next),
            Some(_) => {
                misplaced.complete = false;
                return Ok(misplaced);
            }
            None => return Ok(misplaced),
        }
    }
}

async fn read(
    context: &DriverContext,
    key_space: &str,
    key: Key,
) -> Result<Option<Vec<u8>>, String> {
    let read = aruna_core::effects::StorageEffect::Read {
        key_space: key_space.to_string(),
        key,
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.map(|v| v.to_vec())),
        other => Err(format!("could not read {key_space}: {other:?}")),
    }
}

/// The copy a version names, if it exists.
async fn read_location(
    context: &DriverContext,
    key: &aruna_core::structs::storage::blob::BlobLocationKey,
) -> Result<Option<BackendLocation>, String> {
    let aruna_core::effects::Effect::Storage(read) = blob_location_read(key, None) else {
        return Err("unexpected location read".to_string());
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => match value {
            Some(value) => BackendLocation::from_bytes(value.as_ref())
                .map(Some)
                .map_err(|error| error.to_string()),
            None => Ok(None),
        },
        other => Err(format!("could not read a location: {other:?}")),
    }
}

/// Entries one rename preview page shows by default and at most.
pub const DEFAULT_PAGE: usize = 100;
pub const MAX_PAGE: usize = 1000;
/// Index rows one storage read returns, so other requests go on between the reads.
const INDEX_PAGE: usize = 1024;

/// A check of a key against the caller's access.
pub type KeyCheck<'a> = dyn Fn(&str) -> bool + Sync + 'a;

/// What a rename of a rule's files would do, as far as the caller may read their keys.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RenamePlan {
    /// Files an earlier pattern named for their version, and their bytes.
    pub files: u64,
    pub bytes: u64,
    /// Of them, files whose copy passes through the node unless the provider keeps a version.
    pub streamed_files: u64,
    pub streamed_bytes: u64,
    /// Of them, files whose new name another file of the rule already holds.
    pub taken_files: u64,
    /// Of them, files the caller may not write; they keep their name.
    pub withheld_files: u64,
    /// The rule's other files, which keep their name.
    pub unchanged_files: u64,
    pub unchanged_bytes: u64,
    /// Always `true`: the plan covers every file of the rule.
    pub complete: bool,
}

/// The plan of a rename and one page of the files it renames.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RenamePage {
    pub plan: RenamePlan,
    /// The old and the new path, and whether the caller may write the key.
    pub entries: Vec<(String, String, bool)>,
    /// The index row of the last listed file, after which the next page starts.
    pub next: Option<Vec<u8>>,
}

/// Plans a rename of `rule`'s files from the rule's own index in one pass, without asking
/// the provider, and lists up to `limit` of them in path order after the row `after`, each
/// checked against its version. Keys `readable` refuses are left out.
pub async fn rename_page(
    context: &DriverContext,
    rule: &BackendImport,
    (readable, writable): (&KeyCheck<'_>, &KeyCheck<'_>),
    (filter, after, limit): (Option<&str>, Option<Vec<u8>>, usize),
) -> Result<RenamePage, String> {
    let mut page = RenamePage::default();
    page.plan.complete = true;
    if encrypting(context, &rule.bucket.as_bytes().to_vec().into()).await? {
        return Ok(page);
    }
    let Some(routing) = move_routing(context, &rule.bucket).await? else {
        return Ok(page);
    };
    // The rule as the move reads it; a removed rule renames nothing.
    let Some(rule) = routing
        .imports
        .iter()
        .find(|r| r.import_id == rule.import_id)
    else {
        return Ok(page);
    };
    let earlier = read_earlier(context, &routing).await?;
    let patterns = earlier.get(&rule.import_id).map_or(&[][..], Vec::as_slice);
    // Only S3 copies inside the provider; other storages pass every file through the node.
    let read = StorageEffect::Read {
        key_space: STORAGE_BACKEND_KEYSPACE.to_string(),
        key: crate::groups::backends::backend_key(rule.backend_id),
        txn_id: None,
    };
    let event = context.storage_handle.send_storage_effect(read).await;
    let storage = crate::groups::backends::parse_read(event, GroupStorage::from_bytes);
    let storage = storage.map_err(|error| error.to_string())?;
    let native = storage.is_some_and(|storage| storage.kind == GroupBackendKind::S3);
    let shown = |key: &str| {
        key.starts_with(&rule.key_prefix)
            && filter.is_none_or(|filter| key.contains(filter))
            && readable(key)
    };
    let prefix = object_prefix(rule.import_id);
    let mut start = None::<Key>;
    loop {
        let (rows, next) = crate::jobs::store::iter_prefix_page(
            &context.storage_handle,
            IMPORTED_OBJECT_KEYSPACE,
            Some(prefix.clone()),
            start.take(),
            INDEX_PAGE,
            None,
        )
        .await?;
        let mut candidates = Vec::new();
        for (row_key, value) in &rows {
            let (key, path, row) = row_parts(rule, (row_key, value))?;
            let Some(key) = key.filter(|key| shown(key)) else {
                continue;
            };
            // A mapped key names its file by the storage side of the mapping.
            let named = routing.storage_key(rule, &key);
            match rule.rename_target(patterns, &path, &named, row.version_id) {
                Some(to) => candidates.push((row_key.clone(), key, path, row, to)),
                None => {
                    page.plan.unchanged_files += 1;
                    page.plan.unchanged_bytes += row.size;
                }
            }
        }
        // Only a file its version still reads is work; any other record keeps its name.
        let found = confirmed(context, (&routing, &earlier), &rule.bucket, &candidates).await?;
        let mut renames = Vec::new();
        for ((row_key, key, path, row, to), found) in candidates.into_iter().zip(found) {
            let plan = &mut page.plan;
            let Some(location) = found else {
                plan.unchanged_files += 1;
                plan.unchanged_bytes += row.size;
                continue;
            };
            plan.files += 1;
            plan.bytes += row.size;
            // Like the copy: on S3 a recorded ETag or a one-part MD5 keeps it in the provider.
            if !native || (row.etag.is_none() && !copy_provable(&location)) {
                plan.streamed_files += 1;
                plan.streamed_bytes += row.size;
            }
            plan.withheld_files += u64::from(!writable(&key));
            let listed = after
                .as_deref()
                .is_none_or(|after| row_key.as_ref() > after);
            if listed && page.entries.len() < limit {
                page.entries.push((path, to.clone(), writable(&key)));
                page.next = Some(row_key.to_vec());
            }
            renames.push((key, row, to));
        }
        page.plan.taken_files += taken(context, rule, &renames).await?;
        match next {
            Some(next) => start = Some(next),
            None => break,
        }
    }
    // A cursor only when more files may follow the last listed one.
    if page.entries.len() < limit {
        page.next = None;
    }
    Ok(page)
}

/// The key an index row keeps its file at, the file's path and the record.
fn row_parts(
    rule: &BackendImport,
    (row_key, value): (&[u8], &[u8]),
) -> Result<(Option<String>, String, ImportedObject), String> {
    let path = row_key
        .get(object_prefix(rule.import_id).len()..)
        .unwrap_or_default();
    let path = String::from_utf8(path.to_vec()).map_err(|error| error.to_string())?;
    let row = ImportedObject::from_bytes(value).map_err(|error| error.to_string())?;
    // An older row without a key keeps the rule's own mapping.
    let key = row.key.clone().or_else(|| rule.aruna_key(&path));
    Ok((key, path, row))
}

/// The location of each candidate whose version still reads its file there and whose file a
/// rename gives the candidate's new name; versions and locations are read in one batch each.
async fn confirmed(
    context: &DriverContext,
    (routing, earlier): (&RoutingSnapshot, &super::Earlier),
    bucket: &str,
    candidates: &[(Key, String, String, ImportedObject, String)],
) -> Result<Vec<Option<BackendLocation>>, String> {
    let keys: Vec<VersionKey> = (candidates.iter())
        .map(|(_, key, _, row, _)| VersionKey::new(bucket, key, row.version_id))
        .collect();
    let reads = keys.iter().map(|key| {
        let key = key.to_bytes().map_err(|error| error.to_string())?;
        Ok((BLOB_VERSIONS_KEYSPACE.to_string(), key.into()))
    });
    let versions = batch(context, reads.collect::<Result<_, String>>()?).await?;
    // The copy each version reads, where a move would take it.
    let mut places = Vec::new();
    for (index, (key, value)) in keys.iter().zip(versions).enumerate() {
        let Some(value) = value else {
            continue;
        };
        let version = BlobVersion::from_bytes(&value).map_err(|error| error.to_string())?;
        let Ok((resolved, Some(plain))) = place(routing, key) else {
            continue;
        };
        let target = (resolved.backend.clone(), resolved.compression.into());
        if let Some(source) = source_key(&version, |_: &_| target.clone(), true) {
            places.push((index, plain, source));
        }
    }
    let reads = (places.iter())
        .map(|(_, _, source)| {
            (
                BLOB_LOCATIONS_KEYSPACE.to_string(),
                source.to_bytes().into(),
            )
        })
        .collect();
    let locations = batch(context, reads).await?;
    let mut found = vec![None; candidates.len()];
    for ((index, plain, _), value) in places.into_iter().zip(locations) {
        let Some(value) = value else {
            continue;
        };
        let location = BackendLocation::from_bytes(&value).map_err(|error| error.to_string())?;
        let (_, _, path, _, to) = &candidates[index];
        let renamed = location.backend_path == *path
            && plain.path == *to
            && renames(routing, earlier, &plain, &location, &keys[index]);
        found[index] = renamed.then_some(location);
    }
    Ok(found)
}

/// The values of `reads` in one storage call, in their order.
async fn batch(
    context: &DriverContext,
    reads: Vec<(String, Key)>,
) -> Result<Vec<Option<aruna_core::types::Value>>, String> {
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
        other => Err(format!("could not read the plan's records: {other:?}")),
    }
}

/// Renames whose new path a record of the rule keeps for another key or version, read in
/// one batch.
async fn taken(
    context: &DriverContext,
    rule: &BackendImport,
    renames: &[(String, ImportedObject, String)],
) -> Result<u64, String> {
    let reads = (renames.iter())
        .map(|(_, _, to)| {
            let row = object_key(rule.import_id, to);
            (IMPORTED_OBJECT_KEYSPACE.to_string(), row)
        })
        .collect();
    let mut taken = 0;
    for ((key, row, to), held) in renames.iter().zip(batch(context, reads).await?) {
        let Some(held) = held else {
            continue;
        };
        let held = ImportedObject::from_bytes(&held).map_err(|error| error.to_string())?;
        let holder = held.key.clone().or_else(|| rule.aruna_key(to));
        taken +=
            u64::from(holder.as_deref() != Some(key.as_str()) || held.version_id != row.version_id);
    }
    Ok(taken)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::bucket::rename::tests::{BUCKET, group, put, rule, setup};
    use aruna_core::keyspaces::{
        BACKEND_INDEX_KEYSPACE, IMPORT_PATTERN_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::checksum::{HASH_BLAKE3, HASH_MD5};
    use aruna_core::structs::storage::backend_import::{COPY_PART, write_patterns};
    use aruna_core::structs::storage::blob::{BackendRef, BucketInfo};
    use aruna_core::structs::storage::format::{
        Compression, EncodingClass, StoredFormat, StoredLayout,
    };
    use aruna_core::structs::storage::group_backend::{GroupBackendKind, GroupStorage};
    use aruna_core::structs::storage::routing::{RoutingTarget, StorageRoutingRule};
    use std::time::SystemTime;
    use ulid::Ulid;

    /// A bucket that writes to the rule's storage of `kind`, which the group holds, and the
    /// rule's earlier pattern `{dir}{id}_{name}`.
    async fn linked_bucket(context: &DriverContext, kind: GroupBackendKind) {
        let backend = rule().backend_id;
        let target = RoutingTarget::Backend(BackendRef::Group(backend));
        let info = BucketInfo {
            group_id: group(),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: vec![StorageRoutingRule {
                key_prefix: String::new(),
                exact: false,
                target,
            }],
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let name = BUCKET.as_bytes().to_vec();
        put(context, S3_BUCKET_KEYSPACE, name, info.to_bytes().unwrap()).await;
        let storage = GroupStorage {
            backend_id: backend,
            group_id: group(),
            name: "lab".to_string(),
            kind,
            public_config: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            updated_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            disabled: false,
            cleanup: Default::default(),
        };
        let index = crate::groups::backends::index_key(group(), backend).to_vec();
        put(
            context,
            BACKEND_INDEX_KEYSPACE,
            index,
            storage.to_bytes().unwrap(),
        )
        .await;
        let record = crate::groups::backends::backend_key(backend).to_vec();
        let value = storage.to_bytes().unwrap();
        put(context, STORAGE_BACKEND_KEYSPACE, record, value).await;
        let earlier = write_patterns(&["{dir}{id}_{name}".to_string()]).unwrap();
        let patterns = object_prefix(rule().import_id).to_vec();
        put(context, IMPORT_PATTERN_KEYSPACE, patterns, earlier).await;
    }

    /// Version `id` of `key` reads the linked file at `path`, which the rule's index records.
    async fn linked_file(context: &DriverContext, (key, id): (&str, Ulid), path: &str, size: u64) {
        let backend = BackendRef::Group(rule().backend_id);
        let hash = [size as u8; 32];
        let location = BackendLocation {
            backend: backend.clone(),
            storage_class: None,
            root: String::new(),
            storage_bucket: String::new(),
            backend_path: path.to_string(),
            ulid: id,
            format: StoredFormat {
                layout: StoredLayout::Plain(id),
                ..StoredFormat::default()
            },
            created_by: Default::default(),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: size,
            hashes: [
                (HASH_BLAKE3.to_string(), hash.to_vec()),
                (HASH_MD5.to_string(), hash[..16].to_vec()),
            ]
            .into(),
        };
        let row = location.location_key().unwrap().to_bytes();
        put(
            context,
            BLOB_LOCATIONS_KEYSPACE,
            row,
            location.to_bytes().unwrap(),
        )
        .await;
        let encoding = EncodingClass::Plain { location: id };
        let at = SystemTime::UNIX_EPOCH;
        let version =
            BlobVersion::materialized(hash, backend, encoding, at, Default::default(), None);
        let version_key = VersionKey::new(BUCKET, key, id).to_bytes().unwrap();
        put(
            context,
            BLOB_VERSIONS_KEYSPACE,
            version_key,
            version.to_bytes().unwrap(),
        )
        .await;
        recorded(context, (key, id), path, size).await;
    }

    /// The rule's index records the file at `path` for version `id` of `key`.
    async fn recorded(context: &DriverContext, (key, id): (&str, Ulid), path: &str, size: u64) {
        let recorded = ImportedObject::written(id, key.to_string(), size, None);
        let index = object_key(rule().import_id, path).to_vec();
        put(
            context,
            IMPORTED_OBJECT_KEYSPACE,
            index,
            recorded.to_bytes().unwrap(),
        )
        .await;
    }

    #[tokio::test]
    async fn plans_indexed_renames() {
        // The plan reads the rule's index and counts only files their version still reads.
        let (_directory, context) = setup().await;
        linked_bucket(&context, GroupBackendKind::S3).await;
        let id = |at: u64| Ulid::from_parts(at, 1);
        let files = [
            ("a.txt", id(11), format!("raw/{}_a.txt", id(11)), 3),
            ("b.txt", id(12), format!("raw/{}_b.txt", id(12)), 5),
            (
                "secret/c.txt",
                id(13),
                format!("raw/secret/{}_c.txt", id(13)),
                7,
            ),
            ("d.txt", id(14), format!("raw/d.txt~{}", id(14)), 9),
            ("e.txt", id(15), "raw/e.txt".to_string(), 11),
        ];
        for (key, id, path, size) in &files {
            linked_file(&context, (key, *id), path, *size).await;
        }
        // A name whose id is not its version's names no version of it.
        recorded(
            &context,
            ("f.txt", id(17)),
            &format!("raw/{}_f.txt", id(16)),
            13,
        )
        .await;
        // Another file already holds the new name of `b.txt`.
        recorded(
            &context,
            ("g.txt", id(18)),
            &format!("raw/b.txt~{}", id(12)),
            1,
        )
        .await;
        let large = COPY_PART + 1;
        let path = format!("raw/{}_h.bin", id(19));
        linked_file(&context, ("h.bin", id(19)), &path, large).await;
        // A record whose version is gone is no work; its file keeps its name.
        let stale = format!("raw/{}_i.txt", id(20));
        recorded(&context, ("i.txt", id(20)), &stale, 17).await;
        let readable = |key: &str| !key.starts_with("secret/");
        let writable = |key: &str| key != "b.txt";
        let checks = (&readable as &KeyCheck<'_>, &writable as &KeyCheck<'_>);
        let renamed = |key: &str, at: u64| {
            let from = format!("raw/{}_{key}", id(at));
            (from, format!("raw/{key}~{}", id(at)))
        };

        let page = rename_page(&context, &rule(), checks, (None, None, 1))
            .await
            .unwrap();
        let plan = RenamePlan {
            files: 3,
            bytes: 8 + large,
            streamed_files: 1,
            streamed_bytes: large,
            taken_files: 1,
            withheld_files: 1,
            unchanged_files: 5,
            unchanged_bytes: 51,
            complete: true,
        };
        assert_eq!(page.plan, plan);
        let (from, to) = renamed("a.txt", 11);
        assert_eq!(page.entries, vec![(from, to, true)]);
        let rest = rename_page(&context, &rule(), checks, (None, page.next, 1)).await;
        let rest = rest.unwrap();
        let (from, to) = renamed("b.txt", 12);
        assert_eq!(rest.entries, vec![(from, to, false)]);
        let large = rename_page(&context, &rule(), checks, (None, rest.next, 1)).await;
        let large = large.unwrap();
        assert_eq!(
            large.entries,
            vec![(path, format!("raw/h.bin~{}", id(19)), true)]
        );
        // The stale record and the hidden key are not listed; no cursor follows.
        let last = rename_page(&context, &rule(), checks, (None, large.next, 1)).await;
        let last = last.unwrap();
        assert_eq!((last.entries.len(), last.next), (0, None));
        // A filter limits the plan and the pages alike.
        let filtered = (Some("b.txt"), None, 10);
        let page = rename_page(&context, &rule(), checks, filtered)
            .await
            .unwrap();
        let plan = (page.plan.files, page.plan.bytes, page.plan.taken_files);
        assert_eq!((plan, page.plan.unchanged_files), ((1, 5, 1), 0));
        assert_eq!((page.entries.len(), page.next), (1, None));
        // A storage other than S3 passes every renamed file through the node.
        linked_bucket(&context, GroupBackendKind::Gcs).await;
        let page = rename_page(&context, &rule(), checks, (None, None, 1)).await;
        let plan = page.unwrap().plan;
        let streamed = (plan.streamed_files, plan.streamed_bytes);
        assert_eq!(streamed, (3, 8 + COPY_PART + 1));
    }
}
