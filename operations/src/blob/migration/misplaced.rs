//! Counts the versions of a bucket that a storage move would move now, and the linked files
//! it would rename.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{encrypting, move_routing, place, read_earlier, renames, source_key, stays};
use crate::blob::records::blob_location_read;
use crate::driver::DriverContext;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, BROKEN_VERSION_KEYSPACE, IMPORTED_OBJECT_KEYSPACE,
};
use aruna_core::structs::storage::backend_import::{ImportedObject, object_key};
use aruna_core::structs::storage::blob::{BackendLocation, BlobVersion, VersionKey};
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
/// Version rows one rename preview passes over at most, for its totals and for its page.
const TOTAL_ROWS: usize = 2_000_000;
const PAGE_ROWS: usize = 200_000;

/// One page of the linked files a rename gives their current name, with the totals.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RenamePage {
    /// Files the caller may read that a rename renames; a lower bound while `complete` is false.
    pub total: u64,
    pub complete: bool,
    /// The old and the new path, and whether the caller may write the key.
    pub entries: Vec<(String, String, bool)>,
    /// The key after which the next page starts; a key the caller may read.
    pub next: Option<String>,
}

/// The linked files under `prefix` of `bucket` an asked rename renames, page by page in
/// storage order after the key `after`, as far as `readable` allows the key.
pub async fn rename_page(
    context: &DriverContext,
    (bucket, prefix): (&str, &str),
    (readable, writable): (
        &(dyn Fn(&str) -> bool + Sync),
        &(dyn Fn(&str) -> bool + Sync),
    ),
    (filter, after, limit): (Option<&str>, Option<String>, usize),
) -> Result<RenamePage, String> {
    use crate::blob::unify::KeyWalk;
    let mut page = RenamePage {
        complete: true,
        ..RenamePage::default()
    };
    if encrypting(context, &bucket.as_bytes().to_vec().into()).await? {
        return Ok(page);
    }
    let Some(routing) = move_routing(context, bucket).await? else {
        return Ok(page);
    };
    let earlier = read_earlier(context, &routing).await?;
    let shown = |key: &str| {
        key.starts_with(prefix) && filter.is_none_or(|filter| key.contains(filter)) && readable(key)
    };
    let mut walk = KeyWalk::new(context, bucket, None);
    while let Some((key, versions)) = walk.next().await? {
        if walk.scanned > TOTAL_ROWS {
            page.complete = false;
            break;
        }
        if shown(&key) {
            let found = renamed(context, &routing, &earlier, (bucket, &key), versions).await?;
            page.total += found.len() as u64;
        }
    }
    let mut walk = KeyWalk::new(context, bucket, after.as_deref());
    let mut last = None;
    while let Some((key, versions)) = walk.next().await? {
        if page.entries.len() >= limit || walk.scanned > PAGE_ROWS {
            page.next = last;
            if page.next.is_none() {
                return Err("too many keys in a row are hidden from the caller".to_string());
            }
            return Ok(page);
        }
        if !shown(&key) {
            continue;
        }
        let found = renamed(context, &routing, &earlier, (bucket, &key), versions).await?;
        let allowed = writable(&key);
        page.entries
            .extend(found.into_iter().map(|(from, to)| (from, to, allowed)));
        last = Some(key);
    }
    Ok(page)
}

/// The old and new path of each version of `key` whose linked file a rename renames.
async fn renamed(
    context: &DriverContext,
    routing: &aruna_core::structs::storage::routing::RoutingSnapshot,
    earlier: &super::Earlier,
    (bucket, key): (&str, &str),
    versions: Vec<(ulid::Ulid, Vec<u8>)>,
) -> Result<Vec<(String, String)>, String> {
    let mut found = Vec::new();
    for (id, value) in versions {
        let version_key = VersionKey::new(bucket, key, id);
        let Ok((resolved, Some(plain))) = place(routing, &version_key) else {
            continue;
        };
        let version = BlobVersion::from_bytes(&value).map_err(|error| error.to_string())?;
        let target = (resolved.backend.clone(), resolved.compression.into());
        let Some(source) = source_key(&version, |_: &_| target.clone(), true) else {
            continue;
        };
        let Some(location) = read_location(context, &source).await? else {
            continue;
        };
        if renames(routing, earlier, &plain, &location, &version_key) {
            found.push((location.backend_path, plain.path));
        }
    }
    Ok(found)
}
