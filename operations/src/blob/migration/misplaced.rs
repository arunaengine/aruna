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

/// Versions one count reads at most, so a request stays bounded.
const COUNT_LIMIT: usize = 10_000;
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
}

/// Counts what a move of `prefix` would move, with the move's own skips: an encrypted bucket
/// moves nothing; linked-folder files, unfinished copies and recorded broken versions stay.
pub async fn count_misplaced(
    context: &DriverContext,
    bucket: &str,
    prefix: &str,
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
    let earlier = read_earlier(context, &routing).await?;
    let scan = VersionKey::bucket_prefix(bucket).map_err(|error| error.to_string())?;
    let (mut after, mut seen) = (None::<Key>, 0);
    loop {
        let (versions, next) = crate::jobs::store::iter_prefix_page(
            &context.storage_handle,
            BLOB_VERSIONS_KEYSPACE,
            Some(scan.clone().into()),
            after.take(),
            COUNT_PAGE.min(COUNT_LIMIT - seen),
            None,
        )
        .await?;
        for (key, value) in versions {
            seen += 1;
            let version_key = VersionKey::from_bytes(key.as_ref()).map_err(|e| e.to_string())?;
            if !version_key.key.starts_with(prefix) {
                continue;
            }
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
            Some(_) if seen >= COUNT_LIMIT => {
                misplaced.complete = false;
                return Ok(misplaced);
            }
            Some(next) => after = Some(next),
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
