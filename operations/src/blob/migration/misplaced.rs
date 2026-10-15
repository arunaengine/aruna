//! Counts the versions of a bucket that a storage move would move now.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{encrypting, move_routing, place, source_key, stays};
use crate::blob::records::blob_location_read;
use crate::driver::DriverContext;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_VERSIONS_KEYSPACE, BROKEN_VERSION_KEYSPACE};
use aruna_core::structs::storage::blob::{BackendLocation, BlobVersion, VersionKey};
use aruna_core::types::Key;

/// Versions one count reads at most, so a request stays bounded.
const COUNT_LIMIT: usize = 10_000;
const COUNT_PAGE: usize = 256;

/// Versions under a prefix that are not where the rules place a new write of their key.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Misplaced {
    pub count: u64,
    /// `false` when the count stopped at its limit and more versions exist.
    pub complete: bool,
}

/// Counts what a move of `prefix` would move, with the move's own skips: an encrypted bucket
/// moves nothing; linked-folder files, unfinished copies and recorded broken versions stay.
pub async fn count_misplaced(
    context: &DriverContext,
    bucket: &str,
    prefix: &str,
) -> Result<Misplaced, String> {
    let mut misplaced = Misplaced {
        count: 0,
        complete: true,
    };
    if encrypting(context, &bucket.as_bytes().to_vec().into()).await? {
        return Ok(misplaced);
    }
    let Some(routing) = move_routing(context, bucket).await? else {
        return Ok(misplaced);
    };
    let scan = VersionKey::bucket_prefix(bucket).map_err(|error| error.to_string())?;
    let (mut after, mut seen) = (None::<Key>, 0);
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
            seen += 1;
            let version_key = VersionKey::from_bytes(key.as_ref()).map_err(|e| e.to_string())?;
            if !version_key.key.starts_with(prefix) {
                continue;
            }
            let version = BlobVersion::from_bytes(value.as_ref()).map_err(|e| e.to_string())?;
            // A key the rules cannot place now fails its move; it is not counted.
            let Ok((resolved, _)) = place(&routing, &version_key) else {
                continue;
            };
            let wanted = |_: &_| (resolved.backend.clone(), resolved.compression.into());
            let Some(source) = source_key(&version, wanted) else {
                continue;
            };
            if !recorded(context, &key).await? && moves(context, &source).await? {
                misplaced.count += 1;
            }
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

/// Whether a check recorded the version as broken; a move skips it.
async fn recorded(context: &DriverContext, key: &Key) -> Result<bool, String> {
    let read = aruna_core::effects::StorageEffect::Read {
        key_space: BROKEN_VERSION_KEYSPACE.to_string(),
        key: key.clone(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => Ok(value.is_some()),
        other => Err(format!("could not read a broken version: {other:?}")),
    }
}

/// Whether the copy a version names exists and would leave its place.
async fn moves(
    context: &DriverContext,
    key: &aruna_core::structs::storage::blob::BlobLocationKey,
) -> Result<bool, String> {
    let aruna_core::effects::Effect::Storage(read) = blob_location_read(key, None) else {
        return Err("unexpected location read".to_string());
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => match value {
            Some(value) => BackendLocation::from_bytes(value.as_ref())
                .map(|location| !stays(&location, true))
                .map_err(|error| error.to_string()),
            None => Ok(false),
        },
        other => Err(format!("could not read a location: {other:?}")),
    }
}
