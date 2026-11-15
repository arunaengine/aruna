//! Marks of the object versions a plugin job with a secret wrote: kept by version on the receiver,
//! whatever happens to the install, and dropped only once the version is gone and its work ended.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::keyspaces::PLUGIN_WORK_KEYSPACE;
use aruna_core::structs::execution::job::JobId;
use aruna_plugins::broker::{ErrorKind, HostError};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::check::unavailable;
use super::store::{StoreError, Txn, decode, encode, transact};
use super::work::{WorkKind, WorkState, read_in};
use crate::driver::{DriverContext, drive};
use crate::s3::object::head::{HeadObjectError, HeadObjectInput, HeadObjectOperation};

/// Row kind of the marks in the work keyspace, apart from the rows an install's removal deletes.
pub const MARK_ROW: u8 = b'm';

/// Marks one sweep pass reads at a time.
const PAGE: usize = 100;

/// What every refusal of a marked version says.
pub const MARKED: &str = "this object was written by a job with a secret; plugins cannot read it";

/// One object version on this node: bucket, key and version id.
pub type Version<'a> = (&'a str, &'a str, Ulid);

/// Who wrote a marked version; never a secret or anything released for it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mark {
    pub install_id: Ulid,
    pub work_id: Ulid,
    pub job: Option<JobId>,
    pub kind: WorkKind,
    pub at_ms: u64,
}

fn mark_key((bucket, key, version_id): Version<'_>) -> Result<Vec<u8>, StoreError> {
    Ok([&[MARK_ROW][..], &encode(&(bucket, key, version_id))?].concat())
}

/// Marks `version` in `txn`, the transaction that reserves it, before any byte is written.
pub async fn mark_in(txn: &Txn<'_>, version: Version<'_>, mark: &Mark) -> Result<(), StoreError> {
    let row = vec![(mark_key(version)?, encode(mark)?)];
    txn.write(PLUGIN_WORK_KEYSPACE, row).await
}

pub async fn is_marked(ctx: &DriverContext, version: Version<'_>) -> Result<bool, StoreError> {
    let row = Txn::direct(ctx)
        .read(PLUGIN_WORK_KEYSPACE, &mark_key(version)?)
        .await?;
    Ok(row.is_some())
}

/// The version of `bucket/key` a read or copy reads (`version`, else the current one), refused
/// when it is marked; `None` when there is no such object, so nothing is read either.
pub async fn unmarked(
    ctx: &DriverContext,
    (bucket, key): (&str, &str),
    version: Option<Ulid>,
) -> Result<Option<Ulid>, HostError> {
    let head = HeadObjectInput {
        bucket: bucket.to_string(),
        key: key.to_string(),
        version_id: version,
    };
    let resolved = match drive(HeadObjectOperation::new(head), ctx).await {
        Ok(head) => head.resolved_version_id.or(version),
        Err(error) if absent(&error) => return Ok(None),
        Err(error) => return Err(unavailable(error.to_string())),
    };
    let Some(resolved) = resolved else {
        return Ok(None);
    };
    match is_marked(ctx, (bucket, key, resolved)).await {
        Ok(false) => Ok(Some(resolved)),
        Ok(true) => Err(HostError::new(ErrorKind::Denied, MARKED)),
        Err(error) => Err(unavailable(error.0)),
    }
}

/// Drops the marks nothing can need any more, a page at a time: the work that minted the
/// version can write no more, as its row is settled or gone, and only then the version is
/// absent. The delete reads the row again, so it conflicts with a settlement or reservation.
pub async fn sweep_marks(ctx: &DriverContext) -> Result<usize, StoreError> {
    let (mut dropped, mut after) = (0, None);
    loop {
        let txn = Txn::direct(ctx);
        let (rows, next) = txn
            .page(PLUGIN_WORK_KEYSPACE, &[MARK_ROW], after, PAGE)
            .await?;
        for (key, value) in &rows {
            let mark: Mark = decode(value)?;
            let (bucket, object, version_id) = decode::<(String, String, Ulid)>(&key[1..])?;
            if !ended(&Txn::direct(ctx), &mark).await? {
                continue;
            }
            let head = HeadObjectInput {
                bucket,
                key: object,
                version_id: Some(version_id),
            };
            match drive(HeadObjectOperation::new(head), ctx).await {
                Err(error) if absent(&error) => {}
                // Present, or not known now: the mark stays.
                _ => continue,
            }
            let gone = transact(ctx, async |txn: &Txn<'_>| {
                if !ended(txn, &mark).await? {
                    return Ok::<_, StoreError>(false);
                }
                txn.delete(PLUGIN_WORK_KEYSPACE, vec![key.clone()]).await?;
                Ok(true)
            });
            dropped += usize::from(gone.await?);
        }
        match next {
            Some(key) => after = Some(key),
            None => return Ok(dropped),
        }
    }
}

/// Whether the mark's work can write no more: its row is settled or gone.
async fn ended(txn: &Txn<'_>, mark: &Mark) -> Result<bool, StoreError> {
    let row = read_in(txn, mark.install_id, mark.work_id).await?;
    Ok(row.is_none_or(|(_, row)| row.state == WorkState::Settled))
}

fn absent(error: &HeadObjectError) -> bool {
    matches!(
        error,
        HeadObjectError::NoSuchKey | HeadObjectError::NoSuchVersion | HeadObjectError::DeleteMarker
    )
}

#[cfg(test)]
#[path = "marks_tests.rs"]
mod tests;
