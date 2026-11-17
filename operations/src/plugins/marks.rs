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
use super::rows::{
    Answer, Ask, Plan, Row, RowsOperation, Turn, done, page_rows, row, values,
};
use super::store::{StoreError, decode, encode};
use super::work::{Found, WorkKind, WorkLookup, WorkState};
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

/// The row that marks `version`, written in the transaction that reserves it, before any byte
/// is written.
pub fn mark_row(version: Version<'_>, mark: &Mark) -> Result<Row, StoreError> {
    Ok(row(PLUGIN_WORK_KEYSPACE, mark_key(version)?, encode(mark)?))
}

pub fn is_marked(version: Version<'_>) -> Result<RowsOperation<IsMarked>, StoreError> {
    Ok(RowsOperation::direct(IsMarked(mark_key(version)?)))
}

#[derive(Debug, PartialEq)]
pub struct IsMarked(Vec<u8>);

impl Plan for IsMarked {
    type Output = bool;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<bool>, StoreError> {
        let read = vec![(PLUGIN_WORK_KEYSPACE, self.0.clone())];
        Ok(Turn::Ask(Ask::Read(read)))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<bool>, StoreError> {
        let [row] = values::<1>(answer)?;
        Ok(Turn::Finish(row.is_some()))
    }
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
    let marked = is_marked((bucket, key, resolved)).map_err(|error| unavailable(error.0))?;
    match drive(marked, ctx).await {
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
        let page = page_rows(PLUGIN_WORK_KEYSPACE, vec![MARK_ROW], after, PAGE);
        let (rows, next) = drive(page, ctx).await?;
        for (key, value) in rows {
            let mark: Mark = decode(&value)?;
            let (bucket, object, version_id) = decode::<(String, String, Ulid)>(&key[1..])?;
            if !drive(MarkEnded::check(&mark, None), ctx).await? {
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
            dropped += usize::from(drive(MarkEnded::check(&mark, Some(key)), ctx).await?);
        }
        match next {
            Some(key) => after = Some(key),
            None => return Ok(dropped),
        }
    }
}

/// Whether the mark's work can write no more: its row is settled or gone. With the mark's key,
/// the mark is deleted in the same transaction, so the delete conflicts with a settlement or
/// reservation.
#[derive(Debug, PartialEq)]
pub struct MarkEnded {
    lookup: WorkLookup,
    key: Option<Vec<u8>>,
    deleted: bool,
}

impl MarkEnded {
    pub fn check(mark: &Mark, key: Option<Vec<u8>>) -> RowsOperation<MarkEnded> {
        let lookup = WorkLookup::new(mark.install_id, mark.work_id);
        let plan = MarkEnded {
            lookup,
            deleted: false,
            key,
        };
        match plan.key.is_some() {
            true => RowsOperation::transaction(plan),
            false => RowsOperation::direct(plan),
        }
    }
}

impl Plan for MarkEnded {
    type Output = bool;
    type Error = StoreError;

    fn first(&mut self) -> Result<Turn<bool>, StoreError> {
        Ok(Turn::Ask(self.lookup.ask()))
    }

    fn next(&mut self, answer: Answer) -> Result<Turn<bool>, StoreError> {
        if self.deleted {
            done(answer)?;
            return Ok(Turn::Finish(true));
        }
        let ended = match self.lookup.take(answer)? {
            Found::Ask(ask) => return Ok(Turn::Ask(ask)),
            Found::Row(row) => row.is_none_or(|(_, row)| row.state == WorkState::Settled),
        };
        match (ended, self.key.take()) {
            (true, Some(key)) => {
                self.deleted = true;
                Ok(Turn::Ask(Ask::Delete(vec![(PLUGIN_WORK_KEYSPACE, key)])))
            }
            (ended, _) => Ok(Turn::Finish(ended)),
        }
    }
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
