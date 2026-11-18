//! Marks of the object versions a plugin job with a secret wrote: kept by version on the receiver,
//! whatever happens to the install, and dropped only once the version is gone and its work ended.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::PLUGIN_WORK_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::job::JobId;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::objects::absent;
use super::rows::{
    Row, StoreError, decode, delete_call, done, encode, mapped_child, page_call, page_of,
    read_call, row, values,
};
use super::work::{Found, WorkKind, WorkLookup, WorkState};
use crate::s3::object::head::{HeadObjectInput, HeadObjectOperation};

/// Row kind of the marks in the work keyspace, apart from the rows an install's removal deletes.
pub const MARK_ROW: u8 = b'm';

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
pub(crate) fn mark_row(version: Version<'_>, mark: &Mark) -> Result<Row, StoreError> {
    Ok(row(PLUGIN_WORK_KEYSPACE, mark_key(version)?, encode(mark)?))
}

pub(crate) fn is_marked(version: Version<'_>) -> Result<IsMarked, StoreError> {
    Ok(IsMarked(mark_key(version)?))
}

#[derive(Debug, PartialEq)]
pub struct IsMarked(Vec<u8>);

impl Operation for IsMarked {
    type Output = bool;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<bool>, StoreError> {
        let read = vec![(PLUGIN_WORK_KEYSPACE, self.0.clone())];
        Ok(Next::Call(read_call(read)))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<bool>, StoreError> {
        let [row] = values::<1>(answer)?;
        Ok(Next::Done(row.is_some()))
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
    pub fn check(mark: &Mark, key: Option<Vec<u8>>) -> MarkEnded {
        let lookup = WorkLookup::new(mark.install_id, mark.work_id);
        MarkEnded {
            lookup,
            deleted: false,
            key,
        }
    }
}

impl Operation for MarkEnded {
    type Output = bool;
    type Error = StoreError;

    /// With the mark's key the delete runs in one transaction with the lookup.
    fn transaction(&self) -> TransactionMode {
        match self.key.is_some() {
            true => TransactionMode::Write,
            false => TransactionMode::None,
        }
    }

    fn first(&mut self) -> Result<Next<bool>, StoreError> {
        Ok(Next::Call(self.lookup.ask()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<bool>, StoreError> {
        if self.deleted {
            done(answer)?;
            return Ok(Next::Done(true));
        }
        let ended = match self.lookup.take(answer)? {
            Found::Ask(ask) => return Ok(Next::Call(ask)),
            Found::Row(row) => row.is_none_or(|(_, row)| row.state == WorkState::Settled),
        };
        match (ended, self.key.take()) {
            (true, Some(key)) => {
                self.deleted = true;
                Ok(Next::Call(delete_call(vec![(PLUGIN_WORK_KEYSPACE, key)])))
            }
            (ended, _) => Ok(Next::Done(ended)),
        }
    }
}

/// Marks one sweep pass reads at a time.
const MARK_PAGE: usize = 100;

#[derive(Debug, PartialEq)]
enum SweepStep {
    Paging,
    /// Asks whether the work that minted the version can write no more.
    Ended,
    /// Asks whether the version is gone.
    Head,
    /// Deletes the mark, reading the work row again.
    Dropping,
}

/// One mark a sweep looks at.
#[derive(Debug, PartialEq)]
struct SweptMark {
    key: Vec<u8>,
    mark: Mark,
    head: Option<HeadObjectInput>,
}

/// Drops the marks nothing can need any more, a page at a time: the work that minted the version
/// can write no more, as its row is settled or gone, and only then the version is absent. The
/// delete reads the row again, so it conflicts with a settlement or reservation.
#[derive(Debug, PartialEq)]
pub(crate) struct SweepMarks {
    after: Option<Vec<u8>>,
    more: bool,
    rows: VecDeque<(Vec<u8>, Vec<u8>)>,
    current: Option<SweptMark>,
    dropped: usize,
    step: SweepStep,
}

impl SweepMarks {
    pub(crate) fn new() -> Self {
        Self {
            after: None,
            more: false,
            rows: VecDeque::new(),
            current: None,
            dropped: 0,
            step: SweepStep::Paging,
        }
    }

    fn page(&mut self) -> IoCall {
        self.step = SweepStep::Paging;
        let after = self.after.clone();
        page_call(PLUGIN_WORK_KEYSPACE, vec![MARK_ROW], after, MARK_PAGE)
    }

    /// The call for the next mark, the next page, or the end.
    fn advance(&mut self) -> Result<Next<usize>, StoreError> {
        let Some((key, value)) = self.rows.pop_front() else {
            return Ok(match self.more {
                true => Next::Call(self.page()),
                false => Next::Done(self.dropped),
            });
        };
        let mark: Mark = decode(&value)?;
        let (bucket, object, version_id) = decode::<(String, String, Ulid)>(&key[1..])?;
        let head = HeadObjectInput {
            bucket,
            key: object,
            version_id: Some(version_id),
        };
        let ended = MarkEnded::check(&mark, None);
        self.current = Some(SweptMark {
            key,
            mark,
            head: Some(head),
        });
        self.step = SweepStep::Ended;
        Ok(Next::Call(IoCall::child(ended)))
    }
}

impl Operation for SweepMarks {
    type Output = usize;
    type Error = StoreError;

    fn first(&mut self) -> Result<Next<usize>, StoreError> {
        Ok(Next::Call(self.page()))
    }

    fn next(&mut self, answer: IoResult) -> Result<Next<usize>, StoreError> {
        match self.step {
            SweepStep::Paging => {
                let (rows, next) = page_of(answer)?;
                self.rows = rows.into();
                self.more = next.is_some();
                self.after = next;
            }
            SweepStep::Ended => {
                if answer.child::<bool, StoreError>()?? {
                    let head = (self.current.as_mut()).and_then(|mark| mark.head.take());
                    let head = head.ok_or(IoError::Unfinished)?;
                    self.step = SweepStep::Head;
                    let head = HeadObjectOperation::new(head);
                    // Present, or not known now: the mark stays.
                    return Ok(Next::Call(mapped_child(head, |head| {
                        Ok::<bool, ()>(head.is_err_and(|error| absent(&error)))
                    })));
                }
            }
            SweepStep::Head => {
                if answer.child::<bool, ()>()?.unwrap_or(false) {
                    let swept = self.current.take().ok_or(IoError::Unfinished)?;
                    self.step = SweepStep::Dropping;
                    let drop = MarkEnded::check(&swept.mark, Some(swept.key));
                    return Ok(Next::Call(IoCall::child(drop)));
                }
            }
            SweepStep::Dropping => {
                let dropped = answer.child::<bool, StoreError>()??;
                self.dropped += usize::from(dropped);
            }
        }
        self.advance()
    }
}

#[cfg(test)]
#[path = "marks_tests.rs"]
mod tests;
