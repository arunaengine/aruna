//! Counts the distributed buckets of this node's records, so a bucket total counts each once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Every member node counts its own member bucket in its usage totals, so a realm or group sum of
// node totals holds a distributed bucket once per member; these counts take that back out.

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::keyspaces::DISTRIBUTION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::structs::storage::distribution::{
    DistributionSnapshot, DistributionView, MemberState,
};
use aruna_core::types::GroupId;
use thiserror::Error;

/// At most this many distribution records are read by one scan of this node's records.
pub(crate) const MAX_SETS: usize = 4096;
/// Distribution records one page reads.
const PAGE: usize = 256;

/// The distributed buckets in a bucket total, and the member buckets their nodes count.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SetCounts {
    pub sets: u64,
    pub members: u64,
    /// Sets whose deletion is committed; nothing replicated says which members purged already.
    pub deleting: u64,
    /// False when this node holds more records than one count reads.
    pub complete: bool,
}

impl SetCounts {
    /// A total of node bucket counters with each distributed bucket counted once; `None` while a
    /// set of the scope is being deleted, whose member buckets may or may not be purged yet.
    pub fn distinct(&self, buckets: u64) -> Option<u64> {
        (self.complete && self.deleting == 0).then(|| {
            buckets
                .saturating_sub(self.members)
                .saturating_add(self.sets)
        })
    }

    /// Counts the sets whose member buckets the node totals hold; a failed member never made one.
    /// A committed deletion is counted apart, since its members purge at their own pace.
    pub fn of<'a>(snapshots: impl IntoIterator<Item = &'a DistributionSnapshot>) -> Self {
        let mut counts = Self {
            complete: true,
            ..Self::default()
        };
        snapshots
            .into_iter()
            .for_each(|snapshot| counts.add(snapshot));
        counts
    }

    fn add(&mut self, snapshot: &DistributionSnapshot) {
        let deleted =
            (snapshot.lifecycle.deletion.as_ref()).is_some_and(|deletion| deletion.committed);
        let members = (snapshot.config.members.iter())
            .filter(|member| member.state != MemberState::Failed)
            .count() as u64;
        if members == 0 {
            return;
        }
        if deleted {
            self.deleting += 1;
            return;
        }
        self.sets += 1;
        self.members += members;
    }
}

/// The counts of every set this node holds a record of, or of one owner group's sets. Reads
/// the records page by page; more than [`MAX_SETS`] records leave the counts incomplete.
#[derive(Debug, PartialEq)]
pub struct SetCountsOperation {
    group: Option<GroupId>,
    counts: SetCounts,
    seen: usize,
    pages: PrefixPages,
}

#[derive(Debug, Error, PartialEq)]
pub enum CountsError {
    #[error(transparent)]
    Io(#[from] IoError),
}

impl SetCountsOperation {
    pub fn new(group: Option<GroupId>) -> Self {
        Self {
            group,
            counts: SetCounts {
                complete: true,
                ..SetCounts::default()
            },
            seen: 0,
            pages: PrefixPages::new(DISTRIBUTION_KEYSPACE, Vec::new(), PAGE),
        }
    }
}

impl Operation for SetCountsOperation {
    type Output = SetCounts;
    type Error = CountsError;

    fn first(&mut self) -> Result<Next<SetCounts>, CountsError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SetCounts>, CountsError> {
        let values = self.pages.take(result)?;
        self.seen += values.len();
        // An unreadable record is skipped, as every reader of the records does.
        let views = (values.iter()).filter_map(|(_, value)| DistributionView::from_bytes(value).ok());
        for view in views {
            let snapshot = view.snapshot();
            if (self.group).is_none_or(|group| snapshot.config.owner_group == group) {
                self.counts.add(snapshot);
            }
        }
        if !self.pages.done() && self.seen < MAX_SETS {
            return Ok(Next::Call(self.pages.call()));
        }
        self.counts.complete = self.pages.done();
        Ok(Next::Done(self.counts))
    }
}

#[cfg(test)]
#[path = "counts_tests.rs"]
mod tests;
