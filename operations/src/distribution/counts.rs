//! Counts the distributed buckets of this node's records, so a bucket total counts each once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Every member node counts its own member bucket in its usage totals, so a realm or group sum of
// node totals holds a distributed bucket once per member; these counts take that back out.

use aruna_core::structs::storage::distribution::{DistributionSnapshot, MemberState};
use aruna_core::types::GroupId;

use crate::distribution::resolve::scan_sets;
use crate::driver::DriverContext;

/// The distributed buckets in a bucket total, and the member buckets their nodes count.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SetCounts {
    pub sets: u64,
    pub members: u64,
    /// False when this node holds more records than one count reads.
    pub complete: bool,
}

impl SetCounts {
    /// A total of node bucket counters with each distributed bucket counted once.
    pub fn distinct(&self, buckets: u64) -> u64 {
        buckets
            .saturating_sub(self.members)
            .saturating_add(self.sets)
    }

    /// Counts the sets that still have member buckets: a failed member never made one, and a
    /// committed deletion purges them.
    pub fn of<'a>(snapshots: impl IntoIterator<Item = &'a DistributionSnapshot>) -> Self {
        let mut counts = Self {
            complete: true,
            ..Self::default()
        };
        for snapshot in snapshots {
            let deleted =
                (snapshot.lifecycle.deletion.as_ref()).is_some_and(|deletion| deletion.committed);
            let members = (snapshot.config.members.iter())
                .filter(|member| member.state != MemberState::Failed)
                .count() as u64;
            if deleted || members == 0 {
                continue;
            }
            counts.sets += 1;
            counts.members += members;
        }
        counts
    }
}

/// The counts of every set this node holds a record of, or of one owner group's sets.
pub async fn set_counts(
    context: &DriverContext,
    group: Option<GroupId>,
) -> Result<SetCounts, String> {
    let keep = |snapshot: &DistributionSnapshot| {
        group.is_none_or(|group| snapshot.config.owner_group == group)
    };
    let (snapshots, dropped) = scan_sets(context, keep, usize::MAX).await?;
    Ok(SetCounts {
        complete: !dropped,
        ..SetCounts::of(&snapshots)
    })
}

#[cfg(test)]
#[path = "counts_tests.rs"]
mod tests;
