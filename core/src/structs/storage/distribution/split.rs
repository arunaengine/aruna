//! Splits one key range of a distributed bucket in the record: the coordinator's signed `split`
//! replaces the range by two children with lineage, and ancestor ids resolve to descendants.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{
    DistributionError, DistributionMember, KeyRange, MAX_BOUND_BYTES, MAX_RANGES, RouteError,
    SetPhase, Statement, StatementKind,
};
use crate::structs::identity::realm::RealmId;

use super::snapshot::DistributionSnapshot;

const SPLIT_DOMAIN: &[u8] = b"aruna-distribution-split-v1";

/// A range a split replaced; the record keeps it so an ancestor id still resolves.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetiredRange {
    pub range_id: Ulid,
    /// The range this one was split from itself.
    pub lineage: Option<Ulid>,
    /// The first key of the upper child.
    pub split_key: String,
    pub lower_id: Ulid,
    pub upper_id: Ulid,
}

/// The ids one split derives from its range, epoch and key, so a retry is the same split.
fn split_ids(set_id: Ulid, range_id: Ulid, epoch: u64, split_key: &str) -> [Ulid; 3] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(SPLIT_DOMAIN);
    hasher.update(&set_id.to_bytes());
    hasher.update(&range_id.to_bytes());
    hasher.update(&epoch.to_be_bytes());
    hasher.update(split_key.as_bytes());
    let mut bytes = [0; 48];
    hasher.finalize_xof().fill(&mut bytes);
    let id = |offset: usize| {
        let mut id = [0; 16];
        id.copy_from_slice(&bytes[offset..offset + 16]);
        Ulid::from_bytes(id)
    };
    [id(0), id(16), id(32)]
}

impl Statement {
    /// The coordinator's automatic split of `range` at `split_key`, decided on revision
    /// `basis`. Its id and the child ids follow from the range, epoch and key.
    pub fn split(
        realm_id: RealmId,
        set_id: Ulid,
        signer: &DistributionMember,
        basis: u64,
        range: &KeyRange,
        split_key: &str,
    ) -> Self {
        let [statement_id, lower_id, upper_id] =
            split_ids(set_id, range.range_id, range.epoch, split_key);
        Self {
            realm_id,
            set_id,
            statement_id,
            member_id: signer.member_id,
            incarnation: signer.incarnation.unwrap_or_default(),
            intent: signer.intent,
            kind: StatementKind::Split {
                basis,
                range_id: range.range_id,
                epoch: range.epoch,
                split_key: split_key.to_string(),
                lower_id,
                upper_id,
            },
        }
    }
}

impl DistributionSnapshot {
    pub fn retired_range(&self, range_id: Ulid) -> Option<&RetiredRange> {
        self.retired.iter().find(|range| range.range_id == range_id)
    }

    /// Whether `ancestor` is `range` itself or a range it was split from, at any depth.
    pub fn descends(&self, range: &KeyRange, ancestor: Ulid) -> bool {
        if range.range_id == ancestor {
            return true;
        }
        let mut next = range.lineage;
        // Every retired range appears once in the chain, so the walk is bounded.
        for _ in 0..=self.retired.len() {
            match next {
                Some(id) if id == ancestor => return true,
                Some(id) => next = self.retired_range(id).and_then(|range| range.lineage),
                None => return false,
            }
        }
        false
    }

    /// The range holding `key` when a request names `range_id`, the range itself or one it
    /// descends from. Any other id is stale and the request retries after re-reading.
    pub fn resolve(&self, range_id: Ulid, key: &str) -> Result<&KeyRange, RouteError> {
        let range = self
            .range_for(key)
            .ok_or(RouteError::NoRange(self.set_id()))?;
        match self.descends(range, range_id) {
            true => Ok(range),
            false => Err(RouteError::StaleRange(range_id)),
        }
    }
}

/// Applies a coordinator's split: two children with lineage take the range's place, with the
/// same coordinator, epoch and replicas. No data moves and nothing is fenced.
pub(super) fn apply_split(
    next: &mut DistributionSnapshot,
    statement: &Statement,
) -> Result<(), DistributionError> {
    let StatementKind::Split {
        range_id,
        epoch,
        split_key,
        lower_id,
        upper_id,
        ..
    } = &statement.kind
    else {
        return Err(DistributionError::InvalidStatement("not a split"));
    };
    let invalid = DistributionError::InvalidStatement;
    if next.phase != SetPhase::Active || next.lifecycle.deletion.is_some() {
        return Err(invalid(
            "only an active set that is not being deleted splits",
        ));
    }
    if next.config.ranges.len() >= MAX_RANGES {
        return Err(DistributionError::RangeCount);
    }
    let index = (next.config.ranges.iter())
        .position(|range| range.range_id == *range_id)
        .ok_or(invalid("the range was already split or does not exist"))?;
    let range = next.config.ranges[index].clone();
    if range.coordinator != statement.member_id || range.epoch != *epoch {
        return Err(invalid(
            "only the range's coordinator splits it at its epoch",
        ));
    }
    if (next.lifecycle.transfers.iter()).any(|transfer| transfer.range_id == *range_id) {
        return Err(invalid("a transfer of the range is open"));
    }
    let inside = range.lower.as_str() < split_key.as_str()
        && range
            .upper
            .as_deref()
            .is_none_or(|upper| split_key.as_str() < upper);
    if !inside || split_key.len() > MAX_BOUND_BYTES {
        return Err(invalid("the split key leaves a child empty"));
    }
    let taken = |id: &Ulid| {
        next.config.ranges.iter().any(|range| range.range_id == *id)
            || next.retired_range(*id).is_some()
    };
    if lower_id == upper_id || taken(lower_id) || taken(upper_id) {
        return Err(invalid("the child range ids are taken"));
    }
    let child = |range_id, lower, upper| KeyRange {
        range_id,
        lower,
        upper,
        lineage: Some(range.range_id),
        ..range.clone()
    };
    let lower = child(*lower_id, range.lower.clone(), Some(split_key.clone()));
    let upper = child(*upper_id, split_key.clone(), range.upper.clone());
    next.config.ranges.splice(index..=index, [lower, upper]);
    next.retired.push(RetiredRange {
        range_id: range.range_id,
        lineage: range.lineage,
        split_key: split_key.clone(),
        lower_id: *lower_id,
        upper_id: *upper_id,
    });
    Ok(())
}

/// Retired ranges stay bounded and never reuse a current id.
pub(super) fn check_retired(snapshot: &DistributionSnapshot) -> Result<(), DistributionError> {
    if snapshot.retired.len() > MAX_RANGES {
        return Err(DistributionError::RangeCount);
    }
    let mut ids = std::collections::BTreeSet::new();
    for retired in &snapshot.retired {
        let current =
            (snapshot.config.ranges.iter()).any(|range| range.range_id == retired.range_id);
        if current || !ids.insert(retired.range_id) {
            return Err(DistributionError::DuplicateRange(retired.range_id));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "split_tests.rs"]
mod tests;
