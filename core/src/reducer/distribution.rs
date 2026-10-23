//! Stores distribution record changes per event and reduces them deterministically: from each
//! revision the smallest valid event wins. Losers are pruned, waiting changes are bounded per
//! origin, and revisions far behind the head are folded into a checkpoint record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::structs::storage::distribution::change::{ChangeAuthority, apply_change};
use crate::structs::storage::distribution::{
    DistributionChange, DistributionError, DistributionRecord, DistributionRevision,
};

const CHANGE_PREFIX: &str = "distribution.changes.";
const CHECKPOINT_PATH: &str = "distribution.checkpoint";
/// Revisions this far behind the head are final: they fold into the checkpoint.
pub const FINALITY_WINDOW: usize = 16;
/// Changes one origin may keep while their predecessor is unknown.
pub const MAX_PENDING: usize = 8;

fn change_path(event_id: &Ulid) -> String {
    format!("{CHANGE_PREFIX}{event_id}")
}

fn change_link(dot: &AdminDocumentDot) -> Vec<u8> {
    let mut link = dot.event_id.to_bytes().to_vec();
    link.extend_from_slice(dot.origin_node_id.as_bytes());
    link.extend_from_slice(&dot.origin_seq.to_be_bytes());
    link
}

/// One stored change with the event that published it.
#[derive(Clone, Debug)]
struct Candidate {
    path: String,
    dot: AdminDocumentDot,
    change: DistributionChange,
}

/// The outcome of one reduction over the checkpoint and the stored changes.
#[derive(Debug, Default)]
struct Reduction {
    base: Option<DistributionRecord>,
    record: Option<DistributionRecord>,
    /// The winners after the checkpoint, each with the revision it produced.
    steps: Vec<(DistributionRevision, Candidate)>,
    pending: Vec<Candidate>,
    /// Stored changes that can never apply: losers and changes on final revisions.
    dead: BTreeSet<String>,
    /// Winners folded into the new checkpoint; their stored changes are no longer needed.
    folded: BTreeSet<String>,
    /// A new checkpoint when the head moved past the finality window.
    checkpoint: Option<DistributionRecord>,
}

/// Where a predecessor sits relative to the reduced chain.
enum Position {
    Head,
    Taken(AdminDocumentDot),
    Final,
    Unknown,
}

impl Reduction {
    fn locate(&self, predecessor: Option<DistributionRevision>) -> Position {
        let base_key = self.base.as_ref().map(|base| base.revision);
        let keys =
            std::iter::once(base_key).chain(self.steps.iter().map(|(revision, _)| Some(*revision)));
        if let Some(index) = keys.clone().position(|key| key == predecessor) {
            return match self.steps.get(index) {
                Some((_, winner)) => Position::Taken(winner.dot),
                None => Position::Head,
            };
        }
        let base_number = self.base.as_ref().map(|base| base.revision.number);
        match (predecessor, base_number) {
            (None, Some(_)) => Position::Final,
            (Some(revision), Some(number)) if revision.number < number => Position::Final,
            _ => Position::Unknown,
        }
    }

    fn pending_from(&self, origin: &NodeId) -> usize {
        self.pending
            .iter()
            .filter(|candidate| candidate.dot.origin_node_id == *origin)
            .count()
    }
}

impl AdminDocumentState {
    pub(super) fn apply_distribution(
        &mut self,
        event: &AdminDocumentEvent,
        set_id: &Ulid,
    ) -> Result<AdminApplyStatus, AdminDocumentError> {
        let AdminDocumentOperation::DistributionChanged { change } = &event.op else {
            return Err(AdminDocumentError::UnsupportedTarget);
        };
        if change.set_id != *set_id {
            return Err(DistributionError::SetMismatch(change.set_id).into());
        }
        if !change.op.supported() {
            return Err(DistributionError::Unsupported(change.op.name()).into());
        }
        change.validate_shape()?;
        let before = self.reduce_distribution();
        let origin = event.origin_node_id;
        match before.locate(change.predecessor) {
            // Builds on the current record: it must apply now, authority included.
            Position::Head => {
                apply_change(
                    before.record.as_ref(),
                    change,
                    origin,
                    &change_link(&event.dot()),
                )?;
            }
            // A competing successor already won; a later dot can never replace it.
            Position::Taken(winner) if winner < event.dot() => {
                return Err(DistributionError::StaleRevision.into());
            }
            Position::Taken(_) => {}
            Position::Final => return Err(DistributionError::StaleRevision.into()),
            // Kept only within the origin's bound, and member changes only from the member.
            Position::Unknown => {
                if let (ChangeAuthority::MemberNode(member_id), Some(record)) =
                    (change.op.authority(), before.record.as_ref())
                    && record
                        .member(member_id)
                        .is_some_and(|member| member.node_id != origin)
                {
                    return Err(DistributionError::AdmissionOrigin(member_id).into());
                }
                if before.pending_from(&origin) >= MAX_PENDING {
                    return Err(DistributionError::TooManyPending.into());
                }
            }
        }
        let value = serde_json::to_string(change.as_ref())
            .map_err(|_| DistributionError::Malformed("change does not encode"))?;
        let path = change_path(&event.event_id);
        let version = AdminAttributeVersion {
            value: Some(value),
            dot: event.dot(),
        };
        self.user_subject_ids.insert(path.clone(), version);
        let after = self.reduce_distribution();
        if after.dead.contains(&path) {
            self.user_subject_ids.remove(&path);
            return Err(DistributionError::StaleRevision.into());
        }
        self.settle_distribution(after, event.dot());
        Ok(AdminApplyStatus::Applied)
    }

    /// The reduced record of a distribution target, if its `create` has arrived.
    pub fn materialized_distribution(&self) -> Option<DistributionRecord> {
        if !matches!(&self.target, AdminDocumentTarget::Distribution { .. }) {
            return None;
        }
        self.reduce_distribution().record
    }

    /// Drops changes that can never apply and stores an advanced checkpoint.
    fn settle_distribution(&mut self, reduction: Reduction, dot: AdminDocumentDot) {
        for path in reduction.dead.iter().chain(&reduction.folded) {
            self.user_subject_ids.remove(path);
        }
        if let Some(checkpoint) = reduction.checkpoint
            && let Ok(value) = serde_json::to_string(&checkpoint)
        {
            let version = AdminAttributeVersion {
                value: Some(value),
                dot,
            };
            self.user_subject_ids
                .insert(CHECKPOINT_PATH.to_string(), version);
        }
    }

    fn stored_changes(&self) -> Vec<Candidate> {
        self.user_subject_ids
            .range::<str, _>((Included(CHANGE_PREFIX), Unbounded))
            .take_while(|(path, _)| path.starts_with(CHANGE_PREFIX))
            .filter_map(|(path, version)| {
                let change = serde_json::from_str(version.value.as_deref()?).ok()?;
                Some(Candidate {
                    path: path.clone(),
                    dot: version.dot,
                    change,
                })
            })
            .collect()
    }

    fn reduce_distribution(&self) -> Reduction {
        let base = self
            .user_subject_ids
            .get(CHECKPOINT_PATH)
            .and_then(|version| version.value.as_deref())
            .and_then(|value| serde_json::from_str::<DistributionRecord>(value).ok());
        let mut groups: BTreeMap<Option<DistributionRevision>, Vec<Candidate>> = BTreeMap::new();
        for candidate in self.stored_changes() {
            groups
                .entry(candidate.change.predecessor)
                .or_default()
                .push(candidate);
        }
        let mut reduction = Reduction {
            record: base.clone(),
            base,
            ..Reduction::default()
        };
        // Each revision is consumed once, so the walk visits every change at most once.
        while let Some(mut candidates) =
            groups.remove(&reduction.record.as_ref().map(|record| record.revision))
        {
            candidates.sort_by_key(|candidate| candidate.dot);
            let mut winner = None;
            for candidate in candidates {
                if winner.is_some() {
                    reduction.dead.insert(candidate.path);
                    continue;
                }
                let applied = apply_change(
                    reduction.record.as_ref(),
                    &candidate.change,
                    candidate.dot.origin_node_id,
                    &change_link(&candidate.dot),
                );
                match applied {
                    Ok(record) => winner = Some((record, candidate)),
                    Err(_) => {
                        reduction.dead.insert(candidate.path);
                    }
                }
            }
            let Some((record, candidate)) = winner else {
                break;
            };
            reduction.steps.push((record.revision, candidate));
            reduction.record = Some(record);
        }
        let final_number = reduction.base.as_ref().map(|base| base.revision.number);
        for candidate in groups.into_values().flatten() {
            let stale = match (candidate.change.predecessor, final_number) {
                (None, Some(_)) => true,
                (Some(revision), Some(number)) => revision.number < number,
                (_, None) => false,
            };
            if stale {
                reduction.dead.insert(candidate.path);
            } else {
                reduction.pending.push(candidate);
            }
        }
        reduction.advance_checkpoint();
        reduction
    }
}

impl Reduction {
    /// Folds the steps behind the finality window into a new checkpoint record.
    fn advance_checkpoint(&mut self) {
        let Some(folded) = self.steps.len().checked_sub(FINALITY_WINDOW) else {
            return;
        };
        if folded == 0 {
            return;
        }
        let mut record = self.base.clone();
        for (_, candidate) in &self.steps[..folded] {
            let applied = apply_change(
                record.as_ref(),
                &candidate.change,
                candidate.dot.origin_node_id,
                &change_link(&candidate.dot),
            );
            let Ok(next) = applied else {
                return;
            };
            record = Some(next);
        }
        let Some(checkpoint) = record else {
            return;
        };
        for (_, candidate) in &self.steps[..folded] {
            self.folded.insert(candidate.path.clone());
        }
        let number = checkpoint.revision.number;
        let (live, stale): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|candidate| {
                candidate
                    .change
                    .predecessor
                    .is_some_and(|revision| revision.number >= number)
            });
        self.pending = live;
        self.dead
            .extend(stale.into_iter().map(|candidate| candidate.path));
        self.checkpoint = Some(checkpoint);
    }
}
