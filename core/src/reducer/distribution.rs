//! Stores distribution record changes and reduces them deterministically: from each revision the
//! smallest applicable event wins, so every node that holds the same changes holds one record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::structs::storage::distribution::change::apply_change;
use crate::structs::storage::distribution::{
    DistributionChange, DistributionError, DistributionRecord, DistributionRevision,
};

const CHANGE_PREFIX: &str = "distribution.changes.";

/// One stored change per content, so a republished change lands on its first path.
pub fn distribution_change_path(change: &DistributionChange) -> String {
    let bytes = postcard::to_allocvec(change).unwrap_or_default();
    format!("{CHANGE_PREFIX}{}", blake3::hash(&bytes).to_hex())
}

/// The applied changes in order: the record they reduce to and each step's revision and dot.
#[derive(Debug, Default)]
struct DistributionChain {
    record: Option<DistributionRecord>,
    steps: Vec<(DistributionRevision, AdminDocumentDot)>,
}

impl DistributionChain {
    /// The index of the step that builds on `predecessor`, or `None` when it is unknown.
    fn successor_index(&self, predecessor: Option<DistributionRevision>) -> Option<usize> {
        match predecessor {
            None => Some(0),
            Some(revision) => self
                .steps
                .iter()
                .position(|(step, _)| *step == revision)
                .map(|index| index + 1),
        }
    }
}

fn change_link(dot: &AdminDocumentDot) -> Vec<u8> {
    let mut link = dot.event_id.to_bytes().to_vec();
    link.extend_from_slice(dot.origin_node_id.as_bytes());
    link.extend_from_slice(&dot.origin_seq.to_be_bytes());
    link
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
        let chain = self.distribution_chain();
        match chain.successor_index(change.predecessor) {
            // Builds on the current record: it must apply now, authority included.
            Some(index) if index == chain.steps.len() => {
                apply_change(
                    chain.record.as_ref(),
                    change,
                    event.origin_node_id,
                    &change_link(&event.dot()),
                )?;
            }
            // A competing successor already won; a later dot can never replace it.
            Some(index) if chain.steps[index].1 < event.dot() => {
                return Err(DistributionError::StaleRevision.into());
            }
            // An earlier dot or an unknown predecessor is kept; reduction decides.
            Some(_) | None => {}
        }
        let value = serde_json::to_string(change.as_ref())
            .map_err(|_| DistributionError::Malformed("change does not encode"))?;
        self.apply_immutable_value(event, distribution_change_path(change), value);
        Ok(AdminApplyStatus::Applied)
    }

    /// The reduced record of a distribution target, if its `create` has arrived.
    pub fn materialized_distribution(&self) -> Option<DistributionRecord> {
        if !matches!(&self.target, AdminDocumentTarget::Distribution { .. }) {
            return None;
        }
        self.distribution_chain().record
    }

    fn distribution_chain(&self) -> DistributionChain {
        let mut pending: BTreeMap<Option<DistributionRevision>, Vec<_>> = BTreeMap::new();
        let stored = self
            .user_subject_ids
            .range::<str, _>((Included(CHANGE_PREFIX), Unbounded))
            .take_while(|(path, _)| path.starts_with(CHANGE_PREFIX));
        for (_, version) in stored {
            let Some(change) = version
                .value
                .as_deref()
                .and_then(|value| serde_json::from_str::<DistributionChange>(value).ok())
            else {
                continue;
            };
            pending
                .entry(change.predecessor)
                .or_default()
                .push((version.dot, change));
        }
        let mut chain = DistributionChain::default();
        // Each revision is consumed once, so the walk ends after at most one pass.
        while let Some(mut candidates) = pending.remove(&chain.record.as_ref().map(|r| r.revision))
        {
            candidates.sort_by_key(|(dot, _)| *dot);
            let next = candidates.iter().find_map(|(dot, change)| {
                apply_change(
                    chain.record.as_ref(),
                    change,
                    dot.origin_node_id,
                    &change_link(dot),
                )
                .ok()
                .map(|record| (record, *dot))
            });
            let Some((record, dot)) = next else {
                break;
            };
            chain.steps.push((record.revision, dot));
            chain.record = Some(record);
        }
        chain
    }
}
