//! Decides what a receiver keeps of snapshots whose publisher the realm config does not know
//! yet, and what it does with them once the config changes. Pure: adapters run the effects.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

use super::snapshot::{Installed, install};
use super::{DistributionError, DistributionView, PublisherCutoff, SignedSnapshot};
use crate::NodeId;
use crate::admin_documents::{AdminDocumentEvent, AdminDocumentOperation};
use crate::structs::identity::realm::{RealmConfigDocument, RealmId};

/// At most this many publisher and set pairs wait for their publisher's membership.
pub const MAX_PARKED: usize = 256;

/// The highest parked snapshot of one publisher and set, with the same merge rules as an
/// installed view: a different snapshot of the same number is kept as conflict evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ParkedEntry {
    pub head: AdminDocumentEvent,
    pub conflict: Option<AdminDocumentEvent>,
}

impl ParkedEntry {
    pub fn number(&self) -> u64 {
        snapshot_of(&self.head).map_or(0, |signed| signed.snapshot.number)
    }
}

/// What to do with an incoming parked snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParkDecision {
    Store(Box<ParkedEntry>),
    Unchanged,
    /// The bound is reached; the snapshot must be revisited later, never dropped.
    Full,
}

/// Whether the realm config lets a snapshot's publisher publish it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Authority {
    Current,
    Unknown,
    Ended(&'static str),
}

/// What to do with a parked entry under the current realm config.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetryDecision {
    /// Install these events in order, then drop the entry; a conflict installs after its head.
    Install(Vec<AdminDocumentEvent>),
    Drop(&'static str),
    Wait,
}

pub fn snapshot_of(event: &AdminDocumentEvent) -> Option<&SignedSnapshot> {
    match &event.op {
        AdminDocumentOperation::DistributionPublished { snapshot } => Some(snapshot),
        _ => None,
    }
}

/// Merges an incoming verified snapshot into the parked entry of its publisher and set.
pub fn park(
    existing: Option<&ParkedEntry>,
    parked: usize,
    incoming: AdminDocumentEvent,
) -> Result<ParkDecision, DistributionError> {
    let signed = snapshot_of(&incoming)
        .cloned()
        .ok_or(DistributionError::Malformed("parked event is no snapshot"))?;
    let Some(existing) = existing else {
        if parked >= MAX_PARKED {
            return Ok(ParkDecision::Full);
        }
        return Ok(ParkDecision::Store(Box::new(ParkedEntry {
            head: incoming,
            conflict: None,
        })));
    };
    let view = DistributionView {
        head: snapshot_of(&existing.head)
            .cloned()
            .ok_or(DistributionError::Malformed("parked event is no snapshot"))?,
        conflict: existing.conflict.as_ref().and_then(snapshot_of).cloned(),
    };
    let Installed::Changed(next) = install(Some(&view), signed)? else {
        return Ok(ParkDecision::Unchanged);
    };
    // Map the merged view back to the events that carried its snapshots.
    let candidates = [
        Some(&existing.head),
        existing.conflict.as_ref(),
        Some(&incoming),
    ];
    let event_of = |wanted: &SignedSnapshot| {
        candidates
            .into_iter()
            .flatten()
            .find(|event| snapshot_of(event) == Some(wanted))
            .cloned()
    };
    let head = event_of(&next.head).ok_or(DistributionError::Malformed("parked head lost"))?;
    let conflict = next.conflict.as_ref().and_then(event_of);
    Ok(ParkDecision::Store(Box::new(ParkedEntry {
        head,
        conflict,
    })))
}

/// The publisher's authority from the realm config and, if the config removed the node, the
/// cutoffs its removal recorded: after removal only the exact cutoff snapshot holds.
pub fn publisher_authority(
    realm_id: RealmId,
    config: Option<&RealmConfigDocument>,
    removed: Option<&[PublisherCutoff]>,
    signed: &SignedSnapshot,
) -> Authority {
    let Some(config) = config else {
        return Authority::Unknown;
    };
    if config.realm_id != realm_id {
        return Authority::Ended("stored realm config names another realm");
    }
    let publisher: NodeId = signed.snapshot.root.publisher;
    let publisher = publisher.to_string();
    if let Some(node) = config.nodes.iter().find(|node| node.node_id == publisher) {
        return match node.kind.is_sync_eligible() {
            true => Authority::Current,
            false => Authority::Ended("distribution publisher cannot publish"),
        };
    }
    let Some(cutoffs) = removed else {
        return Authority::Unknown;
    };
    let revision = signed.snapshot.revision();
    let exact = cutoffs
        .iter()
        .any(|cutoff| cutoff.set_id == signed.snapshot.set_id() && cutoff.revision == revision);
    match exact {
        true => Authority::Current,
        false => Authority::Ended("snapshot is not the removed publisher's cutoff"),
    }
}

/// Retries a parked entry: it waits while its publisher is unknown, otherwise every event
/// with authority installs, head first, and the entry is dropped.
pub fn retry(
    entry: &ParkedEntry,
    authority_of: impl Fn(&SignedSnapshot) -> Authority,
) -> RetryDecision {
    let Some(head) = snapshot_of(&entry.head) else {
        return RetryDecision::Drop("parked event is no snapshot");
    };
    let head_authority = authority_of(head);
    if head_authority == Authority::Unknown {
        return RetryDecision::Wait;
    }
    let events: Vec<AdminDocumentEvent> = std::iter::once(&entry.head)
        .chain(&entry.conflict)
        .filter(|event| {
            snapshot_of(event).is_some_and(|signed| authority_of(signed) == Authority::Current)
        })
        .cloned()
        .collect();
    match (events.is_empty(), head_authority) {
        (true, Authority::Ended(reason)) => RetryDecision::Drop(reason),
        (true, _) => RetryDecision::Drop("no parked snapshot has authority"),
        (false, _) => RetryDecision::Install(events),
    }
}

#[cfg(test)]
#[path = "parking_tests.rs"]
mod tests;
