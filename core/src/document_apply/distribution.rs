//! Admits and stores distribution snapshots: only the set's bound publisher publishes them, every
//! embedded statement carries its member's own signature, and the signed form is kept. Snapshots
//! whose publisher the realm config does not know yet wait parked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use byteview::ByteView;
use tracing::warn;
use ulid::Ulid;

use super::reads::{realm_config, reducer_state};
use super::registry::Row;
use super::{ApplyError, Step, invalid};
use crate::NodeId;
use crate::admin_documents::{AdminDocumentEvent, AdminDocumentOperation, AdminDocumentTarget};
use crate::config_authority::AdminEventValidation;
use crate::document::DocumentTarget;
use crate::keyspaces::{
    CATCH_UP_KEYSPACE, DISTRIBUTION_KEYSPACE, DISTRIBUTION_LIFECYCLE_KEYSPACE,
    DISTRIBUTION_PARKED_KEYSPACE, DISTRIBUTION_PUBLISHER_KEYSPACE, NODE_STATS_KEYSPACE,
    PARKED_INDEX_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use crate::reducer::{AdminApplyStatus, AdminDocumentState, config_node_path};
use crate::row_plan::{NeedsRows, PlanRows, PlanStop, Planned, RowPlan};
use crate::storage_entries::reducer_state_entry;
use crate::structs::identity::realm::{RealmConfigDocument, RealmId};
use crate::structs::storage::blob::BucketInfo;
use crate::structs::storage::distribution::parking::{
    Authority, MAX_PARKED, ParkDecision, ParkedEntry, RetryDecision, park, publisher_authority,
    retry, snapshot_of,
};
use crate::structs::storage::distribution::snapshot::forked;
use crate::structs::storage::distribution::{PublisherCutoff, SignedSnapshot, publisher_key};
use crate::structs::storage::usage::dirty_share_key;
use crate::types::{Key, KeySpace, Value};

/// Lost conflicts a distribution change reruns; a transient conflict must not wedge the topic.
const CONFLICT_RETRIES: u32 = 63;

/// Whether a snapshot was handled, or must be revisited because the parking bound is reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Handled {
    Done,
    Full,
}

/// Committed revisions carry the publisher's acceptance, so no actor's current role is rechecked.
/// A publisher the config does not know yet is parked; a removed one keeps authority only for the
/// exact cutoff snapshot its removal recorded for the set.
pub fn validate_distribution_authority(
    rows: &mut PlanRows,
    event: &AdminDocumentEvent,
) -> Step<AdminEventValidation> {
    let (
        AdminDocumentTarget::Distribution { realm_id, .. },
        AdminDocumentOperation::DistributionPublished { snapshot },
    ) = (&event.target, &event.op)
    else {
        return Ok(AdminEventValidation::Rejected(
            "admin event is not a distribution snapshot".to_string(),
        ));
    };
    Ok(
        match Membership::read(rows, *realm_id)?.authority(snapshot) {
            Authority::Current => AdminEventValidation::Accepted,
            Authority::Ended(reason) => AdminEventValidation::Rejected(reason.to_string()),
            // Parked by the apply step, so the topic cursor moves on past a membership gap.
            Authority::Unknown => AdminEventValidation::Accepted,
        },
    )
}

/// The realm config and its removal cutoffs, read once for any number of authority decisions.
struct Membership {
    realm_id: RealmId,
    config: Option<RealmConfigDocument>,
    state: Option<AdminDocumentState>,
}

impl Membership {
    fn read(rows: &mut PlanRows, realm_id: RealmId) -> Step<Self> {
        Ok(Self {
            realm_id,
            config: realm_config(rows, realm_id)?,
            state: reducer_state(rows, &AdminDocumentTarget::RealmConfig { realm_id })?,
        })
    }

    /// The cutoffs of a node the realm config once listed and then removed.
    fn removed(&self, node_id: &NodeId) -> Option<Vec<PublisherCutoff>> {
        let state = self.state.as_ref()?;
        let path = config_node_path(node_id);
        let removed = state
            .user_subject_ids
            .get(&path)
            .is_some_and(|version| version.value.is_none())
            || state.conflicts.contains_key(&path);
        removed.then(|| state.publisher_cutoffs(node_id).unwrap_or_default())
    }

    fn authority(&self, signed: &SignedSnapshot) -> Authority {
        let removed = self.removed(&signed.snapshot.root.publisher);
        publisher_authority(
            self.realm_id,
            self.config.as_ref(),
            removed.as_deref(),
            signed,
        )
    }
}

/// The reduced state of one set while a plan folds snapshots into it.
struct SetFold {
    state: AdminDocumentState,
    applied: bool,
    last_event: Ulid,
}

/// The sets one plan changes.
#[derive(Default)]
struct SetFolds {
    sets: BTreeMap<Ulid, SetFold>,
}

impl SetFolds {
    /// Reduces events of one set; a head with its conflict keeps the equivocation.
    fn fold(
        &mut self,
        rows: &mut PlanRows,
        events: &[AdminDocumentEvent],
        set_id: Ulid,
    ) -> Step<()> {
        let Some(first) = events.first() else {
            return Ok(());
        };
        if !self.sets.contains_key(&set_id) {
            let state = reducer_state(rows, &first.target)?
                .unwrap_or_else(|| AdminDocumentState::new(first.target.clone()));
            self.sets.insert(
                set_id,
                SetFold {
                    state,
                    applied: false,
                    last_event: first.event_id,
                },
            );
        }
        let Some(fold) = self.sets.get_mut(&set_id) else {
            return Ok(());
        };
        for event in events {
            let status = fold.state.apply(event).map_err(invalid)?;
            fold.applied |= status == AdminApplyStatus::Applied;
            fold.last_event = event.event_id;
        }
        if let [head, conflict] = events {
            let status = fold
                .state
                .keep_equivocation(head, conflict)
                .map_err(invalid)?;
            fold.applied |= status == AdminApplyStatus::Applied;
        }
        Ok(())
    }

    /// The rows of every changed set; a set that needs its bucket does not stop the others.
    fn writes(&self, rows: &mut PlanRows) -> Step<Vec<Row>> {
        let mut writes = Vec::new();
        let mut waiting = false;
        for (set_id, fold) in &self.sets {
            match set_writes(rows, *set_id, fold) {
                Ok(set) => writes.extend(set),
                Err(PlanStop::NeedsRows) => waiting = true,
                Err(stop) => return Err(stop),
            }
        }
        if waiting {
            return Err(NeedsRows.into());
        }
        Ok(writes)
    }
}

fn set_writes(rows: &mut PlanRows, set_id: Ulid, fold: &SetFold) -> Step<Vec<Row>> {
    if !fold.applied {
        return Ok(Vec::new());
    }
    let Some(view) = fold.state.distribution_view() else {
        return Ok(Vec::new());
    };
    let bytes = view.to_bytes().map_err(invalid)?;
    let head = view.snapshot();
    let revision = postcard::to_allocvec(&head.revision()).map_err(invalid)?;
    let set_key = || Key::from(set_id.to_bytes().to_vec());
    // Coordinators find the sets whose catching-up members or new copies they walk here.
    let catch_up = head.bootstrap_open().then(|| {
        (
            CATCH_UP_KEYSPACE.to_string(),
            set_key(),
            Value::from(Vec::new()),
        )
    });
    // Nodes find the sets whose transfers, deletion or departures they act on through this row.
    let lifecycle = head.lifecycle_open().then(|| {
        (
            DISTRIBUTION_LIFECYCLE_KEYSPACE.to_string(),
            set_key(),
            Value::from(Vec::new()),
        )
    });
    // A member's replica share may shrink with the record: the gate stops deducting it now.
    let bucket = rows
        .read(S3_BUCKET_KEYSPACE, head.config.bucket.as_bytes().to_vec())?
        .as_deref()
        .map(BucketInfo::from_bytes)
        .transpose()
        .map_err(invalid)?
        .filter(|info| {
            info.distribution
                .as_ref()
                .is_some_and(|bound| bound.set_id == set_id)
        });
    let share = bucket.map(|info| {
        (
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(dirty_share_key(info.group_id)),
            Value::from(fold.last_event.to_bytes().to_vec()),
        )
    });
    Ok([
        (
            DISTRIBUTION_PUBLISHER_KEYSPACE.to_string(),
            Key::from(publisher_key(&head.root.publisher, set_id)),
            revision.into(),
        ),
        reducer_state_entry(&fold.state).map_err(invalid)?,
        (DISTRIBUTION_KEYSPACE.to_string(), set_key(), bytes.into()),
    ]
    .into_iter()
    .chain(catch_up)
    .chain(lifecycle)
    .chain(share)
    .collect())
}

fn parked_entry(rows: &mut PlanRows, key: &ByteView) -> Step<Option<ParkedEntry>> {
    Ok(rows
        .read(DISTRIBUTION_PARKED_KEYSPACE, key.clone())?
        .and_then(|bytes| postcard::from_bytes::<ParkedEntry>(&bytes).ok()))
}

fn unpark_rows(key: ByteView) -> Vec<(KeySpace, Key)> {
    vec![
        (PARKED_INDEX_KEYSPACE.to_string(), key.clone()),
        (DISTRIBUTION_PARKED_KEYSPACE.to_string(), key),
    ]
}

fn parked_keys(rows: &mut PlanRows) -> Step<Vec<ByteView>> {
    let index = rows.scan_limited(PARKED_INDEX_KEYSPACE, None, MAX_PARKED)?;
    Ok(index.into_iter().map(|(key, _)| key).collect())
}

/// Reduces a snapshot into its set's state and stores the signed view every node reads. Parked
/// evidence of the same publisher and set installs first, so a newer head cannot hide it.
#[derive(Debug, PartialEq)]
pub struct DistributionPlan {
    pub target: DocumentTarget,
    pub event: AdminDocumentEvent,
}

impl DistributionPlan {
    fn park(&self, rows: &mut PlanRows, signed: &SignedSnapshot) -> Step<Planned<Handled>> {
        let snapshot = &signed.snapshot;
        let key: ByteView = publisher_key(&snapshot.root.publisher, snapshot.set_id()).into();
        let existing = parked_entry(rows, &key)?;
        let count = parked_keys(rows)?.len();
        let entry = match park(existing.as_ref(), count, self.event.clone()).map_err(invalid)? {
            ParkDecision::Store(entry) => entry,
            ParkDecision::Unchanged => return Ok(Planned::output(Handled::Done)),
            ParkDecision::Full => {
                warn!(set_id = %snapshot.set_id(), "Parked distribution snapshots reached their bound");
                return Ok(Planned::output(Handled::Full));
            }
        };
        let body = postcard::to_allocvec(&*entry).map_err(invalid)?;
        let writes = vec![
            (
                PARKED_INDEX_KEYSPACE.to_string(),
                key.clone(),
                entry.number().to_be_bytes().to_vec().into(),
            ),
            (DISTRIBUTION_PARKED_KEYSPACE.to_string(), key, body.into()),
        ];
        Ok(Planned::changes(Handled::Done, Vec::new(), writes))
    }

    fn install(
        &self,
        rows: &mut PlanRows,
        membership: &Membership,
        signed: &SignedSnapshot,
        set_id: Ulid,
    ) -> Step<Planned<Handled>> {
        let snapshot = &signed.snapshot;
        let key: ByteView = publisher_key(&snapshot.root.publisher, snapshot.set_id()).into();
        let mut folds = SetFolds::default();
        let mut deletes = Vec::new();
        if let Some(entry) = parked_entry(rows, &key)? {
            match retry(&entry, |parked| membership.authority(parked)) {
                RetryDecision::Wait => {}
                RetryDecision::Drop(reason) => {
                    warn!(%set_id, %reason, "Dropping a parked distribution snapshot");
                    deletes = unpark_rows(key);
                }
                RetryDecision::Install(events) => {
                    folds.fold(rows, &events, set_id)?;
                    deletes = unpark_rows(key);
                }
            }
        }
        folds.fold(rows, std::slice::from_ref(&self.event), set_id)?;
        let writes = folds.writes(rows)?;
        Ok(Planned::changes(Handled::Done, deletes, writes))
    }
}

impl RowPlan for DistributionPlan {
    type Output = Handled;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<Handled>> {
        let event = &self.event;
        let (
            DocumentTarget::RealmConfig { realm_id },
            AdminDocumentTarget::Distribution {
                realm_id: event_realm_id,
                set_id,
            },
        ) = (&self.target, &event.target)
        else {
            return Err(ApplyError::Invalid(
                "distribution snapshots ride the realm config document".to_string(),
            )
            .into());
        };
        if realm_id != event_realm_id {
            return Err(ApplyError::Invalid(
                "distribution snapshot realm does not match its document".to_string(),
            )
            .into());
        }
        let AdminDocumentOperation::DistributionPublished { snapshot } = &event.op else {
            return Err(ApplyError::Invalid(
                "distribution records change only through published snapshots".to_string(),
            )
            .into());
        };
        let membership = Membership::read(rows, *realm_id)?;
        match membership.authority(snapshot) {
            Authority::Current => self.install(rows, &membership, snapshot, *set_id),
            Authority::Unknown => self.park(rows, snapshot),
            Authority::Ended(reason) => {
                warn!(%set_id, %reason, "Dropping a distribution snapshot without authority");
                Ok(Planned::output(Handled::Done))
            }
        }
    }

    fn conflict_retries() -> u32 {
        CONFLICT_RETRIES
    }
}

/// Retries every parked entry of the realm against one read of the realm config.
#[derive(Debug, PartialEq)]
pub struct RetryParkedPlan {
    pub realm_id: RealmId,
}

impl RetryParkedPlan {
    /// The events to install and whether the entry leaves the parking place.
    fn decide(
        &self,
        rows: &mut PlanRows,
        membership: &Membership,
        key: &ByteView,
        folds: &mut SetFolds,
        drops: &mut Vec<(Ulid, String)>,
    ) -> Step<bool> {
        let Some(entry) = parked_entry(rows, key)? else {
            return Ok(true);
        };
        let AdminDocumentTarget::Distribution {
            realm_id: event_realm,
            set_id,
        } = entry.head.target
        else {
            return Ok(true);
        };
        if event_realm != self.realm_id {
            return Ok(false);
        }
        match retry(&entry, |signed| membership.authority(signed)) {
            RetryDecision::Wait => Ok(false),
            RetryDecision::Drop(reason) => {
                drops.push((set_id, reason.to_string()));
                Ok(true)
            }
            RetryDecision::Install(events) => {
                // Head and conflict install together, so the view keeps the equivocation.
                folds.fold(rows, &events, set_id)?;
                Ok(true)
            }
        }
    }
}

impl RowPlan for RetryParkedPlan {
    type Output = ();
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<()>> {
        let membership = Membership::read(rows, self.realm_id)?;
        let keys = parked_keys(rows)?;
        for key in &keys {
            rows.want(DISTRIBUTION_PARKED_KEYSPACE, key.clone());
        }
        let mut folds = SetFolds::default();
        let mut drops = Vec::new();
        let mut deletes = Vec::new();
        let mut waiting = false;
        for key in keys {
            match self.decide(rows, &membership, &key, &mut folds, &mut drops) {
                Ok(true) => deletes.extend(unpark_rows(key)),
                Ok(false) => {}
                Err(PlanStop::NeedsRows) => waiting = true,
                Err(stop) => return Err(stop),
            }
        }
        if waiting {
            return Err(NeedsRows.into());
        }
        let writes = folds.writes(rows)?;
        for (set_id, reason) in drops {
            warn!(%set_id, %reason, "Dropping a parked distribution snapshot");
        }
        Ok(Planned::changes((), deletes, writes))
    }

    fn conflict_retries() -> u32 {
        CONFLICT_RETRIES
    }
}

/// Installs a replayed snapshot together with the last replayed one of its set when the two prove
/// equivocation, so a newer head cannot hide the fork. The output tells whether it did.
#[derive(Debug, PartialEq)]
pub struct ReplayForkPlan {
    pub earlier: AdminDocumentEvent,
    pub event: AdminDocumentEvent,
}

impl RowPlan for ReplayForkPlan {
    type Output = bool;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<bool>> {
        let (AdminDocumentTarget::Distribution { realm_id, set_id }, Some(first), Some(second)) = (
            &self.event.target,
            snapshot_of(&self.earlier),
            snapshot_of(&self.event),
        ) else {
            return Ok(Planned::output(false));
        };
        if !forked(&first.snapshot, &second.snapshot) {
            return Ok(Planned::output(false));
        }
        let membership = Membership::read(rows, *realm_id)?;
        let current = |signed: &SignedSnapshot| membership.authority(signed) == Authority::Current;
        if !current(first) || !current(second) {
            return Ok(Planned::output(false));
        }
        let mut folds = SetFolds::default();
        let pair = [self.earlier.clone(), self.event.clone()];
        folds.fold(rows, &pair, *set_id)?;
        let writes = folds.writes(rows)?;
        Ok(Planned::changes(true, Vec::new(), writes))
    }

    fn conflict_retries() -> u32 {
        CONFLICT_RETRIES
    }
}
