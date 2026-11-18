//! Admits replicated metadata create and update events, one by one or as one reconcile batch.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet};

use byteview::ByteView;
use ulid::Ulid;

use super::quarantine::quarantine_entries;
use super::registry::{
    Row, accepted_create, create_fence, event_logged, lifecycle_stale, placement_fence,
    revision_entries, same_create_event, validate_metadata_event,
};
use super::{ApplyError, PlacementOutcome, Step, invalid};
use crate::deferred_topics::DocumentSyncDependency;
use crate::document::{DocumentChange, DocumentEvent, DocumentTarget};
use crate::metadata::MetadataEventRecord;
use crate::row_plan::{NeedsRows, PlanRows, PlanStop, Planned, RowPlan};
use crate::storage_entries::{
    commit_message_entry, create_acceptance_entry, create_projection_entries,
};
use crate::structs::{SyncQuarantineIdentity, SyncRejection};

/// One replicated create or update waiting for the batch's single transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingCreateApply {
    pub identity: SyncQuarantineIdentity,
    /// The event exactly as received, so a reject keeps the genuine envelope.
    pub event: DocumentEvent,
    pub target: DocumentTarget,
    pub record: MetadataEventRecord,
    pub bytes: Vec<u8>,
    pub lifecycle_revision: Option<DocumentChange>,
    /// The author's commit message for the event's ARC snapshot.
    pub message: Option<String>,
}

/// Stores one create event unless a delete or graph tombstone fences it out.
#[derive(Debug, PartialEq)]
pub struct CreateEventPlan {
    pub event: MetadataEventRecord,
    pub target: DocumentTarget,
    pub bytes: Vec<u8>,
}

impl RowPlan for CreateEventPlan {
    type Output = ();
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<()>> {
        if create_fence(rows, &self.event)? {
            return Ok(Planned::output(()));
        }
        let writes = vec![(
            self.target.storage_keyspace().to_string(),
            self.target.storage_key(),
            ByteView::from(self.bytes.clone()),
        )];
        Ok(Planned::changes((), Vec::new(), writes))
    }
}

/// Something a batch run wants the operator to see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateNote {
    pub topic: irokle::TopicId,
    pub document_id: Ulid,
    pub message: String,
}

/// What one create batch did.
#[derive(Debug, Default, PartialEq)]
pub struct CreateBatchOutcome {
    pub accepted: Vec<(DocumentTarget, MetadataEventRecord)>,
    /// Topics whose events wait for a dependency to replicate.
    pub deferred: Vec<(DocumentSyncDependency, irokle::TopicId)>,
    pub notes: Vec<CreateNote>,
}

#[derive(Debug, PartialEq)]
struct Candidate {
    apply: PendingCreateApply,
    revision: Vec<Row>,
    entries: Vec<Row>,
}

/// Admits the pending creates of a reconcile pass and writes the cursors that waited for them,
/// with the evidence of everything rejected, in one transaction.
#[derive(Debug, PartialEq)]
pub struct CreateBatchPlan {
    candidates: Vec<Candidate>,
    cursor_writes: Vec<(irokle::TopicId, Row)>,
    rejections: Vec<SyncRejection>,
    notes: Vec<CreateNote>,
}

impl CreateBatchPlan {
    /// Checks each pending event on its own; one with a broken identity is rejected at once.
    pub fn new(
        pending: Vec<PendingCreateApply>,
        cursor_writes: Vec<(irokle::TopicId, Row)>,
        mut rejections: Vec<SyncRejection>,
    ) -> Result<Self, ApplyError> {
        let mut candidates = Vec::with_capacity(pending.len());
        let mut notes = Vec::new();
        for apply in pending {
            if let Err(error) = validate_metadata_event(&apply.record) {
                notes.push(note(
                    &apply,
                    "Rejecting replicated metadata event with inconsistent identity",
                ));
                rejections.push(SyncRejection::new(
                    apply.identity,
                    apply.event,
                    format!("replicated metadata event has an inconsistent identity: {error}"),
                ));
                continue;
            }
            candidates.push(candidate(apply)?);
        }
        Ok(Self {
            candidates,
            cursor_writes,
            rejections,
            notes,
        })
    }

    fn is_empty(&self) -> bool {
        self.candidates.is_empty() && self.cursor_writes.is_empty() && self.rejections.is_empty()
    }
}

fn note(apply: &PendingCreateApply, message: &str) -> CreateNote {
    CreateNote {
        topic: apply.identity.topic,
        document_id: apply.record.record.document_id,
        message: message.to_string(),
    }
}

fn candidate(apply: PendingCreateApply) -> Result<Candidate, ApplyError> {
    let revision = match &apply.lifecycle_revision {
        Some(change) => revision_rows(&apply.target, change)?,
        None => Vec::new(),
    };
    let mut entries = create_projection_entries(&apply.record).map_err(invalid)?;
    if let Some((_, _, value)) = entries.first_mut() {
        *value = ByteView::from(apply.bytes.clone());
    }
    if let Some(message) = &apply.message {
        entries.push(commit_message_entry(&apply.record, message).map_err(invalid)?);
    }
    if apply.record.is_create() {
        entries.push(create_acceptance_entry(&apply.record).map_err(invalid)?);
    }
    Ok(Candidate {
        apply,
        revision,
        entries,
    })
}

fn revision_rows(target: &DocumentTarget, change: &DocumentChange) -> Result<Vec<Row>, ApplyError> {
    revision_entries(target, change).map_err(|stop| match stop {
        PlanStop::Failed(error) => error,
        PlanStop::NeedsRows => ApplyError::Failed("revision rows read storage".to_string()),
    })
}

/// What the batch does with one candidate.
enum Verdict {
    Accept { with_revision: bool },
    Skip,
    Defer(Option<DocumentSyncDependency>, &'static str),
    Reject(&'static str, &'static str),
}

#[derive(Default)]
struct BatchRun {
    acceptances: BTreeMap<Ulid, MetadataEventRecord>,
}

impl BatchRun {
    fn accepted(
        &mut self,
        rows: &mut PlanRows,
        document_id: Ulid,
    ) -> Step<Option<MetadataEventRecord>> {
        if let Some(event) = self.acceptances.get(&document_id) {
            return Ok(Some(event.clone()));
        }
        let event = accepted_create(rows, document_id)?;
        if let Some(event) = &event {
            if !event.is_create() {
                return Err(ApplyError::Invalid(
                    "metadata create acceptance contains a non-create event".to_string(),
                )
                .into());
            }
            validate_metadata_event(event)?;
            self.acceptances.insert(document_id, event.clone());
        }
        Ok(event)
    }

    fn check(&mut self, rows: &mut PlanRows, apply: &PendingCreateApply) -> Step<Verdict> {
        if create_fence(rows, &apply.record)? {
            return Ok(Verdict::Skip);
        }
        let mut with_revision = false;
        if let Some(revision) = &apply.lifecycle_revision {
            // One lifecycle target carries every event, so an older concurrent event still
            // belongs in the log; only the lifecycle revision must not regress.
            if !lifecycle_stale(rows, &apply.target, revision)? {
                with_revision = true;
            } else if event_logged(rows, &apply.record)? {
                return Ok(Verdict::Skip);
            }
        }
        match placement_fence(rows, &apply.record.record)? {
            PlacementOutcome::Accepted(()) => {}
            PlacementOutcome::Deferred(dependency) => {
                return Ok(Verdict::Defer(
                    Some(dependency),
                    "Deferring replicated metadata create until its placement strategy is available",
                ));
            }
            PlacementOutcome::Rejected => {
                return Ok(Verdict::Reject(
                    "replicated metadata create has a mismatched placement configuration",
                    "Rejecting replicated metadata create with mismatched placement configuration",
                ));
            }
        }
        let document_id = apply.record.record.document_id;
        let accepted = self.accepted(rows, document_id)?;
        if apply.record.is_create() {
            if accepted
                .as_ref()
                .is_some_and(|accepted| !same_create_event(accepted, &apply.record))
            {
                return Ok(Verdict::Reject(
                    "divergent replicated metadata create",
                    "Rejecting divergent replicated metadata create",
                ));
            }
            self.acceptances
                .entry(document_id)
                .or_insert_with(|| apply.record.clone());
        } else if accepted.is_none() {
            return Ok(Verdict::Defer(
                None,
                "Deferring replicated metadata update until its create is accepted",
            ));
        } else if accepted
            .as_ref()
            .is_some_and(|accepted| !accepted.record.same_identity(&apply.record.record))
        {
            return Ok(Verdict::Reject(
                "replicated metadata update has a mismatched accepted create",
                "Rejecting replicated metadata update with mismatched accepted create",
            ));
        }
        Ok(Verdict::Accept { with_revision })
    }
}

impl RowPlan for CreateBatchPlan {
    type Output = CreateBatchOutcome;
    type Error = ApplyError;

    fn plan(&self, rows: &mut PlanRows) -> Step<Planned<CreateBatchOutcome>> {
        let mut outcome = CreateBatchOutcome {
            notes: self.notes.clone(),
            ..CreateBatchOutcome::default()
        };
        if self.is_empty() {
            return Ok(Planned::output(outcome));
        }
        let mut run = BatchRun::default();
        let mut rejections = self.rejections.clone();
        let mut writes = Vec::new();
        let mut held = BTreeSet::new();
        let mut waiting = false;
        for candidate in &self.candidates {
            let apply = &candidate.apply;
            // A candidate that needs rows does not stop the others, so one read serves them all.
            let verdict = match run.check(rows, apply) {
                Ok(verdict) => verdict,
                Err(PlanStop::NeedsRows) => {
                    waiting = true;
                    continue;
                }
                Err(stop) => return Err(stop),
            };
            match verdict {
                Verdict::Accept { with_revision } => {
                    writes.extend(candidate.entries.iter().cloned());
                    if with_revision {
                        writes.extend(candidate.revision.iter().cloned());
                    }
                    outcome
                        .accepted
                        .push((apply.target.clone(), apply.record.clone()));
                }
                Verdict::Skip => {}
                Verdict::Defer(dependency, message) => {
                    outcome.notes.push(note(apply, message));
                    held.insert(apply.identity.topic);
                    if let Some(dependency) = dependency {
                        outcome.deferred.push((dependency, apply.identity.topic));
                    }
                }
                Verdict::Reject(reason, message) => {
                    outcome.notes.push(note(apply, message));
                    rejections.push(SyncRejection::new(
                        apply.identity,
                        apply.event.clone(),
                        reason,
                    ));
                }
            }
        }
        if waiting {
            return Err(NeedsRows.into());
        }
        match quarantine_entries(rows, &rejections)? {
            Some(entries) => writes.extend(entries),
            // Fail closed: no topic may advance past evidence that does not fit.
            None => held.extend(rejections.iter().map(SyncRejection::topic_id)),
        }
        writes.extend(
            self.cursor_writes
                .iter()
                .filter(|(topic, _)| !held.contains(topic))
                .map(|(_, write)| write.clone()),
        );
        Ok(Planned::changes(outcome, Vec::new(), writes))
    }
}
