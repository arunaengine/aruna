//! Decides one due family of the prune index from this node's rows, and enters or continues its
//! deletion, or moves its index row to the time the decision can change.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::keyspaces::{
    DEADLINE_INDEX_KEYSPACE, FAMILY_ALIAS_KEYSPACE, FAMILY_OUTBOX_KEYSPACE,
    FAMILY_PENDING_KEYSPACE, FAMILY_PROJECTION_KEYSPACE, FAMILY_PRUNE_KEYSPACE,
    FAMILY_TOMBSTONE_KEYSPACE, JOB_KEYSPACE, JOB_RESERVATION_KEYSPACE, WITNESS_DEADLINE_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::record_keys::{
    alias_key, family_prefix, prune_due_key, prune_rows_at, prune_slot_key,
};
use aruna_core::structs::execution::job::record_rows::{PendingRecord, from_bytes, to_bytes};
use aruna_core::structs::execution::job::{
    CLOCK_MARGIN_MS, ExpiryReason, FamilyTombstone, HoldReason, JobDeletes, JobFamilyId,
    JobFamilyRecord, JobRecordEnvelope, JobWrites, LogicalJobSpec, LogicalJobState, NotRun,
    PruneInput, PruneVerdict, RETENTION_MS, TOMBSTONE_RETENTION_MS, job_record_key, last_record_ms,
    pending_expired, prune_verdict,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::types::{Key, Value};
use tracing::warn;
use ulid::Ulid;

use super::pages::PrunePagesOperation;
use super::{
    BODY_SPACES, FAMILY_PRUNE_PAGE, PruneCommitOperation, PruneError, PruneStep, index_rows,
    slot_of,
};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::cancel::PublishCancelOperation;
use crate::jobs::lifecycle::closure::CloseLaunchesOperation;
use crate::jobs::lifecycle::membership::{MembershipViewOperation, family_not_run};
use crate::jobs::lifecycle::reservation::reservation_key;
use crate::jobs::lifecycle::witness_arm::{WitnessDeadline, deadline_index_key, deadline_key};
use crate::jobs::records::admit::MAX_PENDING_RECORDS;
use crate::jobs::records::family::{FamilyReadError, LoadFamilyOperation};
use crate::jobs::records::reduce::reduce_family;
use crate::jobs::route::JobRouteError;
use crate::secrets::owner::WriterView;

#[derive(Debug, PartialEq)]
enum FamilyStep {
    Rows,
    Pages,
    Body(usize),
    Records,
    Pending,
    Closures,
    View,
    Local,
    Outbox,
    Deadline,
    Commit(PruneStep),
    /// The expiry cancel of an undecided family, published after its row moved.
    Expiry(PruneStep),
}

/// One due row of one family: drop it when stale, continue an unfinished deletion, or decide.
#[derive(Debug, PartialEq)]
pub struct PruneFamilyOperation {
    family: JobFamilyId,
    due_ms: u64,
    serving: Option<(NodeId, RealmId)>,
    now_ms: u64,
    tombstone: Option<FamilyTombstone>,
    body: JobDeletes,
    records: Vec<JobRecordEnvelope>,
    truncated: bool,
    /// Pending rows past the horizon, and the earliest first sight of the others.
    expired: JobDeletes,
    earliest_ms: Option<u64>,
    closed: BTreeSet<Ulid>,
    not_run: NotRun,
    local_work: bool,
    /// The spec of a family that expired here, cancelled once its index row moved.
    expiring: Option<Box<LogicalJobSpec>>,
    step: FamilyStep,
}

type FamilyNext = Result<Next<PruneStep>, PruneError>;

impl PruneFamilyOperation {
    pub fn new(family: JobFamilyId, due_ms: u64, serving: Option<(NodeId, RealmId)>) -> Self {
        Self {
            family,
            due_ms,
            serving,
            now_ms: 0,
            tombstone: None,
            body: Vec::new(),
            records: Vec::new(),
            truncated: false,
            expired: Vec::new(),
            earliest_ms: None,
            closed: BTreeSet::new(),
            not_run: NotRun::default(),
            local_work: false,
            expiring: None,
            step: FamilyStep::Rows,
        }
    }

    fn commit(
        &mut self,
        outcome: PruneStep,
        guarded: bool,
        rows: (JobDeletes, JobWrites),
    ) -> FamilyNext {
        let guard = guarded.then_some((self.family, self.due_ms));
        self.step = FamilyStep::Commit(outcome);
        let commit = PruneCommitOperation::new(guard, rows.0, rows.1);
        Ok(Next::Call(IoCall::child(commit)))
    }

    fn page(&mut self, step: FamilyStep, space: &str, limit: usize) -> FamilyNext {
        self.step = step;
        Ok(Next::Call(IoCall::Page {
            space: space.to_string(),
            prefix: family_prefix(&self.family),
            after: None,
            limit,
        }))
    }

    /// Step 3: one page of body rows; the last page marks the tombstone finished and moves the
    /// index row to the tombstone's expiry.
    fn body_page(&mut self, space: usize) -> FamilyNext {
        let room = FAMILY_PRUNE_PAGE - self.body.len();
        if let Some(name) = BODY_SPACES.get(space).filter(|_| room > 0) {
            return self.page(FamilyStep::Body(space), name, room);
        }
        let body = std::mem::take(&mut self.body);
        if !body.is_empty() {
            return self.commit(PruneStep::Paged, true, (body, Vec::new()));
        }
        let tombstone = self.tombstone.take().ok_or(IoError::Unfinished)?;
        let finished = FamilyTombstone {
            finished: true,
            ..tombstone
        };
        let value = Value::from(to_bytes(&finished)?.as_slice());
        let mut writes = prune_rows_at(&self.family, finished.expires_at_ms()).to_vec();
        writes.push((
            FAMILY_TOMBSTONE_KEYSPACE.to_string(),
            family_prefix(&self.family),
            value,
        ));
        let deletes = vec![(
            FAMILY_PRUNE_KEYSPACE.to_string(),
            prune_due_key(self.due_ms, &self.family),
        )];
        self.commit(PruneStep::Pruned, true, (deletes, writes))
    }

    fn pending_read(&mut self, rows: Vec<(Key, Value)>) -> FamilyNext {
        for (key, value) in rows {
            // An unreadable row can never be admitted, so it goes like an expired one.
            let first_seen = from_bytes::<PendingRecord>(&value).map(|row| row.first_seen_ms);
            match first_seen {
                Ok(first_seen) if !pending_expired(first_seen, self.now_ms) => {
                    let earliest = self.earliest_ms.map_or(first_seen, |e| e.min(first_seen));
                    self.earliest_ms = Some(earliest);
                }
                _ => self
                    .expired
                    .push((FAMILY_PENDING_KEYSPACE.to_string(), key)),
            }
        }
        if !self.records.is_empty() || self.truncated {
            // Due launches are asked about first, so an answer counts in this same decision.
            let records = self.records.clone();
            let close = CloseLaunchesOperation::new(self.family, records, self.serving);
            self.step = FamilyStep::Closures;
            return Ok(Next::Call(IoCall::child(close)));
        }
        // A finished tombstone expires with the last pending row; then nothing is left here.
        let expired = std::mem::take(&mut self.expired);
        let expiry = (self.tombstone.as_ref()).map(FamilyTombstone::expires_at_ms);
        let waits = (self.earliest_ms)
            .map(|first_seen_ms| first_seen_ms.saturating_add(TOMBSTONE_RETENTION_MS))
            .into_iter()
            .chain(expiry.filter(|expiry| *expiry > self.now_ms))
            .min();
        match waits {
            None => {
                let mut deletes = expired;
                deletes.extend(index_rows(&self.family, self.due_ms));
                if let Some(tombstone) = self.tombstone.take() {
                    deletes.extend(tombstone_rows(&self.family, &tombstone));
                }
                self.commit(PruneStep::Pruned, true, (deletes, Vec::new()))
            }
            Some(recheck) => self.moved(recheck, expired),
        }
    }

    /// Moves a held family's index row to `recheck_ms` and drops its expired pending rows.
    fn moved(&mut self, recheck_ms: u64, mut deletes: JobDeletes) -> FamilyNext {
        deletes.push((
            FAMILY_PRUNE_KEYSPACE.to_string(),
            prune_due_key(self.due_ms, &self.family),
        ));
        let writes = prune_rows_at(&self.family, recheck_ms).to_vec();
        self.commit(PruneStep::Moved, true, (deletes, writes))
    }

    fn view(&mut self) -> FamilyNext {
        let Some((_, realm_id)) = self.serving else {
            return self.local();
        };
        self.step = FamilyStep::View;
        Ok(Next::Call(IoCall::child(MembershipViewOperation::new(
            realm_id,
        ))))
    }

    /// The job and reservation rows of every execution of the family this node runs.
    fn local(&mut self) -> FamilyNext {
        let local = self.serving.map(|(local, _)| local);
        let rows: Vec<_> = (self.records.iter())
            .filter_map(|envelope| match &envelope.record {
                JobFamilyRecord::Receipt(receipt) if Some(receipt.executor_node_id) == local => {
                    Some(receipt)
                }
                _ => None,
            })
            .flat_map(|receipt| {
                [
                    (
                        JOB_KEYSPACE.to_string(),
                        job_record_key(receipt.physical_job_id),
                    ),
                    (
                        JOB_RESERVATION_KEYSPACE.to_string(),
                        reservation_key(receipt.execution_id),
                    ),
                ]
            })
            .collect();
        if rows.is_empty() {
            return self.page(FamilyStep::Outbox, FAMILY_OUTBOX_KEYSPACE, 1);
        }
        self.step = FamilyStep::Local;
        Ok(Next::Call(IoCall::Read(rows)))
    }

    fn decide(&mut self, undelivered: bool) -> FamilyNext {
        let state = match reduce_family(self.family, &self.records) {
            Ok(projection) => projection.map_or(LogicalJobState::Queued, |p| p.state),
            Err(_) => LogicalJobState::Indeterminate,
        };
        let verdict = prune_verdict(&PruneInput {
            records: &self.records,
            state,
            truncated: self.truncated,
            closed: &self.closed,
            not_run: &self.not_run,
            local_work: self.local_work,
            undelivered,
            now_ms: self.now_ms,
        });
        match verdict {
            PruneVerdict::Prunable => {
                self.step = FamilyStep::Deadline;
                let row = (
                    DEADLINE_INDEX_KEYSPACE.to_string(),
                    deadline_index_key(&self.family),
                );
                Ok(Next::Call(IoCall::Read(vec![row])))
            }
            // The cancel settles the family, so it is prunable once retention passed again.
            PruneVerdict::Expire => {
                self.expiring = (self.records.iter()).find_map(|envelope| match &envelope.record {
                    JobFamilyRecord::Spec(spec) => Some(spec.clone()),
                    _ => None,
                });
                let expired = std::mem::take(&mut self.expired);
                self.moved(self.now_ms.saturating_add(RETENTION_MS), expired)
            }
            PruneVerdict::Held { reason, recheck_ms } => {
                let later = match reason {
                    HoldReason::LaunchOpen(_) => CLOCK_MARGIN_MS,
                    _ => RETENTION_MS,
                };
                let recheck = recheck_ms.unwrap_or(self.now_ms.saturating_add(later));
                // The family is read again no later than its earliest pending row expires.
                let expiry =
                    (self.earliest_ms).map(|first| first.saturating_add(TOMBSTONE_RETENTION_MS));
                let recheck = expiry.map_or(recheck, |expiry| recheck.min(expiry));
                let expired = std::mem::take(&mut self.expired);
                self.moved(recheck, expired)
            }
        }
    }

    /// Step 2: the tombstone, the projection, the witness deadline and the index row due now, in
    /// one transaction. From then on readers answer "pruned" and no witness round plans it.
    fn enter(&mut self, deadline: Option<Value>) -> FamilyNext {
        let created_by = (self.records.iter()).find_map(|envelope| match &envelope.record {
            JobFamilyRecord::Spec(spec) => Some(spec.created_by),
            _ => None,
        });
        // The replaced tombstone's aliases stay, so expiry deletes every incarnation's alias rows.
        let claimed = (self.records.iter()).filter_map(|envelope| match &envelope.record {
            JobFamilyRecord::Claim(claim) => Some(claim.job_id),
            _ => None,
        });
        let old = (self.tombstone.iter()).flat_map(|tombstone| tombstone.aliases.iter().copied());
        let aliases: BTreeSet<_> = claimed.chain(old).collect();
        let tombstone = FamilyTombstone {
            pruned_at_ms: self.now_ms,
            last_record_ms: last_record_ms(&self.records),
            finished: false,
            created_by,
            aliases: aliases.into_iter().collect(),
        };
        let prefix = family_prefix(&self.family);
        let mut deletes = std::mem::take(&mut self.expired);
        deletes.push((FAMILY_PROJECTION_KEYSPACE.to_string(), prefix.clone()));
        deletes.push((
            FAMILY_PRUNE_KEYSPACE.to_string(),
            prune_due_key(self.due_ms, &self.family),
        ));
        if let Some(deadline) =
            deadline.and_then(|value| from_bytes::<WitnessDeadline>(&value).ok())
        {
            let key = deadline_key(&self.family, deadline.due_at_ms);
            deletes.push((WITNESS_DEADLINE_KEYSPACE.to_string(), key));
        }
        deletes.push((
            DEADLINE_INDEX_KEYSPACE.to_string(),
            deadline_index_key(&self.family),
        ));
        let mut writes = prune_rows_at(&self.family, self.now_ms).to_vec();
        let value = Value::from(to_bytes(&tombstone)?.as_slice());
        writes.push((FAMILY_TOMBSTONE_KEYSPACE.to_string(), prefix, value));
        self.commit(PruneStep::Paged, true, (deletes, writes))
    }
}

/// The rows an expired tombstone leaves: the tombstone and the aliases it kept answering.
fn tombstone_rows(family: &JobFamilyId, tombstone: &FamilyTombstone) -> JobDeletes {
    let aliases = (tombstone.aliases.iter()).map(|job_id| {
        (
            FAMILY_ALIAS_KEYSPACE.to_string(),
            alias_key(*job_id, family),
        )
    });
    let row = (FAMILY_TOMBSTONE_KEYSPACE.to_string(), family_prefix(family));
    aliases.chain([row]).collect()
}

fn page_rows(result: IoResult) -> Result<Vec<(Key, Value)>, PruneError> {
    match result {
        IoResult::Page { rows, .. } => Ok(rows),
        other => Err(IoError::Unexpected {
            expected: "IoResult::Page",
            got: other.label(),
        }
        .into()),
    }
}

impl Operation for PruneFamilyOperation {
    type Output = PruneStep;
    type Error = PruneError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> FamilyNext {
        let prefix = family_prefix(&self.family);
        Ok(Next::Call(IoCall::Read(vec![
            (
                FAMILY_PRUNE_KEYSPACE.to_string(),
                prune_slot_key(&self.family),
            ),
            (FAMILY_TOMBSTONE_KEYSPACE.to_string(), prefix),
        ])))
    }

    fn next(&mut self, result: IoResult) -> FamilyNext {
        match std::mem::replace(&mut self.step, FamilyStep::Rows) {
            FamilyStep::Rows => {
                let IoResult::Values(values) = result else {
                    return Err(IoError::Unfinished.into());
                };
                let mut values = values.into_iter();
                let (slot, tombstone) = (values.next().flatten(), values.next().flatten());
                if slot_of(slot) != Some(self.due_ms) {
                    let stale = vec![(
                        FAMILY_PRUNE_KEYSPACE.to_string(),
                        prune_due_key(self.due_ms, &self.family),
                    )];
                    return self.commit(PruneStep::Moved, false, (stale, Vec::new()));
                }
                self.tombstone = tombstone
                    .map(|value| from_bytes::<FamilyTombstone>(&value))
                    .transpose()?;
                if (self.tombstone.as_ref()).is_some_and(|tombstone| !tombstone.finished) {
                    self.step = FamilyStep::Pages;
                    let pages = PrunePagesOperation::new(self.family);
                    return Ok(Next::Call(IoCall::child(pages)));
                }
                self.step = FamilyStep::Records;
                Ok(Next::Call(IoCall::child(LoadFamilyOperation::family(
                    self.family,
                ))))
            }
            FamilyStep::Pages => {
                let pages = result.child::<JobDeletes, PruneError>()??;
                if pages.is_empty() {
                    return self.body_page(0);
                }
                self.commit(PruneStep::Paged, true, (pages, Vec::new()))
            }
            FamilyStep::Body(space) => {
                let name = BODY_SPACES[space];
                let rows = page_rows(result)?;
                self.body
                    .extend(rows.into_iter().map(|(key, _)| (name.to_string(), key)));
                self.body_page(space + 1)
            }
            FamilyStep::Records => {
                match result.child::<Vec<JobRecordEnvelope>, FamilyReadError>()? {
                    Ok(records) => self.records = records,
                    Err(FamilyReadError::LimitExceeded { .. }) => self.truncated = true,
                    Err(error) => return Err(PruneError::Read(error.to_string())),
                }
                self.page(
                    FamilyStep::Pending,
                    FAMILY_PENDING_KEYSPACE,
                    MAX_PENDING_RECORDS,
                )
            }
            FamilyStep::Pending => self.pending_read(page_rows(result)?),
            FamilyStep::Closures => {
                let closed = result.child::<BTreeSet<Ulid>, LifecycleError>()?;
                self.closed = closed.map_err(|error| PruneError::Read(error.to_string()))?;
                self.view()
            }
            FamilyStep::View => {
                if let Ok(Some(view)) = result.child::<Option<WriterView>, IoError>()? {
                    self.not_run = family_not_run(&self.records, &view);
                }
                self.local()
            }
            FamilyStep::Local => {
                let IoResult::Values(values) = result else {
                    return Err(IoError::Unfinished.into());
                };
                self.local_work = values.iter().any(Option::is_some);
                self.page(FamilyStep::Outbox, FAMILY_OUTBOX_KEYSPACE, 1)
            }
            FamilyStep::Outbox => {
                let undelivered = !page_rows(result)?.is_empty();
                self.decide(undelivered)
            }
            FamilyStep::Deadline => self.enter(result.value()?),
            FamilyStep::Commit(outcome) => match result.child::<bool, IoError>()? {
                Ok(true) => match self.expiring.take() {
                    Some(spec) => {
                        let reason = ExpiryReason::Retention;
                        let cancel = PublishCancelOperation::expiry(
                            *spec,
                            reason,
                            self.serving,
                            self.now_ms,
                        );
                        self.step = FamilyStep::Expiry(outcome);
                        Ok(Next::Call(IoCall::child(cancel)))
                    }
                    None => Ok(Next::Done(outcome)),
                },
                // The family was indexed again meanwhile; a later pass decides.
                Ok(false) => Ok(Next::Done(PruneStep::Moved)),
                Err(error) => Err(error.into()),
            },
            FamilyStep::Expiry(outcome) => {
                if let Err(error) = result.child::<(), JobRouteError>()? {
                    warn!(error = %error, "Expiry cancel of an undecided family was not published");
                }
                Ok(Next::Done(outcome))
            }
        }
    }
}
