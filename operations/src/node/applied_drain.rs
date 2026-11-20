//! Runs the usage, watch, mail and metadata follow-ups of the reconciles shard pulls applied,
//! oldest first, and deletes each row after them; a failed follow-up keeps its row for a rerun.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::Duration;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::{AppliedStep, AppliedSync, applied_sync_entry, order_counter_key};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::keyspaces::{APPLIED_ORDER_KEYSPACE, APPLIED_SYNC_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::task::{NO_TASK_HANDLE, TaskEffect, TaskEvent, TaskKey};
use aruna_core::types::Key;
use tracing::{debug, warn};
use ulid::Ulid;

use crate::metadata::after_sync::{AfterSyncError, AfterSyncOperation, MetadataTimings};
use crate::node::summary_refresh::{RefreshUsageError, RefreshUsageOperation};
use crate::notifications::email::RefreshMailOperation;
use crate::notifications::watch::interest::RefreshInterestOperation;

/// Rows one page of the drain reads at most.
pub(crate) const APPLIED_DRAIN_PAGE: usize = 16;
/// When the drain runs again after a follow-up failed; its row waits at that follow-up.
pub const APPLIED_RETRY_AFTER: Duration = Duration::from_secs(30);

type Turn = Result<Next<Option<Duration>>, IoError>;

#[derive(Clone, Debug, PartialEq)]
enum DrainStep {
    Page,
    /// The first queued entry of the document the order check is at.
    Order,
    /// Whether the batch that entry names is still queued.
    Owner(Key),
    /// The entry of a batch that is gone, deleted.
    Stale,
    Run(AppliedStep),
    Progress,
    Delete,
}

/// One drain run over every queued reconcile, page by page. A follow-up that succeeded is stored
/// in its row and the last deletes it; a failed one keeps it, an unreadable row is deleted, and a
/// metadata follow-up waits while an earlier queued batch of one of its documents is unsettled.
#[derive(Clone, Debug, PartialEq)]
pub struct AppliedDrainOperation {
    step: DrainStep,
    pending: VecDeque<(Key, Option<AppliedSync>)>,
    current: Option<Key>,
    applied: Option<AppliedSync>,
    after: Option<Key>,
    failed: bool,
    /// The current row's documents whose order is still to check, once the check started.
    ordering: Option<VecDeque<Ulid>>,
    /// The current row's own order entries, deleted with it.
    entries: Vec<Key>,
}

impl Default for AppliedDrainOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl AppliedDrainOperation {
    pub fn new() -> Self {
        Self {
            step: DrainStep::Page,
            pending: VecDeque::new(),
            current: None,
            applied: None,
            after: None,
            failed: false,
            ordering: None,
            entries: Vec::new(),
        }
    }

    fn read_page(&mut self) -> Turn {
        self.step = DrainStep::Page;
        Ok(Next::Call(IoCall::Page {
            space: APPLIED_SYNC_KEYSPACE.to_string(),
            prefix: Key::from(Vec::new()),
            after: self.after.clone(),
            limit: APPLIED_DRAIN_PAGE,
        }))
    }

    fn next_row(&mut self) -> Turn {
        let Some((key, applied)) = self.pending.pop_front() else {
            return match self.after {
                Some(_) => self.read_page(),
                None => Ok(Next::Done(self.failed.then_some(APPLIED_RETRY_AFTER))),
            };
        };
        self.current = Some(key);
        self.ordering = None;
        self.entries.clear();
        let Some(applied) = applied else {
            return self.delete();
        };
        let next = applied.next;
        self.applied = Some(applied);
        self.run(next)
    }

    fn applied(&mut self) -> Result<&mut AppliedSync, IoError> {
        self.applied.as_mut().ok_or(IoError::Unfinished)
    }

    fn run(&mut self, step: AppliedStep) -> Turn {
        let unordered = self.ordering.is_none();
        let applied = self.applied()?;
        let targets = &applied.result.targets;
        let call = match step {
            AppliedStep::Usage => IoCall::child(RefreshUsageOperation::new(
                applied.node_id,
                applied.realm_id,
                targets,
                applied.id,
            )),
            AppliedStep::Interest => match applied.realm_id {
                Some(realm_id) => IoCall::child(RefreshInterestOperation::new(targets, realm_id)),
                None => return self.run(AppliedStep::Mail),
            },
            AppliedStep::Mail => IoCall::child(RefreshMailOperation::applied(targets)),
            AppliedStep::Metadata if unordered => {
                let mut documents = VecDeque::new();
                for event in &applied.result.metadata_create_events {
                    if !documents.contains(&event.record.document_id) {
                        documents.push_back(event.record.document_id);
                    }
                }
                self.ordering = Some(documents);
                return self.check_order();
            }
            AppliedStep::Metadata => {
                let result = std::mem::take(&mut applied.result);
                IoCall::child(AfterSyncOperation::new(Some(applied.node_id), result))
            }
        };
        self.step = DrainStep::Run(step);
        Ok(Next::Call(call))
    }

    /// Reads the first queued entry of the next document to check, or runs the metadata
    /// follow-up once every document's first entry is this row's own.
    fn check_order(&mut self) -> Turn {
        let ordering = self.ordering.as_ref().ok_or(IoError::Unfinished)?;
        let Some(document_id) = ordering.front().copied() else {
            return self.run(AppliedStep::Metadata);
        };
        self.step = DrainStep::Order;
        let counter = order_counter_key(document_id);
        Ok(Next::Call(IoCall::Page {
            space: APPLIED_ORDER_KEYSPACE.to_string(),
            prefix: counter.clone(),
            after: Some(counter),
            limit: 1,
        }))
    }

    /// A document without a queued entry, or whose first one is this row's, is in order; an
    /// earlier entry asks whether its batch is still queued.
    fn ordered(&mut self, result: IoResult) -> Turn {
        let (rows, _) = result.page()?;
        let id = self.applied()?.id;
        if let Some((entry, owner)) = rows.into_iter().next() {
            if owner.as_ref() != id.to_bytes().as_slice() {
                self.step = DrainStep::Owner(entry);
                let row = (APPLIED_SYNC_KEYSPACE.to_string(), Key::from(owner.to_vec()));
                return Ok(Next::Call(IoCall::Read(vec![row])));
            }
            self.entries.push(entry);
        }
        if let Some(ordering) = self.ordering.as_mut() {
            ordering.pop_front();
        }
        self.check_order()
    }

    /// An earlier batch still queued keeps this row waiting for a later run; the entry of one
    /// that is gone is deleted and the document checked again.
    fn owned(&mut self, entry: Key, result: IoResult) -> Turn {
        let owner = result.values()?.into_iter().next().flatten();
        if owner.is_none() {
            self.step = DrainStep::Stale;
            let row = (APPLIED_ORDER_KEYSPACE.to_string(), entry);
            return Ok(Next::Call(IoCall::Delete(vec![row])));
        }
        debug!("A queued follow-up waits for an earlier batch of its document");
        self.failed = true;
        self.applied = None;
        self.current = None;
        self.next_row()
    }

    /// Stores that `step` ran, so a rerun goes on with the next one; the last one ends the row.
    fn finished(&mut self, step: AppliedStep) -> Turn {
        let Some(next) = step.after() else {
            return self.delete();
        };
        let applied = self.applied()?;
        applied.next = next;
        let entry = applied_sync_entry(applied);
        let entry = entry.map_err(|error| IoError::Codec(error.to_string()))?;
        self.step = DrainStep::Progress;
        Ok(Next::Call(IoCall::Write(vec![entry])))
    }

    /// Deletes the current row with its own order entries, so its document's next batch goes.
    fn delete(&mut self) -> Turn {
        let key = self.current.take().ok_or(IoError::Unfinished)?;
        self.applied = None;
        self.step = DrainStep::Delete;
        let mut rows = vec![(APPLIED_SYNC_KEYSPACE.to_string(), key)];
        let entries = std::mem::take(&mut self.entries).into_iter();
        rows.extend(entries.map(|entry| (APPLIED_ORDER_KEYSPACE.to_string(), entry)));
        Ok(Next::Call(IoCall::Delete(rows)))
    }
}

fn error_text<T, E: std::fmt::Display>(outcome: Result<T, E>) -> Option<String> {
    outcome.err().map(|error| error.to_string())
}

impl Operation for AppliedDrainOperation {
    type Output = Option<Duration>;
    type Error = IoError;

    fn first(&mut self) -> Turn {
        self.read_page()
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.step, DrainStep::Page) {
            DrainStep::Page => {
                let (rows, next) = result.page()?;
                // Rows kept after a failed follow-up are passed: the next page starts after them.
                self.after = next;
                for (key, value) in rows {
                    let applied = AppliedSync::from_bytes(value.as_ref())
                        .inspect_err(|error| warn!(%error, "Deleting an unreadable applied sync"))
                        .ok();
                    self.pending.push_back((key, applied));
                }
                self.next_row()
            }
            DrainStep::Order => self.ordered(result),
            DrainStep::Owner(entry) => self.owned(entry, result),
            DrainStep::Stale => {
                result.done()?;
                self.check_order()
            }
            DrainStep::Run(step) => {
                let failure = match step {
                    AppliedStep::Usage => error_text(result.child::<(), RefreshUsageError>()?),
                    AppliedStep::Interest => error_text(result.child::<(), IoError>()?),
                    AppliedStep::Mail => error_text(result.child::<(), IoError>()?),
                    AppliedStep::Metadata => {
                        error_text(result.child::<MetadataTimings, AfterSyncError>()?)
                    }
                };
                if let Some(error) = failure {
                    warn!(%error, ?step, "A follow-up stopped after the shard restore; it reruns");
                    self.failed = true;
                    self.applied = None;
                    self.current = None;
                    return self.next_row();
                }
                self.finished(step)
            }
            DrainStep::Progress => {
                result.done()?;
                let next = self.applied()?.next;
                self.run(next)
            }
            DrainStep::Delete => {
                result.done()?;
                self.next_row()
            }
        }
    }
}

/// Runs the queued follow-ups once where a sync applied them, and arms the drain's timer for the
/// work it leaves. Answers whether that work is safe: the timer took it, or nothing is left. A
/// node without a task handle runs no drain at all, so it has nothing to arm.
#[derive(Debug, PartialEq)]
pub struct SettleAppliedOperation {
    arming: bool,
}

impl Default for SettleAppliedOperation {
    fn default() -> Self {
        Self::new()
    }
}

impl SettleAppliedOperation {
    pub fn new() -> Self {
        Self { arming: false }
    }
}

impl Operation for SettleAppliedOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        Ok(Next::Call(IoCall::child(AppliedDrainOperation::new())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        if self.arming {
            return match result.event()? {
                Event::Task(TaskEvent::Error { message, .. }) if message != NO_TASK_HANDLE => {
                    warn!(%message, "Failed to arm the applied sync drain for queued follow-ups");
                    Ok(Next::Done(false))
                }
                _ => Ok(Next::Done(true)),
            };
        }
        let after = match result.child::<Option<Duration>, IoError>()? {
            Ok(None) => return Ok(Next::Done(true)),
            Ok(Some(after)) => after,
            Err(error) => {
                warn!(%error, "Queued follow-ups stopped after a document sync; the drain reruns");
                APPLIED_RETRY_AFTER
            }
        };
        self.arming = true;
        let key = TaskKey::DrainAppliedSync;
        let arm = Effect::Task(TaskEffect::ShortenTimer { key, after });
        Ok(Next::Call(IoCall::from(arm)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::document::{DocumentReconcileResult, DocumentTarget, order_entry_key};
    use aruna_core::id::NodeId;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::types::Value;
    use ulid::Ulid;

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([7; 32])
    }

    fn targets() -> Vec<DocumentTarget> {
        vec![DocumentTarget::RealmConfig { realm_id: realm() }]
    }

    fn applied(realm_id: Option<RealmId>, next: AppliedStep) -> AppliedSync {
        AppliedSync {
            id: Ulid::from_parts(1_000, 7),
            node_id: node(),
            realm_id,
            result: DocumentReconcileResult {
                targets: targets(),
                ..DocumentReconcileResult::default()
            },
            next,
        }
    }

    fn queued(applied: &AppliedSync) -> IoResult {
        let (_, key, value) = applied_sync_entry(applied).unwrap();
        IoResult::Page {
            rows: vec![(key, value)],
            next: None,
        }
    }

    /// The row write that stores `next` as the follow-up a rerun starts at.
    fn progress(realm_id: Option<RealmId>, next: AppliedStep) -> Turn {
        let entry = applied_sync_entry(&applied(realm_id, next)).unwrap();
        Ok(Next::Call(IoCall::Write(vec![entry])))
    }

    fn child<T, E>(outcome: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(outcome))
    }

    fn after_sync() -> IoCall {
        let result = DocumentReconcileResult {
            targets: targets(),
            ..DocumentReconcileResult::default()
        };
        IoCall::child(AfterSyncOperation::new(Some(node()), result))
    }

    // The follow-ups run in order and each is stored once it succeeded; the row goes last.
    #[test]
    fn followups_run_ordered() {
        let realm_id = Some(realm());
        let mut drain = AppliedDrainOperation::new();
        assert!(matches!(drain.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let generation = Ulid::from_parts(1_000, 7);
        let usage = RefreshUsageOperation::new(node(), realm_id, &targets(), generation);
        let row = applied(realm_id, AppliedStep::Usage);
        assert_eq!(
            drain.next(queued(&row)),
            Ok(Next::Call(IoCall::child(usage)))
        );
        let turn = drain.next(child::<(), RefreshUsageError>(Ok(())));
        assert_eq!(turn, progress(realm_id, AppliedStep::Interest));
        let interest = RefreshInterestOperation::new(&targets(), realm());
        assert_eq!(
            drain.next(IoResult::Done),
            Ok(Next::Call(IoCall::child(interest)))
        );
        let turn = drain.next(child::<(), IoError>(Ok(())));
        assert_eq!(turn, progress(realm_id, AppliedStep::Mail));
        let mail = RefreshMailOperation::applied(&targets());
        assert_eq!(
            drain.next(IoResult::Done),
            Ok(Next::Call(IoCall::child(mail)))
        );
        let turn = drain.next(child::<(), IoError>(Ok(())));
        assert_eq!(turn, progress(realm_id, AppliedStep::Metadata));
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Call(after_sync())));
        let timings = MetadataTimings::default();
        let turn = drain.next(child::<_, AfterSyncError>(Ok(timings)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
        assert_eq!(drain.next(IoResult::Done), Ok(Next::Done(None)));
    }

    // A failed follow-up stores no progress and keeps its row, the last one too; the drain
    // goes on with the next row and asks to run again.
    #[test]
    fn failed_followup_kept() {
        let mut drain = AppliedDrainOperation::new();
        drain.first().unwrap();
        let usage = applied(Some(realm()), AppliedStep::Usage);
        let mut last = applied(Some(realm()), AppliedStep::Metadata);
        last.id = Ulid::from_parts(2_000, 7);
        let rows = [&usage, &last].map(|row| {
            let (_, key, value) = applied_sync_entry(row).unwrap();
            (key, value)
        });
        let page = IoResult::Page {
            rows: rows.to_vec(),
            next: None,
        };
        assert!(matches!(drain.next(page), Ok(Next::Call(IoCall::Child(_)))));
        let failed = RefreshUsageError::Io(IoError::Aborted);
        let turn = drain.next(child::<(), _>(Err(failed)));
        assert_eq!(turn, Ok(Next::Call(after_sync())));
        let aborted = AfterSyncError::Io(IoError::Aborted);
        let turn = drain.next(child::<MetadataTimings, _>(Err(aborted)));
        assert_eq!(turn, Ok(Next::Done(Some(APPLIED_RETRY_AFTER))));
    }

    // A rerun after a restart starts at the stored follow-up: the refreshes that ran already
    // do not run again.
    #[test]
    fn rerun_skips_finished() {
        let mut drain = AppliedDrainOperation::new();
        drain.first().unwrap();
        let row = applied(Some(realm()), AppliedStep::Metadata);
        assert_eq!(drain.next(queued(&row)), Ok(Next::Call(after_sync())));
        let timings = MetadataTimings::default();
        let turn = drain.next(child::<_, AfterSyncError>(Ok(timings)));
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
    }

    #[test]
    fn realmless_skips_interest() {
        let mut drain = AppliedDrainOperation::new();
        drain.first().unwrap();
        drain
            .next(queued(&applied(None, AppliedStep::Usage)))
            .unwrap();
        let turn = drain.next(child::<(), RefreshUsageError>(Ok(())));
        assert_eq!(turn, progress(None, AppliedStep::Interest));
        let mail = RefreshMailOperation::applied(&targets());
        assert_eq!(
            drain.next(IoResult::Done),
            Ok(Next::Call(IoCall::child(mail)))
        );
    }

    #[test]
    fn unreadable_row_deleted() {
        let mut drain = AppliedDrainOperation::new();
        drain.first().unwrap();
        let rows = vec![(Key::from(vec![9]), Value::from(vec![0xff]))];
        let turn = drain.next(IoResult::Page { rows, next: None });
        assert!(matches!(turn, Ok(Next::Call(IoCall::Delete(_)))));
    }

    fn task(event: TaskEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Task(event)))
    }

    // Work the drain leaves where the sync ran arms its timer; a timer that did not take it
    // fails the settle, so the caller runs it again, and a drain that finished arms nothing.
    #[test]
    fn settle_arms_leftovers() {
        let mut settle = SettleAppliedOperation::new();
        let drain = Ok(Next::Call(IoCall::child(AppliedDrainOperation::new())));
        assert_eq!(settle.first(), drain);
        let left = child::<_, IoError>(Ok(Some(APPLIED_RETRY_AFTER)));
        let key = TaskKey::DrainAppliedSync;
        let after = APPLIED_RETRY_AFTER;
        let arm = Effect::Task(TaskEffect::ShortenTimer { key, after });
        assert_eq!(settle.next(left), Ok(Next::Call(IoCall::from(arm))));
        let refused = task(TaskEvent::Error {
            key: None,
            message: "full".to_string(),
        });
        assert_eq!(settle.next(refused), Ok(Next::Done(false)));

        let mut finished = SettleAppliedOperation::new();
        finished.first().unwrap();
        let none = child::<Option<Duration>, IoError>(Ok(None));
        assert_eq!(finished.next(none), Ok(Next::Done(true)));
    }

    /// A queued batch of the one document `test_event` names, accepting `event`.
    fn batch(id: Ulid, event: Ulid) -> AppliedSync {
        let mut row = applied(Some(realm()), AppliedStep::Metadata);
        row.id = id;
        let created = crate::metadata::raw_revision::tests::test_event(event, 1);
        row.result.metadata_create_events = vec![created];
        row
    }

    fn rows(batches: &[&AppliedSync]) -> IoResult {
        let rows = batches.iter().map(|batch| {
            let (_, key, value) = applied_sync_entry(batch).unwrap();
            (key, value)
        });
        IoResult::Page {
            rows: rows.collect(),
            next: None,
        }
    }

    /// The order entry `sequence` of the document, naming `batch`.
    fn entry(sequence: u64, batch: &AppliedSync) -> (Key, Value) {
        let document_id = batch.result.metadata_create_events[0].record.document_id;
        let key = order_entry_key(document_id, sequence);
        (key, Value::from(batch.id.to_bytes().to_vec()))
    }

    fn first_entry(entry: (Key, Value)) -> IoResult {
        IoResult::Page {
            rows: vec![entry],
            next: None,
        }
    }

    fn order_read(batch: &AppliedSync) -> Turn {
        let document_id = batch.result.metadata_create_events[0].record.document_id;
        let counter = order_counter_key(document_id);
        Ok(Next::Call(IoCall::Page {
            space: APPLIED_ORDER_KEYSPACE.to_string(),
            prefix: counter.clone(),
            after: Some(counter),
            limit: 1,
        }))
    }

    fn after_project(batch: &AppliedSync) -> Turn {
        let result = batch.result.clone();
        let after = AfterSyncOperation::new(Some(node()), result);
        Ok(Next::Call(IoCall::child(after)))
    }

    // Two transactions queue batches of one document whose ids sort against the order they
    // were accepted in. The later one waits while the first is unsettled, also after the first
    // projection failed once, and goes only once the first one and its entry are gone.
    #[test]
    fn batches_project_ordered() {
        let first = batch(Ulid::from_parts(2_000, 1), Ulid::from_parts(10, 1));
        let later = batch(Ulid::from_parts(1_000, 1), Ulid::from_parts(11, 1));
        let owner = |batch: &AppliedSync| {
            let (_, _, value) = applied_sync_entry(batch).unwrap();
            IoResult::Values(vec![Some(value)])
        };
        for failing in [true, false] {
            let mut drain = AppliedDrainOperation::new();
            drain.first().unwrap();
            assert_eq!(drain.next(rows(&[&later, &first])), order_read(&later));
            let read = drain.next(first_entry(entry(0, &first)));
            assert!(matches!(read, Ok(Next::Call(IoCall::Read(_)))));
            assert_eq!(drain.next(owner(&first)), order_read(&first));
            let projecting = drain.next(first_entry(entry(0, &first)));
            assert_eq!(projecting, after_project(&first));
            if failing {
                let unprojected = AfterSyncError::Io(IoError::Aborted);
                let turn = drain.next(child::<MetadataTimings, _>(Err(unprojected)));
                assert_eq!(turn, Ok(Next::Done(Some(APPLIED_RETRY_AFTER))));
                continue;
            }
            let settled = drain.next(child::<_, AfterSyncError>(Ok(MetadataTimings::default())));
            let Ok(Next::Call(IoCall::Delete(deleted))) = settled else {
                panic!("the settled batch goes, got {settled:?}");
            };
            let (key, _) = entry(0, &first);
            assert!(deleted.contains(&(APPLIED_ORDER_KEYSPACE.to_string(), key)));
        }
        let mut drain = AppliedDrainOperation::new();
        drain.first().unwrap();
        assert_eq!(drain.next(rows(&[&later])), order_read(&later));
        let projecting = drain.next(first_entry(entry(1, &later)));
        assert_eq!(projecting, after_project(&later));
    }
}
