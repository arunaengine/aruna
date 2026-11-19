//! Runs the due mirror repairs of one pass and re-arms the repair timer from the stored records.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::operation::Operation;
use aruna_core::rows::event;
use aruna_core::structs::SyncState;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};

use super::ensure::EnsureMirrorOperation;
use super::record::MirrorRecordOperation;
use super::remove::{RemoveLocalOperation, RemoveMirrorOperation};
use super::scan::{RepairScan, ScanRepairsOperation};
use super::settle::{RollbackClaim, SettleReconcileOperation};
use super::{SyncMirrorError, SyncMirrorIntent, SyncMirrorRecord, SyncMirrorResult};

#[derive(Debug, PartialEq)]
enum RepairPhase {
    Settling,
    RemovingLocal,
    Finishing,
}

/// Carries out one due record; the output tells whether it claimed a rollback that is due now.
#[derive(Debug, PartialEq)]
struct RepairRecordOperation {
    local_node: NodeId,
    record: SyncMirrorRecord,
    phase: RepairPhase,
}

impl RepairRecordOperation {
    fn new(local_node: NodeId, record: SyncMirrorRecord) -> Self {
        Self {
            local_node,
            record,
            phase: RepairPhase::Finishing,
        }
    }

    fn remove_mirror(&mut self) -> Next<bool> {
        self.phase = RepairPhase::Finishing;
        let relationship = self.record.relationship.clone();
        let remove = RemoveMirrorOperation::new(self.local_node, relationship);
        Next::Call(IoCall::child(remove))
    }

    fn settled(&mut self, claim: RollbackClaim) -> Next<bool> {
        match claim {
            RollbackClaim::Stale => Next::Done(false),
            RollbackClaim::Claimed => Next::Done(true),
            // Detached stubs only serve retained reference reads; never resurrect the target
            // mirror for them.
            RollbackClaim::Active(relationship) if relationship.state == SyncState::Detached => {
                self.remove_mirror()
            }
            RollbackClaim::Active(relationship) => {
                self.phase = RepairPhase::Finishing;
                let ensure = EnsureMirrorOperation::new(self.local_node, relationship);
                Next::Call(IoCall::child(ensure))
            }
        }
    }
}

impl Operation for RepairRecordOperation {
    type Output = bool;
    type Error = SyncMirrorError;

    fn first(&mut self) -> Result<Next<bool>, SyncMirrorError> {
        let relationship = self.record.relationship.clone();
        let call = match self.record.intent {
            SyncMirrorIntent::Reconcile => {
                self.phase = RepairPhase::Settling;
                IoCall::child(SettleReconcileOperation::new(self.record.clone()))
            }
            // The claim found no outgoing record, so no local cleanup may leave a stub.
            SyncMirrorIntent::Delete if self.record.rollback => IoCall::child(
                RemoveMirrorOperation::rollback(self.local_node, relationship),
            ),
            SyncMirrorIntent::Delete => {
                self.phase = RepairPhase::RemovingLocal;
                IoCall::child(RemoveLocalOperation::new(self.local_node, relationship))
            }
        };
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, SyncMirrorError> {
        match self.phase {
            RepairPhase::Settling => {
                let claim = result.child::<RollbackClaim, SyncMirrorError>()??;
                Ok(self.settled(claim))
            }
            RepairPhase::RemovingLocal => {
                result.child::<(), SyncMirrorError>()??;
                Ok(self.remove_mirror())
            }
            RepairPhase::Finishing => {
                result.child::<(), SyncMirrorError>()??;
                Ok(Next::Done(false))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum DrainPhase {
    Scanning,
    Repairing,
    Clearing,
    Rescheduling,
}

/// One pass over the due repair records. A failed record moves behind its retry delay and keeps
/// the error; only a failure to change the record itself ends the pass.
#[derive(Debug, PartialEq)]
pub struct DrainRepairsOperation {
    local_node: NodeId,
    /// The time of the pass, from the runner's first stamp.
    now_ms: Option<u64>,
    scan: RepairScan,
    position: usize,
    succeeded: usize,
    failed: usize,
    claimed: bool,
    phase: DrainPhase,
}

impl DrainRepairsOperation {
    pub fn new(local_node: NodeId) -> Self {
        Self {
            local_node,
            now_ms: None,
            scan: RepairScan::default(),
            position: 0,
            succeeded: 0,
            failed: 0,
            claimed: false,
            phase: DrainPhase::Scanning,
        }
    }

    fn current(&self) -> Result<SyncMirrorRecord, SyncMirrorError> {
        self.scan
            .records
            .get(self.position)
            .cloned()
            .ok_or_else(|| SyncMirrorError::Mirror("no repair record is running".to_string()))
    }

    fn advance(&mut self) -> Next<SyncMirrorResult> {
        let Some(record) = self.scan.records.get(self.position).cloned() else {
            return Next::Done(self.outcome());
        };
        self.phase = DrainPhase::Repairing;
        let repair = RepairRecordOperation::new(self.local_node, record);
        Next::Call(IoCall::child(repair))
    }

    fn outcome(&self) -> SyncMirrorResult {
        // A claimed rollback is due now, so the next pass runs at once.
        let has_more_due = self.scan.has_more_due || self.claimed;
        SyncMirrorResult {
            processed: self.scan.records.len(),
            succeeded: self.succeeded,
            failed: self.failed,
            has_more_due,
            next_due_after: if has_more_due {
                None
            } else {
                self.scan
                    .next_due_ms
                    .map(|due_at| due_after(due_at, self.now_ms.unwrap_or_default()))
            },
        }
    }
}

impl Operation for DrainRepairsOperation {
    type Output = SyncMirrorResult;
    type Error = SyncMirrorError;

    fn first(&mut self) -> Result<Next<SyncMirrorResult>, SyncMirrorError> {
        Ok(Next::Call(IoCall::child(ScanRepairsOperation::new())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncMirrorResult>, SyncMirrorError> {
        match self.phase {
            DrainPhase::Scanning => {
                self.scan = result.child::<RepairScan, SyncMirrorError>()??;
                Ok(self.advance())
            }
            DrainPhase::Repairing => {
                let record = self.current()?;
                let change = match result.child::<bool, SyncMirrorError>()? {
                    Ok(rollback) => {
                        self.claimed |= rollback;
                        self.phase = DrainPhase::Clearing;
                        MirrorRecordOperation::clear_scanned(record)
                    }
                    Err(error) => {
                        self.phase = DrainPhase::Rescheduling;
                        MirrorRecordOperation::reschedule(record, error.to_string())
                    }
                };
                Ok(Next::Call(IoCall::child(change)))
            }
            DrainPhase::Clearing => {
                result.child::<(), SyncMirrorError>()??;
                self.succeeded = self.succeeded.saturating_add(1);
                self.position += 1;
                Ok(self.advance())
            }
            DrainPhase::Rescheduling => {
                result.child::<(), SyncMirrorError>()??;
                self.failed = self.failed.saturating_add(1);
                self.position += 1;
                Ok(self.advance())
            }
        }
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms.get_or_insert(now_ms);
    }
}

/// Re-arms the repair timer from the stored records: at once for due work, otherwise at the
/// earliest due time. No record leaves the timer alone.
#[derive(Debug, Default, PartialEq)]
pub struct RestoreTimerOperation {
    now_ms: Option<u64>,
    arming: bool,
}

impl RestoreTimerOperation {
    pub fn new() -> Self {
        Self::default()
    }

    fn after(&self, scan: &RepairScan) -> Option<Duration> {
        if !scan.records.is_empty() || scan.has_more_due {
            return Some(Duration::ZERO);
        }
        let now_ms = self.now_ms.unwrap_or_default();
        scan.next_due_ms.map(|due_at| due_after(due_at, now_ms))
    }
}

impl Operation for RestoreTimerOperation {
    type Output = ();
    type Error = SyncMirrorError;

    fn first(&mut self) -> Result<Next<()>, SyncMirrorError> {
        Ok(Next::Call(IoCall::child(ScanRepairsOperation::new())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, SyncMirrorError> {
        if self.arming {
            return match event(result)? {
                Event::Task(TaskEvent::Error { message, .. }) => {
                    Err(SyncMirrorError::Mirror(message))
                }
                _ => Ok(Next::Done(())),
            };
        }
        let scan = result.child::<RepairScan, SyncMirrorError>()??;
        let Some(after) = self.after(&scan) else {
            return Ok(Next::Done(()));
        };
        self.arming = true;
        let reset = Effect::Task(TaskEffect::ResetTimer {
            key: TaskKey::DrainMirrorRepair,
            after,
        });
        Ok(Next::Call(IoCall::Effect(Box::new(reset))))
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms.get_or_insert(now_ms);
    }
}

fn due_after(due_at_ms: u64, now_ms: u64) -> Duration {
    Duration::from_millis(due_at_ms.saturating_sub(now_ms))
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::SyncRelationship;

    fn node() -> NodeId {
        relationship().source.node_id
    }

    fn record(intent: SyncMirrorIntent) -> SyncMirrorRecord {
        SyncMirrorRecord::new(relationship(), intent, 1)
    }

    fn answer<T>(result: Result<T, SyncMirrorError>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn is_child<T>(next: Result<Next<T>, SyncMirrorError>) -> bool {
        matches!(next, Ok(Next::Call(IoCall::Child(_))))
    }

    fn settling() -> RepairRecordOperation {
        let mut operation = RepairRecordOperation::new(node(), record(SyncMirrorIntent::Reconcile));
        assert!(is_child(operation.first()));
        assert_eq!(operation.phase, RepairPhase::Settling);
        operation
    }

    #[test]
    fn settles_reconcile_claims() {
        assert_eq!(
            settling().next(answer(Ok(RollbackClaim::Stale))),
            Ok(Next::Done(false))
        );
        assert_eq!(
            settling().next(answer(Ok(RollbackClaim::Claimed))),
            Ok(Next::Done(true))
        );
        let mut active = settling();
        let claim = RollbackClaim::Active(relationship());
        assert!(is_child(active.next(answer(Ok(claim)))));
        assert_eq!(active.next(answer(Ok(()))), Ok(Next::Done(false)));
        let detached = SyncRelationship {
            state: SyncState::Detached,
            ..relationship()
        };
        let mut stub = settling();
        let claim = RollbackClaim::Active(detached);
        assert!(is_child(stub.next(answer(Ok(claim)))));
        assert_eq!(stub.phase, RepairPhase::Finishing);
    }

    #[test]
    fn removes_local_first() {
        let mut operation = RepairRecordOperation::new(node(), record(SyncMirrorIntent::Delete));
        assert!(is_child(operation.first()));
        assert_eq!(operation.phase, RepairPhase::RemovingLocal);
        assert!(is_child(operation.next(answer(Ok(())))));
        assert_eq!(operation.phase, RepairPhase::Finishing);
        assert_eq!(operation.next(answer(Ok(()))), Ok(Next::Done(false)));
    }

    #[test]
    fn rollback_skips_local() {
        let mut rollback = record(SyncMirrorIntent::Delete);
        rollback.rollback = true;
        let mut operation = RepairRecordOperation::new(node(), rollback);
        assert!(is_child(operation.first()));
        assert_eq!(operation.phase, RepairPhase::Finishing);
    }

    fn scanned(records: Vec<SyncMirrorRecord>, next_due_ms: Option<u64>) -> IoResult {
        answer(Ok(RepairScan {
            records,
            has_more_due: false,
            next_due_ms,
        }))
    }

    #[test]
    fn counts_each_record() {
        let mut operation = DrainRepairsOperation::new(node());
        operation.stamp(100);
        operation.stamp(500);
        assert!(is_child(operation.first()));
        let due = vec![
            record(SyncMirrorIntent::Delete),
            record(SyncMirrorIntent::Reconcile),
        ];
        assert!(is_child(operation.next(scanned(due, Some(160)))));
        assert!(is_child(operation.next(answer(Ok(false)))));
        assert!(is_child(operation.next(answer(Ok(())))));
        let failed = Err::<bool, _>(SyncMirrorError::Mirror("down".to_string()));
        assert!(is_child(operation.next(answer(failed))));
        assert_eq!(operation.phase, DrainPhase::Rescheduling);
        assert_eq!(
            operation.next(answer(Ok(()))),
            Ok(Next::Done(SyncMirrorResult {
                processed: 2,
                succeeded: 1,
                failed: 1,
                has_more_due: false,
                next_due_after: Some(Duration::from_millis(60)),
            }))
        );
    }

    #[test]
    fn claimed_rollback_repeats() {
        let mut operation = DrainRepairsOperation::new(node());
        operation.first().unwrap();
        let due = vec![record(SyncMirrorIntent::Reconcile)];
        operation.next(scanned(due, Some(9))).unwrap();
        operation.next(answer(Ok(true))).unwrap();
        let Ok(Next::Done(result)) = operation.next(answer(Ok(()))) else {
            panic!("the pass ends");
        };
        assert!(result.has_more_due);
        assert_eq!(result.next_due_after, None);
    }

    #[test]
    fn change_failure_ends() {
        let mut operation = DrainRepairsOperation::new(node());
        operation.first().unwrap();
        let due = vec![record(SyncMirrorIntent::Delete)];
        operation.next(scanned(due, None)).unwrap();
        operation.next(answer(Ok(false))).unwrap();
        let failed = SyncMirrorError::Mirror("stuck".to_string());
        assert_eq!(
            operation.next(answer::<()>(Err(SyncMirrorError::Mirror(
                "stuck".to_string()
            )))),
            Err(failed)
        );
    }

    fn timer(after: Duration) -> Next<()> {
        Next::Call(IoCall::Effect(Box::new(Effect::Task(
            TaskEffect::ResetTimer {
                key: TaskKey::DrainMirrorRepair,
                after,
            },
        ))))
    }

    #[test]
    fn restores_from_scan() {
        let mut later = RestoreTimerOperation::new();
        later.stamp(100);
        later.first().unwrap();
        assert_eq!(
            later.next(scanned(Vec::new(), Some(150))),
            Ok(timer(Duration::from_millis(50)))
        );
        let armed = Event::Task(TaskEvent::TimerScheduled {
            key: TaskKey::DrainMirrorRepair,
            after: Duration::ZERO,
        });
        assert_eq!(
            later.next(IoResult::Event(Box::new(armed))),
            Ok(Next::Done(()))
        );
        let due = vec![record(SyncMirrorIntent::Delete)];
        assert_eq!(
            RestoreTimerOperation::new().next(scanned(due, None)),
            Ok(timer(Duration::ZERO))
        );
        assert_eq!(
            RestoreTimerOperation::new().next(scanned(Vec::new(), None)),
            Ok(Next::Done(()))
        );
    }
}
