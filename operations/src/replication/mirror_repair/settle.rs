//! Settles a scanned reconcile record: keeps it for an active relationship or claims a rollback.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::MIRROR_REPAIR_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::done;
use aruna_core::structs::{SyncRelationship, sync_relationship_key};
use aruna_core::types::{Key, Value};
use byteview::ByteView;

use super::{
    SyncMirrorError, SyncMirrorIntent, SyncMirrorRecord, decode_record, one_value, outgoing_read,
    repair_key, repair_read,
};

/// What a reconcile pass finds when it tries to claim a rollback.
#[derive(Debug, PartialEq)]
pub(super) enum RollbackClaim {
    /// The scanned record changed, so this pass has nothing to do.
    Stale,
    /// The relationship was activated and is reconciled as usual.
    Active(SyncRelationship),
    /// The create never activated; the record now asks for a rollback.
    Claimed,
}

#[derive(Debug, PartialEq)]
enum Phase {
    Record,
    Outgoing,
    Claiming,
}

/// Turns a scanned reconcile record whose outgoing record is absent into a rollback, in one
/// transaction. Activation rewrites the same record, so only one of the two commits.
#[derive(Debug, PartialEq)]
pub(super) struct SettleReconcileOperation {
    scanned: SyncMirrorRecord,
    key: Key,
    now_ms: u64,
    phase: Phase,
}

impl SettleReconcileOperation {
    pub(super) fn new(scanned: SyncMirrorRecord) -> Self {
        Self {
            key: repair_key(&scanned.relationship),
            scanned,
            now_ms: 0,
            phase: Phase::Record,
        }
    }

    fn check_record(
        &mut self,
        value: Option<Value>,
    ) -> Result<Next<RollbackClaim>, SyncMirrorError> {
        if decode_record(&self.key, value)?.as_ref() != Some(&self.scanned) {
            return Ok(Next::Done(RollbackClaim::Stale));
        }
        let Some(bucket) = self.scanned.relationship.source.bucket() else {
            return Err(SyncMirrorError::Mirror("invalid source ARN".to_string()));
        };
        let key = ByteView::from(sync_relationship_key(bucket, self.scanned.relationship.id));
        self.phase = Phase::Outgoing;
        Ok(Next::Call(outgoing_read(&key)))
    }

    fn check_outgoing(
        &mut self,
        value: Option<Value>,
    ) -> Result<Next<RollbackClaim>, SyncMirrorError> {
        if let Some(value) = value {
            let existing = SyncRelationship::from_bytes(&value)?;
            return Ok(Next::Done(RollbackClaim::Active(existing)));
        }
        let claimed = SyncMirrorRecord {
            rollback: true,
            ..SyncMirrorRecord::new(
                self.scanned.relationship.clone(),
                SyncMirrorIntent::Delete,
                self.now_ms,
            )
        };
        let value = ByteView::from(claimed.to_bytes()?);
        self.phase = Phase::Claiming;
        let row = (MIRROR_REPAIR_KEYSPACE.to_string(), self.key.clone(), value);
        Ok(Next::Call(IoCall::Write(vec![row])))
    }
}

impl Operation for SettleReconcileOperation {
    type Output = RollbackClaim;
    type Error = SyncMirrorError;

    fn first(&mut self) -> Result<Next<RollbackClaim>, SyncMirrorError> {
        Ok(Next::Call(repair_read(&self.key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<RollbackClaim>, SyncMirrorError> {
        match self.phase {
            Phase::Record => self.check_record(one_value(result)?),
            Phase::Outgoing => self.check_outgoing(one_value(result)?),
            Phase::Claiming => {
                done(result)?;
                Ok(Next::Done(RollbackClaim::Claimed))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::*;
    use aruna_core::calls::IoError;

    const NOW: u64 = 2_000;

    fn scanned() -> SyncMirrorRecord {
        SyncMirrorRecord::new(relationship(), SyncMirrorIntent::Reconcile, 3)
    }

    fn values(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(ByteView::from)])
    }

    fn outgoing_key() -> Key {
        ByteView::from(sync_relationship_key("source", relationship().id))
    }

    /// The operation after the scanned record was read back unchanged.
    fn outgoing_phase() -> SettleReconcileOperation {
        let mut operation = SettleReconcileOperation::new(scanned());
        operation.stamp(NOW);
        assert_eq!(
            operation.first(),
            Ok(Next::Call(repair_read(&repair_key(&relationship()))))
        );
        let read = operation.next(values(Some(scanned().to_bytes().unwrap())));
        assert_eq!(read, Ok(Next::Call(outgoing_read(&outgoing_key()))));
        operation
    }

    #[test]
    fn claims_missing_activation() {
        let mut operation = outgoing_phase();
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        let claimed = SyncMirrorRecord {
            rollback: true,
            ..SyncMirrorRecord::new(relationship(), SyncMirrorIntent::Delete, NOW)
        };
        let row = (
            MIRROR_REPAIR_KEYSPACE.to_string(),
            repair_key(&relationship()),
            ByteView::from(claimed.to_bytes().unwrap()),
        );
        assert_eq!(
            operation.next(values(None)),
            Ok(Next::Call(IoCall::Write(vec![row])))
        );
        assert_eq!(
            operation.next(IoResult::Done),
            Ok(Next::Done(RollbackClaim::Claimed))
        );
    }

    #[test]
    fn active_and_stale() {
        let stored = relationship().to_bytes().unwrap();
        assert_eq!(
            outgoing_phase().next(values(Some(stored))),
            Ok(Next::Done(RollbackClaim::Active(relationship())))
        );
        let mut changed = scanned();
        changed.due_at_ms = 99;
        let mut stale = SettleReconcileOperation::new(scanned());
        stale.first().unwrap();
        assert_eq!(
            stale.next(values(Some(changed.to_bytes().unwrap()))),
            Ok(Next::Done(RollbackClaim::Stale))
        );
    }

    #[test]
    fn refuses_bad_results() {
        assert!(matches!(
            outgoing_phase().next(values(Some(vec![0xff]))),
            Err(SyncMirrorError::Conversion(_))
        ));
        assert!(matches!(
            outgoing_phase().next(IoResult::Done),
            Err(SyncMirrorError::Io(IoError::Unexpected { .. }))
        ));
    }
}
