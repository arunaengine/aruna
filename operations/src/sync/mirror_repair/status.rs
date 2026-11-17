//! Stores a relationship's sync status with a reconcile record, journaling a stop as failed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{RELATIONSHIP_OUT_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::plugin_runs::{SourceEvent, journal_entries};
use aruna_core::rows::done;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::{SyncRelationship, SyncState, sync_relationship_key};
use aruna_core::types::{Key, KeySpace, Value};
use byteview::ByteView;
use ulid::Ulid;

use super::{
    SyncMirrorError, SyncMirrorIntent, SyncMirrorRecord, decode_record, one_value, outgoing_read,
    repair_entry, repair_key, repair_read, same_relationship,
};

type Writes = Vec<(KeySpace, Key, Value)>;

#[derive(Debug, PartialEq)]
enum Phase {
    Outgoing,
    Record(SyncState),
    Bucket(Writes),
    Writing,
}

/// Writes the status of an existing relationship and stages its reconcile record in one
/// transaction. Reports false and writes nothing for a missing or detached relationship, or
/// while a delete is staged. A change to failed also journals one plugin event.
#[derive(Debug, PartialEq)]
pub struct StoreStatusOperation {
    relationship: SyncRelationship,
    now_ms: u64,
    phase: Phase,
}

impl StoreStatusOperation {
    pub fn new(relationship: SyncRelationship) -> Self {
        Self {
            relationship,
            now_ms: 0,
            phase: Phase::Outgoing,
        }
    }

    fn check_outgoing(&mut self, value: Option<Value>) -> Result<Next<bool>, SyncMirrorError> {
        let Some(value) = value else {
            return Ok(Next::Done(false));
        };
        let existing = SyncRelationship::from_bytes(&value)?;
        if existing.state == SyncState::Detached {
            // A detached serving stub cannot be overwritten or restage its deleted mirror.
            return Ok(Next::Done(false));
        }
        if !same_relationship(&existing, &self.relationship) {
            return Err(SyncMirrorError::Mirror(
                "sync relationship identity changed during status update".to_string(),
            ));
        }
        self.phase = Phase::Record(existing.state);
        Ok(Next::Call(repair_read(&repair_key(&self.relationship))))
    }

    fn check_record(
        &mut self,
        previous: SyncState,
        value: Option<Value>,
    ) -> Result<Next<bool>, SyncMirrorError> {
        let key = repair_key(&self.relationship);
        if decode_record(&key, value)?
            .is_some_and(|record| record.intent == SyncMirrorIntent::Delete)
        {
            return Ok(Next::Done(false));
        }
        let repair = SyncMirrorRecord::new(
            self.relationship.clone(),
            SyncMirrorIntent::Reconcile,
            self.now_ms,
        );
        let writes = vec![
            (
                RELATIONSHIP_OUT_KEYSPACE.to_string(),
                outgoing_key(&self.relationship)?,
                ByteView::from(self.relationship.to_bytes()?),
            ),
            repair_entry(repair)?,
        ];
        let failed = |state: &SyncState| matches!(state, SyncState::Failed { .. });
        if failed(&self.relationship.state) && !failed(&previous) {
            let bucket = self.relationship.source.bucket().unwrap_or_default();
            self.phase = Phase::Bucket(writes);
            let row = (
                S3_BUCKET_KEYSPACE.to_string(),
                ByteView::from(bucket.as_bytes()),
            );
            return Ok(Next::Call(IoCall::Read(vec![row])));
        }
        Ok(self.write(writes))
    }

    /// Adds the failure event of the source bucket row; none once that bucket is gone.
    fn journal(
        &mut self,
        mut writes: Writes,
        value: Option<Value>,
    ) -> Result<Next<bool>, SyncMirrorError> {
        if let Some(value) = value {
            let info = BucketInfo::from_bytes(&value)?;
            let event = failure_event(&self.relationship, &info, self.now_ms);
            writes.extend(journal_entries(&event)?);
        }
        Ok(self.write(writes))
    }

    fn write(&mut self, writes: Writes) -> Next<bool> {
        self.phase = Phase::Writing;
        Next::Call(IoCall::Write(writes))
    }
}

impl Operation for StoreStatusOperation {
    type Output = bool;
    type Error = SyncMirrorError;

    fn first(&mut self) -> Result<Next<bool>, SyncMirrorError> {
        self.relationship.validate()?;
        Ok(Next::Call(outgoing_read(&outgoing_key(
            &self.relationship,
        )?)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, SyncMirrorError> {
        match std::mem::replace(&mut self.phase, Phase::Writing) {
            Phase::Outgoing => self.check_outgoing(one_value(result)?),
            Phase::Record(previous) => self.check_record(previous, one_value(result)?),
            Phase::Bucket(writes) => self.journal(writes, one_value(result)?),
            Phase::Writing => {
                done(result)?;
                Ok(Next::Done(true))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }
}

fn outgoing_key(relationship: &SyncRelationship) -> Result<Key, SyncMirrorError> {
    let bucket = (relationship.source.bucket())
        .ok_or_else(|| SyncMirrorError::Mirror("invalid source ARN".to_string()))?;
    Ok(ByteView::from(sync_relationship_key(
        bucket,
        relationship.id,
    )))
}

/// The plugin event of a relationship that stops as failed at `now_ms`. The transition id is
/// derived from the time and the relationship, since one relationship fails once at a time.
fn failure_event(relationship: &SyncRelationship, info: &BucketInfo, now_ms: u64) -> SourceEvent {
    let source = &relationship.source;
    let bucket = source.bucket().unwrap_or_default();
    let prefix = source.key_prefix().unwrap_or_default();
    let path = object_permission_path(
        source.realm_id,
        info.group_id,
        source.node_id,
        bucket,
        prefix,
    );
    let transition = Ulid::from_parts(now_ms, relationship.id.random());
    SourceEvent::sync_failure(
        (info.group_id, path),
        (relationship.id, relationship.created_by),
        (source.node_id, transition),
        now_ms,
    )
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::*;
    use aruna_core::calls::IoError;
    use aruna_core::keyspaces::MIRROR_REPAIR_KEYSPACE;

    const NOW: u64 = 3_000;

    fn values(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(ByteView::from)])
    }

    fn reading(relationship: SyncRelationship) -> StoreStatusOperation {
        let mut operation = StoreStatusOperation::new(relationship);
        operation.stamp(NOW);
        let key = ByteView::from(sync_relationship_key("source", self::relationship().id));
        assert_eq!(operation.first(), Ok(Next::Call(outgoing_read(&key))));
        operation
    }

    /// The operation after the outgoing row and an absent repair record were read.
    fn writing(
        relationship: SyncRelationship,
    ) -> (StoreStatusOperation, Result<Next<bool>, SyncMirrorError>) {
        let mut operation = reading(relationship);
        let read = operation.next(values(Some(self::relationship().to_bytes().unwrap())));
        assert_eq!(
            read,
            Ok(Next::Call(repair_read(&repair_key(&self::relationship()))))
        );
        let next = operation.next(values(None));
        (operation, next)
    }

    fn spaces(next: &Result<Next<bool>, SyncMirrorError>) -> Vec<String> {
        let Ok(Next::Call(IoCall::Write(writes))) = next else {
            panic!("expected one write: {next:?}");
        };
        writes.iter().map(|(space, ..)| space.clone()).collect()
    }

    #[test]
    fn stores_status() {
        let (mut operation, next) = writing(relationship());
        assert!(operation.transaction());
        assert_eq!(
            spaces(&next),
            vec![
                RELATIONSHIP_OUT_KEYSPACE.to_string(),
                MIRROR_REPAIR_KEYSPACE.to_string()
            ]
        );
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(true)));
    }

    #[test]
    fn failure_reads_bucket() {
        let mut failed = relationship();
        failed.state = SyncState::Failed {
            reason: "access_denied".to_string(),
        };
        let (mut operation, next) = writing(failed);
        let row = (
            S3_BUCKET_KEYSPACE.to_string(),
            ByteView::from("source".as_bytes()),
        );
        assert_eq!(next, Ok(Next::Call(IoCall::Read(vec![row]))));
        // A missing source bucket journals nothing.
        assert_eq!(spaces(&operation.next(values(None))).len(), 2);
    }

    #[test]
    fn skips_without_change() {
        assert_eq!(
            reading(relationship()).next(values(None)),
            Ok(Next::Done(false))
        );
        let mut stub = relationship();
        stub.state = SyncState::Detached;
        let detached = reading(relationship()).next(values(Some(stub.to_bytes().unwrap())));
        assert_eq!(detached, Ok(Next::Done(false)));
        let staged = SyncMirrorRecord::new(relationship(), SyncMirrorIntent::Delete, 1);
        let mut deleting = reading(relationship());
        deleting
            .next(values(Some(relationship().to_bytes().unwrap())))
            .unwrap();
        assert_eq!(
            deleting.next(values(Some(staged.to_bytes().unwrap()))),
            Ok(Next::Done(false))
        );
    }

    #[test]
    fn refuses_identity_changes() {
        let mut changed = relationship();
        changed.replicate_deletes = false;
        let mut operation = reading(changed);
        assert!(matches!(
            operation.next(values(Some(relationship().to_bytes().unwrap()))),
            Err(SyncMirrorError::Mirror(_))
        ));
        assert!(matches!(
            reading(relationship()).next(IoResult::Done),
            Err(SyncMirrorError::Io(IoError::Unexpected { .. }))
        ));
    }
}
