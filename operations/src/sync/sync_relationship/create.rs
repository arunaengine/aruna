//! Activates a sync relationship: claims its staged repair record and stores the outgoing row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{MIRROR_REPAIR_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, done, read_row, values};
use aruna_core::structs::{SyncRelationship, SyncState};
use aruna_core::types::Value;
use byteview::ByteView;

use super::{
    SyncRelationshipDirection, SyncRelationshipError, pages, parse_values, same_create_identity,
    storage_key,
};
use crate::sync::mirror_repair::{
    SyncMirrorIntent, SyncMirrorRecord, repair_key, same_relationship,
};

#[derive(Debug, PartialEq)]
enum Phase {
    Scanning,
    Claiming,
    Writing,
}

/// Stores the outgoing record and rewrites the staged reconcile record in one transaction, so a
/// concurrent rollback claim fails at commit; a missing or changed record means a rollback.
/// Run it with `drive_retry`: a conflict reruns it, twice at most.
#[derive(Debug, PartialEq)]
pub struct CreateRelationshipOperation {
    relationship: SyncRelationship,
    /// Scans the source bucket for an enabled relationship with the same endpoints and mode.
    check_duplicates: bool,
    scan: PrefixPages,
    phase: Phase,
}

impl CreateRelationshipOperation {
    /// A new relationship, refused when an enabled one with the same endpoints and mode exists.
    pub fn new(relationship: SyncRelationship) -> Self {
        Self {
            check_duplicates: true,
            ..Self::activate(relationship)
        }
    }

    /// A relationship that replaces older ones of the same endpoints, so no duplicate check.
    pub fn activate(relationship: SyncRelationship) -> Self {
        let bucket = relationship.source.bucket();
        Self {
            scan: pages(SyncRelationshipDirection::Outgoing, bucket),
            relationship,
            check_duplicates: false,
            phase: Phase::Scanning,
        }
    }

    fn claim_read(&mut self) -> Next<SyncRelationship> {
        self.phase = Phase::Claiming;
        let key = repair_key(&self.relationship);
        Next::Call(read_row(MIRROR_REPAIR_KEYSPACE, key))
    }

    fn scan(&mut self, result: IoResult) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        let rows = self.scan.take(result)?;
        let existing = parse_values(SyncRelationshipDirection::Outgoing, rows)?;
        // Detached stubs keep retained references readable without blocking recreation.
        if existing.iter().any(|existing| {
            existing.state != SyncState::Detached
                && same_create_identity(existing, &self.relationship)
        }) {
            return Err(SyncRelationshipError::Duplicate);
        }
        Ok(match self.scan.done() {
            true => self.claim_read(),
            false => Next::Call(self.scan.call()),
        })
    }

    /// Rewrites the staged record unchanged next to the outgoing row.
    fn claim(
        &mut self,
        value: Option<Value>,
    ) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        let value = value.ok_or(SyncRelationshipError::RolledBack)?;
        let key = repair_key(&self.relationship);
        let record = SyncMirrorRecord::from_bytes(&key, &value)?;
        if record.intent != SyncMirrorIntent::Reconcile
            || !same_relationship(&record.relationship, &self.relationship)
        {
            return Err(SyncRelationshipError::RolledBack);
        }
        let outgoing = (
            RELATIONSHIP_OUT_KEYSPACE.to_string(),
            storage_key(SyncRelationshipDirection::Outgoing, &self.relationship)?,
            ByteView::from(self.relationship.to_bytes()?),
        );
        self.phase = Phase::Writing;
        let claim = (MIRROR_REPAIR_KEYSPACE.to_string(), key, value);
        Ok(Next::Call(IoCall::Write(vec![claim, outgoing])))
    }
}

impl Operation for CreateRelationshipOperation {
    type Output = SyncRelationship;
    type Error = SyncRelationshipError;

    fn first(&mut self) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        if self.relationship.source.bucket().is_none() {
            return Err(ConversionError::FromStrError(
                "sync relationship endpoint is not an S3 ARN".to_string(),
            )
            .into());
        }
        if self.check_duplicates {
            return Ok(Next::Call(self.scan.call()));
        }
        Ok(self.claim_read())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        match self.phase {
            Phase::Scanning => self.scan(result),
            Phase::Claiming => {
                let [value] = values::<1>(result)?;
                self.claim(value)
            }
            Phase::Writing => {
                done(result)?;
                Ok(Next::Done(self.relationship.clone()))
            }
        }
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }

    fn conflict_retries() -> u32 {
        2
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{page_call, relationship};
    use super::*;
    use aruna_core::calls::IoError;
    use aruna_core::structs::storage::replication::ArunaArnType;
    use aruna_core::types::Key;

    fn subject() -> SyncRelationship {
        relationship(1, "source-a", "target-a")
    }

    fn row(relationship: &SyncRelationship) -> (Key, Value) {
        (
            storage_key(SyncRelationshipDirection::Outgoing, relationship).unwrap(),
            ByteView::from(relationship.to_bytes().unwrap()),
        )
    }

    fn scan(after: Option<Key>) -> Next<SyncRelationship> {
        Next::Call(page_call(
            SyncRelationshipDirection::Outgoing,
            Some("source-a"),
            after,
        ))
    }

    fn page(rows: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        IoResult::Page { rows, next }
    }

    fn claim_read() -> Next<SyncRelationship> {
        Next::Call(read_row(MIRROR_REPAIR_KEYSPACE, repair_key(&subject())))
    }

    fn staged(intent: SyncMirrorIntent) -> Value {
        ByteView::from(
            SyncMirrorRecord::new(subject(), intent, 1)
                .to_bytes()
                .unwrap(),
        )
    }

    fn values(value: Option<Value>) -> IoResult {
        IoResult::Values(vec![value])
    }

    /// The operation after an empty duplicate scan, waiting for the staged record.
    fn claiming() -> CreateRelationshipOperation {
        let mut operation = CreateRelationshipOperation::new(subject());
        assert_eq!(operation.first(), Ok(scan(None)));
        assert_eq!(operation.next(page(Vec::new(), None)), Ok(claim_read()));
        operation
    }

    #[test]
    fn creates_relationship() {
        let other = row(&relationship(2, "source-a", "target-b"));
        let mut operation = CreateRelationshipOperation::new(subject());
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        assert_eq!(operation.first(), Ok(scan(None)));
        assert_eq!(
            operation.next(page(vec![other.clone()], Some(other.0.clone()))),
            Ok(scan(Some(other.0)))
        );
        assert_eq!(operation.next(page(Vec::new(), None)), Ok(claim_read()));
        let (key, value) = row(&subject());
        let writes = vec![
            (
                MIRROR_REPAIR_KEYSPACE.to_string(),
                repair_key(&subject()),
                staged(SyncMirrorIntent::Reconcile),
            ),
            (RELATIONSHIP_OUT_KEYSPACE.to_string(), key, value),
        ];
        assert_eq!(
            operation.next(values(Some(staged(SyncMirrorIntent::Reconcile)))),
            Ok(Next::Call(IoCall::Write(writes)))
        );
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(subject())));
    }

    #[test]
    fn activation_skips_scan() {
        let mut operation = CreateRelationshipOperation::activate(subject());
        assert_eq!(operation.first(), Ok(claim_read()));
    }

    #[test]
    fn refuses_duplicates() {
        let twin = relationship(2, "source-a", "target-a");
        assert_eq!(
            CreateRelationshipOperation::new(subject()).next(page(vec![row(&twin)], None)),
            Err(SyncRelationshipError::Duplicate)
        );
        let stub = SyncRelationship {
            state: SyncState::Detached,
            ..twin
        };
        assert_eq!(
            CreateRelationshipOperation::new(subject()).next(page(vec![row(&stub)], None)),
            Ok(claim_read())
        );
    }

    #[test]
    fn refuses_lost_claims() {
        assert_eq!(
            claiming().next(values(None)),
            Err(SyncRelationshipError::RolledBack)
        );
        assert_eq!(
            claiming().next(values(Some(staged(SyncMirrorIntent::Delete)))),
            Err(SyncRelationshipError::RolledBack)
        );
    }

    #[test]
    fn retries_conflicts() {
        assert_eq!(CreateRelationshipOperation::conflict_retries(), 2);
        assert!(CreateRelationshipOperation::is_conflict(
            &SyncRelationshipError::from(IoError::Conflict)
        ));
    }

    #[test]
    fn refuses_bad_results() {
        let corrupt = page(vec![(vec![1].into(), vec![0xff].into())], None);
        assert!(matches!(
            CreateRelationshipOperation::new(subject()).next(corrupt),
            Err(SyncRelationshipError::Conversion(_))
        ));
        assert!(matches!(
            claiming().next(IoResult::Done),
            Err(SyncRelationshipError::Io(IoError::Unexpected { .. }))
        ));
        let mut local = subject();
        local.source.resource_type = ArunaArnType::ContentHash;
        assert!(matches!(
            CreateRelationshipOperation::new(local).first(),
            Err(SyncRelationshipError::Conversion(_))
        ));
    }
}
