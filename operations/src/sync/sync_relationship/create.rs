//! Activates a sync relationship: claims its staged repair record and stores the outgoing row.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{MIRROR_REPAIR_KEYSPACE, RELATIONSHIP_OUT_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{SyncRelationship, SyncState};
use aruna_core::types::{Key, Value};
use byteview::ByteView;

use super::{
    SyncRelationshipDirection, SyncRelationshipError, finished, page_call, page_rows, parse_values,
    same_create_identity, storage_key, unexpected,
};
use crate::sync::mirror_repair::{SyncMirrorIntent, SyncMirrorRecord, same_relationship};

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
        Self {
            relationship,
            check_duplicates: false,
            phase: Phase::Scanning,
        }
    }

    fn scan_call(&self, after: Option<Key>) -> IoCall {
        let bucket = self.relationship.source.bucket();
        page_call(SyncRelationshipDirection::Outgoing, bucket, after)
    }

    fn claim_read(&mut self) -> Next<SyncRelationship> {
        self.phase = Phase::Claiming;
        let row = (
            MIRROR_REPAIR_KEYSPACE.to_string(),
            repair_key(&self.relationship),
        );
        Next::Call(IoCall::Read(vec![row]))
    }

    fn scan(
        &mut self,
        rows: Vec<(Key, Value)>,
        next: Option<Key>,
    ) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        let existing = parse_values(SyncRelationshipDirection::Outgoing, rows)?;
        // Detached stubs keep retained references readable without blocking recreation.
        if existing.iter().any(|existing| {
            existing.state != SyncState::Detached
                && same_create_identity(existing, &self.relationship)
        }) {
            return Err(SyncRelationshipError::Duplicate);
        }
        Ok(match next {
            Some(after) => Next::Call(self.scan_call(Some(after))),
            None => self.claim_read(),
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
            self.phase = Phase::Scanning;
            return Ok(Next::Call(self.scan_call(None)));
        }
        Ok(self.claim_read())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<SyncRelationship>, SyncRelationshipError> {
        match self.phase {
            Phase::Scanning => {
                let (rows, next) = page_rows(result)?;
                self.scan(rows, next)
            }
            Phase::Claiming => match result {
                IoResult::Values(mut values) if values.len() == 1 => self.claim(values.remove(0)),
                other => Err(unexpected("IoResult::Values", &other)),
            },
            Phase::Writing => {
                finished(result)?;
                Ok(Next::Done(self.relationship.clone()))
            }
        }
    }

    fn transaction(&self) -> bool {
        true
    }

    fn conflict_retries() -> u32 {
        2
    }
}

fn repair_key(relationship: &SyncRelationship) -> Key {
    ByteView::from(relationship.id.to_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::super::tests::relationship;
    use super::*;
    use aruna_core::calls::IoError;
    use aruna_core::structs::storage::replication::ArunaArnType;

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
        let row = (MIRROR_REPAIR_KEYSPACE.to_string(), repair_key(&subject()));
        Next::Call(IoCall::Read(vec![row]))
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
        assert!(operation.transaction());
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
