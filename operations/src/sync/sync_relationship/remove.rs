//! Removes an outgoing relationship for a delete flow, keeping a stub when it serves references.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::{SyncRelationship, SyncState};

use super::{SyncRelationshipDirection, SyncRelationshipError, delete_call, finished, write_call};

/// Relationships serving references are detached, not deleted: reads are authorized through
/// the outgoing record, so a stub must survive for retained data to stay readable.
#[derive(Debug, PartialEq)]
pub struct RemoveOutgoingOperation {
    relationship: SyncRelationship,
}

impl RemoveOutgoingOperation {
    pub fn new(relationship: SyncRelationship) -> Self {
        Self { relationship }
    }
}

impl Operation for RemoveOutgoingOperation {
    type Output = ();
    type Error = SyncRelationshipError;

    fn first(&mut self) -> Result<Next<()>, SyncRelationshipError> {
        let outgoing = SyncRelationshipDirection::Outgoing;
        let call = if self.relationship.serves_references() {
            let stub = SyncRelationship {
                state: SyncState::Detached,
                ..self.relationship.clone()
            };
            write_call(outgoing, &stub)?
        } else {
            delete_call(outgoing, &self.relationship)?
        };
        Ok(Next::Call(call))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, SyncRelationshipError> {
        finished(result)?;
        Ok(Next::Done(()))
    }
}

#[cfg(test)]
mod tests {
    use super::super::storage_key;
    use super::super::tests::relationship;
    use super::*;
    use aruna_core::calls::{IoCall, IoError};
    use aruna_core::keyspaces::RELATIONSHIP_OUT_KEYSPACE;
    use byteview::ByteView;

    fn plain() -> SyncRelationship {
        relationship(1, "source-a", "target-a")
    }

    fn serving() -> SyncRelationship {
        SyncRelationship {
            reference_serving: true,
            ..plain()
        }
    }

    fn key() -> aruna_core::types::Key {
        storage_key(SyncRelationshipDirection::Outgoing, &plain()).unwrap()
    }

    #[test]
    fn deletes_plain_relationship() {
        let mut operation = RemoveOutgoingOperation::new(plain());
        let row = (RELATIONSHIP_OUT_KEYSPACE.to_string(), key());
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Delete(vec![row]))));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn detaches_serving_relationship() {
        let stub = SyncRelationship {
            state: SyncState::Detached,
            ..serving()
        };
        let mut operation = RemoveOutgoingOperation::new(serving());
        let row = (
            RELATIONSHIP_OUT_KEYSPACE.to_string(),
            key(),
            ByteView::from(stub.to_bytes().unwrap()),
        );
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Write(vec![row]))));
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn refuses_bad_input() {
        let mut looping = plain();
        looping.target = looping.source.clone();
        assert!(matches!(
            RemoveOutgoingOperation::new(looping).first(),
            Err(SyncRelationshipError::Conversion(_))
        ));
        let mut operation = RemoveOutgoingOperation::new(plain());
        operation.first().unwrap();
        assert!(matches!(
            operation.next(IoResult::Values(vec![None])),
            Err(SyncRelationshipError::Io(IoError::Unexpected { .. }))
        ));
    }
}
