//! Reads one object head's placement references and its generation from this node only.
//! Does no authorization; the caller authorizes the read before driving this operation.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::rows::read_row;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::{
    BlobHeadKey, BlobVersion, CurrentVersionPointer, VersionKey,
};
use thiserror::Error;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPlacementInput {
    pub bucket: String,
    pub key: String,
}

/// What the head of one object carries right now.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectPlacement {
    pub version_id: Ulid,
    /// Head generation, which an exact-set mutation presents as its expectation.
    pub generation: u64,
    pub policies: Vec<PlacementPolicyRef>,
}

#[derive(Debug, Error, PartialEq)]
pub enum ObjectPlacementError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("the object has no current version here")]
    NoSuchKey,
    #[error("could not read the object placement: {0}")]
    Io(IoError),
}

impl From<IoError> for ObjectPlacementError {
    fn from(error: IoError) -> Self {
        match error {
            IoError::Storage(error) => Self::Storage(error),
            other => Self::Io(other),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ObjectPlacementOperation {
    input: ObjectPlacementInput,
    /// The head pointer once read; the version read follows.
    head: Option<CurrentVersionPointer>,
}

impl ObjectPlacementOperation {
    pub fn new(input: ObjectPlacementInput) -> Self {
        Self { input, head: None }
    }

    fn read_version(
        &mut self,
        value: &[u8],
    ) -> Result<Next<ObjectPlacement>, ObjectPlacementError> {
        let pointer = CurrentVersionPointer::from_bytes(value)?;
        let key = VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id);
        let key = key.to_bytes()?;
        self.head = Some(pointer);
        Ok(Next::Call(read_row(BLOB_VERSIONS_KEYSPACE, key)))
    }
}

impl Operation for ObjectPlacementOperation {
    type Output = ObjectPlacement;
    type Error = ObjectPlacementError;

    fn first(&mut self) -> Result<Next<ObjectPlacement>, ObjectPlacementError> {
        let key = BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes()?;
        Ok(Next::Call(read_row(BLOB_HEAD_KEYSPACE, key)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ObjectPlacement>, ObjectPlacementError> {
        let value = result.value()?.ok_or(ObjectPlacementError::NoSuchKey)?;
        let Some(head) = self.head.take() else {
            return self.read_version(&value);
        };
        let version = BlobVersion::from_bytes(value.as_ref())?;
        Ok(Next::Done(ObjectPlacement {
            version_id: head.version_id,
            generation: head.generation,
            policies: version.placement_policies,
        }))
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, ObjectPlacementError::NoSuchKey)
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{ObjectPlacementError, ObjectPlacementInput, ObjectPlacementOperation};
    use aruna_core::UserId;
    use aruna_core::calls::{IoResult, Next};
    use aruna_core::operation::Operation;
    use aruna_core::structs::placement::policy::PlacementPolicyRef;
    use aruna_core::structs::storage::blob::{BackendRef, BlobVersion, CurrentVersionPointer};
    use aruna_core::structs::storage::format::EncodingClass;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn operation() -> ObjectPlacementOperation {
        ObjectPlacementOperation::new(ObjectPlacementInput {
            bucket: "raw".to_string(),
            key: "run1.tar".to_string(),
        })
    }

    fn read_result(value: Option<Vec<u8>>) -> IoResult {
        IoResult::Values(vec![value.map(Into::into)])
    }

    #[test]
    fn reports_head_refs() {
        // The generation an exact-set mutation must present comes from the head
        // pointer, and the refs from the version it names.
        let version_id = Ulid::from_bytes([3u8; 16]);
        let policy_ref = PlacementPolicyRef {
            policy_id: Ulid::from_bytes([4u8; 16]),
            digest: [5u8; 32],
        };
        let version = BlobVersion::materialized(
            [7u8; 32],
            BackendRef::node_default(),
            EncodingClass::Raw,
            SystemTime::UNIX_EPOCH,
            UserId::nil(aruna_core::structs::identity::realm::RealmId::from_bytes(
                [1u8; 32],
            )),
            None,
        )
        .with_policies(vec![policy_ref])
        .expect("refs stored");

        let mut operation = operation();
        let _ = operation.first();
        let head = CurrentVersionPointer::new_with_generation(version_id, 7);
        let read = operation.next(read_result(Some(head.to_bytes().unwrap())));
        assert!(matches!(read, Ok(Next::Call(_))));
        let done = operation.next(read_result(Some(version.to_bytes().unwrap())));
        let Ok(Next::Done(placement)) = done else {
            panic!("read finishes");
        };
        assert_eq!(placement.version_id, version_id);
        assert_eq!(placement.generation, 7);
        assert_eq!(placement.policies, vec![policy_ref]);
    }

    #[test]
    fn missing_head_missing() {
        let mut operation = operation();
        let _ = operation.first();
        let missing = operation.next(read_result(None));
        assert_eq!(missing, Err(ObjectPlacementError::NoSuchKey));
    }
}
