//! The placement references one object's current head carries, with the head
//! generation an exact-set mutation has to present.
//!
//! Read-only and node-local: it reports what this node stores, and the caller
//! authorizes the object read before driving it.

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::{
    BlobHeadKey, BlobVersion, CurrentVersionPointer, PlacementPolicyRef, VersionKey,
};
use aruna_core::types::Effects;
use smallvec::smallvec;
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
    #[error("unexpected event during the object placement read")]
    InvalidEvent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadState {
    Init,
    ReadHead,
    ReadVersion,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct ObjectPlacementOperation {
    input: ObjectPlacementInput,
    head: Option<CurrentVersionPointer>,
    state: ReadState,
    output: Option<Result<ObjectPlacement, ObjectPlacementError>>,
}

impl ObjectPlacementOperation {
    pub fn new(input: ObjectPlacementInput) -> Self {
        Self {
            input,
            head: None,
            state: ReadState::Init,
            output: None,
        }
    }

    fn finish(&mut self, result: Result<ObjectPlacement, ObjectPlacementError>) -> Effects {
        self.state = match result.is_ok() {
            true => ReadState::Finish,
            false => ReadState::Error,
        };
        self.output = Some(result);
        smallvec![]
    }

    fn read_version(&mut self, pointer: CurrentVersionPointer) -> Effects {
        let key = match VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id)
            .to_bytes()
        {
            Ok(key) => key,
            Err(error) => return self.finish(Err(error.into())),
        };
        self.head = Some(pointer);
        self.state = ReadState::ReadVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }
}

impl Operation for ObjectPlacementOperation {
    type Output = ObjectPlacement;
    type Error = ObjectPlacementError;

    fn start(&mut self) -> Effects {
        let key = match BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes() {
            Ok(key) => key,
            Err(error) => return self.finish(Err(error.into())),
        };
        self.state = ReadState::ReadHead;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_HEAD_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: None,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            ReadState::Init => self.start(),
            ReadState::ReadHead => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.finish(Err(ObjectPlacementError::InvalidEvent));
                };
                let Some(value) = value else {
                    return self.finish(Err(ObjectPlacementError::NoSuchKey));
                };
                match CurrentVersionPointer::from_bytes(value.as_ref()) {
                    Ok(pointer) => self.read_version(pointer),
                    Err(error) => self.finish(Err(error.into())),
                }
            }
            ReadState::ReadVersion => {
                let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
                    return self.finish(Err(ObjectPlacementError::InvalidEvent));
                };
                let (Some(value), Some(head)) = (value, self.head.take()) else {
                    return self.finish(Err(ObjectPlacementError::NoSuchKey));
                };
                match BlobVersion::from_bytes(value.as_ref()) {
                    Ok(version) => self.finish(Ok(ObjectPlacement {
                        version_id: head.version_id,
                        generation: head.generation,
                        policies: version.placement_policies,
                    })),
                    Err(error) => self.finish(Err(error.into())),
                }
            }
            ReadState::Finish | ReadState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, ReadState::Finish | ReadState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output
            .unwrap_or(Err(ObjectPlacementError::InvalidEvent))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }

    fn expected_error(error: &Self::Error) -> bool {
        matches!(error, ObjectPlacementError::NoSuchKey)
    }
}

#[cfg(test)]
mod tests {
    use super::{ObjectPlacementError, ObjectPlacementInput, ObjectPlacementOperation};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{BackendRef, BlobVersion, CurrentVersionPointer, PlacementPolicyRef};
    use aruna_core::types::UserId;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn operation() -> ObjectPlacementOperation {
        ObjectPlacementOperation::new(ObjectPlacementInput {
            bucket: "raw".to_string(),
            key: "run1.tar".to_string(),
        })
    }

    fn read_result(value: Option<Vec<u8>>) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: b"k".to_vec().into(),
            value: value.map(Into::into),
        })
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
            SystemTime::UNIX_EPOCH,
            UserId::nil(aruna_core::structs::RealmId::from_bytes([1u8; 32])),
            None,
        )
        .with_policies(vec![policy_ref])
        .expect("refs stored");

        let mut operation = operation();
        operation.start();
        operation.step(read_result(Some(
            CurrentVersionPointer::new_with_generation(version_id, 7)
                .to_bytes()
                .unwrap(),
        )));
        operation.step(read_result(Some(version.to_bytes().unwrap())));

        let placement = operation.finalize().expect("read finishes");
        assert_eq!(placement.version_id, version_id);
        assert_eq!(placement.generation, 7);
        assert_eq!(placement.policies, vec![policy_ref]);
    }

    #[test]
    fn missing_head_missing() {
        let mut operation = operation();
        operation.start();
        operation.step(read_result(None));

        assert_eq!(operation.finalize(), Err(ObjectPlacementError::NoSuchKey));
    }
}
