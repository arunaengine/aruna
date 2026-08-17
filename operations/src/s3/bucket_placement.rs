//! Bucket default placement refs.
//!
//! The default governs versions minted after it is set; it never rewrites a
//! stored version. This operation owns the generation bump, because
//! `BucketInfo::with_policies` validates the set without advancing it.

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{BucketInfo, PlacementPolicyError, PlacementPolicyRef};
use aruna_core::types::{Effects, GroupId, Key, TxnId};
use smallvec::smallvec;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PutBucketPlacementInput {
    pub bucket: String,
    pub group_id: GroupId,
    pub policies: Vec<PlacementPolicyRef>,
    /// The generation the caller read. `Some` makes the change a compare-and-set,
    /// so a concurrent default change is refused instead of silently overwritten.
    pub expected_generation: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketPlacementDefault {
    pub policies: Vec<PlacementPolicyRef>,
    pub generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PutPlacementState {
    Init,
    StartTransaction,
    ReadBucket,
    WriteBucket,
    CommitTransaction,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutBucketPlacementError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error(transparent)]
    Policy(#[from] PlacementPolicyError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("the bucket changed owner while the default was written")]
    GroupMismatch,
    #[error("bucket default generation is {current}, not the expected {expected}")]
    GenerationConflict { expected: u64, current: u64 },
    #[error("no transaction found")]
    NoTransactionFound,
    #[error("unexpected event in state {state}")]
    InvalidStateEvent { state: &'static str },
}

/// Sets the bucket default ref set and advances `placement_policy_generation`
/// in the same transaction, so a write that read an older default is detectable.
#[derive(Debug, PartialEq)]
pub struct PutBucketPlacementOperation {
    input: PutBucketPlacementInput,
    state: PutPlacementState,
    txn_id: Option<TxnId>,
    output: Option<Result<BucketPlacementDefault, PutBucketPlacementError>>,
}

impl PutBucketPlacementOperation {
    pub fn new(input: PutBucketPlacementInput) -> Self {
        Self {
            input,
            state: PutPlacementState::Init,
            txn_id: None,
            output: None,
        }
    }

    fn bucket_key(&self) -> Key {
        self.input.bucket.as_bytes().to_vec().into()
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            PutPlacementState::Init => "Init",
            PutPlacementState::StartTransaction => "StartTransaction",
            PutPlacementState::ReadBucket => "ReadBucket",
            PutPlacementState::WriteBucket => "WriteBucket",
            PutPlacementState::CommitTransaction => "CommitTransaction",
            PutPlacementState::Finish => "Finish",
            PutPlacementState::Error => "Error",
        }
    }

    fn fail(&mut self, error: PutBucketPlacementError) -> Effects {
        self.state = PutPlacementState::Error;
        self.output = Some(Err(error));
        self.abort()
    }

    fn invalid_event(&mut self) -> Effects {
        let state = self.state_name();
        self.fail(PutBucketPlacementError::InvalidStateEvent { state })
    }

    fn finish(&mut self, default: BucketPlacementDefault) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(PutBucketPlacementError::NoTransactionFound);
        };
        self.output = Some(Ok(default));
        self.state = PutPlacementState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_bucket_read(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.invalid_event();
        };
        let Some(value) = value else {
            return self.fail(PutBucketPlacementError::NoSuchBucket);
        };
        let current = match BucketInfo::from_bytes(value.as_ref()) {
            Ok(current) => current,
            Err(error) => return self.fail(error.into()),
        };
        if current.group_id != self.input.group_id {
            return self.fail(PutBucketPlacementError::GroupMismatch);
        }
        if let Some(expected) = self.input.expected_generation
            && expected != current.placement_policy_generation
        {
            return self.fail(PutBucketPlacementError::GenerationConflict {
                expected,
                current: current.placement_policy_generation,
            });
        }
        let generation = current.placement_policy_generation;
        let previous = current.placement_policies.clone();
        let updated = match current.with_policies(self.input.policies.clone()) {
            Ok(updated) => updated,
            Err(error) => return self.fail(error.into()),
        };
        // An unchanged default is not a change: replay must not inflate the
        // generation and supersede runs that sealed the same refs.
        if updated.placement_policies == previous {
            return self.finish(BucketPlacementDefault {
                policies: previous,
                generation,
            });
        }
        self.write_default(updated, generation.saturating_add(1))
    }

    fn write_default(&mut self, mut updated: BucketInfo, generation: u64) -> Effects {
        updated.placement_policy_generation = generation;
        let policies = updated.placement_policies.clone();
        let value = match updated.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        self.output = Some(Ok(BucketPlacementDefault {
            policies,
            generation,
        }));
        self.state = PutPlacementState::WriteBucket;
        smallvec![Effect::Storage(StorageEffect::Write {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: self.bucket_key(),
            value: value.into(),
            txn_id: self.txn_id,
        })]
    }
}

impl Operation for PutBucketPlacementOperation {
    type Output = BucketPlacementDefault;
    type Error = PutBucketPlacementError;

    fn start(&mut self) -> Effects {
        self.state = PutPlacementState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            PutPlacementState::Init => self.start(),
            PutPlacementState::StartTransaction => {
                let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
                    return self.invalid_event();
                };
                self.txn_id = Some(txn_id);
                self.state = PutPlacementState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.bucket_key(),
                    txn_id: Some(txn_id),
                })]
            }
            PutPlacementState::ReadBucket => self.handle_bucket_read(event),
            PutPlacementState::WriteBucket => {
                let Event::Storage(StorageEvent::WriteResult { .. }) = event else {
                    return self.invalid_event();
                };
                let Some(txn_id) = self.txn_id else {
                    return self.fail(PutBucketPlacementError::NoTransactionFound);
                };
                self.state = PutPlacementState::CommitTransaction;
                smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
            }
            PutPlacementState::CommitTransaction => {
                let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
                    return self.invalid_event();
                };
                self.txn_id = None;
                self.state = PutPlacementState::Finish;
                smallvec![]
            }
            PutPlacementState::Finish => smallvec![],
            PutPlacementState::Error => self.abort(),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PutPlacementState::Finish | PutPlacementState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.output {
            Some(result) => result,
            None => Err(PutBucketPlacementError::InvalidStateEvent { state: "Finish" }),
        }
    }

    fn abort(&mut self) -> Effects {
        self.txn_id
            .take()
            .map_or_else(smallvec::SmallVec::new, |txn_id| {
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{PutBucketPlacementError, PutBucketPlacementInput, PutBucketPlacementOperation};
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{BucketInfo, PlacementPolicyRef, RealmId};
    use aruna_core::types::UserId;
    use std::time::UNIX_EPOCH;
    use ulid::Ulid;

    fn group_id() -> Ulid {
        Ulid::from_bytes([2u8; 16])
    }

    fn policy_ref(byte: u8) -> PlacementPolicyRef {
        PlacementPolicyRef {
            policy_id: Ulid::from_bytes([byte; 16]),
            digest: [byte; 32],
        }
    }

    fn stored(policies: Vec<PlacementPolicyRef>) -> BucketInfo {
        BucketInfo {
            group_id: group_id(),
            created_at: UNIX_EPOCH,
            created_by: UserId::nil(RealmId::from_bytes([1u8; 32])),
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
            placement_policies: policies,
            placement_policy_generation: 3,
        }
    }

    fn operation(
        policies: Vec<PlacementPolicyRef>,
        expected_generation: Option<u64>,
    ) -> PutBucketPlacementOperation {
        PutBucketPlacementOperation::new(PutBucketPlacementInput {
            bucket: "bucket".to_string(),
            group_id: group_id(),
            policies,
            expected_generation,
        })
    }

    fn started(operation: &mut PutBucketPlacementOperation) {
        operation.start();
        operation.step(Event::Storage(StorageEvent::TransactionStarted {
            txn_id: Ulid::from_bytes([4u8; 16]),
        }));
    }

    fn read(bucket: &BucketInfo) -> Event {
        Event::Storage(StorageEvent::ReadResult {
            key: Vec::new().into(),
            value: Some(bucket.to_bytes().expect("bucket encodes").into()),
        })
    }

    #[test]
    fn bumps_generation_once() {
        let mut operation = operation(vec![policy_ref(6), policy_ref(1)], Some(3));
        started(&mut operation);
        let effects = operation.step(read(&stored(Vec::new())));

        let [Effect::Storage(StorageEffect::Write { value, .. })] = effects.as_slice() else {
            panic!("expected one bucket write");
        };
        let written = BucketInfo::from_bytes(value.as_ref()).expect("bucket decodes");
        assert_eq!(written.placement_policy_generation, 4);
        assert_eq!(
            written.placement_policies,
            vec![policy_ref(1), policy_ref(6)]
        );
    }

    #[test]
    fn skips_unchanged_default() {
        // Replay must not inflate the generation and supersede sealed runs.
        let mut operation = operation(vec![policy_ref(1)], None);
        started(&mut operation);
        let effects = operation.step(read(&stored(vec![policy_ref(1)])));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::CommitTransaction { .. })]
        ));
        operation.step(Event::Storage(StorageEvent::TransactionCommitted {
            txn_id: Ulid::from_bytes([4u8; 16]),
        }));
        let default = operation.finalize().expect("default returned");
        assert_eq!(default.generation, 3);
    }

    #[test]
    fn rejects_stale_generation() {
        let mut operation = operation(vec![policy_ref(1)], Some(2));
        started(&mut operation);
        let effects = operation.step(read(&stored(Vec::new())));

        assert!(matches!(
            effects.as_slice(),
            [Effect::Storage(StorageEffect::AbortTransaction { .. })]
        ));
        assert_eq!(
            operation.finalize(),
            Err(PutBucketPlacementError::GenerationConflict {
                expected: 2,
                current: 3
            })
        );
    }

    #[test]
    fn rejects_other_group() {
        let mut operation = operation(Vec::new(), None);
        started(&mut operation);
        let mut foreign = stored(Vec::new());
        foreign.group_id = Ulid::from_bytes([9u8; 16]);
        operation.step(read(&foreign));

        assert_eq!(
            operation.finalize(),
            Err(PutBucketPlacementError::GroupMismatch)
        );
    }
}
