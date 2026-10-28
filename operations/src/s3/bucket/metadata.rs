//! Stores the metadata of an ordinary bucket on its own node, and reads any bucket's metadata:
//! an ordinary bucket's row, or the settings of its distributed set.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    BUCKET_CREATION_KEYSPACE, BUCKET_METADATA_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation};
use aruna_core::structs::storage::bucket_metadata::{BucketMetadata, StoredMetadata};
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use thiserror::Error;

use crate::driver::DriverContext;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutMetadataState {
    Init,
    StartTransaction,
    ReadBucket,
    ReadCreation,
    Write,
    Commit,
    Cancel,
    Finish,
    Error,
}

#[derive(Debug, Error, PartialEq)]
pub enum PutMetadataError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("The bucket changed owner while its metadata was written")]
    GroupMismatch,
    /// A distributed bucket keeps its metadata in the set's settings instead.
    #[error("the bucket is distributed")]
    Distributed,
    #[error("PutBucketMetadata did not finish")]
    NotFinished,
    #[error("Unexpected event in state {state:?}: expected {expected}, got {received:?}")]
    InvalidStateEvent {
        state: &'static str,
        expected: &'static str,
        received: Event,
    },
}

/// Sets or clears (`None`) an ordinary bucket's metadata, bound to its current incarnation. The
/// caller checked group admin on `group_id`; the bucket must still belong to that group.
#[derive(Debug, PartialEq)]
pub struct PutMetadataOperation {
    bucket: String,
    group_id: GroupId,
    metadata: Option<BucketMetadata>,
    state: PutMetadataState,
    txn_id: Option<TxnId>,
    outcome: Option<Result<(), PutMetadataError>>,
}

impl PutMetadataOperation {
    pub fn new(bucket: String, group_id: GroupId, metadata: Option<BucketMetadata>) -> Self {
        Self {
            bucket,
            group_id,
            metadata,
            state: PutMetadataState::Init,
            txn_id: None,
            outcome: None,
        }
    }

    fn key(&self) -> aruna_core::types::Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn fail(&mut self, error: PutMetadataError) -> Effects {
        self.outcome = Some(Err(error));
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = PutMetadataState::Cancel;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => {
                self.state = PutMetadataState::Error;
                smallvec![]
            }
        }
    }

    fn unexpected(&mut self, expected: &'static str, received: Event) -> Effects {
        let state = match self.state {
            PutMetadataState::StartTransaction => "StartTransaction",
            PutMetadataState::ReadBucket => "ReadBucket",
            PutMetadataState::ReadCreation => "ReadCreation",
            PutMetadataState::Write => "Write",
            PutMetadataState::Commit => "Commit",
            _ => "Other",
        };
        self.fail(PutMetadataError::InvalidStateEvent {
            state,
            expected,
            received,
        })
    }

    fn on_bucket(&mut self, txn_id: TxnId, value: Option<aruna_core::types::Value>) -> Effects {
        let Some(value) = value else {
            return self.fail(PutMetadataError::NoSuchBucket);
        };
        let info = match BucketInfo::from_bytes(value.as_ref()) {
            Ok(info) => info,
            Err(error) => return self.fail(error.into()),
        };
        if info.group_id != self.group_id {
            return self.fail(PutMetadataError::GroupMismatch);
        }
        if info.distribution.is_some() {
            return self.fail(PutMetadataError::Distributed);
        }
        self.state = PutMetadataState::ReadCreation;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BUCKET_CREATION_KEYSPACE.to_string(),
            key: self.key(),
            txn_id: Some(txn_id),
        })]
    }

    fn on_creation(&mut self, txn_id: TxnId, value: Option<aruna_core::types::Value>) -> Effects {
        self.state = PutMetadataState::Write;
        let key = self.key();
        let Some(metadata) = self.metadata.take() else {
            return smallvec![Effect::Storage(StorageEffect::Delete {
                key_space: BUCKET_METADATA_KEYSPACE.to_string(),
                key,
                txn_id: Some(txn_id),
            })];
        };
        let stored = StoredMetadata {
            incarnation: incarnation(value.as_ref().map(|value| value.as_ref())),
            metadata,
        };
        match stored.to_bytes() {
            Ok(bytes) => smallvec![Effect::Storage(StorageEffect::Write {
                key_space: BUCKET_METADATA_KEYSPACE.to_string(),
                key,
                value: bytes.into(),
                txn_id: Some(txn_id),
            })],
            Err(error) => self.fail(error.into()),
        }
    }
}

impl Operation for PutMetadataOperation {
    type Output = ();
    type Error = PutMetadataError;

    fn start(&mut self) -> Effects {
        self.state = PutMetadataState::StartTransaction;
        smallvec![Effect::Storage(StorageEffect::StartTransaction {
            read: false
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        match (self.state, event) {
            (PutMetadataState::Cancel, Event::Storage(StorageEvent::TransactionAborted { .. })) => {
                self.state = PutMetadataState::Error;
                smallvec![]
            }
            (PutMetadataState::Cancel, Event::Storage(StorageEvent::Error { .. })) => {
                self.state = PutMetadataState::Error;
                smallvec![]
            }
            (_, Event::Storage(StorageEvent::Error { error })) => self.fail(error.into()),
            (
                PutMetadataState::StartTransaction,
                Event::Storage(StorageEvent::TransactionStarted { txn_id }),
            ) => {
                self.txn_id = Some(txn_id);
                self.state = PutMetadataState::ReadBucket;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: S3_BUCKET_KEYSPACE.to_string(),
                    key: self.key(),
                    txn_id: Some(txn_id),
                })]
            }
            (
                PutMetadataState::ReadBucket,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => match self.txn_id {
                Some(txn_id) => self.on_bucket(txn_id, value),
                None => self.fail(PutMetadataError::NotFinished),
            },
            (
                PutMetadataState::ReadCreation,
                Event::Storage(StorageEvent::ReadResult { value, .. }),
            ) => match self.txn_id {
                Some(txn_id) => self.on_creation(txn_id, value),
                None => self.fail(PutMetadataError::NotFinished),
            },
            (
                PutMetadataState::Write,
                Event::Storage(
                    StorageEvent::WriteResult { .. } | StorageEvent::DeleteResult { .. },
                ),
            ) => match self.txn_id.take() {
                Some(txn_id) => {
                    self.state = PutMetadataState::Commit;
                    smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
                }
                None => self.fail(PutMetadataError::NotFinished),
            },
            (
                PutMetadataState::Commit,
                Event::Storage(StorageEvent::TransactionCommitted { .. }),
            ) => {
                self.state = PutMetadataState::Finish;
                self.outcome = Some(Ok(()));
                smallvec![]
            }
            (PutMetadataState::StartTransaction, received) => {
                self.unexpected("transaction start", received)
            }
            (PutMetadataState::ReadBucket | PutMetadataState::ReadCreation, received) => {
                self.unexpected("read result", received)
            }
            (PutMetadataState::Write, received) => self.unexpected("write result", received),
            (PutMetadataState::Commit, received) => self.unexpected("commit", received),
            (_, received) => self.unexpected("no event", received),
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            PutMetadataState::Finish | PutMetadataState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.outcome.unwrap_or(Err(PutMetadataError::NotFinished))
    }

    fn abort(&mut self) -> Effects {
        match self.txn_id.take() {
            Some(txn_id) => {
                self.state = PutMetadataState::Error;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            None => smallvec![],
        }
    }
}

/// The current metadata of a local bucket: its distributed set's settings, or the row of this
/// bucket's current incarnation. `None` when there is none.
pub async fn bucket_metadata(
    context: &DriverContext,
    bucket: &str,
    info: &BucketInfo,
) -> Result<Option<BucketMetadata>, String> {
    if let Some(binding) = &info.distribution {
        return crate::distribution::change::set_metadata(context, binding.set_id).await;
    }
    let key: aruna_core::types::Key = bucket.as_bytes().to_vec().into();
    let reads = vec![
        (BUCKET_METADATA_KEYSPACE.to_string(), key.clone()),
        (BUCKET_CREATION_KEYSPACE.to_string(), key),
    ];
    let event = context
        .storage_handle
        .send_storage_effect(StorageEffect::BatchRead {
            reads,
            txn_id: None,
        })
        .await;
    let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
        return Err(format!("bucket metadata read failed: {event:?}"));
    };
    let mut values = values.into_iter().map(|(_, value)| value);
    let (Some(row), creation) = (values.next().flatten(), values.next().flatten()) else {
        return Ok(None);
    };
    let stored = StoredMetadata::from_bytes(row.as_ref()).map_err(|error| error.to_string())?;
    let current = incarnation(creation.as_ref().map(|value| value.as_ref()));
    Ok((stored.incarnation == current).then_some(stored.metadata))
}
