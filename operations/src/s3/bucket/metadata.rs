//! Stores the metadata of an ordinary bucket on its own node, and reads any bucket's metadata:
//! an ordinary bucket's row, or the settings of its distributed set.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    BUCKET_CREATION_KEYSPACE, BUCKET_METADATA_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation};
use aruna_core::structs::storage::bucket_metadata::{BucketMetadata, StoredMetadata};
use aruna_core::structs::storage::dataset_description::{DatasetDescription, describe};
use aruna_core::types::{Effects, GroupId, TxnId};
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::driver::DriverContext;
use crate::metadata::AuthToken;
use crate::metadata::api::{
    ExportMetadataRequest, ExportMetadataResult, MetadataApiError, RoCrateExportView,
};
use crate::metadata::forward::export_rocrate_routed;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutMetadataState {
    Init,
    StartTransaction,
    ReadBucket,
    Authorize,
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
    /// The caller lost admin rights on the bucket's group before the write committed.
    #[error("the caller may not administer the bucket's group")]
    Forbidden,
    /// A distributed bucket keeps its metadata in the set's settings instead.
    #[error("the bucket is distributed")]
    Distributed,
    #[error("the bucket metadata changed meanwhile")]
    Changed,
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
/// bucket must still belong to `group_id`, and `auth` must administer it, in the write's own
/// transaction.
#[derive(Debug, PartialEq)]
pub struct PutMetadataOperation {
    auth: AuthContext,
    realm_id: RealmId,
    bucket: String,
    group_id: GroupId,
    metadata: Option<BucketMetadata>,
    /// Writes only while the stored metadata is still this; none counts as the default.
    expected: Option<BucketMetadata>,
    state: PutMetadataState,
    txn_id: Option<TxnId>,
    outcome: Option<Result<(), PutMetadataError>>,
}

impl PutMetadataOperation {
    pub fn new(
        (auth, realm_id): (AuthContext, RealmId),
        bucket: String,
        group_id: GroupId,
        metadata: Option<BucketMetadata>,
    ) -> Self {
        Self {
            auth,
            realm_id,
            bucket,
            group_id,
            metadata,
            expected: None,
            state: PutMetadataState::Init,
            txn_id: None,
            outcome: None,
        }
    }

    /// Writes only while the bucket's current metadata is still `current`.
    pub fn expecting(mut self, current: BucketMetadata) -> Self {
        self.expected = Some(current);
        self
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
            PutMetadataState::Authorize => "Authorize",
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
        // Roles are read in this transaction, so a revoked admin conflicts the commit.
        self.state = PutMetadataState::Authorize;
        let check = CheckPermissionsConfig {
            auth_context: self.auth.clone(),
            path: group_admin_path(self.realm_id, self.group_id),
            required_permission: Permission::WRITE,
        };
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new_with_txn(check, txn_id),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn on_authorized(&mut self, txn_id: TxnId, allowed: bool) -> Effects {
        if !allowed {
            return self.fail(PutMetadataError::Forbidden);
        }
        self.state = PutMetadataState::ReadCreation;
        let reads = vec![
            (BUCKET_CREATION_KEYSPACE.to_string(), self.key()),
            (BUCKET_METADATA_KEYSPACE.to_string(), self.key()),
        ];
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads,
            txn_id: Some(txn_id),
        })]
    }

    fn on_creation(
        &mut self,
        txn_id: TxnId,
        (value, row): (
            Option<aruna_core::types::Value>,
            Option<aruna_core::types::Value>,
        ),
    ) -> Effects {
        if let Some(expected) = &self.expected {
            let current = match current_row(value.as_ref(), row.as_ref()) {
                Ok(current) => current.unwrap_or_default(),
                Err(error) => return self.fail(error.into()),
            };
            if current != *expected {
                return self.fail(PutMetadataError::Changed);
            }
        }
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
                PutMetadataState::Authorize,
                Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
            ) => match self.txn_id {
                Some(txn_id) => self.on_authorized(txn_id, matches!(allowed, Ok(true))),
                None => self.fail(PutMetadataError::NotFinished),
            },
            (
                PutMetadataState::ReadCreation,
                Event::Storage(StorageEvent::BatchReadResult { values }),
            ) => match self.txn_id {
                Some(txn_id) => {
                    let mut values = values.into_iter().map(|(_, value)| value);
                    let read = (values.next().flatten(), values.next().flatten());
                    self.on_creation(txn_id, read)
                }
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
            (PutMetadataState::ReadBucket, received) => self.unexpected("read result", received),
            (PutMetadataState::ReadCreation, received) => {
                self.unexpected("batch read result", received)
            }
            (PutMetadataState::Authorize, received) => {
                self.unexpected("authorization result", received)
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
    let (row, creation) = (values.next().flatten(), values.next().flatten());
    current_row(creation.as_ref(), row.as_ref()).map_err(|error| error.to_string())
}

/// The metadata a stored row gives the bucket's current incarnation.
fn current_row(
    creation: Option<&aruna_core::types::Value>,
    row: Option<&aruna_core::types::Value>,
) -> Result<Option<BucketMetadata>, ConversionError> {
    let Some(row) = row else {
        return Ok(None);
    };
    let stored = StoredMetadata::from_bytes(row.as_ref())?;
    let current = incarnation(creation.map(|value| value.as_ref()));
    Ok((stored.incarnation == current).then_some(stored.metadata))
}

/// Why a bucket's dataset gives no description.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum DescriptionError {
    #[error("the dataset does not exist or the caller may not read it")]
    Unreadable,
    #[error("{0}")]
    Invalid(String),
    #[error("the dataset could not be read now")]
    Unavailable,
}

/// Bytes the summary of a dataset may have; only its root entity is read.
const SUMMARY_BYTES: u64 = 16 * 1024 * 1024;

/// What the dataset `dataset` says about a bucket, read from its holders with the caller's own
/// access, so a reader never sees a description it may not read.
pub async fn dataset_description(
    context: &DriverContext,
    realm_id: RealmId,
    dataset: Ulid,
    (auth, token): (Option<&AuthContext>, Option<AuthToken>),
) -> Result<DatasetDescription, DescriptionError> {
    let request = ExportMetadataRequest {
        document_id: dataset,
        auth: auth.cloned(),
        view: RoCrateExportView::Summary,
        limit: None,
        offset: None,
        after: None,
    };
    let context = std::sync::Arc::new(context.clone());
    match export_rocrate_routed(&context, realm_id, request, token, SUMMARY_BYTES).await {
        Ok(ExportMetadataResult::Summary { jsonld, .. }) => {
            describe(&jsonld).map_err(DescriptionError::Invalid)
        }
        Err(
            MetadataApiError::NotFound
            | MetadataApiError::Forbidden
            | MetadataApiError::Unauthorized
            | MetadataApiError::BadRequest,
        ) => Err(DescriptionError::Unreadable),
        Ok(_) | Err(_) => Err(DescriptionError::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::drive;
    use aruna_core::UserId;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use aruna_core::structs::storage::format::Compression;
    use std::time::SystemTime;

    async fn write(context: &DriverContext, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let effect = StorageEffect::Write {
            key_space: key_space.to_string(),
            key: key.into(),
            value: value.into(),
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
    }

    #[tokio::test]
    async fn admin_checked_in_write() {
        // Only an admin of the bucket's group writes its metadata, checked in the write itself.
        let temp = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(temp.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let realm_id = RealmId::from_bytes([1; 32]);
        let admin = UserId::local(Ulid::from_bytes([2; 16]), realm_id);
        let stranger = UserId::local(Ulid::from_bytes([3; 16]), realm_id);
        let group_id = Ulid::from_bytes([4; 16]);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[5; 32]).public(),
            user_id: admin,
            realm_id,
        };
        let realm = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let group = GroupAuthorizationDocument::default_group_doc(admin, realm_id, group_id);
        write(
            &context,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            realm.to_bytes(&actor).unwrap(),
        )
        .await;
        write(
            &context,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(&actor).unwrap(),
        )
        .await;
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: admin,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        write(
            &context,
            S3_BUCKET_KEYSPACE,
            b"db".to_vec(),
            info.to_bytes().unwrap(),
        )
        .await;
        let metadata = BucketMetadata {
            database: true,
            ..BucketMetadata::default()
        };
        let put = |user: UserId| {
            let auth = AuthContext {
                user_id: user,
                realm_id,
                path_restrictions: None,
                session: None,
            };
            PutMetadataOperation::new(
                (auth, realm_id),
                "db".into(),
                group_id,
                Some(metadata.clone()),
            )
        };
        assert_eq!(
            drive(put(stranger), &context).await,
            Err(PutMetadataError::Forbidden)
        );
        assert_eq!(bucket_metadata(&context, "db", &info).await, Ok(None));
        drive(put(admin), &context).await.unwrap();
        assert_eq!(
            bucket_metadata(&context, "db", &info).await,
            Ok(Some(metadata.clone()))
        );
        // A conditional write applies only while the stored metadata is what it expects.
        let stale = put(admin).expecting(BucketMetadata::default());
        assert_eq!(drive(stale, &context).await, Err(PutMetadataError::Changed));
        let current = put(admin).expecting(metadata.clone());
        drive(current, &context).await.unwrap();
    }
}
