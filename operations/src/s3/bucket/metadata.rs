//! Stores the metadata of an ordinary bucket on its own node, and reads any bucket's metadata:
//! an ordinary bucket's row, or the settings of its distributed set.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::dataset::current_row;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::{ConversionError, StorageError};
use aruna_core::keyspaces::{
    BUCKET_CREATION_KEYSPACE, BUCKET_METADATA_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::all_values;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::blob::{BucketInfo, incarnation};
use aruna_core::structs::storage::bucket_metadata::{BucketMetadata, StoredMetadata};
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;

use crate::auth::check_permissions::CheckPermissionsConfig;

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
}

impl From<IoError> for PutMetadataError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.into())
    }
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
        }
    }

    /// Writes only while the bucket's current metadata is still `current` or already the new one.
    pub fn expecting(mut self, current: BucketMetadata) -> Self {
        self.expected = Some(current);
        self
    }

    fn key(&self) -> Key {
        self.bucket.as_bytes().to_vec().into()
    }

    fn check(&self) -> CheckPermissionsConfig {
        CheckPermissionsConfig {
            auth_context: self.auth.clone(),
            path: group_admin_path(self.realm_id, self.group_id),
            required_permission: Permission::WRITE,
        }
    }

    /// The bucket, the creation and metadata rows, then the admin roles: all read in the
    /// write's transaction, so a revoked admin conflicts the commit.
    fn reads(&self) -> Result<Vec<(String, Key)>, PutMetadataError> {
        let mut reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), self.key()),
            (BUCKET_CREATION_KEYSPACE.to_string(), self.key()),
            (BUCKET_METADATA_KEYSPACE.to_string(), self.key()),
        ];
        let roles = self.check().reads();
        reads.extend(roles.map_err(|_| PutMetadataError::Forbidden)?);
        Ok(reads)
    }

    fn write(&mut self, values: Vec<Option<Value>>) -> Result<IoCall, PutMetadataError> {
        let mut values = values.into_iter();
        let mut next = || values.next().flatten();
        let (bucket, creation, row) = (next(), next(), next());
        let bucket = bucket.ok_or(PutMetadataError::NoSuchBucket)?;
        let info = BucketInfo::from_bytes(bucket.as_ref())?;
        if info.group_id != self.group_id {
            return Err(PutMetadataError::GroupMismatch);
        }
        if info.distribution.is_some() {
            return Err(PutMetadataError::Distributed);
        }
        if !matches!(self.check().allowed(values.collect()), Ok(true)) {
            return Err(PutMetadataError::Forbidden);
        }
        if let Some(expected) = &self.expected {
            let current = current_row(creation.as_ref(), row.as_ref())?.unwrap_or_default();
            // A retry of a write that already applied finds its own metadata and succeeds.
            let applied = current == self.metadata.clone().unwrap_or_default();
            if current != *expected && !applied {
                return Err(PutMetadataError::Changed);
            }
        }
        let key = self.key();
        let Some(metadata) = self.metadata.take() else {
            return Ok(IoCall::Delete(vec![(
                BUCKET_METADATA_KEYSPACE.to_string(),
                key,
            )]));
        };
        let stored = StoredMetadata {
            incarnation: incarnation(creation.as_ref().map(|value| value.as_ref())),
            metadata,
        };
        let row = (
            BUCKET_METADATA_KEYSPACE.to_string(),
            key,
            stored.to_bytes()?.into(),
        );
        Ok(IoCall::Write(vec![row]))
    }
}

impl Operation for PutMetadataOperation {
    type Output = ();
    type Error = PutMetadataError;

    fn first(&mut self) -> Result<Next<()>, PutMetadataError> {
        Ok(Next::Call(IoCall::Read(self.reads()?)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, PutMetadataError> {
        if let IoResult::Done = result {
            return Ok(Next::Done(()));
        }
        Ok(Next::Call(self.write(all_values(result)?)?))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::s3::bucket::dataset::BucketMetadataOperation;
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::AUTH_KEYSPACE;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::GroupAuthorizationDocument;
    use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
    use aruna_core::structs::storage::format::Compression;
    use std::time::SystemTime;
    use ulid::Ulid;

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
    async fn write_checks_admin() {
        // Only an admin of the bucket's group writes its metadata, checked in the write itself.
        let temp = tempfile::tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(temp.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
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
        let read = || drive(BucketMetadataOperation::new("db", &info), &context);
        assert_eq!(read().await, Ok(None));
        drive(put(admin), &context).await.unwrap();
        assert_eq!(read().await, Ok(Some(metadata.clone())));
        // A conditional write applies only while the stored metadata is what it expects, or
        // already holds what it writes, so a retry of an applied write succeeds.
        let mut stale = put(admin).expecting(BucketMetadata::default());
        stale.metadata = None;
        assert_eq!(drive(stale, &context).await, Err(PutMetadataError::Changed));
        let retried = put(admin).expecting(BucketMetadata::default());
        drive(retried, &context).await.unwrap();
        let current = put(admin).expecting(metadata.clone());
        drive(current, &context).await.unwrap();
    }
}
