//! Decides whether one caller may read one object, as an S3 read of it by the same caller:
//! READ under the bucket's own group, with request policies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::AuthorizationError;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::types::{GroupId, Value};
use thiserror::Error;

use crate::auth::check_permissions::CheckPermissionsConfig;
use crate::auth::enforcement::{policy_reads, read_policies};
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// Why one source read is not allowed. Only `Denied` and `NotFound` are verdicts.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SourceReadError {
    #[error("source bucket not found")]
    NotFound,
    #[error("source read denied")]
    Denied,
    #[error("source read could not be decided: {0}")]
    Unavailable(String),
}

impl From<IoError> for SourceReadError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

/// A missing or malformed target denies; anything else leaves the read undecided.
fn check_failure(error: AuthorizationError) -> SourceReadError {
    match error {
        AuthorizationError::InvalidRealmId
        | AuthorizationError::InvalidGroupId
        | AuthorizationError::GroupNotFound
        | AuthorizationError::DocNotFound => SourceReadError::Denied,
        error => SourceReadError::Unavailable(error.to_string()),
    }
}

fn policy_failure(error: PolicyEnforcementError) -> SourceReadError {
    match error {
        PolicyEnforcementError::Denied { .. } => SourceReadError::Denied,
        error => SourceReadError::Unavailable(error.to_string()),
    }
}

/// The object one read check is about, and the S3 operation it stands for.
#[derive(Clone, Debug, PartialEq)]
pub struct SourceObject {
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
    pub operation: String,
}

/// Checks that `auth` may read one object and answers the group of its bucket. The caller's
/// grants and the request policies of that group are read together.
#[derive(Debug, PartialEq)]
pub struct SourceReadOperation {
    auth: AuthContext,
    object: SourceObject,
    group_id: Option<GroupId>,
}

type ReadNext = Result<Next<GroupId>, SourceReadError>;

impl SourceReadOperation {
    /// The check that `auth` may read `key` of `bucket` on `node_id` for `operation`.
    pub fn of_object(
        auth: &AuthContext,
        node_id: NodeId,
        (bucket, key): (&str, &str),
        operation: &str,
    ) -> Self {
        let object = SourceObject {
            node_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            operation: operation.to_string(),
        };
        Self::new(auth.clone(), object)
    }

    pub fn new(auth: AuthContext, object: SourceObject) -> Self {
        Self {
            auth,
            object,
            group_id: None,
        }
    }

    /// The same check for an object whose bucket group is already known.
    pub fn in_group(auth: AuthContext, object: SourceObject, group_id: GroupId) -> Self {
        Self {
            auth,
            object,
            group_id: Some(group_id),
        }
    }

    fn check(&self, group_id: GroupId) -> CheckPermissionsConfig {
        let object = &self.object;
        CheckPermissionsConfig {
            auth_context: self.auth.clone(),
            path: object_permission_path(
                self.auth.realm_id,
                group_id,
                object.node_id,
                &object.bucket,
                &object.key,
            ),
            required_permission: Permission::READ,
        }
    }

    fn read_rows(&mut self, group_id: GroupId) -> ReadNext {
        self.group_id = Some(group_id);
        let mut reads = self.check(group_id).reads().map_err(check_failure)?;
        reads.extend(policy_reads(self.auth.realm_id, Some(group_id)));
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn decide(&self, group_id: GroupId, mut rows: Vec<Option<Value>>) -> ReadNext {
        let check = self.check(group_id);
        let split = check.reads().map_err(check_failure)?.len();
        let policies = rows.split_off(split.min(rows.len()));
        if !check.allowed(rows).map_err(check_failure)? {
            return Err(SourceReadError::Denied);
        }
        let evaluator =
            read_policies(self.auth.realm_id, Some(group_id), policies).map_err(policy_failure)?;
        let request = policy_request_with(
            &check.path,
            &Permission::READ,
            Some(&self.auth),
            PolicyRequestExtras::operation(&self.object.operation),
        );
        evaluator.evaluate(&request).map_err(policy_failure)?;
        Ok(Next::Done(group_id))
    }
}

impl Operation for SourceReadOperation {
    type Output = GroupId;
    type Error = SourceReadError;

    fn first(&mut self) -> ReadNext {
        match self.group_id {
            Some(group_id) => self.read_rows(group_id),
            None => Ok(Next::Call(IoCall::child(GetBucketOperation::new(
                self.object.bucket.clone(),
            )))),
        }
    }

    fn next(&mut self, result: IoResult) -> ReadNext {
        let Some(group_id) = self.group_id else {
            return match result.child::<BucketInfo, GetBucketError>()? {
                Ok(info) => self.read_rows(info.group_id),
                Err(GetBucketError::NotFound) => Err(SourceReadError::NotFound),
                Err(error) => Err(SourceReadError::Unavailable(error.to_string())),
            };
        };
        match result {
            IoResult::Values(rows) => self.decide(group_id, rows),
            other => Err(IoError::Unexpected {
                expected: "IoResult::Values",
                got: other.label(),
            }
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    fn operation() -> SourceReadOperation {
        let object = SourceObject {
            node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
            bucket: "b".to_string(),
            key: "k".to_string(),
            operation: "s3.GetObject".to_string(),
        };
        SourceReadOperation::new(AuthContext::anonymous(RealmId([1; 32])), object)
    }

    // A missing bucket is its own verdict; a storage failure leaves the read undecided.
    #[test]
    fn bucket_decides_first() {
        let answer = |error| IoResult::Child(ChildOutcome::new(Err::<BucketInfo, _>(error)));
        let mut missing = operation();
        assert!(matches!(missing.first(), Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(
            missing.next(answer(GetBucketError::NotFound)),
            Err(SourceReadError::NotFound)
        );
        let mut broken = operation();
        broken.first().expect("bucket read");
        assert!(matches!(
            broken.next(answer(GetBucketError::Incomplete)),
            Err(SourceReadError::Unavailable(_))
        ));
    }

    // With the group known the rows are read at once, and absent grants deny the read.
    #[test]
    fn missing_grants_deny() {
        let group_id = Ulid::from_parts(2, 2);
        let mut known = operation();
        known.group_id = Some(group_id);
        let Ok(Next::Call(IoCall::Read(reads))) = known.first() else {
            panic!("the grants and policies are read");
        };
        let rows = IoResult::Values(vec![None; reads.len()]);
        assert_eq!(known.next(rows), Err(SourceReadError::Denied));
    }
}

#[cfg(test)]
mod storage_tests {
    use std::time::SystemTime;

    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
    use aruna_core::structs::storage::format::Compression;
    use ulid::Ulid;

    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::testing::test_context;
    use crate::testing_access::seed_open_access;
    use crate::tests::s3::test_storage;
    use aruna_core::NodeId;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::read_grant::READ_OPERATION;

    const BUCKET: &str = "refseq";

    async fn authorize_read(
        context: &DriverContext,
        auth: &AuthContext,
        node_id: NodeId,
        object: (&str, &str),
        operation: &str,
    ) -> Result<GroupId, SourceReadError> {
        let check = SourceReadOperation::of_object(auth, node_id, object, operation);
        drive(check, context).await
    }

    async fn public_read(
        context: &DriverContext,
        realm_id: RealmId,
        (node_id, group_id): (NodeId, GroupId),
        (bucket, key): (&str, &str),
    ) -> Result<bool, SourceReadError> {
        let object = SourceObject {
            node_id,
            bucket: bucket.to_string(),
            key: key.to_string(),
            operation: READ_OPERATION.to_string(),
        };
        let anonymous = AuthContext::anonymous(realm_id);
        let check = SourceReadOperation::in_group(anonymous, object, group_id);
        match drive(check, context).await {
            Ok(_) => Ok(true),
            Err(SourceReadError::Denied) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn scope() -> (RealmId, GroupId, NodeId) {
        let node_id = iroh::SecretKey::from_bytes(&[4; 32]).public();
        (
            RealmId::from_bytes([1; 32]),
            Ulid::from_parts(2, 2),
            node_id,
        )
    }

    /// A bucket of the scope's group whose owner is denied `secret/` and, with `public`,
    /// readable by everyone below `open/`; one request policy denies `blocked.txt`.
    async fn setup(public: bool) -> (tempfile::TempDir, DriverContext, AuthContext) {
        let (directory, storage) = test_storage();
        let context = test_context(storage);
        let (realm_id, group_id, node_id) = scope();
        let path = |key| object_permission_path(realm_id, group_id, node_id, BUCKET, key);
        let policy = RequestPolicy {
            policy_id: Ulid::from_parts(3, 3),
            name: "no-blocked".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: "path.endsWith('/blocked.txt')".to_string(),
            enabled: true,
        };
        let open = path("open/**");
        let owner = seed_open_access(
            &context,
            scope(),
            &path("secret/**"),
            public.then_some(open.as_str()),
            vec![policy],
        )
        .await;
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        let effect = StorageEffect::BatchWrite {
            writes: vec![(
                S3_BUCKET_KEYSPACE.to_string(),
                BUCKET.as_bytes().to_vec().into(),
                info.to_bytes().unwrap().into(),
            )],
            txn_id: None,
        };
        context.storage_handle.send_storage_effect(effect).await;
        (directory, context, owner)
    }

    async fn read(
        context: &DriverContext,
        auth: &AuthContext,
        key: &str,
    ) -> Result<GroupId, SourceReadError> {
        authorize_read(context, auth, scope().2, (BUCKET, key), READ_OPERATION).await
    }

    #[tokio::test]
    async fn uses_bucket_group() {
        // The bucket's own group decides, whichever group a run executes in.
        let (_directory, context, owner) = setup(false).await;
        assert_eq!(read(&context, &owner, "genome.fna").await, Ok(scope().1));
        let stranger = AuthContext {
            user_id: UserId::local(Ulid::from_parts(9, 9), scope().0),
            ..owner.clone()
        };
        assert_eq!(
            read(&context, &stranger, "genome.fna").await,
            Err(SourceReadError::Denied)
        );
        let missing = authorize_read(&context, &owner, scope().2, ("absent", "a"), READ_OPERATION);
        assert_eq!(missing.await, Err(SourceReadError::NotFound));
    }

    #[tokio::test]
    async fn rules_and_policies() {
        // A deny rule and a request policy refuse a read the group role alone would allow.
        let (_directory, context, owner) = setup(false).await;
        assert_eq!(
            read(&context, &owner, "secret/x").await,
            Err(SourceReadError::Denied)
        );
        assert_eq!(
            read(&context, &owner, "a/blocked.txt").await,
            Err(SourceReadError::Denied)
        );
    }

    #[tokio::test]
    async fn public_needs_role() {
        // Only a role for everyone makes an input public, and a policy still applies to it.
        let object = |key| (BUCKET, key);
        let (realm_id, group_id, node_id) = scope();
        let (_directory, context, _owner) = setup(false).await;
        let closed = public_read(&context, realm_id, (node_id, group_id), object("open/a"));
        assert_eq!(closed.await, Ok(false));
        let (_directory, context, _owner) = setup(true).await;
        let open = public_read(&context, realm_id, (node_id, group_id), object("open/a"));
        assert_eq!(open.await, Ok(true));
        let outside = public_read(&context, realm_id, (node_id, group_id), object("b"));
        assert_eq!(outside.await, Ok(false));
        let blocked = public_read(
            &context,
            realm_id,
            (node_id, group_id),
            object("open/blocked.txt"),
        );
        assert_eq!(blocked.await, Ok(false));
    }
}
