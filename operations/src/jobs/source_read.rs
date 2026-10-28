//! Authorizes reading one run input on the node that holds it, exactly like an S3 read of the
//! object by the same caller: READ under the bucket's own group, with request policies.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::read_grant::{ReadGrant, SignedGrant};
use aruna_core::types::GroupId;
use thiserror::Error;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::{PolicyEnforcementError, PolicyRequestExtras};
use crate::driver::{DriverContext, drive};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};

/// The policy operation of a byte read, the same name the S3 surface uses.
pub const READ_OPERATION: &str = "s3.GetObject";
/// The policy operation of a metadata read or of one listed entry.
pub const HEAD_OPERATION: &str = "s3.HeadObject";

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

/// The group of the source bucket once `auth` may read `key` in it for `operation`.
pub async fn authorize_read(
    context: &DriverContext,
    auth: &AuthContext,
    node_id: NodeId,
    (bucket, key): (&str, &str),
    operation: &str,
) -> Result<GroupId, SourceReadError> {
    let group_id = bucket_group(context, bucket).await?;
    check(context, auth, node_id, group_id, (bucket, key), operation).await?;
    Ok(group_id)
}

/// Whether the Everyone principal may read the same object, policies included.
pub async fn public_read(
    context: &DriverContext,
    realm_id: RealmId,
    (node_id, group_id): (NodeId, GroupId),
    (bucket, key): (&str, &str),
) -> Result<bool, SourceReadError> {
    let anonymous = AuthContext::anonymous(realm_id);
    match check(
        context,
        &anonymous,
        node_id,
        group_id,
        (bucket, key),
        READ_OPERATION,
    )
    .await
    {
        Ok(()) => Ok(true),
        Err(SourceReadError::Denied) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Signs `grant` with this node's key; the issuer must be this node. `None` without a network.
pub fn issue_grant(context: &DriverContext, grant: ReadGrant) -> Option<SignedGrant> {
    let net = context.net_handle.as_ref()?;
    if grant.issuer != net.node_id() {
        return None;
    }
    let bytes = grant.signing_bytes().ok()?;
    Some(SignedGrant {
        signature: net.sign(&bytes),
        grant,
    })
}

async fn bucket_group(context: &DriverContext, bucket: &str) -> Result<GroupId, SourceReadError> {
    match Box::pin(drive(GetBucketOperation::new(bucket.to_string()), context)).await {
        Ok(info) => Ok(info.group_id),
        Err(GetBucketError::NotFound) => Err(SourceReadError::NotFound),
        Err(error) => Err(SourceReadError::Unavailable(error.to_string())),
    }
}

async fn check(
    context: &DriverContext,
    auth: &AuthContext,
    node_id: NodeId,
    group_id: GroupId,
    (bucket, key): (&str, &str),
    operation: &str,
) -> Result<(), SourceReadError> {
    let path = object_permission_path(auth.realm_id, group_id, node_id, bucket, key);
    let verdict = Box::pin(authorize(
        context,
        auth.realm_id,
        auth,
        &path,
        &Permission::READ,
        PolicyRequestExtras::operation(operation),
    ))
    .await;
    match verdict {
        Ok(()) => Ok(()),
        Err(
            AuthorizeError::PermissionDenied
            | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }),
        ) => Err(SourceReadError::Denied),
        Err(error) => Err(SourceReadError::Unavailable(error.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::format::Compression;
    use ulid::Ulid;

    use super::*;
    use crate::tests::access::seed_open_access;
    use crate::tests::s3::{test_context, test_storage};

    const BUCKET: &str = "refseq";

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
    async fn reads_under_bucket_group() {
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
    async fn applies_rules_and_policies() {
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
