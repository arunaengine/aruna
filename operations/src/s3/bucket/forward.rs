//! Handles a bucket creation forwarded by a peer node and answers with the result.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::DriverContext;
use crate::driver::drive;
use crate::metadata::protocol::AuthToken;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::placement::process_placements::load_realm_config;
use crate::s3::bucket::create::CreateBucketError;
use crate::s3::bucket::create::CreateBucketOperation;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_core::NodeId;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::blob::{
    BucketInfo, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::format::Compression;
use aruna_core::types::GroupId;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::warn;

use crate::forward::authorize::is_sync_eligible;
use crate::forward::transport::reject;

pub(crate) async fn apply_bucket_create(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    match message {
        MetadataTransportMessage::ForwardCreateBucket {
            auth_token,
            bucket,
            group_id,
        } => {
            let result = create_remote_bucket(context, peer, auth_token, &bucket, group_id).await;
            note_refusal(peer, &bucket, &result);
            MetadataTransportMessage::ForwardedBucketCreated { result }
        }
        MetadataTransportMessage::ForwardNewBucket {
            auth_token,
            bucket,
            group_id,
        } => {
            let result = create_once(context, peer, auth_token, &bucket, group_id).await;
            note_refusal(peer, &bucket, &result);
            MetadataTransportMessage::ForwardedNewBucket { result }
        }
        _ => reject("unexpected bucket create message"),
    }
}

fn note_refusal<T>(peer: NodeId, bucket: &str, result: &Result<T, SyncRefusal>) {
    if let Err(refusal) = result {
        warn!(
            %peer,
            %bucket,
            kind = crate::metadata::device_pull::refusal_kind(refusal),
            "Refused a forwarded bucket creation"
        );
    }
}

/// Creates the bucket, or accepts one of the same group the node already holds.
pub(super) async fn create_remote_bucket(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    bucket: &str,
    group_id: GroupId,
) -> Result<(), SyncRefusal> {
    if create_once(context, peer, auth_token, bucket, group_id).await? {
        return Ok(());
    }
    match drive(
        GetBucketOperation::new(bucket.to_string()),
        context.as_ref(),
    )
    .await
    {
        Ok(info) => accept_existing(&info, group_id, bucket),
        Err(_) => Err(SyncRefusal::Unavailable),
    }
}

/// Creates the bucket inside the node's create transaction. `Ok(false)`: the node already held
/// a bucket of that name, in any group, and created nothing.
async fn create_once(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    bucket: &str,
    group_id: GroupId,
) -> Result<bool, SyncRefusal> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(SyncRefusal::Unavailable)?;
    if !is_sync_eligible(&config, net_handle.node_id()) {
        return Err(SyncRefusal::Unavailable);
    }
    let auth = crate::metadata::device_pull::authorize_peer(context, peer, auth_token).await?;
    if auth.realm_id != realm_id {
        return Err(SyncRefusal::Unauthorized);
    }
    crate::metadata::device_pull::authorize_pull(
        context,
        &auth,
        aruna_core::structs::storage::blob::bucket_permission_path(
            realm_id,
            group_id,
            net_handle.node_id(),
            bucket,
        ),
        Permission::WRITE,
        "s3.CreateBucket",
    )
    .await?;

    let created = drive(
        CreateBucketOperation::new(
            bucket.to_string(),
            BucketInfo {
                group_id,
                created_at: SystemTime::now(),
                created_by: auth.user_id,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
                compression: Compression::Off,
                distribution: None,
            },
        )
        .mapped(),
        context.as_ref(),
    )
    .await;
    match created {
        Ok(_) => Ok(true),
        Err(error @ CreateBucketError::ProviderBucket(_)) => {
            Err(SyncRefusal::Invalid(error.to_string()))
        }
        Err(CreateBucketError::BucketAlreadyExists) => Ok(false),
        // A create that lost the commit race to one of the same name still reports existence.
        Err(_) => match drive(
            GetBucketOperation::new(bucket.to_string()),
            context.as_ref(),
        )
        .await
        {
            Ok(_) => Ok(false),
            Err(_) => Err(SyncRefusal::Unavailable),
        },
    }
}

/// Answers whether the forwarding device's owner may write under one prefix of a bucket this
/// node holds. Nothing is written; a publish asks before it copies anything.
pub(crate) async fn check_bucket_write(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::ForwardCheckBucket {
        auth_token,
        bucket,
        prefix,
    } = message
    else {
        return reject("unexpected bucket check message");
    };
    MetadataTransportMessage::ForwardedBucketChecked {
        result: bucket_writable(context, peer, auth_token, &bucket, &prefix).await,
    }
}

async fn bucket_writable(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    bucket: &str,
    prefix: &str,
) -> Result<(), SyncRefusal> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(SyncRefusal::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let node_id = net_handle.node_id();
    let config = load_realm_config(context, realm_id)
        .await
        .ok_or(SyncRefusal::Unavailable)?;
    if !is_sync_eligible(&config, node_id) {
        return Err(SyncRefusal::Unavailable);
    }
    let auth = crate::metadata::device_pull::authorize_peer(context, peer, auth_token).await?;
    if auth.realm_id != realm_id {
        return Err(SyncRefusal::Unauthorized);
    }
    let info = match drive(
        GetBucketOperation::new(bucket.to_string()),
        context.as_ref(),
    )
    .await
    {
        Ok(info) => info,
        Err(GetBucketError::NotFound) => return Err(SyncRefusal::NotFound),
        Err(_) => return Err(SyncRefusal::Unavailable),
    };
    let path = match prefix.is_empty() {
        true => bucket_permission_path(realm_id, info.group_id, node_id, bucket),
        false => object_permission_path(realm_id, info.group_id, node_id, bucket, prefix),
    };
    crate::metadata::device_pull::authorize_pull(
        context,
        &auth,
        path,
        Permission::WRITE,
        "s3.PutObject",
    )
    .await
}

/// A retried create succeeds only on an ordinary bucket of the same group; a member of a
/// distributed set is never taken for an ordinary bucket.
fn accept_existing(info: &BucketInfo, group_id: GroupId, bucket: &str) -> Result<(), SyncRefusal> {
    if info.distribution.is_some() {
        return Err(SyncRefusal::Invalid(format!(
            "bucket \"{bucket}\" belongs to a distributed bucket"
        )));
    }
    if info.group_id != group_id {
        return Err(SyncRefusal::Invalid(format!(
            "bucket \"{bucket}\" belongs to another group"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::accept_existing;
    use aruna_core::UserId;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::BucketInfo;
    use aruna_core::structs::storage::distribution::DistributionBinding;
    use aruna_core::structs::storage::format::Compression;
    use std::time::SystemTime;
    use ulid::Ulid;

    #[test]
    fn existing_bound_bucket_refused() {
        let group_id = Ulid::from_bytes([1; 16]);
        let mut info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::local(Ulid::from_bytes([2; 16]), RealmId::from_bytes([3; 32])),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        };
        assert!(accept_existing(&info, group_id, "b").is_ok());
        assert!(accept_existing(&info, Ulid::from_bytes([9; 16]), "b").is_err());
        info.distribution = Some(DistributionBinding {
            set_id: Ulid::from_bytes([4; 16]),
            member_id: Ulid::from_bytes([5; 16]),
            incarnation: 6,
            revision: 3,
        });
        assert!(accept_existing(&info, group_id, "b").is_err());
    }
}
