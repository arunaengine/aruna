use crate::driver::DriverContext;
use crate::driver::drive;
use crate::metadata::protocol::AuthToken;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::placement::process_placements::load_realm_config;
use crate::s3::bucket::create::CreateBucketError;
use crate::s3::bucket::create::CreateBucketOperation;
use crate::s3::bucket::get::GetBucketOperation;
use aruna_core::NodeId;
use aruna_core::structs::BucketInfo;
use aruna_core::structs::Permission;
use aruna_core::structs::SyncRefusal;
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
    let MetadataTransportMessage::ForwardCreateBucket {
        auth_token,
        bucket,
        group_id,
    } = message
    else {
        return reject("unexpected bucket create message");
    };
    let result = create_remote_bucket(context, peer, auth_token, &bucket, group_id).await;
    if let Err(refusal) = &result {
        warn!(
            %peer,
            %bucket,
            kind = crate::metadata::device_pull::refusal_kind(refusal),
            "Refused a forwarded bucket creation"
        );
    }
    MetadataTransportMessage::ForwardedBucketCreated { result }
}

pub(super) async fn create_remote_bucket(
    context: &Arc<DriverContext>,
    peer: NodeId,
    auth_token: AuthToken,
    bucket: &str,
    group_id: GroupId,
) -> Result<(), SyncRefusal> {
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
        aruna_core::structs::bucket_permission_path(
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
            },
        ),
        context.as_ref(),
    )
    .await;
    match created {
        Ok(_) => Ok(()),
        Err(CreateBucketError::BucketAlreadyExists) => {
            match drive(
                GetBucketOperation::new(bucket.to_string()),
                context.as_ref(),
            )
            .await
            {
                Ok(info) if info.group_id == group_id => Ok(()),
                Ok(_) => Err(SyncRefusal::Invalid(format!(
                    "bucket \"{bucket}\" belongs to another group"
                ))),
                Err(_) => Err(SyncRefusal::Unavailable),
            }
        }
        Err(_) => Err(SyncRefusal::Unavailable),
    }
}
