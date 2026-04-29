use aruna_core::NodeId;
use aruna_core::structs::{AuthContext, RealmId};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::replication::protocol::ReplicationMode;
use aruna_operations::replication::version_replication::{
    ReplicateScopeInput, ReplicateScopeOperation, ReplicateScopeTarget,
};
use aruna_operations::s3::put_bucket_replication::{
    GetBucketReplicationError, GetBucketReplicationOperation,
};
use std::sync::Arc;
use tracing::{Instrument, info, warn};
use ulid::Ulid;

#[allow(clippy::too_many_arguments)]
async fn trigger_version_replication(
    context: Arc<DriverContext>,
    _realm_id: RealmId,
    node_id: NodeId,
    auth_context: AuthContext,
    bucket: &str,
    key: &str,
    version_id: Ulid,
    delete_marker: bool,
) {
    let config = match drive(
        GetBucketReplicationOperation::new(bucket.to_string()),
        &context,
    )
    .await
    .and_then(|result| result.transpose())
    {
        Ok(Some(config)) => config,
        Ok(None) | Err(GetBucketReplicationError::NotFound) => return,
        Err(err) => {
            warn!(bucket, key, version_id = %version_id, error = %err, "Failed to load bucket replication config");
            return;
        }
    };

    for target in config.targets {
        if target.node_id == node_id {
            continue;
        }
        if delete_marker && !target.replicate_delete_markers {
            continue;
        }

        let input = ReplicateScopeInput {
            bucket: bucket.to_string(),
            target: ReplicateScopeTarget::Version {
                key: key.to_string(),
                version_id,
            },
            target_node_id: target.node_id,
            auth_context: auth_context.clone(),
            replicate_delete_markers: target.replicate_delete_markers,
            mode: ReplicationMode::Live,
        };

        match drive(ReplicateScopeOperation::new(input), &context)
            .await
            .and_then(|result| result.transpose())
        {
            Ok(Some(result)) if result.failed == 0 => {
                info!(bucket, key, version_id = %version_id, target_node = %target.node_id, "Version replication succeeded");
            }
            Ok(Some(result)) => {
                warn!(
                    bucket,
                    key,
                    version_id = %version_id,
                    target_node = %target.node_id,
                    replicated = result.replicated,
                    skipped = result.skipped,
                    failed = result.failed,
                    "Version replication completed with failures"
                );
            }
            Ok(None) => {
                warn!(
                    bucket,
                    key,
                    version_id = %version_id,
                    target_node = %target.node_id,
                    "Version replication produced no result"
                );
            }
            Err(err) => {
                warn!(
                    bucket,
                    key,
                    version_id = %version_id,
                    target_node = %target.node_id,
                    error = %err,
                    "Failed to trigger version replication"
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_version_replication(
    context: Arc<DriverContext>,
    realm_id: RealmId,
    node_id: NodeId,
    auth_context: AuthContext,
    bucket: String,
    key: String,
    version_id: Ulid,
    delete_marker: bool,
) {
    let span = tracing::info_span!(
        "s3.live_replication",
        bucket = %bucket,
        key = %key,
        version_id = %version_id,
        delete_marker,
    );
    tokio::spawn(
        async move {
            trigger_version_replication(
                context,
                realm_id,
                node_id,
                auth_context,
                &bucket,
                &key,
                version_id,
                delete_marker,
            )
            .await;
        }
        .instrument(span),
    );
}
