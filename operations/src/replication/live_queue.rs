//! Queues live replication after a committed write and announces the upload to watchers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, watch_resource_path,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use tracing::warn;
use ulid::Ulid;

use super::queue::{LiveVersionInput, LiveVersionOperation};
use crate::driver::{DriverContext, drive};
use crate::notifications::watch::origin::EmitEventOperation;

#[allow(clippy::too_many_arguments)]
pub async fn complete_put(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    auth: AuthContext,
    group_id: GroupId,
    bucket: String,
    key: String,
    version_id: Ulid,
    size_bytes: u64,
) {
    let actor = auth.user_id;
    queue_live(
        context,
        node_id,
        auth,
        bucket.clone(),
        key.clone(),
        version_id,
        false,
    )
    .await;

    let path = watch_resource_path(group_id, node_id, &bucket, &key);
    let event = WatchEvent {
        event_id: Ulid::generate(),
        realm_id,
        kind: WatchEventKind::DataUploaded,
        path,
        actor,
        occurred_at_ms: unix_timestamp_millis(),
        detail: WatchEventDetail::DataUploaded {
            group_id,
            node_id,
            bucket,
            key,
            size_bytes,
        },
    };
    // Failures only warn inside the operation.
    let _ = drive(EmitEventOperation::new(event), context).await;
}

/// Queues continuous sync for one committed version or delete marker and wakes the drain.
pub async fn queue_live(
    context: &DriverContext,
    node_id: NodeId,
    auth: AuthContext,
    bucket: String,
    key: String,
    version_id: Ulid,
    delete_marker: bool,
) {
    match drive(
        LiveVersionOperation::new(LiveVersionInput {
            local_node_id: node_id,
            auth_context: auth,
            bucket: bucket.clone(),
            key: key.clone(),
            version_id,
            delete_marker,
        }),
        context,
    )
    .await
    {
        Ok(result) => {
            if result.queued > 0 && !result.scheduled {
                warn!(bucket, key, version_id = %version_id, queued = result.queued, "Live replication jobs persisted but drain scheduling was not acknowledged");
            }
        }
        Err(error) => {
            warn!(
                error = %error,
                bucket,
                key,
                version_id = %version_id,
                delete_marker,
                "Failed to queue live replication after committed write; durable obligation remains for repair"
            );
        }
    }
}
