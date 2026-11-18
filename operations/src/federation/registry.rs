//! Decides and sends the realm's registry publication and arms its timer at startup.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::federation::RealmKpis;
use aruna_core::handle::Handle;
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::time::unix_timestamp_secs;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::warn;

use crate::driver::{DriverContext, drive};
use crate::federation::publish::{PUBLISH_INTERVAL, Publication, PublishOperation};
use crate::federation::registration::{RegistrationCounts, SendRegistrationOperation};
use crate::federation::totals::realm_totals;
use crate::metadata::stats::{count_realm_documents, count_realm_groups};
use crate::tasks::task_persistence::persist_task_effect;

/// Runs the decision, reads the counts a registration reports and sends the signed request.
pub(crate) async fn publish_registration(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    capabilities: &NodeCapabilities,
) -> Result<(), String> {
    let publication = drive(PublishOperation::new(realm_id, node_id), context)
        .await
        .map_err(|error| error.to_string())?;
    let now = unix_timestamp_secs();
    let counts = match publication {
        Publication::Register { .. } => RegistrationCounts {
            kpis: realm_kpis(context, realm_id).await,
            totals: realm_totals(context, realm_id, node_id)
                .await
                .inspect_err(|error| warn!(error = %error, "Realm totals unavailable"))
                .ok(),
        },
        Publication::Nothing | Publication::Withdraw { .. } => RegistrationCounts::default(),
    };
    let send = SendRegistrationOperation::new(
        (realm_id, node_id),
        capabilities.clone(),
        publication,
        counts,
        now,
    );
    (drive(send, context).await).map_err(|error| error.to_string())
}

/// The realm's counts; a count that fails is unknown (`None`), never zero.
async fn realm_kpis(context: &DriverContext, realm_id: RealmId) -> RealmKpis {
    RealmKpis {
        live_datasets: count_realm_documents(context, realm_id)
            .await
            .inspect_err(|error| warn!(error = %error, "Dataset count unavailable"))
            .ok()
            .flatten(),
        groups: count_realm_groups(context, realm_id)
            .await
            .inspect_err(|error| warn!(error = %error, "Group count unavailable"))
            .ok(),
        nodes_configured: None,
    }
}

/// Arms the registration timer at startup without postponing a restored due time. The due
/// time is persisted, so repeated restarts do not keep moving the first publication.
pub(crate) async fn restore_publish_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
    let effect = TaskEffect::ShortenTimer {
        key: TaskKey::PublishRegistration,
        after: PUBLISH_INTERVAL,
    };
    if let Err(message) = persist_task_effect(storage, &effect).await {
        warn!(message = %message, "Failed to persist registry publication timer");
    }
    if let Event::Task(TaskEvent::Error { message, .. }) =
        task_handle.send_effect(Effect::Task(effect)).await
    {
        warn!(message = %message, "Failed to arm registry publication timer");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::StorageEffect;
    use tempfile::tempdir;

    #[tokio::test]
    async fn failed_count_unknown() {
        // A count that fails is reported as `null`; a count that works reports zero as zero.
        let dir = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap())
                .unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
            graph_handle: None,
            outbound_handle: None,
        };
        let realm_id = RealmId::from_bytes([7; 32]);
        let kpis = realm_kpis(&context, realm_id).await;
        assert_eq!(kpis.groups, Some(0));
        context
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::GROUP_KEYSPACE.to_string(),
                key: b"broken".to_vec().into(),
                value: b"not a group".to_vec().into(),
                txn_id: None,
            })
            .await;
        let kpis = realm_kpis(&context, realm_id).await;
        let expected = serde_json::json!({
            "live_datasets": null,
            "groups": null,
            "nodes_configured": null
        });
        assert_eq!(serde_json::to_value(kpis).unwrap(), expected);
    }
}
