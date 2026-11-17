//! Sends the decided registration or withdrawal to the registry and arms its timer at startup.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_blob::blob::BlobHandle;
use aruna_core::NodeId;
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::federation::{FederationError, RealmKpis, Registration, Signed, Withdrawal};
use aruna_core::handle::Handle;
use aruna_core::structs::identity::auth::NodeCapabilities;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::task::{TaskEffect, TaskEvent, TaskKey};
use aruna_core::time::unix_timestamp_secs;
use aruna_storage::StorageHandle;
use aruna_tasks::TaskHandle;
use tracing::warn;
use url::Url;

use crate::driver::{DriverContext, drive};
use crate::federation::publish::{PUBLISH_INTERVAL, Publication, PublishOperation, still_due};
use crate::federation::totals::realm_totals;
use crate::metadata::stats::{count_realm_documents, count_realm_groups};
use crate::realm::get_config::GetConfigOperation;
use crate::tasks::task_persistence::persist_task_effect;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Runs the decision and sends the signed request through the screened egress client.
pub async fn publish_registration(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    capabilities: &NodeCapabilities,
) -> Result<(), String> {
    let publication = drive(PublishOperation::new(realm_id, node_id), context)
        .await
        .map_err(|error| error.to_string())?;
    let now = unix_timestamp_secs();
    let sign_error = |error: FederationError| error.to_string();
    let captured = publication.clone();
    let (method, registry_url, body, totals) = match publication {
        Publication::Nothing => return Ok(()),
        Publication::Register {
            registry_url,
            descriptor,
            nodes_configured,
        } => {
            let kpis = realm_kpis(context, realm_id, nodes_configured).await;
            let registration = Registration {
                descriptor,
                kpis,
                observed_at: now,
                issued_at: now,
            };
            let signed = Signed::sign(registration, capabilities).map_err(sign_error)?;
            let body = serde_json::to_vec(&signed).map_err(|error| error.to_string())?;
            let totals = signed_totals(context, realm_id, node_id, capabilities).await;
            (reqwest::Method::PUT, registry_url, body, totals)
        }
        Publication::Withdraw { registry_url, .. } => {
            let withdrawal = Withdrawal {
                realm_id,
                issued_at: now,
            };
            let signed = Signed::sign(withdrawal, capabilities).map_err(sign_error)?;
            let body = serde_json::to_vec(&signed).map_err(|error| error.to_string())?;
            (reqwest::Method::DELETE, registry_url, body, None)
        }
    };
    let blob = context
        .blob_handle
        .as_ref()
        .ok_or("no blob handle for registry egress")?;
    let url = realm_url(&registry_url, &realm_id)?;
    // The KPI counts take time; settings may have changed since the decision.
    let current = drive(GetConfigOperation::new(realm_id), context)
        .await
        .map_err(|error| error.to_string())?;
    if !still_due(&current, node_id, &captured) {
        return Ok(());
    }
    send(blob, method, url.clone(), body).await?;
    // Best effort: a registry from before the totals route keeps the registration.
    if let Some(body) = totals {
        let sent = async { send(blob, reqwest::Method::PUT, totals_url(url)?, body).await };
        if let Err(error) = sent.await {
            warn!(error = %error, "Registry refused the realm totals");
        }
    }
    Ok(())
}

/// The signed realm totals as a request body; `None` when a total is unknown.
async fn signed_totals(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    capabilities: &NodeCapabilities,
) -> Option<Vec<u8>> {
    let totals = realm_totals(context, realm_id, node_id)
        .await
        .inspect_err(|error| warn!(error = %error, "Realm totals unavailable"))
        .ok()?;
    let signed = Signed::sign(totals, capabilities)
        .inspect_err(|error| warn!(error = %error, "Signing realm totals failed"))
        .ok()?;
    serde_json::to_vec(&signed).ok()
}

/// Sends one signed JSON body through the screened egress client.
async fn send(
    blob: &BlobHandle,
    method: reqwest::Method,
    url: Url,
    body: Vec<u8>,
) -> Result<(), String> {
    let response = blob
        .repository_request(method, url)
        .map_err(|error| error.to_string())?
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .timeout(REQUEST_TIMEOUT)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!("registry answered {}", response.status()));
    }
    Ok(())
}

/// The totals route below a realm's registry route.
fn totals_url(mut realm_url: Url) -> Result<Url, String> {
    realm_url
        .path_segments_mut()
        .map_err(|()| "registry URL cannot take a path".to_string())?
        .push("totals");
    Ok(realm_url)
}

/// The realm's totals; a count that fails is unknown (`None`), never zero.
async fn realm_kpis(
    context: &DriverContext,
    realm_id: RealmId,
    nodes_configured: Option<u64>,
) -> RealmKpis {
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
        nodes_configured,
    }
}

/// The registry route of one realm below the configured registry base URL.
fn realm_url(registry_url: &Url, realm_id: &RealmId) -> Result<Url, String> {
    let mut base = registry_url.clone();
    if !base.path().ends_with('/') {
        base.set_path(&format!("{}/", base.path()));
    }
    base.join(&format!("v1/realms/{realm_id}"))
        .map_err(|error| error.to_string())
}

/// Arms the registration timer at startup without postponing a restored due time. The due
/// time is persisted, so repeated restarts do not keep moving the first publication.
pub async fn restore_publish_timer(storage: &StorageHandle, task_handle: &TaskHandle) {
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
        };
        let realm_id = RealmId::from_bytes([7; 32]);
        let kpis = realm_kpis(&context, realm_id, Some(2)).await;
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
        let kpis = realm_kpis(&context, realm_id, Some(2)).await;
        let expected = serde_json::json!({
            "live_datasets": null,
            "groups": null,
            "nodes_configured": 2
        });
        assert_eq!(serde_json::to_value(kpis).unwrap(), expected);
    }
}
