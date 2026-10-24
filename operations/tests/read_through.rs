//! Tests that a document the owner reads online on a device stays there as an unselected copy.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::topology;

use std::time::Duration;

use aruna_core::StructuredId;
use aruna_operations::device::drain::drain_publish_queue;
use aruna_operations::device::realm_documents::fetch_realm_documents;
use aruna_operations::device::replica::read_replica;
use aruna_operations::device::selection::select_document;
use aruna_operations::metadata::api::{
    ExportMetadataRequest, GetVisibleRequest, RoCrateExportView,
};
use aruna_operations::metadata::create_document::mint_local_document;
use aruna_operations::metadata::forward::{
    export_rocrate_routed, get_metadata_routed, route_metadata_update,
};
use aruna_operations::metadata::materialization_queue::process_materialization_batch;
use aruna_operations::metadata::projector::replay_event_log;
use aruna_operations::metadata::update_document::UpdateDocumentMutation;
use topology::{TestNode, TestResult, Topology, wait_until};
use ulid::Ulid;

/// Only ends a hung exchange; never a performance assertion.
const FETCH_BUDGET: Duration = Duration::from_secs(30);
/// Far above one small document's export.
const EXPORT_BYTES: u64 = 16_000_000;

/// Whether the device holds a copy that answers offline and whose installed graph is exactly
/// what it displays, so a later local edit plans against the bytes the owner saw.
async fn usable(device: &TestNode, document_id: Ulid) -> bool {
    let Some(replica) = read_replica(&device.context, document_id).await else {
        return false;
    };
    let Some(metadata) = device.context.metadata_handle.as_ref() else {
        return false;
    };
    let Ok(installed) = metadata
        .export_rocrate_jsonld(replica.graph_iri.clone())
        .await
    else {
        return false;
    };
    replica.serves_reads() && digest(&installed) == digest(&replica.displayed_jsonld)
}

fn digest(jsonld: &str) -> Option<[u8; 32]> {
    craqle::canonicalize_jsonld(jsonld)
        .ok()
        .map(|canonical| canonical.digest)
}

#[tokio::test]
async fn keeps_owner_reads() -> TestResult<()> {
    let realm = Topology::spawn(2, 1, 1).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    assert!(fetch_realm_documents(&device.context, FETCH_BUDGET).await);
    let path = "datasets/field-notes";
    let origin = realm.leading_node(group_id, path);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    realm
        .create_document(origin, group_id, document_id, path, "notes")
        .await?;
    let read = || async {
        get_metadata_routed(
            &device.context,
            realm.realm_id,
            GetVisibleRequest {
                document_id,
                auth: Some(realm.auth_context()),
            },
            Some(realm.bearer_token()),
        )
        .await
        .is_ok_and(|record| record.document_id == document_id)
            && usable(device, document_id).await
    };
    // A holder serves the graph once its render is ready, so a read may come before the copy.
    wait_until("the owner's read leaves a copy", device.node_id(), read).await?;

    let replica = read_replica(&device.context, document_id)
        .await
        .ok_or("the owner's read left no copy on the device")?;
    assert!(!replica.selected, "a read copy is a cache, not a selection");
    Ok(())
}

/// A cold RO-Crate export by the owner leaves the same offline copy a read does.
#[tokio::test]
async fn keeps_owner_exports() -> TestResult<()> {
    let realm = Topology::spawn(2, 1, 1).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    assert!(fetch_realm_documents(&device.context, FETCH_BUDGET).await);
    let path = "datasets/exported";
    let origin = realm.leading_node(group_id, path);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    realm
        .create_document(origin, group_id, document_id, path, "notes")
        .await?;
    let export = || async {
        export_rocrate_routed(
            &device.context,
            realm.realm_id,
            ExportMetadataRequest {
                document_id,
                auth: Some(realm.auth_context()),
                view: RoCrateExportView::Full,
                limit: None,
                offset: None,
                after: None,
            },
            Some(realm.bearer_token()),
            EXPORT_BYTES,
        )
        .await
        .is_ok()
            && usable(device, document_id).await
    };
    wait_until("the owner's export leaves a copy", device.node_id(), export).await?;
    let replica = read_replica(&device.context, document_id)
        .await
        .ok_or("the owner's export left no copy on the device")?;
    assert!(!replica.selected);
    Ok(())
}

/// Selecting a document for offline use installs a usable copy of a scaffold document too.
#[tokio::test]
async fn keeps_selected_document() -> TestResult<()> {
    let realm = Topology::spawn(2, 1, 1).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    assert!(fetch_realm_documents(&device.context, FETCH_BUDGET).await);
    let path = "datasets/selected";
    let origin = realm.leading_node(group_id, path);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    realm
        .create_document(origin, group_id, document_id, path, "notes")
        .await?;
    let select = || async {
        select_document(&device.context, document_id)
            .await
            .is_ok_and(|replica| replica.selected)
            && usable(device, document_id).await
    };
    wait_until(
        "the owner's selection installs a copy",
        device.node_id(),
        select,
    )
    .await?;
    Ok(())
}

/// A replacement the owner makes after a read-through removes the old scaffold values at the
/// holder: the device planned it against the graph it displayed.
#[tokio::test]
async fn replacement_removes_scaffold() -> TestResult<()> {
    let realm = Topology::spawn(2, 1, 1).await?;
    let group_id = realm.seed_group().await?;
    let device = realm.user_node();
    assert!(fetch_realm_documents(&device.context, FETCH_BUDGET).await);
    let path = "datasets/replaced";
    let origin = realm.leading_node(group_id, path);
    let document_id =
        mint_local_document(&realm.config, &realm.actor(origin), group_id, path)?.as_ulid();
    let placement = realm
        .create_document(origin, group_id, document_id, path, "scaffold-notes")
        .await?;
    let read = || async {
        get_metadata_routed(
            &device.context,
            realm.realm_id,
            GetVisibleRequest {
                document_id,
                auth: Some(realm.auth_context()),
            },
            Some(realm.bearer_token()),
        )
        .await
        .is_ok()
            && usable(device, document_id).await
    };
    wait_until("the owner's read leaves a copy", device.node_id(), read).await?;
    let replica = read_replica(&device.context, document_id)
        .await
        .ok_or("the owner's read left no copy on the device")?;
    assert!(replica.displayed_jsonld.contains("scaffold-notes"));

    route_metadata_update(
        &device.context,
        realm.actor(device),
        None,
        document_id,
        None,
        UpdateDocumentMutation::ReplaceRoCrate {
            jsonld: replica
                .displayed_jsonld
                .replace("scaffold-notes", "owner-replaced"),
        },
        None,
        Some(realm.bearer_token()),
        None,
    )
    .await?;

    let holder = realm.holder(&placement);
    let replaced = || async {
        drain_publish_queue(&device.context).await;
        let _ = replay_event_log(holder.context.as_ref()).await;
        let _ = process_materialization_batch(holder.context.as_ref()).await;
        let Some(metadata) = holder.context.metadata_handle.as_ref() else {
            return false;
        };
        metadata
            .export_rocrate_jsonld(replica.graph_iri.clone())
            .await
            .is_ok_and(|jsonld| {
                jsonld.contains("owner-replaced") && !jsonld.contains("scaffold-notes")
            })
    };
    wait_until(
        "the holder drops the replaced value",
        holder.node_id(),
        replaced,
    )
    .await?;
    Ok(())
}
