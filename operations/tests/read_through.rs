//! Tests that a document the owner reads online on a device stays there as an unselected copy.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

// Fresh builds overflow the default query depth in nested async layouts.
#![recursion_limit = "256"]

mod topology;

use std::time::Duration;

use aruna_core::StructuredId;
use aruna_operations::device::realm_documents::fetch_realm_documents;
use aruna_operations::device::replica::read_replica;
use aruna_operations::metadata::api::GetVisibleRequest;
use aruna_operations::metadata::create_document::mint_local_document;
use aruna_operations::metadata::forward::get_metadata_routed;
use topology::{TestResult, Topology, wait_until};

/// Only ends a hung exchange; never a performance assertion.
const FETCH_BUDGET: Duration = Duration::from_secs(30);

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
    };
    wait_until(
        "the owner's read through the device",
        device.node_id(),
        read,
    )
    .await?;

    let replica = read_replica(&device.context, document_id)
        .await
        .ok_or("the owner's read left no copy on the device")?;
    assert!(!replica.selected, "a read copy is a cache, not a selection");
    assert!(replica.serves_reads(), "the copy answers offline reads");
    Ok(())
}
