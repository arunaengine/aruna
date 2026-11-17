//! Tests the realm config and group reads of the document repository.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_core::UserId;
use aruna_core::operation_check::{rejects_events, sample_events};
use aruna_core::structs::identity::auth::Actor;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use tempfile::tempdir;
use ulid::Ulid;

#[test]
fn incomplete_finalize_errors() {
    // A deadline can finalize an operation that never reached a terminal
    // state; that must report an error instead of panicking.
    let mut operation = GetConfigOperation::new(RealmId::from_bytes([7; 32]));
    operation.start();

    assert!(!operation.is_complete());
    assert_eq!(operation.finalize(), Err(GetConfigError::NotFinished));
}

#[tokio::test]
pub async fn test_get_group() {
    let random_path = tempdir().unwrap();
    let storage_handle = storage::FjallStorage::open(random_path.path().to_str().unwrap()).unwrap();
    let net_handle = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage_handle.clone(),
    )
    .await
    .unwrap();
    let task_handle = TaskHandle::new();

    let context = DriverContext {
        storage_handle,
        blob_handle: None,
        net_handle: Some(net_handle.clone()),
        metadata_handle: None,
        graph_handle: None,
        outbound_handle: None,
        task_handle: Some(task_handle),
        compute_handle: None,
    };

    let actor = Actor {
        user_id: UserId::local(
            Ulid::generate(),
            aruna_core::structs::identity::realm::RealmId([0u8; 32]),
        ),
        realm_id: aruna_core::structs::identity::realm::RealmId([0u8; 32]),
        node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
    };

    let group_config = CreateGroupConfig {
        actor,
        display_name: "Test group".to_string(),
        owner_cap: None,
    };
    let group_operation = CreateGroupOperation::new(group_config.clone());
    let create_result = drive(group_operation, &context).await.unwrap();

    let get_group_config = GetGroupConfig {
        group_id: create_result.0.group_id,
    };
    let group_operation = GetGroupOperation::new(get_group_config.clone());
    let get_result = drive(group_operation, &context).await.unwrap();

    assert_eq!(create_result.0, get_result.0);

    net_handle.shutdown().await;
}

// A state that expects no event must reject one instead of ignoring it.
#[test]
fn terminal_rejects_event() {
    let make = || {
        GetGroupOperation::new(GetGroupConfig {
            group_id: Ulid::from_bytes([1u8; 16]),
        })
    };
    for (_, effects, error) in rejects_events(make, sample_events, |_| false) {
        assert!(effects.is_empty());
        assert!(
            matches!(
                error,
                GetGroupError::UnexpectedEvent { .. } | GetGroupError::StorageError(_)
            ),
            "{error:?}"
        );
    }
}
