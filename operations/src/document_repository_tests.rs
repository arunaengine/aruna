//! Tests the realm config and group reads of the document repository.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::groups::create_group::{CreateGroupConfig, CreateGroupOperation};
use aruna_core::UserId;
use aruna_core::structs::identity::auth::Actor;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_storage::storage;
use aruna_tasks::TaskHandle;
use tempfile::tempdir;
use ulid::Ulid;

fn actor(realm_id: RealmId) -> Actor {
    Actor {
        user_id: UserId::local(Ulid::from_bytes([2; 16]), realm_id),
        realm_id,
        node_id: iroh::SecretKey::from_bytes(&[1u8; 32]).public(),
    }
}

#[test]
fn reads_realm_config() {
    let realm_id = RealmId::from_bytes([7; 32]);
    let config = RealmConfigDocument::new(realm_id, Vec::new(), 2);
    let mut operation = GetConfigOperation::new(realm_id);
    let row = read_row(&DocumentTarget::RealmConfig { realm_id });
    assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(vec![row]))));
    let bytes = config.to_bytes(&actor(realm_id)).unwrap();
    let answer = IoResult::Values(vec![Some(bytes.into())]);
    assert_eq!(operation.next(answer), Ok(Next::Done(config)));
}

#[test]
fn config_read_fails() {
    // A missing row, unreadable bytes and a storage fault stay three distinct errors.
    let mut operation = GetConfigOperation::new(RealmId::from_bytes([7; 32]));
    assert_eq!(
        operation.next(IoResult::Values(vec![None])),
        Err(GetConfigError::DocumentNotFound)
    );
    let garbage = IoResult::Values(vec![Some(vec![0xff; 3].into())]);
    assert!(matches!(
        operation.next(garbage),
        Err(GetConfigError::ConversionError(_))
    ));
    assert_eq!(
        GetConfigError::from(IoError::Storage(StorageError::Timeout)),
        GetConfigError::StorageError(StorageError::Timeout)
    );
}

/// A stored group with its authorization document, encoded.
fn group_rows() -> (Group, GroupAuthorizationDocument, Vec<u8>, Vec<u8>) {
    let realm_id = RealmId::from_bytes([7; 32]);
    let actor = actor(realm_id);
    let group_id = Ulid::from_bytes([1; 16]);
    let group = Group {
        display_name: "Group".to_string(),
        group_id,
        realm_id,
        roles: Default::default(),
        owner: actor.user_id,
    };
    let auth = GroupAuthorizationDocument::default_group_doc(actor.user_id, realm_id, group_id);
    let (group_bytes, auth_bytes) = (
        group.to_bytes(&actor).unwrap(),
        auth.to_bytes(&actor).unwrap(),
    );
    (group, auth, group_bytes, auth_bytes)
}

fn group_read() -> GetGroupOperation {
    GetGroupOperation::new(GetGroupConfig {
        group_id: Ulid::from_bytes([1; 16]),
    })
}

#[test]
fn reads_group_snapshot() {
    let (group, auth, group_bytes, auth_bytes) = group_rows();
    let mut operation = group_read();
    assert_eq!(operation.transaction(), TransactionMode::ReadSnapshot);
    let key: Key = group.group_id.to_bytes().into();
    let reads = vec![
        (GROUP_KEYSPACE.to_string(), key.clone()),
        (AUTH_KEYSPACE.to_string(), key),
    ];
    assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(reads))));
    let answer = IoResult::Values(vec![Some(group_bytes.into()), Some(auth_bytes.into())]);
    assert_eq!(operation.next(answer), Ok(Next::Done((group, auth))));
}

#[test]
fn group_read_fails() {
    // The group row is judged first, so a missing group never reports a missing document.
    let (_, _, group_bytes, auth_bytes) = group_rows();
    let cases = [
        (None, Some(auth_bytes), GetGroupError::GroupNotFound),
        (None, None, GetGroupError::GroupNotFound),
        (Some(group_bytes), None, GetGroupError::DocNotFound),
    ];
    for (group, auth, expected) in cases {
        let answer = IoResult::Values(vec![group.map(Into::into), auth.map(Into::into)]);
        assert_eq!(group_read().next(answer), Err(expected));
    }
    let garbage = IoResult::Values(vec![Some(vec![0xff; 3].into()), None]);
    assert!(matches!(
        group_read().next(garbage),
        Err(GetGroupError::ConversionError(_))
    ));
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
