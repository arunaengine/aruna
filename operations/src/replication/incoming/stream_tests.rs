//! Tests peer admission and the request policy of inbound bao streams against a real store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};

use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::auth::PathRestriction;
use aruna_storage::FjallStorage;
use tempfile::tempdir;

use super::*;
use aruna_core::structs::storage::replication_queue::ReferenceAdvance;

#[tokio::test]
async fn rejects_foreign_peer() {
    // The blob replication plane must refuse peers that are unknown or not
    // sync-eligible before any manifest is read.
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let realm_id = aruna_core::structs::identity::realm::RealmId::from_bytes([3u8; 32]);
    let server = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
    let user = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
    let mut config = aruna_core::structs::identity::realm::RealmConfigDocument::default_for_realm(
        realm_id,
        Vec::new(),
    );
    config.ensure_node(
        server,
        aruna_core::structs::identity::realm::RealmNodeKind::Server,
    );
    config.ensure_node(
        user,
        aruna_core::structs::identity::realm::RealmNodeKind::User {
            owner: aruna_core::UserId::nil(realm_id),
        },
    );
    let actor = aruna_core::structs::identity::auth::Actor {
        node_id: server,
        user_id: aruna_core::UserId::nil(realm_id),
        realm_id,
    };
    let target = DocumentTarget::RealmConfig { realm_id };
    storage
        .send_storage_effect(aruna_core::effects::StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: config.to_bytes(&actor).unwrap().into(),
            txn_id: None,
        })
        .await;

    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };

    let unknown = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
    let foreign = aruna_core::structs::identity::realm::RealmId::from_bytes([4u8; 32]);
    // Devices read only through realm nodes and cannot source replication.
    assert_eq!(
        bao_peer_admitted(&context, realm_id, server, server).await,
        Some(BaoAdmission::Infra)
    );
    assert_eq!(
        bao_peer_admitted(&context, realm_id, server, user).await,
        Some(BaoAdmission::DeviceRead)
    );
    assert_eq!(
        bao_peer_admitted(&context, realm_id, user, user).await,
        None
    );
    assert_eq!(
        bao_peer_admitted(&context, realm_id, server, unknown).await,
        None
    );
    assert_eq!(
        bao_peer_admitted(&context, foreign, server, server).await,
        None
    );
}

#[tokio::test]
async fn checks_advance_policy() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let realm_id = RealmId::from_bytes([4u8; 32]);
    let foreign_realm = RealmId::from_bytes([5u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let group_id = Ulid::generate();
    let path = object_permission_path(realm_id, group_id, node_id, "bucket", "key");
    let auth_context = AuthContext {
        user_id,
        realm_id,
        path_restrictions: Some(vec![PathRestriction {
            pattern: "/source/**".to_string(),
            permission: Permission::READ,
        }]),
        session: None,
    };
    let mut manifest = VersionReplicationManifest {
        bucket: "bucket".to_string(),
        key: "key".to_string(),
        version_id: Ulid::generate(),
        group_id,
        kind: ReplicationItemKind::Materialized,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: user_id,
        current_version: true,
        current_version_generation: Some(2),
        auth_context,
        blob: None,
        source: Some(VersionSourceBinding {
            strategy: StagingStrategy::Reference,
            descriptor: PortableSourceDescriptor {
                kind: SourceConnectorKind::Http,
                public_config: HashMap::new(),
                source_path: "source".to_string(),
                version_selector: None,
                capabilities: Vec::new(),
                origin_node_id: None,
                namespace: None,
                root: None,
            },
            connector_id: Some(Ulid::generate()),
            mirrors: Vec::new(),
        }),
        multipart: None,
        reference_intent: true,
        origin: None,
        upstream_sources: Vec::new(),
        writer_auth_context: None,
        reference_metadata: Some(SourceMetadata {
            content_length: 1,
            content_type: None,
            etag: None,
            last_modified: None,
            source_version: None,
            digests: Default::default(),
        }),
        metadata: HashMap::new(),
        reference_advance: Some(ReferenceAdvance {
            generation: 2,
            predecessor: Ulid::generate(),
        }),
        reference_advance_count: Some(0),
        placement_policies: Vec::new(),
        original_epoch: Default::default(),
        version_generation: 0,
        logical_identity: None,
        distribution: None,
        version_origin: None,
        version_hop: None,
        sync_source: None,
    };

    assert_eq!(
        manifest_policy(&context, realm_id, node_id, &manifest)
            .await
            .unwrap(),
        (Some(path.clone()), None)
    );

    manifest.auth_context = AuthContext::anonymous(realm_id);
    assert_eq!(
        manifest_policy(&context, realm_id, node_id, &manifest)
            .await
            .unwrap(),
        (Some(path.clone()), None)
    );

    // A user of another realm signed in here is served like a local user.
    manifest.auth_context.user_id = UserId::local(Ulid::generate(), foreign_realm);
    assert_eq!(
        manifest_policy(&context, realm_id, node_id, &manifest)
            .await
            .unwrap(),
        (Some(path), None)
    );

    manifest.auth_context = AuthContext {
        user_id: UserId::nil(foreign_realm),
        realm_id,
        path_restrictions: None,
        session: None,
    };
    assert_eq!(
        manifest_policy(&context, realm_id, node_id, &manifest)
            .await
            .unwrap(),
        (None, None)
    );

    manifest.auth_context = AuthContext::anonymous(foreign_realm);
    assert_eq!(
        manifest_policy(&context, realm_id, node_id, &manifest)
            .await
            .unwrap(),
        (None, None)
    );

    manifest.reference_advance = None;
    manifest.auth_context = AuthContext::anonymous(realm_id);
    manifest.writer_auth_context = Some(manifest.auth_context.clone());
    assert_eq!(
        manifest_policy(&context, realm_id, node_id, &manifest)
            .await
            .unwrap(),
        (None, None)
    );
}

#[tokio::test]
async fn denies_offered_bucket() {
    use crate::device::sync::repository::folder_bucket;
    use crate::driver::drive;
    use crate::staging::offered_directory::{OfferDirectoryInput, offer_directory};

    let fixture = crate::tests::staging::setup_driver_context().await;
    let context = &fixture.driver_context;
    let root = tempdir().unwrap();
    std::fs::write(root.path().join("note.txt"), b"note").unwrap();
    let realm_id = RealmId::from_bytes([4u8; 32]);
    let node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let bucket = folder_bucket(Ulid::generate());
    let offer = OfferDirectoryInput {
        bucket: bucket.clone(),
        root: root.path().to_string_lossy().to_string(),
        group_id: Ulid::generate(),
        realm_id,
        node_id,
        user_id,
    };
    drive(offer_directory(offer), context).await.unwrap();
    let manifest = VersionReplicationManifest {
        bucket,
        key: "note.txt".to_string(),
        version_id: Ulid::generate(),
        group_id: Ulid::generate(),
        kind: ReplicationItemKind::DeleteMarker,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        created_by: user_id,
        current_version: true,
        current_version_generation: Some(2),
        auth_context: AuthContext::anonymous(realm_id),
        blob: None,
        source: None,
        multipart: None,
        reference_intent: false,
        origin: None,
        upstream_sources: Vec::new(),
        writer_auth_context: None,
        reference_metadata: None,
        metadata: HashMap::new(),
        reference_advance: Some(ReferenceAdvance {
            generation: 2,
            predecessor: Ulid::generate(),
        }),
        reference_advance_count: Some(0),
        placement_policies: Vec::new(),
        original_epoch: Default::default(),
        version_generation: 0,
        logical_identity: None,
        distribution: None,
        version_origin: None,
        version_hop: None,
        sync_source: None,
    };

    assert_eq!(
        manifest_policy(context, realm_id, node_id, &manifest)
            .await
            .unwrap(),
        (None, None)
    );
}

#[tokio::test]
async fn foreign_reads_denied() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let realm_id = RealmId::from_bytes([9u8; 32]);
    let foreign_realm = RealmId::from_bytes([10u8; 32]);
    let local_node = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
    let other_node = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let target = |realm_id: RealmId, node_id: NodeId| {
        crate::replication::protocol::BaoReadTarget::ExactVersion(
            aruna_core::structs::storage::replication::VersionedObjectArn {
                realm_id,
                node_id,
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                version: Ulid::generate(),
            },
        )
    };
    let request = |realm_id: RealmId, auth_context: AuthContext, target| {
        crate::replication::protocol::BaoReadRequest {
            auth_context,
            realm_id,
            target,
            expected_blake3: None,
            metadata_only: false,
            destination: None,
            known_refs: Vec::new(),
            range: None,
        }
    };

    let foreign_request = request(
        foreign_realm,
        AuthContext::anonymous(realm_id),
        target(realm_id, local_node),
    );
    let foreign_identity = request(
        realm_id,
        AuthContext::anonymous(foreign_realm),
        target(realm_id, local_node),
    );
    let foreign_target = request(
        realm_id,
        AuthContext::anonymous(realm_id),
        target(realm_id, other_node),
    );
    for request in [foreign_request, foreign_identity, foreign_target] {
        assert_eq!(
            bao_policy(&context, realm_id, local_node, &request)
                .await
                .unwrap(),
            (HashSet::new(), Vec::new(), false)
        );
    }
}
