//! Tests RO-Crate exports that read their sources from other nodes through the peer handler.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna::inbound::initialize_incoming_fixture;
use aruna_core::effects::BlobEffect;
use aruna_core::effects::StorageEffect;
use aruna_core::events::BlobEvent;
use aruna_core::events::Event;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::{BLOB_HEAD_KEYSPACE, PATHS_INDEX_KEYSPACE};
use aruna_core::keyspaces::{BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE};
use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::user::id::UserId;
use aruna_drivers::driver::DriverContext;
use aruna_drivers::driver::drive;
use aruna_operations::jobs::export::ExportFailure;
use aruna_operations::jobs::export::open::OpenCandidateOperation;
use aruna_operations::jobs::export::resolve::ExportCandidate;
use aruna_operations::jobs::export::*;
use aruna_operations::jobs::key_locks::AskWaitsOperation;
use aruna_operations::replication::bao_read::BaoReadOutput;
use aruna_test_support::export::{
    BaoNode, FIXTURE_BYTES, byte_stream, encrypting_bucket, export_node, remote_spec, seed_bao,
};
use futures_util::StreamExt;
use std::sync::Arc;
use ulid::Ulid;

async fn bao_node(realm_id: RealmId) -> BaoNode {
    node_with(realm_id, false).await
}

/// A node as `export_node`, with the peer handler that answers other nodes' reads.
async fn node_with(realm_id: RealmId, metadata: bool) -> BaoNode {
    let node = export_node(realm_id, metadata).await;
    initialize_incoming_fixture(node.driver.clone());
    node
}

async fn keyspace_len(driver: &DriverContext, key_space: &str) -> usize {
    let Event::Storage(StorageEvent::IterResult { values, .. }) = driver
        .storage_handle
        .send_storage_effect(StorageEffect::Iter {
            key_space: key_space.to_string(),
            prefix: None,
            start: None,
            limit: 10,
            txn_id: None,
        })
        .await
    else {
        panic!("keyspace iteration failed")
    };
    values.len()
}

#[tokio::test]
async fn remote_read_ephemeral() {
    let realm_id = RealmId::from_bytes([71; 32]);
    let owner = UserId::local(Ulid::from_bytes([72; 16]), realm_id);
    let group_id = Ulid::from_bytes([73; 16]);
    let version_id = Ulid::from_bytes([74; 16]);
    let client = bao_node(realm_id).await;
    let source = bao_node(realm_id).await;
    client.net.add_peer_addr(source.net.endpoint_addr()).await;
    source.net.add_peer_addr(client.net.endpoint_addr()).await;
    let Event::Blob(BlobEvent::WriteFinished { location }) = source
        .driver
        .blob_handle
        .as_ref()
        .unwrap()
        .send_blob_effect(BlobEffect::Write {
            resolved: aruna_core::structs::storage::blob::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("source blob write failed")
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(
        &source,
        client.net.node_id(),
        owner,
        group_id,
        version_id,
        &location,
    )
    .await;
    let key_spaces = [
        BLOB_HEAD_KEYSPACE,
        BLOB_LOCATIONS_KEYSPACE,
        BLOB_VERSIONS_KEYSPACE,
        PATHS_INDEX_KEYSPACE,
    ];
    for key_space in key_spaces {
        assert_eq!(keyspace_len(client.driver.as_ref(), key_space).await, 0);
    }
    let exact = VersionedObjectArn::new(
        realm_id,
        source.net.node_id(),
        "remote",
        "payload",
        version_id,
    )
    .unwrap();
    let candidate = ExportCandidate::remote_exact(source.net.node_id(), exact, version_id, hash);

    let CandidateOpen::Opened(BaoReadOutput::Stream {
        mut blob,
        size,
        blake3,
        ..
    }) = drive(
        OpenCandidateOperation::new(
            SourceScope {
                spec: Arc::new(remote_spec(realm_id, owner)),
                owner_node: iroh::SecretKey::from_bytes(&[7; 32]).public(),
            },
            candidate.clone(),
            false,
        ),
        client.driver.as_ref(),
    )
    .await
    .unwrap()
    else {
        panic!("remote Bao read did not open")
    };
    let mut received = Vec::new();
    while let Some(chunk) = blob.next().await {
        received.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(received, FIXTURE_BYTES);
    assert_eq!(size, FIXTURE_BYTES.len() as u64);
    assert_eq!(blake3, hash);
    for key_space in key_spaces {
        assert_eq!(keyspace_len(client.driver.as_ref(), key_space).await, 0);
    }
    client.net.shutdown().await;
    source.net.shutdown().await;
}

#[tokio::test]
async fn locked_remote_export() {
    // The source bucket encrypts but its copy is not converted yet; it is still gated.
    let realm_id = RealmId::from_bytes([121; 32]);
    let owner = UserId::local(Ulid::from_bytes([122; 16]), realm_id);
    let group_id = Ulid::from_bytes([123; 16]);
    let version_id = Ulid::from_bytes([124; 16]);
    let client = bao_node(realm_id).await;
    let source = bao_node(realm_id).await;
    client.net.add_peer_addr(source.net.endpoint_addr()).await;
    source.net.add_peer_addr(client.net.endpoint_addr()).await;
    let Event::Blob(BlobEvent::WriteFinished { location }) = source
        .driver
        .blob_handle
        .as_ref()
        .unwrap()
        .send_blob_effect(BlobEffect::Write {
            resolved: aruna_core::structs::storage::blob::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("source blob write failed")
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(
        &source,
        client.net.node_id(),
        owner,
        group_id,
        version_id,
        &location,
    )
    .await;
    encrypting_bucket(&source, "remote", Ulid::from_bytes([125; 16])).await;
    let exact = VersionedObjectArn::new(
        realm_id,
        source.net.node_id(),
        "remote",
        "payload",
        version_id,
    )
    .unwrap();
    let candidate = ExportCandidate::remote_exact(source.net.node_id(), exact, version_id, hash);

    let opened = drive(
        OpenCandidateOperation::new(
            SourceScope {
                spec: Arc::new(remote_spec(realm_id, owner)),
                owner_node: iroh::SecretKey::from_bytes(&[7; 32]).public(),
            },
            candidate.clone(),
            false,
        ),
        client.driver.as_ref(),
    )
    .await;
    let Err(ExportFailure::RemoteLocked {
        node_id, target, ..
    }) = opened
    else {
        panic!("a locked remote input must be typed, got another outcome")
    };
    // The refusal keeps the exact version the read named, not only its content.
    let exact = aruna_core::jobs::WaitTarget::Object {
        bucket: "remote".to_string(),
        key: "payload".to_string(),
        version_id,
    };
    assert_eq!((node_id, target), (source.net.node_id(), exact));
    client.net.shutdown().await;
    source.net.shutdown().await;
}

#[tokio::test]
async fn plaintext_export_parks() {
    // The remote copy is not converted yet; its encrypting bucket's locked key parks the export.
    let realm_id = RealmId::from_bytes([131; 32]);
    let owner = UserId::local(Ulid::from_bytes([132; 16]), realm_id);
    let group_id = Ulid::from_bytes([133; 16]);
    let version_id = Ulid::from_bytes([134; 16]);
    let bucket_id = Ulid::from_bytes([135; 16]);
    let client = bao_node(realm_id).await;
    let source = node_with(realm_id, true).await;
    client.net.add_peer_addr(source.net.endpoint_addr()).await;
    source.net.add_peer_addr(client.net.endpoint_addr()).await;
    let Event::Blob(BlobEvent::WriteFinished { location }) = source
        .driver
        .blob_handle
        .as_ref()
        .unwrap()
        .send_blob_effect(BlobEffect::Write {
            resolved: aruna_core::structs::storage::blob::ResolvedBackend::node_default(),
            bucket: "remote".to_string(),
            key: "payload".to_string(),
            created_by: owner,
            blob: byte_stream(FIXTURE_BYTES),
            size: None,
        })
        .await
    else {
        panic!("source blob write failed")
    };
    let hash: [u8; 32] = location.get_blake3().unwrap().try_into().unwrap();
    seed_bao(
        &source,
        client.net.node_id(),
        owner,
        group_id,
        version_id,
        &location,
    )
    .await;
    encrypting_bucket(&source, "remote", bucket_id).await;
    let alias = aruna_core::structs::storage::blob::HashIndex::new(
        hash,
        version_id,
        realm_id,
        group_id,
        source.net.node_id(),
        "remote",
        "payload",
    );
    let event = source
        .driver
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: aruna_core::keyspaces::PATHS_INDEX_KEYSPACE.to_string(),
            key: alias.to_bytes().unwrap().into(),
            value: Vec::new().into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
    let exact = VersionedObjectArn::new(
        realm_id,
        source.net.node_id(),
        "remote",
        "payload",
        version_id,
    )
    .unwrap();
    let candidate = ExportCandidate::remote_exact(source.net.node_id(), exact, version_id, hash);
    let opened = drive(
        OpenCandidateOperation::new(
            SourceScope {
                spec: Arc::new(remote_spec(realm_id, owner)),
                owner_node: iroh::SecretKey::from_bytes(&[7; 32]).public(),
            },
            candidate.clone(),
            false,
        ),
        client.driver.as_ref(),
    )
    .await;
    let Err(ExportFailure::RemoteLocked {
        node_id,
        target,
        auth,
    }) = opened
    else {
        panic!("a locked remote input must be typed, got another outcome")
    };

    // The source registers the wait and names the dependency, so the export parks.
    let ask = AskWaitsOperation::new(
        &auth,
        JobId::from_bytes([84; 16]),
        node_id,
        std::slice::from_ref(&target),
        client.driver.node_and_realm(),
    );
    let waits = drive(ask, client.driver.as_ref()).await.unwrap();
    assert_eq!(waits.len(), 1);
    assert_eq!(waits[0].node_id, source.net.node_id());
    assert_eq!(waits[0].bucket, "remote");
    assert_eq!(
        waits[0].key,
        aruna_core::structs::storage::encryption::BucketKeyRef::new(bucket_id, 1)
    );
    client.net.shutdown().await;
    source.net.shutdown().await;
}
