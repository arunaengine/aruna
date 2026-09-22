//! Covers deletion recovery, incomplete confirmations and pending local resources.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::keyspaces::*;
use aruna_core::metadata::{MetadataEventPayload, MetadataEventRecord};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::pending_projection_key;
use aruna_core::structs::identity::group_delete::*;
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use ulid::Ulid;

use super::commit::CommitOperation;
use super::prepare::PrepareOperation;
use super::tests::{Fixture, context, node, read, write};

#[tokio::test]
async fn unauthorized_prepare_refuses() {
    let mut fixture = Fixture::new(&[1]).await;
    fixture.plan.requested_by =
        aruna_core::UserId::local(Ulid::from_bytes([8; 16]), fixture.plan.realm_id);
    assert_eq!(
        fixture.prepare(1).await,
        Err(GroupDeletionError::Unauthorized)
    );
    assert!(
        read(
            &fixture.context,
            GROUP_DELETE_KEYSPACE,
            fixture.plan.group_id.to_bytes().to_vec()
        )
        .await
        .is_none()
    );
}

#[tokio::test]
async fn realm_admin_deletes() {
    let mut fixture = Fixture::new(&[1]).await;
    let admin = aruna_core::UserId::local(Ulid::from_bytes([8; 16]), fixture.plan.realm_id);
    fixture.plan.requested_by = admin;
    fixture.actor.user_id = admin;
    fixture.auth.user_id = admin;
    let mut authorization =
        aruna_core::structs::identity::realm::RealmAuthorizationDocument::default_realm_doc(
            fixture.plan.realm_id,
        );
    for role in authorization.roles.values_mut() {
        role.assigned_users.insert(admin);
    }
    write(
        &fixture.context,
        AUTH_KEYSPACE,
        fixture.plan.realm_id.as_bytes().to_vec(),
        authorization.to_bytes(&fixture.actor).unwrap(),
    )
    .await;
    let proof = fixture.prepare(1).await.unwrap();
    fixture.commit(vec![proof]).await.unwrap();
    let key = aruna_core::structs::identity::group::owner_group_key(
        fixture.plan.owner,
        fixture.plan.group_id,
    );
    assert!(
        read(&fixture.context, OWNER_INDEX_KEYSPACE, key)
            .await
            .is_none()
    );
}
use crate::driver::drive;
use crate::groups::fence::GroupWriteOperation;

#[tokio::test]
async fn prepared_restart_survives() {
    let fixture = Fixture::new(&[1]).await;
    fixture.prepare(1).await.unwrap();
    fixture.context.storage_handle.close().await;
    let reopened = context(fixture.dir.path());
    let writes = vec![(
        SOURCE_INDEX_KEYSPACE.to_string(),
        b"new".to_vec().into(),
        b"record".to_vec().into(),
    )];
    assert_eq!(
        drive(
            GroupWriteOperation::new(fixture.plan.group_id, writes.clone()),
            &reopened
        )
        .await,
        Err(GroupWriteError::Frozen)
    );
    drive(
        PrepareOperation::new(
            fixture.plan.clone(),
            GroupDeletePhase::Cancelled,
            node(1),
            SystemTime::UNIX_EPOCH,
        ),
        &reopened,
    )
    .await
    .unwrap();
    drive(
        GroupWriteOperation::new(fixture.plan.group_id, writes),
        &reopened,
    )
    .await
    .unwrap();
    assert!(matches!(
        drive(
            PrepareOperation::new(
                fixture.plan,
                GroupDeletePhase::Preparing,
                node(1),
                SystemTime::UNIX_EPOCH
            ),
            &reopened
        )
        .await,
        Err(GroupDeletionError::Conflict(_))
    ));
}

#[tokio::test]
async fn committed_restart_replays() {
    let fixture = Fixture::new(&[1]).await;
    let proof = fixture.prepare(1).await.unwrap();
    let certificate = GroupDeleteCertificate {
        plan: fixture.plan.clone(),
        proofs: vec![proof.clone()],
    };
    let event = fixture.commit(vec![proof]).await.unwrap();
    fixture.context.storage_handle.close().await;
    let reopened = context(fixture.dir.path());
    let replayed = drive(
        CommitOperation::new(certificate, fixture.actor, None, Some(event.clone())),
        &reopened,
    )
    .await
    .unwrap();
    assert_eq!(event, replayed);
    assert!(
        read(
            &reopened,
            GROUP_KEYSPACE,
            fixture.plan.group_id.to_bytes().to_vec()
        )
        .await
        .is_none()
    );
    let (outbox, _) = crate::jobs::store::iter_prefix_page(
        &reopened.storage_handle,
        SYNC_OUTBOX_KEYSPACE,
        None,
        None,
        10,
        None,
    )
    .await
    .unwrap();
    assert_eq!(outbox.len(), 1);
}

#[tokio::test]
async fn peers_commit_once() {
    let first = Fixture::new(&[1, 2]).await;
    let second = Fixture::new(&[1, 2]).await;
    let proofs = vec![
        first.prepare(1).await.unwrap(),
        second.prepare(2).await.unwrap(),
    ];
    let certificate = GroupDeleteCertificate {
        plan: first.plan.clone(),
        proofs: proofs.clone(),
    };
    let event = first.commit(proofs).await.unwrap();
    for _ in 0..2 {
        drive(
            CommitOperation::new(
                certificate.clone(),
                first.actor.clone(),
                None,
                Some(event.clone()),
            ),
            &second.context,
        )
        .await
        .unwrap();
    }
    for fixture in [&first, &second] {
        for space in [GROUP_KEYSPACE, AUTH_KEYSPACE] {
            assert!(
                read(
                    &fixture.context,
                    space,
                    fixture.plan.group_id.to_bytes().to_vec()
                )
                .await
                .is_none()
            );
        }
    }
}

#[tokio::test]
async fn missing_vote_refuses() {
    let fixture = Fixture::new(&[1, 2]).await;
    let proof = fixture.prepare(1).await.unwrap();
    let mut operation = CommitOperation::new(
        GroupDeleteCertificate {
            plan: fixture.plan.clone(),
            proofs: vec![proof],
        },
        fixture.actor.clone(),
        Some(fixture.auth.clone()),
        None,
    );
    assert!(operation.start().is_empty());
    assert!(matches!(
        operation.finalize(),
        Err(GroupDeletionError::Invalid(_))
    ));
    assert!(
        read(
            &fixture.context,
            GROUP_KEYSPACE,
            fixture.plan.group_id.to_bytes().to_vec()
        )
        .await
        .is_some()
    );
}

#[tokio::test]
async fn frozen_write_atomic() {
    let fixture = Fixture::new(&[1]).await;
    fixture.prepare(1).await.unwrap();
    let writes = vec![
        (
            SOURCE_INDEX_KEYSPACE.to_string(),
            b"connector".to_vec().into(),
            b"record".to_vec().into(),
        ),
        (
            SOURCE_SECRET_KEYSPACE.to_string(),
            b"connector".to_vec().into(),
            b"secret".to_vec().into(),
        ),
    ];
    assert_eq!(
        drive(
            GroupWriteOperation::new(fixture.plan.group_id, writes),
            &fixture.context
        )
        .await,
        Err(GroupWriteError::Frozen)
    );
    for space in [SOURCE_INDEX_KEYSPACE, SOURCE_SECRET_KEYSPACE] {
        assert!(
            read(&fixture.context, space, b"connector".to_vec())
                .await
                .is_none()
        );
    }
}

#[tokio::test]
async fn pending_metadata_refuses() {
    let fixture = Fixture::new(&[1]).await;
    let document_id = Ulid::from_bytes([7; 16]);
    let event_id = Ulid::from_bytes([8; 16]);
    let record = MetadataRegistryRecord {
        realm_id: fixture.plan.realm_id,
        group_id: fixture.plan.group_id,
        document_id,
        document_path: "datasets/pending".into(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: false,
        permission_path: "pending".into(),
        placement: PlacementRef::NIL,
        holder_node_ids: vec![node(1)],
        created_at_ms: 1,
        updated_at_ms: 1,
        establishing_event_id: event_id,
        last_event_id: event_id,
    };
    let event = MetadataEventRecord {
        event_id,
        record,
        user_id: fixture.actor.user_id,
        node_id: node(1),
        payload: MetadataEventPayload::RoCrate {
            jsonld: "{}".into(),
        },
        occurred_at_ms: 1,
    };
    let key = pending_projection_key(document_id, event_id).to_vec();
    write(
        &fixture.context,
        EVENT_LOG_KEYSPACE,
        key.clone(),
        postcard::to_allocvec(&event).unwrap(),
    )
    .await;
    write(
        &fixture.context,
        PENDING_PROJECTION_KEYSPACE,
        key,
        Vec::new(),
    )
    .await;
    assert_eq!(
        fixture.prepare(1).await,
        Err(GroupDeletionError::NotEmpty(
            "pending metadata creation".into()
        ))
    );
}

#[tokio::test]
async fn corrupt_resource_refuses() {
    let fixture = Fixture::new(&[1]).await;
    write(
        &fixture.context,
        S3_BUCKET_KEYSPACE,
        b"broken".to_vec(),
        vec![255],
    )
    .await;
    assert!(matches!(
        fixture.prepare(1).await,
        Err(GroupDeletionError::Unavailable(_))
    ));
    assert!(
        read(
            &fixture.context,
            GROUP_DELETE_KEYSPACE,
            fixture.plan.group_id.to_bytes().to_vec()
        )
        .await
        .is_none()
    );
    assert!(
        read(
            &fixture.context,
            GROUP_KEYSPACE,
            fixture.plan.group_id.to_bytes().to_vec()
        )
        .await
        .is_some()
    );
}

#[tokio::test]
async fn unavailable_peer_preserves() {
    let mut fixture = Fixture::new(&[1, 2]).await;
    let seed = [1, 2]
        .into_iter()
        .find(|seed| node(*seed) == fixture.plan.coordinator)
        .unwrap();
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            secret_key: Some(iroh::SecretKey::from_bytes(&[seed; 32])),
            realm_id: fixture.plan.realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..Default::default()
        },
        fixture.context.storage_handle.clone(),
    )
    .await
    .unwrap();
    fixture.context.net_handle = Some(net.clone());
    let context = Arc::new(fixture.context);
    assert!(matches!(
        super::coordinator::delete_group(&context, fixture.auth, None, fixture.plan.group_id).await,
        Err(GroupDeletionError::Unavailable(_))
    ));
    assert!(
        read(
            &context,
            GROUP_KEYSPACE,
            fixture.plan.group_id.to_bytes().to_vec()
        )
        .await
        .is_some()
    );
    let record = read(
        &context,
        GROUP_DELETE_KEYSPACE,
        fixture.plan.group_id.to_bytes().to_vec(),
    )
    .await
    .unwrap();
    let record = GroupDeleteRecord::from_bytes(&record).unwrap();
    assert_eq!(record.phase, GroupDeletePhase::Aborting);
    assert!(!record.blocks_writes());
    net.shutdown().await;
}
