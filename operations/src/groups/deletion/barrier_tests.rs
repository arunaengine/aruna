//! Exercises enrollment serialization and the durable onboarding deletion snapshot.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::SystemTime;

use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::{GROUP_DELETE_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::group_delete::{
    GroupDeletePhase, GroupDeletionError, GroupWriteError, MembershipFence,
};
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_core::structs::placement::record::{
    BandPool, FIRST_GRANTABLE_HANDLE, HANDLE_BANDS, band_start,
};
use ed25519_dalek::SigningKey;
use tempfile::tempdir;
use ulid::Ulid;

use super::prepare::PrepareOperation;
use super::tests::{Fixture, context, node, read, write};
use crate::driver::drive;
use crate::groups::fence::GroupWriteOperation;
use crate::onboarding::issue_ticket::{IssueSyncInput, IssueSyncOperation};
use crate::realm::ensure_config::{EnsureConfigError, EnsureConfigOperation, EnsureConfigParams};

fn enrollment(fixture: &Fixture) -> EnsureConfigOperation {
    EnsureConfigOperation::new(EnsureConfigParams {
        actor: fixture.actor.clone(),
        target_node_id: node(2),
        target_node_kind: RealmNodeKind::Server,
        metadata_replication_factor: 1,
        realm_description: String::new(),
        create_if_missing: false,
        reject_kind_mismatch: true,
    })
}

async fn enroll_fixture() -> Fixture {
    let mut fixture = Fixture::new(&[1]).await;
    fixture.config.band_pools.push(BandPool {
        pool_id: Ulid::from_bytes([7; 16]),
        parent: None,
        issuer: fixture.actor.node_id,
        owner: fixture.actor.node_id,
        start: FIRST_GRANTABLE_HANDLE,
        end: band_start(HANDLE_BANDS),
    });
    write(
        &fixture.context,
        REALM_CONFIG_KEYSPACE,
        fixture.plan.realm_id.as_bytes().to_vec(),
        fixture.config.to_bytes(&fixture.actor).unwrap(),
    )
    .await;
    fixture
}

#[tokio::test]
async fn prepare_blocks_enrollment() {
    let fixture = enroll_fixture().await;
    fixture.prepare(1).await.unwrap();
    assert_eq!(
        drive(enrollment(&fixture), &fixture.context).await,
        Err(EnsureConfigError::DeletionPending)
    );
    drive(
        PrepareOperation::new(
            fixture.plan.clone(),
            GroupDeletePhase::Cancelled,
            fixture.actor.node_id,
            SystemTime::UNIX_EPOCH,
        ),
        &fixture.context,
    )
    .await
    .unwrap();
    let config = drive(enrollment(&fixture), &fixture.context).await.unwrap();
    assert!(config.has_node(node(2)));
}

#[tokio::test]
async fn enrollment_conflicts_prepare() {
    let fixture = enroll_fixture().await;
    let mut operation = enrollment(&fixture);
    let mut effects = VecDeque::from_iter(operation.start());
    let mut prepared = false;
    while !operation.is_complete() {
        let Effect::Storage(effect) = effects.pop_front().expect("enrollment must progress") else {
            panic!("unexpected enrollment effect");
        };
        if matches!(effect, StorageEffect::CommitTransaction { .. }) {
            assert!(!prepared);
            fixture.prepare(1).await.unwrap();
            prepared = true;
        }
        let event = fixture
            .context
            .storage_handle
            .send_storage_effect(effect)
            .await;
        effects.extend(operation.step(event));
    }
    assert!(prepared);
    assert!(matches!(
        operation.finalize(),
        Err(EnsureConfigError::StorageError(
            StorageError::TransactionConflict
        ))
    ));
}

#[tokio::test]
async fn enrollment_invalidates_prepare() {
    let fixture = enroll_fixture().await;
    drive(enrollment(&fixture), &fixture.context).await.unwrap();
    assert!(matches!(
        fixture.prepare(1).await,
        Err(GroupDeletionError::Conflict(_))
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
}

#[tokio::test]
async fn commit_reopens_enrollment() {
    let fixture = enroll_fixture().await;
    let proof = fixture.prepare(1).await.unwrap();
    fixture.commit(vec![proof]).await.unwrap();
    assert!(
        drive(enrollment(&fixture), &fixture.context)
            .await
            .unwrap()
            .has_node(node(2))
    );
    let fence = read(
        &fixture.context,
        GROUP_DELETE_KEYSPACE,
        MembershipFence::key(fixture.plan.realm_id).to_vec(),
    )
    .await;
    assert!(
        MembershipFence::from_value(fence.as_deref())
            .unwrap()
            .pending
            .is_empty()
    );
}

#[tokio::test]
async fn ticket_installs_tombstone() {
    let fixture = enroll_fixture().await;
    let proof = fixture.prepare(1).await.unwrap();
    fixture.commit(vec![proof]).await.unwrap();
    drive(enrollment(&fixture), &fixture.context).await.unwrap();
    let ticket = drive(
        IssueSyncOperation::new(IssueSyncInput {
            realm_signing_key: SigningKey::from_bytes(&[9; 32]),
            realm_id: fixture.plan.realm_id,
            node_id: node(2),
            issuer_node_id: fixture.actor.node_id,
            now: 100,
            ttl_secs: 300,
        }),
        &fixture.context,
    )
    .await
    .unwrap();
    assert_eq!(ticket.payload.group_deletions.len(), 1);
    let dir = tempdir().unwrap();
    let joined = context(dir.path());
    for _ in 0..2 {
        super::install_onboarding(&joined, &ticket, fixture.plan.realm_id, node(2), 100)
            .await
            .unwrap();
    }
    joined.storage_handle.close().await;
    let joined = context(dir.path());
    assert!(
        read(
            &joined,
            GROUP_KEYSPACE,
            fixture.plan.group_id.to_bytes().to_vec()
        )
        .await
        .is_none()
    );
    assert_eq!(
        drive(
            GroupWriteOperation::new(fixture.plan.group_id, Vec::new()),
            &joined
        )
        .await,
        Err(GroupWriteError::Deleted)
    );
    let mut tampered = ticket.clone();
    tampered.payload.group_deletions.clear();
    assert!(matches!(
        super::install_onboarding(&joined, &tampered, fixture.plan.realm_id, node(2), 100).await,
        Err(GroupDeletionError::Invalid(_))
    ));
    assert!(matches!(
        super::install_onboarding(&joined, &ticket, fixture.plan.realm_id, node(3), 100).await,
        Err(GroupDeletionError::Invalid(_))
    ));
    assert!(matches!(
        super::install_onboarding(&joined, &ticket, fixture.plan.realm_id, node(2), 401).await,
        Err(GroupDeletionError::Invalid(_))
    ));
}

#[tokio::test]
async fn replacement_cannot_reopen() {
    use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
    use crate::connectors::delete_connector::{DeleteSourceInput, DeleteSourceOperation};
    use crate::connectors::replace_connector::{
        ReplaceSourceError, ReplaceSourceInput, ReplaceSourceOperation,
    };
    use crate::connectors::repository::{connector_secret_key, source_connector_key};
    use aruna_core::keyspaces::{SOURCE_INDEX_KEYSPACE, SOURCE_SECRET_KEYSPACE};
    use aruna_core::structs::execution::source_connector::SourceConnectorKind;
    use std::collections::HashMap;

    for rotate_secret in [false, true] {
        let fixture = Fixture::new(&[1]).await;
        let created = drive(
            SourceConnectorOperation::new(SourceConnectorInput {
                group_id: fixture.plan.group_id,
                created_by: fixture.actor.user_id,
                name: "Before".into(),
                kind: SourceConnectorKind::Http,
                public_config: HashMap::from([("endpoint".into(), "https://example.org".into())]),
                secret_config: HashMap::new(),
            }),
            &fixture.context,
        )
        .await
        .unwrap();
        let connector_id = created.connector.connector_id;
        let mut operation = ReplaceSourceOperation::new(ReplaceSourceInput {
            group_id: fixture.plan.group_id,
            connector_id,
            name: "After".into(),
            kind: SourceConnectorKind::Http,
            public_config: created.connector.public_config,
            secret_config: if rotate_secret {
                HashMap::from([("token".into(), "replacement".into())])
            } else {
                HashMap::new()
            },
        });
        let mut effects = VecDeque::from_iter(operation.start());
        let mut deleted = false;
        while !operation.is_complete() {
            let Effect::Storage(effect) = effects.pop_front().expect("replacement must progress")
            else {
                panic!("unexpected replacement effect");
            };
            if !deleted
                && matches!(&effect, StorageEffect::Read { key_space, .. } if key_space == SOURCE_SECRET_KEYSPACE)
            {
                drive(
                    DeleteSourceOperation::new(DeleteSourceInput {
                        group_id: fixture.plan.group_id,
                        connector_id,
                    }),
                    &fixture.context,
                )
                .await
                .unwrap();
                let proof = fixture.prepare(1).await.unwrap();
                fixture.commit(vec![proof]).await.unwrap();
                deleted = true;
            }
            let event = fixture
                .context
                .storage_handle
                .send_storage_effect(effect)
                .await;
            effects.extend(operation.step(event));
        }
        assert!(deleted);
        assert!(matches!(
            operation.finalize(),
            Err(ReplaceSourceError::StorageError(
                StorageError::TransactionConflict
            ))
        ));
        for (space, key) in [
            (
                SOURCE_INDEX_KEYSPACE,
                source_connector_key(fixture.plan.group_id, connector_id),
            ),
            (SOURCE_SECRET_KEYSPACE, connector_secret_key(connector_id)),
        ] {
            assert!(read(&fixture.context, space, key.to_vec()).await.is_none());
        }
    }
}
