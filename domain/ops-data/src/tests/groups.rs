//! Checks that group deletion and the bucket and connector writers of the group exclude each
//! other through the group write fence.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::time::SystemTime;

use aruna_core::admin_documents::AdminDocumentEvent;
use aruna_core::calls::CallRun;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::identity::group_delete::{
    GroupDeleteCertificate, GroupDeletePhase, GroupDeleteProof, GroupDeletionError, GroupWriteError,
};
use aruna_ops_identity::groups::deletion::{commit_operation, prepare_operation};
use aruna_runner::driver::drive;
use aruna_test_harness::groups::{Fixture, node, read};

use crate::s3::bucket::create::{CreateBucketError, CreateBucketOperation};

/// Prepares the deletion on the node of `seed` and returns its signed proof.
async fn prepare(fixture: &Fixture, seed: u8) -> Result<GroupDeleteProof, GroupDeletionError> {
    let phase = GroupDeletePhase::Preparing;
    let operation = prepare_operation(
        fixture.plan.clone(),
        phase,
        node(seed),
        SystemTime::UNIX_EPOCH,
    );
    drive(operation, &fixture.context).await?;
    Ok(GroupDeleteProof {
        node_id: node(seed),
        signature: iroh::SecretKey::from_bytes(&[seed; 32])
            .sign(&fixture.plan.signing_bytes().unwrap()),
    })
}

/// Commits the deletion with `proofs` as the owner.
async fn commit(
    fixture: &Fixture,
    proofs: Vec<GroupDeleteProof>,
) -> Result<AdminDocumentEvent, GroupDeletionError> {
    let certificate = GroupDeleteCertificate {
        plan: fixture.plan.clone(),
        proofs,
    };
    let operation = commit_operation(certificate, fixture.actor.clone(), fixture.auth.clone());
    drive(operation, &fixture.context).await
}

#[tokio::test]
async fn occupied_peer_refuses() {
    let first = Fixture::new(&[1, 2]).await;
    let second = Fixture::new(&[1, 2]).await;
    drive(
        CreateBucketOperation::new("owned".into(), second.bucket()),
        &second.context,
    )
    .await
    .unwrap();
    prepare(&first, 1).await.unwrap();
    assert_eq!(
        prepare(&second, 2).await,
        Err(GroupDeletionError::NotEmpty {
            kinds: vec!["S3 buckets".into()],
            buckets: vec!["owned".into()],
            more: false,
            hidden: 0,
        })
    );
    for fixture in [&first, &second] {
        assert!(
            read(
                &fixture.context,
                GROUP_KEYSPACE,
                fixture.plan.group_id.to_bytes().to_vec()
            )
            .await
            .is_some()
        );
        assert!(
            read(
                &fixture.context,
                AUTH_KEYSPACE,
                fixture.plan.group_id.to_bytes().to_vec()
            )
            .await
            .is_some()
        );
        drive(
            prepare_operation(
                fixture.plan.clone(),
                GroupDeletePhase::Cancelled,
                fixture.actor.node_id,
                SystemTime::UNIX_EPOCH,
            ),
            &fixture.context,
        )
        .await
        .unwrap();
    }
    assert!(
        read(&second.context, S3_BUCKET_KEYSPACE, b"owned".to_vec())
            .await
            .is_some()
    );
    assert!(matches!(
        prepare(&first, 1).await,
        Err(GroupDeletionError::Conflict(_))
    ));
    drive(
        CreateBucketOperation::new("after-cancel".into(), first.bucket()),
        &first.context,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn writer_conflicts_freeze() {
    let fixture = Fixture::new(&[1]).await;
    let mut operation = CallRun::new(CreateBucketOperation::new(
        "racing".into(),
        fixture.bucket(),
    ));
    let mut effects = VecDeque::from_iter(operation.start());
    let mut prepared = false;
    while !operation.is_complete() {
        let effect = effects
            .pop_front()
            .expect("bucket creation must make progress");
        let Effect::Storage(effect) = effect else {
            panic!("unexpected bucket effect");
        };
        if matches!(effect, StorageEffect::CommitTransaction { .. }) {
            assert!(!prepared);
            prepare(&fixture, 1).await.unwrap();
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
        Err(CreateBucketError::StorageError(
            StorageError::TransactionConflict
        ))
    ));
    assert!(
        read(&fixture.context, S3_BUCKET_KEYSPACE, b"racing".to_vec())
            .await
            .is_none()
    );
    assert!(matches!(
        drive(
            CreateBucketOperation::new("blocked".into(), fixture.bucket()),
            &fixture.context
        )
        .await,
        Err(CreateBucketError::GroupWrite(GroupWriteError::Frozen))
    ));
}

#[tokio::test]
async fn replacement_cannot_reopen() {
    use crate::connectors::create_connector::{SourceConnectorInput, SourceConnectorOperation};
    use crate::connectors::delete_connector::{DeleteSourceInput, DeleteSourceOperation};
    use crate::connectors::replace_connector::{
        ReplaceSourceError, ReplaceSourceInput, ReplaceSourceOperation,
    };
    use crate::connectors::repository::{
        parse_secret_read, read_secret_effect, source_connector_key,
    };
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::SOURCE_INDEX_KEYSPACE;
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
        let mut operation = CallRun::new(ReplaceSourceOperation::new(ReplaceSourceInput {
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
        }));
        let mut effects = VecDeque::from_iter(operation.start());
        let mut deleted = false;
        while !operation.is_complete() {
            let Effect::Storage(effect) = effects.pop_front().expect("replacement must progress")
            else {
                panic!("unexpected replacement effect");
            };
            if !deleted && matches!(&effect, StorageEffect::VaultRead { .. }) {
                drive(
                    DeleteSourceOperation::new(DeleteSourceInput {
                        group_id: fixture.plan.group_id,
                        connector_id,
                    }),
                    &fixture.context,
                )
                .await
                .unwrap();
                let proof = prepare(&fixture, 1).await.unwrap();
                commit(&fixture, vec![proof]).await.unwrap();
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
        let key = source_connector_key(fixture.plan.group_id, connector_id);
        let record = read(&fixture.context, SOURCE_INDEX_KEYSPACE, key.to_vec()).await;
        assert!(record.is_none());
        let secret = fixture
            .context
            .storage_handle
            .send_effect(read_secret_effect(connector_id, None))
            .await;
        assert!(parse_secret_read(secret, connector_id).unwrap().is_none());
    }
}
