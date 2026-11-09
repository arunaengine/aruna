//! Tests publishing: the realm entry write, refusals, and the copy count before the answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::plugins::artifacts::read_local;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId, RealmNodeKind};
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

const FIXTURE: &[u8] = include_bytes!("../../../plugins/tests/fixture.wasm");

fn context() -> (TempDir, DriverContext) {
    let dir = tempdir().unwrap();
    let ctx = DriverContext {
        storage_handle: aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    };
    (dir, ctx)
}

fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

fn actor() -> Actor {
    let realm_id = RealmId::from_bytes([1u8; 32]);
    Actor {
        node_id: node(1),
        user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
        realm_id,
    }
}

fn auth(actor: &Actor) -> AuthContext {
    AuthContext {
        user_id: actor.user_id,
        realm_id: actor.realm_id,
        path_restrictions: None,
        session: None,
    }
}

async fn write(ctx: &DriverContext, key_space: &str, key: Key, value: Vec<u8>) {
    let effect = StorageEffect::Write {
        key_space: key_space.to_string(),
        key,
        value: value.into(),
        txn_id: None,
    };
    match ctx.storage_handle.send_storage_effect(effect).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected event: {other:?}"),
    }
}

/// Seeds a config with the actor's node as `kind` plus `others` management nodes.
async fn seed(ctx: &DriverContext, actor: &Actor, kind: RealmNodeKind, others: u8, admin: bool) {
    let mut config = RealmConfigDocument::new(actor.realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, kind);
    for seed in 0..others {
        config.ensure_node(node(10 + seed), RealmNodeKind::Management);
    }
    let target = DocumentTarget::RealmConfig {
        realm_id: actor.realm_id,
    };
    let value = config.to_bytes(actor).unwrap();
    write(ctx, target.storage_keyspace(), target.storage_key(), value).await;
    let mut document = RealmAuthorizationDocument::default_realm_doc(actor.realm_id);
    for role in document.roles.values_mut().filter(|_| admin) {
        role.assigned_users.insert(actor.user_id);
    }
    let key = (*actor.realm_id.as_bytes()).into();
    write(ctx, AUTH_KEYSPACE, key, document.to_bytes(actor).unwrap()).await;
}

fn entry(digest: &str) -> PublishedPlugin {
    PublishedPlugin {
        name: "fixture".into(),
        version: "0.2.0".into(),
        digest: digest.to_string(),
        size: 10,
        manifest: r#"{"name":"fixture"}"#.into(),
        published_by: actor().user_id,
        published_at_ms: 1,
    }
}

async fn write_entry(
    ctx: &DriverContext,
    plugin: PublishedPlugin,
) -> Result<PublishedPlugin, EntryError> {
    let actor = actor();
    let config = PublishEntryConfig {
        auth_context: auth(&actor),
        actor,
        plugin,
    };
    drive(PublishEntryOperation::new(config), ctx).await
}

async fn published(ctx: &DriverContext) -> Vec<PublishedPlugin> {
    drive(GetConfigOperation::new(actor().realm_id), ctx)
        .await
        .unwrap()
        .plugins
}

#[tokio::test]
async fn writes_entry_once() {
    let (_dir, ctx) = context();
    seed(&ctx, &actor(), RealmNodeKind::Management, 0, true).await;
    let digest = format!("sha256:{}", "ab".repeat(32));
    assert_eq!(write_entry(&ctx, entry(&digest)).await, Ok(entry(&digest)));
    assert_eq!(published(&ctx).await, [entry(&digest)]);
    let mut again = entry(&digest);
    again.published_at_ms = 2;
    assert_eq!(
        write_entry(&ctx, again).await,
        Ok(entry(&digest)),
        "the first entry stays"
    );
    let other = format!("sha256:{}", "cd".repeat(32));
    let taken = write_entry(&ctx, entry(&other)).await;
    assert_eq!(taken, Err(EntryError::VersionTaken("0.2.0".into())));
    assert_eq!(published(&ctx).await, [entry(&digest)]);
}

#[tokio::test]
async fn entry_refusals() {
    let (_dir, ctx) = context();
    seed(&ctx, &actor(), RealmNodeKind::Server, 1, true).await;
    let digest = format!("sha256:{}", "ab".repeat(32));
    assert_eq!(
        write_entry(&ctx, entry(&digest)).await,
        Err(EntryError::NotManagementNode)
    );
    let (_dir, ctx) = context();
    seed(&ctx, &actor(), RealmNodeKind::Management, 0, false).await;
    assert_eq!(
        write_entry(&ctx, entry(&digest)).await,
        Err(EntryError::Unauthorized)
    );
    assert!(published(&ctx).await.is_empty());
}

#[tokio::test]
async fn publishes_after_copies() {
    let (_dir, ctx) = context();
    let actor = actor();
    seed(&ctx, &actor, RealmNodeKind::Management, 0, true).await;
    let plugin = publish_plugin(&ctx, actor.clone(), auth(&actor), FIXTURE.to_vec())
        .await
        .unwrap();
    assert_eq!(plugin.digest, ArtifactDigest::of(FIXTURE).to_string());
    assert_eq!(
        (plugin.name.as_str(), plugin.size),
        ("fixture", FIXTURE.len() as u64)
    );
    assert_eq!(published(&ctx).await, [plugin.clone()]);
    let kept = read_local(&ctx, &plugin.digest).await.unwrap();
    assert_eq!(
        kept.as_deref(),
        Some(FIXTURE),
        "the publishing node keeps a copy"
    );
    let again = publish_plugin(&ctx, actor.clone(), auth(&actor), FIXTURE.to_vec())
        .await
        .unwrap();
    assert_eq!(
        again, plugin,
        "publishing the same bytes again changes nothing"
    );
}

#[tokio::test]
async fn too_few_copies() {
    let (_dir, ctx) = context();
    let actor = actor();
    seed(&ctx, &actor, RealmNodeKind::Management, 2, true).await;
    let refused = publish_plugin(&ctx, actor.clone(), auth(&actor), FIXTURE.to_vec()).await;
    assert!(
        matches!(
            refused,
            Err(PublishError::TooFewCopies {
                stored: 1,
                needed: 2
            })
        ),
        "{refused:?}"
    );
    assert!(
        published(&ctx).await.is_empty(),
        "nothing is published without a majority"
    );
}

#[tokio::test]
async fn refuses_before_storing() {
    let (_dir, ctx) = context();
    let actor = actor();
    seed(&ctx, &actor, RealmNodeKind::Management, 0, false).await;
    let refused = publish_plugin(&ctx, actor.clone(), auth(&actor), FIXTURE.to_vec()).await;
    assert!(
        matches!(refused, Err(PublishError::Unauthorized)),
        "{refused:?}"
    );
    let digest = ArtifactDigest::of(FIXTURE).to_string();
    assert_eq!(read_local(&ctx, &digest).await.unwrap(), None);
    let invalid = publish_plugin(&ctx, actor.clone(), auth(&actor), b"not wasm".to_vec()).await;
    assert!(
        matches!(invalid, Err(PublishError::Invalid(_))),
        "{invalid:?}"
    );
    let large = publish_plugin(&ctx, actor.clone(), auth(&actor), vec![0; MAX_ARTIFACT + 1]).await;
    assert!(matches!(large, Err(PublishError::TooLarge)), "{large:?}");
}
