//! Tests publishing: the realm entry write, refusals, and the copy count before the answer.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::plugins::calls::*;
use crate::plugins::runs::tests::opened;
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::StorageEvent;
use aruna_core::keyspaces::AUTH_KEYSPACE;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId, RealmNodeKind};
use std::collections::BTreeSet;
use ulid::Ulid;

const FIXTURE: &[u8] = include_bytes!("../../../plugins/tests/fixture.wasm");

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
    write_copies(ctx, plugin, BTreeSet::from([node(1)])).await
}

/// Writes the entry as if `copies` had confirmed a complete copy.
async fn write_copies(
    ctx: &DriverContext,
    plugin: PublishedPlugin,
    copies: BTreeSet<NodeId>,
) -> Result<PublishedPlugin, EntryError> {
    let actor = actor();
    let config = PublishEntryConfig {
        auth_context: auth(&actor),
        actor,
        plugin,
        copies,
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
    let (_dir, ctx) = opened();
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
    let (_dir, ctx) = opened();
    seed(&ctx, &actor(), RealmNodeKind::Server, 1, true).await;
    let digest = format!("sha256:{}", "ab".repeat(32));
    assert_eq!(
        write_entry(&ctx, entry(&digest)).await,
        Err(EntryError::NotManagementNode)
    );
    let (_dir, ctx) = opened();
    seed(&ctx, &actor(), RealmNodeKind::Management, 0, false).await;
    assert_eq!(
        write_entry(&ctx, entry(&digest)).await,
        Err(EntryError::Unauthorized)
    );
    assert!(published(&ctx).await.is_empty());
}

#[tokio::test]
async fn publishes_after_copies() {
    let (_dir, ctx) = opened();
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
    assert_eq!(published(&ctx).await, std::slice::from_ref(&plugin));
    let kept = read_local(&ctx, &plugin.digest).await.unwrap();
    assert_eq!(
        kept.map(|copy| copy.bytes).as_deref(),
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
    let (_dir, ctx) = opened();
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
    let (_dir, ctx) = opened();
    let actor = actor();
    seed(&ctx, &actor, RealmNodeKind::Management, 0, false).await;
    let refused = publish_plugin(&ctx, actor.clone(), auth(&actor), FIXTURE.to_vec()).await;
    assert!(
        matches!(refused, Err(PublishError::Unauthorized)),
        "{refused:?}"
    );
    let digest = ArtifactDigest::of(FIXTURE).to_string();
    assert!(read_local(&ctx, &digest).await.unwrap().is_none());
    let invalid = publish_plugin(&ctx, actor.clone(), auth(&actor), b"not wasm".to_vec()).await;
    assert!(
        matches!(invalid, Err(PublishError::Invalid(_))),
        "{invalid:?}"
    );
    let large = publish_plugin(&ctx, actor.clone(), auth(&actor), vec![0; MAX_ARTIFACT + 1]).await;
    assert!(matches!(large, Err(PublishError::TooLarge)), "{large:?}");
}

#[tokio::test]
async fn counts_current_managers() {
    // Managers 1, 10 and 11: a copy confirmed by a node that left the managers does not count.
    let (_dir, ctx) = opened();
    seed(&ctx, &actor(), RealmNodeKind::Management, 2, true).await;
    let digest = format!("sha256:{}", "ab".repeat(32));
    let left = BTreeSet::from([node(1), node(99)]);
    let refused = write_copies(&ctx, entry(&digest), left).await;
    assert_eq!(
        refused,
        Err(EntryError::TooFewCopies {
            stored: 1,
            needed: 2
        })
    );
    assert!(published(&ctx).await.is_empty());
    let remote = BTreeSet::from([node(1), node(10)]);
    assert!(write_copies(&ctx, entry(&digest), remote).await.is_ok());
    assert_eq!(published(&ctx).await.len(), 1);
}

/// A finished window member with the answer `stored`.
fn answered(index: usize, stored: Result<bool, String>) -> IoResult {
    let event = Event::Net(NetEvent::ArtifactStored(stored));
    IoResult::Group(GroupEvent::Member {
        index,
        result: GroupResult::Done(Box::new(event)),
    })
}

fn sent(node_id: NodeId, artifact: &[u8], index: u32) -> Effect {
    let chunk = chunk_of("sha256:d", artifact, index);
    Effect::Net(NetEffect::ArtifactStore(Box::new(ArtifactStore {
        node_id,
        chunk,
    })))
}

#[test]
fn copies_in_window() {
    use crate::plugins::copy_rows::CHUNK;
    let artifact = vec![7u8; CHUNK + 1];
    let (first, second) = (node(1), node(2));
    let mut copy = copy_artifact(vec![first, second], "sha256:d".into(), artifact.clone());
    let group = EffectGroup {
        members: vec![sent(first, &artifact, 0), sent(second, &artifact, 0)],
        limit: 2,
        form: GroupForm::Window,
    };
    assert_eq!(copy.first(), Ok(Next::Call(IoCall::Group(group))));
    // The first node goes on with its second chunk and completes; the second fails.
    let next = Box::new(sent(first, &artifact, 1));
    assert_eq!(
        copy.next(answered(0, Ok(false))),
        Ok(Next::Call(IoCall::Effect(next)))
    );
    assert_eq!(copy.next(answered(1, Err("down".into()))), Ok(Next::Wait));
    assert_eq!(copy.next(answered(2, Ok(true))), Ok(Next::Wait));
    let closed = IoResult::Group(GroupEvent::Closed);
    assert_eq!(copy.next(closed), Ok(Next::Done(BTreeSet::from([first]))));
}

#[test]
fn copy_refuses_answers() {
    let mut copy = copy_artifact(vec![node(1)], "sha256:d".into(), vec![1]);
    copy.first().unwrap();
    let got = "GroupEvent::Member";
    let unknown = copy.next(answered(5, Ok(true)));
    assert_eq!(unknown, Err(CopyError::Unexpected { got }));
    let got = "IoResult::Done";
    assert_eq!(
        copy.next(IoResult::Done),
        Err(CopyError::Unexpected { got })
    );
    // Without peers nothing is sent and no node confirms.
    let mut alone = copy_artifact(Vec::new(), "sha256:d".into(), vec![1]);
    assert_eq!(alone.first(), Ok(Next::Done(BTreeSet::new())));
}

mod operations {
    use super::*;
    use crate::plugins::fixtures::{config_result, is_child};
    use crate::plugins::rows::tests::child_result;

    fn managers_of(count: u8) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::new(actor().realm_id, Vec::new(), 3);
        config.ensure_node(actor().node_id, RealmNodeKind::Management);
        for seed in 0..count {
            config.ensure_node(node(100 + seed), RealmNodeKind::Management);
        }
        config
    }

    fn check() -> CheckPublish {
        let actor = actor();
        CheckPublish::new(&actor, &auth(&actor), FIXTURE, 5).unwrap()
    }

    fn allowed() -> IoResult {
        child_result(Ok::<bool, AuthorizationError>(true))
    }

    #[test]
    fn check_refuses_artifacts() {
        let actor = actor();
        let auth = auth(&actor);
        let large = vec![0; MAX_ARTIFACT + 1];
        let refused = CheckPublish::new(&actor, &auth, &large, 5);
        assert_eq!(refused.unwrap_err(), PublishError::TooLarge);
        let invalid = CheckPublish::new(&actor, &auth, b"not wasm", 5);
        assert!(matches!(invalid, Err(PublishError::Invalid(_))));
    }

    #[test]
    fn check_needs_admin() {
        let mut publish = check();
        assert!(is_child(&publish.first()));
        let denied = child_result(Ok::<bool, AuthorizationError>(false));
        assert_eq!(publish.next(denied), Err(PublishError::Unauthorized));
    }

    #[test]
    fn check_plans_store() {
        let mut publish = check();
        publish.first().unwrap();
        assert!(is_child(&publish.next(allowed())));
        let Ok(Next::Done(PublishStep::Store(plan))) = publish.next(config_result(managers_of(0)))
        else {
            panic!("a new version is stored");
        };
        assert_eq!(plan.entry.published_at_ms, 5);
        let mut other = check();
        other.first().unwrap();
        other.next(allowed()).unwrap();
        let bare = RealmConfigDocument::new(actor().realm_id, Vec::new(), 3);
        let refused = other.next(config_result(bare));
        assert_eq!(refused, Err(PublishError::NotManagementNode));
    }

    #[test]
    fn check_knows_version() {
        let mut publish = check();
        let entry = publish.entry.clone();
        publish.first().unwrap();
        publish.next(allowed()).unwrap();
        let mut config = managers_of(0);
        config.plugins.push(entry.clone());
        let known = publish.next(config_result(config));
        assert_eq!(known, Ok(Next::Done(PublishStep::Known(entry))));
    }

    fn finish(managers: u8) -> FinishPublish {
        let actor = actor();
        let auth = auth(&actor);
        let plan = PublishPlan {
            config: managers_of(managers),
            entry: entry("sha256:aa"),
        };
        FinishPublish::new(actor, auth, plan, FIXTURE.to_vec())
    }

    #[test]
    fn finish_counts_copies() {
        let mut publish = finish(2);
        assert!(is_child(&publish.first()));
        let none = child_result(Ok::<_, CopyError>(BTreeSet::<NodeId>::new()));
        let short = PublishError::TooFewCopies {
            stored: 1,
            needed: 2,
        };
        assert_eq!(publish.next(none), Err(short));
    }

    #[test]
    fn finish_writes_entry() {
        let mut publish = finish(2);
        publish.first().unwrap();
        let copies = BTreeSet::from([node(100)]);
        assert!(is_child(
            &publish.next(child_result(Ok::<_, CopyError>(copies)))
        ));
        let written = entry("sha256:aa");
        let answer = publish.next(child_result(Ok::<_, EntryError>(written.clone())));
        assert_eq!(answer, Ok(Next::Done(written)));
    }
}
