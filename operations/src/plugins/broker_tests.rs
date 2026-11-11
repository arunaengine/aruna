//! Tests metadata and object calls through the broker: reads, listings, a revision-checked
//! patch answered again from its intent, a changed call, and objects outside the grants.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use super::*;
use crate::metadata::MetadataHandle;
use crate::metadata::create_document::{
    CreateDocumentConfig, CreateDocumentOperation, CreateDocumentPayload, create_metadata_document,
};
use crate::metadata::materialization_queue::process_materialization_batch;
use crate::metadata::projector::drain_projection_queue;
use crate::plugins::install::tests::Fixture;
use crate::plugins::runs::tests::queued;

/// Runs the projection and graph work a write queued.
async fn project(ctx: &DriverContext) {
    for _ in 0..3 {
        drain_projection_queue(ctx).await.unwrap();
        process_materialization_batch(ctx).await.unwrap();
    }
}

const ENTITY: &str = r#"{"description":"first"}"#;

/// A broker for a run of a fresh install, and a document of the install's group.
async fn broker() -> (Fixture, RunBroker, Ulid, tempfile::TempDir) {
    let fixture = Fixture::new().await;
    let installed = fixture.install(&fixture.request(None)).await.unwrap();
    let mut ctx = fixture.ctx.clone();
    let dir = tempfile::tempdir().unwrap();
    let metadata = MetadataHandle::new(
        dir.path().join("meta"),
        fixture.actor.node_id,
        fixture.ctx.storage_handle.clone(),
        None,
        None,
        None,
    );
    ctx.metadata_handle = Some(metadata.unwrap());
    let ctx = Arc::new(ctx);
    let create = CreateDocumentOperation::new(CreateDocumentConfig {
        actor: fixture.actor.clone(),
        group_id: fixture.group_id,
        document_id: Ulid::nil(),
        document_path: "/docs/one/".to_string(),
        public: false,
        payload: CreateDocumentPayload::Scaffold {
            name: "one".to_string(),
            description: String::new(),
            date_published: "2026-01-01".to_string(),
            license: None,
        },
    });
    let created = create_metadata_document(create, ctx.clone()).await.unwrap();
    project(&ctx).await;
    let mut run = queued(installed.install_id, 1);
    run.digest = installed.digest.clone();
    let scope = RunScope {
        realm_id: fixture.actor.realm_id,
        local: fixture.actor.node_id,
        run,
    };
    let broker = RunBroker::new(ctx, scope);
    (fixture, broker, created.record.document_id, dir)
}

fn cx() -> CallContext {
    CallContext {
        run: "run".into(),
        install: "install".into(),
        max_bytes: 1 << 20,
    }
}

fn patch(document: Ulid, expected: &str, ordinal: u32, entity: &str) -> MetadataPatch {
    MetadataPatch {
        resource: document.to_string(),
        patch: entity.to_string(),
        expected: expected.to_string(),
        intent: Intent::new(ordinal, None, &[entity.as_bytes(), expected.as_bytes()]),
    }
}

#[tokio::test]
async fn metadata_calls() {
    let (_fixture, broker, document, _dir) = broker().await;
    let read = broker
        .metadata_get(&cx(), &document.to_string())
        .await
        .unwrap();
    let listed = broker.metadata_list(&cx(), None).await.unwrap();
    assert_eq!(listed.resources, [document.to_string()]);
    assert_eq!(listed.cursor, None);

    let first = patch(document, &read.revision, 0, ENTITY);
    let revision = broker.metadata_patch(&cx(), first.clone()).await.unwrap();
    assert_ne!(revision, read.revision);
    project(&broker.ctx).await;
    // The same call again answers its stored reply and changes nothing.
    assert_eq!(broker.metadata_patch(&cx(), first).await.unwrap(), revision);
    let now = broker
        .metadata_get(&cx(), &document.to_string())
        .await
        .unwrap();
    assert_eq!(now.revision, revision);

    // A stale revision is refused without an effect; the call may be made again.
    let stale = patch(document, &read.revision, 1, ENTITY);
    let refused = broker.metadata_patch(&cx(), stale).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::Conflict);
    let retried = patch(document, &revision, 1, ENTITY);
    assert!(broker.metadata_patch(&cx(), retried).await.is_ok());
    assert!(!broker.changed());
    // Until the patch is materialized, a read of the document is retryable.
    let pending = broker.metadata_get(&cx(), &document.to_string()).await;
    assert_eq!(pending.unwrap_err().kind, ErrorKind::Unavailable);
    project(&broker.ctx).await;
    let other = patch(document, &revision, 0, r#"{"description":"other"}"#);
    let changed = broker.metadata_patch(&cx(), other).await.unwrap_err();
    assert_eq!(changed.kind, ErrorKind::Conflict);
    assert!(broker.changed());
}

#[tokio::test]
async fn object_calls() {
    let (fixture, broker, _, _dir) = broker().await;
    let data = format!("/{}/g/{}/data", fixture.actor.realm_id, fixture.group_id);
    let path = format!("{data}/{}/bucket/a.txt", fixture.actor.node_id);
    let refused = broker.objects_info(&cx(), &path).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::NotGranted);
    let meta = format!("/{}/g/{}/meta/x", fixture.actor.realm_id, fixture.group_id);
    assert!(broker.granted(&cx(), Permission::MetadataRead, &meta).await);
    assert!(!broker.granted(&cx(), Permission::ObjectsRead, &path).await);

    // With `objects.read`, objects of other nodes and buckets outside the group are refused.
    let reads = serde_json::json!({ "permissions": { "required": ["objects.read"] } });
    let mut request = fixture.request(None);
    request.version = Some(fixture.publish("0.1.1", "cd", reads).await);
    let installed = fixture.install(&request).await.unwrap();
    let mut scope = broker.scope.clone();
    scope.run.install_id = installed.install_id;
    scope.run.digest = installed.digest;
    let objects = RunBroker::new(broker.ctx.clone(), scope);
    let missing = objects.objects_info(&cx(), &path).await.unwrap_err();
    assert_eq!(missing.kind, ErrorKind::NotFound);
    let remote = format!(
        "{data}/{}/bucket/a.txt",
        iroh::SecretKey::from_bytes(&[9u8; 32]).public()
    );
    let remote = objects
        .objects_list(&cx(), &remote, None)
        .await
        .unwrap_err();
    assert_eq!(remote.kind, ErrorKind::Unavailable);
}

#[tokio::test]
async fn state_and_log() {
    let (_fixture, broker, _, _dir) = broker().await;
    let put = StatePut {
        key: "cursor".into(),
        value: b"a".to_vec(),
        expected: None,
        intent: Intent::new(0, None, &[b"cursor", b"a"]),
    };
    assert_eq!(broker.state_put(&cx(), put.clone()).await.unwrap(), 1);
    // The same call again answers its stored version and writes nothing.
    assert_eq!(broker.state_put(&cx(), put).await.unwrap(), 1);
    let entry = broker.state_get(&cx(), "cursor").await.unwrap().unwrap();
    assert_eq!((entry.version, entry.value), (1, b"a".to_vec()));
    let stale = StatePut {
        key: "cursor".into(),
        value: b"b".to_vec(),
        expected: None,
        intent: Intent::new(1, None, &[b"cursor", b"b"]),
    };
    let refused = broker.state_put(&cx(), stale).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::Conflict);
    let empty = broker.state_get(&cx(), "").await.unwrap_err();
    assert_eq!(empty.kind, ErrorKind::Invalid);

    broker.log(&cx(), "info", "started").await;
    let lines = broker.take_log();
    assert_eq!(lines.len(), 1);
    assert_eq!(
        (lines[0].level.as_str(), lines[0].message.as_str()),
        ("info", "started")
    );
}

/// A broker for a run of an install granted `objects.read` on `patterns` only.
async fn objects_broker(
    fixture: &Fixture,
    broker: &RunBroker,
    patterns: &[String],
) -> (RunBroker, PluginInstall) {
    let reads = serde_json::json!({ "permissions": { "required": ["objects.read"] } });
    let mut request = fixture.request(None);
    request.version = Some(fixture.publish("0.1.2", "ef", reads).await);
    let grants = BTreeMap::from([("objects.read".to_string(), patterns.to_vec())]);
    request.grants = grants;
    let installed = fixture.install(&request).await.unwrap();
    let mut scope = broker.scope.clone();
    scope.run.install_id = installed.install_id;
    scope.run.digest = installed.digest.clone();
    (RunBroker::new(broker.ctx.clone(), scope), installed)
}

#[tokio::test]
async fn object_paths() {
    // A key of bucket `data` never passes a grant for bucket `dataset`.
    let (fixture, broker, _, _dir) = broker().await;
    let (realm, group, node) = (
        fixture.actor.realm_id,
        fixture.group_id,
        fixture.actor.node_id,
    );
    let dataset = format!("/{realm}/g/{group}/data/{node}/dataset/**");
    let (objects, install) = objects_broker(&fixture, &broker, &[dataset]).await;
    let listed = vec![("set/private".to_string(), 1, "v1".to_string())];
    assert!(
        objects
            .visible(&install, "data", listed)
            .await
            .unwrap()
            .is_empty()
    );
    let listed = vec![("private".to_string(), 1, "v1".to_string())];
    let shown = objects
        .visible(&install, "dataset", listed.clone())
        .await
        .unwrap();
    let path = format!("/{realm}/g/{group}/data/{node}/dataset/private");
    assert_eq!(
        shown.iter().map(|object| &object.path).collect::<Vec<_>>(),
        [&path]
    );
}

#[tokio::test]
async fn listings_authorize() {
    // A target an individual call refuses is left out of the listing too.
    let (fixture, broker, document, _dir) = broker().await;
    let listed = broker.metadata_list(&cx(), None).await.unwrap();
    assert_eq!(listed.resources, [document.to_string()]);
    fixture.deny(&["operation == 'plugin.metadata.read'"]).await;
    let refused = broker.metadata_get(&cx(), &document.to_string()).await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::Denied);
    assert!(
        broker
            .metadata_list(&cx(), None)
            .await
            .unwrap()
            .resources
            .is_empty()
    );

    let (realm, group, node) = (
        fixture.actor.realm_id,
        fixture.group_id,
        fixture.actor.node_id,
    );
    let data = format!("/{realm}/g/{group}/data/{node}/data/**");
    fixture.deny(&[]).await;
    let (objects, install) = objects_broker(&fixture, &broker, &[data]).await;
    let listed = vec![("a.txt".to_string(), 1, "v1".to_string())];
    let shown = objects
        .visible(&install, "data", listed.clone())
        .await
        .unwrap();
    assert_eq!(shown.len(), 1);
    fixture.deny(&["operation == 'plugin.objects.read'"]).await;
    let path = format!("/{realm}/g/{group}/data/{node}/data/a.txt");
    let refused = objects.objects_info(&cx(), &path).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::Denied);
    assert!(
        objects
            .visible(&install, "data", listed)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn dataset_patches() {
    // The example plugins' patch adds a field to the dataset and keeps the others.
    let (_fixture, broker, document, _dir) = broker().await;
    let resource = document.to_string();
    let read = broker.metadata_get(&cx(), &resource).await.unwrap();
    let example = r#"{"identifier":{"catalog":"c-1"}}"#;
    let revision = broker
        .metadata_patch(&cx(), patch(document, &read.revision, 0, example))
        .await
        .unwrap();
    project(&broker.ctx).await;
    let after = broker.metadata_get(&cx(), &resource).await.unwrap();
    assert_eq!(after.revision, revision);
    let dataset: Value = serde_json::from_str(&after.json).unwrap();
    assert_eq!(dataset["name"], "one");
    assert_eq!(dataset["identifier"], serde_json::json!({"catalog": "c-1"}));
    // A later patch merges into the stored object.
    let more = r#"{"identifier":{"doi":"d-1"}}"#;
    let revision = broker
        .metadata_patch(&cx(), patch(document, &revision, 1, more))
        .await
        .unwrap();
    project(&broker.ctx).await;
    let after = broker.metadata_get(&cx(), &resource).await.unwrap();
    let dataset: Value = serde_json::from_str(&after.json).unwrap();
    let both = serde_json::json!({"catalog": "c-1", "doi": "d-1"});
    assert_eq!(dataset["identifier"], both);
    let moved = patch(document, &revision, 2, r#"{"@id":"./other"}"#);
    let refused = broker.metadata_patch(&cx(), moved).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::Invalid);
}

#[test]
fn merge_patch() {
    let crate_json = r#"{"@graph":[{"@id":"ro-crate-metadata.json","about":{"@id":"./"}},{"@id":"./","name":"a","keywords":["x"],"identifier":{"doi":"d"}}]}"#;
    let merged = merge_dataset(
        crate_json,
        r#"{"identifier":{"catalog":"c"},"keywords":null}"#,
    );
    let merged = merged.unwrap();
    let stored: Value = serde_json::from_str(&merged).unwrap();
    assert_eq!(stored["@graph"][1]["identifier"]["@type"], "@json");
    let dataset: Value = serde_json::from_str(&dataset_of(&merged).unwrap()).unwrap();
    assert_eq!(dataset["name"], "a");
    let both = serde_json::json!({"doi": "d", "catalog": "c"});
    assert_eq!(dataset["identifier"], both);
    assert!(dataset.get("keywords").is_none());
    assert!(merge_dataset(crate_json, "[1]").is_err());
}
