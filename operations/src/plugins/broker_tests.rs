//! Tests metadata, object and secret value calls through the broker: reads, listings, a
//! revision-checked patch answered again from its intent, a changed call, and missing grants.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use super::*;
use crate::metadata::MetadataHandle;
use crate::metadata::api::{DocumentQueryRequest, query_metadata_document};
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
fn change_patch() {
    let crate_json = r#"{"@graph":[{"@id":"ro-crate-metadata.json","about":{"@id":"./"}},{"@id":"./","@type":"Dataset","name":"a","keywords":["x"],"description":"kept","identifier":{"@type":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON","@value":"{\"doi\":\"d\"}"}}]}"#;
    let patch = r#"{"identifier":{"catalog":"c"},"keywords":null}"#;
    let change: Value = serde_json::from_str(&dataset_change(crate_json, patch).unwrap()).unwrap();
    // Only the id, types, name and the patched fields are part of the change.
    let mut fields: Vec<&str> = change
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    fields.sort();
    assert_eq!(fields, ["@id", "@type", "identifier", "keywords", "name"]);
    assert_eq!(change["keywords"], Value::Null);
    let identifier = change["identifier"]["@value"].as_str().unwrap();
    let identifier: Value = serde_json::from_str(identifier).unwrap();
    assert_eq!(identifier, serde_json::json!({"doi": "d", "catalog": "c"}));
    assert!(dataset_change(crate_json, "[1]").is_err());
    assert!(dataset_change(crate_json, r#"{"@id":"./b"}"#).is_err());
}

#[tokio::test]
async fn patch_keeps_entities() {
    // A patch of the dataset leaves nested data entities and unreferenced contextual ones.
    let (fixture, broker, _, _dir) = broker().await;
    let jsonld = serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.1/context",
        "@graph": [
            {"@id": "ro-crate-metadata.json", "@type": "CreativeWork", "about": {"@id": "./"},
             "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}},
            {"@id": "./", "@type": "Dataset", "name": "two", "description": "d",
             "datePublished": "2026-01-01", "hasPart": [{"@id": "data/"}]},
            {"@id": "data/", "@type": "Dataset", "name": "data", "hasPart": [{"@id": "data/file.txt"}]},
            {"@id": "data/file.txt", "@type": "File", "name": "nested file"},
            {"@id": "#lab", "@type": "Organization", "name": "unreferenced lab"}
        ]
    });
    let create = CreateDocumentOperation::new(CreateDocumentConfig {
        actor: fixture.actor.clone(),
        group_id: fixture.group_id,
        document_id: Ulid::nil(),
        document_path: "/docs/two/".to_string(),
        public: false,
        payload: CreateDocumentPayload::RoCrate {
            jsonld: jsonld.to_string(),
        },
    });
    let created = create_metadata_document(create, broker.ctx.clone())
        .await
        .unwrap();
    project(&broker.ctx).await;
    let document = created.record.document_id;
    let read = broker
        .metadata_get(&cx(), &document.to_string())
        .await
        .unwrap();
    let example = r#"{"identifier":{"catalog":"c-1"}}"#;
    let change = patch(document, &read.revision, 0, example);
    broker.metadata_patch(&cx(), change).await.unwrap();
    project(&broker.ctx).await;
    let request = DocumentQueryRequest {
        document_id: document,
        auth: Some(fixture.auth()),
        bearer_token: None,
        query: "SELECT ?s ?o WHERE { ?s <http://schema.org/name> ?o }".to_string(),
        mode: None,
        allow_partial: false,
    };
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    let names = query_metadata_document(&broker.ctx, realm, node, request)
        .await
        .unwrap();
    let names = format!("{:?}", names.results);
    for kept in ["nested file", "unreferenced lab", "two"] {
        assert!(names.contains(kept), "{kept} is gone: {names}");
    }
    let after = broker
        .metadata_get(&cx(), &document.to_string())
        .await
        .unwrap();
    assert!(after.json.contains("c-1"), "{}", after.json);
}

#[tokio::test]
async fn write_only_patches() {
    // A plugin that may write a document but not read it patches it at a revision it was given.
    let (fixture, broker, document, _dir) = broker().await;
    let read = broker
        .metadata_get(&cx(), &document.to_string())
        .await
        .unwrap();
    let meta = format!("/{}/g/{}/meta", fixture.actor.realm_id, fixture.group_id);
    let mut request = fixture.request(None);
    request.grants = BTreeMap::from([
        (
            "metadata.read".to_string(),
            vec![format!("{meta}/other/**")],
        ),
        ("metadata.write".to_string(), vec![format!("{meta}/**")]),
    ]);
    let installed = fixture.install(&request).await.unwrap();
    let mut scope = broker.scope.clone();
    scope.run.install_id = installed.install_id;
    scope.run.digest = installed.digest.clone();
    let writer = RunBroker::new(broker.ctx.clone(), scope);
    let unread = writer.metadata_get(&cx(), &document.to_string()).await;
    assert!(unread.is_err());
    let change = patch(
        document,
        &read.revision,
        0,
        r#"{"description":"by a writer"}"#,
    );
    assert!(writer.metadata_patch(&cx(), change).await.is_ok());
}

#[tokio::test]
async fn expected_revision() {
    // A patch starts only from the revision it expects, and writes no intent otherwise.
    use crate::plugins::intents::open_intent;
    let (_fixture, broker, document, _dir) = broker().await;
    let read = broker
        .metadata_get(&cx(), &document.to_string())
        .await
        .unwrap();
    let newer = Ulid::generate().to_string();
    let behind = broker
        .metadata_patch(&cx(), patch(document, &newer, 0, ENTITY))
        .await;
    assert_eq!(behind.unwrap_err().kind, ErrorKind::Unavailable);
    let older = Ulid::from_parts(1, 1).to_string();
    let ahead = broker
        .metadata_patch(&cx(), patch(document, &older, 0, ENTITY))
        .await;
    assert_eq!(ahead.unwrap_err().kind, ErrorKind::Conflict);
    assert_eq!(
        open_intent(&broker.ctx, broker.scope.run.run_id)
            .await
            .unwrap(),
        None
    );
    let now = broker
        .metadata_get(&cx(), &document.to_string())
        .await
        .unwrap();
    assert_eq!(now.revision, read.revision);
    assert!(same_revision(Ulid::from_parts(5, 1), Ulid::from_parts(5, 1)).is_ok());
}

#[tokio::test]
async fn invalid_patches() {
    // A patch refused before dispatch leaves no intent behind, so the run needs no attention.
    use crate::plugins::intents::open_intent;
    let (_fixture, broker, document, _dir) = broker().await;
    let read = broker
        .metadata_get(&cx(), &document.to_string())
        .await
        .unwrap();
    for (ordinal, bad) in [r#"{"@id":"./other"}"#, "[1]", "not json"]
        .into_iter()
        .enumerate()
    {
        let change = patch(document, &read.revision, ordinal as u32, bad);
        let refused = broker.metadata_patch(&cx(), change).await.unwrap_err();
        assert_eq!(refused.kind, ErrorKind::Invalid, "{bad}");
    }
    assert_eq!(
        open_intent(&broker.ctx, broker.scope.run.run_id)
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn secret_values_granted() {
    use crate::plugins::account::{Identity, ensure_role, guard_of};
    use crate::plugins::bound::tests::{TOKEN, bound, edit_config, keys, secret};
    use crate::plugins::grants::role_paths;
    use crate::plugins::rules::ensure_rules;
    let fixture = Fixture::keyed().await;
    let install = bound(&fixture).await;
    secret(&fixture, &install).await;
    let broker = |install: &PluginInstall| {
        let mut run = queued(install.install_id, 1);
        run.digest = install.digest.clone();
        let scope = RunScope {
            realm_id: fixture.actor.realm_id,
            local: fixture.actor.node_id,
            run,
        };
        RunBroker::new(Arc::new(fixture.ctx.clone()), scope).with_keys(keys())
    };
    let kind = async |install: &PluginInstall| {
        let refused = broker(install).secret_value(&cx(), "api_key").await;
        refused.map(|_| ()).map_err(|error| error.kind)
    };
    assert_eq!(kind(&install).await, Err(ErrorKind::NotGranted));
    let path = install_scope(&install).secret("api_key");
    edit_config(&fixture, |config| {
        let grants = &mut config.installs[0].grants;
        grants.insert("secrets.value".to_string(), vec![path]);
    })
    .await;
    let config = read_config(&fixture.ctx, fixture.actor.realm_id).await;
    let granted = config.unwrap().installs.remove(0);
    let (scope, paths) = (
        install_scope(&granted),
        role_paths(&parse_grants(&granted.grants)),
    );
    let (identity, auth) = ((Identity::of(&granted), guard_of(&granted)), fixture.auth());
    let role = ensure_role(
        &fixture.ctx,
        &fixture.actor,
        &auth,
        &scope,
        identity.0,
        &paths,
        identity.1,
    );
    role.await.unwrap();
    // The secret's rules let this install inject it, not read its value.
    assert_eq!(kind(&granted).await, Err(ErrorKind::Denied));
    let config = read_config(&fixture.ctx, fixture.actor.realm_id)
        .await
        .unwrap();
    let plugin = pick_version(&config, &granted.plugin, Some(&granted.digest)).unwrap();
    let manifest = read_manifest(&plugin).unwrap();
    ensure_rules(&fixture.ctx, &fixture.actor, &granted, &manifest)
        .await
        .unwrap();
    let held = broker(&granted);
    let fields = held.secret_value(&cx(), "api_key").await.unwrap();
    let fields: Vec<(String, String)> = (fields.into_iter())
        .map(|field| (field.name, field.value))
        .collect();
    assert_eq!(fields, [("token".to_string(), TOKEN.to_string())]);
    assert_eq!(held.scrub(&format!("got {TOKEN}")), "got [redacted]");
}

/// A create of a dataset named `name` with the fields RO-Crate requires.
fn create(path: &str, name: &str, key: &str, ordinal: u32) -> MetadataCreate {
    let dataset = format!(r#"{{"name":"{name}","description":"d","datePublished":"2026-01-01"}}"#);
    let dataset = dataset.as_str();
    let parts: [&[u8]; 3] = [path.as_bytes(), dataset.as_bytes(), key.as_bytes()];
    MetadataCreate {
        path: path.to_string(),
        dataset: dataset.to_string(),
        key: key.to_string(),
        intent: Intent::new(ordinal, Some(key.to_string()), &parts),
    }
}

#[tokio::test]
async fn keyed_creates() {
    // A create below the granted path answers again from its intent, and a later run with the
    // same key adopts the document instead of making a second one.
    let (fixture, broker, _, _dir) = broker().await;
    let meta = format!("/{}/g/{}/meta", fixture.actor.realm_id, fixture.group_id);
    let creates = serde_json::json!({ "permissions": { "required": ["metadata.create"] } });
    let mut request = fixture.request(None);
    request.version = Some(fixture.publish("0.1.3", "12", creates).await);
    request.grants = BTreeMap::from([(
        "metadata.create".to_string(),
        vec![format!("{meta}/elabftw/**")],
    )]);
    let installed = fixture.install(&request).await.unwrap();
    let mut scope = broker.scope.clone();
    scope.run.install_id = installed.install_id;
    scope.run.digest = installed.digest.clone();
    let creator = RunBroker::new(broker.ctx.clone(), scope.clone());
    let first = create("elabftw/exp-1", "Exp 1", "e1", 0);
    let created = creator.metadata_create(&cx(), first.clone()).await.unwrap();
    project(&broker.ctx).await;
    assert_eq!(
        creator.metadata_create(&cx(), first.clone()).await,
        Ok(created.clone())
    );
    // A lost reply leaves the call unknown in its run; the next run recovers by the key.
    let lost = "the reply was lost";
    mark_unknown(&broker.ctx, scope.run.run_id, &first.intent, lost)
        .await
        .unwrap();
    let unknown = creator.metadata_create(&cx(), first).await.unwrap_err();
    assert_eq!(unknown.kind, ErrorKind::UnknownOutcome);

    scope.run.run_id = Ulid::generate();
    let later = RunBroker::new(broker.ctx.clone(), scope);
    let edited = create("elabftw/exp-1", "Exp 1 edited", "e1", 0);
    assert_eq!(
        later.metadata_create(&cx(), edited).await,
        Ok(created.clone())
    );
    let other = create("elabftw/exp-2", "Exp 2", "e2", 1);
    let second = later.metadata_create(&cx(), other).await.unwrap();
    assert_ne!(second.resource, created.resource);

    let outside = create("other/exp-3", "Exp 3", "e3", 2);
    let refused = later.metadata_create(&cx(), outside).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::NotGranted);
    let mut nested = create("elabftw/exp-4", "Exp 4", "e4", 3);
    nested.dataset = r##"{"name":"Exp 4","author":{"@id":"#a","name":"A"}}"##.to_string();
    let refused = later.metadata_create(&cx(), nested).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::Invalid);
    // A dataset naming an object needs `objects.read` on it, whatever the property.
    let mut linked = create("elabftw/exp-6", "Exp 6", "e6", 5);
    let arn = private_arn(&fixture);
    linked.dataset = serde_json::json!({"name": "Exp 6", "description": "d",
        "datePublished": "2026-01-01", "https://example.org/location": {"@id": arn}})
    .to_string();
    let refused = later.metadata_create(&cx(), linked).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::NotGranted, "{}", refused.message);
    let mut incomplete = create("elabftw/exp-5", "Exp 5", "e5", 4);
    incomplete.dataset = r#"{"name":"Exp 5"}"#.to_string();
    let refused = later.metadata_create(&cx(), incomplete).await.unwrap_err();
    assert_eq!(refused.kind, ErrorKind::Invalid);
}

#[test]
fn dataset_crates() {
    let dataset = r##"{"name":"a","description":"d","datePublished":"2026-01-01","author":{"@id":"#p"},"extra":{"k":1}}"##;
    let stored = dataset_crate(dataset).unwrap();
    let stored: Value = serde_json::from_str(&stored).unwrap();
    let dataset = &stored["@graph"][1];
    assert_eq!(dataset["@id"], "./");
    assert_eq!(dataset["@type"], "Dataset");
    assert_eq!(dataset["author"], serde_json::json!({"@id": "#p"}));
    assert_eq!(dataset["extra"]["@type"], JSON_LITERAL);
    assert_eq!(stored["@graph"][0]["about"]["@id"], "./");
    for refused in [
        "[1]",
        r#"{"@id":"./b"}"#,
        r#"{"@type":"Person"}"#,
        r#"{"@graph":[]}"#,
        r#"{"hasPart":[{"@id":"a.txt","@type":"File"}]}"#,
        r#"{"name":"a"}"#,
    ] {
        assert!(dataset_crate(refused).is_err(), "{refused}");
    }
}

/// Creates a document with three root-linked files of padded names; returns its id.
async fn three_files(fixture: &Fixture, broker: &RunBroker) -> Ulid {
    files_named(fixture, broker, &["a.txt", "b.txt", "c.txt"]).await
}

/// Creates a document with root-linked files of the given ids and padded names.
async fn files_named(fixture: &Fixture, broker: &RunBroker, ids: &[&str]) -> Ulid {
    let file = |name: &&str| serde_json::json!({"@id": name, "@type": "File", "name": format!("{name} {}", "x".repeat(300))});
    let links: Vec<Value> = ids
        .iter()
        .map(|id| serde_json::json!({"@id": id}))
        .collect();
    let mut graph = vec![
        serde_json::json!({"@id": "ro-crate-metadata.json", "@type": "CreativeWork",
            "about": {"@id": "./"}, "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}}),
        serde_json::json!({"@id": "./", "@type": "Dataset", "name": "files", "description": "d",
            "datePublished": "2026-01-01", "hasPart": links}),
    ];
    graph.extend(ids.iter().map(file));
    let jsonld = serde_json::json!({
        "@context": "https://w3id.org/ro/crate/1.1/context",
        "@graph": graph
    });
    let create = CreateDocumentOperation::new(CreateDocumentConfig {
        actor: fixture.actor.clone(),
        group_id: fixture.group_id,
        document_id: Ulid::nil(),
        document_path: "/docs/files/".to_string(),
        public: false,
        payload: CreateDocumentPayload::RoCrate {
            jsonld: jsonld.to_string(),
        },
    });
    let created = create_metadata_document(create, broker.ctx.clone());
    let created = created.await.unwrap();
    project(&broker.ctx).await;
    created.record.document_id
}

/// One page of `document` after `after`, asked with `limit` within `bound` bytes.
async fn page_of(
    fixture: &Fixture,
    broker: &RunBroker,
    (document, after): (Ulid, Option<String>),
    (limit, bound): (usize, u64),
) -> Result<aruna_core::metadata::MetadataRoCratePage, MetadataApiError> {
    let request = ExportMetadataRequest {
        document_id: document,
        auth: Some(fixture.auth()),
        view: RoCrateExportView::Page,
        limit: Some(limit),
        offset: None,
        after,
    };
    let realm = fixture.actor.realm_id;
    match export_rocrate_routed(&broker.ctx, realm, request, None, bound).await? {
        ExportMetadataResult::Page { page, .. } => Ok(page),
        other => panic!("expected a page, got {other:?}"),
    }
}

#[tokio::test]
async fn bounded_pages() {
    // A page over the bound comes back smaller, and its cursor continues at the next file.
    let (fixture, broker, _, _dir) = broker().await;
    let document = three_files(&fixture, &broker).await;
    let one = page_of(&fixture, &broker, (document, None), (1, u64::MAX)).await;
    let two = page_of(&fixture, &broker, (document, None), (2, u64::MAX)).await;
    let bound = two.unwrap().jsonld.len() as u64 - 1;
    let mut after = None;
    let mut seen = 0;
    loop {
        let page = page_of(&fixture, &broker, (document, after), (3, bound)).await;
        let page = page.unwrap();
        assert!(page.jsonld.len() as u64 <= bound);
        assert_eq!(page.returned_data_entities, 1);
        let name = ["a.txt", "b.txt", "c.txt"][seen];
        assert!(page.jsonld.contains(&format!("{name} x")), "{name}");
        seen += 1;
        after = page.next_cursor;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(seen, 3);
    let small = one.unwrap().jsonld.len() as u64 - 1;
    let refused = page_of(&fixture, &broker, (document, None), (3, small)).await;
    assert!(matches!(refused, Err(MetadataApiError::TooLarge)));
}

/// Walks the files of `document` with a reply bound that fits one file page, checking that every
/// page and its cursor fit; returns the files in order.
async fn paged(fixture: &Fixture, broker: &RunBroker, document: Ulid) -> Vec<String> {
    let one = page_of(fixture, broker, (document, None), (1, u64::MAX))
        .await
        .unwrap();
    let cursor = one.next_cursor.as_ref().map_or(0, String::len);
    let small = CallContext {
        max_bytes: one.jsonld.len() + cursor + 8,
        ..cx()
    };
    let (resource, mut after, mut seen) = (document.to_string(), None, Vec::new());
    loop {
        let page = broker
            .metadata_files(&small, &resource, after)
            .await
            .unwrap();
        let files: usize = page.files.iter().map(|file| file.len() + 8).sum();
        let charge = files + page.cursor.as_ref().map_or(0, String::len);
        assert!(
            page.files.len() == 1 && charge <= small.max_bytes,
            "{charge}"
        );
        seen.extend(page.files);
        after = page.cursor;
        if after.is_none() {
            return seen;
        }
    }
}

#[tokio::test]
async fn long_cursors() {
    // Long ids make long cursors; the holder counts them, so pages still fit the reply bound.
    let (fixture, broker, _, _dir) = broker().await;
    let ids: Vec<String> = (0..3)
        .map(|n| format!("{}{n}.txt", "f".repeat(3000)))
        .collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let document = files_named(&fixture, &broker, &ids).await;
    assert_eq!(paged(&fixture, &broker, document).await.len(), 3);
}

#[tokio::test]
async fn file_pages() {
    // Files come in pages within the reply bound; the summary leaves the file links out, while
    // `get` keeps answering the whole dataset entity with them.
    let (fixture, broker, _, _dir) = broker().await;
    let document = three_files(&fixture, &broker).await;
    let resource = document.to_string();
    let all = broker.metadata_files(&cx(), &resource, None).await.unwrap();
    let names: Vec<Value> = (all.files.iter())
        .map(|file| serde_json::from_str::<Value>(file).unwrap()["@id"].clone())
        .collect();
    assert_eq!(names, ["./a.txt", "./b.txt", "./c.txt"]);
    assert_eq!(all.cursor, None);
    assert_eq!(paged(&fixture, &broker, document).await, all.files);
    let summary = broker.metadata_summary(&cx(), &resource).await.unwrap();
    assert!(!summary.json.contains("a.txt"), "{}", summary.json);
    let whole = broker.metadata_get(&cx(), &resource).await.unwrap();
    assert!(whole.json.contains("a.txt"), "{}", whole.json);
    assert_eq!(summary.revision, whole.revision);
}

fn put(document: Ulid, file: &Value, expected: &str, ordinal: u32) -> FileChange {
    let file = file.to_string();
    let parts: [&[u8]; 2] = [file.as_bytes(), expected.as_bytes()];
    let intent = Intent::new(ordinal, None, &parts);
    FileChange {
        resource: document.to_string(),
        file,
        expected: expected.to_string(),
        intent,
    }
}

/// The current revision of a document, read through its summary.
async fn revision_of(broker: &RunBroker, document: Ulid) -> String {
    let resource = document.to_string();
    let summary = broker.metadata_summary(&cx(), &resource).await;
    summary.unwrap().revision
}

/// The `@id` and fields of each file entity of a document, in order.
async fn files_of(broker: &RunBroker, document: Ulid) -> Vec<Value> {
    let resource = document.to_string();
    let page = broker.metadata_files(&cx(), &resource, None).await.unwrap();
    (page.files.iter())
        .map(|file| serde_json::from_str(file).unwrap())
        .collect()
}

#[tokio::test]
async fn file_puts() {
    // A put replaces one file by `@id`, so a property it omits goes; others and their links stay.
    let (fixture, broker, _, _dir) = broker().await;
    let document = three_files(&fixture, &broker).await;
    let typed = serde_json::json!({"@id": "./a.txt", "@type": "File", "name": "a", "encodingFormat": "text/plain"});
    let first = revision_of(&broker, document).await;
    let after = broker
        .metadata_put_file(&cx(), put(document, &typed, &first, 0))
        .await;
    let after = after.unwrap();
    project(&broker.ctx).await;
    assert_eq!(
        files_of(&broker, document).await[0]["encodingFormat"],
        "text/plain"
    );
    let renamed = serde_json::json!({"@id": "./a.txt", "@type": "File", "name": "renamed"});
    let again = put(document, &renamed, &after, 1);
    let latest = broker
        .metadata_put_file(&cx(), again.clone())
        .await
        .unwrap();
    project(&broker.ctx).await;
    assert_eq!(
        broker.metadata_put_file(&cx(), again).await,
        Ok(latest.clone())
    );
    let files = files_of(&broker, document).await;
    assert_eq!(files.len(), 3);
    assert_eq!(files[0]["name"], "renamed");
    assert!(files[0].get("encodingFormat").is_none(), "{}", files[0]);
    assert!(files[1]["name"].as_str().unwrap().starts_with("b.txt"));

    // A new file is added and linked; a stale revision, the root and a nested entity are refused.
    let added = serde_json::json!({"@id": "d.txt", "@type": "File", "name": "d"});
    let current = revision_of(&broker, document).await;
    assert_eq!(current, latest);
    broker
        .metadata_put_file(&cx(), put(document, &added, &current, 2))
        .await
        .unwrap();
    project(&broker.ctx).await;
    assert_eq!(files_of(&broker, document).await.len(), 4);
    let stale = broker
        .metadata_put_file(&cx(), put(document, &added, &first, 3))
        .await;
    assert_eq!(stale.unwrap_err().kind, ErrorKind::Conflict);
    let current = revision_of(&broker, document).await;
    let root = serde_json::json!({"@id": "./", "@type": "File", "name": "root"});
    let refused = broker
        .metadata_put_file(&cx(), put(document, &root, &current, 4))
        .await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::Invalid);
    let nested =
        serde_json::json!({"@id": "e.txt", "@type": "File", "author": {"@id": "#p", "name": "P"}});
    let refused = broker
        .metadata_put_file(&cx(), put(document, &nested, &current, 5))
        .await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::Invalid);

    // A file naming an exact object needs `objects.read` on it.
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    let arn = aruna_core::structs::storage::replication::VersionedObjectArn::new(
        realm,
        node,
        "bucket",
        "a.txt",
        Ulid::generate(),
    );
    let object = serde_json::json!({"@id": arn.unwrap().to_string(), "@type": "File", "name": "o"});
    let refused = broker
        .metadata_put_file(&cx(), put(document, &object, &current, 6))
        .await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::NotGranted);
}

#[tokio::test]
async fn object_references() {
    // Every object a file names passes the plugin's own `objects.read` check: `s3://` and path
    // references need the grant and pass its policies, content ids name no object to check.
    let (fixture, broker, _, _dir) = broker().await;
    let document = three_files(&fixture, &broker).await;
    let revision = revision_of(&broker, document).await;
    let (realm, group, node) = (
        fixture.actor.realm_id,
        fixture.group_id,
        fixture.actor.node_id,
    );
    let file = |id: String| serde_json::json!({"@id": id, "@type": "File", "name": "o"});
    let s3 = file("s3://data/a.txt".to_string());
    let path = file(format!("/{realm}/g/{group}/data/{node}/data/a.txt"));
    let hashed = aruna_core::structs::storage::data::identity::content_id([7; 32]);
    let refusals = [
        (&s3, ErrorKind::NotGranted),
        (&path, ErrorKind::NotGranted),
        (&file(hashed), ErrorKind::Denied),
    ];
    for (n, (file, kind)) in refusals.into_iter().enumerate() {
        let change = put(document, file, &revision, n as u32);
        let refused = broker.metadata_put_file(&cx(), change).await.unwrap_err();
        assert_eq!(refused.kind, kind, "{file}");
    }
    // Every spelling the converter stores as `contentUrl` is checked, in every value form.
    let aliased = |property: &str, url: Value| {
        let mut file = serde_json::json!({"@id": "./alias.txt", "@type": "File", "name": "o"});
        file[property] = url;
        file
    };
    let aliases = [
        aliased("schema:contentUrl", "s3://data/a.txt".into()),
        aliased("http://schema.org/contentUrl", "s3://data/a.txt".into()),
        aliased(
            "https://schema.org/contentUrl",
            serde_json::json!(["s3://data/a.txt"]),
        ),
        aliased("contentUrl", serde_json::json!({"@id": "s3://data/a.txt"})),
    ];
    for (n, file) in aliases.iter().enumerate() {
        let change = put(document, file, &revision, 10 + n as u32);
        let refused = broker.metadata_put_file(&cx(), change).await.unwrap_err();
        assert_eq!(refused.kind, ErrorKind::NotGranted, "{file}");
    }

    let reads = serde_json::json!({ "permissions": {
        "required": ["metadata.read", "metadata.write", "objects.read"] } });
    let mut request = fixture.request(None);
    request.version = Some(fixture.publish("0.1.4", "34", reads).await);
    let installed = fixture.install(&request).await.unwrap();
    let mut scope = broker.scope.clone();
    scope.run.install_id = installed.install_id;
    scope.run.digest = installed.digest.clone();
    let reader = RunBroker::new(broker.ctx.clone(), scope);
    fixture.deny(&["operation == 'plugin.objects.read'"]).await;
    let refused = reader
        .metadata_put_file(&cx(), put(document, &s3, &revision, 0))
        .await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::Denied);
    // Without the deny the check passes; this fixture has no bucket `data`.
    fixture.deny(&[]).await;
    let missing = reader
        .metadata_put_file(&cx(), put(document, &s3, &revision, 1))
        .await;
    assert_eq!(missing.unwrap_err().kind, ErrorKind::NotFound);
}

#[tokio::test]
async fn superseded_merge_journaled() {
    // A merge older than the materialized status whose change is not applied yet is still
    // journaled as an update once; the sender holds it until its own dot is applied.
    use crate::metadata::get_document::load_document_record;
    use crate::metadata::projector::project_create_event;
    use aruna_core::events::Event;
    use aruna_core::metadata::{
        MetadataBatchSource, MetadataEffect, MetadataEvent, MetadataEventPayload,
        MetadataEventRecord,
    };
    use aruna_core::plugin_runs::source_key;
    let (fixture, broker, _, _dir) = broker().await;
    let document = three_files(&fixture, &broker).await;
    let revision = revision_of(&broker, document).await;
    let typed = serde_json::json!({"@id": "./a.txt", "@type": "File", "name": "newer"});
    broker
        .metadata_put_file(&cx(), put(document, &typed, &revision, 0))
        .await
        .unwrap();
    project(&broker.ctx).await;
    let record = load_document_record(&broker.ctx, document)
        .await
        .unwrap()
        .unwrap();
    let source = MetadataBatchSource::UpsertDataEntity {
        jsonld: r#"{"@id":"./d.txt","@type":"File","name":"d"}"#.to_string(),
    };
    let handle = broker.ctx.metadata_handle.as_ref().unwrap();
    let planned = handle
        .send_metadata_effect(MetadataEffect::PlanBatch {
            graph_iri: record.graph_iri.clone(),
            actor: [9u8; 32],
            counter: 1,
            source: source.clone(),
        })
        .await;
    let Event::Metadata(MetadataEvent::BatchPlanned { batch, .. }) = planned else {
        panic!("expected a planned batch, got {planned:?}");
    };
    let older = record.last_event_id.timestamp_ms() - 1;
    let event = MetadataEventRecord {
        event_id: Ulid::from_parts(older, 5),
        record: record.clone(),
        user_id: fixture.actor.user_id,
        node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
        payload: MetadataEventPayload::ApplyBatch {
            batch,
            authored: source,
        },
        occurred_at_ms: older,
    };
    let local = Some(fixture.actor.node_id);
    project_create_event(&broker.ctx, event.clone(), local)
        .await
        .unwrap();
    let source = source_key(&format!("dataset_updated/{}", event.event_id));
    let txn = Txn::direct(&broker.ctx);
    let journaled = txn
        .read(aruna_core::keyspaces::EVENT_JOURNAL_KEYSPACE, &source)
        .await;
    assert!(journaled.unwrap().is_some());
}

/// Stores object `key` of a new bucket of the fixture's group with content digest `hash`, which
/// the location records only when `recorded` is set, as a pending encrypted upload leaves it out.
async fn stored_object(
    fixture: &Fixture,
    (bucket, key): (&str, &str),
    (hash, recorded): ([u8; 32], bool),
) {
    use aruna_core::keyspaces::{
        BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::storage::blob::{
        BackendLocation, BackendRef, BlobHeadKey, BlobLocationKey, BlobVersion, BucketInfo,
        CurrentVersionPointer, VersionKey,
    };
    use aruna_core::structs::storage::format::{Compression, EncodingClass, StoredFormat};
    let info = BucketInfo {
        group_id: fixture.group_id,
        created_at: std::time::UNIX_EPOCH,
        created_by: fixture.actor.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let location = BackendLocation {
        backend: BackendRef::node_default(),
        storage_class: None,
        root: "/unused".to_string(),
        storage_bucket: bucket.to_string(),
        backend_path: key.to_string(),
        ulid: Ulid::generate(),
        format: StoredFormat::default(),
        created_at: std::time::SystemTime::now(),
        created_by: fixture.actor.user_id,
        staging: false,
        partial: false,
        blob_size: 5,
        hashes: (recorded
            .then(|| (HASH_BLAKE3.to_string(), hash.to_vec()))
            .into_iter())
        .collect(),
    };
    let version_id = Ulid::generate();
    let version = BlobVersion::materialized(
        hash,
        BackendRef::node_default(),
        EncodingClass::Raw,
        location.created_at,
        location.created_by,
        None,
    );
    let located = BlobLocationKey::from_blake3(&hash, EncodingClass::Raw, location.backend.clone());
    let rows = [
        (
            S3_BUCKET_KEYSPACE,
            bucket.as_bytes().to_vec(),
            info.to_bytes().unwrap(),
        ),
        (
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new(bucket, key).to_bytes().unwrap(),
            CurrentVersionPointer::new(version_id).to_bytes().unwrap(),
        ),
        (
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new(bucket, key, version_id).to_bytes().unwrap(),
            version.to_bytes().unwrap(),
        ),
        (
            BLOB_LOCATIONS_KEYSPACE,
            located.unwrap().to_bytes(),
            location.to_bytes().unwrap(),
        ),
    ];
    let txn = Txn::direct(&fixture.ctx);
    for (space, key, value) in rows {
        txn.write(space, vec![(key.to_vec(), value)]).await.unwrap();
    }
}

#[tokio::test]
async fn hashed_files_rename() {
    // An imported file names its content by hash and its object by `s3://`: with the grant it can
    // be renamed and keeps its hash id; a hash alone or a hash the object does not match is refused.
    use aruna_core::structs::storage::data::identity::content_id;
    let (fixture, broker, _, _dir) = broker().await;
    let hash = [5u8; 32];
    stored_object(&fixture, ("imports", "run/a.txt"), (hash, true)).await;
    let id = content_id(hash);
    let document = files_named(&fixture, &broker, &[id.as_str()]).await;
    let reads = serde_json::json!({ "permissions": {
        "required": ["metadata.read", "metadata.write", "objects.read"] } });
    let mut request = fixture.request(None);
    request.version = Some(fixture.publish("0.1.5", "56", reads).await);
    let installed = fixture.install(&request).await.unwrap();
    let mut scope = broker.scope.clone();
    scope.run.install_id = installed.install_id;
    scope.run.digest = installed.digest.clone();
    let writer = RunBroker::new(broker.ctx.clone(), scope);
    let revision = revision_of(&writer, document).await;
    let located = |id: &str, name: &str| {
        serde_json::json!({"@id": id, "@type": "File", "name": name,
            "contentUrl": "s3://imports/run/a.txt"})
    };
    let renamed = located(&id, "renamed");
    let change = put(document, &renamed, &revision, 0);
    writer.metadata_put_file(&cx(), change).await.unwrap();
    project(&broker.ctx).await;
    let files = files_of(&writer, document).await;
    assert_eq!(files[0]["@id"], id.as_str());
    assert_eq!(files[0]["name"], "renamed");

    let revision = revision_of(&writer, document).await;
    let other = located(&content_id([6u8; 32]), "other");
    let refused = writer
        .metadata_put_file(&cx(), put(document, &other, &revision, 1))
        .await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::Invalid);
    let alone = serde_json::json!({"@id": id, "@type": "File", "name": "alone"});
    let refused = writer
        .metadata_put_file(&cx(), put(document, &alone, &revision, 2))
        .await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::Denied);
}

/// A broker of an install that reads metadata and objects and writes metadata, as `version`.
async fn writer_broker(
    fixture: &Fixture,
    broker: &RunBroker,
    (version, byte): (&str, &str),
) -> RunBroker {
    let reads = serde_json::json!({ "permissions": {
        "required": ["metadata.read", "metadata.write", "objects.read"] } });
    let mut request = fixture.request(None);
    request.version = Some(fixture.publish(version, byte, reads).await);
    let installed = fixture.install(&request).await.unwrap();
    let mut scope = broker.scope.clone();
    scope.run.install_id = installed.install_id;
    scope.run.digest = installed.digest.clone();
    RunBroker::new(broker.ctx.clone(), scope)
}

#[tokio::test]
async fn compact_alias_checked() {
    // A doubled prefix still resolves to `contentUrl` in the stored entity, so it is checked too.
    let (fixture, broker, _, _dir) = broker().await;
    let document = three_files(&fixture, &broker).await;
    let revision = revision_of(&broker, document).await;
    let file = serde_json::json!({"@id": "./alias.txt", "@type": "File", "name": "o",
        "schema:schema:contentUrl": "s3://private/key"});
    let refused = broker
        .metadata_put_file(&cx(), put(document, &file, &revision, 0))
        .await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::NotGranted);
}

#[tokio::test]
async fn unverified_hash_refused() {
    // An object without a recorded digest cannot confirm the hash a file claims for it.
    use aruna_core::structs::storage::data::identity::content_id;
    let (fixture, broker, _, _dir) = broker().await;
    stored_object(&fixture, ("pending", "a.bin"), ([5u8; 32], false)).await;
    let document = three_files(&fixture, &broker).await;
    let writer = writer_broker(&fixture, &broker, ("0.1.6", "57")).await;
    let revision = revision_of(&writer, document).await;
    let file = serde_json::json!({"@id": content_id([5u8; 32]), "@type": "File", "name": "p",
        "contentUrl": "s3://pending/a.bin"});
    let refused = writer
        .metadata_put_file(&cx(), put(document, &file, &revision, 0))
        .await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::Invalid);
}

#[tokio::test]
async fn second_hash_checked() {
    // A file naming two different content hashes is refused, even when the first one matches.
    use aruna_core::structs::storage::data::identity::content_id;
    let (fixture, broker, _, _dir) = broker().await;
    stored_object(&fixture, ("imports", "run/a.txt"), ([5u8; 32], true)).await;
    let document = three_files(&fixture, &broker).await;
    let writer = writer_broker(&fixture, &broker, ("0.1.7", "58")).await;
    let revision = revision_of(&writer, document).await;
    let file = serde_json::json!({"@id": content_id([5u8; 32]), "@type": "File", "name": "two",
        "contentUrl": ["s3://imports/run/a.txt", content_id([6u8; 32])]});
    let refused = writer
        .metadata_put_file(&cx(), put(document, &file, &revision, 0))
        .await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::Invalid);
}

/// Creates a crate whose context adds `term`, with one file; returns the document and revision.
async fn crate_with_term(fixture: &Fixture, broker: &RunBroker, term: Value) -> (Ulid, String) {
    let jsonld = serde_json::json!({
        "@context": ["https://w3id.org/ro/crate/1.1/context", term],
        "@graph": [
            {"@id": "ro-crate-metadata.json", "@type": "CreativeWork", "about": {"@id": "./"},
             "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"}},
            {"@id": "./", "@type": "Dataset", "name": "imported", "description": "d",
             "datePublished": "2026-01-01", "hasPart": [{"@id": "a.txt"}]},
            {"@id": "a.txt", "@type": "File", "name": "a.txt"}
        ]
    });
    let create = CreateDocumentOperation::new(CreateDocumentConfig {
        actor: fixture.actor.clone(),
        group_id: fixture.group_id,
        document_id: Ulid::nil(),
        document_path: "/docs/imported/".to_string(),
        public: false,
        payload: CreateDocumentPayload::RoCrate {
            jsonld: jsonld.to_string(),
        },
    });
    let document = create_metadata_document(create, broker.ctx.clone())
        .await
        .unwrap();
    let document = document.record.document_id;
    project(&broker.ctx).await;
    (document, revision_of(broker, document).await)
}

/// A versioned ARN of a private object on the fixture's node.
fn private_arn(fixture: &Fixture) -> String {
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    let arn = aruna_core::structs::storage::replication::VersionedObjectArn::new(
        realm,
        node,
        "private",
        "secret.bin",
        Ulid::generate(),
    );
    arn.unwrap().to_string()
}

#[tokio::test]
async fn context_reference_checked() {
    // A crate whose context types `contentUrl` as `@id` renders a referenced ARN without the `./`
    // the converter adds, so that rendered form is checked before any write.
    let (fixture, broker, _, _dir) = broker().await;
    let term =
        serde_json::json!({"contentUrl": {"@id": "http://schema.org/contentUrl", "@type": "@id"}});
    let (document, revision) = crate_with_term(&fixture, &broker, term).await;
    let file = serde_json::json!({"@id": "./b.txt", "@type": "File", "name": "b",
        "contentUrl": {"@id": private_arn(&fixture)}});
    let refused = broker
        .metadata_put_file(&cx(), put(document, &file, &revision, 0))
        .await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::NotGranted);
}

#[tokio::test]
async fn remapped_term_checked() {
    // A context may name any predicate `contentUrl`; the rendered value is checked whatever the
    // predicate, so a reference under another IRI is refused as well.
    let (fixture, broker, _, _dir) = broker().await;
    let term =
        serde_json::json!({"contentUrl": {"@id": "https://example.org/location", "@type": "@id"}});
    let (document, revision) = crate_with_term(&fixture, &broker, term).await;
    let file = serde_json::json!({"@id": "./b.txt", "@type": "File", "name": "b",
        "https://example.org/location": {"@id": private_arn(&fixture)}});
    let refused = broker
        .metadata_put_file(&cx(), put(document, &file, &revision, 0))
        .await;
    assert_eq!(refused.unwrap_err().kind, ErrorKind::NotGranted);
}

/// Puts `file` into a crate whose context adds `term`, as a plugin without `objects.read`.
async fn put_under(term: Value, file: impl FnOnce(&Fixture) -> Value) -> HostError {
    let (fixture, broker, _, _dir) = broker().await;
    let (document, revision) = crate_with_term(&fixture, &broker, term).await;
    let file = file(&fixture);
    let refused = broker
        .metadata_put_file(&cx(), put(document, &file, &revision, 0))
        .await;
    refused.unwrap_err()
}

fn typed_term() -> Value {
    serde_json::json!({"contentUrl": {"@id": "http://schema.org/contentUrl", "@type": "@id"}})
}

#[tokio::test]
async fn prefixed_location_checked() {
    // A prefix of the stored context resolves `place:` to a private location on export; a stray
    // `id` the converter ignores changes nothing.
    let mut term = typed_term();
    term["place"] = "s3://private/".into();
    let refused = put_under(term, |_| {
        serde_json::json!({"@id": "./b.txt", "@type": "File", "name": "b",
            "contentUrl": "place:secret.bin", "id": "s3://private/secret.bin"})
    })
    .await;
    assert_eq!(refused.kind, ErrorKind::NotGranted, "{}", refused.message);
}

#[tokio::test]
async fn relative_reference_checked() {
    // Export resolves a protocol-relative value against its base into the private object's W3ID.
    let refused = put_under(typed_term(), |fixture| {
        let url = format!("//w3id.org/aruna/data/{}", private_arn(fixture));
        serde_json::json!({"@id": "./b.txt", "@type": "File", "name": "b", "contentUrl": url})
    })
    .await;
    assert_eq!(refused.kind, ErrorKind::NotGranted, "{}", refused.message);
}

#[tokio::test]
async fn compact_reference_checked() {
    // A context prefix expands `aruna:` into the private object's W3ID on export.
    let mut term = typed_term();
    term["aruna"] = "https://w3id.org/aruna/".into();
    let refused = put_under(term, |fixture| {
        let url = format!("aruna:data/{}", private_arn(fixture));
        serde_json::json!({"@id": "./b.txt", "@type": "File", "name": "b", "contentUrl": url})
    })
    .await;
    assert_eq!(refused.kind, ErrorKind::NotGranted, "{}", refused.message);
}

#[tokio::test]
async fn term_literal_passes() {
    // A stored term may name a private location; a plain name that spells the term stays text,
    // so a file whose only object is a granted one is written.
    let (fixture, broker, _, _dir) = broker().await;
    stored_object(&fixture, ("imports", "run/a.txt"), ([5u8; 32], true)).await;
    let term = serde_json::json!({"sample": "s3://private/secret.bin"});
    let (document, revision) = crate_with_term(&fixture, &broker, term).await;
    let writer = writer_broker(&fixture, &broker, ("0.1.8", "59")).await;
    let file = serde_json::json!({"@id": "./b.txt", "@type": "File", "name": "sample",
        "contentUrl": "s3://imports/run/a.txt"});
    let written = writer
        .metadata_put_file(&cx(), put(document, &file, &revision, 0))
        .await;
    assert!(written.is_ok(), "{written:?}");
}

#[tokio::test]
async fn context_fence_refuses() {
    // A change checked under another context than the holder's raw revision has is refused as a
    // revision conflict, without an effect; under the stored context it is written.
    use crate::jobs::export::stored_crate;
    let (fixture, broker, _, _dir) = broker().await;
    let document = three_files(&fixture, &broker).await;
    let revision = Ulid::from_string(&revision_of(&broker, document).await).unwrap();
    let (_, digest) = stored_crate(&broker.ctx, &fixture.auth(), document)
        .await
        .unwrap();
    let record = crate::metadata::get_document::load_document_record(&broker.ctx, document).await;
    let record = record.unwrap().unwrap();
    let update = |context_digest| {
        let mutation = UpdateDocumentMutation::ReplaceDataEntity {
            jsonld: r#"{"@id":"./a.txt","@type":"File","name":"fenced"}"#.to_string(),
            context_digest,
        };
        let token = Some(AuthToken::Internal(fixture.auth()));
        let (ctx, actor) = (&broker.ctx, fixture.actor.clone());
        route_metadata_update(
            ctx,
            actor,
            Some(&record),
            document,
            None,
            mutation,
            Some(revision),
            token,
            None,
        )
    };
    let refused = update(Some([0u8; 32])).await;
    let conflict = matches!(
        refused,
        Err(MetadataWriteError::Update(
            UpdateDocumentError::RevisionConflict { .. }
        ))
    );
    assert!(conflict, "{refused:?}");
    assert!(update(Some(digest)).await.is_ok());
}

#[tokio::test]
async fn sync_style_writes() {
    // The writes a file sync makes pass with the grant: an `s3://` or object path `contentUrl`,
    // schema.org properties and plain literals.
    let (fixture, broker, _, _dir) = broker().await;
    stored_object(&fixture, ("imports", "run/a.txt"), ([5u8; 32], true)).await;
    let document = three_files(&fixture, &broker).await;
    let writer = writer_broker(&fixture, &broker, ("0.1.9", "5a")).await;
    let (realm, group, node) = (
        fixture.actor.realm_id,
        fixture.group_id,
        fixture.actor.node_id,
    );
    let path = format!("/{realm}/g/{group}/data/{node}/imports/run/a.txt");
    for (n, url) in ["s3://imports/run/a.txt".to_string(), path]
        .into_iter()
        .enumerate()
    {
        let revision = revision_of(&writer, document).await;
        let file = serde_json::json!({"@id": format!("./exp/{n}.txt"), "@type": "File",
            "name": "a.txt", "contentUrl": url, "contentSize": "5", "encodingFormat": "text/plain",
            "sha256": "abc", "dateModified": "2026-11-12", "description": "uploaded in eLabFTW"});
        let written = writer
            .metadata_put_file(&cx(), put(document, &file, &revision, n as u32))
            .await;
        assert!(written.is_ok(), "{written:?}");
        project(&broker.ctx).await;
    }
}
