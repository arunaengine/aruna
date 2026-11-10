//! Tests metadata calls through the broker: reads, listings, and a revision-checked patch
//! answered again from its intent, refused when stale, and failing the run when changed.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

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

const ENTITY: &str = r#"{"@id":"./file.txt","@type":"File","name":"file"}"#;

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
    let other = patch(
        document,
        &revision,
        0,
        r#"{"@id":"./b.txt","@type":"File"}"#,
    );
    let changed = broker.metadata_patch(&cx(), other).await.unwrap_err();
    assert_eq!(changed.kind, ErrorKind::Conflict);
    assert!(broker.changed());
}
