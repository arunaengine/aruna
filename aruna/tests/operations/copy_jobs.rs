//! Tests copy jobs that pull a referenced object, run under the node's job host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna::tasks::job_host::run_copy_job;
use aruna_core::UserId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::id::NodeId;
use aruna_core::structs::execution::job::{CopyJobSpec, JobResultPayload};
use aruna_core::structs::execution::job::{JobErrorKind, JobId, JobProgress};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::execution::staging::{
    PortableSourceDescriptor, StagingStrategy, VersionSourceBinding,
};
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::BlobVersion;
use aruna_core::types::GroupId;
use aruna_operations::driver::{DriverContext, drive, node_routing};
use aruna_operations::jobs::context::JobContext;
use aruna_operations::jobs::copy::*;
use aruna_operations::jobs::executor::JobRunOutcome;
use aruna_operations::jobs::executor::ProgressReporter;
use aruna_operations::s3::object::get::{GetObjectInput, GetObjectOperation};
use aruna_operations::testing_objects::seed_bucket;
use aruna_operations::tests::s3::{full_context, spawn_reference_server, write_version};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

const BODY: &[u8] = b"reference-bytes";

struct Fixture {
    realm_id: RealmId,
    group_id: GroupId,
    node_id: NodeId,
    user_id: UserId,
    server: tokio::task::JoinHandle<()>,
}

/// A realm with one bucket holding a reference to a small HTTP source.
async fn fixture(context: &DriverContext) -> Fixture {
    let realm_id = RealmId::from_bytes([5u8; 32]);
    let group_id = Ulid::generate();
    let node_id = context.net_handle.as_ref().unwrap().node_id();
    let user_id = UserId::local(Ulid::generate(), realm_id);
    let target = DocumentTarget::RealmConfig { realm_id };
    let actor = Actor {
        node_id,
        user_id: UserId::nil(realm_id),
        realm_id,
    };
    let config = RealmConfigDocument::default_for_realm(realm_id, Vec::new())
        .to_bytes(&actor)
        .unwrap();
    let _ = context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            value: config.into(),
            txn_id: None,
        })
        .await;
    seed_bucket(context, "bucket", group_id, user_id, Vec::new()).await;
    let (endpoint, server) = spawn_reference_server(BODY).await;
    let source = VersionSourceBinding {
        strategy: StagingStrategy::Reference,
        descriptor: PortableSourceDescriptor {
            kind: SourceConnectorKind::Http,
            public_config: HashMap::from([("endpoint".to_string(), endpoint)]),
            source_path: "folder/file.txt".to_string(),
            version_selector: None,
            capabilities: Vec::new(),
            origin_node_id: None,
            namespace: None,
            root: None,
        },
        connector_id: Some(Ulid::generate()),
        mirrors: Vec::new(),
    };
    let version = BlobVersion::reference(
        source,
        SourceMetadata {
            content_length: BODY.len() as u64,
            content_type: Some("text/plain".to_string()),
            etag: Some("etag-1".to_string()),
            last_modified: Some(SystemTime::UNIX_EPOCH),
            source_version: None,
            digests: Default::default(),
        },
        SystemTime::UNIX_EPOCH,
        user_id,
        SystemTime::UNIX_EPOCH,
    );
    write_version(context, "bucket", "ref.txt", version).await;
    Fixture {
        realm_id,
        group_id,
        node_id,
        user_id,
        server,
    }
}

fn job_context(context: DriverContext, node_id: NodeId) -> JobContext {
    JobContext {
        driver: Arc::new(context),
        job_id: JobId::from_bytes([7u8; 16]),
        owner_node_id: node_id,
        claim_token: Ulid::generate(),
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&JobProgress::new("bytes")),
        range_turns: Default::default(),
        handlers: Default::default(),
    }
}

fn spec(fixture: &Fixture, source_bucket: &str) -> CopyJobSpec {
    CopyJobSpec {
        auth_context: AuthContext {
            user_id: fixture.user_id,
            realm_id: fixture.realm_id,
            path_restrictions: None,
            session: None,
        },
        node_id: fixture.node_id,
        source_bucket: source_bucket.to_string(),
        source_key: "ref.txt".to_string(),
        source_version_id: None,
        source_group_id: fixture.group_id,
        dest_bucket: "bucket".to_string(),
        dest_key: "dest.txt".to_string(),
        group_id: fixture.group_id,
    }
}

#[tokio::test]
async fn pulls_reference_bytes() {
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context).await;
    let ctx = job_context(context, fixture.node_id);

    let outcome = run_copy_job(&ctx, &spec(&fixture, "bucket")).await;
    fixture.server.abort();
    let (bytes, blake3, version_id) = match outcome {
        JobRunOutcome::Succeeded(JobResultPayload::CopyObject {
            bytes,
            blake3,
            version_id,
        }) => (bytes, blake3, version_id),
        _ => panic!("the copy job succeeds"),
    };
    assert_eq!(bytes, BODY.len() as u64);
    assert_eq!(blake3.len(), 64);
    assert!(!version_id.is_empty(), "the source version is recorded");
    let progress = ctx.progress.snapshot();
    assert_eq!((progress.current, progress.total), (bytes, Some(bytes)));

    // The source server is gone, so the bytes must be local now.
    let mut blob = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: "bucket".to_string(),
            key: "dest.txt".to_string(),
            version_id: None,
            range: None,
            group_id: fixture.group_id,
            user_identity: fixture.user_id,
            realm_id: fixture.user_id.realm_id,
            node_id: fixture.node_id,
        }),
        &ctx.driver,
    )
    .await
    .unwrap()
    .blob;
    let mut read = Vec::new();
    while let Some(chunk) = blob.next().await {
        read.extend_from_slice(&chunk.unwrap());
    }
    assert_eq!(read, BODY);
}

#[tokio::test]
async fn missing_bucket_fails() {
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context).await;
    let ctx = job_context(context, fixture.node_id);

    let outcome = run_copy_job(&ctx, &spec(&fixture, "absent")).await;
    fixture.server.abort();
    match outcome {
        JobRunOutcome::Failed(error) => assert_eq!(error.kind, JobErrorKind::Permanent),
        _ => panic!("a missing bucket is a final failure"),
    }
}

#[tokio::test]
async fn cancel_drops_copy() {
    let (_temp, context) = full_context().await;
    let fixture = fixture(&context).await;
    let ctx = job_context(context, fixture.node_id);
    ctx.cancel.cancel();

    let outcome = run_copy_job(&ctx, &spec(&fixture, "bucket")).await;
    fixture.server.abort();
    assert!(matches!(outcome, JobRunOutcome::Cancelled));
    let missing = drive(
        GetObjectOperation::new(GetObjectInput {
            bucket: "bucket".to_string(),
            key: "dest.txt".to_string(),
            version_id: None,
            range: None,
            group_id: fixture.group_id,
            user_identity: fixture.user_id,
            realm_id: fixture.user_id.realm_id,
            node_id: fixture.node_id,
        }),
        &ctx.driver,
    )
    .await;
    assert!(missing.is_err(), "nothing landed under the destination key");
}
