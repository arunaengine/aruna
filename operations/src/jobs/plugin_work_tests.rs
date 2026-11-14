//! Tests plugin work attempts: a missing or rebound work ends, a paused install or locked source
//! waits, a narrowed grant refuses before any byte moves, and a copy writes its version once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::{JobErrorKind, JobId, JobProgress};
use serde_json::json;
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use super::*;
use crate::driver::DriverContext;
use crate::jobs::executor::ProgressReporter;
use crate::plugins::bound::tests::edit_config;
use crate::plugins::install::tests::Fixture;
use crate::plugins::work::{Identity, WorkKind, WorkRow, reserve};

pub(crate) fn job_context(ctx: &DriverContext, fixture: &Fixture) -> JobContext {
    JobContext {
        driver: Arc::new(ctx.clone()),
        job_id: JobId::from_bytes([7u8; 16]),
        owner_node_id: fixture.actor.node_id,
        claim_token: Ulid::generate(),
        final_attempt: false,
        cancel: CancellationToken::new(),
        shutdown: CancellationToken::new(),
        progress: ProgressReporter::from_progress(&JobProgress::new("bytes")),
        range_turns: Default::default(),
        mail: Default::default(),
    }
}

/// An active install of a version that requests `objects.copy`, granted on the whole group.
pub(crate) async fn copier(fixture: &Fixture) -> PluginInstall {
    let copy = json!({ "permissions": { "required": ["objects.copy"] } });
    let digest = fixture.publish("0.2.0", "cd", copy).await;
    let mut request = fixture.request(None);
    request.version = Some(digest);
    fixture.install(&request).await.unwrap()
}

/// A recorded copy of `install` between two object paths of its group on this node.
pub(crate) async fn recorded(
    fixture: &Fixture,
    install: &PluginInstall,
    (source, target): (&str, &str),
) -> PluginWorkSpec {
    let mut row = WorkRow::reserved(install, WorkKind::Copy, Ulid::generate(), 1);
    row.version_id = Some(Ulid::generate());
    let identity = Identity::Call {
        run_id: row.run_id,
        ordinal: 0,
    };
    let (row, _) = reserve(&fixture.ctx, (install, &identity), row)
        .await
        .unwrap();
    let data = format!("/{}/g/{}/data", fixture.actor.realm_id, install.group_id);
    let node = fixture.actor.node_id;
    PluginWorkSpec {
        realm_id: fixture.actor.realm_id,
        install_id: install.install_id,
        work_id: row.work_id,
        group_id: install.group_id,
        action: PluginWorkAction::Copy {
            source: format!("{data}/{node}/{source}"),
            source_version: None,
            target: format!("{data}/{node}/{target}"),
            version_id: row.version_id.unwrap(),
        },
    }
}

fn failure(outcome: JobRunOutcome) -> (JobErrorKind, String) {
    match outcome {
        JobRunOutcome::Failed(error) => (error.kind, error.message),
        other => panic!("expected a failure, got {}", outcome_name(&other)),
    }
}

fn outcome_name(outcome: &JobRunOutcome) -> &'static str {
    match outcome {
        JobRunOutcome::Succeeded(_) => "succeeded",
        JobRunOutcome::Failed(_) => "failed",
        JobRunOutcome::Deferred(_) => "deferred",
        JobRunOutcome::DeferredFor(..) => "deferred for",
        JobRunOutcome::Cancelled => "cancelled",
        JobRunOutcome::Interrupted => "interrupted",
        JobRunOutcome::AwaitingKey(_) => "awaiting key",
        JobRunOutcome::Undecided(_) => "undecided",
    }
}

#[tokio::test]
async fn refuses_changed_work() {
    let fixture = Fixture::new().await;
    let install = copier(&fixture).await;
    let ctx = job_context(&fixture.ctx, &fixture);
    let spec = recorded(&fixture, &install, ("in/a", "out/a")).await;
    let mut unknown = spec.clone();
    unknown.work_id = Ulid::generate();
    let (kind, message) = failure(run_plugin_work(&ctx, &unknown).await);
    assert_eq!(kind, JobErrorKind::Permanent);
    assert!(message.contains("no longer recorded"), "{message}");
    // The install moved to another artifact after the work was recorded.
    edit_config(&fixture, |config| {
        config.installs[0].digest = "sha256:00".into()
    })
    .await;
    let (kind, message) = failure(run_plugin_work(&ctx, &spec).await);
    assert_eq!(kind, JobErrorKind::Permanent);
    assert!(message.contains("another version"), "{message}");
}

#[tokio::test]
async fn paused_install_waits() {
    // A real pause holds the work and a resume lets it run; neither fails it.
    use crate::plugins::lifecycle::{pause_install, resume_install};
    let fixture = Fixture::new().await;
    let (_root, blob, install, spec) = ready_copy(&fixture).await;
    let ctx = job_context(&blob, &fixture);
    let (actor, auth) = (&fixture.actor, fixture.auth());
    let paused = pause_install(&fixture.ctx, actor, &auth, install.install_id).await;
    assert_eq!(paused.unwrap().state, InstallState::Paused);
    let outcome = run_plugin_work(&ctx, &spec).await;
    assert!(
        matches!(outcome, JobRunOutcome::DeferredFor(_, wait) if wait == PAUSED_RETRY),
        "{}",
        outcome_name(&outcome)
    );
    resume_install(&fixture.ctx, actor, &auth, install.install_id)
        .await
        .unwrap();
    let outcome = run_plugin_work(&ctx, &spec).await;
    assert!(copied(&outcome, &spec), "{}", outcome_name(&outcome));
}

#[tokio::test]
async fn narrowed_grant_refuses() {
    let fixture = Fixture::new().await;
    let install = copier(&fixture).await;
    let ctx = job_context(&fixture.ctx, &fixture);
    let spec = recorded(&fixture, &install, ("in/a", "out/a")).await;
    let PluginWorkAction::Copy { source, .. } = &spec.action;
    let only_source = vec![source.clone()];
    edit_config(&fixture, |config| {
        let grants = &mut config.installs[0].grants;
        grants.insert("objects.copy".into(), only_source);
    })
    .await;
    let (kind, message) = failure(run_plugin_work(&ctx, &spec).await);
    assert_eq!(kind, JobErrorKind::Permanent);
    assert!(message.contains("is not granted"), "{message}");
}

/// The install's objects `in/a.txt` (5 bytes) and the empty bucket `out` on a blob backend, and
/// a recorded copy between them.
async fn ready_copy(
    fixture: &Fixture,
) -> (
    tempfile::TempDir,
    DriverContext,
    PluginInstall,
    PluginWorkSpec,
) {
    use crate::driver::drive;
    use crate::s3::object::copy::test::{put_config, seed_bucket, with_blob};
    use crate::s3::object::put::PutObjectOperation;
    let install = copier(fixture).await;
    let root = tempfile::tempdir().unwrap();
    let blob = with_blob(&fixture.ctx, root.path()).await;
    for bucket in ["in", "out"] {
        seed_bucket(&blob, bucket, install.group_id, install.account, Vec::new()).await;
    }
    let (realm, node) = (fixture.actor.realm_id, fixture.actor.node_id);
    let source = put_config(realm, install.group_id, node, "in", "a.txt", b"bytes");
    drive(PutObjectOperation::new(source), &blob).await.unwrap();
    let spec = recorded(fixture, &install, ("in/a.txt", "out/a.txt")).await;
    (root, blob, install, spec)
}

/// Whether `outcome` copied the 5 bytes as the work's minted version.
fn copied(outcome: &JobRunOutcome, spec: &PluginWorkSpec) -> bool {
    let PluginWorkAction::Copy { version_id, .. } = &spec.action;
    matches!(
        outcome,
        JobRunOutcome::Succeeded(JobResultPayload::PluginWork { bytes: 5, version_id: got })
            if *got == Some(*version_id)
    )
}

#[tokio::test]
async fn copies_once() {
    // The copy runs as the account and writes the minted version; a rerun after a lost answer
    // replays that version instead of writing another one.
    let fixture = Fixture::new().await;
    let (_root, blob, _, spec) = ready_copy(&fixture).await;
    let ctx = job_context(&blob, &fixture);
    for attempt in 0..2 {
        let outcome = run_plugin_work(&ctx, &spec).await;
        assert!(
            copied(&outcome, &spec),
            "attempt {attempt}: {}",
            outcome_name(&outcome)
        );
    }
}

#[tokio::test]
async fn locked_source_waits() {
    // A locked source parks the work on its key; once unlocked, the same work copies.
    use aruna_core::compute::{SecretBytes, SharedSecret};
    use aruna_core::effects::{BlobEffect, StorageEffect};
    use aruna_core::events::{BlobEvent, Event};
    use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
    use aruna_core::structs::storage::encryption::{
        BucketEncryption, BucketKeyRef, EncryptionMode, public_key_of,
    };
    let fixture = Fixture::new().await;
    let (_root, blob, _, spec) = ready_copy(&fixture).await;
    let ctx = job_context(&blob, &fixture);
    let key = BucketKeyRef::new(Ulid::from_bytes([115; 16]), 1);
    let settings = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(key.bucket_id),
        key_generation: key.generation,
        ..Default::default()
    };
    let write = StorageEffect::Write {
        key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
        key: b"in".to_vec().into(),
        value: settings.to_bytes().unwrap().into(),
        txn_id: None,
    };
    blob.storage_handle.send_storage_effect(write).await;
    let outcome = run_plugin_work(&ctx, &spec).await;
    let JobRunOutcome::AwaitingKey(waits) = outcome else {
        panic!("a locked source must wait, got {}", outcome_name(&outcome));
    };
    assert_eq!(waits.len(), 1);
    assert_eq!((waits[0].key, waits[0].bucket.as_str()), (key, "in"));
    let private = || SecretBytes::new(vec![9; 32]);
    let prepare = BlobEffect::PrepareKey {
        key,
        public_key: public_key_of(&private()).unwrap(),
        private_key: SharedSecret::new(private()),
        duration: None,
        max: None,
    };
    let handle = blob.blob_handle.as_ref().unwrap();
    let Event::Blob(BlobEvent::KeyPrepared { ticket }) = handle.send_blob_effect(prepare).await
    else {
        panic!("prepare failed")
    };
    handle
        .send_blob_effect(BlobEffect::ActivateKey { ticket })
        .await;
    let outcome = run_plugin_work(&ctx, &spec).await;
    assert!(copied(&outcome, &spec), "{}", outcome_name(&outcome));
}
