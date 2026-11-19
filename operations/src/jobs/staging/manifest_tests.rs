//! Tests of the manifest chunk reader.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::driver::{DriverContext, drive};
use crate::jobs::staging::pure_tests::{child_answer, test_run};
use aruna_core::effects::StorageEffect;
use aruna_core::errors::StorageError;
use aruna_core::structs::execution::job::{
    JobClaim, JobId, JobPayload, JobRecord, StagingJobPhase, StagingJobPrefix, StagingJobSpec,
};
use aruna_core::structs::execution::staging::StagingStrategy;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use std::sync::Arc;
use ulid::Ulid;

fn spec() -> StagingJobSpec {
    let realm_id = RealmId::from_bytes([1; 32]);
    StagingJobSpec {
        auth_context: AuthContext {
            user_id: aruna_core::UserId::local(Ulid::from_bytes([2; 16]), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        },
        group_id: Ulid::from_bytes([3; 16]),
        node_id: iroh::SecretKey::from_bytes(&[4; 32]).public(),
        connector_id: Ulid::from_bytes([5; 16]),
        bucket: "bucket".to_string(),
        bucket_identity: (
            Ulid::nil(),
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
        ),
        strategy: StagingStrategy::Reference,
        items: Vec::new(),
        prefixes: vec![StagingJobPrefix {
            source_prefix: "data".to_string(),
            target_prefix: String::new(),
        }],
        checksum_manifest: Some("MD5SUMS".to_string()),
        table: None,
        mirrors: Vec::new(),
    }
}

fn checkpoint() -> StagingJobCheckpoint {
    StagingJobCheckpoint {
        phase: StagingJobPhase::Discovering,
        flat: Some(true),
        items_done: 0,
        prefixes_done: 1,
        walk: None,
        manifest: Some(ManifestRead {
            source_path: "data/MD5SUMS".to_string(),
            offset: 4096,
            observation: None,
        }),
        table: None,
        queue_head: 0,
        queue_tail: 0,
        items_current: 0,
        items_succeeded: 0,
        items_failed: 0,
        items_total: None,
        bytes_current: 0,
        bytes_total: None,
        bytes_discovered: 0,
        current_path: None,
        errors: Vec::new(),
    }
}

// Transient failures keep the manifest for a retry; only lasting ones skip it.
#[test]
fn transient_errors_retry() {
    let transient = || {
        [
            StagingSourceError::ReadError("reset".to_string()),
            StagingSourceError::HandleMissing,
            StagingSourceError::StatError("timeout".to_string()),
        ]
    };
    assert!(transient().into_iter().all(|error| !lasting_source(&error)));
    for error in transient() {
        assert!(!lasting_read(&ReadSourceError::Staging(error)));
    }
    for error in transient() {
        assert!(!lasting_head(&HeadSourceError::Staging(error)));
    }
    assert!(lasting_read(&ReadSourceError::Staging(
        StagingSourceError::NotFound
    )));
    assert!(lasting_head(&HeadSourceError::Staging(
        StagingSourceError::AccessDenied
    )));
    for storage in [StorageError::Timeout, StorageError::QueueFull] {
        let resolution = SourceResolutionError::StorageError(storage);
        assert!(!lasting_read(&ReadSourceError::Resolve(resolution)));
    }
    assert!(lasting_head(&HeadSourceError::Resolve(
        SourceResolutionError::NotFound
    )));
}

// A resumed manifest read is authorized again on its own path before anything is read.
#[tokio::test]
async fn manifest_read_authorized() {
    let tempdir = tempfile::tempdir().unwrap();
    let storage_handle =
        aruna_storage::storage::FjallStorage::open(tempdir.path().to_str().unwrap()).unwrap();
    let context = crate::testing::test_context(storage_handle.clone());
    let node_id = iroh::SecretKey::from_bytes(&[4; 32]).public();
    let claim_token = Ulid::from_bytes([6; 16]);
    let job_id = JobId::from_bytes([7; 16]);
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Staging(spec()),
        Default::default(),
        node_id,
        1,
        1,
        None,
    );
    record.claim = Some(JobClaim {
        holder_node_id: node_id,
        claim_token,
        lease_expires_ms: u64::MAX,
    });
    crate::jobs::store::test_calls::insert_job(&context, &record)
        .await
        .unwrap();
    let driver = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    // The submitter may read the source, except the manifest's own path.
    let mut spec = spec();
    let denied = source_permission_path(&spec, "data/MD5SUMS");
    let scope = (spec.auth_context.realm_id, spec.group_id, spec.node_id);
    spec.auth_context = crate::testing_access::seed_access(&driver, scope, &denied).await;
    let checkpoint = checkpoint();
    // Digests an earlier chunk stored, and one of another folder's manifest.
    let digest = postcard::to_allocvec(&(ChecksumAlgorithm::Md5, vec![1u8; 16])).unwrap();
    let writes = ["data/a.txt", "other/b.txt"]
        .map(|path| {
            let key = manifest_key(job_id, path);
            (
                STAGING_MANIFEST_KEYSPACE.to_string(),
                key,
                ByteView::from(digest.clone()),
            )
        })
        .to_vec();
    let write = StorageEffect::BatchWrite {
        writes,
        txn_id: None,
    };
    driver.storage_handle.send_storage_effect(write).await;

    let run = StagingRun {
        spec: Arc::new(spec.clone()),
        job_id,
        claim_token,
        routing: Default::default(),
        seed: 1,
    };
    let chunk = ManifestChunkOperation::new(run, &checkpoint);
    let checkpoint = match drive(chunk, &driver).await {
        Ok(checkpoint) => checkpoint,
        Err(failure) => panic!("the manifest step failed: {failure:?}"),
    };

    assert_eq!(checkpoint.manifest, None);
    assert_eq!(checkpoint.items_failed, 1);
    assert_eq!(checkpoint.errors[0].error, "checksum manifest read denied");
    // The skipped manifest leaves no partial digests behind.
    let paths = ["data/a.txt", "other/b.txt"];
    let IoCall::Read(reads) = digest_reads(job_id, &paths) else {
        panic!("expected the digest reads");
    };
    let read = StorageEffect::BatchRead {
        reads,
        txn_id: None,
    };
    let event = driver.storage_handle.send_storage_effect(read).await;
    let Event::Storage(aruna_core::events::StorageEvent::BatchReadResult { values }) = event else {
        panic!("unexpected read answer");
    };
    let values = values.into_iter().map(|(_, value)| value).collect();
    let digests = parse_digests(IoResult::Values(values)).unwrap();
    assert_eq!(digests[0], None);
    assert!(digests[1].is_some());
}

/// The reader with no observation stored yet, past its first answer.
fn allowed_reader(offset: u64) -> ManifestChunkOperation {
    let mut checkpoint = checkpoint();
    if let Some(manifest) = checkpoint.manifest.as_mut() {
        manifest.offset = offset;
    }
    let mut chunk = ManifestChunkOperation::new(test_run(), &checkpoint);
    assert!(matches!(chunk.first(), Ok(Next::Call(IoCall::Child(_)))));
    chunk
}

// A denied manifest is skipped at its start without any read of the source.
#[test]
fn denied_manifest_skipped() {
    let mut chunk = allowed_reader(0);
    let denied = child_answer(Ok::<bool, AuthorizationError>(false));
    assert!(matches!(
        chunk.next(denied),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(chunk.step, ChunkStep::Commit);
    assert_eq!(chunk.next.items_failed, 1);
    assert_eq!(chunk.next.manifest, None);
}

// A manifest failing after earlier chunks deletes their digests before it is skipped.
#[test]
fn failed_manifest_cleared() {
    let mut chunk = allowed_reader(4096);
    let denied = child_answer(Ok::<bool, AuthorizationError>(false));
    assert!(matches!(
        chunk.next(denied),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(chunk.step, ChunkStep::Clear);
    let cleared = child_answer(Ok::<(), IoError>(()));
    assert!(matches!(
        chunk.next(cleared),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert_eq!(chunk.step, ChunkStep::Commit);
}

// A line longer than a chunk fails the manifest; complete lines are stored with their offset.
#[test]
fn chunk_needs_newline() {
    let observation = SourceMetadata {
        content_length: MANIFEST_CHUNK * 2,
        content_type: None,
        etag: None,
        last_modified: None,
        source_version: None,
        digests: Default::default(),
    };
    let mut chunk = allowed_reader(0);
    chunk.observation = Some(observation.clone());
    chunk.end = MANIFEST_CHUNK;
    assert!(chunk.collected(b"no newline here".to_vec()).is_ok());
    assert_eq!(chunk.next.items_failed, 1);

    let mut chunk = allowed_reader(0);
    chunk.observation = Some(observation);
    chunk.end = MANIFEST_CHUNK;
    let complete = format!("{}  a.txt", "0".repeat(32));
    let (algorithm, digest, _) = parse_manifest_line(&complete).unwrap();
    let turn = chunk.collected(format!("{complete}\nrest").into_bytes());
    let row = (
        STAGING_MANIFEST_KEYSPACE.to_string(),
        manifest_key(chunk.run.job_id, "data/a.txt"),
        ByteView::from(postcard::to_allocvec(&(algorithm, digest)).unwrap()),
    );
    let commit = chunk
        .run
        .commit_call(None, &chunk.next, vec![row], Vec::new());
    assert_eq!(turn, Ok(Next::Call(commit.unwrap())));
    // The next chunk starts after the stored line and its newline: 32 + 2 + 5 + 1 bytes.
    let next = chunk.next.manifest.as_ref().expect("the manifest goes on");
    assert_eq!(next.offset, 40);
}
