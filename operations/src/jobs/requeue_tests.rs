//! Tests that a payload that panicked on its last attempt exhausts its job without losing the
//! upload, the spooled input or the checkpoint it may resume from.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::UserId;
use aruna_core::effects::StorageEffect;
use aruna_core::events::Event;
use aruna_core::handle::Handle;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::JOB_STATE_KEYSPACE;
use aruna_core::structs::execution::job::{
    ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec, ImportRoCrateTarget, JobClaim,
    JobError, JobErrorKind, JobId, JobPayload, JobRecord, JobState, RoCrateLimits,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::time::unix_timestamp_millis;
use byteview::ByteView;
use ulid::Ulid;

use crate::jobs::JOB_MAX_ATTEMPTS;
use crate::jobs::store::job_calls::RequeueOutcome;
use crate::jobs::store::test_calls::{insert_job, list_job_entries, requeue_job};

fn node_id(seed: u8) -> NodeId {
    let mut seed_bytes = [0u8; 32];
    seed_bytes[0] = seed;
    iroh::SecretKey::from_bytes(&seed_bytes).public()
}

fn import_record(job_id: JobId) -> JobRecord {
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    JobRecord::new(
        job_id,
        JobPayload::ImportRoCrate(ImportRoCrateSpec {
            auth_context: AuthContext {
                user_id: owner,
                realm_id: RealmId([1u8; 32]),
                path_restrictions: None,
                session: None,
            },
            source: ImportRoCrateSource::Upload {
                upload_id: Ulid::from_bytes([3u8; 16]),
            },
            target: ImportRoCrateTarget {
                bucket: "target".to_string(),
                prefix: "crate".to_string(),
            },
            metadata: ImportMetadataTarget {
                group_id: Ulid::from_bytes([4u8; 16]),
                path: "crate".to_string(),
                public: false,
            },
            limits: RoCrateLimits::default(),
            document_id: Ulid::from_bytes([5u8; 16]),
        }),
        owner,
        node_id(7),
        1,
        1,
        None,
    )
}

#[tokio::test]
async fn panicked_import_kept() {
    use aruna_core::effects::BlobEffect;
    use aruna_core::events::BlobEvent;
    use aruna_core::stream::BackendStream;

    let fixture = crate::tests::staging::setup_driver_context().await;
    let driver = Arc::new(fixture.driver_context);
    let storage = driver.storage_handle.clone();
    let job_id = JobId::from_bytes([0x4B; 16]);
    let token = Ulid::generate();
    let mut record = import_record(job_id);
    record.state = JobState::Running;
    record.attempts = JOB_MAX_ATTEMPTS - 1;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: token,
        lease_expires_ms: 60_000,
    });
    insert_job(&driver, &record).await.unwrap();

    let Event::Blob(BlobEvent::HiddenSpooled {
        location,
        blake3,
        size,
    }) = driver
        .blob_handle
        .as_ref()
        .unwrap()
        .send_blob_effect(BlobEffect::SpoolHidden {
            namespace: job_id.as_ulid(),
            name: "input".to_string(),
            created_by: record.created_by,
            max_bytes: Some(1024),
            deadline: None,
            blob: BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
                b"archive bytes".to_vec(),
            ))),
        })
        .await
    else {
        panic!("input spool failed")
    };
    let blob_path = location.get_full_path().unwrap();
    let blob_len = std::fs::metadata(&blob_path).unwrap().len();
    let mut upload = crate::jobs::import::upload::tests::upload_record(
        record.created_by,
        Ulid::from_bytes([3u8; 16]),
        0,
    );
    upload.location = location.clone();
    upload.blake3 = blake3;
    upload.size = size;
    upload.claimed_by = Some(job_id);
    crate::driver::drive(
        crate::jobs::import::upload_store::WriteUploadOperation::new(upload.clone()),
        &driver,
    )
    .await
    .map_err(|error| error.0)
    .unwrap();
    let checkpoint_value =
        crate::jobs::import::tests::checkpoint_bytes(location, size, blake3, upload.upload_id);
    let checkpoint_key = ByteView::from(job_id.to_bytes().to_vec());
    let Event::Storage(aruna_core::events::StorageEvent::WriteResult { .. }) = storage
        .send_storage_effect(StorageEffect::Write {
            key_space: JOB_STATE_KEYSPACE.to_string(),
            key: checkpoint_key.clone(),
            value: ByteView::from(checkpoint_value.clone()),
            txn_id: None,
        })
        .await
    else {
        panic!("checkpoint seed failed")
    };

    // The error the job runner fails a panicking payload with.
    let error = JobError::retryable("job payload panicked");
    assert_eq!(error.kind, JobErrorKind::Retryable);
    let outcome = requeue_job(
        &driver,
        job_id,
        Some(token),
        unix_timestamp_millis(),
        None,
        Some(error),
    )
    .await
    .unwrap();
    let RequeueOutcome::Exhausted(exhausted) = outcome else {
        panic!("the last attempt must exhaust the job");
    };
    assert_eq!(exhausted.state, JobState::Indeterminate);
    assert!(exhausted.locally_exhausted);

    let (rows, _) = list_job_entries(&driver, job_id, None, 10).await.unwrap();
    assert!(rows.is_empty(), "panic must not write a failure report");
    assert_eq!(
        crate::driver::drive(
            crate::jobs::import::upload_store::ReadUploadOperation::new(upload.upload_id),
            &driver
        )
        .await
        .map_err(|error| error.0)
        .unwrap(),
        Some(upload)
    );
    assert_eq!(std::fs::metadata(&blob_path).unwrap().len(), blob_len);
    let Event::Storage(aruna_core::events::StorageEvent::ReadResult {
        value: Some(checkpoint),
        ..
    }) = storage
        .send_storage_effect(StorageEffect::Read {
            key_space: JOB_STATE_KEYSPACE.to_string(),
            key: checkpoint_key,
            txn_id: None,
        })
        .await
    else {
        panic!("checkpoint must survive");
    };
    assert_eq!(checkpoint.as_ref(), checkpoint_value.as_slice());
}
