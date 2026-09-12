use super::*;
use aruna_core::structs::{
    AuthContext, ComputeResources, ExecutionSpec, FIRST_GRANTABLE_HANDLE, ImportMetadataTarget,
    ImportReportDetail, ImportReportRow, ImportRoCrateResult, ImportRoCrateSource,
    ImportRoCrateSpec, ImportRoCrateTarget, JOB_LEASE_INDEX_PREFIX, JobPayload,
    MintPersistentIdSpec, RealmId, ReasonCode, RoCrateLimits, parse_schedule_key, pid_dedup_key,
};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_core::types::UserId;
use aruna_storage::FjallStorage;
use tempfile::tempdir;

fn temp_storage() -> (tempfile::TempDir, StorageHandle) {
    let dir = tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
    (dir, storage)
}

fn node_id(seed: u8) -> NodeId {
    let mut seed_bytes = [0u8; 32];
    seed_bytes[0] = seed;
    iroh::SecretKey::from_bytes(&seed_bytes).public()
}

fn job_id(timestamp_ms: u64) -> JobId {
    JobId::from_parts(
        timestamp_ms,
        PlacementHandle::new(FIRST_GRANTABLE_HANDLE).unwrap(),
        BucketId::new(0).unwrap(),
        0,
    )
    .unwrap()
}

fn queued_record(job_id: JobId) -> JobRecord {
    JobRecord::new(
        job_id,
        JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        },
        UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
        node_id(7),
        1_000,
        1_000,
        None,
    )
}

fn rocrate_record(job_id: JobId) -> JobRecord {
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
        1_000,
        1_000,
        None,
    )
}

fn report_row(entry_key: &str) -> ImportReportRow {
    ImportReportRow {
        entry_key: entry_key.to_string(),
        code: ReasonCode::Imported,
        message: None,
        detail: ImportReportDetail {
            archive_path: entry_key.to_string(),
            target_key: Some(entry_key.to_string()),
            version_id: None,
            blake3: None,
            size: None,
            arn: None,
            w3id: None,
            validation: None,
        },
    }
}

fn digest_values(values: &[Vec<u8>]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for value in values {
        hash_report_value(&mut hasher, value);
    }
    *hasher.finalize().as_bytes()
}

proptest::proptest! {
    #[test]
    fn digest_is_stable(
        first in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
        second in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..64),
    ) {
        let framed = digest_values(&[first.clone(), second.clone()]);
        let mut concatenated = first;
        concatenated.extend_from_slice(&second);
        proptest::prop_assert_ne!(framed, digest_values(&[concatenated]));
    }
}

async fn schedule_keys(storage: &StorageHandle) -> Vec<Key> {
    let (values, _) = iter_prefix_page(storage, JOB_SCHEDULE_INDEX_KEYSPACE, None, None, 64, None)
        .await
        .expect("scan schedule index");
    values.into_iter().map(|(key, _)| key).collect()
}

// Stealing a claim whose lease is still live would put two supervisors on one
// container. Only a genuinely expired lease may be adopted.
#[tokio::test]
async fn adopt_spares_live() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0x77; 16]);
    let mut record = queued_record(job_id);
    record.execution_class = JobExecutionClass::ExternalAttempt;
    record.state = JobState::Running;
    let live_token = Ulid::generate();
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: live_token,
        lease_expires_at_ms: 10_000,
    });
    insert_job(&storage, &record).await.unwrap();
    record_attempt_intent(
        &storage,
        job_id,
        live_token,
        AttemptIntent {
            attempt_no: 0,
            external_name: aruna_core::structs::attempt_external_name(job_id, 0),
            executor_kind: "docker".to_string(),
            pinned_image:
                "alpine@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            attempt_epoch: 0,
        },
        None,
        1_000,
    )
    .await
    .unwrap();

    // The holder renewed: its lease outlives `now`, so adoption must skip.
    let outcome = adopt_external_attempt(&storage, job_id, node_id(4), 9_000)
        .await
        .unwrap();
    assert!(matches!(outcome, AdoptOutcome::Skipped));
    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.claim.unwrap().claim_token, live_token);

    // Once the lease really has expired, the attempt is adopted with a fresh token.
    let AdoptOutcome::Adopted(adopted, _) =
        adopt_external_attempt(&storage, job_id, node_id(4), 11_000)
            .await
            .unwrap()
    else {
        panic!("expired lease must be adopted");
    };
    let claim = adopted.claim.unwrap();
    assert_ne!(claim.claim_token, live_token);
    assert_eq!(claim.holder_node_id, node_id(4));
}

#[test]
fn prune_covers_controls() {
    // Every attempt epoch handed out must have its control row pruned.
    let mut record = queued_record(JobId::from_bytes([0x55; 16]));
    record.next_attempt_epoch = 3;

    let deletes = prune_delete_entries(&record);

    let controls: Vec<_> = deletes
        .iter()
        .filter(|(space, _)| space == JOB_ATTEMPT_CONTROL_KEYSPACE)
        .map(|(_, key)| key.as_ref().to_vec())
        .collect();
    assert_eq!(
        controls,
        vec![
            attempt_control_key(record.job_id, 1),
            attempt_control_key(record.job_id, 2),
        ]
    );
}

#[test]
fn prune_covers_dedup() {
    let mut record = rocrate_record(JobId::from_bytes([0x57; 16]));
    record.dedup_key = Some(b"import".to_vec());

    let deletes = prune_delete_entries(&record);

    assert!(deletes.contains(&(
        JOB_DEDUP_INDEX_KEYSPACE.to_string(),
        dedup_index_key(record.created_by, b"import"),
    )));
}

fn pid_record(job_id: JobId, minted_by: UserId) -> JobRecord {
    let document_id = Ulid::from_bytes([0x99u8; 16]);
    JobRecord::new(
        job_id,
        JobPayload::MintPersistentId(MintPersistentIdSpec {
            document_id,
            minted_by,
        }),
        minted_by,
        node_id(7),
        1_000,
        1_000,
        Some(pid_dedup_key(document_id)),
    )
}

#[test]
fn dedup_index_global() {
    let realm = RealmId([1u8; 32]);
    let first = UserId::new(Ulid::from_bytes([2u8; 16]), realm);
    let second = UserId::new(Ulid::from_bytes([3u8; 16]), realm);
    let document_id = Ulid::from_bytes([0x99u8; 16]);

    assert_eq!(
        dedup_index_key(first, &pid_dedup_key(document_id)),
        dedup_index_key(second, &pid_dedup_key(document_id)),
    );
    assert_ne!(
        dedup_index_key(first, b"user/local"),
        dedup_index_key(second, b"user/local"),
    );
}

#[test]
fn prune_reclaims_pid() {
    let minted_by = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    let record = pid_record(JobId::from_bytes([0x58; 16]), minted_by);
    let dedup_key = record
        .dedup_key
        .clone()
        .expect("pid jobs carry a dedup key");

    let deletes = prune_delete_entries(&record);

    assert!(deletes.contains(&(
        JOB_DEDUP_INDEX_KEYSPACE.to_string(),
        dedup_index_key(record.created_by, &dedup_key),
    )));
    assert!(record.payload.dedup_until_prune());
}

#[test]
fn schedule_uses_retention() {
    let mut record = queued_record(JobId::from_bytes([0x56; 16]));
    record.state = JobState::Succeeded;
    record.finished_at_ms = Some(2_000);
    record.retention_ms = 42;

    let (expiry, job_id) = parse_schedule_key(job_schedule_key(&record).as_ref()).unwrap();

    assert_eq!(expiry, 2_042);
    assert_eq!(job_id, record.job_id);
}

#[tokio::test]
async fn handoff_releases_preintent() {
    // Shutdown while staging: no attempt intent exists yet, so the lease
    // must be released instead of failing with MissingControl.
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0x21; 16]);
    let mut record = queued_record(job_id);
    record.execution_class = JobExecutionClass::ExternalAttempt;
    record.state = JobState::Preparing;
    let token = Ulid::generate();
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: token,
        lease_expires_at_ms: 10_000,
    });
    insert_job(&storage, &record).await.unwrap();

    let outcome = handoff_external_attempt(&storage, job_id, token, 2_000)
        .await
        .unwrap();

    let ReleaseOutcome::Released(released) = outcome else {
        panic!("pre-intent handoff must release the job");
    };
    assert_eq!(released.state, JobState::Queued);
    assert!(released.claim.is_none());
}

#[tokio::test]
async fn claim_moves_index() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([9u8; 16]);
    insert_job(&storage, &queued_record(job_id)).await.unwrap();

    let ClaimOutcome::Claimed(record) = claim_job(&storage, job_id, node_id(3), 5_000)
        .await
        .expect("claim succeeds")
    else {
        panic!("expected claimed outcome");
    };

    let claim = record.claim.expect("claim present");
    assert_eq!(record.state, JobState::Claimed);
    assert_eq!(claim.lease_expires_at_ms, 5_000 + JOB_LEASE_MS);

    let keys = schedule_keys(&storage).await;
    assert_eq!(keys.len(), 1, "exactly one schedule index entry");
    assert!(keys[0].starts_with(JOB_LEASE_INDEX_PREFIX));
    let (ts, parsed) = parse_schedule_key(keys[0].as_ref()).unwrap();
    assert_eq!(ts, 5_000 + JOB_LEASE_MS);
    assert_eq!(parsed, job_id);
}

// Perf budget: a claim is start + read + batch-write + batch-delete + commit.
#[tokio::test]
async fn claim_transition_bounded() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([2u8; 16]);
    insert_job(&storage, &queued_record(job_id)).await.unwrap();

    let before = storage.snapshot_metrics().requests_total;
    claim_job(&storage, job_id, node_id(3), 5_000)
        .await
        .unwrap();
    assert_eq!(storage.snapshot_metrics().requests_total - before, 5);
}

#[tokio::test]
async fn report_freezes() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0x42; 16]);
    insert_job(&storage, &rocrate_record(job_id)).await.unwrap();
    let ClaimOutcome::Claimed(claimed) = claim_job(&storage, job_id, node_id(3), 2_000)
        .await
        .unwrap()
    else {
        panic!("job must be claimed");
    };
    let token = claimed.claim.unwrap().claim_token;
    let row_b = report_row("b");
    let row_a = report_row("a");
    put_job_entry(&storage, job_id, token, b"b", &row_b)
        .await
        .unwrap();
    put_job_entry(&storage, job_id, token, b"a", &row_a)
        .await
        .unwrap();
    transition_to_running(&storage, job_id, token, 3_000)
        .await
        .unwrap();

    let terminal = complete_job(
        &storage,
        job_id,
        token,
        JobResultPayload::ImportRoCrate(ImportRoCrateResult {
            document_id: None,
            entries_total: 2,
            imported: 2,
            unlisted: 0,
            failed: 0,
            report_digest: [0; 32],
        }),
        JobProgress {
            current: 2,
            total: Some(2),
            unit: "items".to_string(),
        },
        4_000,
    )
    .await
    .unwrap();

    let mut hasher = blake3::Hasher::new();
    hash_report_value(&mut hasher, &postcard::to_allocvec(&row_a).unwrap());
    hash_report_value(&mut hasher, &postcard::to_allocvec(&row_b).unwrap());
    let digest = *hasher.finalize().as_bytes();
    assert_eq!(terminal.report_digest, Some(digest));
    let JobResultPayload::ImportRoCrate(result) = terminal.result.unwrap() else {
        panic!("import result expected");
    };
    assert_eq!(result.report_digest, digest);

    let (first, cursor) = list_job_entries(&storage, job_id, None, 1).await.unwrap();
    assert_eq!(first[0].0, b"a");
    let (second, cursor) = list_job_entries(&storage, job_id, cursor, 1).await.unwrap();
    assert_eq!(second[0].0, b"b");
    assert!(cursor.is_none());
    assert!(matches!(
        put_job_entry(&storage, job_id, token, b"c", &report_row("c")).await,
        Err(JobMutationError::ReportFrozen)
    ));
}

// The framework is opt-in: a write to any other keyspace never touches a job one.
#[tokio::test]
async fn other_write_untouched() {
    let (_dir, storage) = temp_storage();
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: "unrelated".to_string(),
            key: ByteView::from(vec![1]),
            value: ByteView::from(vec![2]),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected write event: {other:?}"),
    }
    for key_space in [
        JOB_KEYSPACE,
        JOB_SCHEDULE_INDEX_KEYSPACE,
        JOB_OWNER_INDEX_KEYSPACE,
        JOB_DEDUP_INDEX_KEYSPACE,
    ] {
        let (values, _) = iter_prefix_page(&storage, key_space, None, None, 8, None)
            .await
            .unwrap();
        assert!(values.is_empty(), "{key_space} must stay empty");
    }
}

#[tokio::test]
async fn zombie_rejected() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([1u8; 16]);
    insert_job(&storage, &queued_record(job_id)).await.unwrap();
    claim_job(&storage, job_id, node_id(3), 5_000)
        .await
        .unwrap();

    let err = complete_job(
        &storage,
        job_id,
        Ulid::generate(),
        JobResultPayload::Probe { completed_steps: 1 },
        JobProgress::new("steps"),
        6_000,
    )
    .await
    .expect_err("stale token must be rejected");
    assert!(matches!(err, JobMutationError::TokenMismatch));

    let record = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        record.state,
        JobState::Claimed,
        "record unchanged after rejection"
    );
}

#[tokio::test]
async fn stale_checkpoint_rejected() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([2u8; 16]);
    insert_job(&storage, &queued_record(job_id)).await.unwrap();
    let ClaimOutcome::Claimed(record) = claim_job(&storage, job_id, node_id(3), 5_000)
        .await
        .unwrap()
    else {
        panic!("job must be claimed")
    };
    let token = record.claim.unwrap().claim_token;
    let key = ByteView::from(job_id.to_bytes().to_vec());
    put_state(
        &storage,
        job_id,
        token,
        STAGING_JOB_STATE_KEYSPACE,
        key.clone(),
        &b"newer".to_vec(),
    )
    .await
    .unwrap();

    assert!(matches!(
        put_state(
            &storage,
            job_id,
            Ulid::generate(),
            STAGING_JOB_STATE_KEYSPACE,
            key.clone(),
            &b"stale".to_vec(),
        )
        .await,
        Err(JobMutationError::TokenMismatch)
    ));
    assert_eq!(
        read_state::<Vec<u8>>(
            &storage,
            STAGING_JOB_STATE_KEYSPACE,
            key,
            "staging checkpoint",
        )
        .await
        .unwrap()
        .unwrap(),
        b"newer".to_vec()
    );
}

#[tokio::test]
async fn requeue_backs_off() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([4u8; 16]);
    insert_job(&storage, &queued_record(job_id)).await.unwrap();
    claim_job(&storage, job_id, node_id(3), 5_000)
        .await
        .unwrap();

    let RequeueOutcome::Requeued(record) = requeue_job(
        &storage,
        job_id,
        None,
        6_000,
        None,
        Some(JobError::retryable("lease expired")),
    )
    .await
    .unwrap() else {
        panic!("expected requeue");
    };
    assert_eq!(record.state, JobState::Queued);
    assert_eq!(record.attempts, 1);
    // retry_delay_ms(1) = 500ms.
    assert_eq!(record.due_at_ms, 6_500);
    assert!(record.claim.is_none());
}

#[tokio::test]
async fn deferral_keeps_attempt() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0x44u8; 16]);
    insert_job(&storage, &queued_record(job_id)).await.unwrap();
    let ClaimOutcome::Claimed(record) = claim_job(&storage, job_id, node_id(3), 5_000)
        .await
        .unwrap()
    else {
        panic!("job must be claimed")
    };
    let token = record.claim.unwrap().claim_token;

    let ReleaseOutcome::Released(deferred) = defer_job(
        &storage,
        job_id,
        token,
        6_000,
        1_000,
        JobError::retryable("metadata projection pending"),
    )
    .await
    .unwrap() else {
        panic!("job must be deferred")
    };
    assert_eq!(deferred.state, JobState::Queued);
    assert_eq!(deferred.attempts, 0);
    assert_eq!(deferred.due_at_ms, 7_000);
    assert!(deferred.claim.is_none());
    assert_eq!(
        deferred
            .last_error
            .as_ref()
            .map(|error| error.message.as_str()),
        Some("metadata projection pending")
    );
}

// Retried payload work starts over, so persisted progress must start over too.
#[tokio::test]
async fn retry_resets_progress() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([12u8; 16]);
    let mut record = queued_record(job_id);
    record.progress.current = 1;
    record.progress.total = Some(1);
    insert_job(&storage, &record).await.unwrap();
    claim_job(&storage, job_id, node_id(3), 5_000)
        .await
        .unwrap();

    let RequeueOutcome::Requeued(record) = requeue_job(&storage, job_id, None, 6_000, None, None)
        .await
        .unwrap()
    else {
        panic!("expected requeue");
    };
    assert_eq!(
        record.progress,
        JobProgress::new(record.payload.progress_unit())
    );
}

#[tokio::test]
async fn requeue_exhausts() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([6u8; 16]);
    let mut record = queued_record(job_id);
    record.attempts = JOB_MAX_ATTEMPTS - 1;
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: Ulid::generate(),
        lease_expires_at_ms: 5_000,
    });
    insert_job(&storage, &record).await.unwrap();

    let RequeueOutcome::Exhausted(failed) = requeue_job(&storage, job_id, None, 6_000, None, None)
        .await
        .unwrap()
    else {
        panic!("expected local exhaustion");
    };
    assert_eq!(failed.state, JobState::Indeterminate);
    assert!(failed.locally_exhausted);
    assert!(failed.claim.is_none());
    assert_eq!(failed.attempts, JOB_MAX_ATTEMPTS);
}

// A job-specific permanent verdict is the only exhaustion that terminalizes.
#[tokio::test]
async fn permanent_cap_fails() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xD4; 16]);
    let mut record = queued_record(job_id);
    record.attempts = JOB_MAX_ATTEMPTS - 1;
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: Ulid::generate(),
        lease_expires_at_ms: 5_000,
    });
    insert_job(&storage, &record).await.unwrap();

    let RequeueOutcome::Exhausted(failed) = requeue_job(
        &storage,
        job_id,
        None,
        6_000,
        None,
        Some(JobError::permanent("input is not readable")),
    )
    .await
    .unwrap() else {
        panic!("expected exhaustion");
    };
    assert_eq!(failed.state, JobState::Failed);
    assert!(!failed.locally_exhausted);
    assert_eq!(failed.finished_at_ms, Some(6_000));
}

// Exhaustion means no further automatic attempt: the sweep must not pick it up.
#[tokio::test]
async fn exhausted_stops_sweep() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xD5; 16]);
    let mut record = queued_record(job_id);
    record.attempts = JOB_MAX_ATTEMPTS - 1;
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: Ulid::generate(),
        lease_expires_at_ms: 5_000,
    });
    insert_job(&storage, &record).await.unwrap();
    requeue_job(&storage, job_id, None, 6_000, Some(6_000), None)
        .await
        .unwrap();

    let swept = requeue_job(&storage, job_id, None, 9_000, Some(9_000), None)
        .await
        .unwrap();
    assert!(matches!(swept, RequeueOutcome::Skipped));
    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.attempts, JOB_MAX_ATTEMPTS);
}

#[tokio::test]
async fn cleanup_retries_forever() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xC5; 16]);
    let mut record = JobRecord::new(
        job_id,
        JobPayload::TerminalCleanup {
            for_job: JobId::from_bytes([0xC6; 16]),
            attempt: None,
            access_key: "access".to_string(),
        },
        UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
        node_id(7),
        1_000,
        1_000,
        None,
    );
    record.attempts = JOB_MAX_ATTEMPTS - 1;
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: Ulid::generate(),
        lease_expires_at_ms: 5_000,
    });
    insert_job(&storage, &record).await.unwrap();

    let RequeueOutcome::Requeued(requeued) = requeue_job(&storage, job_id, None, 6_000, None, None)
        .await
        .unwrap()
    else {
        panic!("cleanup must remain retryable");
    };
    assert_eq!(requeued.state, JobState::Queued);
    assert_eq!(requeued.attempts, JOB_MAX_ATTEMPTS);
}

#[tokio::test]
async fn import_exhausts() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xC7; 16]);
    let mut record = rocrate_record(job_id);
    record.attempts = JOB_MAX_ATTEMPTS - 1;
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: Ulid::generate(),
        lease_expires_at_ms: 5_000,
    });
    insert_job(&storage, &record).await.unwrap();

    let RequeueOutcome::Exhausted(failed) = requeue_job(&storage, job_id, None, 6_000, None, None)
        .await
        .unwrap()
    else {
        panic!("import must stop after exhausting attempts");
    };
    assert_eq!(failed.state, JobState::Indeterminate);
    assert!(failed.locally_exhausted);
    assert_eq!(failed.attempts, JOB_MAX_ATTEMPTS);
}

#[tokio::test]
async fn release_keeps_attempts() {
    // Lease hand-back preserves attempts but resets execution progress.
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xA1; 16]);
    let token = Ulid::generate();
    let mut record = running_record(job_id, token, 60_000);
    record.attempts = 2;
    record.progress.current = 4;
    record.progress.total = Some(10);
    insert_job(&storage, &record).await.unwrap();

    let ReleaseOutcome::Released(released) =
        release_job(&storage, job_id, token, 6_000).await.unwrap()
    else {
        panic!("expected release");
    };
    assert_eq!(released.state, JobState::Queued);
    assert_eq!(released.attempts, 2);
    assert_eq!(released.due_at_ms, 6_000);
    assert_eq!(
        released.progress,
        JobProgress::new(released.payload.progress_unit())
    );
    assert!(released.claim.is_none());
}

#[tokio::test]
async fn release_guards_token() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xB2; 16]);
    insert_job(&storage, &running_record(job_id, Ulid::generate(), 60_000))
        .await
        .unwrap();

    let error = release_job(&storage, job_id, Ulid::generate(), 6_000)
        .await
        .expect_err("a foreign token must not release the lease");
    assert!(matches!(error, JobMutationError::TokenMismatch));
    let record = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, JobState::Running);
    assert!(record.claim.is_some());
}

#[tokio::test]
async fn cancel_before_claim() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([7u8; 16]);
    let mut record = queued_record(job_id);
    record.cancel_requested = true;
    insert_job(&storage, &record).await.unwrap();

    let ClaimOutcome::CancelledFresh(cancelled) = claim_job(&storage, job_id, node_id(3), 5_000)
        .await
        .unwrap()
    else {
        panic!("expected fresh cancellation");
    };
    assert_eq!(cancelled.state, JobState::Cancelled);
}

#[tokio::test]
async fn cancel_terminal_noop() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([8u8; 16]);
    let mut record = queued_record(job_id);
    record.state = JobState::Succeeded;
    record.finished_at_ms = Some(2_000);
    insert_job(&storage, &record).await.unwrap();

    let outcome = set_cancel_requested(&storage, job_id, 9_000).await.unwrap();
    assert!(matches!(outcome, CancelRequestOutcome::AlreadyTerminal(_)));
    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert!(!stored.cancel_requested);
}

#[tokio::test]
async fn terminal_clears_dedup() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([5u8; 16]);
    let mut record = queued_record(job_id);
    record.dedup_key = Some(b"dedup".to_vec());
    record.state = JobState::Running;
    let token = Ulid::generate();
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: token,
        lease_expires_at_ms: 5_000,
    });
    insert_job(&storage, &record).await.unwrap();
    assert_eq!(
        find_dedup_job(&storage, record.created_by, b"dedup", None)
            .await
            .unwrap(),
        Some(job_id)
    );

    complete_job(
        &storage,
        job_id,
        token,
        JobResultPayload::Probe { completed_steps: 1 },
        JobProgress::new("steps"),
        6_000,
    )
    .await
    .unwrap();

    assert_eq!(
        find_dedup_job(&storage, record.created_by, b"dedup", None)
            .await
            .unwrap(),
        None
    );
    let keys = schedule_keys(&storage).await;
    assert_eq!(keys.len(), 1);
    assert!(keys[0].starts_with(aruna_core::structs::JOB_PRUNE_INDEX_PREFIX));
}

#[tokio::test]
async fn malformed_retained() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([3u8; 16]);
    batch_write(
        &storage,
        vec![(
            JOB_KEYSPACE.to_string(),
            job_record_key(job_id),
            ByteView::from(vec![1, 2, 3]),
        )],
        None,
    )
    .await
    .unwrap();

    assert!(
        read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        read_raw(&storage, JOB_KEYSPACE, job_record_key(job_id), None)
            .await
            .unwrap()
            .is_some()
    );
}

fn running_record(job_id: JobId, token: Ulid, lease_expires: u64) -> JobRecord {
    let mut record = queued_record(job_id);
    record.state = JobState::Running;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: token,
        lease_expires_at_ms: lease_expires,
    });
    record
}

fn execution_record(job_id: JobId, token: Ulid, state: JobState) -> JobRecord {
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Execution(ExecutionSpec {
            group_id: Ulid::from_bytes([3u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        }),
        UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32])),
        node_id(7),
        1_000,
        1_000,
        None,
    );
    record.state = state;
    record.claim = Some(JobClaim {
        holder_node_id: node_id(3),
        claim_token: token,
        lease_expires_at_ms: 10_000,
    });
    record
}

fn execution_result() -> JobResultPayload {
    JobResultPayload::Execution {
        exit_code: Some(0),
        workspace_bucket: Some("ws-test".to_string()),
        outputs: Vec::new(),
        stdout: String::new(),
        stderr: String::new(),
        output_digest: None,
    }
}

fn named_result(digest: [u8; 32]) -> JobResultPayload {
    let mut result = execution_result();
    if let JobResultPayload::Execution { output_digest, .. } = &mut result {
        *output_digest = Some(digest);
    }
    result
}

/// Fence an attempt so the success path has a control row to read.
async fn fence_attempt(storage: &StorageHandle, job_id: JobId, token: Ulid) -> AttemptControl {
    record_attempt_intent(
        storage,
        job_id,
        token,
        AttemptIntent {
            attempt_no: 1,
            external_name: "aruna-test-a1".to_string(),
            executor_kind: "docker".to_string(),
            pinned_image: "alpine@sha256:0".to_string(),
            attempt_epoch: 0,
        },
        None,
        2_000,
    )
    .await
    .unwrap()
    .control
}

#[tokio::test]
async fn revived_lease_kept() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([2u8; 16]);
    insert_job(&storage, &running_record(job_id, Ulid::generate(), 10_000))
        .await
        .unwrap();

    // Sweep at now=5_000 with the lease valid until 10_000 must not revoke it.
    let outcome = requeue_job(
        &storage,
        job_id,
        None,
        5_000,
        Some(5_000),
        Some(JobError::retryable("lease expired")),
    )
    .await
    .unwrap();
    assert!(matches!(outcome, RequeueOutcome::Skipped));
    let record = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, JobState::Running);
}

// A sweep racing a completed requeue (record already Queued, claim gone) must not
// charge a second attempt or overwrite last_error.
#[tokio::test]
async fn sweep_skips_requeued() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([11u8; 16]);
    let mut record = queued_record(job_id);
    record.attempts = 1;
    insert_job(&storage, &record).await.unwrap();

    let outcome = requeue_job(
        &storage,
        job_id,
        None,
        6_000,
        Some(6_000),
        Some(JobError::retryable("lease expired")),
    )
    .await
    .unwrap();
    assert!(matches!(outcome, RequeueOutcome::Skipped));
    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Queued);
    assert_eq!(stored.attempts, 1);
    assert!(stored.last_error.is_none());
}

#[tokio::test]
async fn reclaim_not_eligible() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([2u8; 16]);
    insert_job(&storage, &queued_record(job_id)).await.unwrap();
    claim_job(&storage, job_id, node_id(3), 5_000)
        .await
        .unwrap();

    // A second claim of an already-Claimed job must not hand back a token.
    let outcome = claim_job(&storage, job_id, node_id(4), 6_000)
        .await
        .unwrap();
    assert!(matches!(outcome, ClaimOutcome::NotEligible));
}

// A state filter must scan past non-matching pages, not return an empty page + cursor.
#[tokio::test]
async fn filter_scans_pages() {
    let (_dir, storage) = temp_storage();
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    let make = |seq: u64, state: JobState| {
        let mut record = JobRecord::new(
            job_id(seq),
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            owner,
            node_id(7),
            seq * 1000,
            seq * 1000,
            None,
        );
        record.state = state;
        record
    };
    for record in [
        make(5, JobState::Queued),
        make(4, JobState::Failed),
        make(3, JobState::Queued),
    ] {
        insert_job(&storage, &record).await.unwrap();
    }
    batch_write(
        &storage,
        vec![(
            JOB_OWNER_INDEX_KEYSPACE.to_string(),
            owner_index_key(owner, 2_000, job_id(2)),
            empty_value(),
        )],
        None,
    )
    .await
    .unwrap();

    let (page, cursor) = list_user_jobs(&storage, owner, None, 1, |record| {
        record.state == JobState::Failed
    })
    .await
    .unwrap();
    assert_eq!(page.len(), 1, "the older Failed job is found across pages");
    assert_eq!(page[0].state, JobState::Failed);
    assert!(
        cursor.is_none(),
        "no dangling cursor on an exhausted filter"
    );
}

#[tokio::test]
async fn internal_jobs_hidden() {
    let (_dir, storage) = temp_storage();
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    let job_id = JobId::from_bytes([0xC1; 16]);
    let record = JobRecord::new(
        job_id,
        JobPayload::WriteRunCrate {
            for_job: JobId::from_bytes([0xC2; 16]),
        },
        owner,
        node_id(7),
        1_000,
        1_000,
        None,
    );
    insert_job(&storage, &record).await.unwrap();

    let (indexed, _) = iter_prefix_page(
        &storage,
        JOB_OWNER_INDEX_KEYSPACE,
        Some(owner_index_prefix(owner)),
        None,
        1,
        None,
    )
    .await
    .unwrap();
    assert!(indexed.is_empty(), "internal jobs are not owner-indexed");

    batch_write(
        &storage,
        vec![(
            JOB_OWNER_INDEX_KEYSPACE.to_string(),
            owner_index_key(owner, record.created_at_ms, job_id),
            empty_value(),
        )],
        None,
    )
    .await
    .unwrap();
    let (listed, cursor) = list_user_jobs(&storage, owner, None, 1, |_| true)
        .await
        .unwrap();
    assert!(listed.is_empty(), "stale owner entries stay hidden");
    assert!(cursor.is_none());
}

#[tokio::test]
async fn cleanup_jobs_hidden() {
    let (_dir, storage) = temp_storage();
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    let job_id = JobId::from_bytes([0xC7; 16]);
    let record = JobRecord::new(
        job_id,
        JobPayload::TerminalCleanup {
            for_job: JobId::from_bytes([0xC8; 16]),
            attempt: None,
            access_key: "access".to_string(),
        },
        owner,
        node_id(7),
        1_000,
        1_000,
        None,
    );
    insert_job(&storage, &record).await.unwrap();

    let (indexed, _) = iter_prefix_page(
        &storage,
        JOB_OWNER_INDEX_KEYSPACE,
        Some(owner_index_prefix(owner)),
        None,
        1,
        None,
    )
    .await
    .unwrap();
    assert!(indexed.is_empty());

    batch_write(
        &storage,
        vec![(
            JOB_OWNER_INDEX_KEYSPACE.to_string(),
            owner_index_key(owner, record.created_at_ms, job_id),
            empty_value(),
        )],
        None,
    )
    .await
    .unwrap();
    let (listed, cursor) = list_user_jobs(&storage, owner, None, 1, |_| true)
        .await
        .unwrap();
    assert!(listed.is_empty());
    assert!(cursor.is_none());
}

#[tokio::test]
async fn terminal_enqueues_cleanup() {
    let (_dir, storage) = temp_storage();
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    let job_id = JobId::from_bytes([0xC9; 16]);
    let token = Ulid::from_bytes([0xCA; 16]);
    let intent = AttemptIntent {
        attempt_no: 2,
        external_name: aruna_core::structs::attempt_external_name(job_id, 2),
        executor_kind: "docker".to_string(),
        pinned_image:
            "alpine@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
        attempt_epoch: 1,
    };
    let mut record = JobRecord::new(
        job_id,
        JobPayload::Execution(ExecutionSpec {
            group_id: Ulid::from_bytes([3u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        }),
        owner,
        node_id(7),
        1_000,
        1_000,
        None,
    );
    record.state = JobState::Running;
    record.attempt_intent = Some(intent.clone());
    record.claim = Some(JobClaim {
        holder_node_id: node_id(7),
        claim_token: token,
        lease_expires_at_ms: 5_000,
    });
    insert_job(&storage, &record).await.unwrap();

    let terminal = complete_job(
        &storage,
        job_id,
        token,
        JobResultPayload::Execution {
            exit_code: Some(0),
            workspace_bucket: Some("ws-test".to_string()),
            outputs: Vec::new(),
            stdout: String::new(),
            stderr: String::new(),
            output_digest: None,
        },
        JobProgress::new("phases"),
        6_000,
    )
    .await
    .unwrap();

    assert!(terminal.claim.is_none());
    let child = read_job_record(&storage, cleanup_job_id(job_id), None)
        .await
        .unwrap()
        .unwrap();
    let expected_access = UserAccess::build_access_key(&workspace_credential_id(job_id)).unwrap();
    assert_eq!(
        child.payload,
        JobPayload::TerminalCleanup {
            for_job: job_id,
            attempt: Some(intent),
            access_key: expected_access,
        }
    );
    assert_eq!(child.state, JobState::Queued);
    assert!(child.payload.is_internal());
}

#[tokio::test]
async fn dedup_delete_guarded() {
    let (_dir, storage) = temp_storage();
    let job_a = JobId::from_bytes([1u8; 16]);
    let job_b = JobId::from_bytes([2u8; 16]);
    let token = Ulid::generate();
    let mut record = running_record(job_a, token, 10_000);
    record.dedup_key = Some(b"k".to_vec());
    insert_job(&storage, &record).await.unwrap();

    // A raced submit repoints the dedup row at job B.
    batch_write(
        &storage,
        vec![(
            JOB_DEDUP_INDEX_KEYSPACE.to_string(),
            dedup_index_key(record.created_by, b"k"),
            ByteView::from(encode_dedup_value(job_b, [3u8; 32])),
        )],
        None,
    )
    .await
    .unwrap();

    complete_job(
        &storage,
        job_a,
        token,
        JobResultPayload::Probe { completed_steps: 1 },
        JobProgress::new("steps"),
        6_000,
    )
    .await
    .unwrap();

    // A going terminal must not delete B's dedup row.
    assert_eq!(
        find_dedup_job(&storage, record.created_by, b"k", None)
            .await
            .unwrap(),
        Some(job_b)
    );
}

// An external attempt with an expired lease is never requeued; it is reconciled,
// and the sweep leaves it untouched so a restart whose adoption succeeds is free.
#[tokio::test]
async fn external_never_requeued() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xE0; 16]);
    let mut record = execution_record(job_id, Ulid::generate(), JobState::Running);
    record.claim.as_mut().unwrap().lease_expires_at_ms = 1;
    record.attempt_intent = Some(AttemptIntent {
        attempt_no: 1,
        external_name: "attempt".to_string(),
        executor_kind: "docker".to_string(),
        pinned_image: "alpine@sha256:digest".to_string(),
        attempt_epoch: 1,
    });
    insert_job(&storage, &record).await.unwrap();

    let outcome = requeue_job(&storage, job_id, None, 9_000, Some(9_000), None)
        .await
        .unwrap();
    assert!(matches!(outcome, RequeueOutcome::NeedsReconcile(_)));
    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Running, "not requeued");
    assert_eq!(stored.attempts, 0, "attempt untouched");
    assert!(stored.claim.is_some(), "claim untouched");
}

// The effective retry budget of a park that repeats: one attempt per supervision
// cycle, because the sweep that re-routes the parked job never charges a second.
#[tokio::test]
async fn park_sweep_budget() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xE4; 16]);
    let token = Ulid::generate();
    let mut record = execution_record(job_id, token, JobState::Running);
    record.claim.as_mut().unwrap().lease_expires_at_ms = 1;
    record.workspace_bucket = Some("ws-test".to_string());
    record.attempt_intent = Some(AttemptIntent {
        attempt_no: 1,
        external_name: "attempt".to_string(),
        executor_kind: "docker".to_string(),
        pinned_image: "alpine@sha256:digest".to_string(),
        attempt_epoch: 1,
    });
    insert_job(&storage, &record).await.unwrap();

    for cycle in 1..JOB_MAX_ATTEMPTS {
        let parked = mark_indeterminate(
            &storage,
            job_id,
            token,
            JobError::retryable("attempt unobservable"),
            6_000,
        )
        .await
        .unwrap();
        assert!(matches!(parked, ParkOutcome::Parked(_)), "cycle {cycle}");
        let swept = requeue_job(&storage, job_id, None, 9_000, Some(9_000), None)
            .await
            .unwrap();
        assert!(
            matches!(swept, RequeueOutcome::NeedsReconcile(_)),
            "cycle {cycle}"
        );
        let stored = read_job_record(&storage, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.attempts, cycle, "one charge per cycle");
        assert_eq!(stored.state, JobState::Indeterminate);
    }

    let capped = mark_indeterminate(
        &storage,
        job_id,
        token,
        JobError::retryable("attempt unobservable"),
        7_000,
    )
    .await
    .unwrap();
    let ParkOutcome::Exhausted(failed) = capped else {
        panic!("the last cycle must exhaust");
    };
    assert_eq!(failed.state, JobState::Indeterminate);
    assert!(failed.locally_exhausted);
    assert_eq!(failed.attempts, JOB_MAX_ATTEMPTS);
}

// Parking spends the attempt and stops at the cap; without that a failure
// repeating on every reconcile pass re-drives the job forever.
#[tokio::test]
async fn park_spends_attempt() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xE5; 16]);
    let token = Ulid::generate();
    let mut record = execution_record(job_id, token, JobState::Running);
    record.workspace_bucket = Some("ws-test".to_string());
    insert_job(&storage, &record).await.unwrap();

    for attempt in 1..JOB_MAX_ATTEMPTS {
        let ParkOutcome::Parked(parked) = mark_indeterminate(
            &storage,
            job_id,
            token,
            JobError::retryable("output capture failed"),
            6_000,
        )
        .await
        .unwrap() else {
            panic!("a park below the cap must stay Indeterminate");
        };
        assert_eq!(parked.state, JobState::Indeterminate);
        assert_eq!(parked.attempts, attempt);
    }

    let ParkOutcome::Exhausted(failed) = mark_indeterminate(
        &storage,
        job_id,
        token,
        JobError::retryable("output capture failed"),
        7_000,
    )
    .await
    .unwrap() else {
        panic!("the capped park must exhaust");
    };
    assert_eq!(failed.state, JobState::Indeterminate);
    assert!(failed.locally_exhausted);
    assert_eq!(failed.attempts, JOB_MAX_ATTEMPTS);
    assert!(failed.finished_at_ms.is_none());
    assert!(failed.claim.is_none());
    assert!(
        matches!(failed.result, Some(JobResultPayload::Execution { .. })),
        "cleanup and the run crate need a result"
    );
}

// Every cap site must leave the result payload terminal cleanup and the run crate
// read, not just the park that exhaustion was introduced for.
#[tokio::test]
async fn cap_sites_result() {
    let (_dir, storage) = temp_storage();
    let swept = JobId::from_bytes([0xE6; 16]);
    let token = Ulid::generate();
    let mut record = execution_record(swept, token, JobState::Ready);
    record.claim.as_mut().unwrap().lease_expires_at_ms = 1;
    record.attempts = JOB_MAX_ATTEMPTS - 1;
    record.workspace_bucket = Some("ws-test".to_string());
    insert_job(&storage, &record).await.unwrap();
    let presubmit = JobId::from_bytes([0xE9; 16]);
    let mut record = execution_record(presubmit, token, JobState::Ready);
    record.attempts = JOB_MAX_ATTEMPTS - 1;
    record.workspace_bucket = Some("ws-test".to_string());
    insert_job(&storage, &record).await.unwrap();

    let RequeueOutcome::Exhausted(failed) =
        requeue_job(&storage, swept, None, 9_000, Some(9_000), None)
            .await
            .unwrap()
    else {
        panic!("the sweep must exhaust at the cap");
    };
    assert!(matches!(
        failed.result,
        Some(JobResultPayload::Execution { .. })
    ));

    let failed = requeue_before_attempt(
        &storage,
        presubmit,
        token,
        9_000,
        JobError::retryable("image pull failed"),
    )
    .await
    .unwrap();
    assert_eq!(failed.state, JobState::Indeterminate);
    assert!(failed.locally_exhausted);
    assert!(matches!(
        failed.result,
        Some(JobResultPayload::Execution { .. })
    ));
}

#[tokio::test]
async fn preintent_requeues() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xE3; 16]);
    let mut record = execution_record(job_id, Ulid::generate(), JobState::Ready);
    record.claim.as_mut().unwrap().lease_expires_at_ms = 1;
    insert_job(&storage, &record).await.unwrap();

    let outcome = requeue_job(&storage, job_id, None, 9_000, Some(9_000), None)
        .await
        .unwrap();
    assert!(matches!(outcome, RequeueOutcome::Requeued(_)));
    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Queued);
    assert_eq!(stored.attempts, 1);
    assert!(stored.claim.is_none());
}

#[tokio::test]
async fn attempt_intent_persists() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xA0; 16]);
    let token = Ulid::generate();
    insert_job(&storage, &running_record(job_id, token, 10_000))
        .await
        .unwrap();

    let intent = AttemptIntent {
        attempt_no: 1,
        external_name: aruna_core::structs::attempt_external_name(job_id, 1),
        executor_kind: "docker".to_string(),
        pinned_image:
            "alpine@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
        attempt_epoch: 0,
    };
    let committed = record_attempt_intent(&storage, job_id, token, intent, None, 6_000)
        .await
        .unwrap();

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.attempt_intent, committed.record.attempt_intent);
    assert_eq!(committed.control.attempt_epoch, 1);
}

#[tokio::test]
async fn cancel_blocks_intent() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xA1; 16]);
    let token = Ulid::generate();
    let mut record = running_record(job_id, token, 10_000);
    record.execution_class = JobExecutionClass::ExternalAttempt;
    record.state = JobState::Ready;
    insert_job(&storage, &record).await.unwrap();
    set_cancel_requested(&storage, job_id, 5_000).await.unwrap();

    let intent = AttemptIntent {
        attempt_no: 0,
        external_name: aruna_core::structs::attempt_external_name(job_id, 0),
        executor_kind: "docker".to_string(),
        pinned_image:
            "alpine@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
        attempt_epoch: 0,
    };
    let error = record_attempt_intent(&storage, job_id, token, intent, None, 6_000)
        .await
        .unwrap_err();

    assert!(matches!(error, JobMutationError::IntentConflict));
}

#[tokio::test]
async fn cancel_blocks_running() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xA2; 16]);
    let token = Ulid::generate();
    insert_job(&storage, &execution_record(job_id, token, JobState::Ready))
        .await
        .unwrap();
    let intent = AttemptIntent {
        attempt_no: 0,
        external_name: aruna_core::structs::attempt_external_name(job_id, 0),
        executor_kind: "docker".to_string(),
        pinned_image:
            "alpine@sha256:0000000000000000000000000000000000000000000000000000000000000000"
                .to_string(),
        attempt_epoch: 0,
    };
    let committed = record_attempt_intent(&storage, job_id, token, intent, None, 4_000)
        .await
        .unwrap();
    set_cancel_requested(&storage, job_id, 5_000).await.unwrap();

    let stored = begin_external_running(&storage, job_id, token, Some(5_500), 6_000)
        .await
        .unwrap();

    assert_eq!(stored.state, JobState::Ready);
    assert!(stored.cancel_requested);
    assert_eq!(stored.attempt_intent, committed.record.attempt_intent);
    assert!(stored.started_at_ms.is_none());
    assert!(stored.claim.is_some());
}

#[tokio::test]
async fn running_backfills_start() {
    // Kubernetes and Apptainer accept an attempt before reporting a start
    // time; without a start the walltime cap would have no anchor.
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xA9; 16]);
    let token = Ulid::generate();
    insert_job(&storage, &execution_record(job_id, token, JobState::Ready))
        .await
        .unwrap();

    let stored = begin_external_running(&storage, job_id, token, None, 6_000)
        .await
        .unwrap();

    assert_eq!(stored.state, JobState::Running);
    assert_eq!(stored.started_at_ms, Some(6_000));

    // Later evidence must not move an already anchored start.
    let stored = begin_external_running(&storage, job_id, token, Some(9_000), 9_100)
        .await
        .unwrap();
    assert_eq!(stored.started_at_ms, Some(6_000));
}

#[tokio::test]
async fn cancel_blocks_completion() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xA3; 16]);
    let token = Ulid::generate();
    insert_job(
        &storage,
        &execution_record(job_id, token, JobState::Running),
    )
    .await
    .unwrap();
    fence_attempt(&storage, job_id, token).await;
    set_cancel_requested(&storage, job_id, 5_000).await.unwrap();

    let ExecutionCompleteOutcome::CancelRequested(stored) = complete_execution(
        &storage,
        job_id,
        token,
        execution_result(),
        JobProgress::new("phases"),
        6_000,
    )
    .await
    .unwrap() else {
        panic!("cancellation must win completion");
    };

    assert_eq!(stored.state, JobState::Running);
    assert!(stored.result.is_none());
    assert!(stored.finished_at_ms.is_none());
    assert!(stored.claim.is_some());
    assert!(
        read_job_record(&storage, cleanup_job_id(job_id), None)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn success_wins_cancel() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xA4; 16]);
    let token = Ulid::generate();
    insert_job(
        &storage,
        &execution_record(job_id, token, JobState::Running),
    )
    .await
    .unwrap();

    let control = fence_attempt(&storage, job_id, token).await;
    let digest = [8u8; 32];
    persist_output_record(&storage, job_id, &control, digest, vec![1, 2, 3])
        .await
        .unwrap();

    let ExecutionCompleteOutcome::Completed(completed) = complete_execution(
        &storage,
        job_id,
        token,
        named_result(digest),
        JobProgress::new("phases"),
        6_000,
    )
    .await
    .unwrap() else {
        panic!("success must complete without cancellation");
    };
    assert_eq!(completed.state, JobState::Succeeded);
    assert!(completed.claim.is_none());

    let outcome = set_cancel_requested(&storage, job_id, 7_000).await.unwrap();
    assert!(matches!(outcome, CancelRequestOutcome::AlreadyTerminal(_)));
    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Succeeded);
    assert!(!stored.cancel_requested);
    assert!(stored.result.is_some());
    assert!(
        read_job_record(&storage, cleanup_job_id(job_id), None)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn output_record_cas() {
    // A replay may confirm the same digest, but a later write cannot replace it.
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xA8; 16]);
    let token = Ulid::generate();
    insert_job(
        &storage,
        &execution_record(job_id, token, JobState::Running),
    )
    .await
    .unwrap();
    let control = fence_attempt(&storage, job_id, token).await;
    let first = vec![1, 2, 3];

    persist_output_record(&storage, job_id, &control, [8u8; 32], first.clone())
        .await
        .unwrap();
    persist_output_record(&storage, job_id, &control, [8u8; 32], vec![4, 5])
        .await
        .unwrap();
    let conflict = persist_output_record(&storage, job_id, &control, [9u8; 32], vec![6]).await;
    assert!(matches!(
        conflict,
        Err(JobMutationError::OutputRecordConflict)
    ));

    let raw = read_raw(
        &storage,
        JOB_OUTPUT_RECORD_KEYSPACE,
        ByteView::from(attempt_control_key(job_id, control.attempt_epoch)),
        None,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(raw.as_ref(), first.as_slice());
    let stored = read_attempt_control(&storage, job_id, control.attempt_epoch, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.output_record, Some([8u8; 32]));
}

#[tokio::test]
async fn success_needs_record() {
    // Without a durable output record naming the same digest, the storage
    // mutation itself refuses to publish Succeeded.
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xA7; 16]);
    let token = Ulid::generate();
    insert_job(
        &storage,
        &execution_record(job_id, token, JobState::Running),
    )
    .await
    .unwrap();
    let control = fence_attempt(&storage, job_id, token).await;

    let unproven = complete_execution(
        &storage,
        job_id,
        token,
        execution_result(),
        JobProgress::new("phases"),
        6_000,
    )
    .await;
    assert!(matches!(
        unproven,
        Err(JobMutationError::OutputsUnproven(_))
    ));

    persist_output_record(&storage, job_id, &control, [8u8; 32], vec![9])
        .await
        .unwrap();
    let mismatched = complete_execution(
        &storage,
        job_id,
        token,
        named_result([1u8; 32]),
        JobProgress::new("phases"),
        6_100,
    )
    .await;
    assert!(matches!(
        mismatched,
        Err(JobMutationError::OutputsUnproven(_))
    ));

    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Running);
    assert!(stored.result.is_none());
}

// A live renewed external lease is a plain sweep Skip, never routed to reconcile.
#[tokio::test]
async fn external_live_skipped() {
    let (_dir, storage) = temp_storage();
    let job_id = JobId::from_bytes([0xE2; 16]);
    let mut record = running_record(job_id, Ulid::generate(), 10_000);
    record.execution_class = JobExecutionClass::ExternalAttempt;
    insert_job(&storage, &record).await.unwrap();

    let outcome = requeue_job(&storage, job_id, None, 5_000, Some(5_000), None)
        .await
        .unwrap();
    assert!(matches!(outcome, RequeueOutcome::Skipped));
    let stored = read_job_record(&storage, job_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, JobState::Running);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_not_conflict() {
    // Cleanup-capacity exhaustion must fail fast, never feed the OCC retry.
    let (storage, receivers) = StorageHandle::new();
    let receiver = receivers.foreground;
    let actor = std::thread::spawn(move || {
        let (_effect, response_tx, ..) = receiver.recv().expect("commit effect arrives");
        assert!(response_tx.send(StorageEvent::Error {
            error: StorageError::CleanupCapacity,
        }));
    });

    let result = commit_txn(&storage, Ulid::generate()).await;
    actor.join().expect("storage actor finishes");

    assert!(matches!(result, CommitResult::Failed(_)));
}
