//! Tests of job submission and of reading jobs back from a node's own store.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::calls::{IoCall, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{
    ExecutionSpec, ImportMetadataTarget, ImportRoCrateSource, ImportRoCrateSpec,
    ImportRoCrateTarget, JobId, JobPayload, JobRecord, JobState, MAX_EXECUTION_OUTPUTS,
    OutputDestination, RoCrateLimits, WorkspaceMode, user_dedup_key,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::time::unix_timestamp_millis;
use aruna_storage::FjallStorage;
use aruna_tasks::TaskHandle;
use tempfile::tempdir;
use ulid::Ulid;

use super::job_artifacts::{ArtifactLookup, OwnedArtifactOperation};
use super::job_cancel::CancelJobOutcome;
use super::job_reads::tests::{cancel_owned_job, read_owned_job, read_owned_report};
use super::job_reports::{JobReportLookup, ReportQuery};
use super::runtime::JobsRuntime;
use super::store::row_calls::preserve_artifact_tombstone;
use super::submit::{SubmitJobError, SubmitJobResult};
use super::submit_execution::{ExecutionSubmission, SubmitExecutionOperation, validate_execution};
use super::submit_local::DedupLookupOperation;
use crate::driver::{DriverContext, drive};
use crate::jobs::store::test_calls::insert_job;
use crate::jobs::store::test_calls::read_job_record;

#[allow(clippy::too_many_arguments)]
async fn submit_execution_job(
    context: &DriverContext,
    spec: ExecutionSpec,
    created_by: UserId,
    owner_node_id: NodeId,
    idempotency_key: Option<String>,
    workspace_mode: WorkspaceMode,
    workspace_bucket: Option<String>,
    retention_ms: u64,
    active_cap: Option<u32>,
) -> Result<SubmitJobResult, SubmitJobError> {
    let submission = ExecutionSubmission {
        spec,
        created_by,
        owner_node_id,
        idempotency_key,
        workspace_mode,
        workspace_bucket,
        retention_ms,
        active_cap,
        serving: context.node_and_realm(),
        now_ms: unix_timestamp_millis(),
    };
    drive(SubmitExecutionOperation::new(submission), context).await
}

fn node_id() -> NodeId {
    iroh::SecretKey::from_bytes(&[7u8; 32]).public()
}

fn submission(serving: Option<(NodeId, RealmId)>) -> ExecutionSubmission {
    let realm_id = RealmId([1u8; 32]);
    ExecutionSubmission {
        spec: ExecutionSpec {
            group_id: Ulid::from_bytes([3u8; 16]),
            name: None,
            description: None,
            tags: Default::default(),
            image: "alpine:3".to_string(),
            entrypoint: None,
            command: Vec::new(),
            workdir: None,
            env: Default::default(),
            resources: Default::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        },
        created_by: UserId::new(Ulid::from_bytes([2u8; 16]), realm_id),
        owner_node_id: node_id(),
        idempotency_key: None,
        workspace_mode: WorkspaceMode::None,
        workspace_bucket: None,
        retention_ms: 1,
        active_cap: None,
        serving,
        now_ms: 1,
    }
}

// An unplaced node submits at once; a placed node reads its realm config first and refuses
// to own a job for another node.
#[test]
fn mints_from_placement() {
    let mut unplaced = SubmitExecutionOperation::new(submission(None));
    assert!(matches!(unplaced.first(), Ok(Next::Call(IoCall::Child(_)))));
    let realm_id = RealmId([1u8; 32]);
    let mut placed = SubmitExecutionOperation::new(submission(Some((node_id(), realm_id))));
    assert!(matches!(placed.first(), Ok(Next::Call(IoCall::Child(_)))));
    let other = iroh::SecretKey::from_bytes(&[8u8; 32]).public();
    let mut foreign = SubmitExecutionOperation::new(submission(Some((other, realm_id))));
    assert!(matches!(
        foreign.first(),
        Err(SubmitJobError::PlacementUnavailable(_))
    ));
}

#[tokio::test]
async fn caps_declared_outputs() {
    // The declaration bound is the immutable record's bound, so a valid
    // local success can never be impossible to publish.
    let (storage_handle, _receivers) = aruna_storage::StorageHandle::new();
    let context = DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let realm_id = RealmId([1u8; 32]);
    let mut spec = ExecutionSpec {
        group_id: Ulid::from_bytes([3u8; 16]),
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine:3".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: Default::default(),
        resources: Default::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: (0..=MAX_EXECUTION_OUTPUTS)
            .map(
                |index| aruna_core::structs::execution::job::WorkspaceOutput {
                    container_path: format!("/out/{index}"),
                    dest_key: format!("out/{index}"),
                },
            )
            .collect(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    };

    let error = submit_execution_job(
        &context,
        spec.clone(),
        UserId::new(Ulid::from_bytes([2u8; 16]), realm_id),
        node_id(),
        None,
        WorkspaceMode::Existing,
        Some("shared".to_string()),
        1,
        None,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        SubmitJobError::TooManyOutputs {
            limit: MAX_EXECUTION_OUTPUTS
        }
    ));

    spec.workspace_outputs.pop();
    spec.output_prefixes.push("out/".to_string());
    assert!(matches!(
        validate_execution(&mut spec, WorkspaceMode::Existing, Some("shared")),
        Err(SubmitJobError::InvalidWorkspace(_))
    ));
    spec.output_prefixes.clear();
    assert!(!matches!(
        submit_execution_job(
            &context,
            spec,
            UserId::new(Ulid::from_bytes([2u8; 16]), realm_id),
            node_id(),
            None,
            WorkspaceMode::Existing,
            Some("shared".to_string()),
            1,
            None,
        )
        .await,
        Err(SubmitJobError::TooManyOutputs { .. })
    ));
}

#[test]
fn refuses_pathless_input() {
    // Nothing copies an input into a bucket, so it must land in the container.
    let mut spec = ExecutionSpec {
        group_id: Ulid::from_bytes([3u8; 16]),
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine:3".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: Default::default(),
        resources: Default::default(),
        executor_constraint: None,
        inputs: vec![aruna_core::structs::execution::job::InputSelection {
            source: aruna_core::structs::execution::job::InputSource::S3 {
                bucket: "data".to_string(),
                key: "in.txt".to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: "in.txt".to_string(),
            mode: aruna_core::structs::execution::job::InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        }],
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    };

    let error = validate_execution(&mut spec, WorkspaceMode::None, None).unwrap_err();

    assert!(
        matches!(error, SubmitJobError::InvalidWorkspace(message) if message.contains("in.txt"))
    );
    spec.inputs[0].container_path = Some("/in.txt".to_string());
    assert!(validate_execution(&mut spec, WorkspaceMode::None, None).is_ok());
}

#[test]
fn refuses_shared_destination() {
    // An explicit output inside the workspace bucket and a workspace output
    // naming the same key would reserve one version twice.
    let mut spec = ExecutionSpec {
        group_id: Ulid::from_bytes([3u8; 16]),
        name: None,
        description: None,
        tags: Default::default(),
        image: "alpine:3".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: Default::default(),
        resources: Default::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: vec![aruna_core::structs::execution::job::OutputSelection {
            container_path: "/out/result.txt".to_string(),
            path_prefix: None,
            destination: OutputDestination::S3 {
                bucket: "shared".to_string(),
                key: "result.txt".to_string(),
            },
            destination_node_id: None,
            name: None,
            description: None,
        }],
        workspace_outputs: vec![aruna_core::structs::execution::job::WorkspaceOutput {
            container_path: "/result.txt".to_string(),
            dest_key: "result.txt".to_string(),
        }],
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    };

    let error = validate_execution(&mut spec, WorkspaceMode::Existing, Some("shared")).unwrap_err();

    assert!(
        matches!(error, SubmitJobError::InvalidWorkspace(message) if message.contains("shared/result.txt"))
    );
    spec.workspace_outputs[0].dest_key = "other.txt".to_string();
    assert!(validate_execution(&mut spec, WorkspaceMode::Existing, Some("shared")).is_ok());

    // Nextflow sends every glob with the task directory as its url.
    let mut glob = spec.file_outputs[0].clone();
    glob.container_path = "/out/*.html".to_string();
    glob.path_prefix = Some("/out".to_string());
    spec.file_outputs = vec![glob.clone(), glob];
    spec.file_outputs[1].container_path = "/out/*.zip".to_string();
    assert!(validate_execution(&mut spec, WorkspaceMode::Existing, Some("shared")).is_ok());
}

#[tokio::test]
async fn dedup_lookup_matches() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    let payload = JobPayload::Probe {
        steps: 1,
        step_sleep_ms: 0,
        fail_at: None,
        panic_at: None,
        cleanup_marker: None,
    };
    let mut record = JobRecord::new(
        JobId::from_bytes([3u8; 16]),
        payload.clone(),
        owner,
        node_id(),
        1,
        1,
        Some(user_dedup_key(owner, "key")),
    );
    record.state = JobState::Succeeded;
    record.finished_at_ms = Some(1);
    insert_job(&context, &record).await.unwrap();

    let lookup = DedupLookupOperation::new(owner, "key", payload.plan_digest());
    let found = drive(lookup, &context).await.unwrap().unwrap();
    assert_eq!(found.job_id, record.job_id);
    assert!(!found.created);
    assert!(matches!(
        drive(DedupLookupOperation::new(owner, "key", [9u8; 32]), &context).await,
        Err(SubmitJobError::JobPlanConflict { existing_job_id })
            if existing_job_id == record.job_id
    ));
}

#[tokio::test]
async fn tombstone_preserves_gone() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    let job_id = JobId::from_bytes([3u8; 16]);
    drive(preserve_artifact_tombstone(job_id, owner, 10), &context)
        .await
        .unwrap();

    assert!(matches!(
        drive(OwnedArtifactOperation::new(owner, job_id, 9), &context).await,
        Ok(ArtifactLookup::Gone)
    ));
    assert!(matches!(
        drive(
            OwnedArtifactOperation::new(
                UserId::new(Ulid::from_bytes([4u8; 16]), RealmId([1u8; 32])),
                job_id,
                9,
            ),
            &context,
        )
        .await,
        Ok(ArtifactLookup::NotFound)
    ));
    assert!(matches!(
        drive(OwnedArtifactOperation::new(owner, job_id, 10), &context).await,
        Ok(ArtifactLookup::NotFound)
    ));
}

#[tokio::test]
async fn report_expiry_hides() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: None,
        compute_handle: None,
        outbound_handle: None,
    };
    let realm_id = RealmId([1u8; 32]);
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), realm_id);
    let job_id = JobId::from_bytes([3u8; 16]);
    let mut record = JobRecord::new(
        job_id,
        JobPayload::ImportRoCrate(ImportRoCrateSpec {
            auth_context: AuthContext {
                user_id: owner,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            source: ImportRoCrateSource::Upload {
                upload_id: Ulid::from_bytes([4u8; 16]),
            },
            target: ImportRoCrateTarget {
                bucket: "target".to_string(),
                prefix: String::new(),
            },
            metadata: ImportMetadataTarget {
                group_id: Ulid::from_bytes([5u8; 16]),
                path: "crate".to_string(),
                public: false,
            },
            limits: RoCrateLimits::default(),
            document_id: Ulid::from_bytes([6u8; 16]),
        }),
        owner,
        node_id(),
        1,
        1,
        None,
    );
    record.state = JobState::Succeeded;
    record.finished_at_ms = Some(1);
    record.retention_ms = 1;
    record.report_digest = Some([7u8; 32]);
    insert_job(&context, &record).await.unwrap();

    assert!(matches!(
        read_owned_report(
            &context,
            owner,
            job_id,
            ReportQuery {
                expected_digest: None,
                last_key: None,
                limit: 1,
            },
        )
        .await,
        Ok(JobReportLookup::NotFound)
    ));
}

#[tokio::test]
async fn internal_access_hidden() {
    let dir = tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let context = DriverContext {
        storage_handle: storage.clone(),
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        graph_handle: None,
        task_handle: Some(TaskHandle::new()),
        compute_handle: None,
        outbound_handle: None,
    };
    let owner = UserId::new(Ulid::from_bytes([2u8; 16]), RealmId([1u8; 32]));
    let runtime = JobsRuntime::new();
    for (job_id, payload) in [
        (
            JobId::from_bytes([0xC3; 16]),
            JobPayload::WriteRunCrate {
                for_job: JobId::from_bytes([0xC4; 16]),
            },
        ),
        (
            JobId::from_bytes([0xC5; 16]),
            JobPayload::TerminalCleanup {
                for_job: JobId::from_bytes([0xC6; 16]),
                attempt: None,
                access_key: "access".to_string(),
            },
        ),
    ] {
        let record = JobRecord::new(job_id, payload, owner, node_id(), 1_000, 1_000, None);
        insert_job(&context, &record).await.unwrap();

        assert!(
            read_owned_job(&context, owner, job_id)
                .await
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            cancel_owned_job(&context, &runtime, owner, job_id)
                .await
                .unwrap(),
            CancelJobOutcome::NotFound
        ));
        let stored = read_job_record(&context, job_id, None)
            .await
            .unwrap()
            .unwrap();
        assert!(!stored.cancel_requested);
        assert_eq!(stored.state, JobState::Queued);
    }
}
