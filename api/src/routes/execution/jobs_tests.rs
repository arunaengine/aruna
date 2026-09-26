//! Tests the job routes for listing, report paging, artifact headers, delete and cancel.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use crate::jobs::{
    JobRequestError, MAX_OUTPUT_PREFIXES, mount_permission_path, native_input, native_outputs,
    output_buckets, validate_output_prefixes, workspace_request,
};
use aruna_core::UserId;
use aruna_core::id::NodeId;
use aruna_core::structs::checksum::HASH_BLAKE3;
use aruna_core::structs::execution::job::{
    ArtifactRef, ExportOmissionCounts, ExportRoCrateResult, ExportRoCrateSpec,
    ImportMetadataTarget, ImportReportDetail, ImportRoCrateResult, ImportRoCrateSource,
    ImportRoCrateSpec, ImportRoCrateTarget, JobPayload, JobProgress, JobResultPayload, ReasonCode,
    RoCrateLimits,
};
use aruna_core::structs::execution::job::{
    CollisionPolicy, ComputeResources, ExecutionSpec, OutputDestination, WorkspaceMode,
};
use aruna_core::structs::identity::auth::{NodeCapabilities, PathRestriction, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::record::FIRST_GRANTABLE_HANDLE;
use aruna_core::structs::storage::blob::{BackendLocation, BackendRef};
use aruna_core::structured_id::{BucketId, PlacementHandle};
use aruna_operations::driver::DriverContext;
use aruna_operations::jobs::command::{
    ExecutionInput, ExecutionOutput, ExecutionTarget, InputMode,
    WorkspaceMode as CommandWorkspaceMode, WorkspaceSpec,
};
use aruna_operations::jobs::runtime::JobsRuntime;
use aruna_operations::jobs::store::{
    ClaimOutcome, claim_job, complete_job, insert_job, put_job_entry, transition_to_running,
};
use aruna_storage::FjallStorage;
use std::collections::HashMap;
use std::time::SystemTime;
use tempfile::TempDir;
use ulid::Ulid;

/// One reduced family with a duplicate success and a locally exhausted view.
fn family_report_fixture() -> FamilyReport {
    use aruna_core::jobs::{JobKind, JobStatusView};
    use aruna_core::structs::execution::job::{
        EffectiveResources, ExecutionSpec, JobAdmissionRecord, JobRetryPolicy, LogicalJobSpec,
        LogicalJobState, OutputObject, SubmissionId,
    };
    use aruna_core::structs::placement::record::PlacementRef;

    let created_by = user(2);
    let job_id = JobId::from_bytes([3u8; 16]);
    let submission_id = SubmissionId([5u8; 32]);
    let resources = EffectiveResources {
        cpu_cores: 1,
        ram_bytes: 1,
        disk_bytes: 0,
        max_walltime_ms: 1_000,
        preemptible: false,
    };
    let payload = ExecutionSpec {
        group_id: Ulid::from_bytes([6u8; 16]),
        name: None,
        description: None,
        tags: BTreeMap::new(),
        image: "img".to_string(),
        entrypoint: None,
        command: vec!["true".to_string()],
        workdir: None,
        env: BTreeMap::new(),
        resources: aruna_core::structs::execution::job::ComputeResources::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    };
    FamilyReport {
        job: JobStatusView {
            job_id,
            created_by,
            kind: JobKind::Execution,
            state: JobState::Indeterminate,
            attempts: 2,
            cancel_requested: false,
            created_at_ms: 10,
            updated_at_ms: 20,
            finished_at_ms: None,
            progress: JobProgress::new("phases"),
            last_error: None,
            result: None,
            workspace_bucket: None,
            workspace_mode: WorkspaceMode::None,
            locally_exhausted: false,
            session_runtime: None,
        },
        spec: LogicalJobSpec {
            submission_id,
            job_id,
            origin_node_id: node_id(),
            ingress_node_id: node_id(),
            realm_id: realm(),
            group_id: payload.group_id,
            created_by,
            created_at_ms: 10,
            retention_ms: aruna_core::structs::execution::job::RETENTION_MS,
            payload,
            request_digest: [7u8; 32],
            spec_digest: [8u8; 32],
            resources,
            retry: JobRetryPolicy {
                launches_per_witness: 3,
            },
            admission: JobAdmissionRecord {
                submission_id,
                request_digest: [7u8; 32],
                job_id,
                group_id: Ulid::from_bytes([6u8; 16]),
                admitting_node_id: node_id(),
                membership_generation: 0,
                resources,
                admitted_at_ms: 10,
            },
            captured_inputs: Vec::new(),
            output_policies: Vec::new(),
            placement: PlacementRef::NIL,
        },
        submission_id,
        request_digest: [7u8; 32],
        canonical_job_id: job_id,
        aliases: vec![job_id, JobId::from_bytes([4u8; 16])],
        conflicts: 1,
        state: LogicalJobState::Indeterminate,
        canonical_execution_id: None,
        canonical_result: None,
        executions: 2,
        execution_list: Vec::new(),
        started_at_ms: Some(15),
        duplicate_successes: 1,
        outputs: vec![OutputObject {
            node_id: node_id(),
            bucket: "dest".to_string(),
            key: "out/r.txt".to_string(),
            version_id: Ulid::from_bytes([9u8; 16]),
            execution_id: Ulid::from_bytes([10u8; 16]),
            container_path: "/out/r.txt".to_string(),
            size: 3,
            digest: None,
        }],
        output_endpoints: BTreeMap::from([(node_id(), "https://s3.example".to_string())]),
        revision: 4,
        digest: [11u8; 32],
        cancel_requested: false,
        responder: Some(node_id()),
        partial: true,
        locally_exhausted: true,
        plan: None,
    }
}

#[test]
fn partitioned_view_marked() {
    // A partitioned read must name its responder and say that it is local
    // and exhausted here, without ever reading as a converged failure.
    let response = family_response(&family_report_fixture());

    assert_eq!(response.logical_state, "indeterminate");
    assert!(response.locally_exhausted);
    assert!(response.partial);
    assert_eq!(
        response.responder_node_id.as_deref(),
        Some(&*node_id().to_string())
    );
    assert_eq!(response.alias_count, 2);
    assert_eq!(response.conflict_count, 1);
    assert_eq!(response.duplicate_successes, 1);
    assert_eq!(response.revision, 4);
    assert_eq!(response.projection_digest.len(), 64);
    assert!(response.canonical_execution_id.is_none());
}

#[test]
fn outputs_preserve_versions() {
    // The exact VersionId and its producing execution are the identity of a
    // job output; the object's current version is a different question.
    let response = family_response(&family_report_fixture());
    let mut result = Some(serde_json::json!({ "outputs": [] }));
    bind_output_routes(&mut result, &response.outputs).expect("routes bind");

    assert_eq!(response.outputs.len(), 1);
    assert_eq!(
        response.outputs[0].version_id,
        Ulid::from_bytes([9u8; 16]).to_string()
    );
    assert_eq!(
        response.outputs[0].execution_id,
        Ulid::from_bytes([10u8; 16]).to_string()
    );
    assert_eq!(response.outputs[0].bucket, "dest");
    assert_eq!(
        result.as_ref().unwrap()["outputs"][0]["endpoint_url"],
        "https://s3.example"
    );
}

#[test]
fn reads_without_endpoint() {
    // A node info document this responder lacks must not make a succeeded
    // family unreadable; only the address of that output is unknown.
    let mut report = family_report_fixture();
    report.output_endpoints.clear();

    let response = family_response(&report);
    assert_eq!(response.outputs.len(), 1);
    assert!(response.outputs[0].endpoint_url.is_none());
}

fn realm() -> RealmId {
    RealmId([1u8; 32])
}

fn node_id() -> NodeId {
    iroh::SecretKey::from_bytes(&[7u8; 32]).public()
}

fn user(byte: u8) -> UserId {
    UserId::new(Ulid::from_bytes([byte; 16]), realm())
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

fn auth_for(user_id: UserId) -> Option<AuthContext> {
    Some(AuthContext {
        user_id,
        realm_id: realm(),
        path_restrictions: None,
        session: None,
    })
}

fn restricted_auth_for(user_id: UserId) -> Option<AuthContext> {
    Some(AuthContext {
        user_id,
        realm_id: realm(),
        path_restrictions: Some(vec![PathRestriction {
            pattern: "/realm/g/group/data/**".to_string(),
            permission: Permission::READ,
        }]),
        session: None,
    })
}

async fn build_state() -> (TempDir, Arc<ServerState>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let state = ServerState::new(
        ctx,
        realm(),
        node_id(),
        NodeCapabilities::user_node(realm()).unwrap(),
        false,
        None,
        JobsRuntime::new(),
    )
    .await;
    (dir, Arc::new(state))
}

fn job_for(job_id: JobId, owner: UserId, created_at_ms: u64) -> JobRecord {
    JobRecord::new(
        job_id,
        JobPayload::Probe {
            steps: 1,
            step_sleep_ms: 0,
            fail_at: None,
            panic_at: None,
            cleanup_marker: None,
        },
        owner,
        node_id(),
        created_at_ms,
        created_at_ms,
        None,
    )
}

fn import_job(job_id: JobId, owner: UserId) -> JobRecord {
    JobRecord::new(
        job_id,
        JobPayload::ImportRoCrate(ImportRoCrateSpec {
            auth_context: auth_for(owner).unwrap(),
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
        node_id(),
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

#[test]
fn decodes_session_report() {
    use aruna_core::structs::execution::job::{SessionReportDetail, SessionReportRow};

    let row = SessionReportRow {
        entry_key: "input/0000".to_string(),
        detail: SessionReportDetail::Input {
            dest_key: "data/input.txt".to_string(),
            bytes: 12,
            blake3: "hash".to_string(),
            source_node_id: "source".to_string(),
            version_id: "source-version".to_string(),
        },
    };
    let bytes = postcard::to_allocvec(&row).unwrap();
    let decoded = decode_report_row(JobKind::Execution, b"input/0000", &bytes).unwrap();
    assert_eq!(decoded["code"], "input");
    assert!(
        decoded["message"]
            .as_str()
            .unwrap()
            .contains("source-version")
    );
    assert_eq!(decoded["detail"]["Input"]["version_id"], "source-version");
    assert!(decode_report_row(JobKind::Execution, b"other", &bytes).is_err());
}

#[test]
fn decodes_system_key() {
    let owner = user(2);
    let payload = import_job(JobId::from_bytes([9u8; 16]), owner).payload;
    for entry_key in [
        "signature/ro-crate-metadata.json.minisig",
        "warning/00000000",
        "failure/write",
    ] {
        let row = report_row(entry_key);
        let value = postcard::to_allocvec(&row).unwrap();
        let mut stored_key = vec![SYSTEM_ENTRY_PREFIX];
        stored_key.extend_from_slice(entry_key.as_bytes());
        let decoded = decode_report_row(JobKind::from(&payload), &stored_key, &value).unwrap();
        assert_eq!(decoded["entry_key"], entry_key);
    }
}

fn export_job(job_id: JobId, owner: UserId, expires_at_ms: u64) -> JobRecord {
    let document_id = Ulid::from_bytes([6u8; 16]);
    let blake3 = [7u8; 32];
    let mut hashes = HashMap::new();
    hashes.insert(HASH_BLAKE3.to_string(), blake3.to_vec());
    let artifact = ArtifactRef {
        location: BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/tmp".to_string(),
            storage_bucket: "hidden".to_string(),
            backend_path: format!("_jobs/{job_id}/artifact.zip"),
            ulid: Ulid::from_bytes([8u8; 16]),
            compressed: false,
            encrypted: false,
            created_by: owner,
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 5,
            hashes,
        },
        blake3,
        size: 5,
        expires_at_ms,
    };
    let digest = *blake3::hash(&[]).as_bytes();
    let mut record = JobRecord::new(
        job_id,
        JobPayload::ExportRoCrate(ExportRoCrateSpec {
            destination: None,
            auth_context: auth_for(owner).unwrap(),
            document_id,
            limits: RoCrateLimits::default(),
        }),
        owner,
        node_id(),
        1_000,
        1_000,
        None,
    );
    record.state = JobState::Succeeded;
    record.finished_at_ms = Some(2_000);
    record.report_digest = Some(digest);
    record.result = Some(JobResultPayload::ExportRoCrate(ExportRoCrateResult {
        repository: None,
        artifact: Some(artifact),
        included: 1,
        omitted: ExportOmissionCounts::default(),
        report_digest: digest,
    }));
    record
}

async fn finish_report(state: &ServerState, job_id: JobId, owner: UserId) -> [u8; 32] {
    let context = state.get_ctx();
    let storage = &context.storage_handle;
    insert_job(storage, &import_job(job_id, owner))
        .await
        .unwrap();
    let ClaimOutcome::Claimed(record) = claim_job(storage, job_id, node_id(), 2_000).await.unwrap()
    else {
        panic!("job was not claimed")
    };
    let token = record.claim.unwrap().claim_token;
    put_job_entry(storage, job_id, token, b"b", &report_row("b"))
        .await
        .unwrap();
    put_job_entry(storage, job_id, token, b"a", &report_row("a"))
        .await
        .unwrap();
    transition_to_running(storage, job_id, token, 2_500)
        .await
        .unwrap();
    complete_job(
        storage,
        job_id,
        token,
        JobResultPayload::ImportRoCrate(ImportRoCrateResult {
            document_id: Some(Ulid::from_bytes([5u8; 16])),
            entries_total: 2,
            imported: 2,
            unlisted: 0,
            failed: 0,
            report_digest: [0u8; 32],
        }),
        JobProgress {
            current: 2,
            total: Some(2),
            unit: "entries".to_string(),
        },
        aruna_core::time::unix_timestamp_millis(),
    )
    .await
    .unwrap()
    .report_digest
    .unwrap()
}

async fn response_json<T: serde::de::DeserializeOwned>(response: Response) -> T {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn list_newest_first() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    for seq in 1..=3u64 {
        insert_job(
            &state.get_ctx().storage_handle,
            &job_for(job_id(seq), owner, seq * 1000),
        )
        .await
        .unwrap();
    }

    let (_, Json(page1)) = list_jobs(
        State(state.clone()),
        Extension(auth_for(owner)),
        Query(ListJobsQuery {
            limit: Some(2),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(page1.jobs.len(), 2);
    // Newest first: seq 3 then seq 2.
    assert_eq!(page1.jobs[0].job_id, job_id(3).to_string());
    let cursor = page1.next_cursor.clone().expect("cursor for next page");

    let (_, Json(page2)) = list_jobs(
        State(state.clone()),
        Extension(auth_for(owner)),
        Query(ListJobsQuery {
            limit: Some(2),
            cursor: Some(cursor),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(page2.jobs.len(), 1);
    assert_eq!(page2.jobs[0].job_id, job_id(1).to_string());
    assert!(page2.next_cursor.is_none());
}

#[tokio::test]
async fn report_pages_frozen() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let job_id = JobId::from_bytes([9u8; 16]);
    let digest = finish_report(&state, job_id, owner).await;

    let first = get_job_report(
        State(state.clone()),
        Extension(auth_for(owner)),
        Extension(None),
        Path(job_id.to_string()),
        Query(ReportQuery {
            limit: Some(1),
            cursor: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first: JobReportResponse = response_json(first).await;
    assert_eq!(first.rows[0]["entry_key"], "a");
    assert_eq!(first.report_digest, hex::encode(digest));

    let second = get_job_report(
        State(state.clone()),
        Extension(auth_for(owner)),
        Extension(None),
        Path(job_id.to_string()),
        Query(ReportQuery {
            limit: Some(1),
            cursor: first.next_cursor,
        }),
    )
    .await
    .unwrap();
    let second: JobReportResponse = response_json(second).await;
    assert_eq!(second.rows[0]["entry_key"], "b");
    assert!(second.next_cursor.is_none());

    let conflict = ReportCursor {
        job_id,
        report_digest: [0u8; 32],
        last_key: b"a".to_vec(),
    };
    let conflict = URL_SAFE_NO_PAD.encode(postcard::to_allocvec(&conflict).unwrap());
    let response = get_job_report(
        State(state.clone()),
        Extension(auth_for(owner)),
        Extension(None),
        Path(job_id.to_string()),
        Query(ReportQuery {
            limit: None,
            cursor: Some(conflict),
        }),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let foreign = get_job_report(
        State(state),
        Extension(auth_for(user(3))),
        Extension(None),
        Path(job_id.to_string()),
        Query(ReportQuery::default()),
    )
    .await;
    assert!(matches!(foreign, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn report_pending_typed() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let job_id = JobId::from_bytes([10u8; 16]);
    insert_job(&state.get_ctx().storage_handle, &import_job(job_id, owner))
        .await
        .unwrap();

    let response = get_job_report(
        State(state),
        Extension(auth_for(owner)),
        Extension(None),
        Path(job_id.to_string()),
        Query(ReportQuery::default()),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body: ReportPendingResponse = response_json(response).await;
    assert_eq!(body.code, "report_pending");
    assert_eq!(body.state, "queued");
}

#[test]
fn submit_conflict_typed() {
    let existing_job_id = JobId::from_bytes([15u8; 16]);
    let error = map_submit_error(
        aruna_operations::jobs::submit::SubmitJobError::JobPlanConflict { existing_job_id },
    );
    assert!(matches!(
        error,
        ServerError::JobPlanConflict(message)
            if message.contains(&existing_job_id.to_string())
    ));
}

#[test]
fn report_openapi_union() {
    let openapi = serde_json::to_value(crate::openapi::ApiDoc::openapi()).unwrap();
    let schema = &openapi["paths"]["/compute/jobs/{job_id}/report"]["get"]["responses"]["404"]["content"]
        ["application/json"]["schema"];
    assert_eq!(
        schema["$ref"],
        "#/components/schemas/ReportUnavailableResponse"
    );
    let variants = openapi["components"]["schemas"]["ReportUnavailableResponse"]["oneOf"]
        .as_array()
        .unwrap();
    assert!(
        variants
            .iter()
            .any(|variant| variant["$ref"] == "#/components/schemas/ReportPendingResponse")
    );
    assert!(
        variants
            .iter()
            .any(|variant| variant["$ref"] == "#/components/schemas/ErrorResponse")
    );
}

#[tokio::test]
async fn artifact_head_headers() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let job_id = JobId::from_bytes([11u8; 16]);
    insert_job(
        &state.get_ctx().storage_handle,
        &export_job(
            job_id,
            owner,
            aruna_core::time::unix_timestamp_millis() + 60_000,
        ),
    )
    .await
    .unwrap();

    let response = head_job_artifact(
        State(state.clone()),
        Extension(auth_for(owner)),
        Extension(None),
        Path(job_id.to_string()),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[ACCEPT_RANGES], "bytes");
    assert_eq!(response.headers()[CONTENT_LENGTH], "5");
    assert_eq!(
        response.headers()[ETAG].to_str().unwrap(),
        format!("\"{}\"", hex::encode([7u8; 32]))
    );
    let document_id = Ulid::from_bytes([6u8; 16]);
    let disposition = response.headers()[CONTENT_DISPOSITION].to_str().unwrap();
    assert!(disposition.contains(&format!("filename=\"{document_id}.zip\"")));
    assert!(disposition.contains(&format!("filename*=UTF-8''{document_id}%2Ezip")));

    let foreign = head_job_artifact(
        State(state),
        Extension(auth_for(user(3))),
        Extension(None),
        Path(job_id.to_string()),
        HeaderMap::new(),
    )
    .await;
    assert!(matches!(foreign, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn artifact_expiry_gone() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let job_id = JobId::from_bytes([12u8; 16]);
    insert_job(
        &state.get_ctx().storage_handle,
        &export_job(job_id, owner, 1),
    )
    .await
    .unwrap();

    let response = head_job_artifact(
        State(state),
        Extension(auth_for(owner)),
        Extension(None),
        Path(job_id.to_string()),
        HeaderMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::GONE);
}

#[test]
fn range_parses_single() {
    let mut headers = HeaderMap::new();
    headers.insert(RANGE, HeaderValue::from_static("bytes=2-4"));
    assert_eq!(
        range_request(&headers),
        Ok(Some(ObjectRangeRequest::StartEnd { start: 2, end: 4 }))
    );
    headers.insert(RANGE, HeaderValue::from_static("bytes=-3"));
    assert_eq!(
        range_request(&headers),
        Ok(Some(ObjectRangeRequest::Suffix { length: 3 }))
    );
    headers.insert(RANGE, HeaderValue::from_static("bytes=2-3,5-6"));
    assert_eq!(range_request(&headers), Err(()));
}

#[test]
fn forwarded_auth_bounds() {
    assert!(forwarded_job_auth(None).unwrap().is_none());
    let accepted = ValidatedBearer::new_for_test("a".repeat(4_096));
    assert!(forwarded_job_auth(Some(accepted)).unwrap().is_some());
    let rejected = ValidatedBearer::new_for_test("a".repeat(4_097));
    assert!(matches!(
        forwarded_job_auth(Some(rejected)),
        Err(ServerError::BadRequest)
    ));
}

#[tokio::test]
async fn urls_are_absolute() {
    let (_dir, state) = build_state().await;
    state
        .register_rest_public(
            "127.0.0.1:3000".parse().unwrap(),
            Some("https://owner.example/"),
        )
        .await;
    let job_id = JobId::from_bytes([13u8; 16]);
    let urls = job_urls(&state, job_id).await.unwrap();
    assert_eq!(urls.owner_node_url, "https://owner.example/api/v1");
    assert_eq!(
        urls.status_url,
        format!("https://owner.example/api/v1/compute/jobs/{job_id}")
    );
    assert_eq!(
        urls.report_url,
        format!("https://owner.example/api/v1/compute/jobs/{job_id}/report")
    );
    assert_eq!(
        urls.artifact_url,
        format!("https://owner.example/api/v1/compute/jobs/{job_id}/artifacts/rocrate")
    );
}

#[tokio::test]
async fn foreign_not_found() {
    let (_dir, state) = build_state().await;
    let job_id = JobId::from_bytes([9u8; 16]);
    insert_job(
        &state.get_ctx().storage_handle,
        &job_for(job_id, user(2), 1000),
    )
    .await
    .unwrap();

    let result = get_job(
        State(state.clone()),
        Extension(auth_for(user(3))),
        Extension(None),
        Path(job_id.to_string()),
    )
    .await;
    assert!(matches!(result, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn delete_job_rules() {
    // Only the owner removes a finished run; a live one answers 409 and a
    // second delete answers 404.
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let storage = &state.get_ctx().storage_handle;
    let finished = JobId::from_bytes([0xA1; 16]);
    let running = JobId::from_bytes([0xA2; 16]);
    let mut record = job_for(finished, owner, 1000);
    record.payload = JobPayload::Execution(ExecutionSpec {
        group_id: Ulid::from_bytes([5u8; 16]),
        name: None,
        description: None,
        tags: BTreeMap::new(),
        image: "alpine:3".to_string(),
        entrypoint: None,
        command: Vec::new(),
        workdir: None,
        env: BTreeMap::new(),
        resources: ComputeResources::default(),
        executor_constraint: None,
        inputs: Vec::new(),
        file_outputs: Vec::new(),
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: CollisionPolicy::default(),
    });
    record.state = JobState::Succeeded;
    record.finished_at_ms = Some(2000);
    insert_job(storage, &record).await.unwrap();
    let mut live = record.clone();
    live.job_id = running;
    live.state = JobState::Running;
    live.finished_at_ms = None;
    insert_job(storage, &live).await.unwrap();

    let call = |who: UserId, id: JobId| {
        delete_job(
            State(state.clone()),
            Extension(auth_for(who)),
            Path(id.to_string()),
        )
    };
    assert!(matches!(
        call(user(3), finished).await,
        Err(ServerError::NotFound)
    ));
    assert!(matches!(
        call(owner, running).await,
        Err(ServerError::Conflict(_))
    ));
    assert_eq!(call(owner, finished).await.unwrap(), StatusCode::NO_CONTENT);
    assert!(matches!(
        call(owner, finished).await,
        Err(ServerError::NotFound)
    ));
    let result = get_job(
        State(state.clone()),
        Extension(auth_for(owner)),
        Extension(None),
        Path(finished.to_string()),
    )
    .await;
    assert!(matches!(result, Err(ServerError::NotFound)));
}

#[tokio::test]
async fn cancel_is_idempotent() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let job_id = JobId::from_bytes([9u8; 16]);
    // A prior run keeps repeated cancellation on the live, cancel-requested path.
    let mut record = job_for(job_id, owner, 1000);
    record.has_run = true;
    insert_job(&state.get_ctx().storage_handle, &record)
        .await
        .unwrap();

    let (status, _) = cancel_job(
        State(state.clone()),
        Extension(auth_for(owner)),
        Extension(None),
        Path(job_id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::ACCEPTED);

    // Repeated cancel of a still-live job stays 202.
    let (status, Json(body)) = cancel_job(
        State(state.clone()),
        Extension(auth_for(owner)),
        Extension(None),
        Path(job_id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.cancel_requested);
}

#[tokio::test]
async fn cancel_terminal_noop() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let job_id = JobId::from_bytes([4u8; 16]);
    let mut record = job_for(job_id, owner, 1000);
    record.state = JobState::Succeeded;
    record.finished_at_ms = Some(2000);
    insert_job(&state.get_ctx().storage_handle, &record)
        .await
        .unwrap();

    let (status, _) = cancel_job(
        State(state.clone()),
        Extension(auth_for(owner)),
        Extension(None),
        Path(job_id.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::OK);
}

#[test]
fn openapi_has_jobs() {
    let openapi = crate::openapi::ApiDoc::openapi();
    assert!(openapi.paths.paths.contains_key("/compute/jobs"));
    assert!(openapi.paths.paths.contains_key("/compute/jobs/{job_id}"));
    assert!(
        openapi
            .paths
            .paths
            .contains_key("/compute/jobs/{job_id}/cancel")
    );
    assert!(
        openapi
            .paths
            .paths
            .contains_key("/compute/jobs/{job_id}/report")
    );
    assert!(
        openapi
            .paths
            .paths
            .contains_key("/compute/jobs/{job_id}/artifacts/rocrate")
    );
}

// A path-restricted (delegated) token must not reach any user-scoped job surface.
#[tokio::test]
async fn restricted_token_rejected() {
    let (_dir, state) = build_state().await;
    let owner = user(2);
    let job_id = JobId::from_bytes([9u8; 16]);
    insert_job(
        &state.get_ctx().storage_handle,
        &job_for(job_id, owner, 1000),
    )
    .await
    .unwrap();

    let list = list_jobs(
        State(state.clone()),
        Extension(restricted_auth_for(owner)),
        Query(ListJobsQuery::default()),
    )
    .await;
    assert!(matches!(list, Err(ServerError::Forbidden)));

    let get = get_job(
        State(state.clone()),
        Extension(restricted_auth_for(owner)),
        Extension(None),
        Path(job_id.to_string()),
    )
    .await;
    assert!(matches!(get, Err(ServerError::Forbidden)));

    let cancel = cancel_job(
        State(state.clone()),
        Extension(restricted_auth_for(owner)),
        Extension(None),
        Path(job_id.to_string()),
    )
    .await;
    assert!(matches!(cancel, Err(ServerError::Forbidden)));
}

fn local_request() -> SubmitExecutionRequest {
    SubmitExecutionRequest {
        group_id: Ulid::from_bytes([5u8; 16]).to_string(),
        name: None,
        description: None,
        image: "alpine:3".to_string(),
        runtime: None,
        session_idle_ms: None,
        session_mount: None,
        entrypoint: None,
        command: vec!["true".to_string()],
        env: BTreeMap::new(),
        tags: BTreeMap::new(),
        workdir: None,
        cpu_cores: None,
        ram_bytes: None,
        max_walltime_ms: None,
        executor_constraint: None,
        inputs: Vec::new(),
        outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
        idempotency_key: None,
        workspace: None,
        target: Some(super::ExecutionTarget::Local),
    }
}

async fn management_state() -> (TempDir, Arc<ServerState>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
    let ctx = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: None,
        blob_handle: None,
        metadata_handle: None,
        task_handle: None,
        compute_handle: None,
    });
    let capabilities =
        NodeCapabilities::management_node(aruna_core::keys::generate_signing_key()).unwrap();
    let state = ServerState::new(
        ctx,
        realm(),
        node_id(),
        capabilities,
        false,
        None,
        JobsRuntime::new(),
    )
    .await;
    (dir, Arc::new(state))
}

async fn enroll_device(state: &ServerState, owner: UserId) {
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
    let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
    config.seed_default_placement();
    config.ensure_node(node_id(), RealmNodeKind::User { owner });
    let actor = Actor {
        node_id: node_id(),
        user_id: UserId::nil(realm()),
        realm_id: realm(),
    };
    let bytes = config.to_bytes(&actor).expect("config serializes");
    let event = state
        .get_ctx()
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::Write {
            key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
            key: realm().as_bytes().to_vec().into(),
            value: bytes.into(),
            txn_id: None,
        })
        .await;
    assert!(matches!(
        event,
        aruna_core::events::Event::Storage(aruna_core::events::StorageEvent::WriteResult { .. })
    ));
}

#[tokio::test]
async fn local_needs_device() {
    // The realm never runs a job locally on behalf of a target it has not
    // enrolled as somebody's machine.
    let (_dir, state) = management_state().await;

    let result = submit_job(
        State(state),
        Extension(auth_for(user(2))),
        Extension(None),
        Json(local_request()),
    )
    .await;

    assert!(matches!(result, Err(ServerError::BadRequestMessage(_))));
}

#[tokio::test]
async fn device_checks_group() {
    // A device caches the group documents, so its group check is the realm's
    // own: an owner holding no grant is refused here as on a realm node.
    let (_dir, state) = build_state().await;
    enroll_device(&state, user(2)).await;

    for target in [None, Some(super::ExecutionTarget::Local)] {
        let result = submit_job(
            State(state.clone()),
            Extension(auth_for(user(2))),
            Extension(None),
            Json(SubmitExecutionRequest {
                target,
                ..local_request()
            }),
        )
        .await;
        assert!(
            matches!(result, Err(ServerError::Forbidden)),
            "{target:?} must run the local group check"
        );
    }
}

#[tokio::test]
async fn local_refuses_stranger() {
    let (_dir, state) = build_state().await;
    enroll_device(&state, user(2)).await;

    let result = submit_job(
        State(state),
        Extension(auth_for(user(3))),
        Extension(None),
        Json(local_request()),
    )
    .await;

    assert!(matches!(result, Err(ServerError::Forbidden)));
}

#[test]
fn local_names_holder() {
    // Only a local run may name the holder: the planner stores it otherwise.
    let input = ExecutionInput {
        bucket: "src".to_string(),
        key: "data.csv".to_string(),
        version_id: Some(Ulid::from_bytes([4u8; 16]).to_string()),
        source_node_id: Some(node_id().to_string()),
        dest_key: "data.csv".to_string(),
        container_path: None,
        mode: InputMode::Snapshot,
    };

    assert!(matches!(
        native_input(input.clone(), ExecutionTarget::Realm),
        Err(JobRequestError::BadRequestMessage(_))
    ));
    assert_eq!(
        native_input(input, ExecutionTarget::Local)
            .unwrap()
            .source_node_id,
        Some(node_id())
    );
}

#[tokio::test]
async fn rejects_huge_ram() {
    // ram_bytes above i64::MAX would wrap negative in the Docker backend.
    let (_dir, state) = build_state().await;
    for ram_bytes in [u64::MAX, i64::MAX as u64 + 1, 0] {
        let request = SubmitExecutionRequest {
            group_id: Ulid::from_bytes([5u8; 16]).to_string(),
            name: None,
            description: None,
            image: "alpine:3".to_string(),
            runtime: None,
            session_idle_ms: None,
            session_mount: None,
            entrypoint: None,
            command: vec!["true".to_string()],
            env: BTreeMap::new(),
            tags: BTreeMap::new(),
            workdir: None,
            cpu_cores: None,
            ram_bytes: Some(ram_bytes),
            max_walltime_ms: None,
            executor_constraint: None,
            inputs: Vec::new(),
            outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
            idempotency_key: None,
            workspace: None,
            target: None,
        };
        let result = submit_job(
            State(state.clone()),
            Extension(auth_for(user(2))),
            Extension(None),
            Json(request),
        )
        .await;
        assert!(
            matches!(result, Err(ServerError::BadRequest)),
            "ram_bytes {ram_bytes} must be rejected"
        );
    }
}

#[test]
fn maps_native_input() {
    // Missing container_path defaults to /inputs/<dest_key>.
    let input = ExecutionInput {
        bucket: "src".to_string(),
        key: "data.csv".to_string(),
        version_id: None,
        source_node_id: None,
        dest_key: "in/data.csv".to_string(),
        container_path: None,
        mode: InputMode::Snapshot,
    };
    let mapped = native_input(input.clone(), ExecutionTarget::Realm).unwrap();
    assert_eq!(
        mapped.container_path.as_deref(),
        Some("/inputs/in/data.csv")
    );

    let explicit = ExecutionInput {
        container_path: Some("/data/input.csv".to_string()),
        ..input.clone()
    };
    assert_eq!(
        native_input(explicit, ExecutionTarget::Realm)
            .unwrap()
            .container_path
            .as_deref(),
        Some("/data/input.csv")
    );

    let traversal = ExecutionInput {
        container_path: Some("/in/../etc".to_string()),
        ..input
    };
    assert!(native_input(traversal, ExecutionTarget::Realm).is_err());
}

fn output_request(path: &str, key: &str, bucket: Option<&str>) -> ExecutionOutput {
    ExecutionOutput {
        container_path: path.to_string(),
        dest_key: key.to_string(),
        bucket: bucket.map(str::to_string),
    }
}

#[test]
fn maps_native_output() {
    let (explicit, workspace) = native_outputs(
        vec![output_request(
            "/out/report.txt",
            "outputs/report.txt",
            None,
        )],
        WorkspaceMode::Existing,
    )
    .unwrap();
    assert!(explicit.is_empty());
    assert_eq!(workspace[0].container_path, "/out/report.txt");
    assert_eq!(workspace[0].dest_key, "outputs/report.txt");

    assert!(
        native_outputs(
            vec![output_request("relative/path", "k", None)],
            WorkspaceMode::Existing
        )
        .is_err()
    );
    assert!(
        native_outputs(
            vec![output_request("/out", "", None)],
            WorkspaceMode::Existing
        )
        .is_err()
    );
}

#[test]
fn maps_bucket_output() {
    // Without a workspace every output resolves against the bucket it names.
    let (explicit, workspace) = native_outputs(
        vec![
            output_request("/out/report.txt", "reports/report.txt", Some("results")),
            output_request("/out/run.log", "logs/run.log", Some("archive")),
        ],
        WorkspaceMode::None,
    )
    .unwrap();
    assert!(workspace.is_empty());
    assert_eq!(explicit.len(), 2);
    assert_eq!(explicit[0].container_path, "/out/report.txt");
    assert!(explicit[0].path_prefix.is_none());
    assert!(explicit[0].destination_node_id.is_none());
    assert_eq!(
        explicit[0].destination,
        OutputDestination::S3 {
            bucket: "results".to_string(),
            key: "reports/report.txt".to_string(),
        }
    );
    assert_eq!(
        output_buckets(None, &explicit),
        ["results".to_string(), "archive".to_string()]
    );
    assert_eq!(
        output_buckets(Some("results"), &explicit),
        ["results".to_string(), "archive".to_string()]
    );
}

#[test]
fn output_requires_bucket() {
    let error = native_outputs(
        vec![output_request("/out/report.txt", "reports/r.txt", None)],
        WorkspaceMode::None,
    )
    .expect_err("a none-mode output has nowhere to land");
    let JobRequestError::BadRequestMessage(message) = error else {
        panic!("expected a described bad request");
    };
    assert!(message.contains("/out/report.txt"), "{message}");
    assert!(message.contains("bucket"), "{message}");
}

#[test]
fn mixes_output_destinations() {
    // Inside a workspace an output may still pin its own bucket.
    let (explicit, workspace) = native_outputs(
        vec![
            output_request("/out/report.txt", "reports/report.txt", Some("results")),
            output_request("/out/run.log", "logs/run.log", None),
        ],
        WorkspaceMode::Existing,
    )
    .unwrap();
    assert_eq!(
        explicit[0].destination,
        OutputDestination::S3 {
            bucket: "results".to_string(),
            key: "reports/report.txt".to_string(),
        }
    );
    assert_eq!(workspace[0].dest_key, "logs/run.log");

    // The same container path may only be captured once.
    assert!(
        native_outputs(
            vec![
                output_request("/out/report.txt", "a", Some("results")),
                output_request("/out/report.txt", "b", None),
            ],
            WorkspaceMode::Existing
        )
        .is_err()
    );
    assert!(
        native_outputs(
            vec![
                output_request("/out/a.txt", "same", Some("results")),
                output_request("/out/b.txt", "same", Some("results")),
            ],
            WorkspaceMode::None
        )
        .is_err()
    );
}

#[test]
fn normalizes_prefixes() {
    assert_eq!(
        validate_output_prefixes(vec!["results/".to_string(), "results/".to_string()]).unwrap(),
        ["results/"]
    );
    assert!(validate_output_prefixes(vec![String::new()]).is_err());
    assert!(validate_output_prefixes(vec!["result".to_string(); MAX_OUTPUT_PREFIXES + 1]).is_err());
}

#[test]
fn workspace_defaults_none() {
    // An omitted block, like an explicit `none`, gives the run no bucket.
    assert_eq!(
        workspace_request(None).unwrap(),
        (WorkspaceMode::None, None)
    );
    assert_eq!(
        workspace_request(Some(WorkspaceSpec {
            mode: CommandWorkspaceMode::None,
            bucket: None,
        }))
        .unwrap(),
        (WorkspaceMode::None, None)
    );
    let record = job_for(job_id(1), user(2), 1);
    assert_eq!(job_status_response(&record).workspace_mode, "none");
    assert!(
        workspace_request(Some(WorkspaceSpec {
            mode: CommandWorkspaceMode::Existing,
            bucket: None,
        }))
        .is_err()
    );
    assert!(
        workspace_request(Some(WorkspaceSpec {
            mode: CommandWorkspaceMode::None,
            bucket: Some("shared".to_string()),
        }))
        .is_err()
    );
}

#[tokio::test]
async fn invalid_cursor_rejected() {
    let (_dir, state) = build_state().await;
    let result = list_jobs(
        State(state.clone()),
        Extension(auth_for(user(2))),
        Query(ListJobsQuery {
            cursor: Some("not-base64!".to_string()),
            ..Default::default()
        }),
    )
    .await;
    assert!(matches!(result, Err(ServerError::BadRequest)));
}

#[test]
fn mount_path_checked() {
    // The whole bucket is checked on the bucket path; a folder on its own
    // object path, so a policy may deny that folder alone.
    let bucket = "/realm/g/group/data/node/lab-data".to_string();
    assert_eq!(mount_permission_path(bucket.clone(), ""), bucket);
    assert_eq!(
        mount_permission_path(bucket.clone(), "raw/2024/"),
        format!("{bucket}/raw/2024")
    );
}
