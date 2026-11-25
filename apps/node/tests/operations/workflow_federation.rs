//! Tests multi-node workflow log replication, failover resume and data-aware placement planning.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_compute::registry::ExecutorRegistry;
use aruna_core::compute::quota::{ComputeDemandSnapshot, ComputeReservationSnapshot};
use aruna_core::compute::{ExecutorAvailability, ExecutorCapability, ResourceEnvelope};
use aruna_core::effects::StorageEffect;
use aruna_core::heartbeat::{HeldHeartbeat, NodeHeartbeat};
use aruna_core::id::NodeId;
use aruna_core::job_request::SubmissionScope;
use aruna_core::keyspaces::NODE_INFO_KEYSPACE;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::identity::RETENTION_MS;
use aruna_core::structs::execution::job::local::WorkspaceMode;
use aruna_core::structs::execution::workflow::{StoredWorkflowLanguage, WorkflowVisibility};
use aruna_core::structs::execution::workflow_run::{RunOutput, RunState, WorkflowRunSpec};
use aruna_core::structs::placement::policy::PlacementSubject;
use aruna_core::structs::storage::node_info::{
    AdvertisementEpoch, NodeInfoDocument, NodeUrls, NodeUtilization, node_info_key,
};
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::time::unix_timestamp_millis;
use aruna_core::types::GroupId;
use aruna_core::user::id::UserId;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowError, WorkflowHost};
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_ops_work::jobs::lifecycle::submit_external::{
    ExternalSubmission, IngressNode, SubmitExternalOperation,
};
use aruna_ops_work::workflows::CatalogError;
use aruna_ops_work::workflows::import::{CreateWorkflowOperation, NewWorkflow, SourceInput};
use aruna_ops_work::workflows::runs::admit::{PinnedInput, RunRequest, StartRunOperation};
use aruna_ops_work::workflows::runs::control::{Follow, FollowRunOperation};
use aruna_ops_work::workflows::runs::log::{
    AppendLogOperation, LineageLogOperation, LoadLogOperation,
};
use aruna_ops_work::workflows::runs::plan::{COMPUTE_TO_DATA, DATA_TO_COMPUTE, PlanRunOperation};
use aruna_ops_work::workflows::runs::reads::{ListRunsOperation, RunFilter, RunSnapshotOperation};
use aruna_ops_work::workflows::runs::replica::{
    ReplicateLogOperation, UNREACHABLE, record_holders, replica_note,
};
use aruna_ops_work::workflows::runs::run::{PrepareRunOperation, Prepared};
use aruna_ops_work::workflows::runs::tasks::{task_key_of, task_spec};
use aruna_ops_work::workflows::runs::{ChangeRunOperation, RunChange};
use aruna_runner::driver::{DriverContext, drive};
use aruna_runner::testing::seed_bucket;
use aruna_workflow_contract::host::{DirectInput, StartTask, TaskFolders, TaskResources};
use aruna_workflow_contract::preflight::{RunLogEntry, SourceFile, WorkflowGraph};
use aruna_workflow_contract::{ContractError, PreflightKind, PreflightResult};
use serde_json::{Value, json};
use ulid::Ulid;

use crate::topology::{TestNode, TestResult, Topology};

const CWL: &str = "cwlVersion: v1.2\nclass: CommandLineTool\nbaseCommand: echo\n";
const BIG_BYTES: usize = 1 << 20;
const SMALL_BYTES: usize = 1024;

struct PlanHost {
    graph: Value,
}

#[async_trait::async_trait]
impl WorkflowHost for PlanHost {
    async fn serve(&self, effect: WorkflowEffect) -> WorkflowAnswer {
        match effect {
            WorkflowEffect::Preflight(PreflightKind::Check { .. }) => {
                WorkflowAnswer::Preflight(Ok(PreflightResult::Check {
                    runnable: true,
                    report_json: json!({"language": "cwl", "parts": []}),
                    images: vec!["debian:stable-slim".into()],
                }))
            }
            WorkflowEffect::Preflight(PreflightKind::Inspect { .. }) => {
                WorkflowAnswer::Preflight(Ok(PreflightResult::Inspect {
                    form_json: json!({"schema": {"type": "object"}}),
                    graph: WorkflowGraph {
                        nodes: Vec::new(),
                        edges: Vec::new(),
                    },
                }))
            }
            WorkflowEffect::Preflight(PreflightKind::ValidateValues { .. }) => {
                WorkflowAnswer::Preflight(Ok(PreflightResult::ValidateValues {
                    valid: true,
                    response_json: json!({}),
                }))
            }
            WorkflowEffect::Preflight(PreflightKind::DryRun { .. }) => {
                WorkflowAnswer::Preflight(Ok(PreflightResult::DryRun {
                    graph_json: self.graph.clone(),
                    tasks: Vec::new(),
                    diagnostics: Vec::new(),
                }))
            }
            WorkflowEffect::Preflight(_) => WorkflowAnswer::Preflight(Err(WorkflowError::Refused(
                ContractError::unsupported("test"),
            ))),
            WorkflowEffect::Open { .. } => WorkflowAnswer::Opened(Ok(())),
            WorkflowEffect::Send { .. } => WorkflowAnswer::Sent(Ok(())),
            WorkflowEffect::Receive { .. } => WorkflowAnswer::Received(Ok(None)),
            WorkflowEffect::Close { .. } => WorkflowAnswer::Closed(Ok(())),
        }
    }
}

fn attach_host(node: &TestNode, graph: Value) -> DriverContext {
    let mut ctx = (*node.context).clone();
    ctx.compute_handle = Some(Arc::new(
        ExecutorRegistry::new().with_workflow(Arc::new(PlanHost { graph })),
    ));
    ctx
}

async fn seed_blob(
    realm: &Topology,
    node: &TestNode,
    group_id: GroupId,
    bucket: &str,
    key: &str,
    size: usize,
) -> TestResult<Ulid> {
    let body = BackendStream::new(tokio_util::io::ReaderStream::new(std::io::Cursor::new(
        vec![b'A'; size],
    ))) as BackendStream<Result<bytes::Bytes, StreamError>>;
    let put = PutObjectOperation::new(PutObjectConfig {
        user_id: realm.user_id,
        group_id,
        realm_id: realm.realm_id,
        node_id: node.node_id(),
        request: PutObjectInput {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            content_length: Some(size as u64),
            body: Some(body),
        },
        expected_checksums: Vec::new(),
        checksum_type: None,
        exists: false,
        version_source: None,
        preassigned_version_id: None,
        quota_ceiling: None,
        routing: RoutingSnapshot::single(group_id),
    });
    Ok(drive(put, node.context.as_ref()).await?.version_id)
}

fn upload_workflow(group_id: GroupId) -> NewWorkflow {
    NewWorkflow {
        group_id,
        name: "fed-hello".to_owned(),
        description: None,
        language: Some(StoredWorkflowLanguage::Cwl),
        visibility: WorkflowVisibility::Group,
        source: SourceInput::Upload {
            entrypoint: "hello.cwl".to_owned(),
            files: vec![SourceFile {
                path: "hello.cwl".to_owned(),
                content: CWL.as_bytes().to_vec(),
                executable: false,
            }],
        },
    }
}

async fn advertise_executor(
    realm: &Topology,
    reader: &TestNode,
    target: NodeId,
    gpus: u32,
    now_ms: u64,
) -> TestResult<HeldHeartbeat> {
    let subject = PlacementSubject {
        node_id: target,
        generation: 1,
        location: "eu".to_owned(),
        labels: BTreeMap::new(),
        executor_kind: None,
        local_to_controller: true,
    };
    let mut cap = ExecutorCapability::new("docker".into(), "docker".into(), subject)?;
    cap.file_staging = true;
    cap.declared.gpus = gpus;
    cap.limits = ResourceEnvelope {
        max_cpu_cores: Some(8),
        max_ram_bytes: Some(16 << 30),
        max_disk_bytes: Some(100 << 30),
        max_concurrent: Some(4),
    };
    let availability = ExecutorAvailability {
        free_cpu_cores: Some(8),
        free_ram_bytes: Some(16 << 30),
        free_disk_bytes: Some(100 << 30),
        active_executions: 0,
        observed_at_ms: now_ms,
    };
    cap.availability = Some(availability);
    let epoch = AdvertisementEpoch {
        membership_generation: 1,
        publisher_generation: 1,
        observed_at_ms: now_ms,
    };
    let utilization = NodeUtilization {
        storage_bytes_used: 0,
        documents_held: None,
        load_permille: Some(0),
        heartbeat_at_ms: now_ms,
        download_slots: None,
        storage_capacity_bytes: None,
    };
    let demand = ComputeDemandSnapshot {
        epoch,
        groups: Vec::new(),
        truncated: false,
    };
    let reservation = ComputeReservationSnapshot {
        epoch,
        reserved: Default::default(),
    };
    let doc = NodeInfoDocument {
        node_id: target,
        executors: vec![cap],
        labels: BTreeMap::new(),
        urls: NodeUrls {
            api: None,
            s3: None,
        },
        utilization: utilization.clone(),
        updated_at_ms: now_ms,
        epoch,
        compute_draining: false,
        leaving: false,
        demand: demand.clone(),
        reservation: reservation.clone(),
    };
    reader
        .context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: NODE_INFO_KEYSPACE.to_owned(),
            key: node_info_key(target).into(),
            value: postcard::to_allocvec(&doc)?.into(),
            txn_id: None,
        })
        .await;
    Ok(HeldHeartbeat {
        node_id: target,
        heartbeat: NodeHeartbeat {
            realm_id: realm.realm_id,
            epoch,
            sequence: 1,
            utilization,
            availability: vec![("docker".into(), availability)],
            reservation,
            demand,
            jobs: None,
        },
        age_ms: 0,
        received_at_ms: now_ms,
    })
}

/// Replicates an active workflow run and its log pages over real P2P to a group record holder,
/// shows the unreachable note there, refuses non-owner resume, and resumes the lineage as owner.
#[tokio::test]
async fn replicates_workflow_run() -> TestResult<()> {
    let realm = Topology::spawn(3, 0, 2).await?;
    let group_id = realm.seed_group().await?;
    let other_user = UserId::local(Ulid::generate(), realm.realm_id);
    realm.grant_group_user(group_id, other_user).await?;

    let coord = realm.node(0);
    let holders = record_holders(&realm.config, group_id, coord.node_id());
    assert!(!holders.is_empty(), "group has remote record holders");
    let surviving = realm.find(holders[0]);

    let coord_ctx = attach_host(coord, Value::Null);
    let surviving_ctx = attach_host(surviving, Value::Null);
    let (owner, other) = (realm.auth_context(), realm.auth_for(other_user));

    seed_bucket(&coord_ctx, "results", group_id, realm.user_id, Vec::new()).await;
    seed_bucket(
        &surviving_ctx,
        "results",
        group_id,
        realm.user_id,
        Vec::new(),
    )
    .await;
    let wf = drive(
        CreateWorkflowOperation::new(&owner, coord.node_id(), upload_workflow(group_id)),
        &coord_ctx,
    )
    .await?;

    let request = RunRequest {
        group_id,
        workflow_id: wf.workflow_id,
        version: wf.version,
        values: json!({"message": "replicate"}),
        inputs: Vec::new(),
        inputs_json: None,
        output: Some(RunOutput {
            bucket: "results".to_owned(),
            prefix: "fed/".to_owned(),
            node_id: None,
            dataset_json: None,
        }),
        provenance_crate: false,
        idempotency_key: None,
        placement_site: None,
        secrets: BTreeMap::new(),
    };
    let coord_serving = (coord.node_id(), realm.realm_id);
    let run_id = drive(
        StartRunOperation::new(&owner, coord_serving, request),
        &coord_ctx,
    )
    .await?
    .record
    .run_id;

    drive(
        ChangeRunOperation::new(
            run_id,
            vec![RunChange::State {
                state: RunState::Running,
                error: None,
            }],
        ),
        &coord_ctx,
    )
    .await?;
    drive(
        AppendLogOperation::new(
            run_id,
            RunLogEntry::Begin {
                run_start_json: json!({"run": run_id.to_string()}),
            },
        ),
        &coord_ctx,
    )
    .await?;
    for seq in 1..=3 {
        drive(
            AppendLogOperation::new(
                run_id,
                RunLogEntry::Change {
                    seq,
                    change_json: json!({"task": seq}),
                },
            ),
            &coord_ctx,
        )
        .await?;
    }

    let next_page = drive(
        ReplicateLogOperation::new(run_id, coord_serving, 0),
        &coord_ctx,
    )
    .await?;
    assert_eq!(next_page, 0, "cursor stays on the mutable tail page");

    let coord_pages = drive(LoadLogOperation::new(run_id), &coord_ctx).await?;
    let replica_pages = drive(LoadLogOperation::new(run_id), &surviving_ctx).await?;
    assert_eq!(replica_pages, coord_pages);

    let snapshot = drive(RunSnapshotOperation::new(&owner, run_id), &surviving_ctx).await?;
    assert_eq!(
        replica_note(&snapshot.record, surviving.node_id()),
        Some(UNREACHABLE)
    );

    // Non-owner non-admin group member cannot resume an unreachable run on the surviving holder.
    let surviving_serving = (surviving.node_id(), realm.realm_id);
    let refused = drive(
        FollowRunOperation::new(&other, surviving_serving, run_id, Follow::Resume),
        &surviving_ctx,
    )
    .await;
    assert_eq!(refused, Err(CatalogError::Forbidden));

    // Surviving holder resumes directly from the workflow + log records copied by replication.
    let resumed = drive(
        FollowRunOperation::new(&owner, surviving_serving, run_id, Follow::Resume),
        &surviving_ctx,
    )
    .await?;
    assert_eq!(resumed.resumes_run_id, Some(run_id));
    assert_eq!(resumed.lineage, run_id);
    assert_eq!(resumed.node_id, surviving.node_id());
    let replayed = drive(LineageLogOperation::new(resumed.run_id), &surviving_ctx).await?;
    assert_eq!(replayed, coord_pages);
    let resumed_spec = WorkflowRunSpec {
        run_id: resumed.run_id,
        group_id,
        created_by: realm.user_id,
        realm_id: realm.realm_id,
    };
    let prepared = drive(
        PrepareRunOperation::new(&resumed_spec, surviving.node_id(), "blake3:resume".into()),
        &surviving_ctx,
    )
    .await?;
    assert!(matches!(prepared, Prepared::Ready(_)));
    Ok(())
}

/// Plans a multi-step workflow across a 3-node topology over real P2P input resolution:
/// compute-to-data for bulk CPU, data-to-compute for GPU, and fan-in minimizing staged bytes.
#[tokio::test]
async fn plans_multi_node() -> TestResult<()> {
    let realm = Topology::spawn(3, 0, 2).await?;
    let group_id = realm.seed_group().await?;
    let (planner, data_node, gpu_node) = (realm.node(0), realm.node(1), realm.node(2));
    let owner = realm.auth_context();

    for node in [data_node, gpu_node] {
        seed_bucket(
            node.context.as_ref(),
            "data",
            group_id,
            realm.user_id,
            Vec::new(),
        )
        .await;
    }
    let big_ver = seed_blob(&realm, data_node, group_id, "data", "big.bin", BIG_BYTES).await?;
    let small_ver = seed_blob(&realm, gpu_node, group_id, "data", "small.bin", SMALL_BYTES).await?;

    let pin = |name: &str, key: &str, ver: Ulid, node: &TestNode| PinnedInput {
        name: name.to_owned(),
        bucket: "data".to_owned(),
        key: key.to_owned(),
        version_id: ver,
        source_node_id: Some(node.node_id()),
    };
    let big_pin = pin("big", "big.bin", big_ver, data_node);
    let small_pin = pin("small", "small.bin", small_ver, gpu_node);

    let task = |id: u64, step: &str, gpus: Option<u32>, reads: &[&PinnedInput]| {
        let inputs: Vec<Value> = reads
            .iter()
            .map(|p| {
                let src = match p.source_node_id.filter(|n| *n != planner.node_id()) {
                    Some(node) => format!(
                        "aruna://{}/{}?version={}&node={node}",
                        p.bucket, p.key, p.version_id
                    ),
                    None => format!("aruna://{}/{}?version={}", p.bucket, p.key, p.version_id),
                };
                json!({"path": p.key, "source": src, "directory": false})
            })
            .collect();
        json!({"task": {"id": id, "step": step, "image": "tool:1", "inputs": inputs,
            "resources": {"cpus": 2.0, "gpus": gpus}, "after": []}, "markers": []})
    };
    let graph = json!({
        "nodes": [
            task(1, "align", None, &[&big_pin]),
            task(2, "train", Some(1), &[&big_pin]),
            task(3, "merge", None, &[&big_pin, &small_pin]),
        ],
        "edges": [],
    });

    let ctx = attach_host(planner, graph);
    let created = drive(
        CreateWorkflowOperation::new(&owner, planner.node_id(), upload_workflow(group_id)),
        &ctx,
    )
    .await?;

    let now_ms = unix_timestamp_millis();
    let fresh = BTreeMap::from([
        (
            data_node.node_id(),
            advertise_executor(&realm, planner, data_node.node_id(), 0, now_ms).await?,
        ),
        (
            gpu_node.node_id(),
            advertise_executor(&realm, planner, gpu_node.node_id(), 1, now_ms).await?,
        ),
    ]);
    let ingress = IngressNode {
        local: planner.node_id(),
        realm_id: realm.realm_id,
        now_ms,
        fresh,
    };
    let inputs_json = json!({
        "big": {
            "bucket": big_pin.bucket,
            "key": big_pin.key,
            "version_id": big_pin.version_id.to_string(),
            "source_node_id": big_pin.source_node_id.map(|n| n.to_string()),
        },
        "small": {
            "bucket": small_pin.bucket,
            "key": small_pin.key,
            "version_id": small_pin.version_id.to_string(),
            "source_node_id": small_pin.source_node_id.map(|n| n.to_string()),
        }
    })
    .to_string();
    let request = RunRequest {
        group_id,
        workflow_id: created.workflow_id,
        version: created.version,
        values: json!({}),
        inputs: vec![big_pin, small_pin],
        inputs_json: Some(inputs_json),
        output: None,
        provenance_crate: false,
        idempotency_key: None,
        placement_site: None,
        secrets: BTreeMap::new(),
    };
    let plan = drive(
        PlanRunOperation::new(&owner, ingress.clone(), request.clone())
            .with_token(Some(realm.bearer_token())),
        &ctx,
    )
    .await?;
    assert!(plan.complete, "plan should be complete: {plan:?}");
    assert_eq!(plan.tasks.len(), 3, "{plan:?}");

    let by_step = |name: &str| {
        plan.tasks
            .iter()
            .find(|t| t.step == name)
            .unwrap_or_else(|| panic!("missing step {name} in {plan:?}"))
    };
    let align = by_step("align");
    assert_eq!(align.node_id, Some(data_node.node_id()));
    assert_eq!(align.mode, COMPUTE_TO_DATA);
    assert_eq!(align.bytes, Some(0));

    let train = by_step("train");
    assert_eq!(train.node_id, Some(gpu_node.node_id()));
    assert_eq!(train.mode, DATA_TO_COMPUTE);
    assert_eq!(train.bytes, Some(BIG_BYTES as u64));

    let merge = by_step("merge");
    assert_eq!(merge.node_id, Some(data_node.node_id()));
    assert_eq!(merge.bytes, Some(SMALL_BYTES as u64));

    let listed = drive(
        ListRunsOperation::new(&owner, RunFilter::default(), None, 16),
        &ctx,
    )
    .await?;
    assert_eq!(listed.total, 0, "planning must not admit any runs");

    // Admitting the same remote-pinned run captures input-scoped grants so a child job submits
    // without a bearer token and resolves its remote direct input cleanly.
    seed_bucket(&ctx, "results", group_id, realm.user_id, Vec::new()).await;
    let mut start_req = request;
    start_req.output = Some(RunOutput {
        bucket: "results".to_owned(),
        prefix: "plan/".to_owned(),
        node_id: None,
        dataset_json: None,
    });
    let admitted = drive(
        StartRunOperation::new(&owner, (planner.node_id(), realm.realm_id), start_req)
            .with_token(Some(realm.bearer_token())),
        &ctx,
    )
    .await?;
    assert_eq!(admitted.record.captured_remote.len(), 2);
    assert!(
        admitted
            .record
            .captured_remote
            .iter()
            .all(|capture| capture.grant.is_some()),
        "remote pins carry signed read grants"
    );
    let child_req = StartTask {
        task_id: Some(1),
        step_id: "align".into(),
        name: "align-1".into(),
        attempt: 1,
        task_key_seed: "align-seed".into(),
        spec_json: json!({}),
        image: Some("tool:1".into()),
        command_wrapper_bytes: b"#!/bin/sh\necho ok\n".to_vec(),
        folders: TaskFolders {
            work: "/work".into(),
            control: "/control".into(),
            home: "/home".into(),
        },
        direct_inputs: vec![DirectInput {
            path: "big.bin".into(),
            address: format!(
                "aruna://data/big.bin?version={big_ver}&node={}",
                data_node.node_id()
            ),
            digest: String::new(),
            size: BIG_BYTES as u64,
            executable: false,
            aruna_id: None,
            endpoints: Vec::new(),
        }],
        staged_files: Vec::new(),
        expected_outputs: Vec::new(),
        resources: TaskResources {
            cpus: 2,
            memory_bytes: None,
            disk_bytes: None,
            gpus: 0,
            time_seconds: None,
        },
        site: None,
        step_settings: None,
        network_allowed: false,
        secret_placeholders: Vec::new(),
    };
    let spec = task_spec(&admitted.record, &child_req, &task_key_of("align-seed"))?;
    let submission = ExternalSubmission {
        spec,
        created_by: realm.user_id,
        scope: SubmissionScope::Keyed("workflow-task/test/align/1/1".into()),
        workspace_mode: WorkspaceMode::None,
        workspace_bucket: None,
        retention_ms: RETENTION_MS,
        auth_token: None,
    };
    let accepted = drive(
        SubmitExternalOperation::new(submission, Some(ingress))
            .with_precaptured(admitted.record.captured_remote.clone()),
        &ctx,
    )
    .await?;
    assert!(!accepted.job_id.to_string().is_empty());
    Ok(())
}
