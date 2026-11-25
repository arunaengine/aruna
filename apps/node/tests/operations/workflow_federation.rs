//! Tests multi-node workflow log replication and failover resume across a real P2P topology.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_compute::registry::ExecutorRegistry;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    WORKFLOW_KEYSPACE, WORKFLOW_SOURCE_KEYSPACE, WORKFLOW_VERSION_KEYSPACE,
};
use aruna_core::structs::execution::workflow::{
    StoredWorkflowLanguage, WorkflowVisibility, version_key, workflow_key,
};
use aruna_core::structs::execution::workflow_run::{RunOutput, RunState};
use aruna_core::types::GroupId;
use aruna_core::user::id::UserId;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowError, WorkflowHost};
use aruna_ops_work::workflows::CatalogError;
use aruna_ops_work::workflows::import::{CreateWorkflowOperation, NewWorkflow, SourceInput};
use aruna_ops_work::workflows::runs::admit::{RunRequest, StartRunOperation};
use aruna_ops_work::workflows::runs::control::{Follow, FollowRunOperation};
use aruna_ops_work::workflows::runs::log::{
    AppendLogOperation, LineageLogOperation, LoadLogOperation,
};
use aruna_ops_work::workflows::runs::reads::RunSnapshotOperation;
use aruna_ops_work::workflows::runs::replica::{
    ReplicateLogOperation, UNREACHABLE, record_holders, replica_note,
};
use aruna_ops_work::workflows::runs::{ChangeRunOperation, RunChange};
use aruna_runner::driver::{DriverContext, drive};
use aruna_runner::testing::seed_bucket;
use aruna_workflow_contract::preflight::{RunLogEntry, SourceFile, WorkflowGraph};
use aruna_workflow_contract::{ContractError, PreflightKind, PreflightResult};
use serde_json::{Value, json};
use ulid::Ulid;

use crate::topology::{TestNode, TestResult, Topology};

const CWL: &str = "cwlVersion: v1.2\nclass: CommandLineTool\nbaseCommand: echo\n";

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

async fn copy_workflow(from: &TestNode, to: &TestNode, workflow_id: Ulid) {
    let ver = version_key(workflow_id, 1);
    for (space, key) in [
        (WORKFLOW_KEYSPACE, workflow_key(workflow_id)),
        (WORKFLOW_VERSION_KEYSPACE, ver.clone()),
        (WORKFLOW_SOURCE_KEYSPACE, ver),
    ] {
        let ev = from
            .context
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: space.to_owned(),
                key: key.clone(),
                txn_id: None,
            })
            .await;
        if let Event::Storage(StorageEvent::ReadResult {
            value: Some(val), ..
        }) = ev
        {
            to.context
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: space.to_owned(),
                    key,
                    value: val,
                    txn_id: None,
                })
                .await;
        }
    }
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

    let sent = drive(
        ReplicateLogOperation::new(run_id, coord_serving, 0),
        &coord_ctx,
    )
    .await?;
    assert!(sent >= 1, "replicated pages: {sent}");

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

    copy_workflow(coord, surviving, wf.workflow_id).await;
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
    Ok(())
}
