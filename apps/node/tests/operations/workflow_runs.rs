//! Tests single-node workflow catalog, run lifecycle, provenance crates, secrets and encryption.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_compute::registry::ExecutorRegistry;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::secrets::SecretName;
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::workflow::{StoredWorkflowLanguage, WorkflowVisibility};
use aruna_core::structs::execution::workflow_run::{
    RunFile, RunOutput, RunState, WorkflowSecretBinding, WorkflowSecretScope,
};
use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
use aruna_core::structs::storage::routing::RoutingSnapshot;
use aruna_core::types::GroupId;
use aruna_core::user::id::UserId;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowHost};
use aruna_ops_data::metadata::record_reads::DocumentRecordOperation;
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_ops_work::workflows::CatalogError;
use aruna_ops_work::workflows::import::{CreateWorkflowOperation, NewWorkflow, SourceInput};
use aruna_ops_work::workflows::runs::admit::{PinnedInput, RunRequest, StartRunOperation};
use aruna_ops_work::workflows::runs::control::{Follow, FollowRunOperation};
use aruna_ops_work::workflows::runs::events::ReadEventsOperation;
use aruna_ops_work::workflows::runs::provenance::{
    RunCrateOperation, WriteProvenanceOperation, status,
};
use aruna_ops_work::workflows::runs::run::{
    ApplyEventOperation, FinishRunOperation, PrepareRunOperation, Prepared,
};
use aruna_ops_work::workflows::runs::{ChangeRunOperation, RunChange};
use aruna_ops_work::workflows::versions::GetVersionOperation;
use aruna_runner::driver::{DriverContext, drive};
use aruna_runner::testing::seed_bucket;
use aruna_workflow_contract::message::{LiveWorkflowEvent, TaskChange, TaskState};
use aruna_workflow_contract::preflight::{GraphNode, NodeKind, SourceFile, WorkflowGraph};
use aruna_workflow_contract::{ContractError, PreflightKind, PreflightResult};
use serde_json::json;
use ulid::Ulid;

use crate::topology::{TestNode, TestResult, Topology};

const CWL: &str = "cwlVersion: v1.2\nclass: CommandLineTool\nbaseCommand: echo\n";
const SAMPLE: &[u8] = b"sample-input\n";

struct ScriptedHost;

#[async_trait::async_trait]
impl WorkflowHost for ScriptedHost {
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
                        nodes: vec![GraphNode {
                            id: "echo".into(),
                            label: "echo".into(),
                            kind: NodeKind::Step,
                            tool: None,
                            version: None,
                        }],
                        edges: Vec::new(),
                    },
                }))
            }
            WorkflowEffect::Preflight(_) => WorkflowAnswer::Preflight(Err(
                aruna_core::workflow::WorkflowError::Refused(ContractError::unsupported("test")),
            )),
            WorkflowEffect::Open { .. } => WorkflowAnswer::Opened(Ok(())),
            WorkflowEffect::Send { .. } => WorkflowAnswer::Sent(Ok(())),
            WorkflowEffect::Receive { .. } => WorkflowAnswer::Received(Ok(None)),
            WorkflowEffect::Close { .. } => WorkflowAnswer::Closed(Ok(())),
        }
    }
}

fn with_workflow(node: &TestNode) -> DriverContext {
    let mut ctx = (*node.context).clone();
    let registry = ExecutorRegistry::new().with_workflow(Arc::new(ScriptedHost));
    ctx.compute_handle = Some(Arc::new(registry));
    ctx
}

async fn put_object(
    realm: &Topology,
    node: &TestNode,
    group_id: GroupId,
    bucket: &str,
    key: &str,
    data: &'static [u8],
) -> TestResult<Ulid> {
    let result = drive(
        PutObjectOperation::new(PutObjectConfig {
            user_id: realm.user_id,
            group_id,
            realm_id: realm.realm_id,
            node_id: node.node_id(),
            request: PutObjectInput {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                content_length: Some(data.len() as u64),
                body: Some(BackendStream::new(tokio_util::io::ReaderStream::new(data))
                    as BackendStream<Result<bytes::Bytes, StreamError>>),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: None,
            quota_ceiling: None,
            routing: RoutingSnapshot::single(group_id),
        }),
        node.context.as_ref(),
    )
    .await?;
    Ok(result.version_id)
}

fn new_workflow(group_id: GroupId) -> NewWorkflow {
    NewWorkflow {
        group_id,
        name: "hello".to_owned(),
        description: Some("End-to-end test workflow".to_owned()),
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

/// Imports a workflow, runs it end-to-end with pinned inputs, secret bindings and a published
/// realm RO-Crate, then enforces secret-scoped repeat permissions.
#[tokio::test]
async fn drives_workflow_lifecycle() -> TestResult<()> {
    let realm = Topology::spawn(2, 0, 1).await?;
    let group_id = realm.seed_group().await?;
    let other_user = UserId::local(Ulid::generate(), realm.realm_id);
    realm.grant_group_user(group_id, other_user).await?;

    let node = realm.node(0);
    let ctx = with_workflow(node);
    let owner = realm.auth_context();
    let other = realm.auth_for(other_user);
    seed_bucket(&ctx, "results", group_id, realm.user_id, Vec::new()).await;
    let input_ver = put_object(&realm, node, group_id, "results", "in.txt", SAMPLE).await?;
    let out_ver = put_object(&realm, node, group_id, "results", "out/said.txt", SAMPLE).await?;

    let created = drive(
        CreateWorkflowOperation::new(&owner, node.node_id(), new_workflow(group_id)),
        &ctx,
    )
    .await?;
    let view = drive(
        GetVersionOperation::new(&owner, created.workflow_id, created.version),
        &ctx,
    )
    .await?;
    assert_eq!(view.workflow.name, "hello");

    let mut secrets = BTreeMap::new();
    secrets.insert(
        "API_TOKEN".to_owned(),
        WorkflowSecretBinding {
            scope: WorkflowSecretScope::User,
            name: SecretName::try_from("my-token".to_owned())?,
            field: None,
        },
    );
    let request = RunRequest {
        group_id,
        workflow_id: created.workflow_id,
        version: created.version,
        values: json!({"message": "hello"}),
        inputs: vec![PinnedInput {
            name: "sample".to_owned(),
            bucket: "results".to_owned(),
            key: "in.txt".to_owned(),
            version_id: input_ver,
            source_node_id: Some(node.node_id()),
        }],
        inputs_json: None,
        output: Some(RunOutput {
            bucket: "results".to_owned(),
            prefix: "out/".to_owned(),
            node_id: None,
            dataset_json: None,
        }),
        provenance_crate: true,
        idempotency_key: Some("e2e-run-1".to_owned()),
        placement_site: None,
        secrets,
    };
    let serving = (node.node_id(), realm.realm_id);
    let admitted = drive(StartRunOperation::new(&owner, serving, request), &ctx).await?;
    assert!(admitted.created);
    assert!(!admitted.record.local_only);
    let run_id = admitted.record.run_id;

    let spec = aruna_core::structs::execution::workflow_run::WorkflowRunSpec {
        run_id,
        group_id,
        created_by: realm.user_id,
        realm_id: realm.realm_id,
    };
    let prepared = drive(
        PrepareRunOperation::new(&spec, node.node_id(), "blake3:claim".into()),
        &ctx,
    )
    .await?;
    assert!(matches!(prepared, Prepared::Ready(_)));

    let task_done = LiveWorkflowEvent::TaskChanged(TaskChange {
        task_id: 1,
        step: "echo".into(),
        name: "echo-1".into(),
        attempt: Some(1),
        state: TaskState::Finished,
        time_ms: 100,
        started_ms: Some(10),
        ended_ms: Some(100),
        exit_code: Some(0),
        cached: false,
        error: None,
    });
    drive(ApplyEventOperation::new(run_id, &task_done), &ctx).await?;
    let delivered = RunChange::Delivered {
        file: RunFile {
            name: "said.txt".into(),
            bucket: Some("results".into()),
            key: Some("out/said.txt".into()),
            version_id: Some(out_ver.to_string()),
            node_id: Some(node.node_id()),
            size: Some(SAMPLE.len() as u64),
            digest: Some(blake3::hash(SAMPLE).to_hex().to_string()),
        },
        origin: None,
    };
    drive(ChangeRunOperation::new(run_id, vec![delivered]), &ctx).await?;
    let ended = drive(
        FinishRunOperation::new(run_id, RunState::Succeeded, None),
        &ctx,
    )
    .await?;
    assert_eq!(ended.state, RunState::Succeeded);

    let written = drive(WriteProvenanceOperation::new(run_id, serving), &ctx).await?;
    assert_eq!(written.provenance_status.as_deref(), Some(status::WRITTEN));
    let doc_id = written
        .provenance_document_id
        .ok_or("written workflow crate has a document id")?;

    let mut stored_doc = false;
    for member in &realm.nodes {
        if let Ok(Some(record)) = drive(
            DocumentRecordOperation::new(doc_id),
            member.context.as_ref(),
        )
        .await
        {
            assert_eq!(record.group_id, group_id);
            stored_doc = true;
        }
    }
    assert!(stored_doc, "published workflow RO-Crate exists in realm");

    let events = drive(ReadEventsOperation::new(&owner, run_id, 0, 32), &ctx).await?;
    assert!(
        events.events.len() >= 4,
        "events replayed: {}",
        events.events.len()
    );

    // A non-creator group member cannot repeat a run that binds User secrets.
    let refused = drive(
        FollowRunOperation::new(&other, serving, run_id, Follow::Repeat),
        &ctx,
    )
    .await;
    assert_eq!(refused, Err(CatalogError::Forbidden));

    let repeated = drive(
        FollowRunOperation::new(&owner, serving, run_id, Follow::Repeat),
        &ctx,
    )
    .await?;
    assert_eq!(repeated.repeats_run_id, Some(run_id));
    assert_eq!(repeated.secrets.len(), 1);
    Ok(())
}

/// A workflow targeting an encrypted bucket is marked protected and keeps its provenance crate
/// private to the run instead of publishing plain realm metadata.
#[tokio::test]
async fn protects_encrypted_workflow() -> TestResult<()> {
    let realm = Topology::spawn(2, 0, 1).await?;
    let group_id = realm.seed_group().await?;
    let node = realm.node(0);
    let ctx = with_workflow(node);
    let owner = realm.auth_context();
    let stranger = realm.auth_for(UserId::local(Ulid::generate(), realm.realm_id));
    seed_bucket(&ctx, "secret-results", group_id, realm.user_id, Vec::new()).await;
    let encryption = BucketEncryption {
        mode: EncryptionMode::VaultLocked,
        bucket_id: Some(Ulid::generate()),
        key_generation: 1,
        ..BucketEncryption::default()
    };
    node.context
        .storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: BUCKET_ENCRYPTION_KEYSPACE.to_owned(),
            key: b"secret-results".to_vec().into(),
            value: encryption.to_bytes()?.into(),
            txn_id: None,
        })
        .await;

    let created = drive(
        CreateWorkflowOperation::new(&owner, node.node_id(), new_workflow(group_id)),
        &ctx,
    )
    .await?;
    let serving = (node.node_id(), realm.realm_id);
    let request = RunRequest {
        group_id,
        workflow_id: created.workflow_id,
        version: created.version,
        values: json!({}),
        inputs: Vec::new(),
        inputs_json: None,
        output: Some(RunOutput {
            bucket: "secret-results".to_owned(),
            prefix: "enc/".to_owned(),
            node_id: None,
            dataset_json: None,
        }),
        provenance_crate: true,
        idempotency_key: None,
        placement_site: None,
        secrets: BTreeMap::new(),
    };
    let run = drive(StartRunOperation::new(&owner, serving, request), &ctx)
        .await?
        .record;
    assert!(run.protected);
    drive(
        FinishRunOperation::new(run.run_id, RunState::Succeeded, None),
        &ctx,
    )
    .await?;
    let written = drive(WriteProvenanceOperation::new(run.run_id, serving), &ctx).await?;
    assert_eq!(
        written.provenance_status.as_deref(),
        Some(status::PROTECTED)
    );
    assert_eq!(written.provenance_document_id, None);
    assert!(
        drive(RunCrateOperation::new(&owner, run.run_id), &ctx)
            .await
            .is_ok()
    );
    assert_eq!(
        drive(RunCrateOperation::new(&stranger, run.run_id), &ctx).await,
        Err(CatalogError::NotFound)
    );
    Ok(())
}
