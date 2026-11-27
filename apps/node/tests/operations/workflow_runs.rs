//! Tests single-node workflow catalog, run lifecycle, provenance crates, secrets and encryption.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_compute::registry::ExecutorRegistry;
use aruna_core::compute::Secret;
use aruna_core::effects::StorageEffect;
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::node::vault::NodeVaultKey;
use aruna_core::secrets::{
    Delivery, ResponseExposure, SecretConsumer, SecretFields, SecretKind, SecretName, UseRule,
};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::input::parse_workflow_secrets;
use aruna_core::structs::execution::workflow::{StoredWorkflowLanguage, WorkflowVisibility};
use aruna_core::structs::execution::workflow_run::{
    RunOutput, RunState, WorkflowSecretBinding, WorkflowSecretScope,
};
use aruna_core::structs::storage::encryption::{BucketEncryption, EncryptionMode};
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_core::types::GroupId;
use aruna_core::user::id::UserId;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowHost};
use aruna_ops_data::metadata::record_reads::DocumentRecordOperation;
use aruna_ops_data::s3::object::put::{PutObjectConfig, PutObjectInput, PutObjectOperation};
use aruna_ops_identity::secrets::SecretChange;
use aruna_ops_identity::secrets::node::{NodeChange, NodeSecretOperation};
use aruna_ops_work::jobs::lifecycle::submit_external::IngressNode;
use aruna_ops_work::jobs::workflow::credential::RunIdentity;
use aruna_ops_work::jobs::workflow::vault_secrets::ReleaseVaultSecrets;
use aruna_ops_work::workflows::CatalogError;
use aruna_ops_work::workflows::import::{CreateWorkflowOperation, NewWorkflow, SourceInput};
use aruna_ops_work::workflows::runs::admit::{PinnedInput, RunRequest, StartRunOperation};
use aruna_ops_work::workflows::runs::control::{Follow, FollowRunOperation};
use aruna_ops_work::workflows::runs::events::ReadEventsOperation;
use aruna_ops_work::workflows::runs::provenance::{
    RunCrateOperation, WriteProvenanceOperation, status,
};
use aruna_ops_work::workflows::runs::reads::RunSnapshotOperation;
use aruna_ops_work::workflows::runs::run::{
    AnswerCallOperation, ApplyEventOperation, CallNode, FinishRunOperation, PrepareRunOperation,
    Prepared,
};
use aruna_ops_work::workflows::runs::tasks::{task_key_of, task_spec};
use aruna_ops_work::workflows::versions::GetVersionOperation;
use aruna_runner::driver::{DriverContext, drive};
use aruna_runner::testing::seed_bucket;
use aruna_workflow_contract::host::{SecretPlaceholder, StartTask, TaskFolders, TaskResources};
use aruna_workflow_contract::message::{LiveWorkflowEvent, TaskChange, TaskState};
use aruna_workflow_contract::preflight::{GraphNode, NodeKind, SourceFile, WorkflowGraph};
use aruna_workflow_contract::{
    ContractError, HostRpcRequest, HostRpcResult, PreflightKind, PreflightResult,
};
use serde_json::json;
use ulid::Ulid;

use crate::topology::{TestNode, TestResult, Topology, wait_until};

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
    ctx.storage_handle.open_vault(NodeVaultKey::random());

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

    drive(
        NodeSecretOperation::new(NodeChange {
            realm_id: realm.realm_id,
            node_id: node.node_id(),
            actor: realm.user_id,
            change: SecretChange::Create {
                name: SecretName::try_from("node-token".to_owned())?,
                kind: SecretKind::parse("bearer").unwrap(),
                description: String::new(),
                settings: BTreeMap::new(),
                uses: vec![UseRule {
                    consumer: SecretConsumer::Workflow {
                        workflow_id: created.workflow_id,
                    },
                    delivery: Delivery::Value,
                    origins: Vec::new(),
                    paths: Vec::new(),
                    methods: Vec::new(),
                    response: ResponseExposure::Status,
                    groups: vec![group_id],
                }],
                fields: SecretFields::new(BTreeMap::from([(
                    "token".to_owned(),
                    Secret::new("synthetic-workflow-secret".to_owned()),
                )])),
            },
            now_ms: aruna_core::time::unix_timestamp_millis(),
            write_id: Ulid::generate(),
        }),
        &ctx,
    )
    .await?;

    let mut secrets = BTreeMap::new();
    secrets.insert(
        "API_TOKEN".to_owned(),
        WorkflowSecretBinding {
            scope: WorkflowSecretScope::User,
            name: SecretName::try_from("my-token".to_owned())?,
            field: None,
        },
    );
    secrets.insert(
        "NODE_TOKEN".to_owned(),
        WorkflowSecretBinding {
            scope: WorkflowSecretScope::Node,
            name: SecretName::try_from("node-token".to_owned())?,
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

    // Build a real secret-bearing child spec and release its vault secret against the stored run.
    let task_req = StartTask {
        task_id: Some(1),
        step_id: "echo".into(),
        name: "echo-1".into(),
        attempt: 1,
        task_key_seed: "echo-seed".into(),
        spec_json: json!({}),
        image: Some("debian:stable-slim".into()),
        command_wrapper_bytes: b"#!/bin/sh\necho ok\n".to_vec(),
        folders: TaskFolders {
            work: "/work".into(),
            control: "/control".into(),
            home: "/home".into(),
        },
        direct_inputs: Vec::new(),
        staged_files: Vec::new(),
        expected_outputs: Vec::new(),
        resources: TaskResources {
            cpus: 1,
            memory_bytes: None,
            disk_bytes: None,
            gpus: 0,
            time_seconds: None,
        },
        site: None,
        step_settings: None,
        network_allowed: false,
        secret_placeholders: vec![SecretPlaceholder {
            name: "NODE_TOKEN".into(),
            placeholder: "aruna-secret:NODE_TOKEN".into(),
            env_name: Some("NODE_TOKEN".into()),
        }],
    };
    let child_spec = task_spec(&admitted.record, &task_req, &task_key_of("echo-seed"))?;
    let child_bindings = parse_workflow_secrets(&child_spec.tags)?;
    let released = drive(
        ReleaseVaultSecrets::new(
            RunIdentity {
                job_id: JobId::from_bytes([7; 16]),
                user_id: realm.user_id,
                group_id,
                realm_id: realm.realm_id,
                node_id: node.node_id(),
            },
            run_id,
            child_bindings,
        ),
        &ctx,
    )
    .await
    .map_err(|error| format!("secret release failed: {error:?}"))?;
    assert_eq!(released["NODE_TOKEN"].expose(), "synthetic-workflow-secret");

    // Deliver the task output through the coordinator's DeliverOutput RPC and inspect stored output.
    let call_node = CallNode {
        node_id: node.node_id(),
        realm_id: realm.realm_id,
        routing: NodeRouting::default(),
        ingress: IngressNode {
            local: node.node_id(),
            realm_id: realm.realm_id,
            now_ms: 100,
            fresh: BTreeMap::new(),
        },
    };
    let delivery = drive(
        AnswerCallOperation::new(
            admitted.record.clone(),
            call_node,
            1,
            HostRpcRequest::DeliverOutput {
                source_ref: format!("aruna://results/in.txt?version={input_ver}"),
                target_path: "said.txt".into(),
            },
        ),
        &ctx,
    )
    .await?;
    let HostRpcResult::Delivered { location } = delivery.result else {
        return Err(format!("expected delivery result, got {:?}", delivery.result).into());
    };
    let out_ver = location
        .split_once("?version=")
        .map(|(_, v)| v.to_owned())
        .ok_or("delivered output location must carry a version id")?;
    let after_deliver = drive(RunSnapshotOperation::new(&owner, run_id), &ctx).await?;
    assert_eq!(after_deliver.record.outputs.len(), 1);
    assert_eq!(after_deliver.record.outputs[0].name, "said.txt");
    assert_eq!(
        after_deliver.record.outputs[0].size,
        Some(SAMPLE.len() as u64)
    );
    assert_eq!(
        after_deliver.record.outputs[0].version_id.as_deref(),
        Some(out_ver.as_str())
    );

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

    wait_until(
        "published workflow RO-Crate reaches realm",
        node.node_id(),
        || async {
            for member in &realm.nodes {
                if let Ok(Some(record)) = drive(
                    DocumentRecordOperation::new(doc_id),
                    member.context.as_ref(),
                )
                .await
                {
                    assert_eq!(record.group_id, group_id);
                    return true;
                }
            }
            false
        },
    )
    .await?;

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
    assert_eq!(repeated.secrets.len(), 2);
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
