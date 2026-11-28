//! Tests atomic single-transaction workflow + automation saves and workflow_id filtering.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use aruna_compute::registry::ExecutorRegistry;
use aruna_core::automation::record::{Spec, Stage, StageKind, Trigger, When};
use aruna_core::automation::scope::{Audience, Scope};
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::keyspaces::{AUTOMATIONS_KEYSPACE, WORKFLOW_KEYSPACE, WORKFLOW_VERSION_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::workflow::{
    StoredWorkflowLanguage, WorkflowRecord, WorkflowVisibility, workflow_key,
};
use aruna_core::structs::identity::features::{Feature, FeatureMode};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::workflow::{WorkflowAnswer, WorkflowEffect, WorkflowError, WorkflowHost};
use aruna_ops_identity::document_repository::GetConfigError;
use aruna_runner::driver::drive;
use aruna_workflow_contract::preflight::{GraphNode, NodeKind, WorkflowGraph};
use aruna_workflow_contract::{ContractError, PreflightKind, PreflightResult};
use serde_json::json;
use ulid::Ulid;

use super::{SaveAutomation, SaveBundle, SaveBundleOperation, SaveWorkflow};
use crate::automations::AutoError;
use crate::automations::flow_tests::Env;
use crate::automations::reads::ListAutosOperation;
use crate::workflows::import::SourceInput;

struct StubHost {
    refuse: AtomicBool,
}

impl StubHost {
    fn new(refuse: bool) -> Self {
        Self {
            refuse: AtomicBool::new(refuse),
        }
    }
}

#[async_trait::async_trait]
impl WorkflowHost for StubHost {
    async fn serve(&self, effect: WorkflowEffect) -> WorkflowAnswer {
        let WorkflowEffect::Preflight(kind) = effect else {
            return effect.unavailable();
        };
        if self.refuse.load(Ordering::Relaxed) {
            return WorkflowAnswer::Preflight(Err(WorkflowError::Refused(ContractError::invalid(
                "invalid workflow graph",
            ))));
        }
        let outcome = match kind {
            PreflightKind::Check { .. } => Ok(PreflightResult::Check {
                runnable: true,
                report_json: json!({"language": "wir", "parts": [], "problems": []}),
                images: Vec::new(),
            }),
            PreflightKind::Inspect { .. } => Ok(PreflightResult::Inspect {
                form_json: json!({"schema": {"type": "object"}}),
                graph: WorkflowGraph {
                    nodes: vec![GraphNode {
                        id: "step".into(),
                        label: "step".into(),
                        kind: NodeKind::Step,
                        tool: None,
                        version: None,
                    }],
                    edges: Vec::new(),
                },
            }),
            other => Err(WorkflowError::Refused(ContractError::unsupported(
                other.name(),
            ))),
        };
        WorkflowAnswer::Preflight(outcome)
    }
}

fn placeholder_spec() -> Spec {
    Spec {
        triggers: vec![Trigger::Manual { form_json: None }],
        conditions: Default::default(),
        stages: vec![Stage {
            name: "run".into(),
            needs: Some(Vec::new()),
            when: When::Passed,
            kind: StageKind::Run {
                workflow_id: Ulid::nil(),
                version: 0,
                values_json: "{}".into(),
                inputs_json: None,
                output_bucket: None,
                output_prefix: None,
                placement_site: None,
                secrets: BTreeMap::new(),
                provenance_crate: false,
            },
        }],
        scope: Scope {
            workflows: vec![(Ulid::nil(), 0)],
            audiences: vec![Audience::GroupAdmins],
            ..Scope::default()
        },
    }
}

#[test]
fn steps_save_phases() {
    let realm = aruna_core::structs::identity::realm::RealmId([4; 32]);
    let node = iroh::SecretKey::from_bytes(&[5; 32]).public();
    let group_id = Ulid::from_bytes([6; 16]);
    let auth = aruna_core::structs::identity::auth::AuthContext::new(
        aruna_core::user::id::UserId::local(Ulid::from_bytes([1; 16]), realm),
        realm,
    );

    // Missing source when creating a new workflow fails immediately in first().
    let missing_source = SaveBundle {
        group_id,
        workflow: SaveWorkflow {
            workflow_id: None,
            name: Some("qc".into()),
            description: None,
            language: Some(StoredWorkflowLanguage::Wir),
            visibility: Some(WorkflowVisibility::Group),
            message: None,
            source: None,
        },
        automation: SaveAutomation {
            automation_id: None,
            expected_revision: None,
            name: Some("qc-auto".into()),
            description: None,
            spec: placeholder_spec(),
            pin_version: true,
        },
    };
    let mut bad = SaveBundleOperation::new(&auth, node, missing_source);
    assert!(matches!(bad.first(), Err(AutoError::Invalid(_))));

    // Disabled Workflows feature refuses inside the domain operation.
    let valid_bundle = SaveBundle {
        group_id,
        workflow: SaveWorkflow {
            workflow_id: None,
            name: Some("qc".into()),
            description: None,
            language: Some(StoredWorkflowLanguage::Wir),
            visibility: Some(WorkflowVisibility::Group),
            message: Some("initial".into()),
            source: Some(SourceInput::Wir(json!({"wir_version": "1"}))),
        },
        automation: SaveAutomation {
            automation_id: None,
            expected_revision: None,
            name: Some("qc-auto".into()),
            description: None,
            spec: placeholder_spec(),
            pin_version: true,
        },
    };
    let mut gated = SaveBundleOperation::new(&auth, node, valid_bundle);
    assert!(matches!(gated.first(), Ok(Next::Call(IoCall::Child(_)))));
    let mut config = RealmConfigDocument::default_for_realm(realm, Vec::new());
    config.features.insert(
        Feature::Workflows,
        FeatureMode::Scoped {
            default: false,
            groups: Default::default(),
        },
    );
    let refused = gated.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
        config,
    ))));
    assert!(matches!(
        refused,
        Err(AutoError::FeatureOff(Feature::Workflows, _))
    ));
}

#[tokio::test]
async fn commits_bundle_atomically() {
    let mut env = Env::new().await;
    let host = Arc::new(StubHost::new(true));
    env.ctx.compute_handle = Some(Arc::new(
        ExecutorRegistry::new().with_workflow(host.clone()),
    ));

    let make_bundle = |workflow_id, automation_id, expected_revision, message: &str| SaveBundle {
        group_id: env.group,
        workflow: SaveWorkflow {
            workflow_id,
            name: Some("qc-flow".into()),
            description: Some(Some("Studio workflow".into())),
            language: Some(StoredWorkflowLanguage::Wir),
            visibility: Some(WorkflowVisibility::Group),
            message: Some(message.into()),
            source: Some(SourceInput::Wir(
                json!({"wir_version": "1", "note": message}),
            )),
        },
        automation: SaveAutomation {
            automation_id,
            expected_revision,
            name: Some("qc-auto".into()),
            description: Some("Runs QC".into()),
            spec: placeholder_spec(),
            pin_version: true,
        },
    };

    // Preflight refusal writes zero workflow, version or automation rows.
    let refused = drive(
        SaveBundleOperation::new(&env.owner, env.node, make_bundle(None, None, None, "bad")),
        &env.ctx,
    )
    .await;
    assert!(matches!(refused, Err(AutoError::Invalid(_))), "{refused:?}");
    assert!(env.scan_rows(WORKFLOW_KEYSPACE).await.is_empty());
    assert!(env.scan_rows(WORKFLOW_VERSION_KEYSPACE).await.is_empty());
    assert!(env.scan_rows(AUTOMATIONS_KEYSPACE).await.is_empty());

    // Valid preflight commits workflow v1 + paired automation pinned to (workflow_id, 1).
    host.refuse.store(false, Ordering::Relaxed);
    let saved = drive(
        SaveBundleOperation::new(&env.owner, env.node, make_bundle(None, None, None, "v1")),
        &env.ctx,
    )
    .await
    .unwrap();
    let workflow_id = saved.workflow.workflow_id;
    let automation_id = saved.automation.automation_id;
    assert_eq!(saved.workflow.version_count, 1);
    assert_eq!(saved.workflow.automation_id, Some(automation_id));
    assert_eq!(saved.automation.workflow_id, Some(workflow_id));
    assert_eq!(saved.created.as_ref().map(|item| item.version), Some(1));
    assert_eq!(
        saved.automation.spec.scope.workflows,
        vec![(workflow_id, 1)]
    );
    assert!(matches!(
        saved.automation.spec.stages[0].kind,
        StageKind::Run {
            workflow_id: stage_wf,
            version: 1,
            ..
        } if stage_wf == workflow_id
    ));

    // Stale expected_revision conflicts without committing workflow v2.
    let stale = drive(
        SaveBundleOperation::new(
            &env.owner,
            env.node,
            make_bundle(Some(workflow_id), Some(automation_id), Some(99), "v2"),
        ),
        &env.ctx,
    )
    .await;
    assert!(matches!(stale, Err(AutoError::Conflict(_))), "{stale:?}");
    let stored_workflow = WorkflowRecord::from_bytes(
        &env.read_row(WORKFLOW_KEYSPACE, &workflow_key(workflow_id))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(stored_workflow.version_count, 1);

    // Matching revision atomically advances to workflow v2 and re-pins the run stage to v2.
    let updated = drive(
        SaveBundleOperation::new(
            &env.owner,
            env.node,
            make_bundle(
                Some(workflow_id),
                Some(automation_id),
                Some(saved.automation.revision),
                "v2",
            ),
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(updated.workflow.version_count, 2);
    assert_eq!(
        updated.automation.spec.scope.workflows,
        vec![(workflow_id, 2)]
    );

    // Listing by workflow_id matches linked automations and excludes unrelated ones.
    let _unrelated = env.create_active(env.base_spec()).await;
    let filtered = drive(
        ListAutosOperation::new(&env.owner, (env.group, env.node), None, None, 20)
            .with_workflow(Some(workflow_id)),
        &env.ctx,
    )
    .await
    .unwrap();
    assert_eq!(filtered.items.len(), 1);
    assert_eq!(filtered.items[0].automation_id, automation_id);
}
