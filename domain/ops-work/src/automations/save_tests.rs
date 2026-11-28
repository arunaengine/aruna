//! Tests atomic single-transaction workflow + automation saves, bidirectional links and identity sync.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use aruna_compute::registry::ExecutorRegistry;
use aruna_core::automation::record::{
    Automation, Lifecycle, Spec, Stage, StageKind, Trigger, When,
};
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
use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::document_repository::GetConfigError;
use aruna_runner::driver::drive;
use aruna_workflow_contract::preflight::{GraphNode, NodeKind, WorkflowGraph};
use aruna_workflow_contract::{ContractError, PreflightKind, PreflightResult};
use serde_json::json;
use ulid::Ulid;

use super::{SaveAutomation, SaveBundle, SaveBundleOperation, SaveWorkflow, SavedBundle};
use crate::automations::AutoError;
use crate::automations::change::{AutoChange, CreateAutoOperation, NewAuto};
use crate::automations::flow_tests::Env;
use crate::automations::lifecycle::ChangeAutoOperation;
use crate::automations::reads::{ListAutosOperation, LoadAutoOperation};
use crate::automations::upkeep::AutoUpkeepOperation;
use crate::workflows::CatalogError;
use crate::workflows::import::{Prepared, SourceInput};
use crate::workflows::reads::DeleteWorkflowOperation;

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

fn enabled_config(realm: aruna_core::structs::identity::realm::RealmId) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm, Vec::new());
    for feature in [Feature::Workflows, Feature::Automations, Feature::Applets] {
        config.features.insert(
            feature,
            FeatureMode::Scoped {
                default: true,
                groups: Default::default(),
            },
        );
    }
    config
}

fn sample_prepared() -> Prepared {
    Prepared {
        language: Some(StoredWorkflowLanguage::Wir),
        entrypoint: crate::workflows::import::WIR_ENTRY.into(),
        ..Prepared::default()
    }
}

#[test]
fn steps_save_phases() {
    let realm = aruna_core::structs::identity::realm::RealmId([4; 32]);
    let node = iroh::SecretKey::from_bytes(&[5; 32]).public();
    let group_id = Ulid::from_bytes([6; 16]);
    let workflow_id = Ulid::from_bytes([7; 16]);
    let automation_id = Ulid::from_bytes([8; 16]);
    let auth = aruna_core::structs::identity::auth::AuthContext::new(
        aruna_core::user::id::UserId::local(Ulid::from_bytes([1; 16]), realm),
        realm,
    );

    // 1. first() validation refusals: missing source, missing name, missing expected_revision.
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
    assert!(matches!(
        SaveBundleOperation::new(&auth, node, missing_source).first(),
        Err(AutoError::Invalid(_))
    ));

    let valid_bundle = SaveBundle {
        group_id,
        workflow: SaveWorkflow {
            workflow_id: Some(workflow_id),
            name: Some("qc".into()),
            description: None,
            language: Some(StoredWorkflowLanguage::Wir),
            visibility: Some(WorkflowVisibility::Group),
            message: Some("initial".into()),
            source: Some(SourceInput::Wir(json!({"wir_version": "1"}))),
        },
        automation: SaveAutomation {
            automation_id: Some(automation_id),
            expected_revision: Some(2),
            name: Some("qc-auto".into()),
            description: None,
            spec: placeholder_spec(),
            pin_version: true,
        },
    };

    // 2. Disabled Workflows feature refuses inside Config phase.
    let mut gated = SaveBundleOperation::new(&auth, node, valid_bundle.clone());
    assert!(matches!(gated.first(), Ok(Next::Call(IoCall::Child(_)))));
    let mut disabled = RealmConfigDocument::default_for_realm(realm, Vec::new());
    disabled.features.insert(
        Feature::Workflows,
        FeatureMode::Scoped {
            default: false,
            groups: Default::default(),
        },
    );
    assert!(matches!(
        gated.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            disabled
        )))),
        Err(AutoError::FeatureOff(Feature::Workflows, _))
    ));

    // 3. Group auth denial and workflow auth denial both return Forbidden.
    let mut group_denied = SaveBundleOperation::new(&auth, node, valid_bundle.clone());
    group_denied.first().unwrap();
    group_denied
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            enabled_config(realm),
        ))))
        .unwrap();
    assert_eq!(
        group_denied.next(IoResult::Child(ChildOutcome::new(
            Err::<(), AuthorizeError>(AuthorizeError::PermissionDenied)
        ))),
        Err(AutoError::Forbidden)
    );

    let mut workflow_denied = SaveBundleOperation::new(&auth, node, valid_bundle.clone());
    workflow_denied.first().unwrap();
    workflow_denied
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            enabled_config(realm),
        ))))
        .unwrap();
    workflow_denied
        .next(IoResult::Child(ChildOutcome::new(
            Ok::<(), AuthorizeError>(()),
        )))
        .unwrap();
    assert_eq!(
        workflow_denied.next(IoResult::Child(ChildOutcome::new(
            Err::<(), AuthorizeError>(AuthorizeError::PermissionDenied)
        ))),
        Err(AutoError::Forbidden)
    );

    // 4. Cross-group workflow and stale automation revision refuse before Prepare.
    let foreign_workflow = WorkflowRecord {
        workflow_id,
        group_id: Ulid::from_bytes([99; 16]),
        name: "other".into(),
        description: None,
        language: StoredWorkflowLanguage::Wir,
        visibility: WorkflowVisibility::Group,
        version_count: 1,
        edam_formats: Vec::new(),
        created_by: auth.user_id,
        created_at_ms: 1,
        updated_at_ms: 1,
        automation_id: None,
    };
    let mut cross_group = SaveBundleOperation::new(&auth, node, valid_bundle.clone());
    cross_group.first().unwrap();
    cross_group
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            enabled_config(realm),
        ))))
        .unwrap();
    cross_group
        .next(IoResult::Child(ChildOutcome::new(
            Ok::<(), AuthorizeError>(()),
        )))
        .unwrap();
    cross_group
        .next(IoResult::Child(ChildOutcome::new(
            Ok::<(), AuthorizeError>(()),
        )))
        .unwrap();
    assert!(matches!(
        cross_group.next(IoResult::Values(vec![Some(
            foreign_workflow.to_bytes().unwrap().into()
        )])),
        Err(AutoError::Invalid(_))
    ));

    // 5. Full flow through LoadWorkflow -> LoadAutomation -> Prepare -> Commit -> Sync tolerance.
    let mut own_workflow = foreign_workflow;
    own_workflow.group_id = group_id;
    let existing_auto = Automation {
        automation_id,
        group_id,
        name: "qc-auto".into(),
        description: String::new(),
        host_node: node,
        lifecycle: Lifecycle::Active,
        pause_reason: None,
        spec: placeholder_spec(),
        pending: None,
        account: Some(auth.user_id),
        role_id: Some(automation_id),
        approved_by: Some(auth.user_id),
        approved_at_ms: Some(1),
        created_by: auth.user_id,
        created_at_ms: 1,
        updated_at_ms: 1,
        revision: 2,
        identity_synced_revision: 2,
        workflow_id: Some(workflow_id),
    };

    // Stale revision fails at LoadAutomation.
    let mut stale_auto = existing_auto.clone();
    stale_auto.revision = 5;
    let mut stale_op = SaveBundleOperation::new(&auth, node, valid_bundle.clone());
    stale_op.first().unwrap();
    stale_op
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
            enabled_config(realm),
        ))))
        .unwrap();
    stale_op
        .next(IoResult::Child(ChildOutcome::new(
            Ok::<(), AuthorizeError>(()),
        )))
        .unwrap();
    stale_op
        .next(IoResult::Child(ChildOutcome::new(
            Ok::<(), AuthorizeError>(()),
        )))
        .unwrap();
    stale_op
        .next(IoResult::Values(vec![Some(
            own_workflow.to_bytes().unwrap().into(),
        )]))
        .unwrap();
    assert!(matches!(
        stale_op.next(IoResult::Values(vec![Some(
            stale_auto.to_bytes().unwrap().into()
        )])),
        Err(AutoError::Conflict(_))
    ));

    // Matching revision runs Prepare -> Commit -> Sync; post-commit Sync failure still returns
    // committed SavedBundle with identity_synced = false so upkeep repairs asynchronously.
    let mut full = SaveBundleOperation::new(&auth, node, valid_bundle);
    full.first().unwrap();
    full.next(IoResult::Child(ChildOutcome::new(Ok::<_, GetConfigError>(
        enabled_config(realm),
    ))))
    .unwrap();
    full.next(IoResult::Child(ChildOutcome::new(
        Ok::<(), AuthorizeError>(()),
    )))
    .unwrap();
    full.next(IoResult::Child(ChildOutcome::new(
        Ok::<(), AuthorizeError>(()),
    )))
    .unwrap();
    full.next(IoResult::Values(vec![Some(
        own_workflow.to_bytes().unwrap().into(),
    )]))
    .unwrap();
    assert!(matches!(
        full.next(IoResult::Values(vec![Some(
            existing_auto.to_bytes().unwrap().into()
        )])),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    assert!(matches!(
        full.next(IoResult::Child(ChildOutcome::new(Ok::<_, CatalogError>(
            sample_prepared()
        )))),
        Ok(Next::Call(IoCall::Child(_)))
    ));
    let mut committed_auto = existing_auto;
    committed_auto.revision = 3;
    let committed = SavedBundle {
        workflow: own_workflow,
        created: None,
        automation: committed_auto,
        identity_synced: false,
    };
    let sync_call = full
        .next(IoResult::Child(ChildOutcome::new(Ok::<_, AutoError>(
            committed,
        ))))
        .unwrap();
    assert!(matches!(sync_call, Next::Call(IoCall::Child(_))));
    let Next::Done(done) = full
        .next(IoResult::Child(ChildOutcome::new(Err::<
            Automation,
            AutoError,
        >(
            AutoError::Storage("transient role write error".into()),
        ))))
        .unwrap()
    else {
        panic!("expected Done after post-commit sync failure");
    };
    assert!(!done.identity_synced);
    assert!(done.automation.needs_identity_sync());
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
    assert!(saved.identity_synced);
    assert_eq!(saved.created.as_ref().map(|item| item.version), Some(1));
    assert_eq!(
        saved.automation.spec.scope.workflows,
        vec![(workflow_id, 1)]
    );

    // Saving a second automation against the already-paired workflow conflicts cleanly.
    let conflict = drive(
        SaveBundleOperation::new(
            &env.owner,
            env.node,
            make_bundle(Some(workflow_id), None, None, "conflict"),
        ),
        &env.ctx,
    )
    .await;
    assert!(
        matches!(conflict, Err(AutoError::Conflict(_))),
        "{conflict:?}"
    );

    // Standalone create against a missing or already-paired workflow is refused.
    let missing_link = drive(
        CreateAutoOperation::new(
            &env.owner,
            env.node,
            NewAuto {
                group_id: env.group,
                name: "orphan".into(),
                description: String::new(),
                spec: env.base_spec(),
                workflow_id: Some(Ulid::from_bytes([77; 16])),
            },
        ),
        &env.ctx,
    )
    .await;
    assert_eq!(missing_link, Err(AutoError::NotFound));
    let taken_link = drive(
        CreateAutoOperation::new(
            &env.owner,
            env.node,
            NewAuto {
                group_id: env.group,
                name: "taken".into(),
                description: String::new(),
                spec: env.base_spec(),
                workflow_id: Some(workflow_id),
            },
        ),
        &env.ctx,
    )
    .await;
    assert!(matches!(taken_link, Err(AutoError::Conflict(_))));

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

    // Deleting the linked workflow atomically clears `automation.workflow_id`.
    drive(
        DeleteWorkflowOperation::new(&env.owner, workflow_id),
        &env.ctx,
    )
    .await
    .unwrap();
    let unlinked = drive(LoadAutoOperation::new(automation_id), &env.ctx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unlinked.workflow_id, None);

    // Upkeep detects and repairs an active automation whose `identity_synced_revision` drifted.
    let mut drifted = env.create_active(env.base_spec()).await;
    drifted.identity_synced_revision = 0;
    env.write_row(
        AUTOMATIONS_KEYSPACE,
        aruna_core::automation::keys::record_key(drifted.automation_id),
        drifted.to_bytes().unwrap(),
    )
    .await;
    let upkeep = drive(
        AutoUpkeepOperation::new((env.node, env.realm), 100),
        &env.ctx,
    )
    .await
    .unwrap();
    assert!(upkeep.repaired >= 1);
    let repaired = drive(LoadAutoOperation::new(drifted.automation_id), &env.ctx)
        .await
        .unwrap()
        .unwrap();
    assert!(!repaired.needs_identity_sync());

    // Deleting a linked automation clears `workflow.automation_id` atomically.
    let pair = drive(
        SaveBundleOperation::new(&env.owner, env.node, make_bundle(None, None, None, "v1")),
        &env.ctx,
    )
    .await
    .unwrap();
    drive(
        ChangeAutoOperation::new(
            &env.owner,
            (pair.automation.automation_id, None),
            AutoChange::Delete,
            env.node,
        ),
        &env.ctx,
    )
    .await
    .unwrap();
    let cleared_wf = WorkflowRecord::from_bytes(
        &env.read_row(WORKFLOW_KEYSPACE, &workflow_key(pair.workflow.workflow_id))
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(cleared_wf.automation_id, None);
}
