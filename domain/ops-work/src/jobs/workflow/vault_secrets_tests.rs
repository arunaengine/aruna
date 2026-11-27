//! Tests workflow vault secret release and rule enforcement.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::node::vault::NodeVaultKey;
use aruna_core::realm_vault::RealmKeys;
use aruna_core::secrets::{ResponseExposure, SecretName, UseRule};
use aruna_core::structs::execution::job::identity::JobId;
use aruna_ops_identity::secrets::SecretChange;
use aruna_ops_identity::secrets::node::{NodeChange, NodeSecretOperation};
use aruna_ops_identity::secrets::owner::{OwnerChange, OwnerSecretOperation};
use aruna_runner::driver::drive;
use aruna_test_harness::plugins::{Fixture, REALM_KEY};

fn secret_name(name: &str) -> SecretName {
    SecretName::try_from(name.to_string()).unwrap()
}

fn bearer_fields(token: &str) -> SecretFields {
    SecretFields::new(BTreeMap::from([(
        "token".to_string(),
        Secret::new(token.to_string()),
    )]))
}

// Node and user secrets release into `vault_env`, while an unauthorized group secret fails
// permanently and leaves an audit row.
#[tokio::test]
async fn releases_workflow_secrets() {
    let fixture = Fixture::keyed().await;
    fixture
        .ctx
        .storage_handle
        .open_vault(NodeVaultKey::random());
    let realm_key = ed25519_dalek::SigningKey::from_bytes(&REALM_KEY);
    fixture
        .ctx
        .storage_handle
        .open_realm(RealmKeys::derive(&realm_key));
    let ctx = &fixture.ctx;
    let realm_id = fixture.actor.realm_id;
    let node_id = fixture.actor.node_id;
    let group_id = fixture.group_id;
    let user_id = fixture.actor.user_id;
    let workflow_id = Ulid::from_bytes([8; 16]);
    let run = RunIdentity {
        job_id: JobId::from_bytes([7; 16]),
        user_id,
        group_id,
        realm_id,
        node_id,
    };

    let user_create = OwnerChange {
        realm_id,
        node_id,
        owner: SecretOwner::User(user_id),
        actor: user_id,
        change: SecretChange::Create {
            name: secret_name("my-api-key"),
            kind: SecretKind::parse("bearer").unwrap(),
            description: String::new(),
            settings: BTreeMap::new(),
            uses: Vec::new(),
            fields: bearer_fields("user-secret-val"),
        },
        readable: false,
        now_ms: 10,
        write_ids: [
            Ulid::from_parts(10, 1),
            Ulid::from_parts(10, 2),
            Ulid::from_parts(10, 3),
        ],
    };
    drive(OwnerSecretOperation::new(user_create), ctx)
        .await
        .unwrap();

    let node_rule = UseRule {
        consumer: SecretConsumer::Workflow { workflow_id },
        delivery: Delivery::Value,
        origins: Vec::new(),
        paths: Vec::new(),
        methods: Vec::new(),
        response: ResponseExposure::Status,
        groups: vec![group_id],
    };
    let node_create = NodeChange {
        realm_id,
        node_id,
        actor: user_id,
        change: SecretChange::Create {
            name: secret_name("node-token"),
            kind: SecretKind::parse("bearer").unwrap(),
            description: String::new(),
            settings: BTreeMap::new(),
            uses: vec![node_rule],
            fields: bearer_fields("node-secret-val"),
        },
        now_ms: 11,
        write_id: Ulid::from_parts(11, 1),
    };
    drive(NodeSecretOperation::new(node_create), ctx)
        .await
        .unwrap();

    let group_create = OwnerChange {
        realm_id,
        node_id,
        owner: SecretOwner::Group(group_id),
        actor: user_id,
        change: SecretChange::Create {
            name: secret_name("locked-key"),
            kind: SecretKind::parse("bearer").unwrap(),
            description: String::new(),
            settings: BTreeMap::new(),
            uses: Vec::new(),
            fields: bearer_fields("must-not-leak"),
        },
        readable: false,
        now_ms: 12,
        write_ids: [
            Ulid::from_parts(12, 1),
            Ulid::from_parts(12, 2),
            Ulid::from_parts(12, 3),
        ],
    };
    drive(OwnerSecretOperation::new(group_create), ctx)
        .await
        .unwrap();

    let run_id = Ulid::from_bytes([9; 16]);
    let bindings = BTreeMap::from([
        (
            "USER_TOKEN".to_string(),
            TaskSecretBinding {
                workflow_id,
                scope: WorkflowSecretScope::User,
                name: secret_name("my-api-key"),
                field: None,
            },
        ),
        (
            "NODE_TOKEN".to_string(),
            TaskSecretBinding {
                workflow_id,
                scope: WorkflowSecretScope::Node,
                name: secret_name("node-token"),
                field: None,
            },
        ),
    ]);
    // Without a stored run record, release fails closed instead of trusting task tags.
    let missing = drive(ReleaseVaultSecrets::new(run, run_id, bindings.clone()), ctx)
        .await
        .unwrap_err();
    assert!(missing.message.contains("not found"), "{missing:?}");

    let mut valid_record = WorkflowRunRecord {
        run_id,
        realm_id,
        group_id,
        workflow_id,
        workflow_name: "demo".to_string(),
        version: 1,
        lineage: run_id,
        state: aruna_core::structs::execution::workflow_run::RunState::Running,
        node_id,
        created_by: user_id,
        created_at_ms: 10,
        finished_at_ms: None,
        error: None,
        values_json: "{}".to_string(),
        inputs_json: None,
        output: None,
        outputs: Vec::new(),
        provenance_crate: false,
        provenance_document_id: None,
        provenance_status: None,
        last_event_seq: 0,
        local_only: false,
        origin: None,
        parent_job_id: None,
        claim_token_digest: None,
        attempts: 1,
        steps: Vec::new(),
        resumes_run_id: None,
        repeats_run_id: None,
        cache_scope: aruna_core::structs::execution::workflow_run::CacheScope::Run,
        placement_site: None,
        protected: false,
        started_at_ms: Some(10),
        secrets: BTreeMap::from([
            (
                "USER_TOKEN".to_string(),
                aruna_core::structs::execution::workflow_run::WorkflowSecretBinding {
                    scope: WorkflowSecretScope::User,
                    name: secret_name("my-api-key"),
                    field: None,
                },
            ),
            (
                "NODE_TOKEN".to_string(),
                aruna_core::structs::execution::workflow_run::WorkflowSecretBinding {
                    scope: WorkflowSecretScope::Node,
                    name: secret_name("node-token"),
                    field: None,
                },
            ),
            (
                "LOCKED".to_string(),
                aruna_core::structs::execution::workflow_run::WorkflowSecretBinding {
                    scope: WorkflowSecretScope::Group,
                    name: secret_name("locked-key"),
                    field: None,
                },
            ),
        ]),
        path_restrictions: None,
        captured_remote: Vec::new(),
        directory_pins: Vec::new(),
        directory_roots: Vec::new(),
        used_tools: Vec::new(),
        limits: None,
        automation: None,
        applet: None,
    };
    ctx.storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: WORKFLOW_RUN_KEYSPACE.to_string(),
            key: run_key(run_id),
            value: valid_record.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;

    let released = drive(ReleaseVaultSecrets::new(run, run_id, bindings), ctx)
        .await
        .unwrap();
    assert_eq!(released["USER_TOKEN"].expose(), "user-secret-val");
    assert_eq!(released["NODE_TOKEN"].expose(), "node-secret-val");

    let denied = BTreeMap::from([(
        "LOCKED".to_string(),
        TaskSecretBinding {
            workflow_id,
            scope: WorkflowSecretScope::Group,
            name: secret_name("locked-key"),
            field: None,
        },
    )]);
    let err = drive(ReleaseVaultSecrets::new(run, run_id, denied), ctx)
        .await
        .unwrap_err();
    assert!(err.message.contains("refused"), "{err:?}");

    // A stored run record must match the claimed workflow and secret bindings.
    valid_record.workflow_id = Ulid::from_bytes([1; 16]);
    valid_record.secrets.clear();
    ctx.storage_handle
        .send_storage_effect(StorageEffect::Write {
            key_space: WORKFLOW_RUN_KEYSPACE.to_string(),
            key: run_key(run_id),
            value: valid_record.to_bytes().unwrap().into(),
            txn_id: None,
        })
        .await;
    let forged = BTreeMap::from([(
        "NODE_TOKEN".to_string(),
        TaskSecretBinding {
            workflow_id,
            scope: WorkflowSecretScope::Node,
            name: secret_name("node-token"),
            field: None,
        },
    )]);
    let err = drive(ReleaseVaultSecrets::new(run, run_id, forged), ctx)
        .await
        .unwrap_err();
    assert!(err.message.contains("does not match"), "{err:?}");
}
