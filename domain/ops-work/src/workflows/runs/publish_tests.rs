//! Tests local device workflow run admission, crate reads and publish guards.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, WORKFLOW_RUN_KEYSPACE};
use aruna_core::structs::execution::workflow_run::{RunOutput, RunState, run_key};
use aruna_core::structs::identity::realm::RealmNodeKind;
use aruna_runner::driver::drive;
use serde_json::json;

use super::super::admit::{RunRequest, StartRunOperation};
use super::super::provenance::WriteProvenanceOperation;
use super::super::tests::runs;
use super::super::{ChangeRunOperation, RunChange};
use crate::workflows::CatalogError;
use crate::workflows::tests::write;

async fn configure_device(runs: &super::super::tests::Runs, owner: UserId) {
    let fixture = &runs.fixture;
    let realm_id = fixture.alice.realm_id;
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.ensure_node(fixture.node, RealmNodeKind::User { owner });
    config.seed_default_placement();
    let actor = Actor {
        node_id: fixture.node,
        user_id: owner,
        realm_id,
    };
    let bytes = config.to_bytes(&actor).unwrap();
    write(
        &fixture.context,
        REALM_CONFIG_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        bytes,
    )
    .await;
}

// Starting a run on a user device marks it local_only, refuses non-owners, keeps its crate local,
// and exposes that crate through local_device_crate by both run_id and parent_job_id.
#[tokio::test]
async fn keeps_device_workflow() {
    let runs = runs().await;
    let ctx = &runs.fixture.context;
    let request = RunRequest {
        group_id: runs.fixture.group_a,
        workflow_id: runs.workflow_id,
        version: 1,
        values: json!({"message": "hi"}),
        inputs: Vec::new(),
        inputs_json: None,
        output: Some(RunOutput {
            bucket: "results".to_owned(),
            prefix: "local/".to_owned(),
            node_id: None,
            dataset_json: None,
        }),
        provenance_crate: true,
        idempotency_key: None,
        placement_site: None,
        secrets: Default::default(),
    };

    // Even a member with group WRITE cannot start a local run on someone else's device.
    configure_device(&runs, runs.fixture.bob.user_id).await;
    let foreign = StartRunOperation::new(&runs.fixture.alice, runs.serving(), request.clone());
    assert!(matches!(
        drive(foreign, ctx).await,
        Err(CatalogError::Forbidden)
    ));

    configure_device(&runs, runs.fixture.alice.user_id).await;
    let admitted = drive(
        StartRunOperation::new(&runs.fixture.alice, runs.serving(), request),
        ctx,
    )
    .await
    .unwrap();
    assert!(admitted.record.local_only);
    let parent_job_id = admitted.record.parent_job_id.expect("local parent job");
    let run_id = admitted.record.run_id;

    // A non-terminal run cannot be published yet.
    let target = RemoteBinding {
        node_id: aruna_ops_identity::testing::node(9),
        bucket: "lab-data".to_owned(),
        prefix: "runs/out".to_owned(),
    };
    let pending = DevicePublishInput {
        id: run_id,
        owner: runs.fixture.alice.user_id,
        node_id: runs.fixture.node,
        target: target.clone(),
    };
    assert_eq!(
        drive(publish_device_run(Some(runs.serving()), pending), ctx).await,
        Err(PublishRunError::NotFinished)
    );

    // Finish the run and write its local crate.
    drive(change_call_op(run_id, RunState::Succeeded), ctx)
        .await
        .unwrap();
    let written = drive(WriteProvenanceOperation::new(run_id, runs.serving()), ctx)
        .await
        .unwrap();
    assert_eq!(written.provenance_status.as_deref(), Some(status::LOCAL));

    // Readable by run_id and parent_job_id for owner, hidden from other users.
    let by_run = drive(local_device_crate(run_id, runs.fixture.alice.user_id), ctx)
        .await
        .unwrap();
    let by_job = drive(
        local_device_crate(parent_job_id.as_ulid(), runs.fixture.alice.user_id),
        ctx,
    )
    .await
    .unwrap();
    assert_eq!(by_run, by_job);
    assert_eq!(
        drive(local_device_crate(run_id, runs.fixture.bob.user_id), ctx,).await,
        Err(PublishRunError::NotFound)
    );

    // Protected local runs refuse plain realm publication.
    let mut protected = written.clone();
    protected.protected = true;
    write(
        ctx,
        WORKFLOW_RUN_KEYSPACE,
        run_key(run_id).to_vec(),
        protected.to_bytes().unwrap(),
    )
    .await;
    let blocked = DevicePublishInput {
        id: run_id,
        owner: runs.fixture.alice.user_id,
        node_id: runs.fixture.node,
        target,
    };
    assert!(matches!(
        drive(publish_device_run(Some(runs.serving()), blocked), ctx).await,
        Err(PublishRunError::Refused {
            refusal: SyncRefusal::Invalid(_),
            ..
        })
    ));
}

fn change_call_op(run_id: Ulid, state: RunState) -> ChangeRunOperation {
    ChangeRunOperation::new(run_id, vec![RunChange::State { state, error: None }])
}
