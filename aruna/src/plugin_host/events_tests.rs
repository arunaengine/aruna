//! Tests event runs through restarts and narrowed grants, through the plugin host.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::plugin_host::worker::tests::worker;
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::plugin_runs::EVENTS_QUEUED;
use aruna_core::plugin_runs::EventBatch;
use aruna_core::plugin_runs::ForwardRow;
use aruna_core::plugin_runs::Outcome;
use aruna_core::plugin_runs::RunState;
use aruna_core::plugin_runs::RunTrigger;
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::SyncMode;
use aruna_core::structs::SyncRelationship;
use aruna_core::structs::SyncState;
use aruna_core::structs::SyncStatusSnapshot;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::storage::replication_queue::ReplicateScopeInput;
use aruna_core::structs::storage::replication_queue::ReplicateScopeTarget;
use aruna_core::structs::storage::replication_queue::ReplicationMode;
use aruna_core::time::unix_timestamp_millis;
use aruna_operations::driver::drive;
use aruna_operations::plugins::calls::decide_batch;
use aruna_operations::plugins::calls::match_journal;
use aruna_operations::plugins::calls::plan_replays;
use aruna_operations::plugins::calls::set_grants;
use aruna_operations::plugins::flow_support::events::dataset;
use aruna_operations::plugins::flow_support::events::decide;
use aruna_operations::plugins::flow_support::events::ran;
use aruna_operations::plugins::flow_support::events::sync_at;
use aruna_operations::plugins::flow_support::matcher::config;
use aruna_operations::plugins::flow_support::matcher::forwarded;
use aruna_operations::plugins::flow_support::matcher::journal;
use aruna_operations::plugins::runs::list_runs;
use aruna_operations::plugins::store::Txn;
use aruna_operations::plugins::test_support::Fixture;
use aruna_operations::plugins::test_support::created;
use aruna_operations::plugins::test_support::drain_blob_batch;
use aruna_operations::plugins::test_support::echoed_sync;
use aruna_operations::plugins::test_support::install_artifact;
use aruna_operations::plugins::test_support::installed;
use aruna_operations::plugins::test_support::scoped;
use aruna_operations::replication::queue::QueueBlobOperation;
use aruna_operations::sync::sync_relationship::StoreRelationshipOperation;
use aruna_operations::sync::sync_relationship::SyncRelationshipDirection::Outgoing;
use std::time::SystemTime;
use ulid::Ulid;

#[tokio::test]
async fn restart_runs_once() {
    // Rows wait in the replay, the journal, the outbox and the waiting set when the node stops.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now) = (fixture.actor.node_id, unix_timestamp_millis());
    let account = reducer_state_key(&install.prerequisites[0].target).to_vec();
    let txn = Txn::direct(&fixture.ctx);
    let state = txn.read(DOCUMENT_STATE_KEYSPACE, &account).await.unwrap();
    txn.delete(DOCUMENT_STATE_KEYSPACE, vec![account.clone()])
        .await
        .unwrap();
    let config = config(&fixture).await;
    let mut bare = config.clone();
    bare.installs.clear();
    journal(&fixture, &dataset(&install, "replayed", now - 4)).await;
    match_journal(&fixture.ctx, &bare, local, (now, EVENTS_QUEUED))
        .await
        .unwrap();
    let no_clock = |_| async { None };
    plan_replays(&fixture.ctx, &config, local, now, no_clock)
        .await
        .unwrap();
    journal(&fixture, &dataset(&install, "outbox", now - 3)).await;
    match_journal(&fixture.ctx, &config, local, (now, EVENTS_QUEUED))
        .await
        .unwrap();
    journal(&fixture, &dataset(&install, "journal", now - 2)).await;
    let waiting = ForwardRow {
        install_id: install.install_id,
        revision: install.revision,
        event: dataset(&install, "waiting", now - 1),
        queued_ms: now,
    };
    let decided = decide(&fixture, vec![waiting], now).await;
    assert_eq!(decided.events[0].2, Outcome::Waiting);

    // The restarted node delivers everything; once the account's writes arrive, all run.
    worker(&fixture).claim(now).await.unwrap();
    assert!(ran(&fixture, &install).await.is_empty());
    let state = vec![(account, state.unwrap())];
    txn.write(DOCUMENT_STATE_KEYSPACE, state).await.unwrap();
    for _ in 0..2 {
        let restarted = worker(&fixture);
        restarted.claim(now).await.unwrap();
        restarted.settle().await;
    }
    let expected = ["journal", "outbox", "replayed", "waiting"];
    assert_eq!(ran(&fixture, &install).await, expected);
    assert!(forwarded(&fixture, &install).await.is_empty());
}

#[tokio::test]
async fn narrowed_grant_stops() {
    // A row matched before the grant shrank is denied; a later event is not matched at all.
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now) = (fixture.actor.node_id, unix_timestamp_millis());
    journal(&fixture, &dataset(&install, "B/one", now - 1)).await;
    match_journal(
        &fixture.ctx,
        &config(&fixture).await,
        local,
        (now, EVENTS_QUEUED),
    )
    .await
    .unwrap();
    let base = created(&install, "").1;
    let narrowed = scoped(vec![format!("{base}/A/**")]);
    let (auth, id) = (fixture.auth(), install.install_id);
    set_grants(&fixture.ctx, &fixture.actor, &auth, id, &narrowed)
        .await
        .unwrap();
    journal(&fixture, &dataset(&install, "B/two", now)).await;
    worker(&fixture).claim(now).await.unwrap();
    assert!(forwarded(&fixture, &install).await.is_empty());
    assert!(ran(&fixture, &install).await.is_empty());
}

#[tokio::test]
async fn sync_job_runs() {
    // A sync job of the install's group completes on this node; its retirement journals the
    // event, and the worker runs the `sync_completed` handler with it once.
    let fixture = Fixture::new().await;
    let install = install_artifact(&fixture, echoed_sync()).await;
    let realm_id = fixture.actor.realm_id;
    let (source, target) = (
        fixture.actor.node_id,
        iroh::SecretKey::from_bytes(&[8; 32]).public(),
    );
    let info = BucketInfo {
        group_id: fixture.group_id,
        created_at: SystemTime::UNIX_EPOCH,
        created_by: fixture.actor.user_id,
        cors_configuration: None,
        storage_routing: Vec::new(),
        placement_policies: Vec::new(),
        placement_policy_generation: 0,
        compression: Compression::Off,
        distribution: None,
    };
    let bucket = vec![(b"bucket".to_vec(), info.to_bytes().unwrap())];
    let txn = Txn::direct(&fixture.ctx);
    txn.write(S3_BUCKET_KEYSPACE, bucket).await.unwrap();
    let relationship = SyncRelationship {
        id: Ulid::from_parts(4, 4),
        source: ArunaArn::s3_bucket(realm_id, source, "bucket").unwrap(),
        target: ArunaArn::s3_bucket(realm_id, target, "bucket").unwrap(),
        mode: SyncMode::Continuous,
        reference_handling: Default::default(),
        reference_serving: false,
        replicate_deletes: true,
        created_by: fixture.actor.user_id,
        created_at: SystemTime::UNIX_EPOCH,
        state: SyncState::Enabled,
        status: SyncStatusSnapshot::default(),
    };
    let store = StoreRelationshipOperation::new(relationship.clone(), Outgoing);
    drive(store, &fixture.ctx).await.unwrap();
    let input = ReplicateScopeInput {
        bucket: "bucket".to_string(),
        target: ReplicateScopeTarget::Bucket,
        target_node_id: target,
        auth_context: fixture.auth(),
        replicate_delete_markers: true,
        mode: ReplicationMode::OnDemand,
    };
    let queue = QueueBlobOperation::new_relationship(input, None, relationship.id);
    drive(queue, &fixture.ctx).await.unwrap();

    let drained = drain_blob_batch(&fixture.ctx).await;
    assert_eq!(drained.succeeded, 1);
    for _ in 0..2 {
        let worker = worker(&fixture);
        worker.claim(unix_timestamp_millis()).await.unwrap();
        worker.settle().await;
    }

    let runs = drive(list_runs(install.install_id, None, 100), &fixture.ctx).await;
    let runs = runs.unwrap().0;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].state, RunState::Succeeded, "{:?}", runs[0].message);
    let RunTrigger::Event(event) = &runs[0].trigger else {
        panic!("a sync event starts the run")
    };
    let answer = format!(
        "change sync_completed {} {}\"",
        relationship.id, event.revision
    );
    let output = runs[0].output.clone().unwrap_or_default();
    assert!(output.ends_with(&answer), "{output}");
}

#[tokio::test]
async fn remote_sync_runs() {
    // A sync row journaled on another node reaches this receiver and runs once, also when the
    // holder sends it again after a lost acknowledgement.
    let fixture = Fixture::new().await;
    let install = install_artifact(&fixture, echoed_sync()).await;
    let now = unix_timestamp_millis();
    let row = ForwardRow {
        install_id: install.install_id,
        revision: install.revision,
        event: sync_at(&install, 5, now),
        queued_ms: now,
    };
    let holder = iroh::SecretKey::from_bytes(&[7; 32]).public();
    let config = config(&fixture).await;
    for _ in 0..2 {
        let batch = EventBatch {
            events: vec![row.clone()],
            gaps: Vec::new(),
        };
        let nodes = (fixture.actor.node_id, holder);
        decide_batch(&fixture.ctx, &config, nodes, batch, now)
            .await
            .unwrap();
    }
    let worker = worker(&fixture);
    worker.claim(now).await.unwrap();
    worker.settle().await;
    let relationship = Ulid::from_parts(5, 1).to_string();
    assert_eq!(ran(&fixture, &install).await, [relationship]);
}

#[tokio::test]
async fn sync_restart_once() {
    // Sync rows wait in the replay, the journal, the outbox and the waiting set when the node
    // stops; after the restart each runs once.
    let fixture = Fixture::new().await;
    let install = install_artifact(&fixture, echoed_sync()).await;
    let (local, now) = (fixture.actor.node_id, unix_timestamp_millis());
    let account = reducer_state_key(&install.prerequisites[0].target).to_vec();
    let txn = Txn::direct(&fixture.ctx);
    let state = txn.read(DOCUMENT_STATE_KEYSPACE, &account).await.unwrap();
    txn.delete(DOCUMENT_STATE_KEYSPACE, vec![account.clone()])
        .await
        .unwrap();
    let config = config(&fixture).await;
    let mut bare = config.clone();
    bare.installs.clear();
    journal(&fixture, &sync_at(&install, 1, now - 4)).await;
    match_journal(&fixture.ctx, &bare, local, (now, EVENTS_QUEUED))
        .await
        .unwrap();
    let no_clock = |_| async { None };
    plan_replays(&fixture.ctx, &config, local, now, no_clock)
        .await
        .unwrap();
    journal(&fixture, &sync_at(&install, 2, now - 3)).await;
    match_journal(&fixture.ctx, &config, local, (now, EVENTS_QUEUED))
        .await
        .unwrap();
    journal(&fixture, &sync_at(&install, 3, now - 2)).await;
    let waiting = ForwardRow {
        install_id: install.install_id,
        revision: install.revision,
        event: sync_at(&install, 4, now - 1),
        queued_ms: now,
    };
    let decided = decide(&fixture, vec![waiting], now).await;
    assert_eq!(decided.events[0].2, Outcome::Waiting);

    worker(&fixture).claim(now).await.unwrap();
    assert!(ran(&fixture, &install).await.is_empty());
    let state = vec![(account, state.unwrap())];
    txn.write(DOCUMENT_STATE_KEYSPACE, state).await.unwrap();
    for _ in 0..2 {
        let restarted = worker(&fixture);
        restarted.claim(now).await.unwrap();
        restarted.settle().await;
    }
    let mut expected: Vec<_> = (1..=4)
        .map(|n| Ulid::from_parts(n, 1).to_string())
        .collect();
    expected.sort();
    assert_eq!(ran(&fixture, &install).await, expected);
    assert!(forwarded(&fixture, &install).await.is_empty());
}
