//! Tests event delivery: a restart with rows at every stage runs each event once, also sync ones;
//! grants, pause and removal stop delivery; sync jobs and remote sync rows run once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::keyspaces::{DOCUMENT_STATE_KEYSPACE, PLUGIN_OUTBOX_KEYSPACE};
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::plugin_runs::{ForwardRow, RunTrigger, SourceEvent};
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::time::unix_timestamp_millis;

use crate::driver::drive;
use crate::plugins::decide::decide_batch;
use crate::plugins::forward::{Backoff, Decisions, EventBatch, Outcome, send_outbox};
use crate::plugins::install::tests::Fixture;
use crate::plugins::lifecycle::{pause_install, set_grants};
use crate::plugins::matcher::match_pending;
use crate::plugins::matcher::tests::{config, forwarded, journal};
use crate::plugins::replay::plan_replays;
use crate::plugins::runs::list_runs;
use crate::plugins::store::Txn;
use crate::plugins::worker::tests::{
    created, echoed_sync, install_artifact, installed, synced, worker,
};
use crate::replication::protocol::ReplicationMode;
use crate::replication::queue::{QueueBlobOperation, process_blob_batch};
use crate::replication::version_replication::{ReplicateScopeInput, ReplicateScopeTarget};
use crate::sync::sync_relationship::StoreRelationshipOperation;
use crate::sync::sync_relationship::SyncRelationshipDirection::Outgoing;
use aruna_core::keyspaces::S3_BUCKET_KEYSPACE;
use aruna_core::plugin_runs::RunState;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::format::Compression;
use aruna_core::structs::storage::replication::ArunaArn;
use aruna_core::structs::{SyncMode, SyncRelationship, SyncState, SyncStatusSnapshot};
use std::time::SystemTime;
use ulid::Ulid;

/// A created dataset journaled at `at`, with its origin anchor there too.
fn dataset(install: &PluginInstall, resource: &str, at: u64) -> SourceEvent {
    let mut event = created(install, resource).0;
    (event.journaled_ms, event.anchor_ms) = (at, at);
    event
}

/// The resources of the install's runs, sorted.
async fn ran(fixture: &Fixture, install: &PluginInstall) -> Vec<String> {
    let runs = list_runs(&fixture.ctx, install.install_id, None, 100).await;
    let mut resources: Vec<String> = (runs.unwrap().0.into_iter())
        .filter_map(|run| match run.trigger {
            RunTrigger::Event(event) => Some(event.resource),
            _ => None,
        })
        .collect();
    resources.sort();
    resources
}

/// Decides `rows` on this node as their receiver.
async fn decide(fixture: &Fixture, rows: Vec<ForwardRow>, now: u64) -> Decisions {
    let nodes = (fixture.actor.node_id, fixture.actor.node_id);
    let batch = EventBatch {
        events: rows,
        gaps: Vec::new(),
    };
    let config = config(fixture).await;
    decide_batch(&fixture.ctx, &config, nodes, batch, now)
        .await
        .unwrap()
}

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
    match_pending(&fixture.ctx, &bare, local, now)
        .await
        .unwrap();
    let no_clock = |_| async { None };
    plan_replays(&fixture.ctx, &config, local, now, no_clock)
        .await
        .unwrap();
    journal(&fixture, &dataset(&install, "outbox", now - 3)).await;
    match_pending(&fixture.ctx, &config, local, now)
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
    match_pending(&fixture.ctx, &config(&fixture).await, local, now)
        .await
        .unwrap();
    let base = created(&install, "").1;
    let narrowed = BTreeMap::from([("metadata.read".into(), vec![format!("{base}/A/**")])]);
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
async fn pause_and_removal() {
    let fixture = Fixture::new().await;
    let install = installed(&fixture).await;
    let (local, now) = (fixture.actor.node_id, unix_timestamp_millis());
    for (resource, at) in [("D1", now - 1), ("D2", now)] {
        journal(&fixture, &dataset(&install, resource, at)).await;
    }
    match_pending(&fixture.ctx, &config(&fixture).await, local, now)
        .await
        .unwrap();
    let (auth, id) = (fixture.auth(), install.install_id);
    pause_install(&fixture.ctx, &fixture.actor, &auth, id)
        .await
        .unwrap();
    let paused = ForwardRow {
        install_id: id,
        revision: install.revision,
        event: dataset(&install, "D1", now - 1),
        queued_ms: now,
    };
    let decided = decide(&fixture, vec![paused], now).await;
    assert!(matches!(&decided.events[0].2, Outcome::Denied(reason) if reason.contains("paused")));
    // Once the install is removing, its senders drop the rows they still hold.
    let mut removing = config(&fixture).await;
    removing.installs[0].state = InstallState::Removing;
    let unsent = |_, _| std::future::ready(Err::<Decisions, _>("not called".to_string()));
    let mut backoff = Backoff::default();
    send_outbox(&fixture.ctx, &removing, now, &mut backoff, unsent)
        .await
        .unwrap();
    let txn = Txn::direct(&fixture.ctx);
    assert!(
        txn.scan(PLUGIN_OUTBOX_KEYSPACE, &[])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(ran(&fixture, &install).await.is_empty());
}

/// A sync event of `install` journaled at `at`, for the relationship numbered `relationship`.
fn sync_at(install: &PluginInstall, relationship: u128, at: u64) -> SourceEvent {
    let mut event = synced(install, "raw/", Ulid::from_parts(relationship as u64, 1));
    (event.journaled_ms, event.anchor_ms) = (at, at);
    event
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

    let drained = process_blob_batch(&fixture.ctx).await.unwrap();
    assert_eq!(drained.succeeded, 1);
    for _ in 0..2 {
        let worker = worker(&fixture);
        worker.claim(unix_timestamp_millis()).await.unwrap();
        worker.settle().await;
    }

    let runs = list_runs(&fixture.ctx, install.install_id, None, 100).await;
    let runs = runs.unwrap().0;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].state, RunState::Succeeded, "{:?}", runs[0].message);
    let RunTrigger::Event(event) = &runs[0].trigger else {
        panic!("a sync event starts the run")
    };
    let answer = format!(
        "change sync_completed {} {}",
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
    match_pending(&fixture.ctx, &bare, local, now)
        .await
        .unwrap();
    let no_clock = |_| async { None };
    plan_replays(&fixture.ctx, &config, local, now, no_clock)
        .await
        .unwrap();
    journal(&fixture, &sync_at(&install, 2, now - 3)).await;
    match_pending(&fixture.ctx, &config, local, now)
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
