//! Tests plugin tasks across nodes: a receiver that holds no family forwards its tasks, only it
//! may admit and cancel them, and it settles them by pull and adopts claims whose answers it lost.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::id::NodeId;
use aruna_core::job_request::{SubmissionScope, submission_of};
use aruna_core::keyspaces::{AUTH_KEYSPACE, PLUGIN_WORK_KEYSPACE};
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::plugins::PublishedPlugin;
use aruna_core::structs::execution::job::identity::JobId;
use aruna_core::structs::execution::job::input::ExecutionSpec;
use aruna_core::structs::identity::realm::RealmAuthorizationDocument;
use aruna_core::time::unix_timestamp_millis;
use aruna_drivers::driver::{DriverContext, drive};
use aruna_operations::jobs::lifecycle::family_reads::FamilyProjectionOperation;
use aruna_operations::jobs::lifecycle::submit_external::IngressNode;
use aruna_operations::jobs::node_inputs::ingress_node;
use aruna_operations::plugins::calls::install_plugin;
use aruna_operations::plugins::install::InstallRequest;
use aruna_operations::plugins::marks::{MARKED, Mark, mark_row};
use aruna_operations::plugins::publish::{PublishEntryConfig, PublishEntryOperation};
use aruna_operations::plugins::rows::{decode, encode};
use aruna_operations::plugins::screen::ScreenTask;
use aruna_operations::plugins::store::Txn;
use aruna_operations::plugins::task::cancel::CancelTask;
use aruna_operations::plugins::task::pass::SettleTasks;
use aruna_operations::plugins::task::row::{TaskRow, task_key};
use aruna_operations::plugins::task::send::SendTask;
use aruna_operations::plugins::task::spec::{TaskIo, task_spec};
use aruna_operations::plugins::work::calls::JobRow;
use aruna_operations::plugins::work::{
    FinalState, Identity, SendBoundary, WORK_ROW, WorkKind, WorkRow, WorkState, reserve,
};
use aruna_operations::s3::object::put::PutObjectOperation;
use aruna_operations::testing_objects::{put_config, seed_bucket};
use aruna_plugin_types::recipe::Recipe;
use serde_json::json;
use ulid::Ulid;

use crate::convergence::wait_for_convergence;
use crate::topology::{self, TestNode, TestResult, Topology, wait_until};

const MANAGEMENT_NODES: usize = 5;
const REPLICATION_FACTOR: u32 = 3;
/// Family shards of the fixture: with three of five nodes holding each, the receiver holds all
/// of them with a chance of 0.6^64, so a foreign family is always found.
const FAMILY_SHARDS: u32 = 64;
/// Work ids tried before a search gives up.
const SEARCH_LIMIT: u64 = 4096;

fn recipe() -> serde_json::Value {
    json!({
        "image": format!("harbor.example/map@sha256:{}", "a".repeat(64)),
        "command": ["/opt/map.sh"],
        "cpu": 1,
        "memory": 1_048_576,
        "time": 60,
    })
}

/// This node as a task send and the settle pass see it.
fn ingress(node: &TestNode) -> IngressNode {
    let context = &node.context;
    let fresh = context.fresh_heartbeats();
    let now = unix_timestamp_millis();
    ingress_node(context.node_and_realm(), fresh, now).expect("a test node serves a network")
}

/// A realm whose group installed a plugin with one task, seen by every node's task check.
async fn installed() -> TestResult<(Topology, PluginInstall)> {
    let mut realm =
        Topology::spawn_sharded(MANAGEMENT_NODES, 0, REPLICATION_FACTOR, FAMILY_SHARDS).await?;
    realm_admin(&realm).await?;
    let group_id = realm.seed_group().await?;
    let node = realm.node(0);
    let manifest = json!({
        "name": "mapper",
        "version": "0.1.0",
        "permissions": { "required": ["jobs.run", "objects.read"] },
        "recipes": { "map": recipe() },
    });
    let plugin = PublishedPlugin {
        name: "mapper".into(),
        version: "0.1.0".into(),
        digest: format!("sha256:{}", "b".repeat(64)),
        size: 10,
        manifest: manifest.to_string(),
        published_by: realm.user_id,
        published_at_ms: 1,
    };
    let publish = PublishEntryOperation::new(PublishEntryConfig {
        actor: realm.actor(node),
        auth_context: realm.auth_context(),
        plugin,
        // The manifest is all a task check reads, so no artifact bytes are stored.
        copies: realm.node_ids().into_iter().collect(),
    });
    let published = drive(publish, node.context.as_ref()).await;
    published.map_err(|error| format!("publish: {error}"))?;
    let request = InstallRequest {
        plugin: "mapper".into(),
        group_id,
        version: None,
        settings: Default::default(),
        grants: BTreeMap::new(),
        install_id: None,
        secrets: BTreeMap::new(),
    };
    let (actor, auth) = (realm.actor(node), realm.auth_context());
    let install = install_plugin(node.context.as_ref(), &actor, &auth, &request).await;
    let install = install.map_err(|error| format!("install: {error}"))?;
    let id = install.install_id;
    let label = "the install never reached every node";
    let has = move |config: &aruna_core::structs::identity::realm::RealmConfigDocument| {
        (config.installs.iter()).any(|install| install.install_id == id)
    };
    realm.await_config(label, has).await?;
    // The task writes to a bucket of the group; every node has one, so a bystander's request
    // reaches a holder.
    for node in &realm.nodes {
        let context = node.context.as_ref();
        seed_bucket(context, "results", group_id, realm.user_id, Vec::new()).await;
    }
    // The account, its roles and grants replicate with the group; every node must screen.
    let probe = task(&install, Ulid::generate());
    let config = Arc::new(realm.config.clone());
    wait_for_convergence::<_, _, Box<dyn std::error::Error>>(label, || async {
        let mut pending = 0;
        for node in realm.nodes.iter() {
            let task = (&probe, install.account, install.receiver);
            let screen = ScreenTask::new(config.clone(), task);
            pending += usize::from(drive(screen, node.context.as_ref()).await.is_err());
        }
        Ok(pending)
    })
    .await?;
    Ok((realm, install))
}

/// Makes the fixture's user a realm admin on every node, as publishing needs.
async fn realm_admin(realm: &Topology) -> TestResult<()> {
    let mut document = RealmAuthorizationDocument::default_realm_doc(realm.realm_id);
    for role in document.roles.values_mut() {
        role.assigned_users.insert(realm.user_id);
    }
    for node in &realm.nodes {
        let bytes = document.to_bytes(&realm.actor(node))?;
        let key = realm.realm_id.as_bytes().to_vec();
        topology::write(node, AUTH_KEYSPACE, key, bytes).await?;
    }
    Ok(())
}

/// The spec of work `work_id`: the task writes below one prefix of the group's results bucket.
fn task(install: &PluginInstall, work_id: Ulid) -> ExecutionSpec {
    let recipe: Recipe = serde_json::from_value(recipe()).expect("recipe decodes");
    let io = TaskIo {
        outputs: vec![("/out/".into(), "results".into(), "maps/".into())],
        ..TaskIo::default()
    };
    task_spec(install, work_id, (&recipe, &BTreeMap::new()), &io).expect("task spec builds")
}

/// The `nth` work id, in a fixed order, whose family the install's receiver does not hold, and
/// that family's holders. The search is bounded, so a fixture without one fails here.
fn foreign_work(realm: &Topology, install: &PluginInstall, nth: usize) -> (Ulid, Vec<NodeId>) {
    let found = (0..SEARCH_LIMIT)
        .map(|seed| Ulid::from_parts(seed, u128::from(seed)))
        .filter_map(|work_id| {
            let key = SubmissionScope::Keyed(format!("plugin-work/{work_id}"));
            let submission = submission_of(install.account, &key);
            let placement = realm.config.family_placement(submission).ok()?;
            let holders = realm.holders(&placement);
            (!holders.contains(&install.receiver)).then_some((work_id, holders))
        })
        .nth(nth);
    found.expect("the fixture has a family shard without the receiver")
}

/// Reserves work `work_id` on `node` within a task limit of `limit`, as a run's task call does.
async fn reserve_row(
    node: &TestNode,
    install: &PluginInstall,
    (work_id, limit): (Ulid, usize),
) -> TestResult<()> {
    let mut row = WorkRow::reserved(install, WorkKind::Recipe, (Ulid::generate(), work_id), 1);
    row.binding.recipe = Some("map".into());
    let identity = Identity::Call {
        run_id: row.run_id,
        ordinal: 0,
    };
    let reserve = reserve((install, &identity), row, None)?;
    drive(reserve.limited(limit), node.context.as_ref()).await?;
    Ok(())
}

/// Reserves and sends work `work_id` from `node`; answers its row afterwards.
async fn sent_from(node: &TestNode, install: &PluginInstall, work_id: Ulid) -> TestResult<WorkRow> {
    reserve_row(node, install, (work_id, 2)).await?;
    let spec = task(install, work_id);
    let send = SendTask::new(install, work_id, Some(spec), ingress(node));
    let sent = drive(send, node.context.as_ref()).await;
    sent.map_err(|error| error.0.message)?;
    work_row(node.context.as_ref(), install, work_id).await
}

async fn sent(realm: &Topology, install: &PluginInstall, work_id: Ulid) -> TestResult<WorkRow> {
    sent_from(realm.find(install.receiver), install, work_id).await
}

async fn work_row(
    ctx: &DriverContext,
    install: &PluginInstall,
    work_id: Ulid,
) -> TestResult<WorkRow> {
    let row = drive(JobRow::new(install, &work_id.to_string()), ctx).await;
    Ok(row.map_err(|error| error.0.message)?)
}

async fn task_row(
    ctx: &DriverContext,
    install: &PluginInstall,
    work_id: Ulid,
) -> TestResult<TaskRow> {
    let key = task_key(install.install_id, work_id);
    let read = Txn::direct(ctx).read(PLUGIN_WORK_KEYSPACE, &key).await?;
    Ok(decode(&read.ok_or("no task row")?)?)
}

async fn cancel_requested(node: &TestNode, alias: JobId) -> bool {
    let read = FamilyProjectionOperation::new(alias, unix_timestamp_millis());
    let projected = drive(read, node.context.as_ref()).await;
    let projection = (projected.ok().flatten()).and_then(|(projected, _)| projected.projection);
    projection.is_some_and(|projection| projection.cancel_requested)
}

/// One settle pass on the install's receiver.
async fn settle(realm: &Topology, install: &PluginInstall) -> TestResult<usize> {
    let receiver = realm.find(install.receiver);
    let pass = SettleTasks::new(ingress(receiver));
    Ok(drive(pass, receiver.context.as_ref()).await?)
}

#[tokio::test]
async fn admits_receiver_only() -> TestResult<()> {
    // A receiver that holds no family forwards with the install's identity and a holder admits
    // it without any group WRITE; the same request from another server node is refused.
    let (realm, install) = installed().await?;
    let (work_id, holders) = foreign_work(&realm, &install, 0);
    let row = sent(&realm, &install, work_id).await?;
    assert_eq!(row.state, WorkState::Dispatched, "{row:?}");
    let alias = row.job.ok_or("a dispatched task names its job")?;
    let holder = realm.find(holders[0]);
    wait_until("the holder's claim", holder.node_id(), || async {
        let read = FamilyProjectionOperation::new(alias, unix_timestamp_millis());
        matches!(drive(read, holder.context.as_ref()).await, Ok(Some(_)))
    })
    .await?;

    let (other_work, other_holders) = foreign_work(&realm, &install, 1);
    let bystander = (realm.nodes.iter())
        .find(|node| node.node_id() != install.receiver && !other_holders.contains(&node.node_id()))
        .ok_or("five nodes with three holders leave a bystander")?;
    let row = sent_from(bystander, &install, other_work).await?;
    assert!(
        row.job.is_none() && row.state != WorkState::Dispatched,
        "only the receiver may forward the install's tasks: {row:?}"
    );
    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn cancel_needs_receiver() -> TestResult<()> {
    // Another server node presenting the install account is refused and publishes nothing;
    // the receiver's cancel reaches a holder, and its settle pass pulls the cancelled family.
    let (realm, install) = installed().await?;
    let (work_id, holders) = foreign_work(&realm, &install, 0);
    let row = sent(&realm, &install, work_id).await?;
    let alias = row.job.ok_or("a dispatched task names its job")?;
    let receiver = realm.find(install.receiver);
    let task = task_row(receiver.context.as_ref(), &install, work_id).await?;
    let family = task.family.ok_or("no adopted family")?;

    let bystander = (realm.nodes.iter())
        .find(|node| node.node_id() != install.receiver && !holders.contains(&node.node_id()))
        .ok_or("five nodes with three holders leave a bystander")?;
    let config = Arc::new(realm.config.clone());
    let cancel = (install.account, alias, family);
    let refused = CancelTask::new(config.clone(), bystander.node_id(), cancel);
    let asked = drive(refused, bystander.context.as_ref()).await?;
    assert!(!asked, "every holder refused the bystander");
    for holder in &holders {
        assert!(!cancel_requested(realm.find(*holder), alias).await);
    }

    let accepted = CancelTask::new(config, install.receiver, cancel);
    assert!(drive(accepted, receiver.context.as_ref()).await?);
    let holder = realm.find(holders[0]);
    wait_until("the cancel record", holder.node_id(), || async {
        cancel_requested(holder, alias).await
    })
    .await?;
    let ctx = receiver.context.as_ref();
    wait_until("the settled task", install.receiver, || async {
        let _ = settle(&realm, &install).await;
        let row = work_row(ctx, &install, work_id).await;
        row.is_ok_and(|row| row.state == WorkState::Settled)
    })
    .await?;
    let row = work_row(ctx, &install, work_id).await?;
    let summary = row.summary.ok_or("a settled task keeps its summary")?;
    assert_eq!(summary.state, FinalState::Cancelled);
    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn refusal_frees_slot() -> TestResult<()> {
    // A holder refuses the first send for the group's quota: the row settles failed and frees
    // its task slot, and no cancel is ever sent.
    let (mut realm, install) = installed().await?;
    let mut config = realm.config.clone();
    config.compute.default_group_quota.max_jobs = Some(0);
    realm.apply_config(config).await?;
    let (work_id, _) = foreign_work(&realm, &install, 0);
    let row = sent(&realm, &install, work_id).await?;
    assert_eq!(row.state, WorkState::Settled, "{row:?}");
    let summary = row.summary.ok_or("a refused task keeps its summary")?;
    assert_eq!(summary.state, FinalState::Failed);
    assert!(row.job.is_none() && !row.cancel);
    // The slot is free again: a limit of one admits the next task.
    let receiver = realm.find(install.receiver);
    reserve_row(receiver, &install, (Ulid::generate(), 1)).await?;
    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn adopts_lost_answer() -> TestResult<()> {
    // The receiver lost the answer of an admitted send; its settle pass pulls the family's
    // records and adopts the claim instead of sending again.
    let (realm, install) = installed().await?;
    let (work_id, _) = foreign_work(&realm, &install, 0);
    let row = sent(&realm, &install, work_id).await?;
    let alias = row.job.ok_or("a dispatched task names its job")?;
    let receiver = realm.find(install.receiver);
    let ctx = receiver.context.as_ref();
    let txn = Txn::direct(ctx);
    let rows = txn.scan(PLUGIN_WORK_KEYSPACE, &[WORK_ROW]).await?;
    let (key, mut lost) = (rows.into_iter())
        .filter_map(|(key, value)| Some((key, decode::<WorkRow>(&value).ok()?)))
        .find(|(_, row)| row.work_id == work_id)
        .ok_or("no row")?;
    (lost.state, lost.job) = (WorkState::Reserved, None);
    let mut task = task_row(ctx, &install, work_id).await?;
    (task.family, task.uncertain) = (None, true);
    let rows = vec![
        (key, encode(&lost)?),
        (task_key(install.install_id, work_id), encode(&task)?),
    ];
    txn.write(PLUGIN_WORK_KEYSPACE, rows).await?;

    wait_until("the adopted claim", install.receiver, || async {
        let _ = settle(&realm, &install).await;
        let row = work_row(ctx, &install, work_id).await;
        row.is_ok_and(|row| row.job == Some(alias))
    })
    .await?;
    let row = work_row(ctx, &install, work_id).await?;
    assert_eq!(row.state, WorkState::Dispatched);
    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn paged_marked_input() -> TestResult<()> {
    // A directory input too large for the spec record is paged; a version in it that a job with
    // a secret wrote must still be refused before anything is sent.
    let (realm, install) = installed().await?;
    let receiver = realm.find(install.receiver);
    let ctx = receiver.context.as_ref();
    // Long keys make the input list large; each path segment stays a valid file name.
    let padding = vec!["r".repeat(120); 5].join("/");
    let mut marked = None;
    for index in 0..200 {
        let key = format!("big/{padding}-{index:03}.fastq");
        let place = (realm.realm_id, install.group_id, receiver.node_id());
        let put = put_config(place.0, place.1, place.2, "results", &key, b"ACGT");
        let version = drive(PutObjectOperation::new(put), ctx).await?.version_id;
        marked.get_or_insert((key, version));
    }
    let (key, version) = marked.ok_or("no object")?;
    let mark = Mark {
        install_id: install.install_id,
        work_id: Ulid::generate(),
        job: None,
        kind: WorkKind::Download,
        at_ms: 1,
    };
    let (_, mark_key, value) = mark_row(("results", &key, version), &mark)?;
    let rows = vec![(mark_key.to_vec(), value.to_vec())];
    Txn::direct(ctx).write(PLUGIN_WORK_KEYSPACE, rows).await?;

    let (work_id, _) = foreign_work(&realm, &install, 0);
    reserve_row(receiver, &install, (work_id, 2)).await?;
    let recipe: Recipe = serde_json::from_value(recipe())?;
    let io = TaskIo {
        objects: vec![("/in/".into(), "results".into(), "big/".into(), None)],
        outputs: vec![("/out/".into(), "results".into(), "maps/".into())],
        ..TaskIo::default()
    };
    let spec = task_spec(&install, work_id, (&recipe, &BTreeMap::new()), &io)?;
    let send = SendTask::new(&install, work_id, Some(spec), ingress(receiver));
    let refused = drive(send, ctx)
        .await
        .expect_err("a marked input never reaches a container");
    assert_eq!(refused.0.message, MARKED);
    realm.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn resends_stored_request() -> TestResult<()> {
    // No holder answered the first send, so it stays uncertain. Before the deadline the settle
    // pass sends the stored request again as it was, without preparing it anew.
    let (realm, install) = installed().await?;
    let (work_id, holders) = foreign_work(&realm, &install, 0);
    for holder in &holders {
        let _ = realm.find(*holder).net.shutdown().await;
    }
    let row = sent(&realm, &install, work_id).await?;
    assert_eq!(row.state, WorkState::Reserved, "{row:?}");
    assert_eq!(row.boundary, Some(SendBoundary::Sending { attempt: 1 }));
    settle(&realm, &install).await?;
    let ctx = realm.find(install.receiver).context.as_ref();
    let row = work_row(ctx, &install, work_id).await?;
    assert_eq!(row.boundary, Some(SendBoundary::Sending { attempt: 2 }));
    realm.shutdown().await;
    Ok(())
}
