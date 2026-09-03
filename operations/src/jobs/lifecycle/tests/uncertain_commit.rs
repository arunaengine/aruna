//! Reservation outcomes that prove nothing about this node's willingness to
//! run work: a commit that neither succeeded nor was refused is reconciled from
//! the store, a refused write is retried, and neither is ever a drain.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use aruna_core::NodeId;
use aruna_core::compute::ExecutorCapability;
use aruna_core::compute_quota::{ComputeDemandSnapshot, ComputeReservationSnapshot};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::StorageError;
use aruna_core::events::{Event, LaunchDecline, StorageEvent};
use aruna_core::handle::Handle;
use aruna_core::keyspaces::{
    JOB_FAMILY_RECORD_KEYSPACE, JOB_RESERVATION_KEYSPACE, NODE_INFO_KEYSPACE,
};
use aruna_core::structs::{
    AdvertisementEpoch, JobFamilyRecord, JobRecordKind, LaunchIntent, NodeInfoDocument, NodeUrls,
    NodeUtilization, PlacementSubject, node_info_storage_key,
};
use aruna_core::task::TaskKey;
use aruna_core::types::{Key, Value};
use aruna_tasks::{InboundTaskHandler, TaskHandle};
use async_trait::async_trait;
use tempfile::TempDir;
use tokio::sync::mpsc;
use ulid::Ulid;

use super::admission_race::{config, envelope, minted, receipts, rows, seed};
use crate::driver::{DriverContext, drive};
use crate::jobs::lifecycle::LifecycleError;
use crate::jobs::lifecycle::reservation::{ReserveExecutionOperation, reservation_key};
use crate::jobs::lifecycle::target::{
    CommitVerdict, RESERVE_ATTEMPTS, classify, commit_receipt, commit_with, local_capability,
};
use crate::jobs::records::keys::record_key;
use crate::jobs::records::load_kind_complete;
use crate::jobs::records::rows::to_bytes;
use crate::jobs::records::tests::fixture::{Family, REALM, context};
use crate::node_info::set_operator_drain;

/// Detects a wakeup that never arrives, not a slow machine.
const WAKEUP_LIMIT: Duration = Duration::from_secs(30);

/// The outcome the injected reservation reports instead of committing.
fn failed(error: StorageError) -> Result<Ulid, LifecycleError> {
    Err(LifecycleError::Storage(error))
}

/// Records every timer key the scheduler fires, so the wakeups an acceptance
/// owes are waited for instead of slept on.
struct Fired(mpsc::UnboundedSender<TaskKey>);

#[async_trait]
impl InboundTaskHandler for Fired {
    async fn handle_timer(&self, key: TaskKey) {
        let _ = self.0.send(key);
    }
}

/// A storage-backed context whose task scheduler reports the timers it fires.
async fn wired(
    family: &Family,
) -> (
    TempDir,
    Arc<DriverContext>,
    mpsc::UnboundedReceiver<TaskKey>,
) {
    let (dir, mut ctx) = context(&family.config, family.holder.public()).await;
    let (sender, fired) = mpsc::unbounded_channel();
    let task = TaskHandle::new();
    task.set_inbound_handler(Arc::new(Fired(sender))).await;
    ctx.task_handle = Some(task);
    (dir, Arc::new(ctx), fired)
}

async fn wakeups(fired: &mut mpsc::UnboundedReceiver<TaskKey>) -> HashSet<TaskKey> {
    let mut keys = HashSet::new();
    for _ in 0..2 {
        let key = tokio::time::timeout(WAKEUP_LIMIT, fired.recv())
            .await
            .expect("a wakeup timer fires")
            .expect("the scheduler stays alive");
        keys.insert(key);
    }
    keys
}

/// Replication of the receipt and the start of the execution.
fn both_wakeups() -> HashSet<TaskKey> {
    HashSet::from([TaskKey::DrainJobFamilyOutbox, TaskKey::DrainJobQueue])
}

/// A receipt for this launch id storing different content. The row is written
/// directly on purpose: admission would never sign this receipt.
async fn seed_receipt(ctx: &DriverContext, family: &Family, launch: &LaunchIntent) {
    let mut receipt = family.receipt(launch, 9);
    receipt.launch_digest = [9u8; 32];
    let envelope = family.sign(&family.target, JobFamilyRecord::Receipt(Box::new(receipt)));
    let event = ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: JOB_FAMILY_RECORD_KEYSPACE.to_string(),
            key: record_key(&envelope.key()),
            value: Value::from(to_bytes(&envelope).expect("record encodes").as_slice()),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

/// This node's advertisement with one docker executor.
fn advertised(local: NodeId, draining: bool) -> NodeInfoDocument {
    let subject = PlacementSubject {
        node_id: local,
        generation: 1,
        location: "eu-west".to_string(),
        labels: BTreeMap::new(),
        executor_kind: None,
        local_to_controller: true,
    };
    NodeInfoDocument {
        node_id: local,
        executors: vec![
            ExecutorCapability::new("docker".to_string(), subject).expect("capability is valid"),
        ],
        labels: BTreeMap::new(),
        urls: NodeUrls {
            api: None,
            s3: None,
        },
        utilization: NodeUtilization {
            storage_bytes_used: 0,
            documents_held: None,
            load_permille: None,
            heartbeat_at_ms: 1,
        },
        updated_at_ms: 1,
        epoch: AdvertisementEpoch::default(),
        compute_draining: draining,
        leaving: false,
        demand: ComputeDemandSnapshot::default(),
        reservation: ComputeReservationSnapshot::default(),
    }
}

async fn advertise(ctx: &DriverContext, document: &NodeInfoDocument) {
    let event = ctx
        .storage_handle
        .send_effect(Effect::Storage(StorageEffect::Write {
            key_space: NODE_INFO_KEYSPACE.to_string(),
            key: Key::from(node_info_storage_key(document.node_id)),
            value: Value::from(document.to_bytes().expect("document validates").as_slice()),
            txn_id: None,
        }))
        .await;
    assert!(matches!(
        event,
        Event::Storage(StorageEvent::WriteResult { .. })
    ));
}

#[tokio::test]
async fn recovers_durable_commit() {
    // An unknown commit outcome may already be durable: the committed receipt
    // answers the offer, exactly once, and still wakes the runtime.
    let family = Family::new([4u8; 32]);
    let (_dir, ctx, mut fired) = wired(&family).await;
    let (launch, _) = seed(&ctx, &family).await;
    let config = config(&family, &launch, 1, envelope(4));
    let calls = AtomicUsize::new(0);
    let counter = &calls;
    let driver: &DriverContext = ctx.as_ref();

    let decided = commit_with(&ctx, config.clone(), &launch, move |config| async move {
        counter.fetch_add(1, Ordering::Relaxed);
        drive(ReserveExecutionOperation::new(config), driver).await?;
        failed(StorageError::CommitFailed)
    })
    .await;

    let frame = decided.expect("offer is decided").expect("admitted");
    let stored = load_kind_complete(driver, family.family(), JobRecordKind::Receipt)
        .await
        .expect("receipt read completes");
    assert_eq!(stored.len(), 1);
    assert_eq!(frame.envelope().digest(), stored[0].digest());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(rows(&ctx, JOB_RESERVATION_KEYSPACE, None).await, 1);
    assert_eq!(
        rows(
            &ctx,
            JOB_RESERVATION_KEYSPACE,
            Some(reservation_key(config.execution_id))
        )
        .await,
        1
    );
    assert_eq!(minted(&ctx).await, 1);
    assert_eq!(wakeups(&mut fired).await, both_wakeups());
}

#[tokio::test]
async fn undecided_without_receipt() {
    // An unknown commit that left no receipt decides nothing, and the same
    // offer must still be admissible when the scheduler asks again.
    let family = Family::new([5u8; 32]);
    let (_dir, ctx, mut fired) = wired(&family).await;
    let (launch, _) = seed(&ctx, &family).await;
    let config = config(&family, &launch, 1, envelope(4));
    let calls = AtomicUsize::new(0);
    let counter = &calls;

    let decided = commit_with(&ctx, config.clone(), &launch, move |_| async move {
        counter.fetch_add(1, Ordering::Relaxed);
        failed(StorageError::CommitFailed)
    })
    .await;

    assert!(decided.is_none());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    assert_eq!(rows(&ctx, JOB_RESERVATION_KEYSPACE, None).await, 0);
    assert_eq!(receipts(&ctx, &family).await, 0);
    assert_eq!(minted(&ctx).await, 0);

    let retried = commit_receipt(&ctx, config, &launch).await;

    assert!(retried.expect("offer is decided").is_ok());
    assert_eq!(rows(&ctx, JOB_RESERVATION_KEYSPACE, None).await, 1);
    assert_eq!(receipts(&ctx, &family).await, 1);
    assert_eq!(minted(&ctx).await, 1);
    assert_eq!(wakeups(&mut fired).await, both_wakeups());
}

#[tokio::test]
async fn conflicting_digest_conflicts() {
    // One launch id under different signed content is a conflict, and an
    // unknown commit outcome must report it as one instead of a drain.
    let family = Family::new([6u8; 32]);
    let (_dir, ctx, _fired) = wired(&family).await;
    let (launch, _) = seed(&ctx, &family).await;
    seed_receipt(&ctx, &family, &launch).await;

    let decided = commit_with(
        &ctx,
        config(&family, &launch, 1, envelope(4)),
        &launch,
        move |_| async move { failed(StorageError::CommitFailed) },
    )
    .await;

    assert!(matches!(decided, Some(Err(LaunchDecline::LaunchConflict))));
    assert_eq!(rows(&ctx, JOB_RESERVATION_KEYSPACE, None).await, 0);
    assert_eq!(receipts(&ctx, &family).await, 1);
    assert_eq!(minted(&ctx).await, 0);
}

#[tokio::test]
async fn bounds_queue_full() {
    // A write storage never accepted proves no commit: it is retried, bounded,
    // and left undecided rather than reported as a drain.
    let family = Family::new([7u8; 32]);
    let (_dir, ctx, _fired) = wired(&family).await;
    let (launch, _) = seed(&ctx, &family).await;
    let calls = AtomicUsize::new(0);
    let counter = &calls;

    let decided = commit_with(
        &ctx,
        config(&family, &launch, 1, envelope(4)),
        &launch,
        move |_| async move {
            counter.fetch_add(1, Ordering::Relaxed);
            failed(StorageError::QueueFull)
        },
    )
    .await;

    assert!(decided.is_none());
    assert_eq!(calls.load(Ordering::Relaxed) as u32, RESERVE_ATTEMPTS);
    assert_eq!(rows(&ctx, JOB_RESERVATION_KEYSPACE, None).await, 0);
    assert_eq!(receipts(&ctx, &family).await, 0);
}

#[tokio::test]
async fn drain_still_drains() {
    // Draining stays the answer for a real drain: the published flag and the
    // operator's durable flag both refuse every new execution here.
    let family = Family::new([8u8; 32]);
    let local = family.target.public();
    let spec = family.spec();
    let launch = family.launch(&spec, family.holder.public(), 0);

    let (_published_dir, published) = context(&family.config, family.holder.public()).await;
    advertise(&published, &advertised(local, true)).await;
    assert!(matches!(
        local_capability(&published, &family.config, local, &launch, &spec).await,
        Err(LaunchDecline::Draining)
    ));

    let (_operator_dir, operator) = context(&family.config, family.holder.public()).await;
    set_operator_drain(&operator, local, REALM, true)
        .await
        .expect("operator drain records");
    advertise(&operator, &advertised(local, false)).await;
    assert!(matches!(
        local_capability(&operator, &family.config, local, &launch, &spec).await,
        Err(LaunchDecline::Draining)
    ));

    let (_open_dir, open) = context(&family.config, family.holder.public()).await;
    advertise(&open, &advertised(local, false)).await;
    assert!(!matches!(
        local_capability(&open, &family.config, local, &launch, &spec).await,
        Err(LaunchDecline::Draining)
    ));
}

#[tokio::test]
async fn capacity_still_capacity() {
    // A backend with room for nothing is the one refusal capacity may report.
    let family = Family::new([10u8; 32]);
    let (_dir, ctx, _fired) = wired(&family).await;
    let (launch, _) = seed(&ctx, &family).await;

    let decided = commit_receipt(&ctx, config(&family, &launch, 1, envelope(0)), &launch).await;

    assert!(matches!(decided, Some(Err(LaunchDecline::Capacity))));
    assert_eq!(rows(&ctx, JOB_RESERVATION_KEYSPACE, None).await, 0);
    assert_eq!(receipts(&ctx, &family).await, 0);
}

#[test]
fn uncertain_never_drains() {
    // Every storage outcome is classified by what it proves about the commit,
    // and none of them is this node declaring itself drained.
    let outcomes = [
        (StorageError::KeyNotFound, CommitVerdict::Uncertain),
        (StorageError::TransactionConflict, CommitVerdict::Raced),
        (StorageError::CommitFailed, CommitVerdict::Uncertain),
        (StorageError::TransactionNotFound, CommitVerdict::Uncertain),
        (StorageError::CleanupCapacity, CommitVerdict::Retry),
        (
            StorageError::KeyspaceError("x".to_string()),
            CommitVerdict::Uncertain,
        ),
        (
            StorageError::ReadError("x".to_string()),
            CommitVerdict::Uncertain,
        ),
        (
            StorageError::WriteError("x".to_string()),
            CommitVerdict::Uncertain,
        ),
        (StorageError::DeleteError, CommitVerdict::Uncertain),
        (
            StorageError::PersistError("x".to_string()),
            CommitVerdict::Uncertain,
        ),
        (StorageError::ChannelClosed, CommitVerdict::Uncertain),
        (StorageError::QueueFull, CommitVerdict::Retry),
        (StorageError::Timeout, CommitVerdict::Uncertain),
        (StorageError::InvalidEffect, CommitVerdict::Uncertain),
        (StorageError::Closed, CommitVerdict::Uncertain),
    ];

    for (error, expected) in outcomes {
        let verdict = classify(&LifecycleError::Storage(error.clone()));
        assert_eq!(verdict, expected, "{error}");
        assert_ne!(verdict, CommitVerdict::Drained, "{error}");
    }
    assert_eq!(classify(&LifecycleError::Capacity), CommitVerdict::Capacity);
    assert_eq!(classify(&LifecycleError::NotHolder), CommitVerdict::Drained);
    assert_eq!(
        classify(&LifecycleError::RealmConfigMissing),
        CommitVerdict::Drained
    );
    assert_eq!(
        classify(&LifecycleError::NotFinished),
        CommitVerdict::Faulted
    );
}
