//! Tests the timer and task scheduler: admission, ordering and drain.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

struct StampHandler {
    seen: Arc<Mutex<Option<Option<u64>>>>,
    notify: Arc<Notify>,
}

#[async_trait]
impl InboundTaskHandler for StampHandler {
    async fn handle_timer(&self, _key: TaskKey) {}

    async fn handle_firing(&self, _key: TaskKey, stamp: Option<u64>) {
        *self.seen.lock().expect("stamp lock") = Some(stamp);
        self.notify.notify_one();
    }
}

// A firing carries the stamp of the change that armed it, so the handler deletes that row.
#[tokio::test]
async fn firing_carries_stamp() {
    let handle = TaskHandle::new();
    let seen = Arc::new(Mutex::new(None));
    let notify = Arc::new(Notify::new());
    let handler = StampHandler {
        seen: seen.clone(),
        notify: notify.clone(),
    };
    handle.set_inbound_handler(Arc::new(handler)).await;
    let reset = TaskEffect::ResetTimer {
        key: test_key(),
        after: Duration::ZERO,
    };
    handle.send_stamped(reset, Some(42), None).await;
    tokio::time::timeout(Duration::from_secs(5), notify.notified())
        .await
        .expect("the timer fires");
    assert_eq!(*seen.lock().expect("stamp lock"), Some(Some(42)));
}

// A reset that waits eleven seconds for the scheduler keeps the deadline planned before it.
#[tokio::test(start_paused = true)]
async fn queued_reset_deadline() {
    let (command_tx, command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);
    let admission_closed = Arc::new(AtomicBool::new(false));
    let handle = TaskHandle {
        command_tx: command_tx.clone(),
        admission_closed: admission_closed.clone(),
        pulls: Arc::default(),
    };
    let after = Duration::from_secs(60);
    let deadline = Instant::now().checked_add(after);
    let reset = TaskEffect::ResetTimer {
        key: test_key(),
        after,
    };
    let sent = tokio::spawn(async move { handle.send_stamped(reset, Some(1), deadline).await });
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::spawn(run_scheduler(
        command_rx,
        command_tx.downgrade(),
        admission_closed,
    ));
    let event = sent.await.expect("the reset ends");
    let Event::Task(TaskEvent::TimerScheduled { after, .. }) = event else {
        panic!("the reset is armed");
    };
    assert_eq!(after, Duration::from_secs(49));
}

#[derive(Clone)]
struct ReschedulingHandler {
    handle: TaskHandle,
    count: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

#[async_trait]
impl InboundTaskHandler for ReschedulingHandler {
    async fn handle_timer(&self, key: TaskKey) {
        let seen = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        if seen == 1 {
            let _ = self
                .handle
                .send_effect(Effect::Task(TaskEffect::ResetTimer {
                    key,
                    after: Duration::from_millis(10),
                }))
                .await;
        }

        self.notify.notify_waiters();
    }
}

/// Panics on its first run and counts every run.
struct PanickingHandler {
    runs: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

#[async_trait]
impl InboundTaskHandler for PanickingHandler {
    async fn handle_timer(&self, _key: TaskKey) {
        let run = self.runs.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_one();
        assert!(run > 0, "the first run fails");
    }
}

#[derive(Clone)]
struct SelfReschedulingHandler {
    handle: TaskHandle,
    started: Arc<Notify>,
    finished: Arc<Notify>,
}

struct NotifyOnDrop(Arc<Notify>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[derive(Clone)]
struct BlockingHandler {
    started: Arc<Notify>,
    dropped: Arc<Notify>,
}

#[async_trait]
impl InboundTaskHandler for SelfReschedulingHandler {
    async fn handle_timer(&self, key: TaskKey) {
        self.started.notify_one();
        let _ = self
            .handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key,
                after: Duration::from_secs(3600),
            }))
            .await;

        tokio::task::yield_now().await;
        self.finished.notify_one();
    }
}

#[async_trait]
impl InboundTaskHandler for BlockingHandler {
    async fn handle_timer(&self, _key: TaskKey) {
        let _notify_on_drop = NotifyOnDrop(self.dropped.clone());
        self.started.notify_one();
        std::future::pending::<()>().await;
    }
}

#[derive(Clone)]
struct CountingGatedHandler {
    runs: Arc<AtomicUsize>,
    started: Arc<Notify>,
    gate: Arc<tokio::sync::Semaphore>,
}

#[async_trait]
impl InboundTaskHandler for CountingGatedHandler {
    async fn handle_timer(&self, _key: TaskKey) {
        self.runs.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        let permit = self
            .gate
            .acquire()
            .await
            .expect("handler gate should stay open");
        permit.forget();
    }
}

#[derive(Clone)]
struct RecordingHandler {
    runs: Arc<Mutex<Vec<(TaskKey, Instant)>>>,
    recorded: Arc<Notify>,
}

#[async_trait]
impl InboundTaskHandler for RecordingHandler {
    async fn handle_timer(&self, key: TaskKey) {
        self.runs
            .lock()
            .expect("run log lock should stay open")
            .push((key, Instant::now()));
        self.recorded.notify_one();
    }
}

async fn fire_timer(handle: &TaskHandle, key: TaskKey) {
    let _ = handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key,
            after: Duration::ZERO,
        }))
        .await;
}

/// Waits until the handler counted `expected` runs; each run notifies `started` once.
async fn wait_for_runs(runs: &Arc<AtomicUsize>, started: &Notify, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while runs.load(Ordering::SeqCst) < expected {
            started.notified().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("expected {expected} handler runs"));
}

async fn registered_drain_waiters(handle: &TaskHandle) -> usize {
    let (response, count) = oneshot::channel();
    handle
        .command_tx
        .send(TaskCommand::RegisteredDrainWaiters { response })
        .await
        .expect("drain waiter query should reach the scheduler");
    count.await.expect("drain waiter query should be answered")
}

/// Waits until the handler recorded `expected` runs; each run notifies `recorded` once.
async fn wait_for_records(handler: &RecordingHandler, expected: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while handler
            .runs
            .lock()
            .expect("run log lock should stay open")
            .len()
            < expected
        {
            handler.recorded.notified().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("expected {expected} handler runs"));
}

// Yields keep the paused clock still while a wrongly anchored timer can run.
async fn yield_runtime() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

fn test_key() -> TaskKey {
    TaskKey::RealmPresence {
        realm_id: aruna_core::structs::identity::realm::RealmId([7u8; 32]),
        node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
    }
}

fn other_key() -> TaskKey {
    TaskKey::RealmPresence {
        realm_id: aruna_core::structs::identity::realm::RealmId([8u8; 32]),
        node_id: iroh::SecretKey::from_bytes(&[9u8; 32]).public(),
    }
}

// Production construction must not return a scheduler-less handle as if it
// were running; outside a runtime the failure is concrete.
#[test]
fn requires_runtime() {
    assert_eq!(TaskHandle::try_new().unwrap_err(), TaskSchedulerUnavailable);
}

#[tokio::test]
async fn starts_in_runtime() {
    assert!(TaskHandle::try_new().is_ok());
}

// The ambiguous fallback is gone: without a runtime construction panics
// instead of handing back an inactive scheduler.
#[test]
#[should_panic(expected = "task scheduler requires an active Tokio runtime")]
fn new_without_runtime() {
    let _ = TaskHandle::new();
}

#[tokio::test]
async fn reset_keeps_reschedule() {
    let handle = TaskHandle::new();
    let count = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());
    let key = test_key();

    handle
        .set_inbound_handler(Arc::new(ReschedulingHandler {
            handle: handle.clone(),
            count: count.clone(),
            notify: notify.clone(),
        }))
        .await;

    let _ = handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key,
            after: Duration::from_millis(10),
        }))
        .await;

    tokio::time::timeout(Duration::from_secs(1), async {
        while count.load(Ordering::SeqCst) < 2 {
            notify.notified().await;
        }
    })
    .await
    .expect("rescheduled timer should fire twice");
}

#[tokio::test]
async fn panic_releases_key() {
    let handle = TaskHandle::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let notify = Arc::new(Notify::new());
    let handler = PanickingHandler {
        runs: runs.clone(),
        notify: notify.clone(),
    };
    handle.set_inbound_handler(Arc::new(handler)).await;
    for seen in 0..2 {
        let _ = handle
            .send_effect(Effect::Task(TaskEffect::ResetTimer {
                key: test_key(),
                after: Duration::ZERO,
            }))
            .await;
        tokio::time::timeout(Duration::from_secs(60), async {
            while runs.load(Ordering::SeqCst) == seen {
                notify.notified().await;
            }
        })
        .await
        .expect("the timer runs after an earlier run panicked");
    }
}

#[tokio::test]
async fn handler_reset_survives() {
    let handle = TaskHandle::new();
    let started = Arc::new(Notify::new());
    let finished = Arc::new(Notify::new());
    let key = test_key();

    handle
        .set_inbound_handler(Arc::new(SelfReschedulingHandler {
            handle: handle.clone(),
            started: started.clone(),
            finished: finished.clone(),
        }))
        .await;

    let _ = handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key,
            after: Duration::from_millis(10),
        }))
        .await;

    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("handler should start");
    tokio::time::timeout(Duration::from_secs(1), finished.notified())
        .await
        .expect("handler should continue after resetting its own timer");
}

#[tokio::test]
async fn abort_stops_handler() {
    let handle = TaskHandle::new();
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let key = test_key();

    handle
        .set_inbound_handler(Arc::new(BlockingHandler {
            started: started.clone(),
            dropped: dropped.clone(),
        }))
        .await;

    let _ = handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_millis(10),
        }))
        .await;

    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("handler should start");

    let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
        .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers { key }))
        .await
    else {
        panic!("expected running handler abort event");
    };
    assert_eq!(count, 1);

    tokio::time::timeout(Duration::from_secs(1), dropped.notified())
        .await
        .expect("handler future should be dropped after abort");
}

#[tokio::test(start_paused = true)]
async fn overlap_coalesces_refire() {
    let handle = TaskHandle::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let key = test_key();

    handle
        .set_inbound_handler(Arc::new(CountingGatedHandler {
            runs: runs.clone(),
            started: started.clone(),
            gate: gate.clone(),
        }))
        .await;

    fire_timer(&handle, key.clone()).await;
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("first handler run should start");

    for _ in 0..3 {
        fire_timer(&handle, key.clone()).await;
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    gate.add_permits(16);
    wait_for_runs(&runs, &started, 2).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn running_fire_refires() {
    let handle = TaskHandle::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let key = test_key();

    handle
        .set_inbound_handler(Arc::new(CountingGatedHandler {
            runs: runs.clone(),
            started: started.clone(),
            gate: gate.clone(),
        }))
        .await;

    fire_timer(&handle, key.clone()).await;
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("first handler run should start");

    fire_timer(&handle, key.clone()).await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    gate.add_permits(16);
    wait_for_runs(&runs, &started, 2).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn fires_run_once() {
    let handle = TaskHandle::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(100));
    let key = test_key();

    handle
        .set_inbound_handler(Arc::new(CountingGatedHandler {
            runs: runs.clone(),
            started: started.clone(),
            gate: gate.clone(),
        }))
        .await;

    fire_timer(&handle, key.clone()).await;
    wait_for_runs(&runs, &started, 1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    fire_timer(&handle, key).await;
    wait_for_runs(&runs, &started, 2).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn shorten_preserves_earlier() {
    let handle = TaskHandle::new();
    let key = test_key();

    let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(3600),
        }))
        .await
    else {
        panic!("expected timer scheduled event");
    };
    assert_eq!(after, Duration::from_secs(3600));

    let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
        .send_effect(Effect::Task(TaskEffect::ShortenTimer {
            key,
            after: Duration::from_secs(7200),
        }))
        .await
    else {
        panic!("expected timer scheduled event");
    };
    assert!(after <= Duration::from_secs(3600));
}

#[tokio::test]
async fn idle_keeps_timer() {
    let handle = TaskHandle::new();
    let key = test_key();

    let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(3600),
        }))
        .await
    else {
        panic!("expected timer scheduled event");
    };
    assert_eq!(after, Duration::from_secs(3600));

    let TaskEvent::TimerScheduled { after, .. } =
        handle.schedule_idle_timer(key, Duration::ZERO).await
    else {
        panic!("expected timer scheduled event");
    };
    assert!(after > Duration::from_secs(3000));
}

// An idle scheduler must anchor a relative timer when the command arrives,
// not when its loop iteration started before the command wait.
#[tokio::test(start_paused = true)]
async fn idle_reset_anchors() {
    let handle = TaskHandle::new();
    let runs = Arc::new(Mutex::new(Vec::new()));
    let key = test_key();
    let handler = RecordingHandler {
        runs: runs.clone(),
        recorded: Arc::new(Notify::new()),
    };
    handle.set_inbound_handler(Arc::new(handler.clone())).await;

    tokio::time::advance(Duration::from_secs(3600)).await;

    let submitted_at = Instant::now();
    let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(10),
        }))
        .await
    else {
        panic!("expected timer scheduled event");
    };
    assert_eq!(after, Duration::from_secs(10));

    yield_runtime().await;
    assert!(
        runs.lock()
            .expect("run log lock should stay open")
            .is_empty(),
        "an idle wait must not consume the relative timer"
    );

    tokio::time::advance(Duration::from_secs(9)).await;
    yield_runtime().await;
    assert!(
        runs.lock()
            .expect("run log lock should stay open")
            .is_empty(),
        "the timer must not fire before its full delay"
    );

    tokio::time::advance(Duration::from_secs(1)).await;
    wait_for_records(&handler, 1).await;
    let recorded = runs.lock().expect("run log lock should stay open").clone();
    assert_eq!(recorded[0].0, key);
    assert_eq!(recorded[0].1, submitted_at + Duration::from_secs(10));
}

// A pending deadline must not anchor a newly submitted relative timer.
#[tokio::test(start_paused = true)]
async fn pending_reset_anchors() {
    let handle = TaskHandle::new();
    let runs = Arc::new(Mutex::new(Vec::new()));
    let waiting = test_key();
    let added = other_key();
    let handler = RecordingHandler {
        runs: runs.clone(),
        recorded: Arc::new(Notify::new()),
    };
    handle.set_inbound_handler(Arc::new(handler.clone())).await;

    let _ = handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key: waiting.clone(),
            after: Duration::from_secs(10_000),
        }))
        .await;

    tokio::time::advance(Duration::from_secs(5000)).await;

    let submitted_at = Instant::now();
    let Event::Task(TaskEvent::TimerScheduled { after, .. }) = handle
        .send_effect(Effect::Task(TaskEffect::ResetTimer {
            key: added.clone(),
            after: Duration::from_secs(10),
        }))
        .await
    else {
        panic!("expected timer scheduled event");
    };
    assert_eq!(after, Duration::from_secs(10));

    yield_runtime().await;
    assert!(
        runs.lock()
            .expect("run log lock should stay open")
            .is_empty(),
        "a pending deadline must not shorten the new timer"
    );

    tokio::time::advance(Duration::from_secs(9)).await;
    yield_runtime().await;
    assert!(
        runs.lock()
            .expect("run log lock should stay open")
            .is_empty(),
        "the new timer must not fire before its full delay"
    );

    tokio::time::advance(Duration::from_secs(1)).await;
    wait_for_records(&handler, 1).await;
    {
        let recorded = runs.lock().expect("run log lock should stay open");
        assert_eq!(recorded.len(), 1, "the pending timer must stay pending");
        assert_eq!(recorded[0].0, added);
        assert_eq!(recorded[0].1, submitted_at + Duration::from_secs(10));
    }

    tokio::time::advance(Duration::from_secs(4990)).await;
    wait_for_records(&handler, 2).await;
    let recorded = runs.lock().expect("run log lock should stay open").clone();
    assert_eq!(recorded[1].0, waiting);
}

// A handler that finishes inside the drain budget is joined, not cut.
#[tokio::test]
async fn shutdown_drains_handler() {
    let handle = TaskHandle::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let key = test_key();

    handle
        .set_inbound_handler(Arc::new(CountingGatedHandler {
            runs: runs.clone(),
            started: started.clone(),
            gate: gate.clone(),
        }))
        .await;
    fire_timer(&handle, key).await;
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("handler should start");

    gate.add_permits(1);
    let report = handle.shutdown(Duration::from_secs(5)).await;

    assert_eq!(report.in_flight, 1);
    assert!(report.drained());
}

// A handler that ignores the deadline is aborted instead of hanging exit.
#[tokio::test]
async fn shutdown_aborts_stuck() {
    let handle = TaskHandle::new();
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let key = test_key();

    handle
        .set_inbound_handler(Arc::new(BlockingHandler {
            started: started.clone(),
            dropped: dropped.clone(),
        }))
        .await;
    fire_timer(&handle, key).await;
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("handler should start");

    let report = handle.shutdown(Duration::from_millis(50)).await;

    assert_eq!(report.aborted, 1);
    assert!(!report.drained());
    // The run stays owned: its end is observed by the scheduler, not by the
    // caller that asked for the forced stop.
    tokio::time::timeout(Duration::from_secs(1), dropped.notified())
        .await
        .expect("the force-stopped handler future must still drop");
    let settled = handle.shutdown(Duration::from_secs(1)).await;
    assert!(
        settled.drained(),
        "observed completion must release the run"
    );
}

#[test]
fn shutdown_reports_unavailable() {
    let handle = TaskHandle::inactive();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime should build");
    let report = runtime.block_on(handle.shutdown(Duration::ZERO));

    assert!(report.scheduler_unavailable);
    assert!(!report.drained());
}

// Dropping the drain future does not detach the run: the scheduler keeps it
// owned, so the forced stop is still observed and a resumed drain is clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_keeps_run() {
    let handle = TaskHandle::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let finished = Arc::new(Notify::new());
    let (release, released) = std::sync::mpsc::channel();
    let key = test_key();
    handle
        .set_inbound_handler(Arc::new(SlowSyncHandler {
            runs: runs.clone(),
            started: started.clone(),
            finished: finished.clone(),
            release: Arc::new(Mutex::new(released)),
        }))
        .await;
    fire_timer(&handle, key.clone()).await;
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("handler should start");

    // The caller is dropped while it waits for the drain deadline.
    let interrupted = tokio::time::timeout(
        Duration::from_millis(10),
        handle.shutdown(Duration::from_secs(30)),
    )
    .await;
    assert!(interrupted.is_err(), "the drain wait must be interrupted");

    // The forced stop is requested, but the caller is dropped again while
    // it observes the force-stopped run ending.
    let forced =
        tokio::time::timeout(Duration::from_millis(10), handle.shutdown(Duration::ZERO)).await;
    assert!(forced.is_err(), "the forced settle must be interruptible");

    // The run is still owned and force-stopped, not silently released.
    let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
        .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers {
            key: key.clone(),
        }))
        .await
    else {
        panic!("expected running handler abort event");
    };
    assert_eq!(count, 1, "the force-stopped run must stay owned");

    release.send(()).expect("the handler waits for its release");
    tokio::time::timeout(Duration::from_secs(1), finished.notified())
        .await
        .expect("handler should reach its completion");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let settled = handle.shutdown(Duration::from_secs(1)).await;
    assert!(
        settled.drained(),
        "observed completion must release the run"
    );
}

// Concurrent drains wait on the same completion: both must be released by
// the single handler finish instead of only the first waiter.
#[tokio::test]
async fn drains_share_completion() {
    let handle = TaskHandle::new();
    let started = Arc::new(Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let key = test_key();
    handle
        .set_inbound_handler(Arc::new(CountingGatedHandler {
            runs: Arc::new(AtomicUsize::new(0)),
            started: started.clone(),
            gate: gate.clone(),
        }))
        .await;
    fire_timer(&handle, key).await;
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("handler should start");

    let first = handle.shutdown(Duration::from_secs(5));
    let second = handle.shutdown(Duration::from_secs(5));
    let release = async {
        // Both drains must have registered before the handler may finish.
        tokio::time::timeout(Duration::from_secs(2), async {
            while registered_drain_waiters(&handle).await < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both drains should register");
        gate.add_permits(1);
    };
    let (first, second, ()) = tokio::join!(first, second, release);

    assert_eq!(first.in_flight, 1);
    assert_eq!(second.in_flight, 1);
    assert!(
        first.drained(),
        "the first drain must observe the completion"
    );
    assert!(
        second.drained(),
        "the second drain must observe the same completion"
    );
}

#[tokio::test]
async fn handlers_wait_capacity() {
    let mut state = SchedulerState::new(Arc::new(AtomicBool::new(false)));
    state.handler_limit = 2;
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    state.inbound_handler = Some(Arc::new(CountingGatedHandler {
        runs: Arc::new(AtomicUsize::new(0)),
        started: Arc::new(Notify::new()),
        gate: gate.clone(),
    }));
    let (command_tx, mut command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);
    let weak = command_tx.downgrade();
    let now = Instant::now();
    for key in [
        TaskKey::DrainSyncOutbox,
        TaskKey::DrainPruneQueue,
        TaskKey::DrainRefreshQueue,
    ] {
        state.reset_timer((key, None), Duration::ZERO, now);
    }
    state.dispatch_due_timers(now, &weak);
    assert_eq!(state.running_by_id.len(), 2);
    assert!(
        state
            .timers_by_key
            .contains_key(&TaskKey::DrainRefreshQueue)
    );
    assert!(
        state.next_deadline().is_none_or(|deadline| deadline > now),
        "a full scheduler does not wake for the waiting deadline"
    );
    gate.add_permits(1);
    let Some(TaskCommand::HandlerCompleted {
        run_id,
        key,
        elapsed,
    }) = command_rx.recv().await
    else {
        panic!("a handler completes");
    };
    state.complete_handler(run_id, key, elapsed, Instant::now(), &weak);
    state.dispatch_due_timers(Instant::now(), &weak);
    assert_eq!(state.running_by_id.len(), 2);
    assert!(state.timers_by_key.is_empty(), "the waiting deadline ran");
    gate.add_permits(2);
}

// A drain waiter that the scheduler drops without a completion is not a
// successful drain.
#[tokio::test]
async fn lost_drain_unavailable() {
    let (command_tx, mut command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);
    let handle = TaskHandle {
        command_tx,
        admission_closed: Arc::new(AtomicBool::new(false)),
        pulls: Arc::default(),
    };
    let scheduler = tokio::spawn(async move {
        match command_rx.recv().await {
            Some(TaskCommand::StopAdmission { response }) => {
                let _ = response.send(1);
            }
            _ => panic!("expected stop admission command"),
        }
        match command_rx.recv().await {
            Some(TaskCommand::AwaitDrained { response }) => drop(response),
            _ => panic!("expected await drained command"),
        }
    });

    let report = handle.shutdown(Duration::from_secs(1)).await;
    scheduler.await.expect("fake scheduler should finish");

    assert_eq!(report.in_flight, 1);
    assert!(report.scheduler_unavailable);
    assert!(!report.drained());
}

// A dropped stop-admission response must not become a confirmed zero
// in-flight count and a clean report.
#[tokio::test]
async fn lost_stop_unavailable() {
    let (command_tx, mut command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);
    let handle = TaskHandle {
        command_tx,
        admission_closed: Arc::new(AtomicBool::new(false)),
        pulls: Arc::default(),
    };
    let scheduler = tokio::spawn(async move {
        match command_rx.recv().await {
            Some(TaskCommand::StopAdmission { response }) => drop(response),
            _ => panic!("expected stop admission command"),
        }
    });

    let report = handle.shutdown(Duration::from_secs(1)).await;
    scheduler.await.expect("fake scheduler should finish");

    assert!(report.scheduler_unavailable);
    assert!(!report.drained());
}

// A lost forced-abort acknowledgement must not collapse to zero aborted
// work and a clean report.
#[tokio::test]
async fn lost_abort_unavailable() {
    let (command_tx, mut command_rx) = mpsc::channel(TASK_COMMAND_BUFFER);
    let handle = TaskHandle {
        command_tx,
        admission_closed: Arc::new(AtomicBool::new(false)),
        pulls: Arc::default(),
    };
    let scheduler = tokio::spawn(async move {
        match command_rx.recv().await {
            Some(TaskCommand::StopAdmission { response }) => {
                let _ = response.send(1);
            }
            _ => panic!("expected stop admission command"),
        }
        match command_rx.recv().await {
            Some(TaskCommand::AwaitDrained { response }) => {
                // Hold the waiter past the caller's drain deadline.
                let _held = response;
                match command_rx.recv().await {
                    Some(TaskCommand::AbortAllHandlers { response }) => drop(response),
                    _ => panic!("expected abort-all command"),
                }
            }
            _ => panic!("expected await drained command"),
        }
    });

    let report = handle.shutdown(Duration::from_millis(5)).await;
    scheduler.await.expect("fake scheduler should finish");

    assert_eq!(report.in_flight, 1);
    assert!(report.scheduler_unavailable);
    assert!(!report.drained());
}

// Admission stops first: timers that fire after shutdown find no handler.
#[tokio::test]
async fn shutdown_stops_admission() {
    let handle = TaskHandle::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let key = test_key();

    handle
        .set_inbound_handler(Arc::new(CountingGatedHandler {
            runs: runs.clone(),
            started: started.clone(),
            gate,
        }))
        .await;

    let report = handle.shutdown(Duration::from_secs(1)).await;
    assert_eq!(report.in_flight, 0);

    fire_timer(&handle, key.clone()).await;
    let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
        .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers { key }))
        .await
    else {
        panic!("expected running handler abort event");
    };
    assert_eq!(count, 0);
    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

// Admission closes on its own, before any drain wait: a timer that fires
// afterwards finds no handler even though nothing was waited on.
#[tokio::test]
async fn close_stops_handlers() {
    let handle = TaskHandle::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let key = test_key();

    handle
        .set_inbound_handler(Arc::new(CountingGatedHandler {
            runs: runs.clone(),
            started: started.clone(),
            gate,
        }))
        .await;
    handle.close_admission();

    fire_timer(&handle, key.clone()).await;
    let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
        .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers { key }))
        .await
    else {
        panic!("expected running handler abort event");
    };
    assert_eq!(count, 0);
    assert_eq!(runs.load(Ordering::SeqCst), 0);
}

/// Runs a synchronous section with no await point, so an abort request
/// cannot be observed until the section ends. The markers make both the
/// start and the end of the run explicit.
#[derive(Clone)]
struct SlowSyncHandler {
    runs: Arc<AtomicUsize>,
    started: Arc<Notify>,
    finished: Arc<Notify>,
    release: Arc<Mutex<std::sync::mpsc::Receiver<()>>>,
}

#[async_trait]
impl InboundTaskHandler for SlowSyncHandler {
    async fn handle_timer(&self, _key: TaskKey) {
        self.runs.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        let release = self.release.lock().expect("release lock should stay open");
        release.recv().expect("the test releases the section");
        drop(release);
        self.finished.notify_one();
    }
}

// A per-key abort keeps the run owned until its completion is observed: no
// second abort can see an empty entry while the handler is still running,
// and a third abort only reports zero after the completion arrived.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_key_retention() {
    let handle = TaskHandle::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let finished = Arc::new(Notify::new());
    let (release, released) = std::sync::mpsc::channel();
    let key = test_key();
    handle
        .set_inbound_handler(Arc::new(SlowSyncHandler {
            runs: runs.clone(),
            started: started.clone(),
            finished: finished.clone(),
            release: Arc::new(Mutex::new(released)),
        }))
        .await;
    fire_timer(&handle, key.clone()).await;
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("handler should start");

    let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
        .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers {
            key: key.clone(),
        }))
        .await
    else {
        panic!("expected running handler abort event");
    };
    assert_eq!(count, 1);

    // The handler is inside its synchronous section, so it cannot have
    // reported completion; the entry must still be owned and abortable.
    let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
        .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers {
            key: key.clone(),
        }))
        .await
    else {
        panic!("expected running handler abort event");
    };
    assert_eq!(count, 1, "the aborted run must stay owned until completion");

    release.send(()).expect("the handler waits for its release");
    tokio::time::timeout(Duration::from_secs(1), finished.notified())
        .await
        .expect("handler should reach its completion");
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let Event::Task(TaskEvent::RunningHandlersAborted { count, .. }) = handle
                .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers {
                    key: key.clone(),
                }))
                .await
            else {
                panic!("expected running handler abort event");
            };
            if count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("completion must clear the retained entry");
}

// After a per-key abort, the same key can be rescheduled; the new run only
// starts once the aborted run reported completion.
#[tokio::test]
async fn reschedule_after_abort() {
    let handle = TaskHandle::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let finished = Arc::new(Notify::new());
    let (release, released) = std::sync::mpsc::channel();
    let key = test_key();
    handle
        .set_inbound_handler(Arc::new(SlowSyncHandler {
            runs: runs.clone(),
            started: started.clone(),
            finished: finished.clone(),
            release: Arc::new(Mutex::new(released)),
        }))
        .await;
    // One thread runs the test and the handler, so both runs are released up front.
    release.send(()).expect("the handler waits for its release");
    release.send(()).expect("the handler waits for its release");
    fire_timer(&handle, key.clone()).await;
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("handler should start");

    let _ = handle
        .send_effect(Effect::Task(TaskEffect::AbortRunningHandlers {
            key: key.clone(),
        }))
        .await;
    fire_timer(&handle, key.clone()).await;

    wait_for_runs(&runs, &started, 2).await;
}

// Repeating shutdown after handlers were already drained is a clean no-op
// instead of a second teardown.
#[tokio::test]
async fn repeat_shutdown_clean() {
    let handle = TaskHandle::new();
    let first = handle.shutdown(Duration::from_millis(10)).await;
    let second = handle.shutdown(Duration::from_millis(10)).await;

    assert!(!first.scheduler_unavailable);
    assert_eq!(first.in_flight, 0);
    assert!(first.drained());
    assert!(!second.scheduler_unavailable);
    assert_eq!(second.in_flight, 0);
    assert_eq!(second.aborted, 0);
    assert!(second.drained());
}

// A retained clone owns the command channel just like the original, so
// dropping one handle must not stop the scheduler.
#[tokio::test]
async fn clone_keeps_scheduler() {
    let handle = TaskHandle::new();
    let clone = handle.clone();
    let key = test_key();
    drop(handle);

    let Event::Task(TaskEvent::TimerCancelled { .. }) = clone
        .send_effect(Effect::Task(TaskEffect::CancelTimer { key }))
        .await
    else {
        panic!("the retained clone must reach the scheduler");
    };
}

#[tokio::test(start_paused = true)]
async fn idle_ignores_running() {
    let handle = TaskHandle::new();
    let runs = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let key = test_key();

    handle
        .set_inbound_handler(Arc::new(CountingGatedHandler {
            runs: runs.clone(),
            started: started.clone(),
            gate: gate.clone(),
        }))
        .await;

    fire_timer(&handle, key.clone()).await;
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("first handler run should start");

    let TaskEvent::TimerScheduled { .. } = handle.schedule_idle_timer(key, Duration::ZERO).await
    else {
        panic!("expected timer scheduled event");
    };
    gate.add_permits(1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
}
