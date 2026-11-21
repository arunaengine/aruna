//! Restores the durable task timers into the task handle when the node starts.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::time::Duration;

use aruna_clock::unix_timestamp_millis;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::TASK_TIMER_KEYSPACE;
use aruna_core::task::{PersistedTaskTimer, TaskEvent};
use aruna_storage::storage::handle::StorageHandle;
use aruna_storage::task_timers::{delete_malformed, hold_timer, read_timer};
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use tracing::warn;

const RESTORE_PAGE_SIZE: usize = 256;

/// Where a timer restoration stopped: the row after which a retry starts, and why.
#[derive(Debug)]
pub struct RestoreStopped {
    pub resume_after: Option<ByteView>,
    pub error: String,
}

/// Re-arms the persisted timers after the row `start_after`, page by page. A failed page or a
/// timer the scheduler refused stops it there, so a retry resumes without skipping a row. A timer
/// only moves earlier, so a retry never pushes back a newer live deadline.
pub async fn restore_task_timers(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
    start_after: Option<ByteView>,
) -> Result<(), RestoreStopped> {
    let mut accepted = start_after;
    loop {
        let event = storage
            .send_storage_effect(StorageEffect::Iter {
                key_space: TASK_TIMER_KEYSPACE.to_string(),
                prefix: None,
                start: accepted.clone().map(IterStart::After),
                limit: RESTORE_PAGE_SIZE,
                txn_id: None,
            })
            .await;
        let stopped = |resume_after, error| {
            Err(RestoreStopped {
                resume_after,
                error,
            })
        };
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return stopped(accepted, error.to_string());
            }
            other => return stopped(accepted, format!("unexpected storage event: {other:?}")),
        };

        for (key_bytes, value) in values {
            match postcard::from_bytes::<PersistedTaskTimer>(&value) {
                Ok(record) => {
                    if let Err(message) = restore_timer(storage, task_handle, record).await {
                        return stopped(accepted, message);
                    }
                }
                Err(error) => {
                    warn!(error = %error, "Failed to decode persisted task timer");
                    if let Err(error) = delete_malformed(storage, key_bytes.clone()).await {
                        warn!(%error, "Failed to delete an undecodable persisted task timer");
                    }
                }
            }
            accepted = Some(key_bytes);
        }

        match next_start_after {
            Some(next) => accepted = Some(next),
            None => return Ok(()),
        }
    }
}

/// Arms the timer of a row the page read, as its row stands under the key's change lock: a row a
/// cancellation removed since stays unarmed, and a reset row arms at its new deadline.
async fn restore_timer(
    storage: &StorageHandle,
    task_handle: &TaskHandle,
    record: PersistedTaskTimer,
) -> Result<(), String> {
    let _held = hold_timer(&record.key).await;
    let Some(current) = read_timer(storage, &record.key)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(());
    };
    let now_ms = unix_timestamp_millis();
    let after = Duration::from_millis(current.due_unix_millis.saturating_sub(now_ms));
    // The row already exists, so its own admission arms the wake and its firing deletes it.
    let admission = current.admission;
    match task_handle
        .shorten_owned(current.key, after, admission)
        .await
    {
        (Event::Task(TaskEvent::Error { message, .. }), _) => Err(message),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::effects::Effect;
    use aruna_core::handle::Handle;
    use aruna_core::task::{TaskEffect, TaskKey};
    use aruna_drivers::effect_adapters::task::change_timer;
    use aruna_storage::storage::worker::FjallStorage;
    use aruna_storage::task_timers::{persist_task_effect, read_timer};
    use aruna_tasks::InboundTaskHandler;
    use async_trait::async_trait;
    use std::sync::Arc;
    use tokio::sync::{Mutex, Notify, oneshot};

    struct RecordingHandler {
        observed: Arc<Mutex<Option<TaskKey>>>,
        notify: Arc<Notify>,
    }

    #[async_trait]
    impl InboundTaskHandler for RecordingHandler {
        async fn handle_timer(&self, key: TaskKey) {
            *self.observed.lock().await = Some(key);
            self.notify.notify_one();
        }
    }

    #[tokio::test]
    async fn restores_persisted_handle() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::RefreshBlobHolders;

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: key.clone(),
                after: Duration::from_millis(1),
            },
        )
        .await
        .expect("timer persists");

        let task_handle = TaskHandle::new();
        let observed = Arc::new(Mutex::new(None));
        let notify = Arc::new(Notify::new());
        task_handle
            .set_inbound_handler(Arc::new(RecordingHandler {
                observed: observed.clone(),
                notify: notify.clone(),
            }))
            .await;

        restore_task_timers(&storage, &task_handle, None)
            .await
            .expect("timers restore");
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("restored timer should fire");

        assert_eq!(*observed.lock().await, Some(key));
    }

    /// The deadline the scheduler holds for `key`, read by asking it to shorten to `probe`.
    async fn armed_after(task_handle: &TaskHandle, key: &TaskKey, probe: Duration) -> Duration {
        let shorten = TaskEffect::ShortenTimer {
            key: key.clone(),
            after: probe,
        };
        let Event::Task(TaskEvent::TimerScheduled { after, .. }) =
            task_handle.send_effect(Effect::Task(shorten)).await
        else {
            panic!("expected timer scheduled");
        };
        after
    }

    // A row the page read but a later change cancelled or postponed is restored as it stands now,
    // so restoration never revives cancelled work or pulls a postponed timer forward.
    #[tokio::test]
    async fn stale_row_reread() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let task_handle = TaskHandle::new();
        let probe = Duration::from_secs(7_200);
        let stale = |key: &TaskKey| PersistedTaskTimer {
            key: key.clone(),
            due_unix_millis: unix_timestamp_millis(),
            admission: 9,
        };
        let cancelled = TaskKey::RefreshBlobHolders;
        let cancel = TaskEffect::CancelTimer {
            key: cancelled.clone(),
        };
        persist_task_effect(&storage, &cancel)
            .await
            .expect("row deleted");
        restore_timer(&storage, &task_handle, stale(&cancelled))
            .await
            .expect("restores");
        assert_eq!(armed_after(&task_handle, &cancelled, probe).await, probe);

        let postponed = TaskKey::PublishRegistration;
        let reset = TaskEffect::ResetTimer {
            key: postponed.clone(),
            after: Duration::from_secs(3_600),
        };
        persist_task_effect(&storage, &reset)
            .await
            .expect("row written");
        restore_timer(&storage, &task_handle, stale(&postponed))
            .await
            .expect("restores");
        let after = armed_after(&task_handle, &postponed, probe).await;
        assert!(
            after > Duration::from_secs(60),
            "armed at the new deadline: {after:?}"
        );
    }

    // A firing deletes only the row its stamp names: a replacement written while it waited to start
    // stays, even when that replacement is already due itself.
    #[tokio::test]
    async fn firing_keeps_replacement() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::RefreshBlobHolders;
        let fired = PersistedTaskTimer {
            key: key.clone(),
            due_unix_millis: unix_timestamp_millis().saturating_sub(10_000),
            admission: 5,
        };
        let row = StorageEffect::Write {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: key.storage_key().expect("key encodes"),
            value: ByteView::from(postcard::to_allocvec(&fired).expect("row encodes")),
            txn_id: None,
        };
        storage.send_storage_effect(row).await;
        let replacement = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::ZERO,
        };
        persist_task_effect(&storage, &replacement)
            .await
            .expect("row written");
        let stamp = Some(fired.admission);
        aruna_storage::task_timers::delete_persisted_timer(&storage, &key, stamp).await;
        assert!(read_timer(&storage, &key).await.expect("reads").is_some());
    }

    /// Admits a firing and holds it: its stamp is kept, and the run never ends.
    struct HeldFiring {
        stamp: Arc<Mutex<Option<u64>>>,
        notify: Arc<Notify>,
    }

    #[async_trait]
    impl InboundTaskHandler for HeldFiring {
        async fn handle_timer(&self, _key: TaskKey) {}

        async fn handle_firing(&self, _key: TaskKey, stamp: Option<u64>) {
            *self.stamp.lock().await = stamp;
            self.notify.notify_one();
            std::future::pending::<()>().await;
        }
    }

    // A shorten that arrives after the scheduler admitted a firing, but before that firing
    // deleted its row, arms a new admission whose row the old firing cannot delete.
    #[tokio::test]
    async fn shorten_after_dispatch() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let task_handle = TaskHandle::new();
        let stamp = Arc::new(Mutex::new(None));
        let notify = Arc::new(Notify::new());
        let firing = HeldFiring {
            stamp: stamp.clone(),
            notify: notify.clone(),
        };
        task_handle.set_inbound_handler(Arc::new(firing)).await;
        let key = TaskKey::PublishRegistration;
        let reset = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::ZERO,
        };
        change_timer(&storage, Some(&task_handle), reset, false).await;
        tokio::time::timeout(Duration::from_secs(5), notify.notified())
            .await
            .expect("the first wake is admitted");
        let shorten = TaskEffect::ShortenTimer {
            key: key.clone(),
            after: Duration::from_secs(10),
        };
        change_timer(&storage, Some(&task_handle), shorten, false).await;

        let fired = *stamp.lock().await;
        aruna_storage::task_timers::delete_persisted_timer(&storage, &key, fired).await;
        let row = read_timer(&storage, &key).await.expect("reads");
        let row = row.expect("the shortened wake keeps its row");
        assert_ne!(Some(row.admission), fired);
    }

    // A shorten whose row was refused keeps its live wake; a later shorten that keeps that wake
    // writes the missing row at the kept deadline, so a restart still wakes early.
    #[tokio::test]
    async fn kept_wake_stored() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let task_handle = TaskHandle::new();
        let key = TaskKey::PublishRegistration;
        // The first shorten armed admission 7, but storage refused its row.
        let early = Duration::from_secs(5);
        let (_, holder) = task_handle.shorten_owned(key.clone(), early, 7).await;
        assert_eq!(holder, Some(7));
        let later = TaskEffect::ShortenTimer {
            key: key.clone(),
            after: Duration::from_secs(60),
        };
        change_timer(&storage, Some(&task_handle), later, false).await;
        let row = read_timer(&storage, &key).await.expect("reads");
        let row = row.expect("the kept wake owns a row");
        assert_eq!(row.admission, 7);
        assert!(row.due_unix_millis <= aruna_clock::unix_timestamp_millis() + 5_000);
    }

    // A kept wake never postpones a stored row due earlier, such as one startup could not read.
    #[tokio::test]
    async fn earlier_row_kept() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let task_handle = TaskHandle::new();
        let key = TaskKey::PublishRegistration;
        let stored = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(5),
        };
        persist_task_effect(&storage, &stored)
            .await
            .expect("timer persists");
        let before = read_timer(&storage, &key).await.expect("reads");
        // A wake from memory, due later than the row, holds the key.
        let kept = Duration::from_secs(60);
        let (_, holder) = task_handle.shorten_owned(key.clone(), kept, 7).await;
        assert_eq!(holder, Some(7));
        let later = TaskEffect::ShortenTimer {
            key: key.clone(),
            after: Duration::from_secs(120),
        };
        change_timer(&storage, Some(&task_handle), later, false).await;
        assert_eq!(read_timer(&storage, &key).await.expect("reads"), before);
    }

    /// A durable shorten of `key` to five seconds on a storage that refuses timer rows.
    async fn refused_shorten(storage: &StorageHandle, task_handle: &TaskHandle, key: &TaskKey) {
        let refusing = aruna_test_support::metadata::refusing(storage.clone(), TASK_TIMER_KEYSPACE);
        let shorten = TaskEffect::ShortenTimer {
            key: key.clone(),
            after: Duration::from_secs(5),
        };
        let event = change_timer(&refusing, Some(task_handle), shorten, false).await;
        assert!(matches!(event, Event::Task(TaskEvent::Error { .. })));
    }

    // A shorten whose row is refused arms no wake, so nothing fires for the refused change.
    #[tokio::test]
    async fn refused_shorten_unarmed() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let task_handle = TaskHandle::new();
        let key = TaskKey::PublishRegistration;
        refused_shorten(&storage, &task_handle, &key).await;
        let probe = Duration::from_secs(60);
        let (_, plan) = task_handle.plan_shorten(key.clone(), probe, 1).await;
        let holder = plan.and_then(|plan| plan.holder);
        assert_eq!(holder, Some(1), "no wake holds the key");
        assert_eq!(read_timer(&storage, &key).await.expect("reads"), None);
    }

    // A refused shorten keeps the later wake armed before and its row, so its firing cleans up.
    #[tokio::test]
    async fn refused_shorten_kept() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let task_handle = TaskHandle::new();
        let key = TaskKey::PublishRegistration;
        let armed = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(60),
        };
        change_timer(&storage, Some(&task_handle), armed, false).await;
        let before = read_timer(&storage, &key).await.expect("reads");
        let before = before.expect("the armed wake owns a row");
        refused_shorten(&storage, &task_handle, &key).await;
        let probe = Duration::from_secs(120);
        let (event, plan) = task_handle.plan_shorten(key.clone(), probe, 1).await;
        assert_eq!(plan.and_then(|plan| plan.holder), Some(before.admission));
        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = event else {
            panic!("the armed wake is answered");
        };
        assert!(
            after > Duration::from_secs(5),
            "the wake keeps its deadline"
        );
        let row = read_timer(&storage, &key).await.expect("reads");
        assert_eq!(row, Some(before));
    }

    /// A timer row store on the test runtime: write number `held` is stored, then answered only
    /// after `arrived` is notified and the returned sender fires. The first `refused` reads fail.
    fn gated_store(
        held: usize,
        refused: usize,
    ) -> (StorageHandle, Arc<Notify>, oneshot::Sender<()>) {
        let (store, receivers) = StorageHandle::new();
        let receiver = receivers.foreground.into_async();
        let arrived = Arc::new(Notify::new());
        let (release, mut gate) = oneshot::channel::<()>();
        let signal = arrived.clone();
        tokio::spawn(async move {
            let mut rows = std::collections::HashMap::new();
            let (mut writes, mut reads) = (0, 0);
            while let Ok((effect, response, ..)) = receiver.recv().await {
                let event = match effect {
                    StorageEffect::Read { .. } if reads < refused => {
                        reads += 1;
                        let error = aruna_core::errors::StorageError::QueueFull;
                        StorageEvent::Error { error }
                    }
                    StorageEffect::Read { key, .. } => {
                        let value = rows.get(&key).cloned();
                        StorageEvent::ReadResult { key, value }
                    }
                    StorageEffect::Write { key, value, .. } => {
                        rows.insert(key.clone(), value);
                        if writes == held {
                            signal.notify_one();
                            let _ = (&mut gate).await;
                        }
                        writes += 1;
                        StorageEvent::WriteResult { key }
                    }
                    StorageEffect::Delete { key, .. } => {
                        rows.remove(&key);
                        StorageEvent::DeleteResult { key }
                    }
                    other => panic!("unexpected storage effect: {other:?}"),
                };
                response.send(event);
            }
        });
        (store, arrived, release)
    }

    /// A timer key no other test changes, so no other test holds its change lock.
    fn gated_key() -> TaskKey {
        TaskKey::RealmPresence {
            realm_id: aruna_core::structs::identity::realm::RealmId::from_bytes([0x5a; 32]),
            node_id: iroh::SecretKey::from_bytes(&[0x5b; 32]).public(),
        }
    }

    /// A durable shorten of `key` to `after`, run on its own task.
    fn spawn_shorten(
        store: &StorageHandle,
        task_handle: &TaskHandle,
        key: &TaskKey,
        after: Duration,
    ) -> tokio::task::JoinHandle<Event> {
        let (store, task_handle) = (store.clone(), task_handle.clone());
        let shorten = TaskEffect::ShortenTimer {
            key: key.clone(),
            after,
        };
        tokio::spawn(async move { change_timer(&store, Some(&task_handle), shorten, false).await })
    }

    /// A durable reset of `key` to sixty seconds, run on its own task.
    fn spawn_reset(
        store: &StorageHandle,
        task_handle: &TaskHandle,
        key: &TaskKey,
        keep_live: bool,
    ) -> tokio::task::JoinHandle<Event> {
        let (store, task_handle) = (store.clone(), task_handle.clone());
        let reset = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(60),
        };
        tokio::spawn(
            async move { change_timer(&store, Some(&task_handle), reset, keep_live).await },
        )
    }

    // A shorten whose row write took five seconds arms at the deadline it planned, so the live
    // wake does not count its delay again after the write.
    #[tokio::test(start_paused = true)]
    async fn held_write_deadline() {
        let task_handle = TaskHandle::new();
        let (store, arrived, release) = gated_store(0, 0);
        let key = gated_key();
        let change = spawn_shorten(&store, &task_handle, &key, Duration::from_secs(60));
        arrived.notified().await;
        tokio::time::advance(Duration::from_secs(5)).await;
        release.send(()).expect("the write waits");
        change.await.expect("the change ends");
        let (event, _) = task_handle
            .plan_shorten(key, Duration::from_secs(120), 1)
            .await;
        let Event::Task(TaskEvent::TimerScheduled { after, .. }) = event else {
            panic!("the armed wake is answered");
        };
        assert_eq!(after, Duration::from_secs(55));
    }

    // A shorten whose row write is answered only after the storage timeout reads the stored row
    // once the write settled, also after a refused read, so the committed row has its live wake.
    #[tokio::test(start_paused = true)]
    async fn late_write_armed() {
        for refused in [0, 1] {
            let task_handle = TaskHandle::new();
            let (store, arrived, release) = gated_store(0, refused);
            let key = gated_key();
            let change = spawn_shorten(&store, &task_handle, &key, Duration::from_secs(60));
            arrived.notified().await;
            tokio::time::advance(Duration::from_secs(11)).await;
            release.send(()).expect("the write waits");
            let event = change.await.expect("the change ends");
            assert!(matches!(
                event,
                Event::Task(TaskEvent::TimerScheduled { .. })
            ));
            let row = read_timer(&store, &key).await.expect("reads");
            let row = row.expect("the row is stored");
            let (_, plan) = task_handle
                .plan_shorten(key, Duration::from_secs(120), 1)
                .await;
            assert_eq!(plan.and_then(|plan| plan.holder), Some(row.admission));
        }
    }

    // A best-effort reset whose row write times out is armed at once, owned by the admission its
    // late row carries, so a slow storage never holds back the live retry.
    #[tokio::test(start_paused = true)]
    async fn best_effort_armed() {
        let task_handle = TaskHandle::new();
        let (store, arrived, release) = gated_store(0, 0);
        let key = gated_key();
        let change = spawn_reset(&store, &task_handle, &key, true);
        arrived.notified().await;
        let waited = tokio::time::timeout(Duration::from_secs(30), change).await;
        let event = waited.expect("armed without waiting");
        assert!(matches!(
            event.expect("the change ends"),
            Event::Task(TaskEvent::TimerScheduled { .. })
        ));
        release.send(()).expect("the write waits");
        let row = read_timer(&store, &key).await.expect("reads");
        let row = row.expect("the row is stored");
        let (_, plan) = task_handle
            .plan_shorten(key, Duration::from_secs(120), 1)
            .await;
        assert_eq!(plan.and_then(|plan| plan.holder), Some(row.admission));
    }

    /// Deletes the row its firing owns, as a production firing does, and reports start and end.
    struct DeletingFiring {
        storage: StorageHandle,
        started: Arc<Notify>,
        ended: Arc<Notify>,
    }

    #[async_trait]
    impl InboundTaskHandler for DeletingFiring {
        async fn handle_timer(&self, _key: TaskKey) {}

        async fn handle_firing(&self, key: TaskKey, stamp: Option<u64>) {
            self.started.notify_one();
            aruna_storage::task_timers::delete_persisted_timer(&self.storage, &key, stamp).await;
            self.ended.notify_one();
        }
    }

    // A later wake that fires while a shorten's row write is held serves that shorten: no second
    // wake is armed, and the fired run owns the row and deletes it.
    #[tokio::test(start_paused = true)]
    async fn fired_wake_owns() {
        let task_handle = TaskHandle::new();
        let (store, arrived, release) = gated_store(1, 0);
        let (started, ended) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        let firing = DeletingFiring {
            storage: store.clone(),
            started: started.clone(),
            ended: ended.clone(),
        };
        task_handle.set_inbound_handler(Arc::new(firing)).await;
        let key = gated_key();
        let reset = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(3),
        };
        change_timer(&store, Some(&task_handle), reset, false).await;
        let change = spawn_shorten(&store, &task_handle, &key, Duration::from_secs(1));
        arrived.notified().await;
        tokio::time::advance(Duration::from_secs(4)).await;
        started.notified().await;
        release.send(()).expect("the write waits");
        change.await.expect("the change ends");
        ended.notified().await;
        assert_eq!(read_timer(&store, &key).await.expect("reads"), None);
        let (_, plan) = task_handle
            .plan_shorten(key, Duration::from_secs(60), 1)
            .await;
        assert_eq!(plan.and_then(|plan| plan.holder), Some(1), "no second wake");
    }

    // A caller that goes away after its row write committed still leaves one live wake, owned by
    // the row's admission.
    #[tokio::test]
    async fn cancelled_caller_armed() {
        let task_handle = TaskHandle::new();
        let (store, arrived, release) = gated_store(0, 0);
        let key = gated_key();
        let caller = spawn_shorten(&store, &task_handle, &key, Duration::from_secs(60));
        arrived.notified().await;
        caller.abort();
        let cancelled = caller.await.expect_err("the caller is cancelled");
        assert!(cancelled.is_cancelled());
        release.send(()).expect("the write waits");
        // The change lock is free once the handoff ended.
        drop(hold_timer(&key).await);
        let row = read_timer(&store, &key).await.expect("reads");
        let row = row.expect("the row is stored");
        let (_, plan) = task_handle
            .plan_shorten(key, Duration::from_secs(120), 1)
            .await;
        assert_eq!(plan.and_then(|plan| plan.holder), Some(row.admission));
    }

    // Two resets with the same deadline are two admissions: the first one's firing leaves the
    // row the second one wrote.
    #[tokio::test]
    async fn equal_resets_owned() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let task_handle = TaskHandle::new();
        let key = TaskKey::PublishRegistration;
        let reset = || TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(60),
        };
        change_timer(&storage, Some(&task_handle), reset(), false).await;
        let first = read_timer(&storage, &key).await.expect("reads");
        let first = first.expect("row");
        change_timer(&storage, Some(&task_handle), reset(), false).await;
        let second = read_timer(&storage, &key).await.expect("reads");
        let second = second.expect("row");
        assert_ne!(first.admission, second.admission);
        let stamp = Some(first.admission);
        aruna_storage::task_timers::delete_persisted_timer(&storage, &key, stamp).await;
        assert_eq!(
            read_timer(&storage, &key).await.expect("reads"),
            Some(second)
        );
    }

    // A row that stopped decoding is deleted only while it still does not decode, so a timer a
    // later change wrote for its key stays.
    #[tokio::test]
    async fn malformed_delete_rechecks() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::RefreshBlobHolders;
        let row_key = key.storage_key().expect("key encodes");
        let reset = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(60),
        };
        persist_task_effect(&storage, &reset)
            .await
            .expect("row written");
        delete_malformed(&storage, row_key.clone())
            .await
            .expect("checked");
        assert!(read_timer(&storage, &key).await.expect("reads").is_some());
        let garbage = StorageEffect::Write {
            key_space: TASK_TIMER_KEYSPACE.to_string(),
            key: row_key.clone(),
            value: ByteView::from(vec![0xff, 0xff]),
            txn_id: None,
        };
        storage.send_storage_effect(garbage).await;
        delete_malformed(&storage, row_key).await.expect("deleted");
        assert_eq!(read_timer(&storage, &key).await.expect("reads"), None);
    }

    // A failed page stops the restoration with its resume point instead of ending it as done, and
    // a retry from there arms the timer.
    #[tokio::test]
    async fn failed_page_resumes() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::RefreshBlobHolders;
        let reset = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_millis(1),
        };
        persist_task_effect(&storage, &reset)
            .await
            .expect("timer persists");
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first_page = move |effect: &StorageEffect| {
            let page = matches!(effect, StorageEffect::Iter { .. });
            page && !failed.swap(true, std::sync::atomic::Ordering::SeqCst)
        };
        let flaky = aruna_test_support::metadata::refusing_when(storage.clone(), first_page);
        let task_handle = TaskHandle::new();
        let observed = Arc::new(Mutex::new(None));
        let notify = Arc::new(Notify::new());
        task_handle
            .set_inbound_handler(Arc::new(RecordingHandler {
                observed: observed.clone(),
                notify: notify.clone(),
            }))
            .await;

        let restored = restore_task_timers(&flaky, &task_handle, None).await;
        let stopped = restored.expect_err("the failed page stops the restoration");
        assert_eq!(stopped.resume_after, None);
        restore_task_timers(&flaky, &task_handle, stopped.resume_after)
            .await
            .expect("the retry restores");
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("restored timer should fire");
        assert_eq!(*observed.lock().await, Some(key));
    }

    // A fired timer removes only the row its admission owns: a firing without one, or of another
    // admission, leaves a due row for the wake that owns it.
    #[tokio::test]
    async fn fired_keeps_replacement() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::RefreshBlobHolders;
        let reset = |after| TaskEffect::ResetTimer {
            key: key.clone(),
            after,
        };
        let later = reset(Duration::from_secs(3600));
        persist_task_effect(&storage, &later)
            .await
            .expect("timer persists");

        aruna_storage::task_timers::delete_persisted_timer(&storage, &key, Some(7)).await;
        assert!(
            read_timer(&storage, &key)
                .await
                .expect("timer reads")
                .is_some()
        );

        let due = reset(Duration::ZERO);
        persist_task_effect(&storage, &due)
            .await
            .expect("timer persists");
        aruna_storage::task_timers::delete_persisted_timer(&storage, &key, None).await;
        let unstamped = read_timer(&storage, &key).await.expect("timer reads");
        assert!(
            unstamped.is_some(),
            "a firing without an admission owns no row"
        );
        let owner = Some(aruna_storage::task_timers::NO_ADMISSION);
        aruna_storage::task_timers::delete_persisted_timer(&storage, &key, owner).await;
        assert!(
            read_timer(&storage, &key)
                .await
                .expect("timer reads")
                .is_none()
        );
    }

    // A cancellation whose row could not be deleted fails, so no caller acknowledges it while the
    // row would schedule the work again at the next restart.
    #[tokio::test]
    async fn failed_cancel_errs() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::RefreshBlobHolders;
        let reset = TaskEffect::ResetTimer {
            key: key.clone(),
            after: Duration::from_secs(60),
        };
        persist_task_effect(&storage, &reset)
            .await
            .expect("timer persists");
        let refused = aruna_test_support::metadata::refusing(storage.clone(), TASK_TIMER_KEYSPACE);

        let cancel = TaskEffect::CancelTimer { key: key.clone() };
        assert!(persist_task_effect(&refused, &cancel).await.is_err());
        assert!(
            read_timer(&storage, &key)
                .await
                .expect("timer reads")
                .is_some()
        );
    }

    #[tokio::test]
    async fn outbox_timer_restores() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::DrainSyncOutbox;
        let reset_after = Duration::from_secs(3600);

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: key.clone(),
                after: reset_after,
            },
        )
        .await
        .expect("drain timer persists");

        let reset_timer = read_timer(&storage, &key)
            .await
            .expect("reset timer reads")
            .expect("reset timer exists");

        let shorten_after = Duration::from_secs(1800);
        persist_task_effect(
            &storage,
            &TaskEffect::ShortenTimer {
                key: key.clone(),
                after: shorten_after,
            },
        )
        .await
        .expect("shortened drain timer persists");

        let shortened_timer = read_timer(&storage, &key)
            .await
            .expect("shortened timer reads")
            .expect("shortened timer exists");
        assert_eq!(shortened_timer.key, key);
        assert!(shortened_timer.due_unix_millis < reset_timer.due_unix_millis);

        let task_handle = TaskHandle::new();
        restore_task_timers(&storage, &task_handle, None)
            .await
            .expect("timers restore");
        let aruna_core::task::TaskEvent::TimerScheduled { after, .. } =
            task_handle.schedule_idle_timer(key, Duration::ZERO).await
        else {
            panic!("expected restored timer");
        };
        assert!(
            after > Duration::ZERO,
            "restored deadline must retain a non-immediate retry"
        );
    }

    #[tokio::test]
    async fn metadata_graph_persisted() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: TaskKey::DrainPruneQueue,
                after: Duration::ZERO,
            },
        )
        .await
        .expect("prune drain timer persistence is redundant");

        assert_eq!(storage.snapshot_metrics().requests_total, 0);
    }

    #[tokio::test]
    async fn drain_notification_persisted() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: TaskKey::DrainNotificationOutbox,
                after: Duration::ZERO,
            },
        )
        .await
        .expect("notification drain timer persistence is redundant");

        assert_eq!(storage.snapshot_metrics().requests_total, 0);
    }

    #[tokio::test]
    async fn prune_timer_persisted() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: TaskKey::PruneNotifications,
                after: Duration::ZERO,
            },
        )
        .await
        .expect("prune timer persistence is redundant");

        assert_eq!(storage.snapshot_metrics().requests_total, 0);
    }

    #[tokio::test]
    async fn metadata_projection_persisted() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: TaskKey::DrainProjectionQueue,
                after: Duration::ZERO,
            },
        )
        .await
        .expect("projection timer persists");

        let timer = read_timer(&storage, &TaskKey::DrainProjectionQueue)
            .await
            .expect("timer reads")
            .expect("projection timer exists");
        assert_eq!(timer.key, TaskKey::DrainProjectionQueue);
    }

    #[tokio::test]
    async fn timed_locks_ephemeral() {
        // A restart locks the bucket anyway, so its timed lock never reaches storage.
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::LockBucket {
            bucket_id: ulid::Ulid::from_bytes([1; 16]),
            generation: 2,
            session_id: ulid::Ulid::from_bytes([3; 16]),
        };
        let after = Duration::from_secs(60);
        for effect in [
            TaskEffect::ResetTimer {
                key: key.clone(),
                after,
            },
            TaskEffect::ShortenTimer {
                key: key.clone(),
                after,
            },
        ] {
            persist_task_effect(&storage, &effect)
                .await
                .expect("nothing to persist");
        }
        assert_eq!(read_timer(&storage, &key).await.expect("timer read"), None);
    }

    #[tokio::test]
    async fn hidden_sweep_persists() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: TaskKey::SweepHiddenBlobs,
                after: Duration::ZERO,
            },
        )
        .await
        .expect("sweep timer persists");

        let timer = read_timer(&storage, &TaskKey::SweepHiddenBlobs)
            .await
            .expect("timer read")
            .expect("timer exists");
        assert_eq!(timer.key, TaskKey::SweepHiddenBlobs);
    }

    #[tokio::test]
    async fn metadata_projection_handle() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let storage = FjallStorage::open(temp_dir.path().to_str().expect("utf-8 path"))
            .expect("storage opens");
        let key = TaskKey::DrainProjectionQueue;

        persist_task_effect(
            &storage,
            &TaskEffect::ResetTimer {
                key: key.clone(),
                after: Duration::from_millis(1),
            },
        )
        .await
        .expect("timer persists");

        let task_handle = TaskHandle::new();
        let observed = Arc::new(Mutex::new(None));
        let notify = Arc::new(Notify::new());
        task_handle
            .set_inbound_handler(Arc::new(RecordingHandler {
                observed: observed.clone(),
                notify: notify.clone(),
            }))
            .await;

        restore_task_timers(&storage, &task_handle, None)
            .await
            .expect("timers restore");
        tokio::time::timeout(Duration::from_secs(1), notify.notified())
            .await
            .expect("restored timer should fire");

        assert_eq!(*observed.lock().await, Some(key));
    }
}
