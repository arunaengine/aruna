//! Runs keyspace compaction jobs on a background thread once deleted rows pile up.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use aruna_core::telemetry::duration_ms;
use fjall::{
    AbstractTree, KeyspaceCreateOptions, OptimisticTxDatabase, OptimisticTxKeyspace, Readable,
};
use tracing::{debug, warn};

pub(crate) const DELETE_THRESHOLD: u64 = 5_000;
pub(crate) const MAX_KEYSPACE_BYTES: u64 = 256 * 1024 * 1024;
/// Least time between two compactions of one keyspace. Each flushes its memtable while every
/// commit waits, so a queue that deletes thousands of rows a second must not compact each time.
pub(crate) const COMPACT_INTERVAL: Duration = Duration::from_secs(60);
/// Key a checkpoint writes back unchanged, or removes while it is absent.
const CHECKPOINT: &[u8] = b"\xffcheckpoint";
/// Journal count that calls for a sweep, about twice Fjall's own 512 MiB journal limit.
pub(crate) const JOURNAL_LIMIT: usize = 16;
/// Job name of a sweep, which no keyspace name matches.
const SWEEP: &str = "journal sweep";

pub(crate) struct CompactionJob {
    pub(crate) key_space: String,
    pub(crate) deletes: u64,
    pub(crate) run: Box<dyn FnOnce() -> fjall::Result<()> + Send>,
}

pub(crate) struct Compactor {
    pub(crate) sender: Option<std::sync::mpsc::Sender<CompactionJob>>,
    thread: Option<thread::JoinHandle<()>>,
    pub(crate) active: Arc<Mutex<HashSet<String>>>,
    pub(crate) stopping: Arc<AtomicBool>,
    /// When each keyspace was last queued for compaction.
    pub(crate) queued: HashMap<String, Instant>,
    /// Least time from one queued compaction of a keyspace to its next.
    pub(crate) interval: Duration,
}

impl Compactor {
    pub(crate) fn spawn() -> Self {
        let (sender, receiver) = std::sync::mpsc::channel::<CompactionJob>();
        let active = Arc::new(Mutex::new(HashSet::new()));
        let stopping = Arc::new(AtomicBool::new(false));
        let running = active.clone();
        let stop = stopping.clone();
        let thread = thread::spawn(move || {
            while let Ok(job) = receiver.recv() {
                if stop.load(Ordering::Acquire) {
                    break;
                }
                run_job(job, &running);
            }
        });
        Self {
            sender: Some(sender),
            thread: Some(thread),
            active,
            stopping,
            queued: HashMap::new(),
            interval: COMPACT_INTERVAL,
        }
    }

    #[cfg(test)]
    pub(crate) fn idle() -> Self {
        Self {
            sender: None,
            thread: None,
            active: Arc::new(Mutex::new(HashSet::new())),
            stopping: Arc::new(AtomicBool::new(false)),
            queued: HashMap::new(),
            interval: Duration::ZERO,
        }
    }

    #[cfg(test)]
    pub(crate) fn stub(sender: std::sync::mpsc::Sender<CompactionJob>) -> Self {
        Self {
            sender: Some(sender),
            thread: None,
            active: Arc::new(Mutex::new(HashSet::new())),
            stopping: Arc::new(AtomicBool::new(false)),
            queued: HashMap::new(),
            interval: Duration::ZERO,
        }
    }

    pub(crate) fn submit(
        &mut self,
        key_space: &str,
        deletes: u64,
        db: OptimisticTxDatabase,
        keyspace: OptimisticTxKeyspace,
    ) {
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        let now = Instant::now();
        let last = self.queued.get(key_space);
        if last.is_some_and(|last| now.duration_since(*last) < self.interval) {
            return;
        }
        if !self
            .active
            .lock()
            .expect("storage compaction mutex poisoned")
            .insert(key_space.to_string())
        {
            return;
        }
        self.queued.insert(key_space.to_string(), now);
        let job = CompactionJob {
            key_space: key_space.to_string(),
            deletes,
            run: Box::new(move || {
                let inner: &fjall::Keyspace = keyspace.as_ref();
                inner.rotate_memtable_and_wait()?;
                inner.major_compact()?;
                checkpoint(&db, &keyspace)
            }),
        };
        if sender.send(job).is_err() {
            self.release(key_space);
        }
    }

    /// Checkpoints every keyspace without unflushed rows, since any of them may hold old journals.
    pub(crate) fn sweep(&self, db: OptimisticTxDatabase) {
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        if !self
            .active
            .lock()
            .expect("storage compaction mutex poisoned")
            .insert(SWEEP.to_string())
        {
            return;
        }
        let job = CompactionJob {
            key_space: SWEEP.to_string(),
            deletes: 0,
            run: Box::new(move || {
                for name in db.list_keyspace_names() {
                    let keyspace = db.keyspace(&name, KeyspaceCreateOptions::default)?;
                    let inner = keyspace.inner();
                    // Fjall flushes keyspaces with unflushed rows itself once journals grow.
                    if inner.tree.active_memtable().is_empty() && inner.sealed_memtable_count() == 0
                    {
                        checkpoint(&db, &keyspace)?;
                    }
                }
                Ok(())
            }),
        };
        if sender.send(job).is_err() {
            self.release(SWEEP);
        }
    }

    fn release(&self, key_space: &str) {
        self.active
            .lock()
            .expect("storage compaction mutex poisoned")
            .remove(key_space);
    }

    pub(crate) fn shutdown(&mut self) {
        self.stopping.store(true, Ordering::Release);
        self.sender = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Compactor {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Flushes a write newer than every journal holding `keyspace`. Compaction can drop its newest
/// rows, and Fjall keeps those journals until the keyspace has a table that new again.
fn checkpoint(db: &OptimisticTxDatabase, keyspace: &OptimisticTxKeyspace) -> fjall::Result<()> {
    let mut tx = db.write_tx()?;
    // The read turns a concurrent write of the key into a conflict, so no row ever changes.
    match tx.get(keyspace, CHECKPOINT)? {
        Some(value) => tx.insert(keyspace, CHECKPOINT, value),
        None => tx.remove(keyspace, CHECKPOINT),
    }
    // After a conflict the other write is newer still, so the flush covers it as well.
    tx.commit()?.ok();
    keyspace.inner().rotate_memtable().map(drop)
}

/// Whether `count` journals call for a sweep, and the count to remember. A further sweep waits
/// for another limit of journals, so a pin no sweep clears is retried rarely.
pub(crate) fn sweep_due(count: usize, swept: usize) -> (bool, usize) {
    if count < JOURNAL_LIMIT {
        (false, 0)
    } else if count >= swept + JOURNAL_LIMIT {
        (true, count)
    } else {
        (false, swept)
    }
}

pub(crate) fn compactable(disk_space: u64) -> bool {
    disk_space <= MAX_KEYSPACE_BYTES
}

pub(crate) fn run_job(job: CompactionJob, active: &Arc<Mutex<HashSet<String>>>) {
    let CompactionJob {
        key_space,
        deletes,
        run,
    } = job;
    active
        .lock()
        .expect("storage compaction mutex poisoned")
        .remove(&key_space);
    let started = Instant::now();
    match run() {
        Ok(()) => debug!(
            event = "storage.keyspace.compacted",
            key_space = %key_space,
            deletes,
            elapsed_ms = duration_ms(started.elapsed()),
            "Compacted a delete-heavy keyspace"
        ),
        Err(error) => warn!(
            event = "storage.keyspace.compact_failed",
            key_space = %key_space,
            deletes,
            error = %error,
            "Keyspace compaction failed"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CompactionJob, Compactor, DELETE_THRESHOLD, JOURNAL_LIMIT, MAX_KEYSPACE_BYTES, compactable,
        sweep_due,
    };
    use std::sync::atomic::Ordering;

    #[test]
    fn size_gate() {
        assert!(compactable(0));
        assert!(compactable(MAX_KEYSPACE_BYTES));
        assert!(!compactable(MAX_KEYSPACE_BYTES + 1));
    }

    #[test]
    fn sweep_gate() {
        assert_eq!(sweep_due(JOURNAL_LIMIT - 1, 0), (false, 0));
        assert_eq!(sweep_due(JOURNAL_LIMIT, 0), (true, JOURNAL_LIMIT));
        assert_eq!(
            sweep_due(2 * JOURNAL_LIMIT - 1, JOURNAL_LIMIT),
            (false, JOURNAL_LIMIT)
        );
        assert_eq!(
            sweep_due(2 * JOURNAL_LIMIT, JOURNAL_LIMIT),
            (true, 2 * JOURNAL_LIMIT)
        );
        assert_eq!(
            sweep_due(3, 2 * JOURNAL_LIMIT),
            (false, 0),
            "a drop below resets"
        );
    }

    #[test]
    fn failure_continues() {
        let mut compactor = Compactor::spawn();
        let (done, finished) = std::sync::mpsc::channel();
        let sender = compactor.sender.clone().expect("compactor accepts jobs");
        compactor
            .active
            .lock()
            .expect("compaction mutex")
            .insert("failing".to_string());
        sender
            .send(CompactionJob {
                key_space: "failing".to_string(),
                deletes: DELETE_THRESHOLD,
                run: Box::new(|| Err(fjall::Error::Poisoned)),
            })
            .expect("failing job is queued");
        sender
            .send(CompactionJob {
                key_space: "next".to_string(),
                deletes: 1,
                run: Box::new(move || {
                    done.send(()).expect("test receives signal");
                    Ok(())
                }),
            })
            .expect("next job is queued");
        drop(sender);

        finished.recv().expect("next job ran");
        compactor.shutdown();
        assert!(
            compactor
                .active
                .lock()
                .expect("compaction mutex")
                .is_empty()
        );
    }

    #[test]
    fn shutdown_drops_backlog() {
        let mut compactor = Compactor::spawn();
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let (entered, running) = std::sync::mpsc::channel::<()>();
        let (ran, second) = std::sync::mpsc::channel::<()>();
        let sender = compactor.sender.clone().expect("compactor accepts jobs");
        sender
            .send(CompactionJob {
                key_space: "first".to_string(),
                deletes: 1,
                run: Box::new(move || {
                    entered.send(()).expect("first job observed");
                    blocked.recv().expect("first job released");
                    Ok(())
                }),
            })
            .expect("first job is queued");
        sender
            .send(CompactionJob {
                key_space: "second".to_string(),
                deletes: 1,
                run: Box::new(move || {
                    ran.send(()).expect("second job observed");
                    Ok(())
                }),
            })
            .expect("second job is queued");
        drop(sender);

        running
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("first job is running");
        compactor.stopping.store(true, Ordering::Release);
        release.send(()).expect("first job is released");
        compactor.shutdown();
        assert!(second.try_recv().is_err(), "backlog ran after shutdown");
    }
}
