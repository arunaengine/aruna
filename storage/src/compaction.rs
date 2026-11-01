//! Runs keyspace compaction jobs on a background thread once deleted rows pile up.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
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
}

/// Keeps the compactions of one keyspace an interval apart. Work that comes sooner waits,
/// merged per keyspace, until the interval has passed; sweeps never wait.
pub(crate) struct Spacing {
    interval: Duration,
    /// When the last compaction of each keyspace started.
    last: HashMap<String, Instant>,
    waiting: HashMap<String, CompactionJob>,
}

impl Spacing {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            last: HashMap::new(),
            waiting: HashMap::new(),
        }
    }

    fn due_at(&self, key_space: &str) -> Option<Instant> {
        self.last.get(key_space).map(|last| *last + self.interval)
    }

    /// The job to run now, or `None` when it waits for its keyspace's interval.
    pub(crate) fn offer(&mut self, job: CompactionJob, now: Instant) -> Option<CompactionJob> {
        if job.key_space == SWEEP {
            return Some(job);
        }
        if let Some(waiting) = self.waiting.get_mut(&job.key_space) {
            waiting.deletes = waiting.deletes.saturating_add(job.deletes);
            return None;
        }
        if self.due_at(&job.key_space).is_some_and(|due| now < due) {
            self.waiting.insert(job.key_space.clone(), job);
            return None;
        }
        Some(job)
    }

    /// The waiting jobs whose interval has passed at `now`.
    pub(crate) fn due(&mut self, now: Instant) -> Vec<CompactionJob> {
        let ready: Vec<String> = (self.waiting.keys())
            .filter(|key_space| self.due_at(key_space).is_none_or(|due| due <= now))
            .cloned()
            .collect();
        (ready.iter())
            .filter_map(|key_space| self.waiting.remove(key_space))
            .collect()
    }

    /// When the first waiting job becomes due; `None` while nothing waits.
    pub(crate) fn next_due(&self) -> Option<Instant> {
        (self.waiting.keys())
            .filter_map(|key_space| self.due_at(key_space))
            .min()
    }

    /// Notes that a compaction of `key_space` starts at `now`.
    pub(crate) fn started(&mut self, key_space: &str, now: Instant) {
        if key_space != SWEEP {
            self.last.insert(key_space.to_string(), now);
        }
    }
}

impl Compactor {
    pub(crate) fn spawn() -> Self {
        Self::spaced(COMPACT_INTERVAL)
    }

    /// A compactor whose thread keeps compactions of one keyspace `interval` apart.
    pub(crate) fn spaced(interval: Duration) -> Self {
        let (sender, receiver) = std::sync::mpsc::channel::<CompactionJob>();
        let active = Arc::new(Mutex::new(HashSet::new()));
        let stopping = Arc::new(AtomicBool::new(false));
        let running = active.clone();
        let stop = stopping.clone();
        let thread = thread::spawn(move || {
            let mut spacing = Spacing::new(interval);
            loop {
                // Waiting work wakes the thread once due, even if no other job arrives.
                let received = match spacing.next_due() {
                    Some(due) => {
                        receiver.recv_timeout(due.saturating_duration_since(Instant::now()))
                    }
                    None => receiver.recv().map_err(|_| RecvTimeoutError::Disconnected),
                };
                let offered = match received {
                    Ok(job) => Some(job),
                    Err(RecvTimeoutError::Timeout) => None,
                    Err(RecvTimeoutError::Disconnected) => break,
                };
                let now = Instant::now();
                let ready = offered.and_then(|job| spacing.offer(job, now));
                for job in ready.into_iter().chain(spacing.due(now)) {
                    if stop.load(Ordering::Acquire) {
                        return;
                    }
                    spacing.started(&job.key_space, Instant::now());
                    run_job(job, &running);
                }
            }
        });
        Self {
            sender: Some(sender),
            thread: Some(thread),
            active,
            stopping,
        }
    }

    #[cfg(test)]
    pub(crate) fn idle() -> Self {
        Self {
            sender: None,
            thread: None,
            active: Arc::new(Mutex::new(HashSet::new())),
            stopping: Arc::new(AtomicBool::new(false)),
        }
    }

    #[cfg(test)]
    pub(crate) fn stub(sender: std::sync::mpsc::Sender<CompactionJob>) -> Self {
        Self {
            sender: Some(sender),
            thread: None,
            active: Arc::new(Mutex::new(HashSet::new())),
            stopping: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Hands a keyspace's compaction to the thread, which runs it once its interval allows; a
    /// compaction of the keyspace that has not started yet covers these deletes as well.
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
        if !self
            .active
            .lock()
            .expect("storage compaction mutex poisoned")
            .insert(key_space.to_string())
        {
            return;
        }
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
        CompactionJob, Compactor, DELETE_THRESHOLD, JOURNAL_LIMIT, MAX_KEYSPACE_BYTES, SWEEP,
        Spacing, compactable, sweep_due,
    };
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    fn job(key_space: &str, deletes: u64) -> CompactionJob {
        CompactionJob {
            key_space: key_space.to_string(),
            deletes,
            run: Box::new(|| Ok(())),
        }
    }

    // Work that comes within the interval waits, merged per keyspace, and is due once the
    // interval passed; other keyspaces and sweeps do not wait.
    #[test]
    fn waiting_work_due() {
        let start = Instant::now();
        let at = |secs| start + Duration::from_secs(secs);
        let mut spacing = Spacing::new(Duration::from_secs(60));
        let first = spacing.offer(job("queue", 5_000), at(0));
        spacing.started(&first.expect("the first runs at once").key_space, at(0));

        assert!(spacing.offer(job("queue", 5_000), at(10)).is_none());
        assert!(spacing.offer(job("queue", 3_000), at(20)).is_none());
        assert!(spacing.offer(job("other", 1), at(20)).is_some());
        assert!(spacing.offer(job(SWEEP, 0), at(20)).is_some());
        assert_eq!(spacing.next_due(), Some(at(60)));
        assert!(spacing.due(at(59)).is_empty());
        let due = spacing.due(at(60));
        let due: Vec<_> = due
            .iter()
            .map(|job| (job.key_space.as_str(), job.deletes))
            .collect();
        assert_eq!(due, [("queue", 8_000)]);
        assert_eq!(spacing.next_due(), None);
    }

    // The thread runs waiting work once its interval passed, though no further job arrives.
    #[test]
    fn deferred_job_runs() {
        let mut compactor = Compactor::spaced(Duration::from_millis(20));
        let sender = compactor.sender.clone().expect("compactor accepts jobs");
        let (ran, runs) = std::sync::mpsc::channel();
        for deletes in [1, 2] {
            let ran = ran.clone();
            let run = Box::new(move || {
                ran.send(deletes).expect("test receives the run");
                Ok(())
            });
            let key_space = "queue".to_string();
            let job = CompactionJob {
                key_space,
                deletes,
                run,
            };
            sender.send(job).expect("job is queued");
        }
        // No job follows the second.
        drop(sender);
        // The caps only detect a job that never runs.
        let cap = Duration::from_secs(60);
        assert_eq!(runs.recv_timeout(cap), Ok(1));
        assert_eq!(runs.recv_timeout(cap), Ok(2));
        compactor.shutdown();
    }

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
