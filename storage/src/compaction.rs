use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use aruna_core::telemetry::duration_ms;
use fjall::OptimisticTxKeyspace;
use tracing::{debug, warn};

pub(crate) const DELETE_THRESHOLD: u64 = 5_000;
pub(crate) const MAX_KEYSPACE_BYTES: u64 = 256 * 1024 * 1024;

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

    pub(crate) fn submit(&self, key_space: &str, deletes: u64, keyspace: OptimisticTxKeyspace) {
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
                let keyspace: &fjall::Keyspace = keyspace.as_ref();
                keyspace.rotate_memtable_and_wait()?;
                keyspace.major_compact()
            }),
        };
        if sender.send(job).is_err() {
            self.release(key_space);
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
    use super::{CompactionJob, Compactor, DELETE_THRESHOLD, MAX_KEYSPACE_BYTES, compactable};
    use std::sync::atomic::Ordering;

    #[test]
    fn size_gate() {
        assert!(compactable(0));
        assert!(compactable(MAX_KEYSPACE_BYTES));
        assert!(!compactable(MAX_KEYSPACE_BYTES + 1));
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
