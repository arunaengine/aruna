//! Holds the registry of jobs running on this node: their slots, cancel tokens and class caps.
//! The tasks that run them live in the task wiring.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use aruna_core::heartbeat::RunningJobs;
use aruna_core::jobs::{JobKind, RunningJobView};
use aruna_core::structs::execution::job::{JobExecutionClass, JobId, JobRecord};
use tokio::sync::{RwLock, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::executor::{ProgressReporter, RangeTurns};
use super::{EXTERNAL_CONCURRENCY_CAP, JOB_CONCURRENCY_CAP};
use crate::jobs::context::ExternalReconciler;
use crate::jobs::context::PayloadHandler;

struct RunningJob {
    /// Per-execution identity: a re-spawned job at the same id gets a fresh nonce so a
    /// zombie's `finish` cannot evict the newer execution that replaced it.
    nonce: u64,
    class: JobExecutionClass,
    view: RunningJobView,
    /// The live counter of an in-process job; external attempts report elsewhere.
    progress: Option<ProgressReporter>,
    cancel: CancellationToken,
    completion: watch::Sender<bool>,
    claim_token: ulid::Ulid,
}

#[derive(Default)]
struct RuntimeState {
    running: HashMap<JobId, RunningJob>,
    /// A claim that raced the drain stop is released by a detached task; `shutdown` must
    /// be able to await it, or the process can exit before the lease is handed back.
    pending_releases: Vec<JoinHandle<()>>,
    draining: bool,
}

/// What a shutdown did with the jobs that were in flight.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct JobShutdownReport {
    /// Wound down inside the grace period and wrote their own state.
    pub finished: usize,
    /// Still running at grace expiry: lease handed back, no attempt spent.
    pub released: usize,
    /// Claim was no longer ours, so the lease was left alone.
    pub skipped: usize,
}

/// The slot of one admitted run.
pub struct RunSlot {
    pub nonce: u64,
    pub claim_token: ulid::Ulid,
    pub cancel: CancellationToken,
    pub progress: Option<ProgressReporter>,
}

/// What the registry did with a claimed job.
pub enum Admission {
    Run(RunSlot),
    /// A shutdown began, so the claim is being released.
    Draining,
    NoClaim,
}

/// What the registry did with an adopted external attempt.
pub enum RecoveredAdmission {
    Run {
        nonce: u64,
        cancel: CancellationToken,
    },
    /// This very claim already runs here.
    AlreadyRunning,
    /// Draining or at the class cap: the caller hands the attempt off.
    Refused,
    NoClaim,
}

/// One running job a shutdown waits for.
pub type RunWaiter = (JobId, JobExecutionClass, ulid::Ulid, watch::Receiver<bool>);

/// Registry of locally executing jobs, their cancellation tokens, and per-class caps.
pub struct JobsRuntime {
    state: Mutex<RuntimeState>,
    started: AtomicBool,
    next_nonce: AtomicU64,
    internal_cap: usize,
    external_cap: usize,
    reconciler: Mutex<Option<Arc<dyn ExternalReconciler>>>,
    claim_producer_barrier: RwLock<()>,
    /// Node shutdown. Deliberately not the per-job cancel token: that one means the
    /// user asked for a `Cancelled` job and runs cleanup handlers, while this one only
    /// means "stop here"; in-process jobs re-queue and external attempts hand off.
    shutdown: CancellationToken,
    range_turns: RangeTurns,
    handlers: Mutex<Vec<Arc<dyn PayloadHandler>>>,
}

impl std::fmt::Debug for JobsRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobsRuntime")
            .field("internal_cap", &self.internal_cap)
            .field("external_cap", &self.external_cap)
            .field("running", &self.running_count())
            .finish_non_exhaustive()
    }
}

impl JobsRuntime {
    pub fn new() -> Arc<Self> {
        Self::with_capacity(JOB_CONCURRENCY_CAP)
    }

    pub fn new_paused() -> Arc<Self> {
        Self::build(JOB_CONCURRENCY_CAP, EXTERNAL_CONCURRENCY_CAP, None, false)
    }

    pub fn with_capacity(cap: usize) -> Arc<Self> {
        Self::build(cap, EXTERNAL_CONCURRENCY_CAP, None, true)
    }

    /// Wire the reconcile hook that receives lost external attempts (Stage 2).
    pub fn with_reconciler(reconciler: Arc<dyn ExternalReconciler>) -> Arc<Self> {
        Self::build(
            JOB_CONCURRENCY_CAP,
            EXTERNAL_CONCURRENCY_CAP,
            Some(reconciler),
            true,
        )
    }

    fn build(
        internal_cap: usize,
        external_cap: usize,
        reconciler: Option<Arc<dyn ExternalReconciler>>,
        started: bool,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RuntimeState::default()),
            started: AtomicBool::new(started),
            next_nonce: AtomicU64::new(0),
            internal_cap,
            external_cap,
            reconciler: Mutex::new(reconciler),
            claim_producer_barrier: RwLock::new(()),
            shutdown: CancellationToken::new(),
            range_turns: RangeTurns::default(),
            handlers: Mutex::default(),
        })
    }

    fn state(&self) -> std::sync::MutexGuard<'_, RuntimeState> {
        self.state.lock().expect("runtime mutex poisoned")
    }

    pub fn reconciler(&self) -> Option<Arc<dyn ExternalReconciler>> {
        self.reconciler
            .lock()
            .expect("reconciler mutex poisoned")
            .clone()
    }

    pub fn set_reconciler(&self, reconciler: Arc<dyn ExternalReconciler>) {
        *self.reconciler.lock().expect("reconciler mutex poisoned") = Some(reconciler);
    }

    /// Adds the runner of payload kinds another domain owns; the node wiring calls it at start.
    pub fn register_handler(&self, handler: Arc<dyn PayloadHandler>) {
        self.handlers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(handler);
    }

    /// The registered runners of payload kinds other domains own.
    pub fn handlers(&self) -> Vec<Arc<dyn PayloadHandler>> {
        (self.handlers.lock())
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn running_count(&self) -> usize {
        self.state().running.len()
    }

    /// Jobs running here and the imports among them, as counts for the node telemetry.
    pub fn running_jobs(&self) -> RunningJobs {
        let state = self.state();
        let imports = state
            .running
            .values()
            .filter(|job| {
                matches!(
                    job.view.kind,
                    JobKind::Staging | JobKind::ImportRoCrate | JobKind::BackendImport
                )
            })
            .count();
        RunningJobs {
            total: u32::try_from(state.running.len()).unwrap_or(u32::MAX),
            imports: u32::try_from(imports).unwrap_or(u32::MAX),
        }
    }

    /// Jobs running here with live progress where the job counts something.
    pub fn running_list(&self) -> Vec<RunningJobView> {
        let state = self.state();
        state
            .running
            .values()
            .map(|job| RunningJobView {
                progress: job
                    .progress
                    .as_ref()
                    .map(ProgressReporter::snapshot)
                    .filter(|progress| progress.current > 0 || progress.total.is_some()),
                ..job.view.clone()
            })
            .collect()
    }

    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    pub fn start(&self) {
        self.started.store(true, Ordering::Release);
    }

    fn cap_for(&self, class: JobExecutionClass) -> usize {
        match class {
            JobExecutionClass::InProcess => self.internal_cap,
            JobExecutionClass::ExternalAttempt => self.external_cap,
        }
    }

    /// Free slots for a class; internal and external accounting are independent. Zero once
    /// draining, so the drain claims nothing more once a shutdown has begun.
    pub fn available_slots_for(&self, class: JobExecutionClass) -> usize {
        if !self.is_started() {
            return 0;
        }
        let state = self.state();
        if state.draining {
            return 0;
        }
        let running = state
            .running
            .values()
            .filter(|job| job.class == class)
            .count();
        self.cap_for(class).saturating_sub(running)
    }

    pub fn available_slots(&self) -> usize {
        self.available_slots_for(JobExecutionClass::InProcess)
    }

    /// Poke a locally running job's cancel token; `false` if it is not running here.
    pub fn request_cancel(&self, job_id: JobId) -> bool {
        match self.state().running.get(&job_id) {
            Some(job) => {
                job.cancel.cancel();
                true
            }
            None => false,
        }
    }

    /// Stops claiming further jobs and cancels the running ones, without
    /// waiting for them or handing their leases back.
    pub fn close_admission(&self) {
        self.state().draining = true;
        self.shutdown.cancel();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn register_test_execution(&self, job_id: JobId, class: JobExecutionClass) -> u64 {
        let nonce = self.next_nonce.fetch_add(1, Ordering::Relaxed);
        let (completion, _) = watch::channel(false);
        self.state().running.insert(
            job_id,
            RunningJob {
                nonce,
                class,
                view: RunningJobView {
                    kind: JobKind::Probe,
                    target: None,
                    group_id: None,
                    created_by: aruna_core::UserId::nil(
                        aruna_core::structs::identity::realm::RealmId([0; 32]),
                    ),
                    progress: None,
                    started_at_ms: 0,
                },
                progress: None,
                cancel: CancellationToken::new(),
                completion,
                claim_token: ulid::Ulid::generate(),
            },
        );
        nonce
    }

    pub fn is_draining(&self) -> bool {
        self.state().draining
    }

    pub fn claim_gate(&self) -> &RwLock<()> {
        &self.claim_producer_barrier
    }

    pub fn shutdown_signal(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub fn turns(&self) -> RangeTurns {
        self.range_turns.clone()
    }

    /// Takes a slot for a claimed job. A claim that raced the drain stop starts `release`
    /// under the lock, so a shutdown that follows always finds the release to wait for.
    pub fn admit(
        &self,
        record: &JobRecord,
        now_ms: u64,
        release: impl FnOnce(ulid::Ulid) -> JoinHandle<()>,
    ) -> Admission {
        let Some(claim_token) = record.claim.as_ref().map(|claim| claim.claim_token) else {
            return Admission::NoClaim;
        };
        let class = record.execution_class;
        let progress = (class != JobExecutionClass::ExternalAttempt)
            .then(|| ProgressReporter::from_progress(&record.progress));
        let nonce = self.next_nonce.fetch_add(1, Ordering::Relaxed);
        let cancel = CancellationToken::new();
        let (completion, _) = watch::channel(false);
        let mut state = self.state();
        if state.draining {
            let release = release(claim_token);
            state.pending_releases.push(release);
            return Admission::Draining;
        }
        state.running.insert(
            record.job_id,
            RunningJob {
                nonce,
                class,
                view: RunningJobView::new(record, now_ms),
                progress: progress.clone(),
                cancel: cancel.clone(),
                completion,
                claim_token,
            },
        );
        Admission::Run(RunSlot {
            nonce,
            claim_token,
            cancel,
            progress,
        })
    }

    /// Takes a slot for an adopted external supervisor under the normal class cap.
    pub fn admit_recovered(&self, record: &JobRecord, now_ms: u64) -> RecoveredAdmission {
        let job_id = record.job_id;
        let Some(claim_token) = record.claim.as_ref().map(|claim| claim.claim_token) else {
            return RecoveredAdmission::NoClaim;
        };
        let class = record.execution_class;
        let nonce = self.next_nonce.fetch_add(1, Ordering::Relaxed);
        let cancel = CancellationToken::new();
        let (completion, _) = watch::channel(false);
        let mut state = self.state();
        if state.draining {
            return RecoveredAdmission::Refused;
        }
        if state
            .running
            .get(&job_id)
            .is_some_and(|job| job.claim_token == claim_token)
        {
            return RecoveredAdmission::AlreadyRunning;
        }
        let replacing = state.running.contains_key(&job_id);
        let running = state
            .running
            .values()
            .filter(|job| job.class == class)
            .count();
        if !replacing && running >= self.cap_for(class) {
            return RecoveredAdmission::Refused;
        }
        state.running.insert(
            job_id,
            RunningJob {
                nonce,
                class,
                view: RunningJobView::new(record, now_ms),
                progress: None,
                cancel: cancel.clone(),
                completion,
                claim_token,
            },
        );
        RecoveredAdmission::Run { nonce, cancel }
    }

    /// Frees the slot of the run `nonce` names; a newer run of the same job keeps its own.
    pub fn retire(&self, job_id: JobId, nonce: u64) {
        let removed = {
            let mut state = self.state();
            match state.running.get(&job_id) {
                Some(job) if job.nonce == nonce => state.running.remove(&job_id),
                _ => None,
            }
        };
        if let Some(job) = removed {
            let _ = job.completion.send(true);
        }
    }

    pub fn running_waiters(&self) -> Vec<RunWaiter> {
        let state = self.state();
        (state.running.iter())
            .map(|(job_id, job)| {
                let completion = job.completion.subscribe();
                (*job_id, job.class, job.claim_token, completion)
            })
            .collect()
    }

    pub fn is_running(&self, job_id: JobId) -> bool {
        self.state().running.contains_key(&job_id)
    }

    pub fn completion_of(&self, job_id: JobId) -> Option<watch::Receiver<bool>> {
        let state = self.state();
        (state.running.get(&job_id)).map(|job| job.completion.subscribe())
    }

    pub fn take_releases(&self) -> Vec<JoinHandle<()>> {
        std::mem::take(&mut self.state().pending_releases)
    }
}
