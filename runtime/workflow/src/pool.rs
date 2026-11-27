//! Warm preflight coordinators: a bounded number answer at once, each is reused until it served
//! its request count, and one that fails, times out or ends is dropped and started anew.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use aruna_core::workflow::WorkflowError;
use aruna_workflow_contract::{CoordinatorMode, PreflightKind, PreflightResult};
use tokio::sync::Semaphore;

use crate::channel::Connection;
use crate::transport::CoordinatorTransport;

/// One warm coordinator and how many requests it served.
struct Worker {
    connection: Connection,
    served: u64,
}

/// The preflight coordinators of a node.
pub struct PreflightPool {
    transport: Arc<dyn CoordinatorTransport>,
    permits: Semaphore,
    idle: Mutex<Vec<Worker>>,
    timeout: Duration,
    recycle_after: u64,
    next_request: AtomicU64,
    started: AtomicU64,
}

impl PreflightPool {
    pub fn new(
        transport: Arc<dyn CoordinatorTransport>,
        workers: usize,
        timeout: Duration,
        recycle_after: u64,
    ) -> Self {
        Self {
            transport,
            permits: Semaphore::new(workers.max(1)),
            idle: Mutex::new(Vec::new()),
            timeout,
            recycle_after: recycle_after.max(1),
            next_request: AtomicU64::new(1),
            started: AtomicU64::new(0),
        }
    }

    /// How many coordinators this pool started, for its tests and metrics.
    pub fn started(&self) -> u64 {
        self.started.load(Ordering::SeqCst)
    }

    /// Answers one request. A warm coordinator that turns out to be gone is replaced once.
    pub async fn request(&self, kind: PreflightKind) -> Result<PreflightResult, WorkflowError> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| WorkflowError::Unavailable)?;
        let warm = self.idle().pop();
        let reused = warm.is_some();
        let worker = match warm {
            Some(worker) => worker,
            None => self.spawn().await?,
        };
        match self.ask(worker, kind.clone()).await {
            Err(WorkflowError::Crashed(reason)) if reused => {
                tracing::debug!(%reason, "a warm preflight coordinator was gone");
                let worker = self.spawn().await?;
                self.ask(worker, kind).await
            }
            answered => answered,
        }
    }

    async fn ask(
        &self,
        mut worker: Worker,
        kind: PreflightKind,
    ) -> Result<PreflightResult, WorkflowError> {
        let request_id = self.next_request.fetch_add(1, Ordering::SeqCst);
        let asked =
            tokio::time::timeout(self.timeout, worker.connection.preflight(request_id, kind));
        let result = match asked.await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(self.timed_out()),
        };
        worker.served += 1;
        if worker.served < self.recycle_after {
            self.idle().push(worker);
        }
        result.map_err(WorkflowError::Refused)
    }

    /// Starts and greets a new coordinator within the request timeout.
    async fn spawn(&self) -> Result<Worker, WorkflowError> {
        let number = self.started.fetch_add(1, Ordering::SeqCst) + 1;
        let start = async {
            let mut connection = Connection::new(self.transport.start().await?);
            let name = format!("preflight-{number}");
            connection
                .greet(CoordinatorMode::Preflight, &name, "")
                .await?;
            Ok(Worker {
                connection,
                served: 0,
            })
        };
        match tokio::time::timeout(self.timeout, start).await {
            Ok(started) => started,
            Err(_) => Err(self.timed_out()),
        }
    }

    fn timed_out(&self) -> WorkflowError {
        WorkflowError::Timeout {
            seconds: self.timeout.as_secs(),
        }
    }

    fn idle(&self) -> std::sync::MutexGuard<'_, Vec<Worker>> {
        self.idle.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
