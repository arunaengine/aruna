//! Supervises the guest runs of a node: the runtime, which runs execute, why one was cancelled,
//! and the tasks that must end before the node stops.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use ulid::Ulid;

use crate::broker::Broker;
use crate::limits::{Limits, RuntimeConfig};
use crate::runtime::{CompileError, Compiled, Invocation, InvokeError, Runtime};

type Running = HashMap<Ulid, (Ulid, CancellationToken)>;

/// The node's plugin runtime with its executing runs, by run id and install.
pub struct Supervisor {
    runtime: Runtime,
    limits: Limits,
    running: Mutex<Running>,
    reasons: Mutex<HashMap<Ulid, &'static str>>,
    tasks: TaskTracker,
    stopping: CancellationToken,
}

impl Supervisor {
    /// A supervisor whose runtime has `config` and whose runs get the budget `limits`.
    pub fn new(config: RuntimeConfig, limits: Limits) -> Result<Self, CompileError> {
        Ok(Self {
            runtime: Runtime::new(config)?,
            limits,
            running: Mutex::new(HashMap::new()),
            reasons: Mutex::new(HashMap::new()),
            tasks: TaskTracker::new(),
            stopping: CancellationToken::new(),
        })
    }

    fn running(&self) -> MutexGuard<'_, Running> {
        (self.running.lock()).unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn reasons(&self) -> MutexGuard<'_, HashMap<Ulid, &'static str>> {
        (self.reasons.lock()).unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The budget of each run.
    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// Whether the node is stopping, so no run starts any more.
    pub fn stopping(&self) -> bool {
        self.stopping.is_cancelled()
    }

    /// The executing runs with their install.
    pub fn active(&self) -> BTreeMap<Ulid, Ulid> {
        (self.running().iter())
            .map(|(run, (install, _))| (*run, *install))
            .collect()
    }

    /// Records `run` as executing and answers the token that cancels it.
    pub fn admit(&self, run: Ulid, install: Ulid) -> CancellationToken {
        let cancel = CancellationToken::new();
        self.running().insert(run, (install, cancel.clone()));
        cancel
    }

    /// Runs one attempt as a task that [`Supervisor::stop`] and [`Supervisor::settle`] wait for.
    pub fn spawn(&self, attempt: impl Future<Output = ()> + Send + 'static) {
        self.tasks.spawn(attempt);
    }

    /// Cancels each run whose install `reason_of` names a reason for, and keeps that reason.
    pub fn revoke(&self, reason_of: impl Fn(Ulid) -> Option<&'static str>) {
        for (run, (install, cancel)) in self.running().iter() {
            if let Some(reason) = reason_of(*install) {
                self.reasons().insert(*run, reason);
                cancel.cancel();
            }
        }
    }

    /// Why `run` was cancelled, once.
    pub fn take_reason(&self, run: Ulid) -> Option<&'static str> {
        self.reasons().remove(&run)
    }

    /// Forgets `run` after its attempt ended.
    pub fn finish(&self, run: Ulid) {
        self.reasons().remove(&run);
        self.running().remove(&run);
    }

    /// Compiles an artifact, or answers its cached compilation.
    pub async fn compile(&self, bytes: Vec<u8>) -> Result<Arc<Compiled>, CompileError> {
        self.runtime.prepare(bytes).await
    }

    /// Runs one trigger of a compiled artifact.
    pub async fn invoke(
        &self,
        compiled: &Compiled,
        call: Invocation,
        broker: Arc<dyn Broker>,
    ) -> Result<Option<String>, InvokeError> {
        self.runtime.invoke(compiled, call, broker).await
    }

    /// Stops new attempts, cancels every run and waits until each attempt ended.
    pub async fn stop(&self) {
        self.stopping.cancel();
        for (_, cancel) in self.running().values() {
            cancel.cancel();
        }
        self.tasks.close();
        self.tasks.wait().await;
        self.runtime.close();
    }

    /// Waits until the attempts started so far ended.
    pub async fn settle(&self) {
        self.tasks.close();
        self.tasks.wait().await;
        self.tasks.reopen();
    }
}

#[cfg(test)]
#[path = "supervisor_tests.rs"]
mod tests;
