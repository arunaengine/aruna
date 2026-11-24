//! The run store of a coordinator run: an in-memory run database that answers uwe's reads, and
//! a run log on the host that gets the run start and every change first, in order.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use aruna_workflow_contract::message::LiveWorkflowEvent;
use aruna_workflow_contract::preflight::RunLogPage;
use aruna_workflow_contract::{ContractError, HostRpcRequest, HostRpcResult};
use run_database::{
    Attempt, Change, Comparison, DatabaseError, Delivered, EdgeRow, Event, FileRow, ImagePin, Log,
    MarkerRow, MemoryStore, Owner, RemoteFile, Reusable, Run, RunInputs, RunReader, RunStart,
    RunStore, Source, TaskDetail, TaskRow, TimeSpent, Why,
};
use workflow_graph::ExecutionGraph;

use crate::changes::{LoggedChange, LoggedStart, Relocation, replay_pages};
use crate::protocol::RpcClient;

/// Forwards each write to the host before it applies it to its own database, so that the
/// host's run log is never behind what uwe reads back.
pub struct CoordinatorRunStore {
    inner: MemoryStore,
    rpc: RpcClient,
    /// The number of the last change sent; held while a change is on its way, to keep order.
    sequence: Mutex<u64>,
}

impl CoordinatorRunStore {
    pub fn new(rpc: RpcClient) -> Result<CoordinatorRunStore, ContractError> {
        Ok(CoordinatorRunStore {
            inner: memory_store()?,
            rpc,
            sequence: Mutex::new(0),
        })
    }

    /// Loads the recorded runs of a lineage before the run that resumes them starts, with
    /// their workflow where this attempt keeps it. They are in the host's log already, so
    /// nothing is forwarded.
    pub fn replay_log_pages(
        &self,
        pages: &[RunLogPage],
        relocation: &Relocation,
    ) -> Result<(), ContractError> {
        replay_pages(&self.inner, pages, Some(relocation))
    }

    fn forward(&self, request: HostRpcRequest) -> Result<(), DatabaseError> {
        let expected = match &request {
            HostRpcRequest::RunLogAppend { seq, .. } => Some(*seq),
            _ => None,
        };
        match self.rpc.call_blocking(request) {
            Ok(HostRpcResult::RunLogAck { seq }) if expected.is_none_or(|sent| sent == seq) => {
                Ok(())
            }
            Ok(other) => Err(host_error(format!("unexpected run log answer {other:?}"))),
            Err(error) => Err(host_error(error.to_string())),
        }
    }
}

/// A new in-memory run database.
pub fn memory_store() -> Result<MemoryStore, ContractError> {
    MemoryStore::new().map_err(|error| ContractError::internal(format!("run database: {error}")))
}

fn host_error(message: String) -> DatabaseError {
    DatabaseError::Io(std::io::Error::other(format!(
        "the host did not keep the run log: {message}"
    )))
}

impl RunStore for CoordinatorRunStore {
    fn begin(&self, run: &RunStart) -> Result<(), DatabaseError> {
        let mut sequence = self.sequence.lock().unwrap_or_else(PoisonError::into_inner);
        let run_start_json = serde_json::to_value(LoggedStart::from(run))?;
        self.forward(HostRpcRequest::RunLogBegin { run_start_json })?;
        *sequence = 0;
        self.inner.begin(run)
    }

    fn apply(&self, run: &str, change: Change<'_>) -> Result<(), DatabaseError> {
        let mut sequence = self.sequence.lock().unwrap_or_else(PoisonError::into_inner);
        let logged = LoggedChange::of(&change);
        let change_json = serde_json::to_value(&logged)?;
        let seq = *sequence + 1;
        let is_event = change.event();
        self.forward(HostRpcRequest::RunLogAppend {
            seq,
            change_json,
            is_event,
        })?;
        *sequence = seq;
        if let LoggedChange::Marked { task, kind, reason } = logged {
            self.rpc.emit(LiveWorkflowEvent::TaskMarked {
                task_id: task.0,
                kind,
                reason,
            });
        }
        self.inner.apply(run, change)
    }

    fn reconcile(&self, owner: &Owner, reason: &str) -> Result<bool, DatabaseError> {
        self.inner.reconcile(owner, reason)
    }
}

/// Implements `RunReader` by reading through `$inner`, an expression of `$this`, with the given
/// body for `log`.
macro_rules! reader {
    ($type:ty, $this:ident => $inner:expr, $log:expr) => {
        impl RunReader for $type {
            fn runs(&self, limit: u32) -> Result<Vec<Run>, DatabaseError> {
                let $this = self;
                $inner.runs(limit)
            }
            fn run(&self, id: &str) -> Result<Option<Run>, DatabaseError> {
                let $this = self;
                $inner.run(id)
            }
            fn last_run(
                &self,
                workflow: &str,
                real: bool,
                namespace: Option<&str>,
            ) -> Result<Option<Run>, DatabaseError> {
                let $this = self;
                $inner.last_run(workflow, real, namespace)
            }
            fn lineage(&self, lineage: &str) -> Result<Vec<String>, DatabaseError> {
                let $this = self;
                $inner.lineage(lineage)
            }
            fn cache_namespace(&self, run: &str) -> Result<Option<String>, DatabaseError> {
                let $this = self;
                $inner.cache_namespace(run)
            }
            fn reusable(
                &self,
                runs: &[String],
            ) -> Result<HashMap<String, Reusable>, DatabaseError> {
                let $this = self;
                $inner.reusable(runs)
            }
            fn run_inputs(&self, run: &str) -> Result<Option<RunInputs>, DatabaseError> {
                let $this = self;
                $inner.run_inputs(run)
            }
            fn owner(&self, run: &str) -> Result<Option<Owner>, DatabaseError> {
                let $this = self;
                $inner.owner(run)
            }
            fn tasks(&self, run: &str) -> Result<Vec<TaskRow>, DatabaseError> {
                let $this = self;
                $inner.tasks(run)
            }
            fn task_row(&self, run: &str, task: u64) -> Result<Option<TaskRow>, DatabaseError> {
                let $this = self;
                $inner.task_row(run, task)
            }
            fn task(&self, run: &str, task: u64) -> Result<Option<TaskDetail>, DatabaseError> {
                let $this = self;
                $inner.task(run, task)
            }
            fn task_attempts(&self, run: &str, task: u64) -> Result<Vec<Attempt>, DatabaseError> {
                let $this = self;
                $inner.task_attempts(run, task)
            }
            fn attempts(&self, run: &str) -> Result<Vec<(u64, Attempt)>, DatabaseError> {
                let $this = self;
                $inner.attempts(run)
            }
            fn files(&self, run: &str, task: Option<u64>) -> Result<Vec<FileRow>, DatabaseError> {
                let $this = self;
                $inner.files(run, task)
            }
            fn edges(&self, run: &str) -> Result<Vec<EdgeRow>, DatabaseError> {
                let $this = self;
                $inner.edges(run)
            }
            fn markers(&self, run: &str) -> Result<Vec<MarkerRow>, DatabaseError> {
                let $this = self;
                $inner.markers(run)
            }
            fn log(
                &self,
                run: &str,
                task: u64,
                attempt: u32,
                stream: &str,
            ) -> Result<Option<Log>, DatabaseError> {
                let $this = self;
                let read = $inner.log(run, task, attempt, stream);
                ($log)(read)
            }
            fn events(
                &self,
                run: &str,
                after: i64,
                limit: u32,
            ) -> Result<Vec<Event>, DatabaseError> {
                let $this = self;
                $inner.events(run, after, limit)
            }
            fn last_event(&self, run: &str) -> Result<i64, DatabaseError> {
                let $this = self;
                $inner.last_event(run)
            }
            fn deliveries(&self, run: &str) -> Result<Vec<Delivered>, DatabaseError> {
                let $this = self;
                $inner.deliveries(run)
            }
            fn sources(&self, run: &str) -> Result<Vec<Source>, DatabaseError> {
                let $this = self;
                $inner.sources(run)
            }
            fn remote_files(&self, run: &str) -> Result<Vec<RemoteFile>, DatabaseError> {
                let $this = self;
                $inner.remote_files(run)
            }
            fn images(&self, run: &str) -> Result<Vec<ImagePin>, DatabaseError> {
                let $this = self;
                $inner.images(run)
            }
            fn pins(&self) -> Result<HashMap<String, String>, DatabaseError> {
                let $this = self;
                $inner.pins()
            }
            fn graph(&self, run: &str) -> Result<Option<ExecutionGraph>, DatabaseError> {
                let $this = self;
                $inner.graph(run)
            }
            fn compare(
                &self,
                first: &str,
                second: &str,
            ) -> Result<Option<Comparison>, DatabaseError> {
                let $this = self;
                $inner.compare(first, second)
            }
            fn time_spent(&self, run: &str) -> Result<Option<TimeSpent>, DatabaseError> {
                let $this = self;
                $inner.time_spent(run)
            }
            fn why(&self, run: &str, task: u64) -> Result<Option<Why>, DatabaseError> {
                let $this = self;
                $inner.why(run, task)
            }
            fn replays_of(&self, run: &str, task: u64) -> Result<Vec<Run>, DatabaseError> {
                let $this = self;
                $inner.replays_of(run, task)
            }
            fn producers(&self, location: &str) -> Result<Vec<(String, FileRow)>, DatabaseError> {
                let $this = self;
                $inner.producers(location)
            }
            fn chains(&self, workflow: &str) -> Result<HashMap<String, u64>, DatabaseError> {
                let $this = self;
                $inner.chains(workflow)
            }
        }
    };
}

type LogRead = Result<Option<Log>, DatabaseError>;

reader!(CoordinatorRunStore, this => this.inner, |read: LogRead| read);

/// Replayed runs for reports. Their deferred logs lay in the store of the attempt that ran
/// them, which is gone, so a log shows its size and digest; the node keeps the text.
pub struct ReportReader {
    pub store: MemoryStore,
}

reader!(ReportReader, this => this.store, |read: LogRead| {
    read.map(|log| {
        log.map(|log| Log {
            location: None,
            ..log
        })
    })
});
