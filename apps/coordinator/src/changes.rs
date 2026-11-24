//! The JSON form of uwe's run start and run changes, as the run log of the node keeps them, and
//! the replay of recorded run log pages into an in-memory run database.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::PathBuf;

use aruna_workflow_contract::ContractError;
use aruna_workflow_contract::preflight::{RunLogEntry, RunLogPage};
use run_database::{Change, MemoryStore, RemoteFile, RunStart, RunStore, Source, SourceKind};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use workflow_graph::{Edge, Language, Outcome, Task, TaskId};

/// The start record of a run, field by field as `RunStart` holds it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LoggedStart {
    pub id: String,
    pub resumes: Option<String>,
    pub workflow: String,
    pub language: Language,
    pub revision: Option<String>,
    pub arguments: Vec<String>,
    pub parameters: Vec<(String, String)>,
    pub executor: String,
    pub simulated: bool,
    pub store: Option<String>,
    pub program: Option<String>,
    pub launch: Option<String>,
    pub workflow_folder: Option<String>,
    pub repeats: Option<String>,
}

/// One change of a run, owning what `Change` borrows.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum LoggedChange {
    Requested {
        task: Box<Task>,
        edges: Vec<Edge>,
    },
    Started {
        task: TaskId,
        attempt: u32,
    },
    Finished {
        outcome: Box<Outcome>,
        attempt: u32,
        retried: bool,
    },
    Validated {
        task: TaskId,
        attempt: u32,
        accepted: bool,
    },
    Reused {
        task: TaskId,
        attempt: u32,
        source: String,
    },
    Marked {
        task: TaskId,
        kind: String,
        reason: String,
    },
    Inputs {
        schema: Value,
        values: Value,
    },
    Document {
        path: String,
        digest: String,
    },
    CacheNamespace {
        namespace: String,
    },
    Outputs {
        schema: Value,
    },
    Sources {
        sources: Vec<LoggedSource>,
    },
    RemoteFiles {
        files: Vec<LoggedRemote>,
    },
    Delivered {
        target: String,
        location: String,
    },
    Replays {
        run: String,
        task: u64,
    },
    Ended {
        error: Option<String>,
        stopped: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoggedSource {
    pub workflow: bool,
    pub path: String,
    pub digest: String,
    pub size: u64,
    pub executable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoggedRemote {
    pub address: String,
    pub digest: String,
    pub size: u64,
    pub fetched: i64,
    pub version: Option<String>,
}

impl From<&RunStart> for LoggedStart {
    fn from(start: &RunStart) -> LoggedStart {
        LoggedStart {
            id: start.id.clone(),
            resumes: start.resumes.clone(),
            workflow: start.workflow.clone(),
            language: start.language,
            revision: start.revision.clone(),
            arguments: start.arguments.clone(),
            parameters: start.parameters.clone(),
            executor: start.executor.clone(),
            simulated: start.simulated,
            store: start.store.clone(),
            program: start.program.clone(),
            launch: start.launch.clone(),
            workflow_folder: start.workflow_folder.clone(),
            repeats: start.repeats.clone(),
        }
    }
}

impl From<LoggedStart> for RunStart {
    fn from(start: LoggedStart) -> RunStart {
        RunStart {
            id: start.id,
            resumes: start.resumes,
            workflow: start.workflow,
            language: start.language,
            revision: start.revision,
            arguments: start.arguments,
            parameters: start.parameters,
            executor: start.executor,
            simulated: start.simulated,
            store: start.store,
            program: start.program,
            launch: start.launch,
            workflow_folder: start.workflow_folder,
            repeats: start.repeats,
        }
    }
}

impl LoggedChange {
    pub fn of(change: &Change<'_>) -> LoggedChange {
        match *change {
            Change::Requested { task, edges } => LoggedChange::Requested {
                task: Box::new(task.clone()),
                edges: edges.to_vec(),
            },
            Change::Started { task, attempt } => LoggedChange::Started { task, attempt },
            Change::Finished {
                outcome,
                attempt,
                retried,
            } => LoggedChange::Finished {
                outcome: Box::new(outcome.clone()),
                attempt,
                retried,
            },
            Change::Validated {
                task,
                attempt,
                accepted,
            } => LoggedChange::Validated {
                task,
                attempt,
                accepted,
            },
            Change::Reused {
                task,
                attempt,
                source,
            } => LoggedChange::Reused {
                task,
                attempt,
                source: source.to_owned(),
            },
            Change::Marked { task, kind, reason } => LoggedChange::Marked {
                task,
                kind: kind.to_owned(),
                reason: reason.to_owned(),
            },
            Change::Inputs { schema, values } => LoggedChange::Inputs {
                schema: schema.clone(),
                values: values.clone(),
            },
            Change::Document { path, digest } => LoggedChange::Document {
                path: path.to_owned(),
                digest: digest.to_owned(),
            },
            Change::CacheNamespace(namespace) => LoggedChange::CacheNamespace {
                namespace: namespace.to_owned(),
            },
            Change::Outputs { schema } => LoggedChange::Outputs {
                schema: schema.clone(),
            },
            Change::Sources(sources) => LoggedChange::Sources {
                sources: sources.iter().map(LoggedSource::from).collect(),
            },
            Change::RemoteFiles(files) => LoggedChange::RemoteFiles {
                files: files.iter().map(LoggedRemote::from).collect(),
            },
            Change::Delivered { target, location } => LoggedChange::Delivered {
                target: target.to_owned(),
                location: location.to_owned(),
            },
            Change::Replays { run, task } => LoggedChange::Replays {
                run: run.to_owned(),
                task,
            },
            Change::Ended { error, stopped } => LoggedChange::Ended {
                error: error.map(str::to_owned),
                stopped,
            },
        }
    }

    /// Applies the change to a store, borrowing what `Change` needs from this record.
    pub fn apply_to(&self, store: &dyn RunStore, run: &str) -> Result<(), ContractError> {
        let sources: Vec<Source>;
        let files: Vec<RemoteFile>;
        let change = match self {
            LoggedChange::Requested { task, edges } => Change::Requested { task, edges },
            LoggedChange::Started { task, attempt } => Change::Started {
                task: *task,
                attempt: *attempt,
            },
            LoggedChange::Finished {
                outcome,
                attempt,
                retried,
            } => Change::Finished {
                outcome,
                attempt: *attempt,
                retried: *retried,
            },
            LoggedChange::Validated {
                task,
                attempt,
                accepted,
            } => Change::Validated {
                task: *task,
                attempt: *attempt,
                accepted: *accepted,
            },
            LoggedChange::Reused {
                task,
                attempt,
                source,
            } => Change::Reused {
                task: *task,
                attempt: *attempt,
                source,
            },
            LoggedChange::Marked { task, kind, reason } => Change::Marked {
                task: *task,
                kind,
                reason,
            },
            LoggedChange::Inputs { schema, values } => Change::Inputs { schema, values },
            LoggedChange::Document { path, digest } => Change::Document { path, digest },
            LoggedChange::CacheNamespace { namespace } => Change::CacheNamespace(namespace),
            LoggedChange::Outputs { schema } => Change::Outputs { schema },
            LoggedChange::Sources { sources: logged } => {
                sources = logged.iter().map(Source::from).collect();
                Change::Sources(&sources)
            }
            LoggedChange::RemoteFiles { files: logged } => {
                files = logged.iter().map(RemoteFile::from).collect();
                Change::RemoteFiles(&files)
            }
            LoggedChange::Delivered { target, location } => Change::Delivered { target, location },
            LoggedChange::Replays { run, task } => Change::Replays { run, task: *task },
            LoggedChange::Ended { error, stopped } => Change::Ended {
                error: error.as_deref(),
                stopped: *stopped,
            },
        };
        store.apply(run, change).map_err(replay_error)
    }
}

impl From<&Source> for LoggedSource {
    fn from(source: &Source) -> LoggedSource {
        LoggedSource {
            workflow: source.kind == SourceKind::Workflow,
            path: source.path.clone(),
            digest: source.digest.clone(),
            size: source.size,
            executable: source.executable,
        }
    }
}

impl From<&LoggedSource> for Source {
    fn from(source: &LoggedSource) -> Source {
        Source {
            kind: match source.workflow {
                true => SourceKind::Workflow,
                false => SourceKind::Named,
            },
            path: source.path.clone(),
            digest: source.digest.clone(),
            size: source.size,
            executable: source.executable,
        }
    }
}

impl From<&RemoteFile> for LoggedRemote {
    fn from(file: &RemoteFile) -> LoggedRemote {
        LoggedRemote {
            address: file.address.clone(),
            digest: file.digest.clone(),
            size: file.size,
            fetched: file.fetched,
            version: file.version.clone(),
        }
    }
}

impl From<&LoggedRemote> for RemoteFile {
    fn from(file: &LoggedRemote) -> RemoteFile {
        RemoteFile {
            address: file.address.clone(),
            digest: file.digest.clone(),
            size: file.size,
            fetched: file.fetched,
            version: file.version.clone(),
        }
    }
}

/// Where the workflows of replayed runs lie now. Every coordinator attempt writes its workflow
/// to `<root>/workflow` of a folder of its own, and uwe resumes and compares only runs of the
/// same workflow path, so replayed runs move to one root.
pub struct Relocation {
    pub root: PathBuf,
}

impl Relocation {
    /// Moves the folders of a recorded run from its attempt's root to this one.
    fn apply(&self, start: &mut LoggedStart) {
        let folder = (start.workflow_folder.as_deref())
            .and_then(|folder| folder.strip_suffix("/workflow"))
            .or_else(|| {
                start
                    .workflow
                    .find("/workflow/")
                    .map(|end| &start.workflow[..end])
            });
        let Some(old) = folder.map(str::to_owned) else {
            return;
        };
        let new = self.root.to_string_lossy();
        let moved = |path: &mut String| {
            if let Some(rest) = path.strip_prefix(&old) {
                *path = format!("{new}{rest}");
            }
        };
        moved(&mut start.workflow);
        for path in [
            &mut start.workflow_folder,
            &mut start.launch,
            &mut start.store,
        ] {
            if let Some(path) = path.as_mut() {
                moved(path);
            }
        }
    }
}

/// Fills a store with recorded runs, page by page in log order. A change belongs to the run
/// of the last start record before it.
pub fn replay_pages(
    store: &MemoryStore,
    pages: &[RunLogPage],
    relocation: Option<&Relocation>,
) -> Result<(), ContractError> {
    let mut run: Option<String> = None;
    let mut last_seq: Option<u64> = None;
    for entry in pages.iter().flat_map(|page| &page.entries) {
        match entry {
            RunLogEntry::Begin { run_start_json } => {
                let mut start: LoggedStart = serde_json::from_value(run_start_json.clone())
                    .map_err(|error| invalid_log(format!("a run start: {error}")))?;
                if let Some(relocation) = relocation {
                    relocation.apply(&mut start);
                }
                store
                    .begin(&RunStart::from(start.clone()))
                    .map_err(replay_error)?;
                run = Some(start.id);
                last_seq = None;
            }
            RunLogEntry::Change { seq, change_json } => {
                let current = run
                    .as_deref()
                    .ok_or_else(|| invalid_log("a change before the start of its run".into()))?;
                if last_seq.is_some_and(|last| *seq <= last) {
                    return Err(invalid_log(format!("change {seq} is out of order")));
                }
                last_seq = Some(*seq);
                let change: LoggedChange = serde_json::from_value(change_json.clone())
                    .map_err(|error| invalid_log(format!("change {seq}: {error}")))?;
                change.apply_to(store, current)?;
            }
        }
    }
    Ok(())
}

fn invalid_log(message: String) -> ContractError {
    ContractError::invalid(format!("the run log does not replay: {message}"))
}

fn replay_error(error: run_database::DatabaseError) -> ContractError {
    ContractError::invalid(format!("the run log does not replay: {error}"))
}
