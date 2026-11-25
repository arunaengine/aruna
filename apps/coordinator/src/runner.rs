//! Runs one workflow with uwe: tasks go to the node as calls, data through the host storage,
//! every run change to the node's run log, and uwe's events out as live events.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use aruna_workflow_contract::host::SecretPlaceholder;
use aruna_workflow_contract::message::{
    LogStream, ProgressCounts, RunFinishedPayload, RunStartPayload, TaskChange, TaskState,
};
use aruna_workflow_contract::{ContractError, LiveWorkflowEvent};
use executor::MemoryJournal;
use serde_json::json;
use uwe::application::Policy;
use uwe::run::{
    Event, HostExecutor, HostStorage, Listener, NetworkRule, PrintSink, RunRequest, SecretValues,
};

use crate::backend::CoordinatorBackend;
use crate::changes::Relocation;
use crate::composed;
use crate::protocol::RpcClient;
use crate::run_store::CoordinatorRunStore;
use crate::source::Workspace;
use crate::storage::CoordinatorStorage;

/// Runs the workflow of `payload` as run `run_id` and says how it ended. A run that cannot
/// start ends as failed with the reason.
pub async fn run_workflow(
    rpc: RpcClient,
    run_id: String,
    payload: RunStartPayload,
    secrets_file: Option<&Path>,
) -> RunFinishedPayload {
    let cancel = rpc.cancel_token();
    let ended = prepare_and_run(&rpc, run_id.clone(), payload, secrets_file).await;
    let stopped = cancel.is_cancelled();
    if stopped {
        let request = aruna_workflow_contract::HostRpcRequest::StopOrphans { run_id };
        if let Err(error) = rpc.call(request).await {
            tracing::warn!(%error, "the host did not stop the tasks of the run");
        }
    }
    match ended {
        Ok(ran) => RunFinishedPayload {
            success: ran.error.is_none() && !stopped,
            error: ran.error,
            stopped,
            final_summary_json: ran.summary,
        },
        Err(error) => RunFinishedPayload {
            success: false,
            error: Some(error.to_string()),
            stopped,
            final_summary_json: json!({}),
        },
    }
}

/// How a run that started ended: the error of the run, if any, and its summary.
struct Ran {
    error: Option<String>,
    summary: serde_json::Value,
}

async fn prepare_and_run(
    rpc: &RpcClient,
    run_id: String,
    payload: RunStartPayload,
    secrets_file: Option<&Path>,
) -> Result<Ran, ContractError> {
    let workspace = Workspace::create(&payload.source, &payload.entrypoint)?;
    let mut step_settings = workspace.step_settings.clone();
    step_settings.extend(payload.step_settings);
    let placeholders = secret_placeholders(&payload.secret_names)?;
    let store = Arc::new(CoordinatorRunStore::new(rpc.clone())?);
    let relocation = Relocation {
        root: workspace.path().to_owned(),
    };
    store.replay_log_pages(&payload.prior_log_pages, &relocation)?;
    let steps = composed::step_matches(&workspace.entry, &step_settings)
        .map_err(ContractError::workflow)?;
    let allowed_steps: Vec<String> = (steps.iter())
        .filter(|step| step.settings.execution.network == Some(true))
        .flat_map(|step| step.names.iter().cloned())
        .collect();
    let backend = CoordinatorBackend::new(
        rpc.clone(),
        run_id.clone(),
        workspace.scratch(),
        steps,
        placeholders.clone(),
    );
    let mut settings = workspace.settings(payload.language, &SecretValues::default())?;
    if !placeholders.is_empty() {
        let file = secrets_file.map_or_else(|| workspace.path().join("secrets.json"), Into::into);
        write_placeholders(&file, &placeholders)?;
        if let Some(isolation) = settings.isolation.as_mut() {
            isolation.secret_store = true;
        }
    }
    settings.run_id = Some(run_id);
    settings.host_executor = Some(HostExecutor::new(Arc::new(backend)));
    settings.host_storage = vec![HostStorage::new(Arc::new(CoordinatorStorage::new(
        rpc.clone(),
    )))];
    settings.run_store = Some(store);
    settings.journal_store = Some(Arc::new(MemoryJournal::new()));
    settings.pinned_images = payload.pinned_images.into_iter().collect();
    settings.cache_namespace = payload.cache_namespace;
    settings.resume = payload.resume_lineage.map(uwe::Resume::Run);
    settings.max_pending = payload.max_pending.map(|pending| pending as usize);
    if payload.network_isolated {
        settings.network = Some(NetworkRule {
            none_by_default: true,
            allowed_steps,
        });
    }
    if !payload.inputs.is_null() {
        settings.inputs = Some(payload.inputs);
    }
    settings.params = payload.params;
    settings.listener = Some(listener(rpc.clone()));
    let printer = rpc.clone();
    settings.print_sink = Some(PrintSink::new(move |text| {
        printer.emit(LiveWorkflowEvent::Printed {
            text: text.to_owned(),
        });
    }));
    settings.cancel = rpc.cancel_token();
    let request = RunRequest {
        settings,
        policy: Policy::Relaxed,
    };
    let run = request
        .run()
        .await
        .map_err(|failure| ContractError::workflow(failure.to_string()))?;
    let nodes = &run.execution.graph.nodes;
    let failed = (nodes.iter())
        .filter(|node| node.exit_code.is_some_and(|code| code != 0))
        .count();
    Ok(Ran {
        error: run.execution.error.as_ref().map(ToString::to_string),
        summary: json!({
            "run_id": run.id,
            "outputs": run.outputs,
            "tasks": nodes.len(),
            "failed_tasks": failed,
        }),
    })
}

/// Turns uwe's events into live events, with the task counts after each task change.
fn listener(rpc: RpcClient) -> Listener {
    let states: Mutex<HashMap<u64, TaskState>> = Mutex::new(HashMap::new());
    Arc::new(move |event: Event| match event {
        Event::Started { run, time } => rpc.emit(LiveWorkflowEvent::RunStarted {
            run_id: run,
            time_ms: time,
        }),
        Event::Ended { run, time, error } => rpc.emit(LiveWorkflowEvent::RunEnded {
            run_id: run,
            time_ms: time,
            error,
        }),
        Event::Task(task) => {
            let Some(state) = task_state(task.state) else {
                tracing::warn!(
                    task = task.task,
                    "a task state this coordinator does not know"
                );
                return;
            };
            rpc.emit(LiveWorkflowEvent::TaskChanged(TaskChange {
                task_id: task.task,
                name: task.name,
                step: task.step,
                state,
                attempt: task.attempt,
                time_ms: task.time,
                started_ms: task.started,
                ended_ms: task.ended,
                cached: task.cached,
                exit_code: task.exit_code,
                error: task.error,
            }));
            let counts = {
                let mut states = states.lock().unwrap_or_else(PoisonError::into_inner);
                states.insert(task.task, state);
                progress(states.values())
            };
            rpc.emit(LiveWorkflowEvent::Progress(counts));
        }
        Event::Log {
            task,
            attempt,
            stream,
            chunk,
            time,
            ..
        } => rpc.emit(LiveWorkflowEvent::TaskLog {
            task_id: task,
            attempt,
            stream: match stream {
                uwe::run::LogStream::Stdout => LogStream::Stdout,
                uwe::run::LogStream::Stderr => LogStream::Stderr,
            },
            chunk,
            time_ms: time,
        }),
        Event::Usage {
            task,
            attempt,
            cpu_percent,
            memory_bytes,
            time,
            ..
        } => rpc.emit(LiveWorkflowEvent::TaskUsage {
            task_id: task,
            attempt,
            cpu_percent,
            memory_bytes,
            time_ms: time,
        }),
        _ => tracing::debug!("an event this coordinator does not forward"),
    })
}

fn task_state(state: uwe::run::TaskState) -> Option<TaskState> {
    use uwe::run::TaskState as Uwe;
    Some(match state {
        Uwe::Requested => TaskState::Requested,
        Uwe::Started => TaskState::Started,
        Uwe::Retried => TaskState::Retried,
        Uwe::Finished => TaskState::Finished,
        Uwe::Reused => TaskState::Reused,
        Uwe::Failed => TaskState::Failed,
        Uwe::Stopped => TaskState::Stopped,
        _ => return None,
    })
}

fn progress<'a>(states: impl Iterator<Item = &'a TaskState>) -> ProgressCounts {
    let mut counts = ProgressCounts::default();
    for state in states {
        counts.requested += 1;
        match state {
            TaskState::Started | TaskState::Retried => counts.running += 1,
            TaskState::Finished => counts.finished += 1,
            TaskState::Reused => {
                counts.finished += 1;
                counts.cached += 1;
            }
            TaskState::Failed => counts.failed += 1,
            TaskState::Stopped => counts.stopped += 1,
            TaskState::Requested => {}
        }
    }
    counts
}

/// A placeholder per secret name that no workflow can guess: the node puts the real value
/// into the task in its place.
pub fn secret_placeholders(names: &[String]) -> Result<Vec<SecretPlaceholder>, ContractError> {
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let mut nonce = [0u8; 12];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut random| random.read_exact(&mut nonce))
        .map_err(|error| ContractError::internal(format!("no random bytes: {error}")))?;
    let nonce: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
    let mut placeholders = Vec::new();
    for name in names {
        let valid = name.chars().enumerate().all(|(index, letter)| {
            letter == '_' || letter.is_ascii_alphabetic() || (index > 0 && letter.is_ascii_digit())
        });
        if name.is_empty() || !valid {
            return Err(ContractError::invalid(format!(
                "{name:?} is no secret name"
            )));
        }
        placeholders.push(SecretPlaceholder {
            name: name.clone(),
            placeholder: format!("__ARUNA_SECRET_{nonce}_{name}__"),
            env_name: None,
        });
    }
    Ok(placeholders)
}

/// Writes the Nextflow secret store of the run with only placeholders, readable by its owner.
pub fn write_placeholders(
    path: &Path,
    placeholders: &[SecretPlaceholder],
) -> Result<(), ContractError> {
    let rows: Vec<_> = (placeholders.iter())
        .map(|secret| json!({"name": secret.name, "value": secret.placeholder}))
        .collect();
    let failed = |error: std::io::Error| {
        ContractError::internal(format!("cannot write the secret store: {error}"))
    };
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(failed)?;
    // A file that existed keeps its mode on open, so the mode is set again.
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(failed)?;
    file.write_all(json!(rows).to_string().as_bytes())
        .map_err(failed)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_placeholder_store() {
        let names = ["TOKEN".to_owned(), "API_KEY2".to_owned()];
        let placeholders = secret_placeholders(&names).expect("placeholders");
        assert!(placeholders[0].placeholder.starts_with("__ARUNA_SECRET_"));
        assert!(placeholders[1].placeholder.ends_with("_API_KEY2__"));
        let folder = tempfile::tempdir().expect("a folder");
        let path = folder.path().join("store.json");
        std::fs::write(&path, "old").expect("writes");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("mode");
        write_placeholders(&path, &placeholders).expect("writes");
        let mode = std::fs::metadata(&path)
            .expect("exists")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let rows: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).expect("reads")).expect("JSON");
        assert_eq!(rows[0]["name"], "TOKEN");
        assert_eq!(rows[0]["value"], placeholders[0].placeholder.as_str());
        for bad in ["", "1A", "A-B", "A B"] {
            assert!(secret_placeholders(&[bad.to_owned()]).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn counts_task_states() {
        let states = [
            TaskState::Requested,
            TaskState::Started,
            TaskState::Finished,
            TaskState::Reused,
            TaskState::Failed,
            TaskState::Stopped,
        ];
        let counts = progress(states.iter());
        let expected = ProgressCounts {
            requested: 6,
            running: 1,
            finished: 2,
            cached: 1,
            failed: 1,
            stopped: 1,
        };
        assert_eq!(counts, expected);
    }
}
