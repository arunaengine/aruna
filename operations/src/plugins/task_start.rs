//! Starts a container task for a plugin run: checks the recipe of the pinned manifest, the
//! grants on every input and output and the task limit, reserves the work row and sends it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id_mint::IdMint;
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_plugin_types::calls::{ErrorKind, HostError};
use aruna_plugin_types::recipe::Recipe;
use ulid::Ulid;

use super::check::{CallError, CheckCall, RunScope};
use super::install::{config_answer, config_call, pick_version, read_manifest};
use super::objects::LocalObject;
use super::rows::{StoreError, encode};
use super::screen::{ScreenError, ScreenTask};
use super::submit::digest;
use super::task_change::{ChangeTask, Changed, TaskChange};
use super::task_send::SendTask;
use super::tasks::{TaskIo, task_spec};
use super::unavailable;
use super::work::{Identity, Reserve, WorkError, WorkKind, WorkRow, WorkState};
use crate::jobs::lifecycle::submit_external::IngressNode;

/// Unsettled tasks one install may have at a time.
pub const TASK_ACTIVE: usize = 2;

/// A task a run starts. Object paths are full permission paths on this node; an input path
/// ending in `/` names every object below it, an output path the prefix the task writes below.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskRequest {
    pub task: String,
    pub parameters: BTreeMap<String, String>,
    /// Container path, object path and pinned version.
    pub inputs: Vec<(String, String, Option<Ulid>)>,
    /// Container path and the bytes the plugin wrote.
    pub files: Vec<(String, Vec<u8>)>,
    /// Container directory and object path prefix.
    pub outputs: Vec<(String, String)>,
    /// The call's ordinal in its run; with the run it names the work.
    pub ordinal: u32,
}

fn invalid(reason: String) -> CallError {
    CallError(HostError::new(ErrorKind::Invalid, reason))
}

fn screen_error(error: ScreenError) -> CallError {
    CallError(match error {
        ScreenError::Unavailable(reason) => unavailable(reason),
        ScreenError::Refused(reason) => HostError::new(ErrorKind::NotGranted, reason),
        other => HostError::new(ErrorKind::Denied, other.to_string()),
    })
}

fn reserve_error(error: WorkError) -> CallError {
    CallError(match error {
        WorkError::Conflict => {
            let message = "this call started a task with other arguments";
            HostError::new(ErrorKind::Conflict, message)
        }
        WorkError::Limit(_) => HostError::new(ErrorKind::Limit, error.to_string()),
        WorkError::Store(error) => unavailable(error.0),
    })
}

#[derive(Debug, PartialEq)]
enum StartPhase {
    Check,
    Config(Box<PluginInstall>),
    Objects(Box<Checked>, VecDeque<(String, String, Option<Ulid>, bool)>),
    Screen(Box<Checked>),
    Reserve(Box<Checked>),
    Send(Ulid),
    Drop(CallError),
}

/// What the start checked so far: the install, its recipe and the task's resolved objects.
#[derive(Debug, PartialEq)]
struct Checked {
    install: PluginInstall,
    recipe: Recipe,
    io: TaskIo,
}

/// Starts or finds the task of `request` and answers its work id.
#[derive(Debug, PartialEq)]
pub struct StartTask {
    scope: RunScope,
    request: TaskRequest,
    node: IngressNode,
    ids: IdMint,
    config: Option<Arc<RealmConfigDocument>>,
    phase: StartPhase,
}

type StartNext = Result<Next<Ulid>, CallError>;

impl StartTask {
    pub fn new(scope: &RunScope, request: TaskRequest, node: IngressNode) -> Self {
        Self {
            scope: scope.clone(),
            request,
            node,
            ids: IdMint::default(),
            config: None,
            phase: StartPhase::Check,
        }
    }

    fn configured(&mut self, install: PluginInstall, config: RealmConfigDocument) -> StartNext {
        let plugin = pick_version(&config, &install.plugin, Some(&install.digest));
        let manifest = plugin.and_then(|plugin| read_manifest(&plugin));
        let manifest = manifest.map_err(|error| CallError(unavailable(error.to_string())))?;
        let Some(recipe) = manifest.recipes.get(&self.request.task) else {
            return Err(invalid(format!(
                "the plugin declares no task `{}`",
                self.request.task
            )));
        };
        let io = TaskIo {
            files: self.request.files.clone(),
            ..TaskIo::default()
        };
        let request = &self.request;
        let inputs = (request.inputs.iter())
            .map(|(path, object, version)| (path.clone(), object.clone(), *version, true));
        let outputs = (request.outputs.iter())
            .map(|(path, prefix)| (path.clone(), prefix.clone(), None, false));
        let checked = Checked {
            install,
            recipe: recipe.clone(),
            io,
        };
        self.config = Some(Arc::new(config));
        self.next_object(Box::new(checked), inputs.chain(outputs).collect())
    }

    /// Resolves the next input or output path to its bucket on this node.
    fn next_object(
        &mut self,
        checked: Box<Checked>,
        mut objects: VecDeque<(String, String, Option<Ulid>, bool)>,
    ) -> StartNext {
        let Some((_, object, ..)) = objects.front() else {
            return self.screen(checked);
        };
        let place = (self.scope.realm_id, self.scope.local);
        let locate = LocalObject::new(place, &checked.install, object);
        self.phase = StartPhase::Objects(checked, objects);
        Ok(Next::Call(IoCall::child(locate)))
    }

    fn screen(&mut self, checked: Box<Checked>) -> StartNext {
        let values = (&checked.recipe, &self.request.parameters);
        let spec =
            task_spec(&checked.install, Ulid::nil(), values, &checked.io).map_err(invalid)?;
        let config = self.config.clone().ok_or(IoError::Unfinished)?;
        let task = (&spec, checked.install.account, self.scope.local);
        self.phase = StartPhase::Screen(checked);
        Ok(Next::Call(IoCall::child(ScreenTask::new(config, task))))
    }

    fn reserve_row(&mut self, checked: Box<Checked>) -> StartNext {
        let request = &self.request;
        let identity = Identity::Call {
            run_id: self.scope.run.run_id,
            ordinal: request.ordinal,
        };
        let stable = (
            &request.task,
            &request.parameters,
            &request.inputs,
            &request.files,
            &request.outputs,
        );
        let stable = encode(&stable).map_err(|error| CallError(unavailable(error.0)))?;
        let now_ms = self.node.now_ms;
        let ids = (self.scope.run.run_id, self.ids.mint(now_ms));
        let mut proposed = WorkRow::reserved(&checked.install, WorkKind::Recipe, ids, now_ms);
        proposed.digest = digest(&[b"task", &stable]);
        proposed.binding.recipe = Some(request.task.clone());
        let reserve = super::work::reserve((&checked.install, &identity), proposed, None);
        let reserve = reserve.map_err(reserve_error)?;
        self.phase = StartPhase::Reserve(checked);
        Ok(Next::Call(IoCall::child(reserve.limited(TASK_ACTIVE))))
    }

    fn send(&mut self, checked: Box<Checked>, row: WorkRow) -> StartNext {
        if row.state != WorkState::Reserved {
            return Ok(Next::Done(row.work_id));
        }
        let values = (&checked.recipe, &self.request.parameters);
        let spec =
            task_spec(&checked.install, row.work_id, values, &checked.io).map_err(invalid)?;
        let send = SendTask::new(&checked.install, row.work_id, Some(spec), self.node.clone());
        self.phase = StartPhase::Send(row.work_id);
        Ok(Next::Call(IoCall::child(send)))
    }
}

impl Operation for StartTask {
    type Output = Ulid;
    type Error = CallError;

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> StartNext {
        Ok(Next::Call(IoCall::child(CheckCall::new(&self.scope, None))))
    }

    fn next(&mut self, result: IoResult) -> StartNext {
        match std::mem::replace(&mut self.phase, StartPhase::Check) {
            StartPhase::Check => {
                let install = result.child::<PluginInstall, CallError>()??;
                if install.paused {
                    let paused = HostError::new(ErrorKind::Denied, "the install is paused");
                    return Err(CallError(paused));
                }
                self.phase = StartPhase::Config(Box::new(install));
                Ok(Next::Call(config_call(self.scope.realm_id)))
            }
            StartPhase::Config(install) => {
                let config = config_answer(result);
                let config = config.map_err(|error| CallError(unavailable(error.to_string())))?;
                self.configured(*install, config)
            }
            StartPhase::Objects(mut checked, mut objects) => {
                let (bucket, key) = result.child::<(String, String), CallError>()??;
                let (path, _, version, input) = objects.pop_front().ok_or(IoError::Unfinished)?;
                match input {
                    true => checked.io.objects.push((path, bucket, key, version)),
                    false => checked.io.outputs.push((path, bucket, key)),
                }
                self.next_object(checked, objects)
            }
            StartPhase::Screen(checked) => {
                let screened = result.child::<PluginInstall, ScreenError>()?;
                screened.map_err(screen_error)?;
                self.reserve_row(checked)
            }
            StartPhase::Reserve(checked) => {
                let (row, _) = result
                    .child::<(WorkRow, Reserve), WorkError>()?
                    .map_err(reserve_error)?;
                self.send(checked, row)
            }
            StartPhase::Send(work_id) => match result.child::<(), CallError>()? {
                Ok(()) => Ok(Next::Done(work_id)),
                Err(error) => {
                    // Nothing was sent when preparation refused, so the task holds no slot.
                    let ids = (self.scope.run.install_id, work_id);
                    let drop = ChangeTask::new(ids, TaskChange::DropUnsent, self.node.now_ms);
                    self.phase = StartPhase::Drop(error);
                    Ok(Next::Call(IoCall::child(drop)))
                }
            },
            StartPhase::Drop(error) => {
                if let Err(dropped) = result.child::<Changed, StoreError>()? {
                    tracing::warn!(error = %dropped.0, "A task that was never sent kept its row");
                }
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[path = "task_start_tests.rs"]
mod tests;
