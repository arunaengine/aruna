//! Checks a plugin task's job against its install from replicated state, so every family
//! holder decides the same: the install and its account, and the grants on each input and output.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::structs::execution::job::{
    ExecutionSpec, InputSource, JobFamilyId, OutputDestination, PLUGIN_INSTALL_TAG, PLUGIN_WORK_TAG,
};
use aruna_core::structs::identity::auth::Permission as Access;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_plugin_types::calls::{ErrorKind, HostError};
use aruna_plugin_types::permission::Permission;
use ulid::Ulid;

use super::account::{owned, read_user_call, read_user_result};
use super::check::{Authorize, CallError, matching};
use super::grants::InstallScope;
use super::rows::{decode, read_call, values};
use super::task_row::{TaskRow, task_place};
use super::work::{Found, WorkKind, WorkLookup, WorkRow, WorkState};

/// The install and work a task job names in its reserved tags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tagged {
    pub install_id: Ulid,
    pub work_id: Ulid,
}

/// Why a task job does not pass; `Unavailable` decides nothing and is asked again.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ScreenError {
    #[error("the plugin task tags are malformed")]
    Malformed,
    /// No output names a path its `jobs.run` grant is checked on.
    #[error("a plugin task needs at least one output")]
    NoOutput,
    #[error("the plugin install is {0}")]
    Install(&'static str),
    #[error("the plugin account is not the install's active account")]
    Account,
    #[error("{0}")]
    Refused(String),
    #[error("the plugin install cannot be checked now: {0}")]
    Unavailable(String),
    /// The receiver's work is still reserved: a launch waits until the claim is adopted.
    #[error("the plugin task's submission is not adopted yet")]
    Pending,
}

impl From<IoError> for ScreenError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

/// The install and work of a tagged spec; `None` for an ordinary job.
pub fn tagged(spec: &ExecutionSpec) -> Option<Result<Tagged, ScreenError>> {
    let install = spec.tags.get(PLUGIN_INSTALL_TAG);
    let work = spec.tags.get(PLUGIN_WORK_TAG);
    if install.is_none() && work.is_none() {
        return None;
    }
    let parse = |value: Option<&String>| Ulid::from_string(value?).ok();
    Some(match (parse(install), parse(work)) {
        _ if spec.file_outputs.is_empty() => Err(ScreenError::NoOutput),
        (Some(install_id), Some(work_id)) => Ok(Tagged {
            install_id,
            work_id,
        }),
        _ => Err(ScreenError::Malformed),
    })
}

/// The install a tagged job names, and the node that alone may run it.
pub fn named_install(
    config: &RealmConfigDocument,
    tagged: Tagged,
) -> Result<&PluginInstall, ScreenError> {
    let install = (config.installs.iter())
        .find(|install| install.install_id == tagged.install_id)
        .ok_or(ScreenError::Install("removed or conflicting"))?;
    match (install.state, install.paused) {
        (InstallState::Active, false) => Ok(install),
        (_, true) => Err(ScreenError::Install("paused")),
        (state, _) => Err(ScreenError::Install(state.as_str())),
    }
}

/// What a task job reads and writes, as permission paths: read on every object input, and the
/// task permission on every output destination. Inputs without a node are read at `ingress`.
pub fn screen_targets(
    spec: &ExecutionSpec,
    realm_id: RealmId,
    ingress: NodeId,
) -> Vec<(Permission, String)> {
    let inputs = (spec.inputs.iter()).filter_map(|input| match &input.source {
        InputSource::S3 { bucket, key, .. } => {
            let node = input.source_node_id.unwrap_or(ingress);
            let path = object_permission_path(realm_id, spec.group_id, node, bucket, key);
            Some((Permission::ObjectsRead, path))
        }
        InputSource::Inline { .. } => None,
    });
    let outputs = (spec.file_outputs.iter()).map(|output| {
        let OutputDestination::S3 { bucket, key } = &output.destination;
        let node = output.destination_node_id.unwrap_or(ingress);
        let path = object_permission_path(realm_id, spec.group_id, node, bucket, key);
        (Permission::JobsRun, path)
    });
    inputs.chain(outputs).collect()
}

/// The work row's part of [`AdmitTask`]: the family the receiver adopted against the family of
/// the launch.
pub fn work_admits(
    install: &PluginInstall,
    row: Option<&WorkRow>,
    (adopted, family): (Option<JobFamilyId>, JobFamilyId),
) -> Result<(), ScreenError> {
    let refused = |why: &str| Err(ScreenError::Refused(why.to_string()));
    let Some(row) = row.filter(|row| row.account == install.account) else {
        return refused("the plugin task's work is not on this node");
    };
    if row.kind != WorkKind::Recipe {
        return refused("the work is not a plugin task");
    }
    // An install updated to another version never runs work it approved before.
    if row.binding.digest != install.digest {
        return refused("the install changed its version");
    }
    match row.state {
        // Holders that admitted the same request under other aliases are one family.
        WorkState::Dispatched if adopted == Some(family) => Ok(()),
        WorkState::Dispatched => refused("the work runs another family"),
        WorkState::Reserved => Err(ScreenError::Pending),
        WorkState::Settled => refused("the plugin task's work has ended"),
    }
}

fn refused(error: HostError) -> ScreenError {
    match error.kind {
        ErrorKind::Unavailable => ScreenError::Unavailable(error.message),
        _ => ScreenError::Refused(error.message),
    }
}

/// The replicated part of the task check, which any family holder can run: the install named
/// by the tags is active, `created_by` is its active account, and the grants cover every input
/// and output now. Answers the install.
#[derive(Debug, PartialEq)]
pub struct ScreenTask {
    config: Arc<RealmConfigDocument>,
    spec: ExecutionSpec,
    created_by: UserId,
    ingress: NodeId,
    install: Option<PluginInstall>,
    /// The account was read and checked; grant checks follow.
    checked: bool,
    targets: VecDeque<(Permission, String)>,
}

impl ScreenTask {
    pub fn new(
        config: Arc<RealmConfigDocument>,
        (spec, created_by, ingress): (&ExecutionSpec, UserId, NodeId),
    ) -> Self {
        Self {
            config,
            spec: spec.clone(),
            created_by,
            ingress,
            install: None,
            checked: false,
            targets: VecDeque::new(),
        }
    }

    fn install(&self) -> Result<&PluginInstall, ScreenError> {
        Ok(self.install.as_ref().ok_or(IoError::Unfinished)?)
    }

    /// The grant check of the next target, or the install once every target passed.
    fn advance(&mut self) -> Result<Next<PluginInstall>, ScreenError> {
        let target = self.targets.pop_front();
        let install = self.install()?;
        let Some((permission, path)) = target else {
            return Ok(Next::Done(install.clone()));
        };
        let access = match permission {
            Permission::ObjectsRead => Access::READ,
            _ => Access::WRITE,
        };
        let patterns = matching(install, permission, &path).map_err(refused)?;
        let check = Authorize::access(install, (permission, access), &path, &patterns);
        Ok(Next::Call(IoCall::child(check)))
    }
}

impl Operation for ScreenTask {
    type Output = PluginInstall;
    type Error = ScreenError;

    fn first(&mut self) -> Result<Next<PluginInstall>, ScreenError> {
        let tagged = tagged(&self.spec).ok_or(ScreenError::Malformed)??;
        let install = named_install(&self.config, tagged)?;
        if install.account != self.created_by || install.group_id != self.spec.group_id {
            return Err(ScreenError::Account);
        }
        let read = read_user_call(install.account);
        self.install = Some(install.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginInstall>, ScreenError> {
        if self.checked {
            let checked = result.child::<(), CallError>()?;
            checked.map_err(|error| refused(error.0))?;
            return self.advance();
        }
        let user = read_user_result(result).map_err(|error| ScreenError::Unavailable(error.0))?;
        let install = self.install()?;
        let scope = InstallScope {
            realm_id: self.config.realm_id,
            group_id: install.group_id,
            install_id: install.install_id,
        };
        if !user.is_some_and(|user| owned(&user, &scope) && !user.is_deactivated()) {
            return Err(ScreenError::Account);
        }
        self.targets = screen_targets(&self.spec, self.config.realm_id, self.ingress).into();
        self.checked = true;
        self.advance()
    }
}

#[derive(Debug, PartialEq)]
enum AdmitPhase {
    Screen,
    Work(PluginInstall, WorkLookup),
    Task(PluginInstall, Option<WorkRow>),
}

/// The receiver's part of the task check, run where the task executes: the replicated part,
/// this node is the install's receiver, and the work row named by the tags is this install's,
/// bound to its current version and dispatched. A reserved row answers `Pending`, never a refusal.
#[derive(Debug, PartialEq)]
pub struct AdmitTask {
    screen: Option<ScreenTask>,
    local: NodeId,
    family: JobFamilyId,
    work_id: Option<Ulid>,
    phase: Option<AdmitPhase>,
}

impl AdmitTask {
    pub fn new(screen: ScreenTask, (local, family): (NodeId, JobFamilyId)) -> Self {
        let work_id = tagged(&screen.spec).and_then(Result::ok);
        Self {
            screen: Some(screen),
            local,
            family,
            work_id: work_id.map(|tagged| tagged.work_id),
            phase: Some(AdmitPhase::Screen),
        }
    }
}

impl Operation for AdmitTask {
    type Output = PluginInstall;
    type Error = ScreenError;

    fn first(&mut self) -> Result<Next<PluginInstall>, ScreenError> {
        let screen = self.screen.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(screen)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginInstall>, ScreenError> {
        let unavailable = |error: super::rows::StoreError| ScreenError::Unavailable(error.0);
        match self.phase.take().ok_or(IoError::Unfinished)? {
            AdmitPhase::Screen => {
                let install = result.child::<PluginInstall, ScreenError>()??;
                if install.receiver != self.local {
                    let why = "this node is not the install's receiver".to_string();
                    return Err(ScreenError::Refused(why));
                }
                let work_id = self.work_id.ok_or(ScreenError::Malformed)?;
                let lookup = WorkLookup::new(install.install_id, work_id);
                let ask = lookup.ask();
                self.phase = Some(AdmitPhase::Work(install, lookup));
                Ok(Next::Call(ask))
            }
            AdmitPhase::Work(install, mut lookup) => {
                match lookup.take(result).map_err(unavailable)? {
                    Found::Ask(ask) => {
                        self.phase = Some(AdmitPhase::Work(install, lookup));
                        Ok(Next::Call(ask))
                    }
                    Found::Row(found) => {
                        let work_id = self.work_id.ok_or(ScreenError::Malformed)?;
                        let read = read_call(vec![task_place(install.install_id, work_id)]);
                        let row = found.map(|(_, row)| row);
                        self.phase = Some(AdmitPhase::Task(install, row));
                        Ok(Next::Call(read))
                    }
                }
            }
            AdmitPhase::Task(install, row) => {
                let [task] = values::<1>(result).map_err(unavailable)?;
                let task = (task.map(|bytes| decode::<TaskRow>(&bytes)))
                    .transpose()
                    .map_err(unavailable)?;
                let adopted = task.and_then(|task| task.family);
                work_admits(&install, row.as_ref(), (adopted, self.family))?;
                Ok(Next::Done(install))
            }
        }
    }
}

#[cfg(test)]
#[path = "screen_tests.rs"]
pub(crate) mod tests;
