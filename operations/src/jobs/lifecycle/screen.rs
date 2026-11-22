//! Checks a plugin task's job against its install from replicated state, so every family
//! holder decides the same: the install and its account, and the grants on each input and output.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::install_work::{
    Found, TaskRow, WorkKind, WorkLookup, WorkRow, WorkState, task_place,
};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::PluginInstall;
use aruna_core::rows::{decode, values};
use aruna_core::structs::execution::job::family::JobFamilyId;
use aruna_core::structs::execution::job::input::{
    ExecutionSpec, InputSource, OutputDestination, TagError, named_install, tagged,
};
use aruna_core::structs::identity::auth::Permission as Access;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::User;
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::types::Key;
use aruna_core::user::id::UserId;
use aruna_plugin_types::permission::Permission;
use ulid::Ulid;

use aruna_ops_identity::auth::error::AuthorizeError;
use aruna_ops_identity::auth::install_access::{Authorize, granted};
use aruna_ops_identity::users::install_account::{owns_install, read_user_call, read_user_result};

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

impl From<TagError> for ScreenError {
    fn from(error: TagError) -> Self {
        match error {
            TagError::Malformed => Self::Malformed,
            TagError::NoOutput => Self::NoOutput,
            TagError::Install(state) => Self::Install(state),
        }
    }
}

impl From<IoError> for ScreenError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
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

/// An authorization fault decides nothing and is asked again; any other answer refuses.
fn refused(error: AuthorizeError) -> ScreenError {
    match error {
        AuthorizeError::Storage(error) => ScreenError::Unavailable(error.to_string()),
        AuthorizeError::CheckFailed(error) => ScreenError::Unavailable(error),
        error => ScreenError::Refused(error.to_string()),
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
        let patterns = granted(install, permission, &path);
        if patterns.is_empty() {
            let why = format!("{permission} is not granted on {path}");
            return Err(ScreenError::Refused(why));
        }
        let check = Authorize::access(install, (permission, access), &path, &patterns);
        Ok(Next::Call(IoCall::child(check)))
    }
}

impl Operation for ScreenTask {
    type Output = PluginInstall;
    type Error = ScreenError;

    fn first(&mut self) -> Result<Next<PluginInstall>, ScreenError> {
        let tagged = tagged(&self.spec).ok_or(ScreenError::Malformed)??;
        let install = named_install(&self.config.installs, tagged)?;
        if install.account != self.created_by || install.group_id != self.spec.group_id {
            return Err(ScreenError::Account);
        }
        let read = read_user_call(install.account);
        self.install = Some(install.clone());
        Ok(Next::Call(read))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PluginInstall>, ScreenError> {
        if self.checked {
            let checked = result.child::<(), AuthorizeError>()?;
            checked.map_err(refused)?;
            return self.advance();
        }
        let user = read_user_result(result)?;
        let install = self.install()?;
        let (group_id, install_id) = (install.group_id, install.install_id);
        let owned = |user: &User| owns_install(user, group_id, install_id);
        if !user.is_some_and(|user| owned(&user) && !user.is_deactivated()) {
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
            AdmitPhase::Work(install, mut lookup) => match lookup.take(result)? {
                Found::Ask(ask) => {
                    self.phase = Some(AdmitPhase::Work(install, lookup));
                    Ok(Next::Call(ask))
                }
                Found::Row(found) => {
                    let work_id = self.work_id.ok_or(ScreenError::Malformed)?;
                    let (space, key) = task_place(install.install_id, work_id);
                    let read = IoCall::Read(vec![(space.to_string(), Key::from(key))]);
                    let row = found.map(|(_, row)| row);
                    self.phase = Some(AdmitPhase::Task(install, row));
                    Ok(Next::Call(read))
                }
            },
            AdmitPhase::Task(install, row) => {
                let [task] = values::<1>(result)?;
                let task = (task.map(|bytes| decode::<TaskRow>(&bytes))).transpose()?;
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
