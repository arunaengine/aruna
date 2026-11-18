//! The reads and checks behind a work attempt and a run's job calls: admitting an attempt, the
//! grants of a transfer, the work row of a job and the objects it wrote.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::plugin_installs::{InstallState, PluginInstall};
use aruna_core::structs::execution::job::{OutputObject, PluginWorkSpec};
use aruna_core::structs::identity::auth::Permission as Access;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_plugins::broker::{ErrorKind, HostError, ObjectInfo, ObjectPage};
use aruna_plugins::permission::Permission;
use ulid::Ulid;

use super::account::{read_user_call, read_user_result};
use super::check::{Authorize, CallError, check_identity, config_unavailable, matching};
use super::grants::InstallScope;
use super::marks::is_marked;
use super::rows::StoreError;
use super::unavailable;
use super::work::{WorkRow, read_work};
use crate::document_repository::{GetConfigError, GetConfigOperation};

/// Outputs of one page.
const OUTPUT_PAGE: usize = 100;

/// Why a work attempt does not run now.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum WorkRefusal {
    /// State it needs cannot be read now.
    #[error("{0}")]
    Deferred(String),
    /// The install holds the work until it is active again.
    #[error("{0}")]
    Held(String),
    /// The work can never run.
    #[error("{0}")]
    Permanent(String),
    /// A check of the install or its account refused it.
    #[error("{}", .0.message)]
    Refused(HostError),
}

impl From<IoError> for WorkRefusal {
    fn from(error: IoError) -> Self {
        Self::Deferred(error.to_string())
    }
}

/// What an admitted attempt runs with.
#[derive(Debug, PartialEq)]
pub(crate) struct AdmittedWork {
    pub install: PluginInstall,
    pub config: RealmConfigDocument,
    pub row: WorkRow,
}

#[derive(Debug, PartialEq)]
enum AdmitPhase {
    Config,
    Row(Box<RealmConfigDocument>),
    Account(Box<RealmConfigDocument>, Box<WorkRow>),
}

/// The install, account and binding checks of one attempt, before any grant check.
#[derive(Debug, PartialEq)]
pub(crate) struct AdmitWork {
    realm_id: RealmId,
    local: NodeId,
    install_id: Ulid,
    work_id: Ulid,
    phase: AdmitPhase,
}

impl AdmitWork {
    pub(crate) fn new(spec: &PluginWorkSpec, local: NodeId) -> Self {
        Self {
            realm_id: spec.realm_id,
            local,
            install_id: spec.install_id,
            work_id: spec.work_id,
            phase: AdmitPhase::Config,
        }
    }

    fn decide(
        &self,
        config: RealmConfigDocument,
        row: WorkRow,
        result: IoResult,
    ) -> Result<AdmittedWork, WorkRefusal> {
        let user = read_user_result(result).map_err(|error| WorkRefusal::Deferred(error.0))?;
        let place = (&config, self.realm_id, self.local);
        let binding = (self.install_id, row.binding.digest.as_str());
        let install =
            check_identity(place, binding, user.as_ref()).map_err(WorkRefusal::Refused)?;
        if install.account != row.account {
            let earlier = "the plugin work belongs to an earlier install";
            return Err(WorkRefusal::Permanent(earlier.to_string()));
        }
        let state = format!("the install is {}", install.state.as_str());
        // A pause, or an update waiting for approval, holds the work until the install is active.
        match install.state {
            InstallState::Active if !install.paused => {}
            InstallState::Active | InstallState::Paused | InstallState::Blocked => {
                return Err(WorkRefusal::Held(state));
            }
            _ => return Err(WorkRefusal::Permanent(state)),
        }
        let install = install.clone();
        Ok(AdmittedWork {
            install,
            config,
            row,
        })
    }
}

impl Operation for AdmitWork {
    type Output = AdmittedWork;
    type Error = WorkRefusal;

    fn first(&mut self) -> Result<Next<AdmittedWork>, WorkRefusal> {
        let read = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AdmittedWork>, WorkRefusal> {
        match std::mem::replace(&mut self.phase, AdmitPhase::Config) {
            AdmitPhase::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config
                    .map_err(|error| WorkRefusal::Deferred(config_unavailable(error).0.message))?;
                self.phase = AdmitPhase::Row(Box::new(config));
                let read = read_work(self.install_id, self.work_id);
                Ok(Next::Call(IoCall::child(read)))
            }
            AdmitPhase::Row(config) => {
                let row = result.child::<Option<WorkRow>, StoreError>()?;
                let row = row.map_err(|error| WorkRefusal::Deferred(error.to_string()))?;
                let Some(row) = row else {
                    let gone = "the plugin work is no longer recorded";
                    return Err(WorkRefusal::Permanent(gone.to_string()));
                };
                let account = row.account;
                self.phase = AdmitPhase::Account(config, Box::new(row));
                Ok(Next::Call(read_user_call(account)))
            }
            AdmitPhase::Account(config, row) => Ok(Next::Done(self.decide(*config, *row, result)?)),
        }
    }
}

/// The current patterns a transfer may use: the row's connection is still granted and authorized
/// as at submission, then the transfer's own grant on its object.
#[derive(Debug, PartialEq)]
pub(crate) struct TransferGrants {
    install: PluginInstall,
    connection: Option<String>,
    permission: Permission,
    /// The one access checked; `None` checks every access of the permission.
    access: Option<Access>,
    path: String,
    patterns: Option<BTreeSet<String>>,
}

impl TransferGrants {
    pub(crate) fn download(install: &PluginInstall, row: &WorkRow, target: &str) -> Self {
        Self::new(install, row, Permission::TransfersDownload, None, target)
    }

    pub(crate) fn upload(install: &PluginInstall, row: &WorkRow, source: &str) -> Self {
        let read = Some(Access::READ);
        Self::new(install, row, Permission::TransfersUpload, read, source)
    }

    fn new(
        install: &PluginInstall,
        row: &WorkRow,
        permission: Permission,
        access: Option<Access>,
        path: &str,
    ) -> Self {
        let scope = InstallScope {
            realm_id: install.account.realm_id,
            group_id: install.group_id,
            install_id: install.install_id,
        };
        let connection = row.context.as_ref();
        Self {
            install: install.clone(),
            connection: connection.map(|context| scope.connection(&context.connection)),
            permission,
            access,
            path: path.to_string(),
            patterns: None,
        }
    }
}

impl Operation for TransferGrants {
    type Output = BTreeSet<String>;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<Self::Output>, CallError> {
        let connection = self.connection.as_ref();
        let no_request = || CallError(HostError::new(ErrorKind::Invalid, "no request"));
        let connection = connection.ok_or_else(no_request)?;
        let send = Permission::ConnectionsSend;
        let patterns = matching(&self.install, send, connection)?;
        let check = Authorize::call(&self.install, send, connection, &patterns);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, CallError> {
        result.child::<(), CallError>()??;
        if let Some(patterns) = self.patterns.take() {
            return Ok(Next::Done(patterns));
        }
        let patterns = matching(&self.install, self.permission, &self.path)?;
        let check = match &self.access {
            Some(access) => {
                let access = (self.permission, access.clone());
                Authorize::access(&self.install, access, &self.path, &patterns)
            }
            None => Authorize::call(&self.install, self.permission, &self.path, &patterns),
        };
        self.patterns = Some(patterns);
        Ok(Next::Call(IoCall::child(check)))
    }
}

/// The work row of job `id` of an install; a row of an earlier install under the same id is not
/// this install's job.
#[derive(Debug, PartialEq)]
pub(crate) struct JobRow {
    install_id: Ulid,
    account: UserId,
    id: String,
}

impl JobRow {
    pub(crate) fn new(install: &PluginInstall, id: &str) -> Self {
        Self {
            install_id: install.install_id,
            account: install.account,
            id: id.to_string(),
        }
    }

    fn missing(&self) -> CallError {
        let message = format!("the install has no job {}", self.id);
        CallError(HostError::new(ErrorKind::NotFound, message))
    }
}

impl Operation for JobRow {
    type Output = WorkRow;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<WorkRow>, CallError> {
        let work_id = Ulid::from_string(&self.id).map_err(|_| self.missing())?;
        Ok(Next::Call(IoCall::child(read_work(
            self.install_id,
            work_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<WorkRow>, CallError> {
        let row = result.child::<Option<WorkRow>, StoreError>()?;
        let row = row.map_err(|error| CallError(unavailable(error.0)))?;
        let row = row.filter(|row| row.account == self.account);
        Ok(Next::Done(row.ok_or_else(|| self.missing())?))
    }
}

#[derive(Debug, PartialEq)]
enum OutputsPhase {
    Row,
    Marks(Box<ObjectInfo>),
}

/// One page of the objects job `id` wrote, after the output at `cursor`.
#[derive(Debug, PartialEq)]
pub(crate) struct JobOutputs {
    row: Option<JobRow>,
    local: NodeId,
    cursor: Option<String>,
    pending: VecDeque<OutputObject>,
    next_cursor: Option<String>,
    objects: Vec<ObjectInfo>,
    phase: OutputsPhase,
}

impl JobOutputs {
    pub(crate) fn new(
        (install, local): (&PluginInstall, NodeId),
        id: &str,
        cursor: Option<String>,
    ) -> Self {
        Self {
            row: Some(JobRow::new(install, id)),
            local,
            cursor,
            pending: VecDeque::new(),
            next_cursor: None,
            objects: Vec::new(),
            phase: OutputsPhase::Row,
        }
    }

    fn take_row(&mut self, row: WorkRow) -> Result<(), CallError> {
        let start = match &self.cursor {
            Some(text) => text.parse::<usize>().map_err(|_| {
                let message = format!("`{text}` is not an outputs cursor");
                CallError(HostError::new(ErrorKind::Invalid, message))
            })?,
            None => 0,
        };
        let outputs = (row.summary.map(|summary| summary.outputs)).unwrap_or_default();
        let next = start + OUTPUT_PAGE;
        self.next_cursor = (next < outputs.len()).then(|| next.to_string());
        self.pending = (outputs.into_iter().skip(start).take(OUTPUT_PAGE))
            .filter(|output| output.node_id == self.local)
            .collect();
        Ok(())
    }

    fn advance(&mut self) -> Result<Next<ObjectPage>, CallError> {
        let Some(output) = self.pending.pop_front() else {
            return Ok(Next::Done(ObjectPage {
                objects: std::mem::take(&mut self.objects),
                cursor: self.next_cursor.take(),
            }));
        };
        let version = (
            output.bucket.as_str(),
            output.key.as_str(),
            output.version_id,
        );
        let marked = is_marked(version)?;
        self.phase = OutputsPhase::Marks(Box::new(ObjectInfo {
            path: format!("{}/{}", output.bucket, output.key),
            size: output.size,
            version: output.version_id.to_string(),
            marked: false,
        }));
        Ok(Next::Call(IoCall::child(marked)))
    }
}

impl Operation for JobOutputs {
    type Output = ObjectPage;
    type Error = CallError;

    fn first(&mut self) -> Result<Next<ObjectPage>, CallError> {
        let row = self.row.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(row)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ObjectPage>, CallError> {
        match std::mem::replace(&mut self.phase, OutputsPhase::Row) {
            OutputsPhase::Row => {
                let row = result.child::<WorkRow, CallError>()??;
                self.take_row(row)?;
            }
            OutputsPhase::Marks(mut info) => {
                info.marked = result.child::<bool, StoreError>()??;
                self.objects.push(*info);
            }
        }
        self.advance()
    }
}

#[cfg(test)]
#[path = "work_calls_tests.rs"]
mod tests;
