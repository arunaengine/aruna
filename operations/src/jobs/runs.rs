//! Reads where a physical run id leads: its run crate, for the submitter and the crate's readers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{JobId, RunCrateStatus, RunLink};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::error::{AuthorizeError, PolicyEnforcementError};
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::jobs::store::JobMutationError;
use crate::jobs::store::row_calls::{read_crate_status, read_run_link};

#[derive(Debug, Error, PartialEq)]
pub enum RunReadError {
    /// Unknown here, or the caller may not see it; both read the same.
    #[error("run not found")]
    NotFound,
    #[error("{0}")]
    Unavailable(String),
}

impl From<IoError> for RunReadError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

impl From<JobMutationError> for RunReadError {
    fn from(error: JobMutationError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

/// What a caller may see of one run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunView {
    pub run_id: JobId,
    /// The submitted job, shown to the submitter only.
    pub logical_job_id: Option<JobId>,
    pub run_crate: Option<Ulid>,
    /// Why the crate is in its state (written, denied, failed, local); submitter only.
    pub run_crate_status: Option<serde_json::Value>,
}

#[derive(Debug, PartialEq)]
enum RunStep {
    Link,
    Access(Box<RunLink>),
    Status(Box<RunLink>),
}

/// Reads the run link this node keeps. The submitter with an unrestricted credential sees
/// everything; anyone else needs READ on the run crate with their own credential.
#[derive(Debug, PartialEq)]
pub struct ReadRunOperation {
    realm_id: RealmId,
    auth: AuthContext,
    run_id: JobId,
    step: RunStep,
}

type RunNext = Result<Next<RunView>, RunReadError>;

impl ReadRunOperation {
    pub fn new(realm_id: RealmId, auth: &AuthContext, run_id: JobId) -> Self {
        Self {
            realm_id,
            auth: auth.clone(),
            run_id,
            step: RunStep::Link,
        }
    }

    fn view(&self, link: &RunLink, status: Option<serde_json::Value>, full: bool) -> RunView {
        RunView {
            run_id: self.run_id,
            logical_job_id: link.logical_job_id.filter(|_| full),
            run_crate: link.run_crate,
            run_crate_status: status,
        }
    }

    fn linked(&mut self, link: RunLink) -> RunNext {
        if self.auth.user_id == link.created_by && self.auth.path_restrictions.is_none() {
            self.step = RunStep::Status(Box::new(link));
            return Ok(Next::Call(IoCall::child(read_crate_status(self.run_id))));
        }
        let document_id = link.run_crate.ok_or(RunReadError::NotFound)?;
        let path = MetadataRegistryRecord::permission_path_for(
            &self.realm_id,
            link.group_id,
            &format!("runs/{}", self.run_id),
            document_id,
        );
        self.step = RunStep::Access(Box::new(link));
        Ok(Next::Call(IoCall::child(AuthorizeOperation::new(
            self.realm_id,
            self.auth.clone(),
            path,
            Permission::READ,
            PolicyRequestExtras::rest(),
        ))))
    }
}

impl Operation for ReadRunOperation {
    type Output = RunView;
    type Error = RunReadError;

    fn first(&mut self) -> RunNext {
        Ok(Next::Call(IoCall::child(read_run_link(self.run_id))))
    }

    fn next(&mut self, result: IoResult) -> RunNext {
        match std::mem::replace(&mut self.step, RunStep::Link) {
            RunStep::Link => {
                let link = result.child::<Option<RunLink>, JobMutationError>()??;
                self.linked(link.ok_or(RunReadError::NotFound)?)
            }
            RunStep::Access(link) => match result.child::<(), AuthorizeError>()? {
                Ok(()) => Ok(Next::Done(self.view(&link, None, false))),
                Err(AuthorizeError::Storage(error)) => {
                    Err(RunReadError::Unavailable(error.to_string()))
                }
                Err(AuthorizeError::Policy(PolicyEnforcementError::Unavailable(reason))) => {
                    Err(RunReadError::Unavailable(reason))
                }
                Err(_) => Err(RunReadError::NotFound),
            },
            RunStep::Status(link) => {
                let status = result.child::<Option<RunCrateStatus>, JobMutationError>()??;
                let status = status.map(|status| status.to_public_json());
                Ok(Next::Done(self.view(&link, status, true)))
            }
        }
    }

    fn expected_error(error: &RunReadError) -> bool {
        matches!(error, RunReadError::NotFound)
    }
}

#[cfg(test)]
#[path = "runs_tests.rs"]
mod tests;
