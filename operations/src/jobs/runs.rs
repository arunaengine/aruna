//! Reads where a physical run id leads: its run crate, for the submitter and the crate's readers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::structs::execution::job::JobId;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::error::AuthorizeError;
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::driver::DriverContext;
use crate::driver::drive;
use crate::jobs::store::attempt::{read_crate_status, read_run_link};

#[derive(Debug, Error)]
pub enum RunReadError {
    /// Unknown here, or the caller may not see it; both read the same.
    #[error("run not found")]
    NotFound,
    #[error("{0}")]
    Unavailable(String),
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

/// Reads the run link this node keeps. The submitter with an unrestricted credential sees it;
/// anyone else needs READ on the run crate with their own credential.
pub async fn read_run(
    context: &DriverContext,
    realm_id: RealmId,
    auth: &AuthContext,
    run_id: JobId,
) -> Result<RunView, RunReadError> {
    let link = read_run_link(context, run_id)
        .await
        .map_err(RunReadError::Unavailable)?
        .ok_or(RunReadError::NotFound)?;
    let submitter = auth.user_id == link.created_by && auth.path_restrictions.is_none();
    if !submitter {
        let document_id = link.run_crate.ok_or(RunReadError::NotFound)?;
        let path = MetadataRegistryRecord::permission_path_for(
            &realm_id,
            link.group_id,
            &format!("runs/{run_id}"),
            document_id,
        );
        let extras = PolicyRequestExtras::rest();
        match drive(
            AuthorizeOperation::new(realm_id, auth, &path, &Permission::READ, extras),
            context,
        )
        .await
        {
            Ok(()) => {}
            Err(AuthorizeError::Storage(error)) => {
                return Err(RunReadError::Unavailable(error.to_string()));
            }
            Err(AuthorizeError::Policy(PolicyEnforcementError::Unavailable(reason))) => {
                return Err(RunReadError::Unavailable(reason));
            }
            Err(_) => return Err(RunReadError::NotFound),
        }
    }
    let run_crate_status = match submitter {
        true => read_crate_status(context, run_id)
            .await
            .map_err(RunReadError::Unavailable)?
            .map(|status| status.to_public_json()),
        false => None,
    };
    Ok(RunView {
        run_id,
        logical_job_id: link.logical_job_id.filter(|_| submitter),
        run_crate: link.run_crate,
        run_crate_status,
    })
}

#[cfg(test)]
#[path = "runs_tests.rs"]
mod tests;
