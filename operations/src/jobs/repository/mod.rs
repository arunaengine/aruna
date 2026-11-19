//! Coordinates repository transfers of crate jobs and dispatches them to the adapter of each kind.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::BlobError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::repository::remote::{
    JobStop, NodeScope, RemoteAnswer, RemoteCall, RemoteFailure, RemoteRequest, StopReason,
};
use aruna_core::repository::{ImportMode, LinkFailure, RepositoryCredential, descriptor};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::{ImportRoCrateSource, ImportRoCrateSpec, JobId};
use ulid::Ulid;

use crate::harvest::read_connector::ConnectorView;

use super::executor::ProgressReporter;

pub mod check;
pub mod connector;
pub mod invenio;
pub mod link_queue;
pub mod link_store;
pub mod links;
pub mod pull;
pub(crate) mod push;
mod submit;
#[cfg(any(test, feature = "test-hooks"))]
pub mod test_support;
pub use invenio::query::RecordReference;

/// The running job an operation works for: its identity, stop signals and progress.
#[derive(Clone)]
pub struct JobScope {
    pub(crate) job_id: JobId,
    pub(crate) owner_node: NodeId,
    pub(crate) claim_token: Ulid,
    pub(crate) stop: JobStop,
    pub(crate) progress: ProgressReporter,
}

impl std::fmt::Debug for JobScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JobScope")
            .field("job_id", &self.job_id)
            .field("owner_node", &self.owner_node)
            .finish_non_exhaustive()
    }
}

/// The progress counter is output only, so it takes no part in comparing runs.
impl PartialEq for JobScope {
    fn eq(&self, other: &Self) -> bool {
        self.job_id == other.job_id
            && self.owner_node == other.owner_node
            && self.claim_token == other.claim_token
            && self.stop == other.stop
    }
}

/// The call that runs `call` in the blob backend, ending early when `stop` fires.
pub(crate) fn remote_call(call: RemoteCall, stop: Option<JobStop>) -> IoCall {
    IoCall::Effect(Box::new(Effect::Blob(BlobEffect::Remote(Box::new(
        RemoteRequest { stop, call },
    )))))
}

/// The answer to a [`remote_call`].
pub(crate) fn remote_answer(
    result: IoResult,
) -> Result<Result<RemoteAnswer, RemoteFailure>, IoError> {
    match result.event()? {
        Event::Blob(BlobEvent::Remote(answer)) => Ok(answer),
        Event::Blob(BlobEvent::Error(BlobError::HandleMissing)) => Ok(Err(
            RemoteFailure::Retryable("blob handle unavailable".to_string()),
        )),
        Event::Blob(BlobEvent::Error(error)) => {
            Ok(Err(RemoteFailure::Retryable(error.to_string())))
        }
        other => Err(IoError::Unexpected {
            expected: "BlobEvent::Remote",
            got: other.label(),
        }),
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum TransferError {
    #[error("{0}")]
    Permanent(String),
    #[error("{0}")]
    Retryable(String),
    #[error("repository transfer cancelled")]
    Cancelled,
    #[error("repository transfer interrupted")]
    Interrupted,
    /// A refusal a lasting link reports as its failure reason.
    #[error("repository refused the request ({})", .0.reason())]
    Refused(LinkFailure),
}

impl From<aruna_core::repository::RepositoryError> for TransferError {
    fn from(error: aruna_core::repository::RepositoryError) -> Self {
        Self::Permanent(error.to_string())
    }
}

impl From<IoError> for TransferError {
    fn from(error: IoError) -> Self {
        Self::Retryable(error.to_string())
    }
}

impl From<RemoteFailure> for TransferError {
    fn from(failure: RemoteFailure) -> Self {
        match failure {
            RemoteFailure::Stopped(StopReason::Cancelled) => Self::Cancelled,
            RemoteFailure::Stopped(StopReason::Interrupted) => Self::Interrupted,
            RemoteFailure::Invenio(error) => error.into(),
            RemoteFailure::Permanent(message) => Self::Permanent(message),
            RemoteFailure::Retryable(message) => Self::Retryable(message),
        }
    }
}

/// The connector of a repository import in reference mode; `None` for any other import.
pub(crate) fn reference_source(spec: &ImportRoCrateSpec) -> Option<(Ulid, Ulid)> {
    match &spec.source {
        ImportRoCrateSource::Repository {
            group_id,
            connector_id,
            options,
            ..
        } if options.mode == ImportMode::Reference => Some((*group_id, *connector_id)),
        _ => None,
    }
}

/// Whether an entry of a reference import artifact is a reference descriptor, not file bytes.
pub(crate) fn is_reference(kind: RepositoryConnectorKind, path: &str) -> bool {
    match kind {
        RepositoryConnectorKind::Invenio => invenio::import::is_reference(path),
        RepositoryConnectorKind::OaiPmh => false,
    }
}

fn not_supported(action: &str) -> TransferError {
    TransferError::Permanent(format!("this repository kind does not support {action}"))
}

/// An action a repository kind may not support.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Action {
    /// Links, one-time exports and requirement checks.
    Publish,
    /// Publishing an open draft and accepting remote drafts.
    Drafts,
    ReserveIdentifier,
    Review,
    Import,
    /// Keeping an import updated through a pull link.
    Pull,
    /// Continuing a record's version lineage.
    Versions,
    Search,
}

impl Action {
    pub fn name(self) -> &'static str {
        match self {
            Self::Publish => "publishing",
            Self::Drafts => "drafts",
            Self::ReserveIdentifier => "reserving identifiers",
            Self::Review => "reviews",
            Self::Import => "imports",
            Self::Pull => "pull links",
            Self::Versions => "versions",
            Self::Search => "search",
        }
    }
}

/// Whether `kind` supports `action`; API routes and jobs gate on this one rule.
pub fn supports(kind: RepositoryConnectorKind, action: Action) -> bool {
    let Some(descriptor) = descriptor(kind) else {
        return false;
    };
    let can = descriptor.capabilities;
    match action {
        Action::Publish => !descriptor.profiles.is_empty() && descriptor.rules().is_ok(),
        Action::Drafts => can.drafts,
        Action::ReserveIdentifier => can.reserve_identifier,
        Action::Review => can.review,
        Action::Import => can.import,
        Action::Pull => can.pull,
        Action::Versions => can.versions,
        Action::Search => can.search,
    }
}

/// Fails permanently unless `kind` supports `action`.
pub fn ensure_supported(
    kind: RepositoryConnectorKind,
    action: Action,
) -> Result<(), TransferError> {
    if supports(kind, action) {
        return Ok(());
    }
    Err(not_supported(action.name()))
}

/// Seals a user's token for the connector's endpoint, and for `link_id` when a link keeps it.
pub fn seal_token(
    scope: &NodeScope,
    user: aruna_core::UserId,
    view: &ConnectorView,
    link_id: Option<Ulid>,
    token: &str,
) -> Result<RepositoryCredential, TransferError> {
    let key = scope
        .credential_key
        .as_ref()
        .ok_or_else(|| TransferError::Retryable("node credential key unavailable".into()))?;
    Ok(RepositoryCredential::seal_link(
        key,
        user,
        view.connector.group_id,
        view.connector.connector_id,
        link_id,
        view.connector.endpoint.clone(),
        token,
    )?)
}
