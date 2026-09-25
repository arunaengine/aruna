//! Coordinates repository transfers of crate jobs and dispatches them to the adapter of each kind.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;

use aruna_core::handle::Handle;
use aruna_core::repository::{
    ImportMode, ImportOptions, LinkFailure, LinkTarget, PullCheck, RemoteState,
    RepositoryCredential, RepositoryDestination, RepositoryLink, RepositoryPull, RepositoryQuery,
};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::execution::job::{
    ArtifactRef, ExportRoCrateSpec, ImportRoCrateSource, ImportRoCrateSpec,
};
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::secondary_id::SecondaryIdentifier;
use aruna_core::structs::storage::blob::BucketInfo;
use serde_json::Value;
use ulid::Ulid;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::{PolicyEnforcementError, PolicyRequestExtras};
use crate::driver::{DriverContext, drive};
use crate::harvest::create_connector::INVENIO_TOKEN as CONNECTOR_TOKEN;
use crate::harvest::read_connector::{ConnectorView, GetRepositoryOperation, ReadConnectorError};
use crate::harvest::repository::{parse_secret_read, read_secret_effect};

use super::executor::JobContext;
use super::export::ExportCheckpoint;

pub mod check;
pub mod invenio;
pub mod link_queue;
pub mod links;
pub mod pull;
pub(crate) mod push;
pub use invenio::query::RecordReference;

#[derive(Debug, thiserror::Error)]
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

/// Runs a one-time or link export's repository deposit.
pub(crate) async fn deposit(
    kind: RepositoryConnectorKind,
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &RepositoryDestination,
    checkpoint: &mut ExportCheckpoint,
) -> Result<(), TransferError> {
    match kind {
        RepositoryConnectorKind::Invenio => {
            invenio::export::repository_export(ctx, spec, destination, checkpoint).await
        }
        RepositoryConnectorKind::OaiPmh => Err(not_supported("deposit")),
    }
}

/// Downloads a repository record into an import artifact with the identifiers it found.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn acquire(
    kind: RepositoryConnectorKind,
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    group_id: Ulid,
    connector_id: Ulid,
    record_id: &str,
    options: &ImportOptions,
    pull: Option<&RepositoryPull>,
) -> Result<
    (
        ArtifactRef,
        Vec<SecondaryIdentifier>,
        Option<pull::PullProgress>,
    ),
    TransferError,
> {
    match kind {
        RepositoryConnectorKind::Invenio => {
            invenio::import::acquire(ctx, spec, group_id, connector_id, record_id, options, pull)
                .await
        }
        RepositoryConnectorKind::OaiPmh => Err(not_supported("import")),
    }
}

/// Fails as remote changed when a link's lineage moved outside Aruna; returns the published
/// version a parent-only link continues.
pub(crate) async fn check_lineage(
    kind: RepositoryConnectorKind,
    ctx: &JobContext,
    spec: &ExportRoCrateSpec,
    destination: &RepositoryDestination,
    target: &LinkTarget,
) -> Result<Option<String>, TransferError> {
    match kind {
        RepositoryConnectorKind::Invenio => {
            invenio::remote::check_lineage(ctx, spec, destination, target).await
        }
        RepositoryConnectorKind::OaiPmh => Err(not_supported("links")),
    }
}

/// The connector kind of a repository import in reference mode; `None` for any other import.
pub(crate) async fn reference_kind(
    context: &DriverContext,
    spec: &ImportRoCrateSpec,
) -> Result<Option<RepositoryConnectorKind>, TransferError> {
    match &spec.source {
        ImportRoCrateSource::Repository {
            group_id,
            connector_id,
            options,
            ..
        } if options.mode == ImportMode::Reference => Ok(Some(
            connector_kind(context, *group_id, *connector_id).await?,
        )),
        _ => Ok(None),
    }
}

/// Whether an entry of a reference import artifact is a reference descriptor, not file bytes.
pub(crate) fn is_reference(kind: RepositoryConnectorKind, path: &str) -> bool {
    match kind {
        RepositoryConnectorKind::Invenio => invenio::import::is_reference(path),
        RepositoryConnectorKind::OaiPmh => false,
    }
}

/// Writes a reference object whose bytes stay in the repository.
pub(crate) async fn write_reference(
    kind: RepositoryConnectorKind,
    ctx: &JobContext,
    spec: &ImportRoCrateSpec,
    bucket: BucketInfo,
    key: &str,
    version_id: Ulid,
    descriptor: &Value,
) -> Result<SourceMetadata, TransferError> {
    match kind {
        RepositoryConnectorKind::Invenio => {
            invenio::reference::write_reference(ctx, spec, bucket, key, version_id, descriptor)
                .await
        }
        RepositoryConnectorKind::OaiPmh => Err(not_supported("references")),
    }
}

/// Searches the repository's records with its native query syntax.
pub async fn search(
    kind: RepositoryConnectorKind,
    context: &DriverContext,
    auth: &AuthContext,
    query: &RepositoryQuery,
    limit: u64,
) -> Result<Value, TransferError> {
    match kind {
        RepositoryConnectorKind::Invenio => {
            invenio::query::search_records(context, auth, query, limit).await
        }
        RepositoryConnectorKind::OaiPmh => Err(not_supported("search")),
    }
}

/// Resolves a DOI or record URL to the repository's record id.
pub async fn resolve(
    kind: RepositoryConnectorKind,
    context: &DriverContext,
    auth: &AuthContext,
    group_id: Ulid,
    connector_id: Ulid,
    reference: &RecordReference,
    limit: u64,
) -> Result<String, TransferError> {
    match kind {
        RepositoryConnectorKind::Invenio => {
            invenio::query::resolve_record(context, auth, group_id, connector_id, reference, limit)
                .await
        }
        RepositoryConnectorKind::OaiPmh => Err(not_supported("record lookup")),
    }
}

/// The repository's current draft and latest published version, for accepting remote edits.
pub async fn remote_state(
    kind: RepositoryConnectorKind,
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<RemoteState, TransferError> {
    match kind {
        RepositoryConnectorKind::Invenio => invenio::remote::remote_state(context, link).await,
        RepositoryConnectorKind::OaiPmh => Err(not_supported("links")),
    }
}

/// The repository's answer to a pending review; `None` while the review is still open.
pub(crate) async fn review_state(
    kind: RepositoryConnectorKind,
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<Option<RemoteState>, TransferError> {
    match kind {
        RepositoryConnectorKind::Invenio => invenio::remote::review_state(context, link).await,
        RepositoryConnectorKind::OaiPmh => Err(not_supported("reviews")),
    }
}

/// The latest version of a pull link's record lineage.
pub(crate) async fn latest_version(
    kind: RepositoryConnectorKind,
    context: &DriverContext,
    link: &RepositoryLink,
) -> Result<PullCheck, TransferError> {
    match kind {
        RepositoryConnectorKind::Invenio => invenio::remote::latest_version(context, link).await,
        RepositoryConnectorKind::OaiPmh => Err(not_supported("pull links")),
    }
}

fn not_supported(action: &str) -> TransferError {
    TransferError::Permanent(format!("this repository kind does not support {action}"))
}

impl From<std::io::Error> for TransferError {
    fn from(error: std::io::Error) -> Self {
        Self::Retryable(error.to_string())
    }
}

impl From<async_zip::error::ZipError> for TransferError {
    fn from(error: async_zip::error::ZipError) -> Self {
        Self::Retryable(error.to_string())
    }
}

pub async fn seal_credential(
    context: &DriverContext,
    auth: &AuthContext,
    destination: &RepositoryDestination,
    token: &str,
) -> Result<RepositoryCredential, TransferError> {
    let view = repository(context, destination.group_id, destination.connector_id).await?;
    let key = context
        .net_handle
        .as_ref()
        .ok_or_else(|| TransferError::Retryable("node credential key unavailable".into()))?
        .credential_encryption_key();
    Ok(RepositoryCredential::seal(
        &key,
        auth.user_id,
        destination.group_id,
        destination.connector_id,
        view.connector.endpoint,
        token,
    )?)
}

/// Seals a link's token for its creator, bound to the connector's current endpoint.
pub async fn seal_link_token(
    context: &DriverContext,
    user: aruna_core::UserId,
    group_id: Ulid,
    connector_id: Ulid,
    link_id: Ulid,
    token: &str,
) -> Result<RepositoryCredential, TransferError> {
    let view = repository(context, group_id, connector_id).await?;
    let key = context
        .net_handle
        .as_ref()
        .ok_or_else(|| TransferError::Retryable("node credential key unavailable".into()))?
        .credential_encryption_key();
    Ok(RepositoryCredential::seal_link(
        &key,
        user,
        group_id,
        connector_id,
        Some(link_id),
        view.connector.endpoint,
        token,
    )?)
}

/// The repository kind of the group's connector.
pub async fn connector_kind(
    context: &DriverContext,
    group_id: Ulid,
    connector_id: Ulid,
) -> Result<RepositoryConnectorKind, TransferError> {
    Ok(repository(context, group_id, connector_id)
        .await?
        .connector
        .kind)
}

/// Opens the group's connector of `kind` for a transfer after checking the caller's access in the
/// connector group: the connector and its token. A sealed personal credential gives the token,
/// else the connector's own token when it keeps one.
pub(crate) async fn open_connector(
    context: &DriverContext,
    auth: &AuthContext,
    kind: RepositoryConnectorKind,
    group_id: Ulid,
    connector_id: Ulid,
    permission: Permission,
    credential: Option<&RepositoryCredential>,
) -> Result<(ConnectorView, Option<String>), TransferError> {
    authorize(
        context,
        auth.realm_id,
        auth,
        &format!("/{}/g/{group_id}/meta/**", auth.realm_id),
        &permission,
        PolicyRequestExtras::operation("metadata.repository"),
    )
    .await
    .map_err(|error| match error {
        AuthorizeError::Storage(_)
        | AuthorizeError::CheckFailed(_)
        | AuthorizeError::Policy(PolicyEnforcementError::Unavailable(_)) => {
            TransferError::Retryable(error.to_string())
        }
        _ => TransferError::Permanent(error.to_string()),
    })?;
    let view = repository(context, group_id, connector_id).await?;
    if view.connector.kind != kind {
        return Err(TransferError::Permanent(format!(
            "the repository connector is not of kind {kind}"
        )));
    }
    let endpoint = &view.connector.endpoint;
    let token = match credential {
        Some(credential) => {
            let key = context
                .net_handle
                .as_ref()
                .ok_or_else(|| TransferError::Retryable("node credential key unavailable".into()))?
                .credential_encryption_key();
            Some(credential.open(&key, auth.user_id, group_id, connector_id, endpoint)?)
        }
        None if view.has_secret_config => connector_token(context, connector_id).await?,
        None => None,
    };
    Ok((view, token))
}

async fn connector_token(
    context: &DriverContext,
    connector_id: Ulid,
) -> Result<Option<String>, TransferError> {
    let event = context
        .storage_handle
        .send_effect(read_secret_effect(connector_id, None))
        .await;
    let secret = parse_secret_read(event)
        .map_err(|_| TransferError::Retryable("repository connector storage unavailable".into()))?;
    Ok(secret.and_then(|secret| secret.secret_config.get(CONNECTOR_TOKEN).cloned()))
}

/// Reads the group's repository connector.
pub(crate) async fn repository(
    context: &DriverContext,
    group_id: Ulid,
    connector_id: Ulid,
) -> Result<ConnectorView, TransferError> {
    let view = drive(GetRepositoryOperation::new(group_id, connector_id), context)
        .await
        .map_err(|error| match error {
            ReadConnectorError::NotFound => {
                TransferError::Permanent("repository connector unavailable".into())
            }
            ReadConnectorError::Storage(_) | ReadConnectorError::Unexpected => {
                TransferError::Retryable("repository connector storage unavailable".into())
            }
        })?;
    Ok(view)
}

pub(crate) async fn interruptible<T>(
    ctx: &JobContext,
    future: impl Future<Output = Result<T, TransferError>>,
) -> Result<T, TransferError> {
    tokio::select! {
        biased;
        _ = ctx.cancel.cancelled() => Err(TransferError::Cancelled),
        _ = ctx.shutdown.cancelled() => Err(TransferError::Interrupted),
        result = future => result,
    }
}
