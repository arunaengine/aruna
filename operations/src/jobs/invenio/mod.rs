//! Coordinates repository transfers through Invenio repository connectors and crate jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;

use aruna_blob::invenio::{InvenioClient, InvenioError};
use aruna_core::handle::Handle;
use aruna_core::repository::{LinkFailure, RepositoryCredential, RepositoryDestination};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use ulid::Ulid;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::{PolicyEnforcementError, PolicyRequestExtras};
use crate::driver::{DriverContext, drive};
use crate::harvest::create_connector::INVENIO_TOKEN;
use crate::harvest::read_connector::{ConnectorView, GetRepositoryOperation, ReadConnectorError};
use crate::harvest::repository::{parse_secret_read, read_secret_effect};

use super::executor::JobContext;

pub mod export;
pub(crate) mod import;
pub mod link_queue;
pub mod links;
pub mod pull;
pub(crate) mod push;
mod query;
pub(crate) mod reference;
mod verify;
pub use push::remote_state;
pub use query::{RecordReference, resolve_record, search_records};

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

impl From<InvenioError> for TransferError {
    fn from(error: InvenioError) -> Self {
        match error {
            InvenioError::Transport | InvenioError::Status(429 | 500..=599) => {
                Self::Retryable(error.to_string())
            }
            InvenioError::Status(401 | 403) => Self::Refused(LinkFailure::TokenRejected),
            _ => Self::Permanent(error.to_string()),
        }
    }
}

impl From<aruna_core::repository::RepositoryError> for TransferError {
    fn from(error: aruna_core::repository::RepositoryError) -> Self {
        Self::Permanent(error.to_string())
    }
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

pub(crate) async fn connect<'a>(
    context: &'a DriverContext,
    auth: &AuthContext,
    group_id: Ulid,
    connector_id: Ulid,
    permission: Permission,
    limit: u64,
    credential: Option<&RepositoryCredential>,
) -> Result<InvenioClient<'a>, TransferError> {
    authorize(
        context,
        auth.realm_id,
        auth,
        &format!("/{}/g/{group_id}/meta/**", auth.realm_id),
        &permission,
        PolicyRequestExtras::operation("metadata.invenio"),
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
    let endpoint = &view.connector.endpoint;
    let blob = context
        .blob_handle
        .as_ref()
        .ok_or_else(|| TransferError::Retryable("blob handle unavailable".into()))?;
    // Exports only use the personal sealed token; the connector token is for private reads.
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
    Ok(InvenioClient::new(blob, endpoint, token, limit)?)
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

/// Reads the group's Invenio repository connector; other kinds are refused.
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
    if view.connector.kind != RepositoryConnectorKind::Invenio {
        return Err(TransferError::Permanent(
            "repository requires an Invenio repository connector".into(),
        ));
    }
    Ok(view)
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
    Ok(secret.and_then(|secret| secret.secret_config.get(INVENIO_TOKEN).cloned()))
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
