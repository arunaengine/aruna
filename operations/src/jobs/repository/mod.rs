//! Coordinates repository transfers through Invenio repository connectors and crate jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;

use aruna_core::repository::{LinkFailure, RepositoryCredential, RepositoryDestination};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::identity::auth::AuthContext;
use ulid::Ulid;

use crate::driver::{DriverContext, drive};
use crate::harvest::read_connector::{ConnectorView, GetRepositoryOperation, ReadConnectorError};

use super::executor::JobContext;

pub mod invenio;
pub mod link_queue;
pub mod links;
pub mod pull;
pub(crate) mod push;
pub use invenio::query::{RecordReference, resolve_record, search_records};
pub use invenio::remote::remote_state;

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
