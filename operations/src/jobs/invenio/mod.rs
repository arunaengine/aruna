//! Coordinates repository transfers using existing connector authority and crate jobs.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::future::Future;

use aruna_blob::invenio::{InvenioClient, InvenioError};
use aruna_core::structs::execution::source_access::ResolvedSourceAccess;
use aruna_core::structs::execution::source_connector::SourceConnectorKind;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use ulid::Ulid;

use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::{PolicyEnforcementError, PolicyRequestExtras};
use crate::connectors::resolver::{ResolveConnectorInput, ResolveConnectorOperation};
use crate::driver::drive;

use super::executor::JobContext;

pub(crate) mod export;
pub(crate) mod import;

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
}

impl From<InvenioError> for TransferError {
    fn from(error: InvenioError) -> Self {
        match error {
            InvenioError::Transport | InvenioError::Status(429 | 500..=599) => {
                Self::Retryable(error.to_string())
            }
            _ => Self::Permanent(error.to_string()),
        }
    }
}

impl From<aruna_core::invenio::InvenioError> for TransferError {
    fn from(error: aruna_core::invenio::InvenioError) -> Self {
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
    ctx: &'a JobContext,
    auth: &AuthContext,
    group_id: Ulid,
    connector_id: Ulid,
    permission: Permission,
    limit: u64,
) -> Result<InvenioClient<'a>, TransferError> {
    authorize(
        &ctx.driver,
        auth.realm_id,
        auth,
        &format!("/{}/g/{group_id}/data/**", auth.realm_id),
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
    let resolved = drive(
        ResolveConnectorOperation::new(ResolveConnectorInput {
            group_id,
            connector_id,
            source_path: String::new(),
            allow_root: true,
        }),
        &ctx.driver,
    )
    .await
    .map_err(|error| match error {
        aruna_core::errors::SourceResolutionError::StorageError(_) => {
            TransferError::Retryable("repository connector storage unavailable".into())
        }
        _ => TransferError::Permanent("repository connector unavailable".into()),
    })?;
    let ResolvedSourceAccess::OpenDal { kind, config, .. } = resolved.access;
    if kind != SourceConnectorKind::Http || config.get("root").is_some_and(|root| root != "/") {
        return Err(TransferError::Permanent(
            "repository requires an HTTP connector without a root prefix".into(),
        ));
    }
    let endpoint = config
        .get("endpoint")
        .ok_or_else(|| TransferError::Permanent("repository endpoint missing".into()))?;
    let blob = ctx
        .driver
        .blob_handle
        .as_ref()
        .ok_or_else(|| TransferError::Retryable("blob handle unavailable".into()))?;
    Ok(InvenioClient::new(
        blob,
        endpoint,
        config.get("token").cloned(),
        limit,
    )?)
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
