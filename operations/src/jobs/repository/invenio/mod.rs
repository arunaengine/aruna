//! Invenio adapter: connects to InvenioRDM repositories and runs their HTTP transfers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::invenio::{InvenioClient, InvenioError};
use aruna_core::handle::Handle;
use aruna_core::repository::{LinkFailure, RepositoryCredential};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use ulid::Ulid;

use super::{TransferError, repository};
use crate::auth::request_authorization::{AuthorizeError, authorize};
use crate::auth::request_policy::{PolicyEnforcementError, PolicyRequestExtras};
use crate::driver::DriverContext;
use crate::harvest::create_connector::INVENIO_TOKEN;
use crate::harvest::repository::{parse_secret_read, read_secret_effect};

pub mod export;
pub(crate) mod import;
pub(crate) mod query;
pub(crate) mod reference;
mod verify;

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
