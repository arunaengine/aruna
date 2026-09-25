//! Invenio adapter: connects to InvenioRDM repositories and runs their HTTP transfers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_blob::invenio::{InvenioClient, InvenioError};
use aruna_core::repository::{LinkFailure, RepositoryCredential};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use ulid::Ulid;

use super::{TransferError, open_connector};
use crate::driver::DriverContext;

pub mod export;
pub(crate) mod import;
pub(crate) mod query;
pub(crate) mod reference;
pub(crate) mod remote;
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
    // Exports only use the personal sealed token; the connector token is for private reads.
    let (view, token) = open_connector(
        context,
        auth,
        RepositoryConnectorKind::Invenio,
        group_id,
        connector_id,
        permission,
        credential,
    )
    .await?;
    let blob = context
        .blob_handle
        .as_ref()
        .ok_or_else(|| TransferError::Retryable("blob handle unavailable".into()))?;
    Ok(InvenioClient::new(
        blob,
        &view.connector.endpoint,
        token,
        limit,
    )?)
}
