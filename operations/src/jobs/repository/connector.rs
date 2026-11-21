//! Reads repository connectors and opens one for a transfer after checking the caller's access.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent};
use aruna_core::operation::Operation;
use aruna_core::repository::credential::{CredentialOpen, RepositoryCredential};
use aruna_core::repository::remote::{InvenioAccess, JobStop, NodeScope};
use aruna_core::structs::execution::harvest::RepositoryConnectorKind;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use ulid::Ulid;

use super::TransferError;
use super::invenio::Session;
use crate::auth::error::{AuthorizeError, PolicyEnforcementError};
use crate::auth::request_authorization::AuthorizeOperation;
use crate::connectors::harvest_rows::{
    INVENIO_TOKEN as CONNECTOR_TOKEN, parse_secret_read, read_secret_effect,
};
use crate::harvest::read_connector::{ConnectorView, GetRepositoryOperation, ReadConnectorError};
use aruna_core::request_policy::PolicyRequestExtras;

/// Reads the group's repository connector, with the failures a transfer reports.
#[derive(Debug, PartialEq)]
pub struct ConnectorViewOperation {
    group_id: Ulid,
    connector_id: Ulid,
}

impl ConnectorViewOperation {
    pub fn new(group_id: Ulid, connector_id: Ulid) -> Self {
        Self {
            group_id,
            connector_id,
        }
    }
}

impl Operation for ConnectorViewOperation {
    type Output = ConnectorView;
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<ConnectorView>, TransferError> {
        Ok(Next::Call(IoCall::child(GetRepositoryOperation::new(
            self.group_id,
            self.connector_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<ConnectorView>, TransferError> {
        let view = result
            .child::<ConnectorView, ReadConnectorError>()?
            .map_err(connector_failure)?;
        Ok(Next::Done(view))
    }
}

fn connector_failure(error: ReadConnectorError) -> TransferError {
    match error {
        ReadConnectorError::NotFound => {
            TransferError::Permanent("repository connector unavailable".into())
        }
        ReadConnectorError::Storage(_) | ReadConnectorError::Unexpected => {
            TransferError::Retryable("repository connector storage unavailable".into())
        }
    }
}

/// A fault while deciding can pass; a refusal stays.
fn access_failure(error: AuthorizeError) -> TransferError {
    match error {
        AuthorizeError::Storage(_)
        | AuthorizeError::CheckFailed(_)
        | AuthorizeError::Policy(PolicyEnforcementError::Unavailable(_)) => {
            TransferError::Retryable(error.to_string())
        }
        _ => TransferError::Permanent(error.to_string()),
    }
}

/// What a transfer opens a connector with.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ConnectRequest {
    pub(crate) scope: NodeScope,
    pub(crate) auth: AuthContext,
    pub(crate) group_id: Ulid,
    pub(crate) connector_id: Ulid,
    pub(crate) permission: Permission,
    pub(crate) limit: u64,
    pub(crate) credential: Option<RepositoryCredential>,
    /// The stop signal of the session's calls.
    pub(crate) stop: Option<JobStop>,
}

#[derive(Debug, PartialEq)]
enum ConnectStep {
    Access,
    Connector,
    Open(ConnectorView),
    Secret(ConnectorView),
}

/// Opens the group's Invenio connector for a transfer after checking the caller's access in the
/// connector group. A sealed personal credential gives the token, else the connector's own
/// token when it keeps one.
#[derive(Debug, PartialEq)]
pub(crate) struct ConnectOperation {
    request: ConnectRequest,
    step: ConnectStep,
}

impl ConnectOperation {
    pub(crate) fn new(request: ConnectRequest) -> Self {
        Self {
            request,
            step: ConnectStep::Access,
        }
    }

    fn opened(
        &self,
        view: &ConnectorView,
        token: Option<String>,
    ) -> Result<Session, TransferError> {
        if !self.request.scope.has_blob {
            return Err(TransferError::Retryable("blob handle unavailable".into()));
        }
        let access = InvenioAccess::new(&view.connector.endpoint, token, self.request.limit)?;
        Ok(Session::new(access, self.request.stop.clone()))
    }
}

impl Operation for ConnectOperation {
    type Output = Session;
    type Error = TransferError;

    fn first(&mut self) -> Result<Next<Session>, TransferError> {
        let auth = &self.request.auth;
        let path = format!("/{}/g/{}/meta/**", auth.realm_id, self.request.group_id);
        Ok(Next::Call(IoCall::child(AuthorizeOperation::new(
            auth.realm_id,
            auth,
            &path,
            &self.request.permission,
            PolicyRequestExtras::operation("metadata.repository"),
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Session>, TransferError> {
        match std::mem::replace(&mut self.step, ConnectStep::Connector) {
            ConnectStep::Access => {
                result
                    .child::<(), AuthorizeError>()?
                    .map_err(access_failure)?;
                Ok(Next::Call(IoCall::child(ConnectorViewOperation::new(
                    self.request.group_id,
                    self.request.connector_id,
                ))))
            }
            ConnectStep::Connector => {
                let view = result.child::<ConnectorView, TransferError>()??;
                let kind = RepositoryConnectorKind::Invenio;
                if view.connector.kind != kind {
                    return Err(TransferError::Permanent(format!(
                        "the repository connector is not of kind {kind}"
                    )));
                }
                if let Some(credential) = self.request.credential.take() {
                    let open = CredentialOpen {
                        credential,
                        user: self.request.auth.user_id,
                        group: self.request.group_id,
                        connector: self.request.connector_id,
                        endpoint: view.connector.endpoint.clone(),
                    };
                    self.step = ConnectStep::Open(view);
                    let open = Effect::Net(NetEffect::OpenCredential(Box::new(open)));
                    return Ok(Next::Call(open.into()));
                }
                if !view.has_secret_config {
                    return Ok(Next::Done(self.opened(&view, None)?));
                }
                self.step = ConnectStep::Secret(view);
                Ok(Next::Call(
                    read_secret_effect(self.request.connector_id, None).into(),
                ))
            }
            ConnectStep::Open(view) => {
                let Event::Net(NetEvent::CredentialOpened(opened)) = result.event()? else {
                    let expected = "NetEvent::CredentialOpened";
                    let got = "another event";
                    return Err(IoError::Unexpected { expected, got }.into());
                };
                let token = opened.ok_or_else(|| {
                    TransferError::Retryable("node credential key unavailable".into())
                })?;
                let token = token.map_err(TransferError::Permanent)?;
                Ok(Next::Done(
                    self.opened(&view, Some(token.expose().to_string()))?,
                ))
            }
            ConnectStep::Secret(view) => {
                let secret = parse_secret_read(result.event()?).map_err(|_| {
                    TransferError::Retryable("repository connector storage unavailable".into())
                })?;
                let token =
                    secret.and_then(|secret| secret.secret_config.get(CONNECTOR_TOKEN).cloned());
                Ok(Next::Done(self.opened(&view, token)?))
            }
        }
    }
}

#[cfg(test)]
#[path = "connector_tests.rs"]
mod tests;
