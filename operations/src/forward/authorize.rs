//! Decides which peer may forward a request, for which user, and what it may write.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::protocol::WriteAuthError;
use crate::realm::peer_trust::peer_acts_for;
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::events::{Event, NetEvent, WritePeerRefusal};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::rows::event;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use tracing::warn;
use ulid::Ulid;

#[derive(Debug, PartialEq)]
pub(crate) enum ForwardAuthError {
    Unauthorized,
    Forbidden,
    Unavailable(String),
}

impl From<IoError> for ForwardAuthError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

impl From<AuthorizeError> for ForwardAuthError {
    fn from(error: AuthorizeError) -> Self {
        match error {
            AuthorizeError::PermissionDenied | AuthorizeError::Policy(_) => Self::Forbidden,
            AuthorizeError::CheckFailed(error) => Self::Unavailable(error),
            AuthorizeError::Storage(error) => Self::Unavailable(error.to_string()),
        }
    }
}

pub(crate) fn forward_auth_error(error: ForwardAuthError) -> MetadataTransportMessage {
    match error {
        ForwardAuthError::Unauthorized => MetadataTransportMessage::ForwardedWriteDenied {
            error: WriteAuthError::Unauthorized,
        },
        ForwardAuthError::Forbidden => MetadataTransportMessage::ForwardedWriteDenied {
            error: WriteAuthError::Forbidden,
        },
        ForwardAuthError::Unavailable(error) => {
            warn!(%error, "Forwarded metadata authorization is unavailable");
            MetadataTransportMessage::ForwardedWriteUnavailable
        }
    }
}

/// The call that checks a forwarded credential; [`peer_answer`] reads its result.
pub(crate) fn peer_check(peer: NodeId, auth_token: Option<AuthToken>) -> IoCall {
    let auth_token = auth_token.map(Box::new);
    IoCall::Effect(Box::new(Effect::Net(NetEffect::AuthorizeWritePeer {
        peer,
        auth_token,
    })))
}

pub(crate) fn peer_answer(result: IoResult) -> Result<Result<AuthContext, WritePeerRefusal>, IoError> {
    match event(result)? {
        Event::Net(NetEvent::WritePeerChecked(checked)) => Ok(checked.map(|auth| *auth)),
        other => Err(IoError::Unexpected {
            expected: "a peer check",
            got: other.label(),
        }),
    }
}

/// The credential a forwarded write carries.
fn caller_token(message: &MetadataTransportMessage) -> Option<AuthToken> {
    match message {
        MetadataTransportMessage::ForwardCreateDocument { auth_token, .. }
        | MetadataTransportMessage::ForwardUpdateDocument { auth_token, .. }
        | MetadataTransportMessage::ForwardDeleteDocument { auth_token, .. }
        | MetadataTransportMessage::ForwardCreatePolicy { auth_token, .. }
        | MetadataTransportMessage::ForwardGroupCreate { auth_token, .. }
        | MetadataTransportMessage::GroupDeletion { auth_token, .. } => auth_token.clone(),
        MetadataTransportMessage::ForwardTokenRevocation { auth_token, .. }
        | MetadataTransportMessage::ForwardApplyBatch { auth_token, .. } => {
            Some(auth_token.clone())
        }
        _ => None,
    }
}

/// Authenticates the caller of a forwarded request. A User peer is owner-bound: whatever token
/// it presents, the write it forwards must be its owner's own.
#[derive(Debug, PartialEq)]
pub(crate) struct ForwardedCallerOperation {
    peer: NodeId,
    realm_id: RealmId,
    auth_token: Option<AuthToken>,
    owner_bound: bool,
    auth: Option<AuthContext>,
}

impl ForwardedCallerOperation {
    pub(crate) fn new(
        peer: NodeId,
        realm_id: RealmId,
        message: &MetadataTransportMessage,
    ) -> Self {
        Self {
            owner_bound: true,
            ..Self::for_token(peer, realm_id, caller_token(message))
        }
    }

    /// The caller of a request whose peer need not act for that user.
    pub(crate) fn for_token(
        peer: NodeId,
        realm_id: RealmId,
        auth_token: Option<AuthToken>,
    ) -> Self {
        Self {
            peer,
            realm_id,
            auth_token,
            owner_bound: false,
            auth: None,
        }
    }
}

impl Operation for ForwardedCallerOperation {
    type Output = AuthContext;
    type Error = ForwardAuthError;

    fn first(&mut self) -> Result<Next<AuthContext>, ForwardAuthError> {
        Ok(Next::Call(peer_check(self.peer, self.auth_token.take())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AuthContext>, ForwardAuthError> {
        let Some(auth) = self.auth.take() else {
            let auth = peer_answer(result)?.map_err(|refusal| match refusal {
                WritePeerRefusal::Unauthorized => ForwardAuthError::Unauthorized,
                WritePeerRefusal::Unavailable(error) => ForwardAuthError::Unavailable(error),
            })?;
            if auth.realm_id != self.realm_id {
                return Err(ForwardAuthError::Forbidden);
            }
            if !self.owner_bound {
                return Ok(Next::Done(auth));
            }
            self.auth = Some(auth);
            return Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                self.realm_id,
            ))));
        };
        let config = result
            .child::<RealmConfigDocument, GetConfigError>()?
            .map_err(|_| {
                ForwardAuthError::Unavailable(
                    "forwarded metadata write needs the realm configuration".to_string(),
                )
            })?;
        match peer_acts_for(&config, self.peer, auth.user_id) {
            true => Ok(Next::Done(auth)),
            false => Err(ForwardAuthError::Forbidden),
        }
    }
}

/// The WRITE check of a forwarded request on `path`.
pub(crate) fn write_check(auth: &AuthContext, path: &str) -> AuthorizeOperation {
    let extras = PolicyRequestExtras::rest();
    AuthorizeOperation::new(auth.realm_id, auth, path, &Permission::WRITE, extras)
}

/// What a create is checked on besides the document's own permission path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CreateScope {
    /// Write access on the group's whole metadata too, as REST and MCP require.
    Group,
    /// The document's own path only, for an account restricted to some paths.
    Document,
}

/// Checks that the caller may create the document, on its group first when `scope` asks.
#[derive(Debug, PartialEq)]
pub(crate) struct AuthorizeCreateOperation {
    realm_id: RealmId,
    auth: AuthContext,
    extras: PolicyRequestExtras,
    /// Reversed, so the next path pops off the end.
    paths: Vec<String>,
}

impl AuthorizeCreateOperation {
    pub(crate) fn new(
        realm_id: RealmId,
        auth: &AuthContext,
        extras: PolicyRequestExtras,
        group_id: GroupId,
        path: &str,
        document_id: Ulid,
        scope: CreateScope,
    ) -> Self {
        let mut paths = vec![MetadataRegistryRecord::permission_path_for(
            &auth.realm_id,
            group_id,
            path,
            document_id,
        )];
        if scope == CreateScope::Group {
            paths.push(format!("/{realm_id}/g/{group_id}/meta/**"));
        }
        Self {
            realm_id,
            auth: auth.clone(),
            extras,
            paths,
        }
    }

    fn check_next(&mut self) -> Next<()> {
        match self.paths.pop() {
            Some(path) => Next::Call(IoCall::child(AuthorizeOperation::new(
                self.realm_id,
                &self.auth,
                &path,
                &Permission::WRITE,
                self.extras.clone(),
            ))),
            None => Next::Done(()),
        }
    }
}

impl Operation for AuthorizeCreateOperation {
    type Output = ();
    type Error = AuthorizeError;

    fn first(&mut self) -> Result<Next<()>, AuthorizeError> {
        Ok(self.check_next())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, AuthorizeError> {
        result.child::<(), AuthorizeError>()??;
        Ok(self.check_next())
    }
}

#[cfg(test)]
#[path = "authorize_tests.rs"]
mod tests;
