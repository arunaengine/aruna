//! Routes vault changes to a holder and serves vault fetches between realm nodes.
//! A holder checks the forwarded bearer token itself and serves a vault only to its own user.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::calls::{ChildOutcome, IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::VaultQuery;
use aruna_core::events::{Event, WritePeerRefusal};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, boxed_retry};
use aruna_core::placement::{holds_placement, target_placement_ref};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::identity::user::vault::VaultRecords;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use super::read::LocalRecordsOperation;
use super::write::{
    AppendVaultConfig, AppendVaultError, AppendVaultOperation, VaultAppended, VaultRefusal,
};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::authorize::{peer_answer, peer_check};
use crate::forward::routing::{ForwardHoldersOperation, HolderWriteError};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage, WriteAuthError};
use crate::metadata::transport_message_kind;
use crate::realm::peer_trust::{is_sync_eligible, peer_acts_for};

#[derive(Debug, Error, PartialEq)]
pub enum VaultRouteError {
    #[error(transparent)]
    Append(#[from] AppendVaultError),
    #[error(transparent)]
    Forward(#[from] HolderWriteError),
    #[error("a vault holder refused the change: {0:?}")]
    Refused(VaultRefusal),
}

impl From<IoError> for VaultRouteError {
    fn from(error: IoError) -> Self {
        Self::Forward(error.into())
    }
}

/// One append on this node; a save that lost its transaction to a concurrent one runs again.
fn append_call(config: &AppendVaultConfig) -> IoCall {
    let seed = config.record_id.to_bytes();
    let config = config.clone();
    let build = move || AppendVaultOperation::new(config.clone());
    IoCall::Child(boxed_retry(build, &seed, |result| {
        Event::Child(ChildOutcome::new(result))
    }))
}

/// Appends on this node when it holds the user's vault, otherwise on one of the
/// holders; a change no holder accepts fails instead of waiting in an outbox.
#[derive(Debug, PartialEq)]
pub struct AppendRoutedOperation {
    config: AppendVaultConfig,
    auth_token: Option<AuthToken>,
    forwarded: bool,
}

impl AppendRoutedOperation {
    pub fn new(config: AppendVaultConfig, auth_token: Option<AuthToken>) -> Self {
        Self {
            config,
            auth_token,
            forwarded: false,
        }
    }

    fn forward(&mut self, holders: &[NodeId]) -> Next<VaultAppended> {
        let message = MetadataTransportMessage::ForwardVaultChange {
            auth_token: self.auth_token.take(),
            record_id: self.config.record_id,
            change: Box::new(self.config.change.clone()),
            created_at_ms: self.config.now_ms,
        };
        self.forwarded = true;
        let local = Some(self.config.node_id);
        let walk = ForwardHoldersOperation::new(local, holders, &message, None, false);
        Next::Call(IoCall::child(walk))
    }
}

impl Operation for AppendRoutedOperation {
    type Output = VaultAppended;
    type Error = VaultRouteError;

    fn first(&mut self) -> Result<Next<VaultAppended>, VaultRouteError> {
        Ok(Next::Call(append_call(&self.config)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<VaultAppended>, VaultRouteError> {
        if !self.forwarded {
            return match result.child::<VaultAppended, AppendVaultError>()? {
                Err(AppendVaultError::NotHolder { holders }) => Ok(self.forward(&holders)),
                appended => Ok(Next::Done(appended?)),
            };
        }
        match result.child::<MetadataTransportMessage, HolderWriteError>()?? {
            MetadataTransportMessage::ForwardedVaultChange { result } => {
                Ok(Next::Done(result.map_err(VaultRouteError::Refused)?))
            }
            other => Err(HolderWriteError::Undeliverable(format!(
                "holder answered a vault change with {}",
                transport_message_kind(&other)
            ))
            .into()),
        }
    }
}

/// The user a forwarded bearer token names, checked again on this node. Internal
/// principals and path-restricted tokens never reach a vault.
#[derive(Debug, PartialEq)]
struct VaultCallerOperation {
    peer: NodeId,
    realm_id: RealmId,
    auth_token: Option<AuthToken>,
    auth: Option<AuthContext>,
}

impl VaultCallerOperation {
    fn new(peer: NodeId, realm_id: RealmId, auth_token: Option<AuthToken>) -> Self {
        Self {
            peer,
            realm_id,
            auth_token,
            auth: None,
        }
    }
}

impl Operation for VaultCallerOperation {
    type Output = UserId;
    type Error = MetadataReadError;

    fn first(&mut self) -> Result<Next<UserId>, MetadataReadError> {
        if !matches!(self.auth_token, Some(AuthToken::Bearer(_))) {
            return Err(MetadataReadError::Unauthorized);
        }
        Ok(Next::Call(peer_check(self.peer, self.auth_token.take())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<UserId>, MetadataReadError> {
        let Some(auth) = self.auth.take() else {
            let auth = peer_answer(result)?.map_err(|refusal| match refusal {
                WritePeerRefusal::Unauthorized => MetadataReadError::Unauthorized,
                WritePeerRefusal::Unavailable(_) => MetadataReadError::Unavailable,
            })?;
            self.auth = Some(auth);
            let read = GetConfigOperation::new(self.realm_id);
            return Ok(Next::Call(IoCall::child(read)));
        };
        let config = result
            .child::<RealmConfigDocument, GetConfigError>()?
            .map_err(|_| MetadataReadError::Unavailable)?;
        if auth.realm_id != self.realm_id
            || auth.path_restrictions.is_some()
            || !peer_acts_for(&config, self.peer, auth.user_id)
        {
            return Err(MetadataReadError::Forbidden);
        }
        Ok(Next::Done(auth.user_id))
    }
}

type ApplyTurn = Result<Next<MetadataTransportMessage>, IoError>;

/// Runs one forwarded vault change for the user the forwarded token names.
#[derive(Debug, PartialEq)]
pub(crate) struct ApplyVaultOperation {
    local: Option<(NodeId, RealmId)>,
    peer: NodeId,
    message: MetadataTransportMessage,
    appending: bool,
}

impl ApplyVaultOperation {
    pub(crate) fn new(
        local: Option<(NodeId, RealmId)>,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> Self {
        Self {
            local,
            peer,
            message,
            appending: false,
        }
    }

    fn append(&mut self, user_id: UserId) -> ApplyTurn {
        let (
            Some((node_id, realm_id)),
            MetadataTransportMessage::ForwardVaultChange {
                record_id,
                change,
                created_at_ms,
                ..
            },
        ) = (self.local, &self.message)
        else {
            return Ok(Next::Done(
                MetadataTransportMessage::ForwardedWriteUnavailable,
            ));
        };
        let config = AppendVaultConfig {
            node_id,
            realm_id,
            user_id,
            record_id: *record_id,
            change: (**change).clone(),
            now_ms: *created_at_ms,
        };
        self.appending = true;
        Ok(Next::Call(append_call(&config)))
    }
}

impl Operation for ApplyVaultOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> ApplyTurn {
        let MetadataTransportMessage::ForwardVaultChange { auth_token, .. } = &self.message else {
            return Ok(Next::Done(MetadataTransportMessage::Reject(
                "unexpected vault change".to_string(),
            )));
        };
        let Some((_, realm_id)) = self.local else {
            return Ok(Next::Done(
                MetadataTransportMessage::ForwardedWriteUnavailable,
            ));
        };
        let caller = VaultCallerOperation::new(self.peer, realm_id, auth_token.clone());
        Ok(Next::Call(IoCall::child(caller)))
    }

    fn next(&mut self, result: IoResult) -> ApplyTurn {
        if !self.appending {
            let denied = |error| MetadataTransportMessage::ForwardedWriteDenied { error };
            return match result.child::<UserId, MetadataReadError>()? {
                Ok(user_id) => self.append(user_id),
                Err(MetadataReadError::Unauthorized) => {
                    Ok(Next::Done(denied(WriteAuthError::Unauthorized)))
                }
                Err(MetadataReadError::Forbidden) => {
                    Ok(Next::Done(denied(WriteAuthError::Forbidden)))
                }
                Err(_) => Ok(Next::Done(
                    MetadataTransportMessage::ForwardedWriteUnavailable,
                )),
            };
        }
        let result = match result.child::<VaultAppended, AppendVaultError>()? {
            Ok(appended) => Ok(appended),
            Err(AppendVaultError::Record(_)) => Err(VaultRefusal::Invalid),
            Err(AppendVaultError::TooManyKeys) => Err(VaultRefusal::TooManyKeys),
            // Another holder may still accept it.
            Err(error) => {
                warn!(%error, "Forwarded vault change failed");
                return Ok(Next::Done(
                    MetadataTransportMessage::ForwardedWriteUnavailable,
                ));
            }
        };
        Ok(Next::Done(MetadataTransportMessage::ForwardedVaultChange {
            result,
        }))
    }
}

#[derive(Debug, PartialEq)]
enum ServeStage {
    Config,
    Caller(RealmConfigDocument),
    Records,
}

type ServeTurn = Result<Next<VaultRecords>, MetadataReadError>;

/// Serves this holder's records of one user to a peer.
#[derive(Debug, PartialEq)]
pub(crate) struct ServeVaultOperation {
    local: Option<(NodeId, RealmId)>,
    peer: NodeId,
    user_id: UserId,
    query: VaultQuery,
    stage: ServeStage,
}

impl ServeVaultOperation {
    pub(crate) fn new(
        local: Option<(NodeId, RealmId)>,
        peer: NodeId,
        user_id: UserId,
        query: VaultQuery,
    ) -> Self {
        Self {
            local,
            peer,
            user_id,
            query,
            stage: ServeStage::Config,
        }
    }

    fn local(&self) -> Result<(NodeId, RealmId), MetadataReadError> {
        self.local.ok_or(MetadataReadError::Unavailable)
    }

    /// A node that holds no replica has nothing to say, so the requester asks the next holder.
    fn read(&mut self, config: &RealmConfigDocument, heads: bool) -> ServeTurn {
        let (node_id, realm_id) = self.local()?;
        let (user_id, record) = (self.user_id, Ulid::nil());
        let target = match heads {
            true => DocumentTarget::VaultRevision {
                realm_id,
                user_id,
                revision_id: record,
            },
            false => DocumentTarget::UserKey {
                realm_id,
                user_id,
                record_id: record,
            },
        };
        let placement = target_placement_ref(config, &target, Default::default());
        if !holds_placement(config, &placement, node_id) {
            return Err(MetadataReadError::Unavailable);
        }
        self.stage = ServeStage::Records;
        let read = LocalRecordsOperation::new(user_id, heads);
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for ServeVaultOperation {
    type Output = VaultRecords;
    type Error = MetadataReadError;

    fn first(&mut self) -> ServeTurn {
        let (_, realm_id) = self.local()?;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> ServeTurn {
        match std::mem::replace(&mut self.stage, ServeStage::Records) {
            ServeStage::Config => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|_| MetadataReadError::Unavailable)?;
                match &self.query {
                    VaultQuery::Heads { auth_token } => {
                        let (_, realm_id) = self.local()?;
                        let token = auth_token.clone();
                        let caller = VaultCallerOperation::new(self.peer, realm_id, token);
                        self.stage = ServeStage::Caller(config);
                        Ok(Next::Call(IoCall::child(caller)))
                    }
                    VaultQuery::Keys if is_sync_eligible(&config, self.peer) => {
                        self.read(&config, false)
                    }
                    VaultQuery::Keys => Err(MetadataReadError::Forbidden),
                }
            }
            ServeStage::Caller(config) => {
                if result.child::<UserId, MetadataReadError>()?? != self.user_id {
                    return Err(MetadataReadError::Forbidden);
                }
                self.read(&config, true)
            }
            ServeStage::Records => {
                let records = result.child::<VaultRecords, MetadataReadError>()??;
                Ok(Next::Done(records))
            }
        }
    }
}

#[cfg(test)]
#[path = "route_tests.rs"]
mod tests;
