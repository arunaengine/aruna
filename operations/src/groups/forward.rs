//! Forwards a group create to a peer that may publish it and applies one arriving here.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::auth::error::AuthorizeError;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::authorize::{
    ForwardAuthError, ForwardedCallerOperation, forward_auth_error, write_check,
};
use crate::forward::fetch::{peer_call, peer_reply};
use crate::forward::transport::reject;
use crate::groups::create_group::{CreateGroupConfig, CreateGroupError, CreateGroupOperation};
use crate::metadata::protocol::{MetadataTransportMessage, WriteAuthError, encode_message};
use crate::realm::peer_trust::is_sync_eligible;
use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StorageError;
use aruna_core::events::PeerRequestError;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::placement::selector::select_top_peers;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

pub(super) const CREATE_PEER_LIMIT: usize = 4;

pub(super) const CREATE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Why a forwarded group create did not produce a group. `Conflict` is the
/// caller's problem (quota or a racing create) and must not be retried on
/// another ingress.
#[derive(Debug, Error, PartialEq)]
pub enum ForwardGroupError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden,
    #[error("service unavailable")]
    Unavailable,
    #[error("{0}")]
    Conflict(String),
}

impl From<IoError> for ForwardGroupError {
    fn from(_: IoError) -> Self {
        Self::Unavailable
    }
}

type CreatedGroup = (Group, GroupAuthorizationDocument);

/// Sends a User node's group create to a sync-eligible ingress. The device
/// never originates a realm administrative event: the ingress authorizes the
/// caller's own token and originates the event with the caller as actor.
#[derive(Debug, PartialEq)]
pub struct ForwardGroupCreateOperation {
    realm_id: RealmId,
    local: Option<NodeId>,
    auth_token: Option<AuthToken>,
    display_name: String,
    request_id: Ulid,
    frame: Vec<u8>,
    /// Reversed, so the next peer pops off the end.
    peers: Vec<NodeId>,
    asked: Option<NodeId>,
}

impl ForwardGroupCreateOperation {
    /// `request_id` spreads repeated creates of one name over the ingress nodes.
    pub fn new(
        realm_id: RealmId,
        local: Option<NodeId>,
        auth_token: AuthToken,
        display_name: String,
        request_id: Ulid,
    ) -> Self {
        Self {
            realm_id,
            local,
            auth_token: Some(auth_token),
            display_name,
            request_id,
            frame: Vec::new(),
            peers: Vec::new(),
            asked: None,
        }
    }

    fn choose_peers(&mut self, config: &RealmConfigDocument) -> Result<(), ForwardGroupError> {
        let mut subject = self.display_name.clone().into_bytes();
        subject.extend_from_slice(&self.request_id.to_bytes());
        let local = self.local;
        let mut peers = select_top_peers(
            (config.nodes.iter())
                .filter(|node| node.kind.is_sync_eligible())
                .filter_map(|node| NodeId::from_str(&node.node_id).ok())
                .filter(|peer| Some(*peer) != local),
            &subject,
            CREATE_PEER_LIMIT,
            |_| {},
        );
        peers.reverse();
        self.peers = peers;
        let message = MetadataTransportMessage::ForwardGroupCreate {
            auth_token: self.auth_token.take(),
            display_name: self.display_name.clone(),
        };
        self.frame = encode_message(&message).map_err(|_| ForwardGroupError::Unavailable)?;
        Ok(())
    }

    /// Retrying a create on another ingress could mint a second group, so only a request
    /// that never left this node moves on.
    fn ask_next(&mut self) -> Result<Next<CreatedGroup>, ForwardGroupError> {
        let peer = self.peers.pop().ok_or(ForwardGroupError::Unavailable)?;
        self.asked = Some(peer);
        let frame = self.frame.clone();
        Ok(Next::Call(peer_call(peer, frame, CREATE_ATTEMPT_TIMEOUT)))
    }

    fn answered(
        &mut self,
        peer: NodeId,
        reply: Result<MetadataTransportMessage, PeerRequestError>,
    ) -> Result<Next<CreatedGroup>, ForwardGroupError> {
        match reply {
            Ok(MetadataTransportMessage::ForwardedGroupCreated {
                group,
                authorization,
            }) => Ok(Next::Done((*group, *authorization))),
            Ok(MetadataTransportMessage::GroupCreateConflict { reason }) => {
                Err(ForwardGroupError::Conflict(reason))
            }
            Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Unauthorized,
            }) => Err(ForwardGroupError::Unauthorized),
            Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Forbidden,
            }) => Err(ForwardGroupError::Forbidden),
            Ok(MetadataTransportMessage::Reject(error)) => {
                warn!(%peer, %error, "Ingress rejected a forwarded group create");
                Err(ForwardGroupError::Unavailable)
            }
            Ok(_) => {
                warn!(%peer, "Ingress answered a forwarded group create unexpectedly");
                Err(ForwardGroupError::Unavailable)
            }
            Err(PeerRequestError::NotSent(error)) => {
                warn!(%peer, %error, "Failed to reach an ingress for a forwarded group create");
                self.ask_next()
            }
            Err(PeerRequestError::Failed(error)) => {
                warn!(%peer, %error, "Forwarded group create may have been applied");
                Err(ForwardGroupError::Unavailable)
            }
            Err(PeerRequestError::TimedOut) => {
                warn!(%peer, "Forwarded group create timed out on a reached ingress");
                Err(ForwardGroupError::Unavailable)
            }
        }
    }
}

impl Operation for ForwardGroupCreateOperation {
    type Output = CreatedGroup;
    type Error = ForwardGroupError;

    fn first(&mut self) -> Result<Next<CreatedGroup>, ForwardGroupError> {
        let read = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<CreatedGroup>, ForwardGroupError> {
        match self.asked.take() {
            None => {
                let config = result
                    .child::<RealmConfigDocument, GetConfigError>()?
                    .map_err(|_| ForwardGroupError::Unavailable)?;
                self.choose_peers(&config)?;
                self.ask_next()
            }
            Some(peer) => {
                let reply = peer_reply(peer, result)?;
                self.answered(peer, reply)
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum ApplyStage {
    Config,
    Caller(RealmConfigDocument),
    Admin(RealmConfigDocument, AuthContext),
    Create,
}

type ApplyTurn = Result<Next<MetadataTransportMessage>, IoError>;

fn unavailable() -> ApplyTurn {
    Ok(Next::Done(
        MetadataTransportMessage::ForwardedWriteUnavailable,
    ))
}

/// Originates a group create requested by a device. Authority is the caller's
/// forwarded token, checked exactly as the local HTTP handler would; a User
/// peer may only ever act for the owner its realm config binds it to.
#[derive(Debug, PartialEq)]
pub(crate) struct ApplyGroupCreateOperation {
    local: Option<(NodeId, RealmId)>,
    peer: NodeId,
    message: MetadataTransportMessage,
    stage: ApplyStage,
}

impl ApplyGroupCreateOperation {
    pub(crate) fn new(
        local: Option<(NodeId, RealmId)>,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> Self {
        Self {
            local,
            peer,
            message,
            stage: ApplyStage::Config,
        }
    }

    fn create(
        &mut self,
        config: &RealmConfigDocument,
        auth: &AuthContext,
        realm_admin: bool,
    ) -> ApplyTurn {
        let Some((node_id, realm_id)) = self.local else {
            return unavailable();
        };
        let MetadataTransportMessage::ForwardGroupCreate { display_name, .. } = &self.message
        else {
            return Ok(Next::Done(reject("unexpected group create message")));
        };
        let owner_cap = match realm_admin {
            true => None,
            false => config.quota.max_groups_for(&auth.user_id),
        };
        self.stage = ApplyStage::Create;
        let create = CreateGroupOperation::new(CreateGroupConfig {
            actor: Actor {
                node_id,
                user_id: auth.user_id,
                realm_id,
            },
            display_name: display_name.clone(),
            owner_cap,
        });
        Ok(Next::Call(IoCall::child(create)))
    }
}

impl Operation for ApplyGroupCreateOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> ApplyTurn {
        let Some((_, realm_id)) = self.local else {
            return unavailable();
        };
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(&mut self, result: IoResult) -> ApplyTurn {
        let Some((node_id, realm_id)) = self.local else {
            return unavailable();
        };
        match std::mem::replace(&mut self.stage, ApplyStage::Create) {
            ApplyStage::Config => {
                let Ok(config) = result.child::<RealmConfigDocument, GetConfigError>()? else {
                    return unavailable();
                };
                if !is_sync_eligible(&config, node_id) {
                    return unavailable();
                }
                self.stage = ApplyStage::Caller(config);
                let caller = ForwardedCallerOperation::new(self.peer, realm_id, &self.message);
                Ok(Next::Call(IoCall::child(caller)))
            }
            ApplyStage::Caller(config) => {
                let auth = match result.child::<AuthContext, ForwardAuthError>()? {
                    Ok(auth) => auth,
                    Err(error) => return Ok(Next::Done(forward_auth_error(error))),
                };
                if auth.path_restrictions.is_some() {
                    return Ok(Next::Done(MetadataTransportMessage::ForwardedWriteDenied {
                        error: WriteAuthError::Forbidden,
                    }));
                }
                let check = write_check(&auth, &format!("/{realm_id}/admin/groups"));
                self.stage = ApplyStage::Admin(config, auth);
                Ok(Next::Call(IoCall::child(check)))
            }
            ApplyStage::Admin(config, auth) => {
                let realm_admin = result.child::<(), AuthorizeError>()?.is_ok();
                self.create(&config, &auth, realm_admin)
            }
            ApplyStage::Create => Ok(Next::Done(
                match result.child::<CreatedGroup, CreateGroupError>()? {
                    Ok((group, authorization)) => {
                        MetadataTransportMessage::ForwardedGroupCreated {
                            group: Box::new(group),
                            authorization: Box::new(authorization),
                        }
                    }
                    Err(CreateGroupError::GroupLimitReached { limit }) => {
                        MetadataTransportMessage::GroupCreateConflict {
                            reason: format!("owned group limit reached ({limit})"),
                        }
                    }
                    Err(
                        CreateGroupError::StorageError(StorageError::TransactionConflict)
                        | CreateGroupError::PlacementFenced,
                    ) => MetadataTransportMessage::GroupCreateConflict {
                        reason: "concurrent group creation conflict; retry".to_string(),
                    },
                    Err(error) => reject(format!("group create failed: {error}")),
                },
            )),
        }
    }
}

#[cfg(test)]
#[path = "forward_tests.rs"]
mod tests;
