//! Routes a policy publication to a holder of its bucket when this node holds no replica.
//! The holder's create checks realm admin authority again for the forwarded caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::document::PlacementPolicyDocument;
use thiserror::Error;
use tracing::warn;

use crate::forward::authorize::{ForwardAuthError, ForwardedCallerOperation, forward_auth_error};
use crate::forward::routing::{ForwardHoldersOperation, HolderWriteError};
use crate::metadata::protocol::{MetadataTransportMessage, WriteAuthError};
use crate::metadata::transport_message_kind;
use crate::placement::policy::create::{
    CreatePolicyConfig, CreatePolicyError, CreatePolicyOperation,
};

/// Why a routed publication did not commit. The local outcome and the delivery
/// outcome stay distinguishable, so a reused id is never reported as an
/// availability failure.
#[derive(Debug, Error, PartialEq)]
pub enum PolicyForwardError {
    #[error(transparent)]
    Create(#[from] CreatePolicyError),
    #[error(transparent)]
    Forward(#[from] HolderWriteError),
}

impl From<IoError> for PolicyForwardError {
    fn from(error: IoError) -> Self {
        Self::Forward(error.into())
    }
}

type RoutedTurn = Result<Next<PlacementPolicyDocument>, PolicyForwardError>;

/// Creates locally when this node holds the policy's bucket, otherwise at one of
/// the holders the create resolved.
#[derive(Debug, PartialEq)]
pub struct CreateRoutedOperation {
    config: CreatePolicyConfig,
    auth_token: Option<AuthToken>,
    local: Option<NodeId>,
    forwarded: bool,
}

impl CreateRoutedOperation {
    pub fn new(
        config: CreatePolicyConfig,
        auth_token: Option<AuthToken>,
        local: Option<NodeId>,
    ) -> Self {
        Self {
            config,
            auth_token,
            local,
            forwarded: false,
        }
    }
}

impl Operation for CreateRoutedOperation {
    type Output = PlacementPolicyDocument;
    type Error = PolicyForwardError;

    fn first(&mut self) -> RoutedTurn {
        let create = CreatePolicyOperation::new(self.config.clone());
        Ok(Next::Call(IoCall::child(create)))
    }

    fn next(&mut self, result: IoResult) -> RoutedTurn {
        if self.forwarded {
            return match result.child::<MetadataTransportMessage, HolderWriteError>()?? {
                MetadataTransportMessage::PlacementPolicyCreated { document } => {
                    Ok(Next::Done(*document))
                }
                other => Err(HolderWriteError::Undeliverable(format!(
                    "holder answered a policy publication with {}",
                    transport_message_kind(&other)
                ))
                .into()),
            };
        }
        let holders = match result.child::<PlacementPolicyDocument, CreatePolicyError>()? {
            Ok(document) => return Ok(Next::Done(document)),
            Err(CreatePolicyError::NotHolder { holders }) => holders,
            Err(error) => return Err(error.into()),
        };
        let message = MetadataTransportMessage::ForwardCreatePolicy {
            auth_token: self.auth_token.take(),
            policy: Box::new(self.config.policy.clone()),
            created_at_ms: self.config.created_at_ms,
        };
        self.forwarded = true;
        let walk = ForwardHoldersOperation::new(self.local, &holders, &message, None, false);
        Ok(Next::Call(IoCall::child(walk)))
    }
}

fn denied() -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardedWriteDenied {
        error: WriteAuthError::Forbidden,
    }
}

type ApplyTurn = Result<Next<MetadataTransportMessage>, IoError>;

/// Runs one forwarded publication on this holder. The caller's own realm-admin
/// authority is checked by the create operation, not asserted by the relay.
#[derive(Debug, PartialEq)]
pub(crate) struct ApplyPolicyOperation {
    local: Option<(NodeId, RealmId)>,
    peer: NodeId,
    message: MetadataTransportMessage,
    creating: bool,
}

impl ApplyPolicyOperation {
    pub(crate) fn new(
        local: Option<(NodeId, RealmId)>,
        peer: NodeId,
        message: MetadataTransportMessage,
    ) -> Self {
        Self {
            local,
            peer,
            message,
            creating: false,
        }
    }

    fn create(&mut self, node_id: NodeId, realm_id: RealmId, auth: AuthContext) -> ApplyTurn {
        let MetadataTransportMessage::ForwardCreatePolicy {
            policy,
            created_at_ms,
            ..
        } = &self.message
        else {
            return Ok(Next::Done(MetadataTransportMessage::Reject(
                "unexpected policy publication".to_string(),
            )));
        };
        if auth.realm_id != realm_id {
            return Ok(Next::Done(denied()));
        }
        let config = CreatePolicyConfig {
            actor: Actor {
                node_id,
                user_id: auth.user_id,
                realm_id,
            },
            auth_context: AuthContext {
                user_id: auth.user_id,
                realm_id,
                path_restrictions: auth.path_restrictions,
                session: None,
            },
            policy: (**policy).clone(),
            created_at_ms: *created_at_ms,
        };
        self.creating = true;
        Ok(Next::Call(IoCall::child(CreatePolicyOperation::new(
            config,
        ))))
    }
}

impl Operation for ApplyPolicyOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> ApplyTurn {
        let Some((_, realm_id)) = self.local else {
            return Ok(Next::Done(
                MetadataTransportMessage::ForwardedWriteUnavailable,
            ));
        };
        let caller = ForwardedCallerOperation::new(self.peer, realm_id, &self.message);
        Ok(Next::Call(IoCall::child(caller)))
    }

    fn next(&mut self, result: IoResult) -> ApplyTurn {
        let Some((node_id, realm_id)) = self.local else {
            return Ok(Next::Done(
                MetadataTransportMessage::ForwardedWriteUnavailable,
            ));
        };
        if !self.creating {
            return match result.child::<AuthContext, ForwardAuthError>()? {
                Ok(auth) => self.create(node_id, realm_id, auth),
                Err(error) => Ok(Next::Done(forward_auth_error(error))),
            };
        }
        Ok(Next::Done(
            match result.child::<PlacementPolicyDocument, CreatePolicyError>()? {
                Ok(document) => MetadataTransportMessage::PlacementPolicyCreated {
                    document: Box::new(document),
                },
                Err(CreatePolicyError::Unauthorized) => denied(),
                // Another holder may still be able to commit it.
                Err(CreatePolicyError::NotHolder { .. }) => {
                    MetadataTransportMessage::ForwardedWriteUnavailable
                }
                Err(error) => {
                    warn!(%error, "Forwarded policy publication failed");
                    MetadataTransportMessage::Reject(error.to_string())
                }
            },
        ))
    }
}
