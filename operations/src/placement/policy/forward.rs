//! Routes a policy publication to a holder of its bucket when this node holds no replica.
//! The holder's create checks realm admin authority again for the forwarded caller.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::placement::policy::document::PlacementPolicyDocument;
use thiserror::Error;
use tracing::warn;

use crate::driver::{DriverContext, drive};
use crate::forward::authorize::forward_auth_error;
use crate::forward::transport::MetadataWriteError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::placement::policy::create::{
    CreatePolicyConfig, CreatePolicyError, CreatePolicyOperation,
};
use aruna_core::metadata::AuthToken;
use crate::forward::authorize::ForwardedCallerOperation;
use crate::forward::routing::ForwardHoldersOperation;

/// Why a routed publication did not commit. The local outcome and the delivery
/// outcome stay distinguishable, so a reused id is never reported as an
/// availability failure.
#[derive(Debug, Error)]
pub enum PolicyForwardError {
    #[error(transparent)]
    Create(#[from] CreatePolicyError),
    #[error(transparent)]
    Forward(#[from] MetadataWriteError),
}

/// Creates locally when this node holds the policy's bucket, otherwise at one of
/// the holders the create resolved.
pub async fn create_policy_routed(
    context: &Arc<DriverContext>,
    config: CreatePolicyConfig,
    auth_token: Option<AuthToken>,
) -> Result<PlacementPolicyDocument, PolicyForwardError> {
    let policy = config.policy.clone();
    let created_at_ms = config.created_at_ms;
    let holders = match drive(CreatePolicyOperation::new(config), context.as_ref()).await {
        Ok(document) => return Ok(document),
        Err(CreatePolicyError::NotHolder { holders }) => holders,
        Err(error) => return Err(error.into()),
    };
    let response = drive(ForwardHoldersOperation::new(context.node_and_realm().map(|(node_id, _)| node_id), &holders, &MetadataTransportMessage::ForwardCreatePolicy {
            auth_token,
            policy: Box::new(policy),
            created_at_ms,
        }, None, false), context).await.map_err(MetadataWriteError::from)?;
    match response {
        MetadataTransportMessage::PlacementPolicyCreated { document } => Ok(*document),
        other => Err(MetadataWriteError::Undeliverable(format!(
            "holder answered a policy publication with {}",
            crate::metadata::transport_message_kind(&other)
        ))
        .into()),
    }
}

/// Runs one forwarded publication on this holder. The caller's own realm-admin
/// authority is checked by the create operation, not asserted by the relay.
pub(crate) async fn apply_forwarded_policy(
    context: &Arc<DriverContext>,
    peer: aruna_core::NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let realm_id = *net_handle.realm_id();
    let auth = match drive(ForwardedCallerOperation::new(peer, realm_id, &message), context).await {
        Ok(auth) => auth,
        Err(error) => return forward_auth_error(error),
    };
    let MetadataTransportMessage::ForwardCreatePolicy {
        policy,
        created_at_ms,
        ..
    } = message
    else {
        return MetadataTransportMessage::Reject("unexpected policy publication".to_string());
    };
    if auth.realm_id != realm_id {
        return MetadataTransportMessage::ForwardedWriteDenied {
            error: crate::metadata::protocol::WriteAuthError::Forbidden,
        };
    }
    let config = CreatePolicyConfig {
        actor: Actor {
            node_id: net_handle.node_id(),
            user_id: auth.user_id,
            realm_id,
        },
        auth_context: AuthContext {
            user_id: auth.user_id,
            realm_id,
            path_restrictions: auth.path_restrictions,
            session: None,
        },
        policy: *policy,
        created_at_ms,
    };
    match drive(CreatePolicyOperation::new(config), context.as_ref()).await {
        Ok(document) => MetadataTransportMessage::PlacementPolicyCreated {
            document: Box::new(document),
        },
        Err(CreatePolicyError::Unauthorized) => MetadataTransportMessage::ForwardedWriteDenied {
            error: crate::metadata::protocol::WriteAuthError::Forbidden,
        },
        // Another holder may still be able to commit it.
        Err(CreatePolicyError::NotHolder { .. }) => {
            MetadataTransportMessage::ForwardedWriteUnavailable
        }
        Err(error) => {
            warn!(%error, "Forwarded policy publication failed");
            MetadataTransportMessage::Reject(error.to_string())
        }
    }
}
