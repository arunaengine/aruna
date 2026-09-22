//! Authenticates group deletion coordination and signs durable emptiness confirmations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;
use std::time::SystemTime;

use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentOperation;
use aruna_core::structs::identity::group_delete::{
    GroupDeleteAction, GroupDeletePhase, GroupDeleteProof, GroupDeletionError,
};

use crate::driver::{DriverContext, drive};
use crate::forward::authorize::{ForwardAuthError, authorize_forwarded_caller, is_sync_eligible};
use crate::metadata::protocol::MetadataTransportMessage;
use crate::placement::process_placements::load_realm_config;

use super::commit::CommitOperation;
use super::prepare::PrepareOperation;

pub(crate) async fn apply_request(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let result = async {
        let net = context
            .net_handle
            .as_ref()
            .ok_or_else(|| GroupDeletionError::Unavailable("network handle is missing".into()))?;
        let MetadataTransportMessage::GroupDeletion { auth_token, action } = &message else {
            return Err(GroupDeletionError::Invalid("invalid group request".into()));
        };
        if let GroupDeleteAction::Start { group_id } = action {
            let auth = authorize_forwarded_caller(context, peer, *net.realm_id(), &message)
                .await
                .map_err(|error| match error {
                    ForwardAuthError::Unavailable(reason) => {
                        GroupDeletionError::Unavailable(reason)
                    }
                    _ => GroupDeletionError::Unauthorized,
                })?;
            Box::pin(super::coordinator::delete_group(
                context,
                auth,
                auth_token.clone(),
                *group_id,
            ))
            .await?;
            Ok(None)
        } else {
            apply_control(context, peer, action.clone()).await
        }
    }
    .await;
    MetadataTransportMessage::GroupDeletionResult { result }
}

pub(super) async fn apply_control(
    context: &Arc<DriverContext>,
    peer: NodeId,
    action: GroupDeleteAction,
) -> Result<Option<GroupDeleteProof>, GroupDeletionError> {
    let net = context
        .net_handle
        .as_ref()
        .ok_or_else(|| GroupDeletionError::Unavailable("network handle is missing".into()))?;
    let config = load_realm_config(context, *net.realm_id())
        .await
        .ok_or_else(|| GroupDeletionError::Unavailable("realm configuration is missing".into()))?;
    if !is_sync_eligible(&config, peer) {
        return Err(GroupDeletionError::Unauthorized);
    }
    match action {
        GroupDeleteAction::Prepare { plan } => {
            if peer != plan.coordinator
                || !plan.matches_config(&config)
                || super::coordinator::coordinator(&config)? != peer
            {
                return Err(GroupDeletionError::Unauthorized);
            }
            drive(
                PrepareOperation::new(
                    *plan.clone(),
                    GroupDeletePhase::Preparing,
                    net.node_id(),
                    SystemTime::now(),
                ),
                context.as_ref(),
            )
            .await?;
            let signature = net.sign(&plan.signing_bytes()?);
            Ok(Some(GroupDeleteProof {
                node_id: net.node_id(),
                signature,
            }))
        }
        GroupDeleteAction::Abort { plan } => {
            if peer != plan.coordinator || plan.realm_id != *net.realm_id() {
                return Err(GroupDeletionError::Unauthorized);
            }
            drive(
                PrepareOperation::new(
                    *plan,
                    GroupDeletePhase::Cancelled,
                    net.node_id(),
                    SystemTime::now(),
                ),
                context.as_ref(),
            )
            .await?;
            Ok(None)
        }
        GroupDeleteAction::Commit { event } => {
            let AdminDocumentOperation::GroupDeleted { certificate } = &event.op else {
                return Err(GroupDeletionError::Invalid(
                    "missing deletion certificate".into(),
                ));
            };
            if peer != event.origin_node_id
                || peer != certificate.plan.coordinator
                || event.actor.realm_id != *net.realm_id()
            {
                return Err(GroupDeletionError::Unauthorized);
            }
            drive(
                CommitOperation::new(
                    *certificate.clone(),
                    event.actor.clone(),
                    None,
                    Some(*event),
                ),
                context.as_ref(),
            )
            .await?;
            Ok(None)
        }
        GroupDeleteAction::Start { .. } => Err(GroupDeletionError::Invalid(
            "start requires caller authorization".into(),
        )),
    }
}
