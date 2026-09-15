use crate::driver::DriverContext;
use crate::driver::drive;
use crate::forward::authorize::authorize_forwarded_caller;
use crate::forward::authorize::authorize_write;
use crate::forward::authorize::forward_auth_error;
use crate::forward::authorize::is_sync_eligible;
use crate::forward::transport::RetryDisposition;
use crate::forward::transport::reject;
use crate::forward::transport::retry_disposition;
use crate::groups::create_group::CreateGroupConfig;
use crate::groups::create_group::CreateGroupError;
use crate::groups::create_group::CreateGroupOperation;
use crate::metadata::api::MetadataApiError;
use crate::metadata::protocol::MetadataAuthToken;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::protocol::MetadataWriteAuthError;
use crate::placement::process_placements::load_realm_config;
use crate::placement::selector::select_top_peers;
use aruna_core::NodeId;
use aruna_core::errors::StorageError;
use aruna_core::structs::Actor;
use aruna_core::structs::Group;
use aruna_core::structs::GroupAuthorizationDocument;
use aruna_core::structs::RealmId;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::timeout;
use tracing::warn;
use ulid::Ulid;

pub(super) const GROUP_CREATE_PEER_LIMIT: usize = 4;

pub(super) const GROUP_CREATE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Why a forwarded group create did not produce a group. `Conflict` is the
/// caller's problem (quota or a racing create) and must not be retried on
/// another ingress; everything else maps to the usual transport response.
#[derive(Debug, Error)]
pub enum ForwardGroupError {
    #[error(transparent)]
    Api(#[from] MetadataApiError),
    #[error("{0}")]
    Conflict(String),
}

/// Sends a User node's group create to a sync-eligible ingress. The device
/// never originates a realm administrative event: the ingress authorizes the
/// caller's own token and originates the event with the caller as actor.
pub async fn forward_group_create(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    auth_token: MetadataAuthToken,
    display_name: String,
) -> Result<(Group, GroupAuthorizationDocument), ForwardGroupError> {
    let Some(config) = load_realm_config(context, realm_id).await else {
        return Err(MetadataApiError::ServiceUnavailable.into());
    };
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return Err(MetadataApiError::ServiceUnavailable.into());
    };
    let local_node_id = context.net_handle.as_ref().map(|net| net.node_id());
    let mut subject = display_name.clone().into_bytes();
    subject.extend_from_slice(&Ulid::generate().to_bytes());
    let peers = select_top_peers(
        config
            .nodes
            .iter()
            .filter(|node| node.kind.is_sync_eligible())
            .filter_map(|node| NodeId::from_str(&node.node_id).ok())
            .filter(|peer| Some(*peer) != local_node_id),
        &subject,
        GROUP_CREATE_PEER_LIMIT,
        |_| {},
    );
    if peers.is_empty() {
        return Err(MetadataApiError::ServiceUnavailable.into());
    }
    let message = MetadataTransportMessage::ForwardGroupCreate {
        auth_token: Some(auth_token),
        display_name,
    };
    // Retrying a create on another ingress could mint a second group, so only a request
    // that never left this node moves on.
    for peer in peers {
        match timeout(
            GROUP_CREATE_ATTEMPT_TIMEOUT,
            metadata.request_forwarded_write(peer, message.clone()),
        )
        .await
        {
            Ok(Ok(MetadataTransportMessage::ForwardedGroupCreated {
                group,
                authorization,
            })) => return Ok((*group, *authorization)),
            Ok(Ok(MetadataTransportMessage::ForwardedGroupCreateConflict { reason })) => {
                return Err(ForwardGroupError::Conflict(reason));
            }
            Ok(Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: MetadataWriteAuthError::Unauthorized,
            })) => return Err(MetadataApiError::Unauthorized.into()),
            Ok(Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: MetadataWriteAuthError::Forbidden,
            })) => return Err(MetadataApiError::Forbidden.into()),
            Ok(Ok(MetadataTransportMessage::Reject(error))) => {
                warn!(%peer, %error, "Ingress rejected a forwarded group create");
                return Err(MetadataApiError::ServiceUnavailable.into());
            }
            Ok(Ok(_)) => {
                warn!(%peer, "Ingress answered a forwarded group create unexpectedly");
                return Err(MetadataApiError::ServiceUnavailable.into());
            }
            Ok(Err(error)) if retry_disposition(error.delivery()) == RetryDisposition::TryNext => {
                warn!(%peer, %error, "Failed to reach an ingress for a forwarded group create");
                continue;
            }
            Ok(Err(error)) => {
                warn!(%peer, %error, "Forwarded group create may have been applied");
                return Err(MetadataApiError::ServiceUnavailable.into());
            }
            Err(_) => {
                warn!(%peer, "Forwarded group create timed out on a reached ingress");
                return Err(MetadataApiError::ServiceUnavailable.into());
            }
        }
    }
    Err(MetadataApiError::ServiceUnavailable.into())
}

/// Originates a group create requested by a device. Authority is the caller's
/// forwarded token, checked exactly as the local HTTP handler would; a User
/// peer may only ever act for the owner its realm config binds it to.
pub(crate) async fn apply_group_create(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let Some(net_handle) = context.net_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let realm_id = *net_handle.realm_id();
    let Some(config) = load_realm_config(context, realm_id).await else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    if !is_sync_eligible(&config, net_handle.node_id()) {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    }
    let auth = match authorize_forwarded_caller(context, peer, realm_id, &message).await {
        Ok(auth) => auth,
        Err(error) => return forward_auth_error(error),
    };
    if auth.path_restrictions.is_some() {
        return MetadataTransportMessage::ForwardedWriteDenied {
            error: MetadataWriteAuthError::Forbidden,
        };
    }
    let MetadataTransportMessage::ForwardGroupCreate { display_name, .. } = message else {
        return reject("unexpected group create message");
    };
    let realm_admin = authorize_write(context, auth.clone(), format!("/{realm_id}/admin/groups"))
        .await
        .is_ok();
    let owner_cap = if realm_admin {
        None
    } else {
        config.quota.max_groups_for(&auth.user_id)
    };
    match drive(
        CreateGroupOperation::new(CreateGroupConfig {
            actor: Actor {
                node_id: net_handle.node_id(),
                user_id: auth.user_id,
                realm_id,
            },
            display_name,
            owner_cap,
        }),
        context.as_ref(),
    )
    .await
    {
        Ok((group, authorization)) => MetadataTransportMessage::ForwardedGroupCreated {
            group: Box::new(group),
            authorization: Box::new(authorization),
        },
        Err(CreateGroupError::OwnedGroupLimitReached { limit }) => {
            MetadataTransportMessage::ForwardedGroupCreateConflict {
                reason: format!("owned group limit reached ({limit})"),
            }
        }
        Err(
            CreateGroupError::StorageError(StorageError::TransactionConflict)
            | CreateGroupError::PlacementFenced,
        ) => MetadataTransportMessage::ForwardedGroupCreateConflict {
            reason: "concurrent group creation conflict; retry".to_string(),
        },
        Err(error) => reject(format!("group create failed: {error}")),
    }
}
