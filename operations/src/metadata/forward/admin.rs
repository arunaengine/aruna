use crate::auth::revoke_token::RevokeTokenAdmission;
use crate::auth::revoke_token::RevokeTokenConfig;
use crate::auth::revoke_token::RevokeTokenError;
use crate::auth::revoke_token::RevokeTokenOperation;
use crate::driver::DriverContext;
use crate::driver::drive;
use crate::groups::create_group::CreateGroupConfig;
use crate::groups::create_group::CreateGroupError;
use crate::groups::create_group::CreateGroupOperation;
use crate::metadata::api::MetadataApiError;
use crate::metadata::handle::MetadataRequestError;
use crate::metadata::protocol::MetadataAuthToken;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::protocol::MetadataWriteAuthError;
use crate::placement::holds_placement;
use crate::placement::process_placements::load_realm_config;
use crate::placement::selector::select_top_peers;
use crate::sync::document_outbox::new_outbox_record;
use crate::sync::document_outbox::schedule_drain_effect;
use crate::sync::document_outbox::write_outbox_effect;
use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentEvent;
use aruna_core::auth::bearer_token_hash;
use aruna_core::auth::valid_revocation_expiry;
use aruna_core::document::DocumentSyncOutboxEvent;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::errors::StorageError;
use aruna_core::events::Event;
use aruna_core::events::StorageEvent;
use aruna_core::structs::Actor;
use aruna_core::structs::AuthContext;
use aruna_core::structs::Group;
use aruna_core::structs::GroupAuthorizationDocument;
use aruna_core::structs::PlacementRef;
use aruna_core::structs::RealmConfigDocument;
use aruna_core::structs::RealmId;
use aruna_core::time::unix_timestamp_secs;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;
use tokio::time::timeout;
use tracing::warn;
use ulid::Ulid;

use super::authorize::authorize_forwarded_caller;
use super::authorize::authorize_write;
use super::authorize::forward_auth_error;
use super::authorize::is_sync_eligible;
use super::transport::RetryDisposition;
use super::transport::reject;
use super::transport::retry_disposition;
use aruna_core::handle::Handle;
use std::str::FromStr;

pub(super) const TOKEN_REVOKE_PEER_LIMIT: usize = 4;

pub(super) const ADMIN_RELAY_PEER_LIMIT: usize = 3;

pub(super) const ADMIN_RELAY_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) const TOKEN_REVOKE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);

pub(super) const TOKEN_REVOKE_DEADLINE: Duration = Duration::from_secs(15);

pub async fn forward_token_revoke(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    auth_token: MetadataAuthToken,
    token: String,
) -> Result<(), MetadataApiError> {
    let Some(config) = load_realm_config(context, realm_id).await else {
        return Err(MetadataApiError::ServiceUnavailable);
    };
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return Err(MetadataApiError::ServiceUnavailable);
    };
    let local_node_id = context.net_handle.as_ref().map(|net| net.node_id());
    let mut subject = bearer_token_hash(&token).into_bytes();
    subject.extend_from_slice(&Ulid::generate().to_bytes());
    let peers = rank_revoke_peers(
        config
            .nodes
            .iter()
            .filter(|node| node.kind.is_sync_eligible())
            .filter_map(|node| NodeId::from_str(&node.node_id).ok())
            .filter(|peer| Some(*peer) != local_node_id),
        &subject,
    );
    if peers.is_empty() {
        return Err(MetadataApiError::ServiceUnavailable);
    }

    let message = MetadataTransportMessage::ForwardTokenRevocation { auth_token, token };
    run_revoke(
        &peers,
        message,
        Instant::now() + TOKEN_REVOKE_DEADLINE,
        |peer, message| metadata.request_forwarded_write(peer, message),
    )
    .await
}

pub(super) async fn run_revoke<F, Fut>(
    peers: &[NodeId],
    message: MetadataTransportMessage,
    deadline: Instant,
    mut request: F,
) -> Result<(), MetadataApiError>
where
    F: FnMut(NodeId, MetadataTransportMessage) -> Fut,
    Fut: Future<Output = Result<MetadataTransportMessage, MetadataRequestError>>,
{
    let mut seen = Vec::with_capacity(TOKEN_REVOKE_PEER_LIMIT);
    for peer in peers.iter().copied() {
        if seen.len() >= TOKEN_REVOKE_PEER_LIMIT {
            break;
        }
        if seen.contains(&peer) {
            continue;
        }
        seen.push(peer);
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt = remaining.min(TOKEN_REVOKE_ATTEMPT_TIMEOUT);
        match timeout(attempt, request(peer, message.clone())).await {
            Err(_) => {
                warn!(%peer, "Token revocation forwarding attempt timed out");
                continue;
            }
            Ok(Ok(MetadataTransportMessage::ForwardedTokenRevoked)) => return Ok(()),
            Ok(Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: MetadataWriteAuthError::Unauthorized,
            })) => return Err(MetadataApiError::Unauthorized),
            Ok(Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: MetadataWriteAuthError::Forbidden,
            })) => return Err(MetadataApiError::Forbidden),
            Ok(Ok(MetadataTransportMessage::ForwardedWriteUnavailable))
            | Ok(Ok(MetadataTransportMessage::ForwardedTokenRevocationCapacity)) => continue,
            Ok(Ok(MetadataTransportMessage::Reject(error))) => {
                warn!(%peer, %error, "Peer rejected a forwarded token revocation");
                return Err(MetadataApiError::ServiceUnavailable);
            }
            Ok(Ok(response)) => {
                warn!(%peer, response = ?crate::metadata::handle::transport_message_kind(&response), "Peer returned an unexpected token revocation response");
                return Err(MetadataApiError::ServiceUnavailable);
            }
            Ok(Err(error)) => {
                // Revocation is keyed by token hash, so an ambiguous write is safe to replay.
                warn!(%peer, %error, "Failed to forward a token revocation");
            }
        }
    }
    Err(MetadataApiError::ServiceUnavailable)
}

pub(super) fn rank_revoke_peers(
    peers: impl IntoIterator<Item = NodeId>,
    subject: &[u8],
) -> Vec<NodeId> {
    select_top_peers(peers, subject, TOKEN_REVOKE_PEER_LIMIT, |_| {})
}

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
    let peers = rank_revoke_peers(
        config
            .nodes
            .iter()
            .filter(|node| node.kind.is_sync_eligible())
            .filter_map(|node| NodeId::from_str(&node.node_id).ok())
            .filter(|peer| Some(*peer) != local_node_id),
        &subject,
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
            ADMIN_RELAY_ATTEMPT_TIMEOUT,
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

/// Hands an origin-signed administrative envelope to a holder of its shard: the
/// origin holds none of that shard so cannot publish; the holder republishes the
/// exact envelope and receivers still authorize the origin, never the relay.
pub async fn relay_admin_event(
    context: &Arc<DriverContext>,
    holders: &[NodeId],
    target: DocumentSyncTarget,
    event: Box<AdminDocumentEvent>,
    placement: PlacementRef,
    origin_signature: iroh::Signature,
) -> Result<(), MetadataApiError> {
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return Err(MetadataApiError::ServiceUnavailable);
    };
    let local_node_id = context.net_handle.as_ref().map(|net| net.node_id());
    let message = MetadataTransportMessage::ForwardAdminEvent {
        target,
        event,
        placement,
        origin_signature,
    };
    for peer in holders
        .iter()
        .copied()
        .filter(|peer| Some(*peer) != local_node_id)
        .take(ADMIN_RELAY_PEER_LIMIT)
    {
        match timeout(
            ADMIN_RELAY_ATTEMPT_TIMEOUT,
            metadata.request_forwarded_write(peer, message.clone()),
        )
        .await
        {
            Ok(Ok(MetadataTransportMessage::ForwardedAdminEventQueued)) => return Ok(()),
            Ok(Ok(MetadataTransportMessage::Reject(error))) => {
                // A rejection is a verdict on the envelope, not on this peer.
                warn!(%peer, %error, "Holder rejected a relayed admin event");
                return Err(MetadataApiError::ServiceUnavailable);
            }
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => continue,
        }
    }
    Err(MetadataApiError::ServiceUnavailable)
}

/// Whether this node may take custody of a relayed administrative envelope.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RelayAdmission {
    Accept,
    /// The sending peer has no business relaying realm administration.
    Forbidden,
    /// The envelope itself is invalid; no other holder would accept it either.
    Reject(String),
    /// This node cannot publish the record, but another holder can.
    Unavailable,
}

/// Admission decision for a relayed envelope. Authority is the origin
/// signature, never the relaying peer and never a caller token; this node only
/// proves the envelope is sound and that it can publish it at all.
pub(crate) fn admit_relayed_admin(
    config: &RealmConfigDocument,
    local_node_id: NodeId,
    peer: NodeId,
    event: &AdminDocumentEvent,
    placement: &PlacementRef,
    origin_signature: &iroh::Signature,
) -> RelayAdmission {
    if !is_sync_eligible(config, peer) {
        return RelayAdmission::Forbidden;
    }
    if !is_sync_eligible(config, event.origin_node_id) {
        return RelayAdmission::Reject("relayed admin event origin may not publish".to_string());
    }
    if event.actor.realm_id != config.realm_id || event.origin_node_id != event.actor.node_id {
        return RelayAdmission::Reject("relayed admin event identity does not match".to_string());
    }
    if !event.origin_signed(placement, origin_signature) {
        return RelayAdmission::Reject(
            "relayed admin event is not signed by its origin".to_string(),
        );
    }
    if !holds_placement(config, placement, local_node_id) {
        return RelayAdmission::Unavailable;
    }
    RelayAdmission::Accept
}

/// Accepts a relayed administrative envelope and takes custody of publishing it.
pub(crate) async fn apply_admin_relay(
    context: &Arc<DriverContext>,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> MetadataTransportMessage {
    let MetadataTransportMessage::ForwardAdminEvent {
        target,
        event,
        placement,
        origin_signature,
    } = message
    else {
        return reject("unexpected admin relay message");
    };
    let Some(net_handle) = context.net_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let Some(config) = load_realm_config(context, *net_handle.realm_id()).await else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    match admit_relayed_admin(
        &config,
        net_handle.node_id(),
        peer,
        &event,
        &placement,
        &origin_signature,
    ) {
        RelayAdmission::Accept => {}
        RelayAdmission::Forbidden => {
            return MetadataTransportMessage::ForwardedWriteDenied {
                error: MetadataWriteAuthError::Forbidden,
            };
        }
        RelayAdmission::Reject(reason) => return reject(reason),
        RelayAdmission::Unavailable => {
            return MetadataTransportMessage::ForwardedWriteUnavailable;
        }
    }
    // The relay never mints a genesis for another origin's document.
    let record = new_outbox_record(
        net_handle.node_id(),
        target,
        Vec::new(),
        DocumentSyncOutboxEvent::relayed_admin(*event, origin_signature),
        placement,
        false,
    );
    let effect = match write_outbox_effect(&record) {
        Ok(effect) => effect,
        Err(error) => return reject(format!("relayed admin event does not encode: {error}")),
    };
    match context.storage_handle.send_effect(effect).await {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => {
            warn!(event = ?other, "Failed to persist a relayed admin event");
            return MetadataTransportMessage::ForwardedWriteUnavailable;
        }
    }
    if let Some(task_handle) = context.task_handle.as_ref()
        && let Event::Task(aruna_core::task::TaskEvent::Error { message, .. }) =
            task_handle.send_effect(schedule_drain_effect()).await
    {
        warn!(%message, "Failed to schedule the drain for a relayed admin event");
    }
    MetadataTransportMessage::ForwardedAdminEventQueued
}

pub(crate) async fn apply_token_revoke(
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
    let local_node = config
        .nodes
        .iter()
        .find(|node| node.node_id == net_handle.node_id().to_string());
    if !local_node.is_some_and(|node| node.kind.is_sync_eligible()) {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    }
    let Some(metadata) = context.metadata_handle.as_ref() else {
        return MetadataTransportMessage::ForwardedWriteUnavailable;
    };
    let MetadataTransportMessage::ForwardTokenRevocation { auth_token, .. } = &message else {
        return reject("unexpected token revocation message");
    };
    if !matches!(auth_token, MetadataAuthToken::Bearer(_)) {
        return MetadataTransportMessage::ForwardedWriteDenied {
            error: MetadataWriteAuthError::Unauthorized,
        };
    }
    let auth = match authorize_forwarded_caller(context, peer, realm_id, &message).await {
        Ok(auth) => auth,
        Err(error) => return forward_auth_error(error),
    };
    let MetadataTransportMessage::ForwardTokenRevocation { token, .. } = message else {
        return reject("unexpected token revocation message");
    };
    let claims = match metadata.claims_for_revocation(&token).await {
        Ok(claims) => claims,
        Err(error) => return reject(format!("invalid token revocation target: {error}")),
    };
    let expires_at = claims.exp;
    let now = unix_timestamp_secs();
    if !valid_revocation_expiry(expires_at, now) {
        return reject("token revocation expiry is outside the supported window");
    }
    let subject: AuthContext = match claims.try_into() {
        Ok(subject) => subject,
        Err(error) => return reject(format!("invalid token revocation subject: {error}")),
    };
    if subject.realm_id != realm_id {
        return MetadataTransportMessage::ForwardedWriteDenied {
            error: MetadataWriteAuthError::Forbidden,
        };
    }
    if auth.user_id != subject.user_id
        && let Err(error) = authorize_write(
            context,
            auth.clone(),
            format!("/{realm_id}/admin/u/{}", subject.user_id),
        )
        .await
    {
        return forward_auth_error(error);
    }
    match drive(
        RevokeTokenOperation::new(RevokeTokenConfig {
            actor: Actor {
                node_id: net_handle.node_id(),
                user_id: auth.user_id,
                realm_id,
            },
            token_hash: bearer_token_hash(&token),
            expires_at,
            token_owner: subject.user_id,
            admission: if auth.user_id == subject.user_id {
                RevokeTokenAdmission::SelfService
            } else {
                RevokeTokenAdmission::Privileged
            },
            now,
        }),
        context.as_ref(),
    )
    .await
    {
        Ok(_) => MetadataTransportMessage::ForwardedTokenRevoked,
        Err(RevokeTokenError::CapacityReached) => {
            MetadataTransportMessage::ForwardedTokenRevocationCapacity
        }
        Err(error) => reject(format!("token revocation failed: {error}")),
    }
}
