use crate::auth::revoke_token::RevokeTokenAdmission;
use crate::auth::revoke_token::RevokeTokenConfig;
use crate::auth::revoke_token::RevokeTokenError;
use crate::auth::revoke_token::RevokeTokenOperation;
use crate::driver::DriverContext;
use crate::driver::drive;
use crate::forward::authorize::authorize_forwarded_caller;
use crate::forward::authorize::authorize_write;
use crate::forward::authorize::forward_auth_error;
use crate::forward::transport::reject;
use crate::metadata::api::MetadataApiError;
use crate::metadata::handle::MetadataRequestError;
use crate::metadata::protocol::AuthToken;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::protocol::WriteAuthError;
use crate::placement::process_placements::load_realm_config;
use crate::placement::selector::select_top_peers;
use aruna_core::NodeId;
use aruna_core::auth::bearer_token_hash;
use aruna_core::auth::valid_revocation_expiry;
use aruna_core::structs::identity::auth::Actor;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::time::unix_timestamp_secs;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tokio::time::timeout;
use tracing::warn;
use ulid::Ulid;

pub(super) const TOKEN_REVOKE_PEER_LIMIT: usize = 4;

pub(super) const TOKEN_REVOKE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);

pub(super) const TOKEN_REVOKE_DEADLINE: Duration = Duration::from_secs(15);

pub async fn forward_token_revoke(
    context: &Arc<DriverContext>,
    realm_id: RealmId,
    auth_token: AuthToken,
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
                error: WriteAuthError::Unauthorized,
            })) => return Err(MetadataApiError::Unauthorized),
            Ok(Ok(MetadataTransportMessage::ForwardedWriteDenied {
                error: WriteAuthError::Forbidden,
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
    if !matches!(auth_token, AuthToken::Bearer(_)) {
        return MetadataTransportMessage::ForwardedWriteDenied {
            error: WriteAuthError::Unauthorized,
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
            error: WriteAuthError::Forbidden,
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

#[cfg(test)]
mod tests {
    use super::TOKEN_REVOKE_DEADLINE;
    use super::TOKEN_REVOKE_PEER_LIMIT;
    use super::rank_revoke_peers;
    use super::run_revoke;
    use crate::metadata::api::MetadataApiError;
    use crate::metadata::handle::MetadataRequestError;
    use crate::metadata::protocol::AuthToken;
    use crate::metadata::protocol::MetadataTransportMessage;
    use aruna_core::NodeId;
    use aruna_core::auth::bearer_token_hash;
    use aruna_core::metadata::MetadataError;
    use tokio::time::Instant;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn revoke_message() -> MetadataTransportMessage {
        MetadataTransportMessage::ForwardTokenRevocation {
            auth_token: AuthToken::bearer("caller-token").unwrap(),
            token: "target-token".to_string(),
        }
    }

    #[tokio::test]
    async fn capacity_then_success() {
        let peers = [node(1), node(2)];
        let order = rank_revoke_peers(
            peers.iter().copied(),
            bearer_token_hash("target-token").as_bytes(),
        );
        let mut calls = Vec::new();
        let result = run_revoke(
            &order,
            revoke_message(),
            Instant::now() + TOKEN_REVOKE_DEADLINE,
            |peer, _| {
                calls.push(peer);
                std::future::ready(Ok(if peer == order[0] {
                    MetadataTransportMessage::ForwardedTokenRevocationCapacity
                } else {
                    MetadataTransportMessage::ForwardedTokenRevoked
                }))
            },
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(calls, order);
    }

    #[tokio::test]
    async fn retries_possible_send() {
        let peers = [node(1), node(2)];
        let order = rank_revoke_peers(
            peers.iter().copied(),
            bearer_token_hash("target-token").as_bytes(),
        );
        let mut calls = Vec::new();
        let result = run_revoke(
            &order,
            revoke_message(),
            Instant::now() + TOKEN_REVOKE_DEADLINE,
            |peer, _| {
                calls.push(peer);
                if peer == order[0] {
                    std::future::ready(Err(MetadataRequestError::possibly_sent(
                        MetadataError::HandleMissing,
                    )))
                } else {
                    std::future::ready(Ok(MetadataTransportMessage::ForwardedTokenRevoked))
                }
            },
        )
        .await;

        assert!(result.is_ok());
        assert_eq!(calls, order);
    }

    #[tokio::test]
    async fn all_capacity_unavailable() {
        let peers = [node(1), node(2)];
        let order = rank_revoke_peers(
            peers.iter().copied(),
            bearer_token_hash("target-token").as_bytes(),
        );
        let mut calls = Vec::new();
        let result = run_revoke(
            &order,
            revoke_message(),
            Instant::now() + TOKEN_REVOKE_DEADLINE,
            |peer, _| {
                calls.push(peer);
                std::future::ready(Ok(
                    MetadataTransportMessage::ForwardedTokenRevocationCapacity,
                ))
            },
        )
        .await;

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
        assert_eq!(calls, order);
    }

    #[tokio::test]
    async fn reject_stops_retry() {
        let peers = [node(1), node(2)];
        let order = rank_revoke_peers(
            peers.iter().copied(),
            bearer_token_hash("target-token").as_bytes(),
        );
        let mut calls = Vec::new();
        let result = run_revoke(
            &order,
            revoke_message(),
            Instant::now() + TOKEN_REVOKE_DEADLINE,
            |peer, _| {
                calls.push(peer);
                std::future::ready(Ok(MetadataTransportMessage::Reject(
                    "invalid token".to_string(),
                )))
            },
        )
        .await;

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
        assert_eq!(calls, vec![order[0]]);
    }

    #[tokio::test]
    async fn no_retry_loop() {
        let peer = node(1);
        let peers = vec![peer, peer];
        let mut calls = Vec::new();
        let result = run_revoke(
            &peers,
            revoke_message(),
            Instant::now() + TOKEN_REVOKE_DEADLINE,
            |peer, _| {
                calls.push(peer);
                std::future::ready(Ok(
                    MetadataTransportMessage::ForwardedTokenRevocationCapacity,
                ))
            },
        )
        .await;

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
        assert_eq!(calls, vec![peer]);
    }

    #[test]
    fn bounded_peer_order() {
        let peers = (1..=16).map(node).collect::<Vec<_>>();
        let reversed = peers.iter().copied().rev().collect::<Vec<_>>();
        let subject = bearer_token_hash("target-token");
        let first = rank_revoke_peers(peers.iter().copied(), subject.as_bytes());
        let second = rank_revoke_peers(reversed.iter().copied(), subject.as_bytes());

        assert_eq!(first, second);
        assert_eq!(first.len(), TOKEN_REVOKE_PEER_LIMIT);
        assert!(first.iter().all(|peer| peers.contains(peer)));
    }

    #[tokio::test]
    async fn deadline_stops_calls() {
        let peers = vec![node(1), node(2)];
        let mut calls = Vec::new();
        let result = run_revoke(&peers, revoke_message(), Instant::now(), |peer, _| {
            calls.push(peer);
            std::future::ready(Ok(MetadataTransportMessage::ForwardedTokenRevoked))
        })
        .await;

        assert!(matches!(result, Err(MetadataApiError::ServiceUnavailable)));
        assert!(calls.is_empty());
    }
}
