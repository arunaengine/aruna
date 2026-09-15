use crate::driver::DriverContext;
use crate::forward::authorize::is_sync_eligible;
use crate::forward::transport::reject;
use crate::metadata::api::MetadataApiError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::protocol::MetadataWriteAuthError;
use crate::placement::holds_placement;
use crate::placement::process_placements::load_realm_config;
use crate::sync::document_outbox::new_outbox_record;
use crate::sync::document_outbox::schedule_drain_effect;
use crate::sync::document_outbox::write_outbox_effect;
use aruna_core::NodeId;
use aruna_core::admin_documents::AdminDocumentEvent;
use aruna_core::document::DocumentSyncOutboxEvent;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::events::Event;
use aruna_core::events::StorageEvent;
use aruna_core::handle::Handle;
use aruna_core::structs::PlacementRef;
use aruna_core::structs::RealmConfigDocument;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::warn;

pub(super) const ADMIN_RELAY_PEER_LIMIT: usize = 3;

pub(super) const ADMIN_RELAY_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

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

#[cfg(test)]
mod tests {
    use super::RelayAdmission;
    use super::admit_relayed_admin;
    use crate::placement::resolve_shard_holders;
    use aruna_core::NodeId;
    use aruna_core::UserId;
    use aruna_core::admin_documents::AdminDocumentEvent;
    use aruna_core::structs::Actor;
    use aruna_core::structs::PlacementRef;
    use aruna_core::structs::PlacementStrategy;
    use aruna_core::structs::RealmConfigDocument;
    use aruna_core::structs::RealmId;
    use aruna_core::structs::RealmNodeKind;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn config_and_placement() -> (RealmConfigDocument, PlacementRef) {
        let mut config = RealmConfigDocument::new(RealmId::from_bytes([7u8; 32]), Vec::new(), 3);
        let strategy = PlacementStrategy {
            strategy_id: Ulid::from_bytes([4u8; 16]),
            name: "default".to_string(),
            replica_count: Some(2),
            distinct_locations: false,
            affinity: Vec::new(),
            shard_count: 64,
        };
        config.default_strategy_id = Some(strategy.strategy_id);
        config.strategies = vec![strategy.clone()];
        for seed in 1..=4u8 {
            config.ensure_node(node(seed), RealmNodeKind::Server);
        }
        (
            config,
            PlacementRef {
                strategy_id: strategy.strategy_id,
                shard: 9,
            },
        )
    }

    fn relay_fixture() -> (RealmConfigDocument, PlacementRef, NodeId) {
        let (mut config, placement) = config_and_placement();
        let device = node(9);
        let owner = UserId::nil(config.realm_id);
        config.ensure_node(device, RealmNodeKind::User { owner });
        let holder = resolve_shard_holders(&config, &placement)
            .into_iter()
            .next()
            .expect("fixture has holders");
        (config, placement, holder)
    }

    fn signed_event(
        config: &RealmConfigDocument,
        placement: &PlacementRef,
        origin_seed: u8,
    ) -> (AdminDocumentEvent, iroh::Signature) {
        let secret = iroh::SecretKey::from_bytes(&[origin_seed; 32]);
        let user_id = UserId::nil(config.realm_id);
        let event = AdminDocumentEvent {
            event_id: Ulid::from_bytes([5u8; 16]),
            target: aruna_core::admin_documents::AdminDocumentTarget::Group {
                group_id: Ulid::from_bytes([6u8; 16]),
            },
            origin_node_id: secret.public(),
            origin_seq: 1,
            observed: Default::default(),
            actor: Actor {
                node_id: secret.public(),
                user_id,
                realm_id: config.realm_id,
            },
            op: aruna_core::admin_documents::AdminDocumentOperation::GroupCreated {
                realm_id: config.realm_id,
                display_name: "Engineering".to_string(),
                owner: user_id,
            },
        };
        let signature = secret.sign(&event.signing_bytes(placement).expect("event signs"));
        (event, signature)
    }

    #[test]
    fn holder_accepts_relay() {
        let (config, placement, holder) = relay_fixture();
        let (event, signature) = signed_event(&config, &placement, 1);

        assert_eq!(
            admit_relayed_admin(&config, holder, node(2), &event, &placement, &signature),
            RelayAdmission::Accept
        );
    }

    #[test]
    fn relay_rejects_forged() {
        // A relay that rewrites the actor invalidates the origin signature.
        let (config, placement, holder) = relay_fixture();
        let (event, signature) = signed_event(&config, &placement, 1);
        let mut forged = event;
        forged.actor.user_id = UserId::local(Ulid::from_bytes([8u8; 16]), config.realm_id);

        assert!(matches!(
            admit_relayed_admin(&config, holder, node(2), &forged, &placement, &signature),
            RelayAdmission::Reject(reason)
                if reason == "relayed admin event is not signed by its origin"
        ));
    }

    #[test]
    fn rejects_device_origin() {
        // A relayed admin event originated by a device may never be published.
        let (config, placement, holder) = relay_fixture();
        let (event, signature) = signed_event(&config, &placement, 9);

        assert!(matches!(
            admit_relayed_admin(&config, holder, node(2), &event, &placement, &signature),
            RelayAdmission::Reject(reason)
                if reason == "relayed admin event origin may not publish"
        ));
    }

    #[test]
    fn rejects_device_peer() {
        // A device is not a relay: it may not hand an admin event to a holder.
        let (config, placement, holder) = relay_fixture();
        let (event, signature) = signed_event(&config, &placement, 1);

        assert_eq!(
            admit_relayed_admin(&config, holder, node(9), &event, &placement, &signature),
            RelayAdmission::Forbidden
        );
    }

    #[test]
    fn nonholder_defers_relay() {
        // Another holder can still take it, so this is unavailable, not a reject.
        let (config, placement, holder) = relay_fixture();
        let (event, signature) = signed_event(&config, &placement, 1);
        let non_holder = (1..=4u8)
            .map(node)
            .find(|candidate| {
                *candidate != holder
                    && !resolve_shard_holders(&config, &placement).contains(candidate)
            })
            .expect("fixture has a non-holder");

        assert_eq!(
            admit_relayed_admin(&config, non_holder, node(2), &event, &placement, &signature),
            RelayAdmission::Unavailable
        );
    }
}
