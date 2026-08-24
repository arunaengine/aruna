use aruna_core::NodeId;
use aruna_core::structs::{RealmConfigDocument, RealmId};
use aruna_core::types::UserId;
use thiserror::Error;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum RealmPeerError {
    #[error("realm config `{configured}` does not match requested realm `{requested}`")]
    RealmMismatch {
        configured: RealmId,
        requested: RealmId,
    },
    #[error("peer `{peer}` is not configured in realm `{realm_id}`")]
    NotConfigured { peer: NodeId, realm_id: RealmId },
    #[error("peer `{peer}` is not trusted for internal auth in realm `{realm_id}`")]
    NotTrusted { peer: NodeId, realm_id: RealmId },
}

/// How much a peer must be trusted for the request it carries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerTrust {
    /// Realm membership is enough.
    Member,
    /// Node-vouched auth, carrying the user it asserts. `None` asserts no user
    /// and admits infrastructure peers only.
    Vouched(Option<UserId>),
}

pub fn ensure_realm_peer(
    document: &RealmConfigDocument,
    peer: NodeId,
    realm_id: RealmId,
    require_internal_trust: bool,
) -> Result<(), RealmPeerError> {
    let trust = match require_internal_trust {
        true => PeerTrust::Vouched(None),
        false => PeerTrust::Member,
    };
    ensure_peer_trust(document, peer, realm_id, trust)
}

pub fn ensure_peer_trust(
    document: &RealmConfigDocument,
    peer: NodeId,
    realm_id: RealmId,
    trust: PeerTrust,
) -> Result<(), RealmPeerError> {
    if document.realm_id != realm_id {
        return Err(RealmPeerError::RealmMismatch {
            configured: document.realm_id,
            requested: realm_id,
        });
    }
    let node = document
        .nodes
        .iter()
        .find(|node| node.node_id == peer.to_string())
        .ok_or(RealmPeerError::NotConfigured { peer, realm_id })?;
    let PeerTrust::Vouched(vouched_for) = trust else {
        return Ok(());
    };
    // An owner-bound device may vouch for the owner its realm config names and
    // for nobody else; every other kind keeps the sync-eligibility gate.
    match node.kind.owner() {
        Some(owner) if vouched_for == Some(owner) => Ok(()),
        Some(_) => Err(RealmPeerError::NotTrusted { peer, realm_id }),
        None if node.kind.is_sync_eligible() => Ok(()),
        None => Err(RealmPeerError::NotTrusted { peer, realm_id }),
    }
}

#[cfg(test)]
mod tests {
    use super::{PeerTrust, RealmPeerError, ensure_peer_trust};
    use aruna_core::NodeId;
    use aruna_core::structs::{RealmConfigDocument, RealmId, RealmNodeKind};
    use aruna_core::types::UserId;
    use ulid::Ulid;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([9u8; 32])
    }

    fn config(kind: RealmNodeKind) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::default_for_realm(realm(), Vec::new());
        config.ensure_node(node(1), kind);
        config.ensure_node(node(2), RealmNodeKind::Server);
        config
    }

    #[test]
    fn device_vouches_for_owner() {
        // A device's own owner is the one identity it may speak for.
        let owner = UserId::local(Ulid::generate(), realm());
        let config = config(RealmNodeKind::User { owner });
        assert_eq!(
            ensure_peer_trust(&config, node(1), realm(), PeerTrust::Vouched(Some(owner))),
            Ok(())
        );
    }

    #[test]
    fn device_vouches_for_nobody() {
        let owner = UserId::local(Ulid::generate(), realm());
        let stranger = UserId::local(Ulid::generate(), realm());
        let config = config(RealmNodeKind::User { owner });
        for vouched in [Some(stranger), None] {
            assert_eq!(
                ensure_peer_trust(&config, node(1), realm(), PeerTrust::Vouched(vouched)),
                Err(RealmPeerError::NotTrusted {
                    peer: node(1),
                    realm_id: realm(),
                })
            );
        }
    }

    #[test]
    fn infra_keeps_internal_gate() {
        // Vouching changes nothing for an infrastructure peer, and an unknown
        // peer is still refused before any trust question.
        let owner = UserId::local(Ulid::generate(), realm());
        let config = config(RealmNodeKind::User { owner });
        assert_eq!(
            ensure_peer_trust(&config, node(2), realm(), PeerTrust::Vouched(Some(owner))),
            Ok(())
        );
        assert_eq!(
            ensure_peer_trust(&config, node(3), realm(), PeerTrust::Member),
            Err(RealmPeerError::NotConfigured {
                peer: node(3),
                realm_id: realm(),
            })
        );
    }

    #[test]
    fn member_ignores_kind() {
        let owner = UserId::local(Ulid::generate(), realm());
        let config = config(RealmNodeKind::User { owner });
        assert_eq!(
            ensure_peer_trust(&config, node(1), realm(), PeerTrust::Member),
            Ok(())
        );
    }
}
