use aruna_core::NodeId;
use aruna_core::structs::{RealmConfigDocument, RealmId, RealmNodeKind};
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

pub fn ensure_realm_peer(
    document: &RealmConfigDocument,
    peer: NodeId,
    realm_id: RealmId,
    require_internal_trust: bool,
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
    if require_internal_trust
        && !matches!(
            &node.kind,
            RealmNodeKind::Management | RealmNodeKind::Server
        )
    {
        return Err(RealmPeerError::NotTrusted { peer, realm_id });
    }
    Ok(())
}
