use crate::replication::protocol::LocationSummaryRequest;
use aruna_core::structs::{AuthContext, RealmId};
use aruna_core::types::{NodeId, UserId};
use ulid::Ulid;

pub(crate) fn realm_id() -> RealmId {
    RealmId::from_bytes([1u8; 32])
}

pub(crate) fn node_id(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(crate) fn auth() -> AuthContext {
    AuthContext {
        user_id: UserId::nil(realm_id()),
        realm_id: realm_id(),
        path_restrictions: None,
        session: None,
    }
}

pub(crate) fn request(version_id: Option<Ulid>) -> LocationSummaryRequest {
    LocationSummaryRequest {
        realm_id: realm_id(),
        bucket: "raw".to_string(),
        key: "run1.tar".to_string(),
        version_id,
        auth_context: auth(),
    }
}
