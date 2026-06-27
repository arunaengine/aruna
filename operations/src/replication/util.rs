use aruna_core::effects::{DhtEffect, Effect, NetEffect};
use aruna_core::errors::ConversionError;
use aruna_core::id::DhtKeyId;
use aruna_core::structs::RealmId;
use aruna_core::types::NodeId;

pub(super) fn dht_registration_effect(
    blake3: &[u8],
    local_realm_id: RealmId,
    local_node_id: NodeId,
) -> Result<Effect, ConversionError> {
    let key = DhtKeyId::from_bytes(blake3.try_into()?);

    Ok(Effect::Net(NetEffect::Dht(DhtEffect::Put {
        key,
        realm_id: local_realm_id,
        value: local_node_id.as_bytes().to_vec(),
        ttl: Default::default(),
    })))
}
