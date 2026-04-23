use aruna_core::effects::{DhtEffect, Effect, NetEffect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::S3_LOOKUP_KEYSPACE;
use aruna_core::structs::{BackendLocation, Location, LookupKey, RealmId};
use aruna_core::types::{NodeId, TxnId};

pub(super) fn hash_lookup_write_effect(
    location: BackendLocation,
    blake3: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    let key = LookupKey::from_blake3_hash(blake3)?.to_bytes()?;
    let value = Location::Real(location).to_bytes()?;

    Ok(Effect::Storage(StorageEffect::Write {
        key_space: S3_LOOKUP_KEYSPACE.to_string(),
        key: key.into(),
        value: value.into(),
        txn_id,
    }))
}

pub(super) fn dht_registration_effect(
    blake3: &[u8],
    local_realm_id: RealmId,
    local_node_id: NodeId,
) -> Result<Effect, ConversionError> {
    let key: [u8; 32] = blake3.try_into()?;

    Ok(Effect::Net(NetEffect::Dht(DhtEffect::Put {
        key,
        realm_id: local_realm_id,
        value: local_node_id.as_bytes().to_vec(),
        ttl: Default::default(),
    })))
}
