use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_LOCATIONS_KEYSPACE, BLOB_VERSIONS_KEYSPACE, HASH_PATHS_INDEX_KEYSPACE,
};
use aruna_core::structs::{
    BackendLocation, BlobHeadKey, BlobVersion, CurrentVersionPointer, HashPathIndexKey, RealmId,
    VersionKey,
};
use aruna_core::types::{Effects, GroupId, NodeId, TxnId};
use byteview::ByteView;
use smallvec::smallvec;
use ulid::Ulid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeadAliasContext {
    pub realm_id: RealmId,
    pub group_id: GroupId,
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
}

impl HeadAliasContext {
    pub fn new(
        realm_id: RealmId,
        group_id: GroupId,
        node_id: NodeId,
        bucket: impl Into<String>,
        key: impl Into<String>,
    ) -> Self {
        Self {
            realm_id,
            group_id,
            node_id,
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    pub fn head_key(&self) -> BlobHeadKey {
        BlobHeadKey::new(self.bucket.clone(), self.key.clone())
    }

    pub fn hash_path_index_key(&self, blake3_hash: [u8; 32], version_id: Ulid) -> HashPathIndexKey {
        HashPathIndexKey::new(
            blake3_hash,
            version_id,
            self.realm_id,
            self.group_id,
            self.node_id,
            self.bucket.clone(),
            self.key.clone(),
        )
    }
}

pub fn read_blob_location_effect(blake3_hash: &[u8], txn_id: Option<TxnId>) -> Effect {
    Effect::Storage(StorageEffect::Read {
        key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
        key: ByteView::from(blake3_hash.to_vec()),
        txn_id,
    })
}

pub fn write_blob_location_effect(
    blake3_hash: [u8; 32],
    location: BackendLocation,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: BLOB_LOCATIONS_KEYSPACE.to_string(),
        key: ByteView::from(blake3_hash.to_vec()),
        value: location.to_bytes()?.into(),
        txn_id,
    }))
}

pub fn read_blob_head_effect(
    context: &HeadAliasContext,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Read {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key: context.head_key().to_bytes()?.into(),
        txn_id,
    }))
}

pub fn write_blob_head_effect(
    context: &HeadAliasContext,
    pointer: CurrentVersionPointer,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key: context.head_key().to_bytes()?.into(),
        value: pointer.to_bytes()?.into(),
        txn_id,
    }))
}

pub fn delete_blob_head_effect(
    context: &HeadAliasContext,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Delete {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key: context.head_key().to_bytes()?.into(),
        txn_id,
    }))
}

pub fn read_blob_version_effect(
    version_key: &VersionKey,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Read {
        key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
        key: version_key.to_bytes()?.into(),
        txn_id,
    }))
}

pub fn write_blob_version_effect(
    version_key: &VersionKey,
    version: &BlobVersion,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
        key: version_key.to_bytes()?.into(),
        value: version.to_bytes()?.into(),
        txn_id,
    }))
}

pub fn delete_blob_version_effect(
    version_key: &VersionKey,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Delete {
        key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
        key: version_key.to_bytes()?.into(),
        txn_id,
    }))
}

pub fn add_hash_path_index_effect(
    context: &HeadAliasContext,
    blake3_hash: [u8; 32],
    version_id: Ulid,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: HASH_PATHS_INDEX_KEYSPACE.to_string(),
        key: context
            .hash_path_index_key(blake3_hash, version_id)
            .to_bytes()?
            .into(),
        value: ByteView::from(Vec::<u8>::new()),
        txn_id,
    }))
}

pub fn delete_hash_path_index_effect(
    context: &HeadAliasContext,
    blake3_hash: [u8; 32],
    version_id: Ulid,
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Delete {
        key_space: HASH_PATHS_INDEX_KEYSPACE.to_string(),
        key: context
            .hash_path_index_key(blake3_hash, version_id)
            .to_bytes()?
            .into(),
        txn_id,
    }))
}

pub fn iter_hash_path_index_effect(
    blake3_hash: &[u8],
    txn_id: Option<TxnId>,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Iter {
        key_space: HASH_PATHS_INDEX_KEYSPACE.to_string(),
        prefix: Some(HashPathIndexKey::hash_prefix(blake3_hash)?.into()),
        start_after: None,
        limit: usize::MAX,
        txn_id,
    }))
}

pub fn build_head_transition_effects(
    context: &HeadAliasContext,
    new_pointer: Option<CurrentVersionPointer>,
    new_current_hash: Option<[u8; 32]>,
    txn_id: Option<TxnId>,
) -> Result<Effects, ConversionError> {
    let mut effects = smallvec![];

    match new_pointer.as_ref() {
        Some(pointer) => effects.push(write_blob_head_effect(context, pointer.clone(), txn_id)?),
        None => effects.push(delete_blob_head_effect(context, txn_id)?),
    }

    if let Some(new_hash) = new_current_hash {
        if let Some(pointer) = new_pointer.as_ref() {
            effects.push(add_hash_path_index_effect(
                context,
                new_hash,
                pointer.version_id,
                txn_id,
            )?);
        }
    }

    Ok(effects)
}

#[cfg(test)]
mod tests {
    use super::{
        HeadAliasContext, add_hash_path_index_effect, build_head_transition_effects,
        iter_hash_path_index_effect,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::structs::{CurrentVersionPointer, HashPathIndexKey, RealmId};
    use ulid::Ulid;

    fn alias_context() -> HeadAliasContext {
        HeadAliasContext::new(
            RealmId::from_bytes([1u8; 32]),
            Ulid::from_bytes([2u8; 16]),
            iroh::SecretKey::from_bytes(&[3u8; 32]).public(),
            "bucket",
            "path/file.txt",
        )
    }

    #[test]
    fn add_hash_path_index_effect_writes_structured_key_with_marker_value() {
        let context = alias_context();
        let version_id = Ulid::from_bytes([4u8; 16]);
        let effect = add_hash_path_index_effect(&context, [9u8; 32], version_id, None).unwrap();

        let Effect::Storage(StorageEffect::Write { key, value, .. }) = effect else {
            panic!("expected storage write effect");
        };

        let decoded_key = HashPathIndexKey::from_bytes(key.as_ref()).unwrap();
        assert_eq!(decoded_key.blake3_hash, [9u8; 32]);
        assert_eq!(decoded_key.version_id, version_id);
        assert!(value.is_empty());
    }

    #[test]
    fn iter_hash_path_index_effect_uses_hash_prefix() {
        let effect = iter_hash_path_index_effect(&[7u8; 32], None).unwrap();

        let Effect::Storage(StorageEffect::Iter { prefix, .. }) = effect else {
            panic!("expected storage iter effect");
        };

        let prefix = prefix.expect("expected prefix");
        let expected = aruna_core::structs::HashPathIndexKey::hash_prefix(&[7u8; 32]).unwrap();
        assert_eq!(prefix.as_ref(), expected.as_slice());
    }

    #[test]
    fn build_head_transition_effects_orders_head_write_before_new_insert() {
        let context = alias_context();
        let effects = build_head_transition_effects(
            &context,
            Some(CurrentVersionPointer::new_with_generation(
                Ulid::from_bytes([5u8; 16]),
                11,
            )),
            Some([6u8; 32]),
            None,
        )
        .unwrap();

        assert_eq!(effects.len(), 2);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Write { .. })
        ));
        assert!(matches!(
            effects[1],
            Effect::Storage(StorageEffect::Write { .. })
        ));
    }

    #[test]
    fn build_head_transition_effects_can_delete_head_without_new_alias() {
        let context = alias_context();
        let effects = build_head_transition_effects(&context, None, None, None).unwrap();

        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Delete { .. })
        ));
    }
}
