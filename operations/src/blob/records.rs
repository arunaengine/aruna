//! Builds the keys and storage effects for blob head, version, location and index rows.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::id::NodeId;
use aruna_core::keyspaces::{
    BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, BUCKET_RELEASE_KEYSPACE, COPY_OWNER_KEYSPACE,
    PATHS_INDEX_KEYSPACE,
};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{
    ArchiveKey, BlobHeadKey, BlobVersion, CopyOwner, CurrentVersionPointer, HashIndex, VersionKey,
};
use aruna_core::structs::storage::bucket_metadata::{ReleasePointer, release_key, release_marker};
use aruna_core::types::{Effects, GroupId, Key, Value};
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

    pub fn path_index_key(&self, blake3_hash: [u8; 32], version_id: Ulid) -> HashIndex {
        HashIndex::new(
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

pub fn owner_write_effect(owner: &CopyOwner) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: COPY_OWNER_KEYSPACE.to_string(),
        key: ByteView::from(owner.key()?),
        value: ByteView::from(Vec::new()),
        txn_id: None,
    }))
}

pub fn owners_scan_effect(archive: &ArchiveKey) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: COPY_OWNER_KEYSPACE.to_string(),
        prefix: Some(ByteView::from(CopyOwner::prefix(archive))),
        start: None,
        limit: 2,
        txn_id: None,
    })
}

/// Reads the location of an archive whose content hash is still pending.
pub fn write_head_effect(
    context: &HeadAliasContext,
    pointer: CurrentVersionPointer,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key: context.head_key().to_bytes()?.into(),
        value: pointer.to_bytes()?.into(),
        txn_id: None,
    }))
}

fn delete_head_effect(context: &HeadAliasContext) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Delete {
        key_space: BLOB_HEAD_KEYSPACE.to_string(),
        key: context.head_key().to_bytes()?.into(),
        txn_id: None,
    }))
}

pub fn write_version_effect(
    version_key: &VersionKey,
    version: &BlobVersion,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
        key: version_key.to_bytes()?.into(),
        value: version.to_bytes()?.into(),
        txn_id: None,
    }))
}

/// The release row a version's own tags declare and the bucket's release marker, for the same
/// transaction as the version, on every node that commits it, so a tagged version is never
/// missing its row. None for a version without a release tag.
pub fn release_rows(
    version_key: &VersionKey,
    version: &BlobVersion,
) -> Result<Vec<(String, Key, Value)>, ConversionError> {
    let Some(pointer) = ReleasePointer::of(&version_key.key, &version.metadata) else {
        return Ok(Vec::new());
    };
    let space = BUCKET_RELEASE_KEYSPACE.to_string();
    let key = release_key(&version_key.bucket, version_key.version_id).into();
    let marker = release_marker(&version_key.bucket).into();
    let id = version_key.version_id.to_bytes().to_vec().into();
    Ok(vec![
        (space.clone(), key, pointer.to_bytes()?.into()),
        (space, marker, id),
    ])
}

pub fn add_index_effect(
    context: &HeadAliasContext,
    blake3_hash: [u8; 32],
    version_id: Ulid,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Write {
        key_space: PATHS_INDEX_KEYSPACE.to_string(),
        key: context
            .path_index_key(blake3_hash, version_id)
            .to_bytes()?
            .into(),
        value: ByteView::from(Vec::<u8>::new()),
        txn_id: None,
    }))
}

pub fn iter_index_effect(
    blake3_hash: &[u8],
    start_after: Option<Key>,
) -> Result<Effect, ConversionError> {
    iter_hash_page(blake3_hash, start_after, usize::MAX)
}

pub fn iter_hash_page(
    blake3_hash: &[u8],
    start_after: Option<Key>,
    limit: usize,
) -> Result<Effect, ConversionError> {
    Ok(Effect::Storage(StorageEffect::Iter {
        key_space: PATHS_INDEX_KEYSPACE.to_string(),
        prefix: Some(HashIndex::hash_prefix(blake3_hash)?.into()),
        start: start_after.map(IterStart::After),
        limit,
        txn_id: None,
    }))
}

pub fn build_transition_effects(
    context: &HeadAliasContext,
    new_pointer: Option<CurrentVersionPointer>,
    new_current_hash: Option<[u8; 32]>,
) -> Result<Effects, ConversionError> {
    let mut effects = smallvec![];

    match new_pointer.as_ref() {
        Some(pointer) => effects.push(write_head_effect(context, pointer.clone())?),
        None => effects.push(delete_head_effect(context)?),
    }

    if let Some(new_hash) = new_current_hash
        && let Some(pointer) = new_pointer.as_ref()
    {
        effects.push(add_index_effect(context, new_hash, pointer.version_id)?);
    }

    Ok(effects)
}

#[cfg(test)]
mod pure_tests {
    use super::{
        HeadAliasContext, add_index_effect, build_transition_effects, iter_hash_page,
        iter_index_effect,
    };
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::storage::blob::{CurrentVersionPointer, HashIndex};
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
    fn adds_marker_entry() {
        let context = alias_context();
        let version_id = Ulid::from_bytes([4u8; 16]);
        let effect = add_index_effect(&context, [9u8; 32], version_id).unwrap();

        let Effect::Storage(StorageEffect::Write { key, value, .. }) = effect else {
            panic!("expected storage write effect");
        };

        let decoded_key = HashIndex::from_bytes(key.as_ref()).unwrap();
        assert_eq!(decoded_key.blake3_hash, [9u8; 32]);
        assert_eq!(decoded_key.version_id, version_id);
        assert!(value.is_empty());
    }

    #[test]
    fn iterates_hash_prefix() {
        let effect = iter_index_effect(&[7u8; 32], None).unwrap();

        let Effect::Storage(StorageEffect::Iter { prefix, .. }) = effect else {
            panic!("expected storage iter effect");
        };

        let prefix = prefix.expect("expected prefix");
        let expected =
            aruna_core::structs::storage::blob::HashIndex::hash_prefix(&[7u8; 32]).unwrap();
        assert_eq!(prefix.as_ref(), expected.as_slice());
    }

    #[test]
    fn checks_page_limit() {
        let Effect::Storage(StorageEffect::Iter { limit, .. }) =
            iter_hash_page(&[7u8; 32], None, 1024).unwrap()
        else {
            panic!("expected storage iter effect");
        };
        assert_eq!(limit, 1024);
    }

    #[test]
    fn writes_head_first() {
        let context = alias_context();
        let effects = build_transition_effects(
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
    fn deletes_without_alias() {
        let context = alias_context();
        let effects = build_transition_effects(&context, None, None).unwrap();

        assert_eq!(effects.len(), 1);
        assert!(matches!(
            effects[0],
            Effect::Storage(StorageEffect::Delete { .. })
        ));
    }
}
