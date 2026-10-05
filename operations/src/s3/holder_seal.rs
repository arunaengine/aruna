//! Seals missing bucket key copies for a holder who just published a user key, in every
//! encrypted bucket on this node whose active key generation is unlocked.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::driver::{DriverContext, drive};
use crate::s3::bucket::seal_missing::{SealMissingError, SealMissingInput, SealMissingOperation};
use aruna_core::NodeId;
use aruna_core::effects::{IterStart, StorageEffect};
use aruna_core::errors::{BlobError, StorageError};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{BUCKET_ENCRYPTION_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::user::vault::UserKeyRecord;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{BucketEncryption, BucketKeyError};
use aruna_core::structs::storage::holders::KeyLookup;
use aruna_core::types::Key;
use std::collections::BTreeMap;

const PAGE: usize = 256;

/// Seals copies for the publisher of `record`; returns the buckets that sealed one. A locked
/// bucket is skipped: its next unlock seals the missing copies.
pub async fn seal_published(
    context: &DriverContext,
    realm_id: RealmId,
    node_id: NodeId,
    record: &UserKeyRecord,
) -> Result<Vec<String>, StorageError> {
    let mut sealed = Vec::new();
    let mut start = None;
    loop {
        let (rows, next) = encrypted_page(context, start.take()).await?;
        for (bucket, settings) in rows {
            let Some(key) = settings.active_key() else {
                continue;
            };
            let Some(info) = bucket_info(context, &bucket).await? else {
                continue;
            };
            let input = SealMissingInput {
                bucket: bucket.clone(),
                group_id: info.group_id,
                realm_id,
                node_id,
                key,
                lookups: BTreeMap::from([(record.user_id, KeyLookup::Keys(vec![record.clone()]))]),
            };
            match drive(SealMissingOperation::new(input), context).await {
                Ok(users) if users.contains(&record.user_id) => sealed.push(bucket),
                Ok(_) => {}
                Err(SealMissingError::Blob(BlobError::BucketKey(BucketKeyError::Locked(_)))) => {}
                Err(error) => {
                    tracing::warn!(%bucket, %error, "could not seal a copy for a new user key")
                }
            }
        }
        match next {
            Some(next) => start = Some(next),
            None => return Ok(sealed),
        }
    }
}

type SettingsPage = (Vec<(String, BucketEncryption)>, Option<Key>);

async fn encrypted_page(
    context: &DriverContext,
    start: Option<Key>,
) -> Result<SettingsPage, StorageError> {
    let iter = StorageEffect::Iter {
        key_space: BUCKET_ENCRYPTION_KEYSPACE.to_string(),
        prefix: None,
        start: start.map(IterStart::After),
        limit: PAGE,
        txn_id: None,
    };
    let (values, next) = match context.storage_handle.send_storage_effect(iter).await {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after,
        }) => (values, next_start_after),
        Event::Storage(StorageEvent::Error { error }) => return Err(error),
        _ => {
            return Err(StorageError::ReadError(
                "unexpected settings scan".to_string(),
            ));
        }
    };
    let rows = values
        .into_iter()
        .filter_map(|(key, value)| {
            let bucket = String::from_utf8(key.to_vec()).ok()?;
            let settings = BucketEncryption::from_bytes(&value).ok()?;
            settings.is_encrypted().then_some((bucket, settings))
        })
        .collect();
    Ok((rows, next))
}

async fn bucket_info(
    context: &DriverContext,
    bucket: &str,
) -> Result<Option<BucketInfo>, StorageError> {
    let read = StorageEffect::Read {
        key_space: S3_BUCKET_KEYSPACE.to_string(),
        key: bucket.as_bytes().to_vec().into(),
        txn_id: None,
    };
    match context.storage_handle.send_storage_effect(read).await {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => {
            Ok(value.and_then(|value| BucketInfo::from_bytes(&value).ok()))
        }
        Event::Storage(StorageEvent::Error { error }) => Err(error),
        _ => Err(StorageError::ReadError(
            "unexpected bucket read".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::bucket::create::CreateBucketOperation;
    use crate::s3::bucket::encryption::{EnableEncryptionOperation, EnableInput};
    use crate::s3::bucket::key_install::{InstallInput, InstallKeyOperation};
    use aruna_blob::blob::BlobHandler;
    use aruna_core::UserId;
    use aruna_core::compute::SecretBytes;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, KEY_COPY_KEYSPACE};
    use aruna_core::node_vault::NodeVaultKey;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::blob::{Backend, BackendConfig};
    use aruna_core::structs::storage::encryption::{
        BlockCipher, BlockKeys, EncryptionMode, public_key_of,
    };
    use aruna_core::structs::storage::format::Compression;
    use std::collections::HashMap;
    use std::time::SystemTime;
    use ulid::Ulid;

    fn key_record(user_id: UserId, seed: u8, node_id: NodeId) -> UserKeyRecord {
        let public_key = public_key_of(&SecretBytes::new(vec![seed; 32])).unwrap();
        UserKeyRecord {
            user_id,
            record_id: Ulid::from_bytes([seed; 16]),
            key_id: format!("slot-{seed}"),
            public_key,
            fingerprint: aruna_core::vault_format::key_fingerprint(&public_key),
            has_recovery: true,
            node_id,
            placement: PlacementRef::NIL,
            created_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn seals_new_admin_key() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_str().unwrap();
        let storage = aruna_storage::FjallStorage::open(root).unwrap();
        storage.open_vault(NodeVaultKey::random());
        let net = aruna_net::NetHandle::new(aruna_net::NetConfig::default(), storage.clone())
            .await
            .unwrap();
        let config = BackendConfig {
            backend_type: Backend::FileSystem,
            bucket_prefix: Some("aruna_".to_string()),
            max_bucket_size: Some(100_000),
            multipart_bucket: Some("multipart".to_string()),
            root: format!("{root}/blobs"),
            service_config: HashMap::new(),
            timeouts: Default::default(),
        };
        let blob = BlobHandler::new(config, storage.clone(), net.clone())
            .await
            .unwrap();
        let context = DriverContext {
            storage_handle: storage.clone(),
            net_handle: Some(net.clone()),
            blob_handle: Some(blob),
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let (realm_id, node_id) = (RealmId::from_bytes([1; 32]), net.node_id());
        let creator = UserId::new(Ulid::from_bytes([5; 16]), realm_id);
        let admin = UserId::new(Ulid::from_bytes([6; 16]), realm_id);
        let group_id = Ulid::from_bytes([3; 16]);
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: creator,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
        };
        let rows = crate::s3::bucket::key_rows::authority_rows(&info, None, &[admin]);
        let documents = [realm_id.as_bytes().to_vec(), group_id.to_bytes().to_vec()];
        for (key, (_, value)) in documents.into_iter().zip(rows.into_iter().skip(2)) {
            let write = StorageEffect::Write {
                key_space: AUTH_KEYSPACE.to_string(),
                key: key.into(),
                value: value.unwrap(),
                txn_id: None,
            };
            storage.send_storage_effect(write).await;
        }
        let create = CreateBucketOperation::new("sealed".to_string(), info);
        drive(create, &context).await.unwrap();
        let lookups = BTreeMap::from([
            (
                creator,
                KeyLookup::Keys(vec![key_record(creator, 7, node_id)]),
            ),
            (admin, KeyLookup::Missing),
        ]);
        let enabled = drive(
            EnableEncryptionOperation::new(EnableInput {
                bucket: "sealed".to_string(),
                group_id,
                realm_id,
                node_id,
                mode: EncryptionMode::NodeManaged,
                cipher: BlockCipher::ChaCha20Poly1305,
                block_keys: BlockKeys::ContentDerived,
                max_unlock_ms: None,
                expected_generation: 0,
                lookups,
                now_ms: 1,
            }),
            &context,
        )
        .await
        .unwrap();
        let key = enabled.key.key;
        let install = InstallKeyOperation::new(InstallInput {
            key,
            public_key: enabled.key.public_key,
            private_key: enabled.private_key,
            duration: None,
            max: None,
        });
        drive(install, &context).await.unwrap();

        let published = key_record(admin, 8, node_id);
        let sealed = seal_published(&context, realm_id, node_id, &published).await;
        assert_eq!(sealed, Ok(vec!["sealed".to_string()]));
        let copies = StorageEffect::Iter {
            key_space: KEY_COPY_KEYSPACE.to_string(),
            prefix: Some(key.key().into()),
            start: None,
            limit: 8,
            txn_id: None,
        };
        let Event::Storage(StorageEvent::IterResult { values, .. }) =
            storage.send_storage_effect(copies).await
        else {
            panic!("no copies");
        };
        assert_eq!(values.len(), 2);
        // The admin holds a copy now, so publishing again seals nothing.
        let again = seal_published(&context, realm_id, node_id, &published).await;
        assert_eq!(again, Ok(Vec::new()));
    }
}
