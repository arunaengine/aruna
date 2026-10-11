//! Reads a bucket's encryption settings and builds the stored rows of a key generation and the
//! user keys its copies seal to.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BUCKET_AUDIT_KEYSPACE, BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE,
    BUCKET_KEY_KEYSPACE, KEY_COPY_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{RealmAuthorizationDocument, RealmId};
use aruna_core::structs::placement::policy::document::group_admin_path;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyRecord, CopyTarget, GrantState, HolderOrigin, SealedCopy,
};
use aruna_core::structs::storage::holders::{HolderReport, HolderState, KeyLookup, admin_users};
use aruna_core::structs::storage::multipart::MultipartUpload;
use aruna_core::types::{GroupId, Key, TxnId, Value};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

pub type Row = (String, Key, Value);

/// The `group_encrypted_buckets` key of a bucket: group id, then bucket name.
pub fn group_bucket_key(group_id: GroupId, bucket: &str) -> Key {
    [&group_id.to_bytes()[..], bucket.as_bytes()]
        .concat()
        .into()
}

/// The audit row of a key change, written in the same batch as the change it records.
pub fn audit_row(
    record: &aruna_core::structs::storage::key_audit::BucketAuditRecord,
) -> Result<Row, ConversionError> {
    let value = record.to_bytes()?;
    Ok((
        BUCKET_AUDIT_KEYSPACE.to_string(),
        record.key().into(),
        value.into(),
    ))
}

#[derive(Debug, Error, PartialEq)]
pub enum SettingsError {
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("The specified bucket does not exist.")]
    NoSuchBucket,
    #[error("the bucket changed owner")]
    GroupMismatch,
    #[error("an authorization document is missing")]
    MissingAuthority,
}

/// A bucket, its settings and the users with implicit key authority, read together.
#[derive(Debug, PartialEq)]
pub struct BucketState {
    pub info: BucketInfo,
    pub settings: BucketEncryption,
    pub admins: BTreeSet<UserId>,
}

/// Reads a bucket, its settings and the realm and group authorization documents together, so
/// implicit authority comes from the snapshot the change commits against (D30).
pub fn authority_read(
    bucket: &str,
    realm_id: RealmId,
    group_id: GroupId,
    txn_id: Option<TxnId>,
) -> Effect {
    let key: Key = bucket.as_bytes().to_vec().into();
    Effect::Storage(StorageEffect::BatchRead {
        reads: vec![
            (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key),
            (
                AUTH_KEYSPACE.to_string(),
                realm_id.as_bytes().to_vec().into(),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                group_id.to_bytes().to_vec().into(),
            ),
        ],
        txn_id,
    })
}

/// The state an `authority_read` returned; admins hold WRITE on the group admin path now.
pub fn parse_authority(
    mut values: Vec<(Key, Option<Value>)>,
    realm_id: RealmId,
    group_id: GroupId,
) -> Result<BucketState, SettingsError> {
    if values.len() != 4 {
        return Err(ConversionError::InvalidLength("bucket authority read".to_string()).into());
    }
    let documents = values.split_off(2);
    let (info, settings) = parse_settings(values, group_id)?;
    let mut documents = documents.into_iter().map(|(_, value)| value);
    let (Some(Some(realm)), Some(Some(group))) = (documents.next(), documents.next()) else {
        return Err(SettingsError::MissingAuthority);
    };
    let realm = RealmAuthorizationDocument::from_bytes(&realm)?;
    let group = GroupAuthorizationDocument::from_bytes(&group)?;
    let path = group_admin_path(realm_id, group_id);
    let roles = realm.roles.values().chain(group.roles.values());
    let admins = admin_users(roles, &path, realm_id);
    Ok(BucketState {
        info,
        settings,
        admins,
    })
}

/// Rows an `authority_read` answers with, where `admins` hold the group admin role.
#[cfg(test)]
pub(crate) fn authority_rows(
    info: &BucketInfo,
    settings: Option<&BucketEncryption>,
    admins: &[UserId],
) -> Vec<(Key, Option<Value>)> {
    use aruna_core::structs::identity::auth::{Actor, Permission, Role};
    let realm_id = info.created_by.realm_id;
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        user_id: info.created_by,
        realm_id,
    };
    let realm = RealmAuthorizationDocument {
        realm_id,
        roles: Default::default(),
        operation_restrictions: Default::default(),
    };
    let admin = Role {
        role_id: ulid::Ulid::from_bytes([1; 16]),
        name: "keepers".to_string(),
        permissions: [(group_admin_path(realm_id, info.group_id), Permission::WRITE)].into(),
        assigned_users: admins.iter().copied().collect(),
    };
    let group = GroupAuthorizationDocument {
        group_id: info.group_id,
        roles: [(admin.role_id, admin)].into(),
        policies: Vec::new(),
    };
    let row = |value: Vec<u8>| (Key::from(Vec::new()), Some(Value::from(value)));
    let settings = settings.map(|settings| Value::from(settings.to_bytes().unwrap()));
    vec![
        row(info.to_bytes().unwrap()),
        (Key::from(Vec::new()), settings),
        row(realm.to_bytes(&actor).unwrap()),
        row(group.to_bytes(&actor).unwrap()),
    ]
}

/// Reads a bucket record and its encryption settings in one transaction.
pub fn settings_read(bucket: &str, txn_id: Option<TxnId>) -> Effect {
    let key: Key = bucket.as_bytes().to_vec().into();
    Effect::Storage(StorageEffect::BatchRead {
        reads: vec![
            (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key),
        ],
        txn_id,
    })
}

/// The bucket record and settings a `settings_read` returned; the bucket must belong to `group_id`.
pub fn parse_settings(
    values: Vec<(Key, Option<Value>)>,
    group_id: GroupId,
) -> Result<(BucketInfo, BucketEncryption), SettingsError> {
    let mut values = values.into_iter();
    let (Some((_, info)), Some((_, settings)), None) =
        (values.next(), values.next(), values.next())
    else {
        return Err(ConversionError::InvalidLength("bucket settings read".to_string()).into());
    };
    let info = BucketInfo::from_bytes(&info.ok_or(SettingsError::NoSuchBucket)?)?;
    if info.group_id != group_id {
        return Err(SettingsError::GroupMismatch);
    }
    Ok((info, BucketEncryption::from_row(settings.as_deref())?))
}

/// Whether a multipart upload of `bucket` is open; it captured the stored format of its start.
pub fn uploads_open(uploads: &[(Key, Value)], bucket: &str) -> bool {
    uploads.iter().any(|(_, value)| {
        MultipartUpload::from_bytes(value.as_ref()).is_ok_and(|upload| upload.bucket == bucket)
    })
}

/// Every published key of every holder, so any key in a holder's vault opens a copy.
pub fn copy_targets(
    report: &HolderReport,
    lookups: &BTreeMap<UserId, KeyLookup>,
) -> Vec<CopyTarget> {
    report
        .holders
        .iter()
        .filter_map(|holder| match lookups.get(&holder.user_id) {
            Some(KeyLookup::Keys(keys)) => Some(keys),
            _ => None,
        })
        .flatten()
        .map(|record| CopyTarget {
            user_id: record.user_id,
            key_record: record.record_id,
            key_id: record.key_id.clone(),
            public_key: record.public_key,
        })
        .collect()
}

/// The settings, key record and copies of a generation, and the explicit grants it made ready.
pub fn generation_rows(
    bucket: &str,
    settings: &BucketEncryption,
    record: &BucketKeyRecord,
    copies: &[SealedCopy],
    report: &HolderReport,
) -> Result<Vec<Row>, ConversionError> {
    let mut rows = vec![
        (
            BUCKET_ENCRYPTION_KEYSPACE.to_string(),
            bucket.as_bytes().to_vec().into(),
            settings.to_bytes()?.into(),
        ),
        (
            BUCKET_KEY_KEYSPACE.to_string(),
            record.key.key().into(),
            record.to_bytes()?.into(),
        ),
    ];
    for copy in copies {
        rows.push((
            KEY_COPY_KEYSPACE.to_string(),
            copy.key().into(),
            copy.to_bytes()?.into(),
        ));
    }
    let explicit = report.holders.iter().filter(|holder| {
        holder.state == HolderState::Ready && holder.origin == HolderOrigin::Explicit
    });
    for mut grant in explicit.filter_map(|holder| holder.grant.clone()) {
        grant.state = GrantState::Ready;
        rows.push((
            BUCKET_HOLDER_KEYSPACE.to_string(),
            grant.key().into(),
            grant.to_bytes()?.into(),
        ));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::identity::user::vault::UserKeyRecord;
    use aruna_core::structs::placement::record::PlacementRef;
    use aruna_core::structs::storage::encryption::{BucketHolder, BucketKeyRef};
    use aruna_core::structs::storage::holders::resolve_holders;
    use aruna_core::vault_format::key_fingerprint;
    use std::collections::BTreeSet;
    use ulid::Ulid;

    fn user(seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId::from_bytes([1; 32]))
    }

    fn key(user_id: UserId, seed: u8) -> UserKeyRecord {
        UserKeyRecord {
            user_id,
            record_id: Ulid::from_bytes([seed; 16]),
            key_id: format!("slot-{seed}"),
            public_key: [seed; 32],
            fingerprint: key_fingerprint(&[seed; 32]),
            has_recovery: false,
            node_id: iroh::SecretKey::from_bytes(&[2; 32]).public(),
            placement: PlacementRef::NIL,
            created_at_ms: 1,
        }
    }

    #[test]
    fn settings_need_bucket() {
        use aruna_core::structs::storage::encryption::EncryptionMode;
        use aruna_core::structs::storage::format::Compression;
        let group_id = Ulid::from_bytes([3; 16]);
        let info = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user(1),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
        };
        let row = |value: Option<Vec<u8>>| (Key::from(b"bucket".to_vec()), value.map(Value::from));
        let read = |settings| vec![row(Some(info.to_bytes().unwrap())), row(settings)];
        // No settings row means the bucket does not encrypt.
        let (found, settings) = parse_settings(read(None), group_id).unwrap();
        assert_eq!(
            (found, settings),
            (info.clone(), BucketEncryption::default())
        );
        let sealed = BucketEncryption {
            mode: EncryptionMode::VaultLocked,
            bucket_id: Some(Ulid::from_bytes([4; 16])),
            key_generation: 1,
            ..Default::default()
        };
        let read_sealed = read(Some(sealed.to_bytes().unwrap()));
        assert_eq!(parse_settings(read_sealed, group_id).unwrap().1, sealed);
        let other = parse_settings(read(None), Ulid::from_bytes([5; 16]));
        assert_eq!(other, Err(SettingsError::GroupMismatch));
        let missing = parse_settings(vec![row(None), row(None)], group_id);
        assert_eq!(missing, Err(SettingsError::NoSuchBucket));
    }

    #[test]
    fn authority_from_documents() {
        use aruna_core::structs::storage::format::Compression;
        let realm_id = RealmId::from_bytes([1; 32]);
        let group_id = Ulid::from_bytes([3; 16]);
        let info = BucketInfo {
            group_id,
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user(1),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
        };
        let rows = authority_rows(&info, None, &[user(2)]);
        let state = parse_authority(rows, realm_id, group_id).unwrap();
        assert_eq!(state.admins, BTreeSet::from([user(2)]));
        // Without its documents nobody holds implicit authority.
        let mut rows = authority_rows(&info, None, &[user(2)]);
        rows[3].1 = None;
        let missing = parse_authority(rows, realm_id, group_id);
        assert_eq!(missing, Err(SettingsError::MissingAuthority));
    }

    #[test]
    fn rows_cover_generation() {
        let (creator, explicit) = (user(1), user(2));
        let lookups = BTreeMap::from([
            (
                creator,
                KeyLookup::Keys(vec![key(creator, 10), key(creator, 11)]),
            ),
            (explicit, KeyLookup::Keys(vec![key(explicit, 12)])),
            (user(3), KeyLookup::Keys(vec![key(user(3), 13)])),
        ]);
        let reference = BucketKeyRef::new(Ulid::from_bytes([9; 16]), 1);
        let grant = BucketHolder {
            bucket_id: reference.bucket_id,
            user_id: explicit,
            origin: HolderOrigin::Explicit,
            state: GrantState::Pending,
            granted_by: creator,
            granted_at_ms: 1,
        };
        let grants = [grant.clone()];
        let admins = BTreeSet::new();
        let report = resolve_holders(creator, &admins, &grants, &lookups, &[]);
        let targets = copy_targets(&report, &lookups);
        // Both keys of the creator and the grant's key; a user who is no holder gets nothing.
        let records: Vec<_> = targets.iter().map(|target| target.key_record).collect();
        assert_eq!(
            records,
            [10, 11, 12].map(|seed| Ulid::from_bytes([seed; 16]))
        );

        let copies: Vec<_> = targets
            .iter()
            .map(|target| SealedCopy {
                key: reference,
                user_id: target.user_id,
                key_record: target.key_record,
                key_id: target.key_id.clone(),
                enc: [0; 32],
                ciphertext: vec![0; 48],
                created_at_ms: 1,
            })
            .collect();
        let report = resolve_holders(creator, &admins, &grants, &lookups, &copies);
        let settings = BucketEncryption::default();
        let record = BucketKeyRecord::new(reference, Ulid::from_bytes([8; 16]), [4; 32], 1);
        let rows = generation_rows("bucket", &settings, &record, &copies, &report).unwrap();
        let spaces: Vec<_> = rows.iter().map(|(space, _, _)| space.as_str()).collect();
        assert_eq!(
            spaces[..2],
            [BUCKET_ENCRYPTION_KEYSPACE, BUCKET_KEY_KEYSPACE]
        );
        assert_eq!(spaces[2..5], [KEY_COPY_KEYSPACE; 3]);
        let (space, key, value) = &rows[5];
        assert_eq!(
            (space.as_str(), key.as_ref()),
            (BUCKET_HOLDER_KEYSPACE, &grant.key()[..])
        );
        assert_eq!(
            BucketHolder::from_bytes(value).unwrap().state,
            GrantState::Ready
        );
    }
}
