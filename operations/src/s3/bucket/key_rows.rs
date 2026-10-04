//! Builds the stored rows of a bucket key generation and the user keys its copies seal to.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::UserId;
use aruna_core::errors::ConversionError;
use aruna_core::keyspaces::{
    BUCKET_ENCRYPTION_KEYSPACE, BUCKET_HOLDER_KEYSPACE, BUCKET_KEY_KEYSPACE, KEY_COPY_KEYSPACE,
};
use aruna_core::structs::storage::encryption::{
    BucketEncryption, BucketKeyRecord, CopyTarget, GrantState, HolderOrigin, SealedCopy,
};
use aruna_core::structs::storage::holders::{HolderReport, HolderState, KeyLookup};
use aruna_core::types::{Key, Value};
use std::collections::BTreeMap;

pub type Row = (String, Key, Value);

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
