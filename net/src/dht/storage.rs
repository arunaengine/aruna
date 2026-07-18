use std::collections::HashSet;

use aruna_core::id::{DhtKeyId, NodeId};
use aruna_core::structs::RealmId;
use aruna_core::util::unix_timestamp_secs;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::constants::{
    MAX_CLOCK_SKEW_SECS, MAX_ENTRIES_PER_KEY, MAX_STORED_VALUE_SIZE, MAX_TTL_SECS, MAX_VALUE_SIZE,
};
use super::rpc::verify_record;

pub const CLEANUP_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredEntry {
    pub publisher: NodeId,
    pub realm_id: RealmId,
    pub value: Vec<u8>,
    pub expires_at: u64,
    pub revision: u64,
    pub signature: iroh::Signature,
    pub retain_until: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MergeError {
    #[error("record version is stale or conflicting")]
    Stale,
    #[error("record set exceeds the per-key capacity")]
    Capacity,
    #[error("record set cannot be encoded")]
    Encoding,
    #[error("stored record set is invalid")]
    Invalid,
}

pub fn decode_entries(bytes: &[u8]) -> Result<Vec<StoredEntry>, postcard::Error> {
    postcard::from_bytes(bytes)
}

pub fn encode_entries(entries: &[StoredEntry]) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(entries)
}

pub fn live_entries(entries: Vec<StoredEntry>, now: u64) -> Vec<StoredEntry> {
    entries
        .into_iter()
        .filter(|entry| entry_is_fresh(entry.expires_at, now))
        .collect()
}

pub fn retained_entries(entries: Vec<StoredEntry>, now: u64) -> Vec<StoredEntry> {
    entries
        .into_iter()
        .filter(|entry| entry.retain_until > now)
        .collect()
}

pub fn merge_entry(
    key: &DhtKeyId,
    entries: Vec<StoredEntry>,
    new_entry: StoredEntry,
    now: u64,
) -> Result<Vec<StoredEntry>, MergeError> {
    validate_entries(key, &entries, now)?;
    validate_entries(key, std::slice::from_ref(&new_entry), now)?;
    if new_entry.expires_at
        > now
            .saturating_add(MAX_TTL_SECS)
            .saturating_add(MAX_CLOCK_SKEW_SECS)
    {
        return Err(MergeError::Invalid);
    }
    if !entry_is_fresh(new_entry.expires_at, now) {
        return Err(MergeError::Stale);
    }

    let mut new_entry = new_entry;
    new_entry.retain_until = new_entry.retain_until.max(
        now.saturating_add(MAX_TTL_SECS)
            .saturating_add(MAX_CLOCK_SKEW_SECS),
    );
    let mut filtered = retained_entries(entries, now);

    if let Some(position) = filtered.iter().position(|entry| {
        entry.publisher == new_entry.publisher && entry.realm_id == new_entry.realm_id
    }) {
        let existing = &filtered[position];
        match new_entry.revision.cmp(&existing.revision) {
            std::cmp::Ordering::Less => return Err(MergeError::Stale),
            std::cmp::Ordering::Equal if !same_record(&new_entry, existing) => {
                return Err(MergeError::Stale);
            }
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => filtered[position] = new_entry,
        }
    } else {
        if filtered.len() >= MAX_ENTRIES_PER_KEY {
            return Err(MergeError::Capacity);
        }
        filtered.push(new_entry);
    }

    filtered.sort_unstable_by(|left, right| {
        left.publisher
            .as_bytes()
            .cmp(right.publisher.as_bytes())
            .then_with(|| left.realm_id.as_bytes().cmp(right.realm_id.as_bytes()))
    });

    let encoded = encode_entries(&filtered).map_err(|_| MergeError::Encoding)?;
    if encoded.len() > MAX_STORED_VALUE_SIZE {
        return Err(MergeError::Capacity);
    }

    Ok(filtered)
}

pub fn validate_entries(
    key: &DhtKeyId,
    entries: &[StoredEntry],
    now: u64,
) -> Result<(), MergeError> {
    if entries.len() > MAX_ENTRIES_PER_KEY {
        return Err(MergeError::Capacity);
    }
    let encoded = encode_entries(entries).map_err(|_| MergeError::Encoding)?;
    if encoded.len() > MAX_STORED_VALUE_SIZE {
        return Err(MergeError::Capacity);
    }

    let latest_allowed = now
        .saturating_add(MAX_TTL_SECS)
        .saturating_add(MAX_CLOCK_SKEW_SECS)
        .saturating_add(MAX_CLOCK_SKEW_SECS);
    let mut identities = HashSet::with_capacity(entries.len());
    for entry in entries {
        if entry.revision == 0
            || entry.value.len() > MAX_VALUE_SIZE
            || entry.expires_at > latest_allowed
            || entry.retain_until < entry.expires_at
            || entry.retain_until.saturating_sub(entry.expires_at)
                > MAX_TTL_SECS.saturating_add(MAX_CLOCK_SKEW_SECS)
            || !identities.insert((entry.publisher, entry.realm_id))
            || !verify_record(
                key,
                &entry.publisher,
                &entry.realm_id,
                &entry.value,
                entry.expires_at,
                entry.revision,
                &entry.signature,
            )
        {
            return Err(MergeError::Invalid);
        }
    }
    Ok(())
}

fn same_record(left: &StoredEntry, right: &StoredEntry) -> bool {
    left.publisher == right.publisher
        && left.realm_id == right.realm_id
        && left.value == right.value
        && left.expires_at == right.expires_at
        && left.revision == right.revision
        && left.signature == right.signature
}

pub fn entry_is_fresh(expires_at: u64, now: u64) -> bool {
    expires_at > now
}

pub fn now_unix_secs() -> u64 {
    unix_timestamp_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(seed: u16, key: DhtKeyId, revision: u64, expires_at: u64) -> StoredEntry {
        let mut secret = [0u8; 32];
        secret[..2].copy_from_slice(&seed.to_le_bytes());
        let secret = iroh::SecretKey::from_bytes(&secret);
        let publisher = secret.public();
        let mut realm = [0u8; 32];
        realm[..2].copy_from_slice(&seed.to_le_bytes());
        let realm_id = RealmId::from_bytes(realm);
        let value = seed.to_le_bytes().to_vec();
        let signed = super::super::rpc::signed_record_bytes(
            &key, &publisher, &realm_id, &value, expires_at, revision,
        );
        StoredEntry {
            publisher,
            realm_id,
            value,
            expires_at,
            revision,
            signature: secret.sign(&signed),
            retain_until: expires_at,
        }
    }

    #[test]
    fn corrupt_decode_fails() {
        assert!(decode_entries(b"not-postcard").is_err());
    }

    #[test]
    fn stale_replay_rejected() {
        let key = DhtKeyId::from_data(b"stale-replay");
        let current = make_entry(1, key, 2, 150);
        let stale = make_entry(1, key, 1, 200);

        assert_eq!(
            merge_entry(&key, vec![current], stale, 100),
            Err(MergeError::Stale)
        );
    }

    #[test]
    fn expired_highwater_blocks() {
        let key = DhtKeyId::from_data(b"expired-highwater");
        let older = make_entry(1, key, 1, 1_000);
        let newer = make_entry(1, key, 2, 500);

        let entries = merge_entry(&key, Vec::new(), older.clone(), 100).unwrap();
        let entries = merge_entry(&key, entries, newer, 110).unwrap();

        assert_eq!(
            merge_entry(&key, entries, older, 801),
            Err(MergeError::Stale)
        );
    }

    #[test]
    fn clock_rollback_valid() {
        let key = DhtKeyId::from_data(b"clock-rollback");
        let entry = make_entry(1, key, 1, 200);
        let entries = merge_entry(&key, Vec::new(), entry, 100).unwrap();

        assert_eq!(validate_entries(&key, &entries, 99), Ok(()));
    }

    #[test]
    fn future_entry_rejected() {
        let key = DhtKeyId::from_data(b"future-entry");
        let expires_at = 100 + MAX_TTL_SECS + MAX_CLOCK_SKEW_SECS + 1;
        let entry = make_entry(1, key, 1, expires_at);

        assert_eq!(
            merge_entry(&key, Vec::new(), entry, 100),
            Err(MergeError::Invalid)
        );
    }

    #[test]
    fn duplicate_is_idempotent() {
        let key = DhtKeyId::from_data(b"duplicate-record");
        let entry = make_entry(1, key, 7, 200);

        assert_eq!(
            merge_entry(&key, vec![entry.clone()], entry.clone(), 100),
            Ok(vec![entry])
        );
    }

    #[test]
    fn record_limit_enforced() {
        let key = DhtKeyId::from_data(b"record-limit");
        let entries = (0..MAX_ENTRIES_PER_KEY)
            .map(|seed| make_entry(seed as u16, key, 1, 200))
            .collect::<Vec<_>>();

        assert_eq!(
            merge_entry(&key, entries, make_entry(u16::MAX, key, 1, 200), 100),
            Err(MergeError::Capacity)
        );
    }
}
