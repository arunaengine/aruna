//! Stores one entry per realm and the last accepted issue time, which outlives the entry.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::Path;
use std::sync::Mutex;

use aruna_core::federation::{Registration, Signed};
use aruna_core::structs::identity::realm::RealmId;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// An entry leaves the listing this long after its last renewal.
pub const LEASE_SECS: u64 = 72 * 3600;
/// KPIs observed longer ago than this are marked stale.
pub const STALE_SECS: u64 = 24 * 3600;
/// Largest accepted clock skew of a realm ahead of the registry.
pub const MAX_FUTURE_SECS: u64 = 300;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub registration: Signed<Registration>,
    pub received_at: u64,
    /// Both descriptor routes returned the registered descriptor.
    pub verified: bool,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("issued_at is not newer than the last accepted one")]
    Replayed,
    #[error("issued_at is too far in the future")]
    FutureIssued,
    #[error("realm has no entry")]
    NotFound,
    #[error("registry storage is unavailable")]
    Poisoned,
    #[error(transparent)]
    Fjall(#[from] fjall::Error),
    #[error(transparent)]
    Encoding(#[from] postcard::Error),
}

pub struct Store {
    db: Database,
    entries: Keyspace,
    issued: Keyspace,
    /// Serializes the issue-time check with the write that follows it.
    writer: Mutex<()>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let db = Database::builder(path).open()?;
        let entries = db.keyspace("entries", KeyspaceCreateOptions::default)?;
        let issued = db.keyspace("issued", KeyspaceCreateOptions::default)?;
        Ok(Self {
            db,
            entries,
            issued,
            writer: Mutex::new(()),
        })
    }

    /// Refuses an issue time that is not newer than the last accepted one or too far ahead.
    pub fn check_issued(
        &self,
        realm_id: &RealmId,
        issued_at: u64,
        now: u64,
    ) -> Result<(), StoreError> {
        if issued_at > now.saturating_add(MAX_FUTURE_SECS) {
            return Err(StoreError::FutureIssued);
        }
        let last = self
            .issued
            .get(realm_id.as_bytes())?
            .map(|value| postcard::from_bytes::<u64>(&value))
            .transpose()?;
        match last {
            Some(last) if issued_at <= last => Err(StoreError::Replayed),
            _ => Ok(()),
        }
    }

    pub fn register(&self, realm_id: &RealmId, entry: &Entry, now: u64) -> Result<(), StoreError> {
        let _guard = self.writer.lock().map_err(|_| StoreError::Poisoned)?;
        let issued_at = entry.registration.payload.issued_at;
        self.check_issued(realm_id, issued_at, now)?;
        let mut batch = self.db.batch();
        batch.insert(
            &self.entries,
            realm_id.as_bytes().to_vec(),
            postcard::to_allocvec(entry)?,
        );
        batch.insert(
            &self.issued,
            realm_id.as_bytes().to_vec(),
            postcard::to_allocvec(&issued_at)?,
        );
        Ok(batch.commit()?)
    }

    pub fn withdraw(&self, realm_id: &RealmId, issued_at: u64, now: u64) -> Result<(), StoreError> {
        let _guard = self.writer.lock().map_err(|_| StoreError::Poisoned)?;
        if !self.entries.contains_key(realm_id.as_bytes())? {
            return Err(StoreError::NotFound);
        }
        self.check_issued(realm_id, issued_at, now)?;
        let mut batch = self.db.batch();
        batch.remove(&self.entries, realm_id.as_bytes().to_vec());
        batch.insert(
            &self.issued,
            realm_id.as_bytes().to_vec(),
            postcard::to_allocvec(&issued_at)?,
        );
        Ok(batch.commit()?)
    }

    /// The realm's entry while its lease runs.
    pub fn entry(&self, realm_id: &RealmId, now: u64) -> Result<Option<Entry>, StoreError> {
        let Some(value) = self.entries.get(realm_id.as_bytes())? else {
            return Ok(None);
        };
        let entry: Entry = postcard::from_bytes(&value)?;
        Ok(leased(&entry, now).then_some(entry))
    }

    /// Every entry whose lease runs, in realm id order.
    pub fn entries(&self, now: u64) -> Result<Vec<Entry>, StoreError> {
        let mut entries = Vec::new();
        for item in self.entries.iter() {
            let (_, value) = item.into_inner()?;
            let entry: Entry = postcard::from_bytes(&value)?;
            if leased(&entry, now) {
                entries.push(entry);
            }
        }
        Ok(entries)
    }
}

fn leased(entry: &Entry, now: u64) -> bool {
    now < entry.received_at.saturating_add(LEASE_SECS)
}

/// Whether the KPIs were observed longer than the stale bound ago.
pub fn is_stale(entry: &Entry, now: u64) -> bool {
    now >= entry
        .registration
        .payload
        .observed_at
        .saturating_add(STALE_SECS)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use aruna_core::federation::{RealmDescriptor, RealmKpis};
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use ed25519_dalek::SigningKey;
    use url::Url;

    pub(crate) fn capabilities() -> NodeCapabilities {
        NodeCapabilities::management_node(SigningKey::from_bytes(&[7; 32])).unwrap()
    }

    pub(crate) fn realm_id() -> RealmId {
        RealmId::from_bytes(SigningKey::from_bytes(&[7; 32]).verifying_key().to_bytes())
    }

    pub(crate) fn registration(base: &str, issued_at: u64) -> Signed<Registration> {
        let descriptor = RealmDescriptor {
            realm_id: realm_id(),
            name: "Realm".to_string(),
            description: String::new(),
            api_url: Url::parse(&format!("{base}/api/v1")).unwrap(),
            portal_url: Url::parse(base).unwrap(),
            issued_at: 1,
        };
        let registration = Registration {
            descriptor: Signed::sign(descriptor, &capabilities()).unwrap(),
            kpis: RealmKpis {
                live_datasets: Some(3),
                groups: None,
                nodes_configured: Some(1),
            },
            observed_at: issued_at,
            issued_at,
        };
        Signed::sign(registration, &capabilities()).unwrap()
    }

    fn entry(issued_at: u64, received_at: u64) -> Entry {
        Entry {
            registration: registration("https://realm.example.org", issued_at),
            received_at,
            verified: false,
        }
    }

    #[test]
    fn replay_refused() {
        // The last issue time survives withdrawal, so an old registration cannot return.
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let realm = realm_id();
        store.register(&realm, &entry(100, 100), 100).unwrap();
        assert!(matches!(
            store.register(&realm, &entry(100, 101), 101),
            Err(StoreError::Replayed)
        ));
        store.withdraw(&realm, 150, 150).unwrap();
        assert_eq!(store.entry(&realm, 150).unwrap(), None);
        assert!(matches!(
            store.register(&realm, &entry(120, 151), 151),
            Err(StoreError::Replayed)
        ));
        store.register(&realm, &entry(160, 160), 160).unwrap();
        assert!(store.entry(&realm, 160).unwrap().is_some());
        assert!(matches!(
            store.withdraw(&realm, 150, 161),
            Err(StoreError::Replayed)
        ));
    }

    #[test]
    fn withdraw_without_entry() {
        // Nothing is stored, so a later registration with an older issue time still counts.
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let realm = realm_id();
        assert!(matches!(
            store.withdraw(&realm, 200, 200),
            Err(StoreError::NotFound)
        ));
        store.register(&realm, &entry(100, 201), 201).unwrap();
    }

    #[test]
    fn future_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let now = 1_000;
        assert!(matches!(
            store.register(&realm_id(), &entry(now + MAX_FUTURE_SECS + 1, now), now),
            Err(StoreError::FutureIssued)
        ));
        store
            .register(&realm_id(), &entry(now + MAX_FUTURE_SECS, now), now)
            .unwrap();
    }

    #[test]
    fn lease_expires() {
        // Expiry removes the entry from listings but keeps its last issue time.
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let realm = realm_id();
        store.register(&realm, &entry(100, 100), 100).unwrap();
        let expired = 100 + LEASE_SECS;
        assert!(store.entry(&realm, expired - 1).unwrap().is_some());
        assert_eq!(store.entry(&realm, expired).unwrap(), None);
        assert!(store.entries(expired).unwrap().is_empty());
        assert!(matches!(
            store.check_issued(&realm, 100, expired),
            Err(StoreError::Replayed)
        ));
    }

    #[test]
    fn stale_after_bound() {
        let entry = entry(100, 100);
        assert!(!is_stale(&entry, 100 + STALE_SECS - 1));
        assert!(is_stale(&entry, 100 + STALE_SECS));
    }
}
