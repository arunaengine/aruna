//! Stores one entry per realm and the last accepted issue time, which outlives the entry, plus
//! the registry-owned admin state, its audit records and the names a realm has used.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::ops::Bound;
use std::path::Path;
use std::sync::Mutex;

use aruna_core::federation::{RealmTotals, Registration, Signed};
use aruna_core::structs::identity::realm::RealmId;
use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::admin::{AdminError, AdminState, AuditRecord, RealmStatus, ResolveGate, transition};
use crate::verify::RouteProblem;

/// An entry leaves the listing this long after its last renewal.
pub const LEASE_SECS: u64 = 72 * 3600;
/// KPIs observed longer ago than this are marked stale.
pub const STALE_SECS: u64 = 24 * 3600;
/// Largest accepted clock skew of a realm ahead of the registry.
pub const MAX_FUTURE_SECS: u64 = 300;
/// Listing index ranks: approved realms list before listed ones.
const APPROVED_RANK: u8 = 0;
const LISTED_RANK: u8 = 1;

/// The listing index key of a realm: its rank, then its realm id.
fn listing_key(rank: u8, realm_id: &RealmId) -> Vec<u8> {
    let mut key = vec![rank];
    key.extend_from_slice(realm_id.as_bytes());
    key
}

/// The listing rank of a realm with an entry; hidden and retired realms are not listed.
fn listing_rank(state: &AdminState) -> Option<u8> {
    match state.status {
        RealmStatus::Approved => Some(APPROVED_RANK),
        RealmStatus::Listed => Some(LISTED_RANK),
        RealmStatus::Hidden | RealmStatus::Retired => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub registration: Signed<Registration>,
    pub received_at: u64,
    /// Both descriptor routes returned the registered descriptor.
    pub verified: bool,
    /// Why the descriptor routes did not verify; empty when verified.
    pub problems: Vec<RouteProblem>,
}

/// A realm name and the receive time of the first registration that carried it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameRecord {
    pub name: String,
    pub since: u64,
}

/// A listed realm's entry, admin state and latest totals.
pub type Listed = (Entry, AdminState, Option<RealmTotals>);

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
    Admin(#[from] AdminError),
    #[error(transparent)]
    Fjall(#[from] fjall::Error),
    #[error(transparent)]
    Encoding(#[from] postcard::Error),
}

pub struct Store {
    db: Database,
    entries: Keyspace,
    issued: Keyspace,
    admin: Keyspace,
    /// Keyed by realm id and a per-realm sequence number.
    audit: Keyspace,
    names: Keyspace,
    /// The latest self-reported totals per realm, apart from the entry.
    totals: Keyspace,
    /// One row per realm with an entry the public listing shows, ranked approved first.
    listing: Keyspace,
    /// Serializes the issue-time check with the write that follows it.
    writer: Mutex<()>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let db = Database::builder(path).open()?;
        let entries = db.keyspace("entries", KeyspaceCreateOptions::default)?;
        let issued = db.keyspace("issued", KeyspaceCreateOptions::default)?;
        let admin = db.keyspace("admin", KeyspaceCreateOptions::default)?;
        let audit = db.keyspace("audit", KeyspaceCreateOptions::default)?;
        let names = db.keyspace("names", KeyspaceCreateOptions::default)?;
        let totals = db.keyspace("totals", KeyspaceCreateOptions::default)?;
        let listing = db.keyspace("listing", KeyspaceCreateOptions::default)?;
        let store = Self {
            db,
            entries,
            issued,
            admin,
            audit,
            names,
            totals,
            listing,
            writer: Mutex::new(()),
        };
        store.fill_listing()?;
        Ok(store)
    }

    /// Writes the listing rows of a store an earlier release wrote without them. Every write
    /// keeps the index, so an index with a row is complete and costs one read here.
    fn fill_listing(&self) -> Result<(), StoreError> {
        if !self.listing.is_empty()? {
            return Ok(());
        }
        let mut batch = self.db.batch();
        for item in self.entries.iter() {
            let (key, _) = item.into_inner()?;
            let realm = <[u8; 32]>::try_from(key.as_ref()).map_err(|_| StoreError::NotFound)?;
            let realm_id = RealmId::from_bytes(realm);
            let rank = listing_rank(&self.admin(&realm_id)?);
            self.place_listing(&mut batch, &realm_id, rank);
        }
        Ok(batch.commit()?)
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
        let rank = listing_rank(&self.admin(realm_id)?);
        self.place_listing(&mut batch, realm_id, rank);
        let name = &entry.registration.payload.descriptor.payload.name;
        let mut names = self.names(realm_id)?;
        if names.last().is_none_or(|last| last.name != *name) {
            names.push(NameRecord {
                name: name.clone(),
                since: entry.received_at,
            });
            batch.insert(
                &self.names,
                realm_id.as_bytes().to_vec(),
                postcard::to_allocvec(&names)?,
            );
        }
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
        batch.remove(&self.totals, realm_id.as_bytes().to_vec());
        self.place_listing(&mut batch, realm_id, None);
        batch.insert(
            &self.issued,
            realm_id.as_bytes().to_vec(),
            postcard::to_allocvec(&issued_at)?,
        );
        Ok(batch.commit()?)
    }

    /// Keeps `totals` of a realm with an entry when they are newer than the stored ones.
    pub fn put_totals(&self, totals: &RealmTotals, now: u64) -> Result<(), StoreError> {
        let _guard = self.writer.lock().map_err(|_| StoreError::Poisoned)?;
        let key = totals.realm_id.as_bytes();
        if totals.observed_at > now.saturating_add(MAX_FUTURE_SECS) {
            return Err(StoreError::FutureIssued);
        }
        if !self.entries.contains_key(key)? {
            return Err(StoreError::NotFound);
        }
        if let Some(last) = self.totals(&totals.realm_id)?
            && totals.observed_at <= last.observed_at
        {
            return Err(StoreError::Replayed);
        }
        Ok(self.totals.insert(key, postcard::to_allocvec(totals)?)?)
    }

    /// The realm's latest totals; none for a realm that never sent any. A stored row that does
    /// not decode is an error, so it never passes for absent and drops the replay check.
    pub fn totals(&self, realm_id: &RealmId) -> Result<Option<RealmTotals>, StoreError> {
        let value = self.totals.get(realm_id.as_bytes())?;
        Ok(value
            .map(|value| postcard::from_bytes(&value))
            .transpose()?)
    }

    /// The realm's entry while its lease runs.
    pub fn entry(&self, realm_id: &RealmId, now: u64) -> Result<Option<Entry>, StoreError> {
        let Some(value) = self.entries.get(realm_id.as_bytes())? else {
            return Ok(None);
        };
        Ok(Some(decode_entry(&value)?).filter(|entry| leased(entry, now)))
    }

    /// Every entry whose lease runs, in realm id order.
    pub fn entries(&self, now: u64) -> Result<Vec<Entry>, StoreError> {
        let mut entries = self.all_entries()?;
        entries.retain(|entry| leased(entry, now));
        Ok(entries)
    }

    /// One page of the public listing after the index key `after`: approved realms first, then
    /// listed ones, each in realm id order, with their admin state and totals. At most `limit`
    /// index rows are read; `next` is the last of them while more may follow.
    pub fn listing_page(
        &self,
        now: u64,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Listed>, Option<Vec<u8>>), StoreError> {
        // Every write holds the writer lock, so the index row and the rows it joins are read
        // as one state: no status or entry change lands inside a page.
        let _guard = self.writer.lock().map_err(|_| StoreError::Poisoned)?;
        let start = after.map_or(Bound::Unbounded, |after| Bound::Excluded(after.to_vec()));
        let (mut listed, mut read) = (Vec::new(), 0);
        for item in self.listing.range::<Vec<u8>, _>((start, Bound::Unbounded)) {
            let (key, _) = item.into_inner()?;
            read += 1;
            let realm = key
                .get(1..)
                .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok());
            let realm_id = RealmId::from_bytes(realm.ok_or(StoreError::NotFound)?);
            // An entry whose lease ended keeps its row until a write changes it.
            if let Some(entry) = self.entry(&realm_id, now)? {
                listed.push((entry, self.admin(&realm_id)?, self.totals(&realm_id)?));
            }
            if read == limit {
                return Ok((listed, Some(key.to_vec())));
            }
        }
        Ok((listed, None))
    }

    /// Puts the realm's listing row where `rank` places it, or removes it for `None`.
    fn place_listing(&self, batch: &mut OwnedWriteBatch, realm_id: &RealmId, rank: Option<u8>) {
        for other in [APPROVED_RANK, LISTED_RANK] {
            if Some(other) != rank {
                batch.remove(&self.listing, listing_key(other, realm_id));
            }
        }
        if let Some(rank) = rank {
            batch.insert(&self.listing, listing_key(rank, realm_id), Vec::<u8>::new());
        }
    }

    /// Every leased entry the resolver may send identifiers to, in realm id order, read in one
    /// pass over the entries.
    pub fn resolvable(&self, now: u64) -> Result<Vec<Entry>, StoreError> {
        let mut resolvable = Vec::new();
        for item in self.entries.iter() {
            let (_, value) = item.into_inner()?;
            let entry = decode_entry(&value)?;
            let realm_id = entry.registration.payload.descriptor.payload.realm_id;
            if leased(&entry, now) && self.may_resolve(&realm_id)? == ResolveGate::Allowed {
                resolvable.push(entry);
            }
        }
        Ok(resolvable)
    }

    /// Every stored entry, also those whose lease ended, in realm id order.
    pub fn all_entries(&self) -> Result<Vec<Entry>, StoreError> {
        let mut entries = Vec::new();
        for item in self.entries.iter() {
            let (_, value) = item.into_inner()?;
            entries.push(decode_entry(&value)?);
        }
        Ok(entries)
    }

    /// The realm's stored entry, also after its lease ended.
    pub fn stored_entry(&self, realm_id: &RealmId) -> Result<Option<Entry>, StoreError> {
        let value = self.entries.get(realm_id.as_bytes())?;
        value.map(|value| decode_entry(&value)).transpose()
    }

    /// Every realm with an entry, a name history or admin state, so withdrawn realms stay
    /// manageable; in realm id order.
    pub fn known_realms(&self) -> Result<Vec<RealmId>, StoreError> {
        let mut realms = BTreeSet::new();
        for keyspace in [&self.entries, &self.names, &self.admin] {
            for item in keyspace.iter() {
                let (key, _) = item.into_inner()?;
                if let Ok(bytes) = <[u8; 32]>::try_from(key.as_ref()) {
                    realms.insert(bytes);
                }
            }
        }
        Ok(realms.into_iter().map(RealmId::from_bytes).collect())
    }

    fn known(&self, realm_id: &RealmId) -> Result<bool, StoreError> {
        let key = realm_id.as_bytes();
        Ok(self.entries.contains_key(key)?
            || self.names.contains_key(key)?
            || self.admin.contains_key(key)?)
    }

    /// The names the realm registered with, oldest first.
    pub fn names(&self, realm_id: &RealmId) -> Result<Vec<NameRecord>, StoreError> {
        let value = self.names.get(realm_id.as_bytes())?;
        Ok(value
            .map(|value| postcard::from_bytes(&value))
            .transpose()?
            .unwrap_or_default())
    }

    /// The resolver gate of the realm, shared by every resolver route.
    pub fn may_resolve(&self, realm_id: &RealmId) -> Result<ResolveGate, StoreError> {
        Ok(self.admin(realm_id)?.may_resolve())
    }

    /// The realm's admin state; a realm nobody changed is listed.
    pub fn admin(&self, realm_id: &RealmId) -> Result<AdminState, StoreError> {
        let value = self.admin.get(realm_id.as_bytes())?;
        Ok(value
            .map(|value| postcard::from_bytes(&value))
            .transpose()?
            .unwrap_or_default())
    }

    /// Applies an admin change to a known realm and appends its audit record atomically.
    pub fn update_admin(
        &self,
        realm_id: &RealmId,
        next: AdminState,
        actor: &str,
        now: u64,
    ) -> Result<AdminState, StoreError> {
        let _guard = self.writer.lock().map_err(|_| StoreError::Poisoned)?;
        if !self.known(realm_id)? {
            return Err(StoreError::NotFound);
        }
        let current = self.admin(realm_id)?;
        // Only a change that claims a namespace this realm does not hold reads the others.
        let claims = (next.namespace.as_ref())
            .is_some_and(|namespace| current.namespace.as_ref() != Some(namespace));
        let holders = match claims {
            true => self.namespace_holders(realm_id)?,
            false => Vec::new(),
        };
        let after = transition(&current, next, |namespace| {
            holders.iter().any(|held| held == namespace)
        })?;
        let sequence = self.next_audit(realm_id)?;
        let mut key = realm_id.as_bytes().to_vec();
        key.extend_from_slice(&sequence.to_be_bytes());
        let record = AuditRecord {
            at: now,
            actor: actor.to_string(),
            before: current,
            after: after.clone(),
        };
        let mut batch = self.db.batch();
        batch.insert(
            &self.admin,
            realm_id.as_bytes().to_vec(),
            postcard::to_allocvec(&after)?,
        );
        batch.insert(&self.audit, key, postcard::to_allocvec(&record)?);
        let rank = match self.entries.contains_key(realm_id.as_bytes())? {
            true => listing_rank(&after),
            false => None,
        };
        self.place_listing(&mut batch, realm_id, rank);
        batch.commit()?;
        Ok(after)
    }

    /// The namespaces other realms hold.
    fn namespace_holders(&self, realm_id: &RealmId) -> Result<Vec<String>, StoreError> {
        let mut holders = Vec::new();
        for item in self.admin.iter() {
            let (key, value) = item.into_inner()?;
            let state: AdminState = postcard::from_bytes(&value)?;
            if key.as_ref() != realm_id.as_bytes() {
                holders.extend(state.namespace);
            }
        }
        Ok(holders)
    }

    /// The sequence after the realm's last audit record, read from its last key alone, so a
    /// change never counts the realm's whole history.
    fn next_audit(&self, realm_id: &RealmId) -> Result<u64, StoreError> {
        let Some(last) = self.audit.prefix(realm_id.as_bytes()).next_back() else {
            return Ok(0);
        };
        let (key, _) = last.into_inner()?;
        let sequence = key
            .get(32..)
            .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok());
        Ok(sequence.map_or(0, |sequence| u64::from_be_bytes(sequence).saturating_add(1)))
    }

    /// The realm's admin changes, oldest first.
    pub fn audit(&self, realm_id: &RealmId) -> Result<Vec<AuditRecord>, StoreError> {
        let mut records = Vec::new();
        for item in self.audit.prefix(realm_id.as_bytes()) {
            let (_, value) = item.into_inner()?;
            records.push(postcard::from_bytes(&value)?);
        }
        Ok(records)
    }
}

/// A stored entry; one that does not decode is an error, never a missing realm.
fn decode_entry(value: &[u8]) -> Result<Entry, StoreError> {
    Ok(postcard::from_bytes(value)?)
}

/// Whether the entry's lease still runs at `now`.
pub fn leased(entry: &Entry, now: u64) -> bool {
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
        named_registration(base, "Realm", issued_at)
    }

    pub(crate) fn named_registration(
        base: &str,
        name: &str,
        issued_at: u64,
    ) -> Signed<Registration> {
        let descriptor = RealmDescriptor {
            realm_id: realm_id(),
            name: name.to_string(),
            description: String::new(),
            api_url: Url::parse(&format!("{base}/api/v1")).unwrap(),
            portal_url: Url::parse(base).unwrap(),
            contact: None,
            login_providers: Vec::new(),
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
            problems: Vec::new(),
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

    // A stored entry or totals row that does not decode is reported, not taken for a missing
    // realm, so it neither vanishes from the listing nor drops the replay check of totals.
    #[test]
    fn undecodable_rows_reported() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let realm = realm_id();
        store
            .entries
            .insert(realm.as_bytes(), vec![0xff, 0xff])
            .unwrap();
        store
            .totals
            .insert(realm.as_bytes(), vec![0xff, 0xff])
            .unwrap();
        assert!(matches!(
            store.entry(&realm, 1),
            Err(StoreError::Encoding(_))
        ));
        assert!(matches!(
            store.stored_entry(&realm),
            Err(StoreError::Encoding(_))
        ));
        assert!(matches!(store.entries(1), Err(StoreError::Encoding(_))));
        assert!(matches!(store.totals(&realm), Err(StoreError::Encoding(_))));
    }

    // The next audit record follows the realm's last one, so a gap in its history never makes a
    // change overwrite a record it did not write.
    #[test]
    fn audit_follows_last() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let realm = realm_id();
        store.register(&realm, &entry(100, 100), 100).unwrap();
        let noted = |note: &str| AdminState {
            note: note.to_string(),
            ..AdminState::default()
        };
        for note in ["a", "b", "c"] {
            store
                .update_admin(&realm, noted(note), "test", 101)
                .unwrap();
        }
        let mut first = realm.as_bytes().to_vec();
        first.extend_from_slice(&0u64.to_be_bytes());
        store.audit.remove(first).unwrap();
        store.update_admin(&realm, noted("d"), "test", 102).unwrap();
        let notes: Vec<_> = (store.audit(&realm).unwrap().into_iter())
            .map(|record| record.after.note)
            .collect();
        assert_eq!(notes, ["b", "c", "d"]);
    }

    // The listing and the resolver read each entry once with its admin state and keep their
    // gates: the listing shows listed realms with their totals, the resolver approved ones, and
    // neither an entry whose lease ended.
    #[test]
    fn single_pass_gates() {
        use crate::admin::RealmStatus;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let realm = realm_id();
        store.register(&realm, &entry(100, 100), 100).unwrap();
        let (listed, _) = store.listing_page(100, None, 10).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(
            (listed[0].1.status, &listed[0].2),
            (RealmStatus::Listed, &None)
        );
        assert!(store.resolvable(100).unwrap().is_empty());

        let approved = AdminState {
            status: RealmStatus::Approved,
            ..AdminState::default()
        };
        store.update_admin(&realm, approved, "test", 101).unwrap();
        assert_eq!(store.resolvable(101).unwrap().len(), 1);
        let expired = 100 + LEASE_SECS;
        assert!(store.listing_page(expired, None, 10).unwrap().0.is_empty());
        assert!(store.resolvable(expired).unwrap().is_empty());

        let hidden = AdminState {
            status: RealmStatus::Hidden,
            ..AdminState::default()
        };
        store.update_admin(&realm, hidden, "test", 102).unwrap();
        assert!(store.listing_page(102, None, 10).unwrap().0.is_empty());
    }

    // A store an earlier release wrote lists its realms once opened; a hidden one stays out, and
    // opening it again changes nothing.
    #[test]
    fn fills_old_listing() {
        use crate::admin::RealmStatus;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (listed, hidden) = (seeded_entry(3, 100), seeded_entry(4, 100));
        store.register(&listed.0, &listed.1, 100).unwrap();
        store.register(&hidden.0, &hidden.1, 100).unwrap();
        let state = AdminState {
            status: RealmStatus::Hidden,
            ..AdminState::default()
        };
        store.update_admin(&hidden.0, state, "test", 100).unwrap();
        // The earlier release wrote no listing rows.
        store.listing.remove(listing_key(LISTED_RANK, &listed.0)).unwrap();
        assert!(store.listing_page(100, None, 10).unwrap().0.is_empty());
        drop(store);

        for _ in 0..2 {
            let store = Store::open(dir.path()).unwrap();
            let (page, _) = store.listing_page(100, None, 10).unwrap();
            let realms: Vec<RealmId> = (page.iter())
                .map(|(entry, _, _)| entry.registration.payload.descriptor.payload.realm_id)
                .collect();
            assert_eq!(realms, [listed.0]);
        }
    }

    /// A registration of the realm whose management key has `seed`.
    fn seeded_entry(seed: u8, at: u64) -> (RealmId, Entry) {
        let key = SigningKey::from_bytes(&[seed; 32]);
        let realm = RealmId::from_bytes(key.verifying_key().to_bytes());
        let signer = NodeCapabilities::management_node(key).unwrap();
        let descriptor = RealmDescriptor {
            realm_id: realm,
            name: format!("Realm {seed}"),
            description: String::new(),
            api_url: Url::parse("https://realm.example.org/api/v1").unwrap(),
            portal_url: Url::parse("https://realm.example.org").unwrap(),
            contact: None,
            login_providers: Vec::new(),
            issued_at: 1,
        };
        let registration = Registration {
            descriptor: Signed::sign(descriptor, &signer).unwrap(),
            kpis: RealmKpis {
                live_datasets: None,
                groups: None,
                nodes_configured: None,
            },
            observed_at: at,
            issued_at: at,
        };
        let entry = Entry {
            registration: Signed::sign(registration, &signer).unwrap(),
            received_at: at,
            verified: false,
            problems: Vec::new(),
        };
        (realm, entry)
    }

    #[test]
    fn listing_pages_ranked() {
        // Pages list approved realms first and follow status changes; a hidden one leaves.
        use crate::admin::RealmStatus;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let (first, entry) = seeded_entry(7, 100);
        store.register(&first, &entry, 100).unwrap();
        let (second, entry) = seeded_entry(8, 100);
        store.register(&second, &entry, 100).unwrap();
        let last = if first.as_bytes() > second.as_bytes() {
            first
        } else {
            second
        };
        let approved = AdminState {
            status: RealmStatus::Approved,
            ..AdminState::default()
        };
        store.update_admin(&last, approved, "test", 101).unwrap();
        let realm = |listed: &Listed| listed.0.registration.payload.descriptor.payload.realm_id;
        let (page, next) = store.listing_page(101, None, 1).unwrap();
        assert_eq!(page.iter().map(realm).collect::<Vec<_>>(), [last]);
        let (page, _) = store.listing_page(101, next.as_deref(), 1).unwrap();
        assert_ne!(page.iter().map(realm).collect::<Vec<_>>(), [last]);
        let hidden = AdminState {
            status: RealmStatus::Hidden,
            ..AdminState::default()
        };
        store.update_admin(&last, hidden, "test", 102).unwrap();
        let (page, _) = store.listing_page(102, None, 10).unwrap();
        assert!(page.iter().all(|listed| realm(listed) != last));
        store.withdraw(&first, 103, 103).unwrap();
        store.withdraw(&second, 103, 103).unwrap();
        assert!(store.listing_page(103, None, 10).unwrap().0.is_empty());
    }

    #[test]
    fn stale_after_bound() {
        let entry = entry(100, 100);
        assert!(!is_stale(&entry, 100 + STALE_SECS - 1));
        assert!(is_stale(&entry, 100 + STALE_SECS));
    }
}
