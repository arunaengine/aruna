//! Volatile open and lock stateids: seqid ordering, revocation that keeps its status for
//! TEST_STATEID and FREE_STATEID, and the backend cleanup that revocation leaves.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};

use crate::backend::{Caller, ObjectRef, OwnerKey};
use crate::types::{NfsStat4, Stateid, sequence_flag};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Revoked {
    /// The client lease ran out.
    Expired,
    /// A binding, key or host was revoked.
    Admin,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateKind {
    Open {
        access: u32,
        deny: u32,
    },
    /// A lock owner's state on one file, made through the open stateid `open`.
    Lock {
        open: [u8; 12],
        locked: bool,
    },
}

#[derive(Clone, Debug)]
pub struct StateEntry {
    pub seqid: u32,
    pub client_id: u64,
    pub owner: OwnerKey,
    pub object: ObjectRef,
    /// The host and binding the state was made through, for cleanup after revocation.
    pub caller: Caller,
    pub kind: StateKind,
    pub revoked: Option<Revoked>,
}

/// Backend work that revoked or expired state leaves behind.
#[derive(Clone, Debug)]
pub enum Cleanup {
    Close {
        caller: Caller,
        object: ObjectRef,
        owner: OwnerKey,
    },
    Unlock {
        caller: Caller,
        object: ObjectRef,
        owner: OwnerKey,
    },
}

/// The seqid after `seqid`; it skips zero, which means "current" on the wire.
fn next_seqid(seqid: u32) -> u32 {
    if seqid == u32::MAX { 1 } else { seqid + 1 }
}

pub struct StateTable {
    boot: [u8; 4],
    next: u64,
    entries: HashMap<[u8; 12], StateEntry>,
    opens: HashMap<(OwnerKey, ObjectRef), [u8; 12]>,
    locks: HashMap<(OwnerKey, ObjectRef), [u8; 12]>,
    by_client: HashMap<u64, HashSet<[u8; 12]>>,
}

impl StateTable {
    /// `boot` prefixes every `other`, so stateids of an earlier instance are unknown.
    pub fn new(boot: u32) -> Self {
        Self {
            boot: boot.to_be_bytes(),
            next: 0,
            entries: HashMap::new(),
            opens: HashMap::new(),
            locks: HashMap::new(),
            by_client: HashMap::new(),
        }
    }

    fn allocate(&mut self) -> [u8; 12] {
        self.next += 1;
        let mut other = [0u8; 12];
        other[..4].copy_from_slice(&self.boot);
        other[4..].copy_from_slice(&self.next.to_be_bytes());
        other
    }

    fn insert(&mut self, other: [u8; 12], entry: StateEntry) {
        self.by_client
            .entry(entry.client_id)
            .or_default()
            .insert(other);
        self.entries.insert(other, entry);
    }

    fn remove(&mut self, other: &[u8; 12]) -> Option<StateEntry> {
        let entry = self.entries.remove(other)?;
        if let Some(set) = self.by_client.get_mut(&entry.client_id) {
            set.remove(other);
            if set.is_empty() {
                self.by_client.remove(&entry.client_id);
            }
        }
        let key = (entry.owner.clone(), entry.object);
        let index = match entry.kind {
            StateKind::Open { .. } => &mut self.opens,
            StateKind::Lock { .. } => &mut self.locks,
        };
        if index.get(&key) == Some(other) {
            index.remove(&key);
        }
        Some(entry)
    }

    pub fn get(&self, other: &[u8; 12]) -> Option<&StateEntry> {
        self.entries.get(other)
    }

    /// Checks a non-special stateid of `client_id`: revoked state reports its revocation, a
    /// zero seqid skips the ordering check, an older seqid is OLD_STATEID.
    pub fn validate(&self, stateid: &Stateid, client_id: u64) -> Result<&StateEntry, NfsStat4> {
        if stateid.other[..4] != self.boot {
            return Err(NfsStat4::BadStateid);
        }
        let entry = self
            .entries
            .get(&stateid.other)
            .filter(|entry| entry.client_id == client_id)
            .ok_or(NfsStat4::BadStateid)?;
        match entry.revoked {
            Some(Revoked::Admin) => return Err(NfsStat4::AdminRevoked),
            Some(Revoked::Expired) => return Err(NfsStat4::Expired),
            None => {}
        }
        if stateid.seqid != 0 {
            if stateid.seqid > entry.seqid {
                return Err(NfsStat4::BadStateid);
            }
            if stateid.seqid < entry.seqid {
                return Err(NfsStat4::OldStateid);
            }
        }
        Ok(entry)
    }

    /// The access and deny of the live open of `owner` on `object`.
    pub fn open_modes(&self, owner: &OwnerKey, object: ObjectRef) -> Option<(u32, u32)> {
        let other = self.opens.get(&(owner.clone(), object))?;
        match self.entries.get(other)?.kind {
            StateKind::Open { access, deny } => Some((access, deny)),
            StateKind::Lock { .. } => None,
        }
    }

    /// Records an open or upgrades the existing one of the same owner and file.
    pub fn record_open(
        &mut self,
        owner: OwnerKey,
        object: ObjectRef,
        caller: Caller,
        modes: (u32, u32),
    ) -> Stateid {
        let key = (owner.clone(), object);
        if let Some(other) = self.opens.get(&key).copied()
            && let Some(entry) = self.entries.get_mut(&other)
            && let StateKind::Open { access, deny } = &mut entry.kind
        {
            *access |= modes.0;
            *deny |= modes.1;
            entry.seqid = next_seqid(entry.seqid);
            return Stateid {
                seqid: entry.seqid,
                other,
            };
        }
        let other = self.allocate();
        self.opens.insert(key, other);
        self.insert(
            other,
            StateEntry {
                seqid: 1,
                client_id: owner.client_id,
                owner,
                object,
                caller,
                kind: StateKind::Open {
                    access: modes.0,
                    deny: modes.1,
                },
                revoked: None,
            },
        );
        Stateid { seqid: 1, other }
    }

    pub fn downgrade(&mut self, other: &[u8; 12], modes: (u32, u32)) -> Result<Stateid, NfsStat4> {
        let entry = self.entries.get_mut(other).ok_or(NfsStat4::BadStateid)?;
        let StateKind::Open { access, deny } = &mut entry.kind else {
            return Err(NfsStat4::BadStateid);
        };
        *access = modes.0;
        *deny = modes.1;
        entry.seqid = next_seqid(entry.seqid);
        Ok(Stateid {
            seqid: entry.seqid,
            other: *other,
        })
    }

    /// Whether any lock state made through the open `open` still holds a lock.
    pub fn has_locks(&self, open: &[u8; 12]) -> bool {
        self.entries.values().any(|entry| {
            matches!(entry.kind, StateKind::Lock { open: through, locked: true } if through == *open)
        })
    }

    /// Removes an open and the lock states made through it, which hold no locks.
    pub fn close(&mut self, other: &[u8; 12]) {
        let dependents: Vec<[u8; 12]> = self
            .entries
            .iter()
            .filter(
                |(_, entry)| matches!(entry.kind, StateKind::Lock { open, .. } if open == *other),
            )
            .map(|(key, _)| *key)
            .collect();
        for dependent in &dependents {
            self.remove(dependent);
        }
        self.remove(other);
    }

    /// The lock state of `owner` on `object`, made through the open `open` when new.
    pub fn lock_state(
        &mut self,
        open: [u8; 12],
        owner: OwnerKey,
        object: ObjectRef,
        caller: Caller,
    ) -> [u8; 12] {
        let key = (owner.clone(), object);
        if let Some(other) = self.locks.get(&key) {
            return *other;
        }
        let other = self.allocate();
        self.locks.insert(key, other);
        self.insert(
            other,
            StateEntry {
                seqid: 0,
                client_id: owner.client_id,
                owner,
                object,
                caller,
                kind: StateKind::Lock {
                    open,
                    locked: false,
                },
                revoked: None,
            },
        );
        other
    }

    /// Records whether a lock state holds locks after LOCK or LOCKU and advances its seqid.
    pub fn mark_lock(&mut self, other: &[u8; 12], held: bool) -> Result<Stateid, NfsStat4> {
        let entry = self.entries.get_mut(other).ok_or(NfsStat4::BadStateid)?;
        let StateKind::Lock { locked, .. } = &mut entry.kind else {
            return Err(NfsStat4::BadStateid);
        };
        *locked = held;
        entry.seqid = next_seqid(entry.seqid);
        Ok(Stateid {
            seqid: entry.seqid,
            other: *other,
        })
    }

    /// TEST_STATEID for one non-special stateid.
    pub fn test(&self, stateid: &Stateid, client_id: u64) -> NfsStat4 {
        match self.validate(stateid, client_id) {
            Ok(_) => NfsStat4::Ok,
            Err(status) => status,
        }
    }

    /// FREE_STATEID: drops revoked state, or a lock state without locks.
    pub fn free(&mut self, stateid: &Stateid, client_id: u64) -> Result<(), NfsStat4> {
        let entry = self
            .entries
            .get(&stateid.other)
            .filter(|entry| entry.client_id == client_id && stateid.other[..4] == self.boot)
            .ok_or(NfsStat4::BadStateid)?;
        let removable = match (&entry.revoked, &entry.kind) {
            (Some(_), _) => true,
            (None, StateKind::Lock { locked, .. }) => !*locked,
            (None, StateKind::Open { .. }) => false,
        };
        if !removable {
            return Err(NfsStat4::LocksHeld);
        }
        self.remove(&stateid.other);
        Ok(())
    }

    /// Revokes the live state `selected` picks; the entries stay for TEST_STATEID and
    /// FREE_STATEID, but no longer block a new open of the same owner.
    pub fn revoke(
        &mut self,
        selected: impl Fn(&StateEntry) -> bool,
        reason: Revoked,
    ) -> Vec<Cleanup> {
        let mut unlocks = Vec::new();
        let mut closes = Vec::new();
        for entry in self.entries.values_mut() {
            if entry.revoked.is_some() || !selected(entry) {
                continue;
            }
            entry.revoked = Some(reason);
            match entry.kind {
                StateKind::Lock { locked: true, .. } => unlocks.push(Cleanup::Unlock {
                    caller: entry.caller.clone(),
                    object: entry.object,
                    owner: entry.owner.clone(),
                }),
                StateKind::Lock { .. } => {}
                StateKind::Open { .. } => closes.push(Cleanup::Close {
                    caller: entry.caller.clone(),
                    object: entry.object,
                    owner: entry.owner.clone(),
                }),
            }
        }
        let entries = &self.entries;
        self.opens.retain(|_, other| {
            entries
                .get(other)
                .is_some_and(|entry| entry.revoked.is_none())
        });
        self.locks.retain(|_, other| {
            entries
                .get(other)
                .is_some_and(|entry| entry.revoked.is_none())
        });
        unlocks.extend(closes);
        unlocks
    }

    /// Drops all state of a client that no longer exists, live or revoked.
    pub fn purge_client(&mut self, client_id: u64) -> Vec<Cleanup> {
        let cleanups = self.revoke(|entry| entry.client_id == client_id, Revoked::Admin);
        let others: Vec<[u8; 12]> = self
            .by_client
            .get(&client_id)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        for other in &others {
            self.remove(other);
        }
        cleanups
    }

    /// The SEQUENCE status flags for the revoked state `client_id` has not freed yet.
    pub fn status_flags(&self, client_id: u64) -> u32 {
        let Some(others) = self.by_client.get(&client_id) else {
            return 0;
        };
        let mut flags = 0;
        let mut live = false;
        for entry in others.iter().filter_map(|other| self.entries.get(other)) {
            match entry.revoked {
                Some(Revoked::Admin) => flags |= sequence_flag::ADMIN_REVOKED,
                Some(Revoked::Expired) => flags |= sequence_flag::EXPIRED_SOME,
                None => live = true,
            }
        }
        if flags & sequence_flag::EXPIRED_SOME != 0 && !live {
            flags = (flags & !sequence_flag::EXPIRED_SOME) | sequence_flag::EXPIRED_ALL;
        }
        flags
    }
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;
