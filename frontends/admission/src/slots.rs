//! Keyed concurrency slots: a bounded table of per-key semaphores whose empty entries are
//! removed when the last permit drops.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::user::id::UserId;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Most permits one lease holds at a time.
const LEASE_LIMIT: usize = 2;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LocalKey {
    User(UserId),
    Ip(IpAddr),
}

/// Per-key semaphores with `capacity` permits each, and at most `limit` keys at once.
pub(crate) struct SlotTable {
    entries: HashMap<LocalKey, Arc<Semaphore>>,
    capacity: usize,
    limit: usize,
}

impl SlotTable {
    /// Each value floors at one, and the capacity stays within the semaphore maximum.
    pub(crate) fn shared(capacity: usize, limit: usize) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            entries: HashMap::new(),
            capacity: capacity.clamp(1, Semaphore::MAX_PERMITS),
            limit: limit.max(1),
        }))
    }

    pub(crate) fn acquire(table: &Arc<Mutex<Self>>, key: LocalKey) -> Option<LocalPermit> {
        let mut slots = table.lock().unwrap_or_else(PoisonError::into_inner);
        let semaphore = match slots.entries.get(&key) {
            Some(semaphore) => Arc::clone(semaphore),
            None => {
                if slots.entries.len() >= slots.limit {
                    return None;
                }
                let semaphore = Arc::new(Semaphore::new(slots.capacity));
                slots.entries.insert(key, Arc::clone(&semaphore));
                semaphore
            }
        };
        let permit = Arc::clone(&semaphore).try_acquire_owned().ok()?;
        Some(LocalPermit {
            permit: Some(permit),
            table: Arc::downgrade(table),
            key,
            semaphore,
        })
    }
}

/// One held slot. Dropping it frees the slot and removes the key once no slot is held.
pub struct LocalPermit {
    permit: Option<OwnedSemaphorePermit>,
    table: Weak<Mutex<SlotTable>>,
    key: LocalKey,
    semaphore: Arc<Semaphore>,
}

impl LocalPermit {
    /// A permit outside any table, taken from `semaphore` when it has a free permit.
    #[doc(hidden)]
    pub fn test_permit(semaphore: Arc<Semaphore>) -> Self {
        let permit = Arc::clone(&semaphore).try_acquire_owned().ok();
        Self {
            permit,
            table: Weak::new(),
            key: LocalKey::Ip(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
            semaphore,
        }
    }
}

impl std::fmt::Debug for LocalPermit {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalPermit")
            .field("key", &self.key)
            .finish()
    }
}

impl Drop for LocalPermit {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        drop(permit);
        let Some(table) = self.table.upgrade() else {
            return;
        };
        let mut slots = table.lock().unwrap_or_else(PoisonError::into_inner);
        if self.semaphore.available_permits() == slots.capacity
            && slots
                .entries
                .get(&self.key)
                .is_some_and(|entry| Arc::ptr_eq(entry, &self.semaphore))
        {
            slots.entries.remove(&self.key);
        }
    }
}

/// A connection or operation slot of a filesystem frontend. Dropping it frees the slot.
#[derive(Debug)]
pub struct SlotPermit {
    _permit: LocalPermit,
}

impl SlotPermit {
    pub(crate) fn new(permit: LocalPermit) -> Self {
        Self { _permit: permit }
    }
}

/// A request extension that keeps local admission permits alive through the
/// response body. The server may clone this slot before handing the request to
/// s3s and move its clone into the response wrapper.
#[derive(Clone, Default)]
pub struct LocalLease(Arc<Mutex<Vec<LocalPermit>>>);

impl LocalLease {
    pub fn hold(&self, permit: LocalPermit) -> bool {
        let mut slot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if slot.len() >= LEASE_LIMIT {
            return false;
        }
        slot.push(permit);
        true
    }

    pub fn replace(&self, permit: LocalPermit) {
        let mut slot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let previous = std::mem::replace(&mut *slot, vec![permit]);
        drop(slot);
        drop(previous);
    }
}
