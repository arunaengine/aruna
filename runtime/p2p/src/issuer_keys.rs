//! Remembers the issuer public keys this node checked, so the hot peer path checks a key once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use aruna_core::auth::{KEY_CACHE_CAPACITY, KEY_CACHE_TTL};
use lru::LruCache;

/// Valid issuer public keys with the time each was checked, least recent dropped first.
#[derive(Clone, Debug)]
pub struct IssuerKeys(Arc<Mutex<LruCache<[u8; 32], Instant>>>);

impl Default for IssuerKeys {
    fn default() -> Self {
        let capacity = NonZeroUsize::new(KEY_CACHE_CAPACITY).unwrap_or(NonZeroUsize::MIN);
        Self(Arc::new(Mutex::new(LruCache::new(capacity))))
    }
}

impl IssuerKeys {
    /// Whether `key` is a valid public key; only a valid one is kept, until its TTL ends.
    pub fn check(&self, key: [u8; 32]) -> bool {
        self.check_at(key, Instant::now())
    }

    fn check_at(&self, key: [u8; 32], now: Instant) -> bool {
        let mut keys = self.entries();
        if let Some(checked) = keys.get(&key).copied() {
            if now.saturating_duration_since(checked) < KEY_CACHE_TTL {
                return true;
            }
            keys.pop(&key);
        }
        let valid = iroh::PublicKey::from_bytes(&key).is_ok();
        if valid {
            keys.put(key, now);
        }
        valid
    }

    fn entries(&self) -> MutexGuard<'_, LruCache<[u8; 32], Instant>> {
        self.0.lock().unwrap_or_else(|lock| lock.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_key() -> [u8; 32] {
        *iroh::SecretKey::from_bytes(&[1u8; 32]).public().as_bytes()
    }

    #[test]
    fn keeps_valid_key() {
        let keys = IssuerKeys::default();
        let now = Instant::now();
        assert!(keys.check_at(valid_key(), now));
        assert_eq!(keys.entries().peek(&valid_key()), Some(&now));
    }

    #[test]
    fn skips_invalid_key() {
        // A y coordinate of 2 has no point on the curve.
        let mut invalid = [0u8; 32];
        invalid[0] = 2;
        let keys = IssuerKeys::default();
        assert!(!keys.check_at(invalid, Instant::now()));
        assert!(keys.entries().is_empty());
    }

    #[test]
    fn keeps_capacity() {
        let keys = IssuerKeys::default();
        let now = Instant::now();
        for index in 0..=KEY_CACHE_CAPACITY {
            let mut seed = [1u8; 32];
            seed[..8].copy_from_slice(&(index as u64).to_be_bytes());
            let key = *iroh::SecretKey::from_bytes(&seed).public().as_bytes();
            assert!(keys.check_at(key, now));
        }
        assert_eq!(keys.entries().len(), KEY_CACHE_CAPACITY);
    }

    #[test]
    fn expired_key_rechecked() {
        let keys = IssuerKeys::default();
        let first = Instant::now();
        keys.check_at(valid_key(), first);
        let later = first + KEY_CACHE_TTL;
        assert!(keys.check_at(valid_key(), later));
        assert_eq!(keys.entries().peek(&valid_key()), Some(&later));
    }
}
