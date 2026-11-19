//! Keeps compiled request policy sets by content address, so a request compiles a set once.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard};

use aruna_core::request_policy::{
    CompiledPolicySet, CompiledSets, PolicyCompileError, RequestPolicy, policy_set_hash,
};
use lru::LruCache;

/// Compiled sets are content-addressed, so a bounded LRU without a TTL or invalidation is
/// enough: any change makes a new key and stale sets age out.
const POLICY_CACHE_CAPACITY: usize = 256;

/// The compiled policy sets of this node, least recently used dropped first.
pub(super) struct PolicyPrograms(Mutex<LruCache<[u8; 32], Arc<CompiledPolicySet>>>);

impl Default for PolicyPrograms {
    fn default() -> Self {
        let capacity = NonZeroUsize::new(POLICY_CACHE_CAPACITY).unwrap_or(NonZeroUsize::MIN);
        Self(Mutex::new(LruCache::new(capacity)))
    }
}

impl std::fmt::Debug for PolicyPrograms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PolicyPrograms")
            .field(&self.entries().len())
            .finish()
    }
}

impl PolicyPrograms {
    /// Each set compiled, in order; a failed compile names its policy and is not kept.
    pub(super) fn compile(&self, sets: &[Vec<RequestPolicy>]) -> CompiledSets {
        CompiledSets(sets.iter().map(|set| self.compiled(set)).collect())
    }

    fn compiled(
        &self,
        policies: &[RequestPolicy],
    ) -> Result<Arc<CompiledPolicySet>, PolicyCompileError> {
        let key = policy_set_hash(policies);
        if let Some(set) = self.entries().get(&key).cloned() {
            return Ok(set);
        }
        let set = Arc::new(CompiledPolicySet::compile(policies)?);
        self.entries().put(key, set.clone());
        Ok(set)
    }

    fn entries(&self) -> MutexGuard<'_, LruCache<[u8; 32], Arc<CompiledPolicySet>>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::request_policy::PolicyKind;
    use ulid::Ulid;

    fn policy(expression: &str) -> RequestPolicy {
        RequestPolicy {
            policy_id: Ulid::from_bytes([9u8; 16]),
            name: "policy".to_string(),
            kind: PolicyKind::Deny,
            enabled: true,
            expression: expression.to_string(),
            when: None,
        }
    }

    #[test]
    fn caches_by_content() {
        // Identical content reuses the same set; an edit compiles a new one.
        let programs = PolicyPrograms::default();
        let set = vec![policy("permission == 'write'")];
        let edited = vec![policy("permission == 'read'")];
        let CompiledSets(compiled) = programs.compile(&[set.clone(), set, edited]);
        let [Ok(first), Ok(second), Ok(third)] = compiled.as_slice() else {
            panic!("all three sets compile");
        };
        assert!(Arc::ptr_eq(first, second));
        assert!(!Arc::ptr_eq(first, third));
    }

    #[test]
    fn failure_not_kept() {
        let programs = PolicyPrograms::default();
        let CompiledSets(compiled) = programs.compile(&[vec![policy("path.startsWith(")]]);
        assert!(compiled[0].is_err());
        assert!(programs.entries().is_empty());
    }
}
