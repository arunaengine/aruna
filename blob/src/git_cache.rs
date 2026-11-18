//! Keeps what a node remembers about its Git repositories between operations.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use aruna_core::git::Refs;
use aruna_core::git_state::{Ancestry, Projection, ProjectionInputs};
use lru::LruCache;
use ulid::Ulid;

/// Documents whose last refresh and ancestry answers this node keeps; older ones drop first.
const REMEMBERED: NonZeroUsize = NonZeroUsize::new(256).unwrap();
/// More kept ancestry answers than this for one document are dropped instead.
const MAX_ANSWERS: usize = 4096;

pub struct GitCaches {
    recent: Mutex<LruCache<Ulid, (ProjectionInputs, Projection)>>,
    /// Answers that a commit is an ancestor never change, so later projections reuse them.
    answers: Mutex<LruCache<Ulid, Ancestry>>,
    /// The refs each document's cache last served, which a push must build on.
    served: Mutex<HashMap<Ulid, Refs>>,
    /// Documents with a background refresh queued or running on this node.
    refreshing: Arc<Mutex<HashSet<Ulid>>>,
    /// Orders pending merges as they are accepted; later ones build on earlier ones.
    sequence: Mutex<ulid::Generator>,
    /// Keys of receive-pack requests running here, one per document under its lock.
    push_keys: Mutex<HashMap<Ulid, String>>,
}

impl Default for GitCaches {
    fn default() -> Self {
        Self {
            recent: Mutex::new(LruCache::new(REMEMBERED)),
            answers: Mutex::new(LruCache::new(REMEMBERED)),
            served: Mutex::default(),
            refreshing: Arc::default(),
            sequence: Mutex::new(ulid::Generator::new()),
            push_keys: Mutex::default(),
        }
    }
}

impl GitCaches {
    /// The last completed projection of the document, whatever changed since.
    pub fn last(&self, document_id: Ulid) -> Option<Projection> {
        let mut recent = self.recent.lock().ok()?;
        recent
            .get(&document_id)
            .map(|(_, projection)| projection.clone())
    }

    /// The remembered projection, if it was made under the same inputs.
    pub fn remembered(&self, document_id: Ulid, inputs: &ProjectionInputs) -> Option<Projection> {
        let mut recent = self.recent.lock().ok()?;
        let (known, projection) = recent.get(&document_id)?;
        (known == inputs).then(|| projection.clone())
    }

    pub fn remember(&self, document_id: Ulid, inputs: ProjectionInputs, projection: Projection) {
        if let Ok(mut recent) = self.recent.lock() {
            recent.put(document_id, (inputs, projection));
        }
    }

    /// Drops the remembered refresh, so the next one projects again.
    pub fn forget(&self, document_id: Ulid) {
        if let Ok(mut recent) = self.recent.lock() {
            recent.pop(&document_id);
        }
    }

    pub fn answered(&self, document_id: Ulid) -> Ancestry {
        (self.answers.lock().ok())
            .and_then(|mut known| known.get(&document_id).cloned())
            .unwrap_or_default()
    }

    /// Keeps the true answers: a missing commit answers false until its pack arrives.
    pub fn learn(&self, document_id: Ulid, ancestry: &Ancestry) {
        let answers: Ancestry = ancestry
            .iter()
            .filter(|(_, answer)| **answer)
            .map(|(pair, answer)| (pair.clone(), *answer))
            .collect();
        if let Ok(mut known) = self.answers.lock() {
            if answers.len() > MAX_ANSWERS {
                known.pop(&document_id);
            } else {
                known.put(document_id, answers);
            }
        }
    }

    pub fn served(&self, document_id: Ulid) -> Option<Refs> {
        self.served.lock().ok()?.get(&document_id).cloned()
    }

    pub fn serve(&self, document_id: Ulid, refs: Refs) {
        if let Ok(mut served) = self.served.lock() {
            served.insert(document_id, refs);
        }
    }

    /// The claim on the document's background refresh, unless one is held already.
    pub fn claim_refresh(&self, document_id: Ulid) -> Option<RefreshClaim> {
        let mut running = self.refreshing.lock().ok()?;
        running.insert(document_id).then(|| RefreshClaim {
            running: self.refreshing.clone(),
            document_id,
        })
    }

    /// The next id in acceptance order, which sorts after every earlier one.
    pub fn next_sequence(&self) -> Option<Ulid> {
        self.sequence.lock().ok()?.generate().ok()
    }

    /// Makes `value` the document's push key until the returned guard is dropped.
    pub fn open_push(&self, document_id: Ulid, value: String) -> PushKey<'_> {
        if let Ok(mut keys) = self.push_keys.lock() {
            keys.insert(document_id, value.clone());
        }
        PushKey {
            caches: self,
            document_id,
            value,
        }
    }

    /// Whether `key` belongs to the push now running for the document.
    pub fn push_key_valid(&self, document_id: Ulid, key: &str) -> bool {
        self.push_keys.lock().is_ok_and(|keys| {
            keys.get(&document_id).is_some_and(|expected| {
                expected.len() == key.len()
                    && expected
                        .bytes()
                        .zip(key.bytes())
                        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
                        == 0
            })
        })
    }
}

/// Marks one document's background refresh as running until it is dropped.
pub struct RefreshClaim {
    running: Arc<Mutex<HashSet<Ulid>>>,
    document_id: Ulid,
}

impl Drop for RefreshClaim {
    fn drop(&mut self) {
        if let Ok(mut running) = self.running.lock() {
            running.remove(&self.document_id);
        }
    }
}

/// A push key that is valid while the receive-pack request that created it runs.
pub struct PushKey<'a> {
    caches: &'a GitCaches,
    document_id: Ulid,
    value: String,
}

impl PushKey<'_> {
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl Drop for PushKey<'_> {
    fn drop(&mut self) {
        if let Ok(mut keys) = self.caches.push_keys.lock() {
            keys.remove(&self.document_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::git_state::GitState;

    fn inputs(last_event: u128, overdue: bool) -> ProjectionInputs {
        ProjectionInputs {
            node: None,
            last_event_id: Ulid::from(last_event),
            updated_at_ms: 1,
            overdue,
        }
    }

    fn projection() -> Projection {
        let mut state = GitState::default();
        state
            .refs
            .insert("refs/heads/main".to_string(), "a".repeat(40));
        Projection {
            state,
            records: Default::default(),
            holders: Vec::new(),
            writes: 0,
        }
    }

    // Any changed input drops the reuse; the last projection stays until it is forgotten.
    #[test]
    fn inputs_decide_reuse() {
        let caches = GitCaches::default();
        let id = Ulid::from(1);
        caches.remember(id, inputs(1, false), projection());
        assert_eq!(caches.remembered(id, &inputs(1, false)), Some(projection()));
        assert_eq!(caches.remembered(id, &inputs(1, true)), None);
        assert_eq!(caches.remembered(id, &inputs(2, false)), None);
        assert_eq!(caches.last(id), Some(projection()));
        caches.forget(id);
        assert_eq!(caches.last(id), None);
    }

    // A missing commit answers false until its pack arrives, so false is asked again.
    #[test]
    fn keeps_true_answers() {
        let caches = GitCaches::default();
        let id = Ulid::from(2);
        let pair = |first: char, second: char| (first.to_string(), second.to_string());
        let ancestry = Ancestry::from([(pair('a', 'b'), true), (pair('b', 'c'), false)]);
        caches.learn(id, &ancestry);
        assert_eq!(
            caches.answered(id),
            Ancestry::from([(pair('a', 'b'), true)])
        );
        let many = (0..=MAX_ANSWERS)
            .map(|index| ((index.to_string(), "x".to_string()), true))
            .collect();
        caches.learn(id, &many);
        assert!(caches.answered(id).is_empty());
    }

    // One refresh per document runs; its end frees the document again.
    #[test]
    fn refresh_claimed_once() {
        let caches = GitCaches::default();
        let id = Ulid::from(3);
        let claim = caches.claim_refresh(id).expect("the first claim");
        assert!(caches.claim_refresh(id).is_none());
        drop(claim);
        assert!(caches.claim_refresh(id).is_some());
    }

    // Commit ids sort any way; the rows must still follow acceptance.
    #[test]
    fn sequence_keeps_order() {
        let caches = GitCaches::default();
        let first = caches.next_sequence().expect("an id");
        let second = caches.next_sequence().expect("an id");
        assert!(first < second);
    }

    // A push key is valid only while its push runs, and only exactly.
    #[test]
    fn push_key_scoped() {
        let caches = GitCaches::default();
        let id = Ulid::from(4);
        let key = caches.open_push(id, "secret".to_string());
        assert_eq!(key.value(), "secret");
        assert!(caches.push_key_valid(id, "secret"));
        assert!(!caches.push_key_valid(id, "other"));
        assert!(!caches.push_key_valid(Ulid::from(5), "secret"));
        drop(key);
        assert!(!caches.push_key_valid(id, "secret"));
    }
}
