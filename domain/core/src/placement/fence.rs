//! Provides the per-bucket write fence that admits writes at the bucket activation epoch.
//! A writer reads the fence in its commit, so a holder close conflicts with pending writes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::keyspaces::WRITE_FENCE_KEYSPACE;
use crate::storage_entries::placement_fence_key;
use crate::structs::identity::realm::{RealmConfigDocument, RealmId};
use crate::structs::placement::record::PlacementRef;
use crate::types::{Key, Value};

/// Generation a write of `placement` is admitted at: the bucket's activation
/// epoch. `None` while the bucket has no usable activation, which is bootstrap
/// before the first candidate map, where no transition can be in flight.
pub fn write_generation(config: &RealmConfigDocument, placement: &PlacementRef) -> Option<u64> {
    if *placement == PlacementRef::NIL {
        return None;
    }
    config
        .activation(&placement.strategy_id, placement.shard)
        .map(|activation| activation.activation_epoch)
}

/// The bucket's fence read, addressed for a batch read inside a transaction.
pub fn fence_read(realm_id: &RealmId, placement: &PlacementRef) -> (String, Key) {
    (
        WRITE_FENCE_KEYSPACE.to_string(),
        placement_fence_key(realm_id, placement),
    )
}

/// The generation the stored fence value closes, or `None` when the bytes do
/// not parse. Only `close` may act on the difference: every other reader must
/// treat an unparseable value as closed rather than reopen the bucket.
pub fn stored_generation(value: Option<&Value>) -> Option<u64> {
    match value {
        None => Some(0),
        Some(bytes) => <[u8; 8]>::try_from(bytes.as_ref())
            .map(u64::from_be_bytes)
            .ok(),
    }
}

/// The highest generation the stored fence value closes. A truncated value
/// reads as closed at every generation, never as an open bucket.
pub fn closed_generation(value: Option<&Value>) -> u64 {
    stored_generation(value).unwrap_or(u64::MAX)
}

/// Whether a write that resolved holders at `generation` may still commit.
pub fn admits(value: Option<&Value>, generation: u64) -> bool {
    generation > closed_generation(value)
}

/// Buckets a write publishes onto, each with the activation generation it
/// resolved at. The write reads them inside its own transaction, so a
/// departing holder's close either rejects it or conflicts its commit.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WriteFence {
    buckets: Vec<(RealmId, PlacementRef, u64)>,
}

impl WriteFence {
    /// Adds the buckets `placements` resolves to in `config`. A bucket without
    /// a usable activation is skipped: that is bootstrap before the realm's
    /// first candidate map, where no transition can be in flight.
    pub fn add(
        &mut self,
        realm_id: RealmId,
        config: &RealmConfigDocument,
        placements: impl IntoIterator<Item = PlacementRef>,
    ) {
        for placement in placements {
            let Some(generation) = write_generation(config, &placement) else {
                continue;
            };
            if !self
                .buckets
                .iter()
                .any(|(realm, bucket, _)| *realm == realm_id && *bucket == placement)
            {
                self.buckets.push((realm_id, placement, generation));
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Fence reads to append to a batch read inside the write transaction.
    pub fn reads(&self) -> Vec<(String, Key)> {
        self.buckets
            .iter()
            .map(|(realm_id, placement, _)| fence_read(realm_id, placement))
            .collect()
    }

    /// Generation to stamp on a row publishing onto `placement`.
    pub fn generation(&self, realm_id: &RealmId, placement: &PlacementRef) -> u64 {
        self.buckets
            .iter()
            .find(|(realm, bucket, _)| realm == realm_id && bucket == placement)
            .map_or(0, |(_, _, generation)| *generation)
    }

    /// Whether every bucket still admits the write, given the fence values in
    /// `reads` order. A short or long answer is never admitted.
    pub fn admits(&self, values: &[(Key, Option<Value>)]) -> bool {
        values.len() == self.buckets.len()
            && self
                .buckets
                .iter()
                .zip(values)
                .all(|((_, _, generation), (_, value))| admits(value.as_ref(), *generation))
    }
}

#[cfg(test)]
mod tests {
    use ulid::Ulid;

    use super::*;

    fn placement() -> PlacementRef {
        PlacementRef {
            strategy_id: Ulid::from_bytes([7; 16]),
            shard: 3,
        }
    }

    #[test]
    fn open_fence_admits() {
        assert!(admits(None, 1));
        let closed = Value::from(2u64.to_be_bytes().to_vec());
        assert_eq!(closed_generation(Some(&closed)), 2);
        assert!(!admits(Some(&closed), 1));
        assert!(!admits(Some(&closed), 2));
        assert!(admits(Some(&closed), 3));
        // A malformed value must never reopen the bucket.
        let malformed = Value::from(vec![1u8]);
        assert_eq!(closed_generation(Some(&malformed)), u64::MAX);
        assert!(!admits(Some(&malformed), 1));
    }

    #[test]
    fn fence_reads_buckets() {
        // A repeated bucket reads once, and a short or long answer is never
        // admitted: the answer must line up with the reads it was asked for.
        let realm_id = RealmId::from_bytes([11; 32]);
        let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config
            .strategies
            .push(crate::structs::placement::record::PlacementStrategy {
                strategy_id: placement().strategy_id,
                name: "default".to_string(),
                replica_count: Some(1),
                distinct_locations: false,
                affinity: Vec::new(),
                shard_count: 16,
            });
        config.snapshot_candidate_map();

        let mut fence = WriteFence::default();
        fence.add(
            realm_id,
            &config,
            [placement(), placement(), PlacementRef::NIL],
        );
        assert_eq!(fence.reads().len(), 1);
        assert_eq!(fence.generation(&realm_id, &placement()), 1);
        assert_eq!(fence.generation(&realm_id, &PlacementRef::NIL), 0);

        let key = fence.reads()[0].1.clone();
        assert!(fence.admits(&[(key.clone(), None)]));
        assert!(!fence.admits(&[]));
        assert!(!fence.admits(&[(key.clone(), None), (key.clone(), None)]));
        assert!(!fence.admits(&[(key, Some(Value::from(1u64.to_be_bytes().to_vec())))]));
    }
}
