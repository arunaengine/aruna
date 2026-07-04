use crate::NodeId;
use crate::errors::ConversionError;
use crate::types::GroupId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const USAGE_GLOBAL_KEY: &[u8] = b"global";
pub const USAGE_GLOBAL_SHARD_COUNT: usize = 64;

/// Keys in the node-usage keyspace. Per-node snapshots use fixed-length keys so
/// their prefixes are unambiguous: `n/` + node id for a node's global total,
/// `g/` + group id + node id for a node's per-group total (group first so all
/// nodes' snapshots for one group form a single scan range). Local-only keys use
/// text prefixes (`dirty/`, `summary/`) that never collide with the binary
/// snapshot prefixes.
pub const NODE_USAGE_GLOBAL_PREFIX: &[u8] = b"n/";
pub const NODE_USAGE_GROUP_PREFIX: &[u8] = b"g/";
pub const NODE_USAGE_DIRTY_PREFIX: &[u8] = b"dirty/";
pub const NODE_USAGE_DIRTY_GLOBAL_KEY: &[u8] = b"dirty/global";
pub const NODE_USAGE_DIRTY_GROUP_PREFIX: &[u8] = b"dirty/group/";
pub const NODE_USAGE_SUMMARY_GLOBAL_KEY: &[u8] = b"summary/global";
pub const NODE_USAGE_SUMMARY_GROUP_PREFIX: &[u8] = b"summary/group/";

/// A single node's usage total distributed over the sync layer. Single writer
/// per key (each node writes only its own snapshots), so ingest is last-write-wins.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeUsageSnapshot {
    pub node_id: NodeId,
    pub counters: UsageCounters,
}

impl NodeUsageSnapshot {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

pub fn node_usage_global_key(node_id: NodeId) -> Vec<u8> {
    let mut key = Vec::with_capacity(NODE_USAGE_GLOBAL_PREFIX.len() + 32);
    key.extend_from_slice(NODE_USAGE_GLOBAL_PREFIX);
    key.extend_from_slice(node_id.as_bytes());
    key
}

pub fn node_usage_group_prefix(group_id: GroupId) -> Vec<u8> {
    let mut key = Vec::with_capacity(NODE_USAGE_GROUP_PREFIX.len() + 16);
    key.extend_from_slice(NODE_USAGE_GROUP_PREFIX);
    key.extend_from_slice(&group_id.to_bytes());
    key
}

pub fn node_usage_group_key(group_id: GroupId, node_id: NodeId) -> Vec<u8> {
    let mut key = node_usage_group_prefix(group_id);
    key.extend_from_slice(node_id.as_bytes());
    key
}

/// Recovers the owning node id from a snapshot key produced by
/// [`node_usage_global_key`] or [`node_usage_group_key`].
pub fn node_usage_key_node_id(key: &[u8]) -> Option<NodeId> {
    let tail = match key.strip_prefix(NODE_USAGE_GLOBAL_PREFIX) {
        Some(rest) => rest,
        None => key.strip_prefix(NODE_USAGE_GROUP_PREFIX)?.get(16..)?,
    };
    let bytes: [u8; 32] = tail.try_into().ok()?;
    NodeId::from_bytes(&bytes).ok()
}

pub fn node_usage_dirty_group_key(group_id: GroupId) -> Vec<u8> {
    let mut key = Vec::with_capacity(NODE_USAGE_DIRTY_GROUP_PREFIX.len() + 16);
    key.extend_from_slice(NODE_USAGE_DIRTY_GROUP_PREFIX);
    key.extend_from_slice(&group_id.to_bytes());
    key
}

/// Recovers the group id from a `dirty/group/<ulid>` marker key.
pub fn node_usage_dirty_group_id(key: &[u8]) -> Option<GroupId> {
    let tail = key.strip_prefix(NODE_USAGE_DIRTY_GROUP_PREFIX)?;
    let bytes: [u8; 16] = tail.try_into().ok()?;
    Some(GroupId::from_bytes(bytes))
}

pub fn node_usage_summary_group_key(group_id: GroupId) -> Vec<u8> {
    let mut key = Vec::with_capacity(NODE_USAGE_SUMMARY_GROUP_PREFIX.len() + 16);
    key.extend_from_slice(NODE_USAGE_SUMMARY_GROUP_PREFIX);
    key.extend_from_slice(&group_id.to_bytes());
    key
}

pub fn usage_global_shard_index(group_id: GroupId) -> usize {
    group_id
        .to_bytes()
        .iter()
        .fold(0u8, |shard, byte| shard ^ byte) as usize
        % USAGE_GLOBAL_SHARD_COUNT
}

pub fn usage_global_shard_key(shard: usize) -> Vec<u8> {
    format!("global/{shard:02}").into_bytes()
}

pub fn usage_global_key_for_group(group_id: GroupId) -> Vec<u8> {
    usage_global_shard_key(usage_global_shard_index(group_id))
}

pub fn usage_global_shard_keys() -> Vec<Vec<u8>> {
    (0..USAGE_GLOBAL_SHARD_COUNT)
        .map(usage_global_shard_key)
        .collect()
}

pub fn usage_group_key(group_id: GroupId) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + 16);
    key.extend_from_slice(b"group/");
    key.extend_from_slice(&group_id.to_bytes());
    key
}

/// Maintained usage aggregates. `stored_*` fields track physical,
/// content-addressed blobs and are only meaningful on the global counter;
/// `logical_bytes` sums materialized version sizes and is the per-group quota basis.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct UsageCounters {
    pub buckets: u64,
    pub objects: u64,
    pub stored_blobs: u64,
    pub stored_bytes: u64,
    pub logical_bytes: u64,
}

impl UsageCounters {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn apply(&mut self, delta: &UsageDelta) -> Result<(), UsageCounterError> {
        let buckets = apply_delta("buckets", self.buckets, delta.buckets)?;
        let objects = apply_delta("objects", self.objects, delta.objects)?;
        let stored_blobs = apply_delta("stored_blobs", self.stored_blobs, delta.stored_blobs)?;
        let stored_bytes = apply_delta("stored_bytes", self.stored_bytes, delta.stored_bytes)?;
        let logical_bytes = apply_delta("logical_bytes", self.logical_bytes, delta.logical_bytes)?;

        self.buckets = buckets;
        self.objects = objects;
        self.stored_blobs = stored_blobs;
        self.stored_bytes = stored_bytes;
        self.logical_bytes = logical_bytes;
        Ok(())
    }

    pub fn add(&mut self, other: &Self) -> Result<(), UsageCounterError> {
        let buckets = add_counter("buckets", self.buckets, other.buckets)?;
        let objects = add_counter("objects", self.objects, other.objects)?;
        let stored_blobs = add_counter("stored_blobs", self.stored_blobs, other.stored_blobs)?;
        let stored_bytes = add_counter("stored_bytes", self.stored_bytes, other.stored_bytes)?;
        let logical_bytes = add_counter("logical_bytes", self.logical_bytes, other.logical_bytes)?;

        self.buckets = buckets;
        self.objects = objects;
        self.stored_blobs = stored_blobs;
        self.stored_bytes = stored_bytes;
        self.logical_bytes = logical_bytes;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UsageDelta {
    pub buckets: i64,
    pub objects: i64,
    pub stored_blobs: i64,
    pub stored_bytes: i64,
    pub logical_bytes: i64,
}

impl UsageDelta {
    pub fn is_zero(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum UsageCounterError {
    #[error("usage counter {field} would underflow: {value} + ({delta})")]
    Underflow {
        field: &'static str,
        value: u64,
        delta: i64,
    },
    #[error("usage counter {field} would overflow: {value} + {delta}")]
    Overflow {
        field: &'static str,
        value: u64,
        delta: u64,
    },
}

fn apply_delta(field: &'static str, value: u64, delta: i64) -> Result<u64, UsageCounterError> {
    if delta >= 0 {
        add_counter(field, value, delta as u64)
    } else {
        let amount = delta.unsigned_abs();
        value
            .checked_sub(amount)
            .ok_or(UsageCounterError::Underflow {
                field,
                value,
                delta,
            })
    }
}

fn add_counter(field: &'static str, value: u64, delta: u64) -> Result<u64, UsageCounterError> {
    value.checked_add(delta).ok_or(UsageCounterError::Overflow {
        field,
        value,
        delta,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_rejects_underflow() {
        let mut counters = UsageCounters {
            objects: 1,
            ..Default::default()
        };
        let error = counters.apply(&UsageDelta {
            objects: -5,
            logical_bytes: 10,
            ..Default::default()
        });
        assert!(matches!(
            error,
            Err(UsageCounterError::Underflow {
                field: "objects",
                value: 1,
                delta: -5,
            })
        ));
        assert_eq!(counters.objects, 1);
        assert_eq!(counters.logical_bytes, 0);
    }

    #[test]
    fn apply_updates_all_counters() {
        let mut counters = UsageCounters::default();
        counters
            .apply(&UsageDelta {
                buckets: 1,
                objects: 2,
                stored_blobs: 3,
                stored_bytes: 4,
                logical_bytes: 5,
            })
            .unwrap();

        assert_eq!(counters.buckets, 1);
        assert_eq!(counters.objects, 2);
        assert_eq!(counters.stored_blobs, 3);
        assert_eq!(counters.stored_bytes, 4);
        assert_eq!(counters.logical_bytes, 5);
    }

    #[test]
    fn counters_round_trip() {
        let counters = UsageCounters {
            buckets: 1,
            objects: 2,
            stored_blobs: 3,
            stored_bytes: 4,
            logical_bytes: 5,
        };
        let bytes = counters.to_bytes().unwrap();
        assert_eq!(UsageCounters::from_bytes(&bytes).unwrap(), counters);
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[test]
    fn node_usage_snapshot_round_trips() {
        let snapshot = NodeUsageSnapshot {
            node_id: node(7),
            counters: UsageCounters {
                buckets: 2,
                logical_bytes: 9,
                ..Default::default()
            },
        };
        let bytes = snapshot.to_bytes().unwrap();
        assert_eq!(NodeUsageSnapshot::from_bytes(&bytes).unwrap(), snapshot);
    }

    #[test]
    fn node_usage_keys_recover_node_and_group_ids() {
        let node_id = node(3);
        let group_id = ulid::Ulid::from_bytes([5u8; 16]);

        let global_key = node_usage_global_key(node_id);
        assert!(global_key.starts_with(NODE_USAGE_GLOBAL_PREFIX));
        assert_eq!(node_usage_key_node_id(&global_key), Some(node_id));

        let group_key = node_usage_group_key(group_id, node_id);
        assert!(group_key.starts_with(&node_usage_group_prefix(group_id)));
        assert_eq!(node_usage_key_node_id(&group_key), Some(node_id));

        // Group snapshots for one group form a contiguous scan range.
        assert!(group_key.starts_with(NODE_USAGE_GROUP_PREFIX));

        let dirty_key = node_usage_dirty_group_key(group_id);
        assert_eq!(node_usage_dirty_group_id(&dirty_key), Some(group_id));
        assert_eq!(node_usage_dirty_group_id(NODE_USAGE_DIRTY_GLOBAL_KEY), None);
    }
}
