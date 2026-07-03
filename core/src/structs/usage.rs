use crate::errors::ConversionError;
use crate::types::GroupId;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const USAGE_GLOBAL_KEY: &[u8] = b"global";
pub const USAGE_GLOBAL_SHARD_COUNT: usize = 64;

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
}
