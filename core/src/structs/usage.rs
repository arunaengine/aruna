use crate::errors::ConversionError;
use crate::types::GroupId;
use serde::{Deserialize, Serialize};

pub const USAGE_GLOBAL_KEY: &[u8] = b"global";

pub fn usage_group_key(group_id: GroupId) -> Vec<u8> {
    let mut key = Vec::with_capacity(6 + 16);
    key.extend_from_slice(b"group/");
    key.extend_from_slice(&group_id.to_bytes());
    key
}

/// Maintained usage aggregates. `stored_*` fields track physical,
/// content-addressed blobs and are only meaningful on the global counter;
/// `logical_bytes` sums live version sizes and is the per-group quota basis.
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

    pub fn apply(&mut self, delta: &UsageDelta) {
        self.buckets = apply_delta(self.buckets, delta.buckets);
        self.objects = apply_delta(self.objects, delta.objects);
        self.stored_blobs = apply_delta(self.stored_blobs, delta.stored_blobs);
        self.stored_bytes = apply_delta(self.stored_bytes, delta.stored_bytes);
        self.logical_bytes = apply_delta(self.logical_bytes, delta.logical_bytes);
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

fn apply_delta(value: u64, delta: i64) -> u64 {
    if delta >= 0 {
        value.saturating_add(delta as u64)
    } else {
        value.saturating_sub(delta.unsigned_abs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_saturates_at_zero() {
        let mut counters = UsageCounters {
            objects: 1,
            ..Default::default()
        };
        counters.apply(&UsageDelta {
            objects: -5,
            logical_bytes: 10,
            ..Default::default()
        });
        assert_eq!(counters.objects, 0);
        assert_eq!(counters.logical_bytes, 10);
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
