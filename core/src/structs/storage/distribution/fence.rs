//! Encodes the local fence row of one set. Every publication transaction for the set reads it,
//! so setting a fence conflicts every write that started before and commits after it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::{DistributionSnapshot, KeyRange, RouteError};
use crate::errors::ConversionError;

/// A range this node fenced as its coordinator for a transfer; it never writes the range again
/// at the fenced epoch or below, whatever record it later holds.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RangeFence {
    pub range_id: Ulid,
    pub transfer_id: Ulid,
    pub epoch: u64,
    /// Unix milliseconds when the fence was set.
    pub since_ms: u64,
}

/// What this node fenced for one set. Installing a snapshot never clears it.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalFence {
    pub ranges: Vec<RangeFence>,
    /// The deletion identity this node fenced every range of the set for.
    pub deletion: Option<[u8; 32]>,
    /// This member applies no incoming publication: it leaves or the set is deleted.
    pub closed: bool,
}

impl LocalFence {
    /// The fence row key: the set id.
    pub fn key(set_id: Ulid) -> Vec<u8> {
        set_id.to_bytes().to_vec()
    }

    /// Whether a write to `range` at `epoch` may commit: no deletion fence and no range fence
    /// at that epoch or above on the range or a range it was split from.
    pub fn check_write(
        &self,
        snapshot: &DistributionSnapshot,
        range: &KeyRange,
        epoch: u64,
    ) -> Result<(), RouteError> {
        if self.deletion.is_some() {
            return Err(RouteError::Deleting(snapshot.set_id()));
        }
        let fenced = (self.ranges.iter())
            .any(|fence| fence.epoch >= epoch && snapshot.descends(range, fence.range_id));
        match fenced {
            true => Err(RouteError::Moving(range.range_id)),
            false => Ok(()),
        }
    }

    /// Whether this member still applies publications from coordinators.
    pub fn receiving(&self) -> bool {
        !self.closed && self.deletion.is_none()
    }

    pub fn range(&self, range_id: Ulid) -> Option<&RangeFence> {
        self.ranges.iter().find(|fence| fence.range_id == range_id)
    }

    /// Fences `range_id` for `transfer_id`; fencing it again for the same transfer is a no-op.
    pub fn fence_range(&mut self, fence: RangeFence) -> bool {
        if (self.ranges.iter())
            .any(|known| known.range_id == fence.range_id && known.epoch >= fence.epoch)
        {
            return false;
        }
        self.ranges.retain(|known| known.range_id != fence.range_id);
        self.ranges.push(fence);
        true
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Decodes a stored row; an absent row is an empty fence.
    pub fn decode(value: Option<&[u8]>) -> Result<Self, ConversionError> {
        value.map_or_else(|| Ok(Self::default()), Self::from_bytes)
    }
}

#[cfg(test)]
#[path = "fence_tests.rs"]
mod tests;
