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
    /// Uploads opened before the fence may still complete; closing this ends the grace period.
    pub uploads_open: bool,
}

/// What this node fenced for one set. Installing a snapshot never clears it.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocalFence {
    pub ranges: Vec<RangeFence>,
    /// The deletion identity this node fenced every range of the set for.
    pub deletion: Option<[u8; 32]>,
    /// This member applies no incoming publication: it leaves or the set is deleted.
    pub closed: bool,
    /// The byte copies each range had when this node fenced it; its own copy goes only once
    /// the range has as many again without it.
    pub copies: Vec<(Ulid, u32)>,
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
        self.check(snapshot, range, epoch, false)
    }

    /// Whether the completion of an open upload may commit: as [`Self::check_write`], except
    /// that a range fence still in its upload grace period lets it pass.
    pub fn check_completion(
        &self,
        snapshot: &DistributionSnapshot,
        range: &KeyRange,
        epoch: u64,
    ) -> Result<(), RouteError> {
        self.check(snapshot, range, epoch, true)
    }

    fn check(
        &self,
        snapshot: &DistributionSnapshot,
        range: &KeyRange,
        epoch: u64,
        completion: bool,
    ) -> Result<(), RouteError> {
        if self.deletion.is_some() {
            return Err(RouteError::Deleting(snapshot.set_id()));
        }
        let fenced = (self.ranges.iter()).any(|fence| {
            !(completion && fence.uploads_open)
                && fence.epoch >= epoch
                && snapshot.descends(range, fence.range_id)
        });
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

    /// The byte copies `range_id` had when it was fenced here; 0 when not recorded.
    pub fn copies_of(&self, range_id: Ulid) -> u32 {
        (self.copies.iter())
            .find(|(fenced, _)| *fenced == range_id)
            .map_or(0, |(_, copies)| *copies)
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

    /// Ends the upload grace period of `range_id`; `false` when it was already over.
    pub fn close_uploads(&mut self, range_id: Ulid) -> bool {
        let fence = (self.ranges.iter_mut()).find(|fence| fence.range_id == range_id);
        fence.is_some_and(|fence| std::mem::replace(&mut fence.uploads_open, false))
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

/// Where the streaming purge of one range of a member bucket continues.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PurgeCursor {
    /// Every open upload of the range was aborted.
    pub uploads_done: bool,
    /// The last position of the current inventory walk that was purged, inclusive.
    pub after: Option<Vec<u8>>,
    /// The range holds nothing of the set anymore.
    pub done: bool,
}

impl PurgeCursor {
    /// The cursor key: set id, then range id.
    pub fn key(set_id: Ulid, range_id: Ulid) -> Vec<u8> {
        let mut key = set_id.to_bytes().to_vec();
        key.extend_from_slice(&range_id.to_bytes());
        key
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn decode(value: Option<&[u8]>) -> Result<Self, ConversionError> {
        value.map_or_else(|| Ok(Self::default()), |v| Ok(postcard::from_bytes(v)?))
    }
}

/// Where clearing what a node keeps of a set continues: the keyspace step and the last key
/// it deleted there. It sorts before the set's purge cursors, which share its prefix.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClearCursor {
    pub step: u8,
    pub after: Option<Vec<u8>>,
}

impl ClearCursor {
    /// The cursor key: the set id alone.
    pub fn key(set_id: Ulid) -> Vec<u8> {
        set_id.to_bytes().to_vec()
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn decode(value: Option<&[u8]>) -> Result<Self, ConversionError> {
        value.map_or_else(|| Ok(Self::default()), |v| Ok(postcard::from_bytes(v)?))
    }
}

#[cfg(test)]
#[path = "fence_tests.rs"]
mod tests;
