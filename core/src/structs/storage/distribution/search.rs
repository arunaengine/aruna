//! Encodes the search index state of a range and the feed rows that carry head changes of a
//! distributed bucket to the range search indexes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};

use super::inventory::HeadEntry;
use crate::errors::ConversionError;

/// The search index of one range on this member, keyed like the range's account row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IndexState {
    /// A new generation is filled from the inventory heads; `cursor` is the last head
    /// position written to it. Queries treat the range as incomplete.
    Building {
        generation: u64,
        cursor: Option<Vec<u8>>,
    },
    /// The generation holds every head of the range and the feed keeps it current.
    Ready { generation: u64 },
}

impl IndexState {
    pub fn generation(&self) -> u64 {
        match self {
            Self::Building { generation, .. } | Self::Ready { generation } => *generation,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// The feed row value of a head change: the key's head after the change, `None` once the key
/// has none. Equal values mean equal heads, so a feeder may drop a row it saw unchanged.
pub fn feed_value(head: Option<&HeadEntry>) -> Result<Vec<u8>, ConversionError> {
    Ok(postcard::to_allocvec(&head)?)
}

pub fn decode_feed(bytes: &[u8]) -> Result<Option<HeadEntry>, ConversionError> {
    Ok(postcard::from_bytes(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::storage::distribution::OriginalEpoch;
    use crate::structs::storage::distribution::inventory::VersionEntry;
    use ulid::Ulid;

    #[test]
    fn feed_round_trips() {
        let head = HeadEntry {
            version_id: Ulid::from_bytes([1; 16]),
            epoch: OriginalEpoch(2),
            generation: 3,
            version: VersionEntry::default(),
        };
        let value = feed_value(Some(&head)).unwrap();
        assert_eq!(decode_feed(&value).unwrap(), Some(head));
        assert_eq!(decode_feed(&feed_value(None).unwrap()).unwrap(), None);
        assert_ne!(value, feed_value(None).unwrap());
    }
}
