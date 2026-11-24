//! Notices that tell stateless edges to drop cached host decisions, and the feed's cursor.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::errors::ConversionError;
use crate::types::Key;

const NOTICE_ROW: u8 = 2;
/// How long a notice is kept; an older cursor makes the edge drop its whole cache.
pub const NOTICE_KEEP_MS: u64 = 3_600_000;

/// The hosts whose cached decisions an edge must drop.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Notice {
    pub applet_id: Ulid,
    pub hosts: Vec<String>,
}

impl Notice {
    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// The row of one notice in `APPLETS_KEYSPACE`, in time order.
pub fn notice_key(at_ms: u64, applet_id: Ulid, revision: u64) -> Key {
    let mut row = notice_start(at_ms).to_vec();
    row.extend_from_slice(&applet_id.to_bytes());
    row.extend_from_slice(&revision.to_be_bytes());
    Key::from(row)
}

/// The prefix of every notice row.
pub fn notice_prefix() -> Key {
    Key::from(vec![NOTICE_ROW])
}

/// A key below every notice written at `at_ms` or later.
pub fn notice_start(at_ms: u64) -> Key {
    let mut row = vec![NOTICE_ROW];
    row.extend_from_slice(&at_ms.to_be_bytes());
    Key::from(row)
}

/// Where an edge stands in the feed: the last notice row it saw and the feature policy then.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
    pub after: Key,
    pub policy: u64,
}

impl Cursor {
    pub fn encode(&self) -> String {
        format!("{}.{:016x}", hex::encode(&self.after), self.policy)
    }

    /// The cursor an edge sent; anything malformed is `None`, which resets the edge.
    pub fn parse(text: &str) -> Option<Self> {
        let (after, policy) = text.split_once('.')?;
        let after = hex::decode(after).ok()?;
        let valid = after.len() >= 9 && after.first() == Some(&NOTICE_ROW);
        valid.then_some(Self {
            after: Key::from(after),
            policy: u64::from_str_radix(policy, 16).ok()?,
        })
    }

    /// The time of the position the cursor names.
    pub fn at_ms(&self) -> u64 {
        let mut time = [0u8; 8];
        time.copy_from_slice(&self.after[1..9]);
        u64::from_be_bytes(time)
    }
}

/// A short digest of the feature policy an edge's decisions were made under.
pub fn policy_mark<T: Serialize>(policy: &T) -> Result<u64, ConversionError> {
    let bytes = postcard::to_allocvec(policy)?;
    let mut mark = [0u8; 8];
    mark.copy_from_slice(&blake3::hash(&bytes).as_bytes()[..8]);
    Ok(u64::from_be_bytes(mark))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips() {
        let key = notice_key(42, Ulid::from_bytes([1; 16]), 7);
        assert!(key > notice_start(42) && key < notice_start(43));
        let cursor = Cursor {
            after: key,
            policy: policy_mark(&("applets", true)).unwrap(),
        };
        let parsed = Cursor::parse(&cursor.encode()).unwrap();
        assert_eq!(parsed, cursor);
        assert_eq!(parsed.at_ms(), 42);
        assert_ne!(cursor.policy, policy_mark(&("applets", false)).unwrap());
        for refused in ["", "zz.00", "00.00", "0100.zz"] {
            assert!(Cursor::parse(refused).is_none(), "{refused}");
        }
    }
}
