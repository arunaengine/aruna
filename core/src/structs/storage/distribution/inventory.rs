//! Encodes the ordered inventory of a distributed bucket: key-ordered head and version rows,
//! the upload inventory and the version id index, all node-local and keyed by set id.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use ulid::Ulid;

use super::OriginalEpoch;
use crate::errors::ConversionError;
use crate::structs::storage::blob::{BlobVersion, BlobVersionState};

/// Ends an escaped key; it sorts before every continuation of the key.
const TERMINATOR: [u8; 2] = [0x00, 0x01];
/// An escaped zero byte inside a key.
const ESCAPED_ZERO: [u8; 2] = [0x00, 0xFF];
const HEAD_TAG: u8 = 0x00;
const VERSION_TAG: u8 = 0x01;
/// No escaped key starts with this byte, so it bounds every position of a set.
const SET_END: u8 = 0xFF;

/// Appends `key` so that byte order of the encodings is key order, and a key's rows sort
/// before the rows of every longer key it is a prefix of.
fn push_key(out: &mut Vec<u8>, key: &str) {
    for byte in key.as_bytes() {
        match byte {
            0 => out.extend_from_slice(&ESCAPED_ZERO),
            byte => out.push(*byte),
        }
    }
}

fn set_prefix(set_id: Ulid, capacity: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + capacity);
    out.extend_from_slice(&set_id.to_bytes());
    out
}

fn key_prefix(set_id: Ulid, key: &str) -> Vec<u8> {
    let mut out = set_prefix(set_id, key.len() + 40);
    push_key(&mut out, key);
    out.extend_from_slice(&TERMINATOR);
    out
}

/// The position of a key's head row; it sorts before every version of the key.
pub fn head_position(set_id: Ulid, key: &str) -> Vec<u8> {
    let mut out = key_prefix(set_id, key);
    out.push(HEAD_TAG);
    out
}

/// Where a version sorts: its key, then original epoch and generation descending, then id.
pub fn version_position(
    set_id: Ulid,
    key: &str,
    epoch: OriginalEpoch,
    generation: u64,
    version_id: Ulid,
) -> Vec<u8> {
    let mut out = key_prefix(set_id, key);
    out.push(VERSION_TAG);
    out.extend_from_slice(&(u64::MAX - epoch.0).to_be_bytes());
    out.extend_from_slice(&(u64::MAX - generation).to_be_bytes());
    out.extend_from_slice(&version_id.to_bytes());
    out
}

/// The position of an open multipart upload in the upload inventory.
pub fn upload_position(set_id: Ulid, key: &str, upload_id: Ulid) -> Vec<u8> {
    let mut out = key_prefix(set_id, key);
    out.extend_from_slice(&upload_id.to_bytes());
    out
}

/// The first position of the keys at or after `bound`, in either inventory.
pub fn bound_position(set_id: Ulid, bound: &str) -> Vec<u8> {
    let mut out = set_prefix(set_id, bound.len());
    push_key(&mut out, bound);
    out
}

/// The positions of a range `[lower, upper)`: inclusive start, exclusive end.
pub fn range_interval(set_id: Ulid, lower: &str, upper: Option<&str>) -> (Vec<u8>, Vec<u8>) {
    let end = match upper {
        Some(upper) => bound_position(set_id, upper),
        None => {
            let mut end = set_prefix(set_id, 1);
            end.push(SET_END);
            end
        }
    };
    (bound_position(set_id, lower), end)
}

/// The version id index key of a version in a set.
pub fn index_key(set_id: Ulid, version_id: Ulid) -> Vec<u8> {
    let mut out = set_prefix(set_id, 16);
    out.extend_from_slice(&version_id.to_bytes());
    out
}

/// What one inventory row is, read back from its position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Entry {
    Head,
    Version {
        epoch: OriginalEpoch,
        generation: u64,
        version_id: Ulid,
    },
    Upload(Ulid),
}

/// A decoded position: its set, key and row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Position {
    pub set_id: Ulid,
    pub key: String,
    pub entry: Entry,
}

impl Position {
    /// Decodes a position of the ordered or the upload inventory.
    pub fn decode(bytes: &[u8]) -> Result<Self, ConversionError> {
        let invalid = || ConversionError::InvalidLength("inventory position".to_string());
        let set_id = Ulid::from_bytes(bytes.get(..16).ok_or_else(invalid)?.try_into()?);
        let mut key = Vec::new();
        let mut rest = &bytes[16..];
        loop {
            match rest {
                [0x00, 0x01, tail @ ..] => {
                    rest = tail;
                    break;
                }
                [0x00, 0xFF, tail @ ..] => {
                    key.push(0);
                    rest = tail;
                }
                [0x00, ..] | [] => return Err(invalid()),
                [byte, tail @ ..] => {
                    key.push(*byte);
                    rest = tail;
                }
            }
        }
        let word = |slice: &[u8]| -> Result<u64, ConversionError> {
            Ok(u64::from_be_bytes(slice.try_into()?))
        };
        let entry = match rest {
            [HEAD_TAG] => Entry::Head,
            [VERSION_TAG, tail @ ..] if tail.len() == 32 => Entry::Version {
                epoch: OriginalEpoch(u64::MAX - word(&tail[..8])?),
                generation: u64::MAX - word(&tail[8..16])?,
                version_id: Ulid::from_bytes(tail[16..].try_into()?),
            },
            tail if tail.len() == 16 => Entry::Upload(Ulid::from_bytes(tail.try_into()?)),
            _ => return Err(invalid()),
        };
        Ok(Self {
            set_id,
            key: String::from_utf8(key)?,
            entry,
        })
    }
}

/// What a version row contributes to its range's counters, and its logical content.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct VersionEntry {
    pub marker: bool,
    pub size: u64,
    /// The bytes are reached through an external reference, not stored here.
    pub referenced: bool,
    /// The logical content identity from [`content_identity`]; `None` while it is unknown.
    pub identity: Option<[u8; 32]>,
}

impl VersionEntry {
    /// Two entries of one version id may differ only while one identity is unknown.
    pub fn same_content(&self, other: &Self) -> bool {
        match (self.identity, other.identity) {
            (Some(left), Some(right)) => left == right,
            _ => true,
        }
    }
}

const IDENTITY_DOMAIN: &[u8] = b"aruna-reference-identity-v1";

/// What one version holds, independent of where its bytes are: the BLAKE3 of stored bytes,
/// or a digest of the observed source object of a reference. Markers and archives whose
/// hash is not known yet have none.
pub fn content_identity(version: &BlobVersion) -> Option<[u8; 32]> {
    match &version.state {
        BlobVersionState::Materialized { blob_hash, .. } => Some(*blob_hash),
        BlobVersionState::Reference {
            source,
            cached_metadata,
            ..
        } => {
            let descriptor = &source.descriptor;
            let observed = (
                &descriptor.kind,
                &descriptor.source_path,
                &descriptor.version_selector,
                &cached_metadata.etag,
                cached_metadata.content_length,
            );
            let bytes = postcard::to_allocvec(&observed).ok()?;
            let mut hasher = blake3::Hasher::new();
            hasher.update(IDENTITY_DOMAIN);
            hasher.update(&bytes);
            Some(*hasher.finalize().as_bytes())
        }
        BlobVersionState::Deleted | BlobVersionState::PendingContent { .. } => None,
    }
}

/// A key's head row: the current version and what it contributes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HeadEntry {
    pub version_id: Ulid,
    pub epoch: OriginalEpoch,
    pub generation: u64,
    pub version: VersionEntry,
}

/// A version id index row: where the version sorts and what its row contributes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexEntry {
    pub position: Vec<u8>,
    pub version: VersionEntry,
}

macro_rules! postcard_codec {
    ($($kind:ty),+) => {$(
        impl $kind {
            pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
                Ok(postcard::to_allocvec(self)?)
            }

            pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
                Ok(postcard::from_bytes(bytes)?)
            }
        }
    )+};
}

postcard_codec!(VersionEntry, HeadEntry, IndexEntry);

#[cfg(test)]
mod tests {
    use super::*;

    fn set() -> Ulid {
        Ulid::from_bytes([7; 16])
    }

    fn id(seed: u8) -> Ulid {
        Ulid::from_bytes([seed; 16])
    }

    #[test]
    fn orders_by_key() {
        let keys = ["", "a", "a\0", "a\0b", "a/", "ab", "b", "\u{10FFFF}"];
        let heads: Vec<_> = keys.iter().map(|key| head_position(set(), key)).collect();
        assert!(heads.windows(2).all(|pair| pair[0] < pair[1]));
        let versions: Vec<_> = (keys.iter())
            .map(|key| version_position(set(), key, OriginalEpoch(0), 0, id(0)))
            .collect();
        assert!(versions.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(
            head_position(set(), "a") < version_position(set(), "a", OriginalEpoch(9), 9, id(9))
        );
        assert!(
            version_position(set(), "a", OriginalEpoch(0), 0, id(0)) < head_position(set(), "a0")
        );
    }

    #[test]
    fn orders_history_newest_first() {
        let position = |epoch, generation, seed| {
            version_position(set(), "k", OriginalEpoch(epoch), generation, id(seed))
        };
        assert!(
            position(2, 1, 0) < position(1, 9, 0),
            "a later epoch sorts first"
        );
        assert!(
            position(1, 5, 0) < position(1, 4, 0),
            "then a later generation"
        );
        assert!(position(1, 4, 1) < position(1, 4, 2), "then the version id");
    }

    #[test]
    fn bounds_contain_range() {
        let (start, end) = range_interval(set(), "b", Some("d"));
        for key in ["b", "b\0", "c", "czz"] {
            let head = head_position(set(), key);
            assert!(start <= head && head < end, "{key:?} is inside [b, d)");
        }
        for key in ["a", "a\u{10FFFF}", "d", "d\0"] {
            let head = head_position(set(), key);
            assert!(head < start || head >= end, "{key:?} is outside [b, d)");
        }
        let (_, open) = range_interval(set(), "", None);
        let last = version_position(set(), "\u{10FFFF}\0", OriginalEpoch(0), 0, id(255));
        assert!(last < open && open < head_position(Ulid::from_bytes([8; 16]), ""));
    }

    #[test]
    fn decodes_positions() {
        let version = version_position(set(), "a\0b", OriginalEpoch(3), 4, id(5));
        assert_eq!(
            Position::decode(&version).unwrap(),
            Position {
                set_id: set(),
                key: "a\0b".to_string(),
                entry: Entry::Version {
                    epoch: OriginalEpoch(3),
                    generation: 4,
                    version_id: id(5)
                },
            }
        );
        let head = Position::decode(&head_position(set(), "")).unwrap();
        assert_eq!((head.key.as_str(), head.entry), ("", Entry::Head));
        let upload = Position::decode(&upload_position(set(), "k", id(2))).unwrap();
        assert_eq!(upload.entry, Entry::Upload(id(2)));
        assert!(Position::decode(&bound_position(set(), "k")).is_err());
    }
}
