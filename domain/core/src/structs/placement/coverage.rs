//! The saved count of how many current versions of a bucket carry its placement rules.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::errors::ConversionError;
use crate::id::NodeId;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// This node's count of one bucket's current versions against the bucket rules. Keyed by bucket
/// name; in a distributed bucket it counts only the ranges this node writes.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoverageCount {
    /// Last head key counted; the next page resumes after it.
    pub cursor: Option<Vec<u8>>,
    /// The rules generation every counted page compared against.
    pub generation: u64,
    pub observed: u64,
    /// Delete markers, which carry no rules.
    pub deleted: u64,
    /// Versions that carry none or only some of the rules.
    pub gaps: u64,
    pub registered: u64,
    pub quarantined: u64,
    pub absent: u64,
    pub reference_only: u64,
    pub started_at_ms: u64,
    /// Set once every current version was counted.
    pub finished_at_ms: Option<u64>,
    /// The start this count belongs to; every coordinator a distributed start reached holds it.
    pub run: Option<Ulid>,
}

impl CoverageCount {
    pub fn new(started_at_ms: u64) -> Self {
        Self {
            started_at_ms,
            ..Self::default()
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// What a node signs to start one count run on the other members of a distributed bucket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountRelay {
    pub bucket: String,
    pub set_id: Ulid,
    pub run: Ulid,
}

impl CountRelay {
    /// The signed bytes, behind a prefix that no other signed record of a node starts with.
    pub fn signing_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        let mut bytes = b"aruna-count-relay-v1\0".to_vec();
        bytes.extend(postcard::to_allocvec(self)?);
        Ok(bytes)
    }

    pub fn signed_by(&self, node_id: &NodeId, signature: &iroh::Signature) -> bool {
        (self.signing_bytes()).is_ok_and(|bytes| node_id.verify(&bytes, signature).is_ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A signature holds for its bucket, set and run only, and only under the signing node.
    #[test]
    fn relay_signature_bound() {
        let key = iroh::SecretKey::from_bytes(&[7; 32]);
        let relay = CountRelay {
            bucket: "photos".to_string(),
            set_id: Ulid::from_parts(1, 1),
            run: Ulid::from_parts(2, 2),
        };
        let signature = key.sign(&relay.signing_bytes().unwrap());
        assert!(relay.signed_by(&key.public(), &signature));
        let other = iroh::SecretKey::from_bytes(&[8; 32]).public();
        assert!(!relay.signed_by(&other, &signature));
        let moved = CountRelay {
            run: Ulid::from_parts(3, 3),
            ..relay.clone()
        };
        assert!(!moved.signed_by(&key.public(), &signature));
        // The bare record bytes, without the prefix, are not what a node signs.
        let bare = key.sign(&postcard::to_allocvec(&relay).unwrap());
        assert!(!relay.signed_by(&key.public(), &bare));
    }
}
