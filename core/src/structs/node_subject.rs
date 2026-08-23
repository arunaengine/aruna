//! The placement subject this node advertises for the governed data it holds.
//!
//! The subject is the destination every write and every internal serve is
//! evaluated against. Its generation is a runtime rule the pure contract cannot
//! enforce, so it lives here: the record advances the generation exactly when
//! the digest changes, and blocks serving until the local inventory has been
//! revalidated under the new generation.

use crate::errors::ConversionError;
use crate::structs::{NodePlacementEntry, PlacementPolicyError, PlacementSubject};
use serde::{Deserialize, Serialize};

/// Single-row key of the local subject record.
pub const NODE_SUBJECT_KEY: &[u8] = b"subject";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeSubjectRecord {
    pub subject: PlacementSubject,
    /// Quarantined copies from an older generation are still unresolved.
    pub policy_draining: bool,
    /// No governed copy serves until the inventory scan revalidated it. Set on
    /// every observed subject change and on rejoin.
    pub serving_blocked: bool,
}

impl NodeSubjectRecord {
    /// First subject of a fresh node. Generation starts at one so a stored
    /// generation of zero can never pass for a real advertisement.
    pub fn seed(subject: PlacementSubject) -> Result<Self, PlacementPolicyError> {
        subject.validate()?;
        Ok(Self {
            subject: PlacementSubject {
                generation: 1,
                ..subject
            },
            policy_draining: false,
            serving_blocked: false,
        })
    }

    /// Advances only when a digest-covered field changed, so a no-op
    /// reconfiguration keeps existing registrations and receipts valid.
    /// `Ok(None)` means the observed subject is unchanged.
    pub fn advance(&self, observed: PlacementSubject) -> Result<Option<Self>, ConversionError> {
        let candidate = PlacementSubject {
            generation: self.subject.generation,
            ..observed
        };
        if candidate.digest()? == self.subject.digest()? {
            return Ok(None);
        }
        let generation = self
            .subject
            .generation
            .checked_add(1)
            .ok_or(ConversionError::HeadGenerationExhausted)?;
        Ok(Some(Self {
            subject: PlacementSubject {
                generation,
                ..candidate
            },
            policy_draining: true,
            serving_blocked: true,
        }))
    }

    /// Ends the transition once no non-compliant copy remains.
    pub fn cleared(&self) -> Self {
        Self {
            subject: self.subject.clone(),
            policy_draining: false,
            serving_blocked: false,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        self.subject.validate()?;
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let record: Self = postcard::from_bytes(bytes)?;
        record.subject.validate()?;
        Ok(record)
    }
}

/// Storage subject of one node, taken from the placement entry the realm
/// already agrees on. A node absent from the placement map has no subject and
/// therefore holds no governed data.
pub fn storage_subject(entry: &NodePlacementEntry, generation: u64) -> PlacementSubject {
    let mut labels = entry.labels.clone();
    crate::structs::stamp_location(&mut labels, &entry.location);
    PlacementSubject {
        node_id: entry.node_id,
        generation,
        location: entry.effective_location().to_string(),
        labels,
        executor_kind: None,
        local_to_controller: true,
    }
}

#[cfg(test)]
mod tests {
    use super::{NodeSubjectRecord, storage_subject};
    use crate::structs::{DEFAULT_NODE_WEIGHT, NodePlacementEntry};
    use std::collections::BTreeMap;

    fn entry(location: &str, label: Option<(&str, &str)>) -> NodePlacementEntry {
        NodePlacementEntry {
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            location: location.to_string(),
            weight: DEFAULT_NODE_WEIGHT,
            full: false,
            draining: false,
            labels: label
                .map(|(key, value)| BTreeMap::from([(key.to_string(), value.to_string())]))
                .unwrap_or_default(),
        }
    }

    #[test]
    fn advance_needs_change() {
        // A reconfiguration that changes nothing digest-covered must not
        // invalidate every registration and receipt sealed under the old one.
        let record = NodeSubjectRecord::seed(storage_subject(&entry("eu-west", None), 1))
            .expect("subject is valid");
        assert_eq!(
            record.advance(storage_subject(&entry("eu-west", None), 99)),
            Ok(None)
        );
        assert!(!record.serving_blocked);
    }

    #[test]
    fn advance_blocks_serving() {
        let record = NodeSubjectRecord::seed(storage_subject(&entry("eu-west", None), 1))
            .expect("subject is valid");
        let moved = record
            .advance(storage_subject(&entry("us-east", None), 1))
            .expect("digest computes")
            .expect("a moved node advances");

        assert_eq!(moved.subject.generation, record.subject.generation + 1);
        assert!(moved.serving_blocked && moved.policy_draining);
        assert!(!moved.cleared().serving_blocked);
    }

    #[test]
    fn labels_advance_subject() {
        // An external worker-label change is a subject change like any other.
        let record = NodeSubjectRecord::seed(storage_subject(&entry("eu-west", None), 1))
            .expect("subject is valid");
        assert!(
            record
                .advance(storage_subject(
                    &entry("eu-west", Some(("tier", "cold"))),
                    1
                ))
                .expect("digest computes")
                .is_some()
        );
    }

    #[test]
    fn record_roundtrip() {
        let record =
            NodeSubjectRecord::seed(storage_subject(&entry("eu-west", Some(("a", "b"))), 1))
                .expect("subject is valid");
        let bytes = record.to_bytes().expect("record encodes");
        assert_eq!(
            NodeSubjectRecord::from_bytes(&bytes).expect("record decodes"),
            record
        );
    }
}
