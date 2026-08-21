use crate::NodeId;
use crate::compute::{AdvertisementError, ExecutorCapability, MAX_ADVERTISED_EXECUTORS};
use crate::compute_quota::{ComputeDemandSnapshot, ComputeReservationSnapshot, SnapshotError};
use crate::errors::ConversionError;
use crate::structs::{MAX_LABEL_KEY_LEN, MAX_LABEL_VALUE_LEN};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Maximum labels one node advertises. Above the placement-view labels plus a
/// storage-class label per registered backend.
pub const MAX_NODE_INFO_LABELS: usize = 64;
/// Maximum length of an advertised url.
pub const MAX_NODE_URL_LEN: usize = 512;

/// Derived read-only label carrying a node's `RealmNode.kind`; writes are rejected.
pub const KIND_LABEL_KEY: &str = "aruna-engine.org/kind";

/// Derived read-only label prefix advertising a storage class this node's
/// operator registered. Capability only: it reaches `NodeInfo` and never the
/// realm placement map that placement selection reads.
pub const STORAGE_CLASS_LABEL_PREFIX: &str = "aruna-engine.org/storage-class/";

/// Names the first derived label a write surface tried to set. Every such label
/// is stamped by the owning node, so no operator input may claim one.
pub fn reserved_label(labels: &BTreeMap<String, String>) -> Option<&str> {
    labels
        .keys()
        .find(|key| key.as_str() == KIND_LABEL_KEY || key.starts_with(STORAGE_CLASS_LABEL_PREFIX))
        .map(String::as_str)
}

/// Storage key for a node's info document. One document per node, so the raw
/// node id is unambiguous within the dedicated `NODE_INFO_KEYSPACE`.
pub fn node_info_storage_key(node_id: NodeId) -> Vec<u8> {
    node_id.as_bytes().to_vec()
}

/// Supersession tuple of one publisher's advertisement. `membership_generation`
/// is the realm-membership state the publisher observed, so a node that rejoins
/// under a newer membership is never shadowed by its own delayed older
/// advertisement, whatever local counter that one carried.
#[derive(Clone, Copy, Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
pub struct AdvertisementEpoch {
    pub membership_generation: u64,
    pub publisher_generation: u64,
    pub observed_at_ms: u64,
}

impl AdvertisementEpoch {
    fn order(&self) -> (u64, u64) {
        (self.membership_generation, self.publisher_generation)
    }

    /// Membership generation first, so a wiped node that rejoins publishes a
    /// superseding epoch even though its own counter restarted at one.
    pub fn supersedes(&self, other: &Self) -> bool {
        self.order() > other.order()
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct NodeInfoDocument {
    pub node_id: NodeId,
    pub executors: Vec<ExecutorCapability>,
    pub labels: BTreeMap<String, String>,
    pub urls: NodeUrls,
    pub utilization: NodeUtilization,
    pub updated_at_ms: u64,
    pub epoch: AdvertisementEpoch,
    /// Administratively drained: this node plans no new execution here.
    pub compute_draining: bool,
    /// Graceful departure announced; the node is leaving the realm.
    pub leaving: bool,
    /// Logical admission demand this node observes, for the group quota view.
    pub demand: ComputeDemandSnapshot,
    /// Exact local physical reservations. It ranks targets and is never added
    /// to logical demand: the two controls count different things.
    pub reservation: ComputeReservationSnapshot,
}

impl NodeInfoDocument {
    /// Bounds every advertised attribute and rejects a backend whose subject
    /// does not belong to this publisher or does not match its sealed digest.
    pub fn validate(&self) -> Result<(), AdvertisementError> {
        if self.executors.len() > MAX_ADVERTISED_EXECUTORS {
            return Err(AdvertisementError::ExecutorCount);
        }
        let mut kinds = BTreeSet::new();
        for executor in &self.executors {
            executor.validate(self.node_id)?;
            if !kinds.insert(executor.kind.trim()) {
                return Err(AdvertisementError::DuplicateKind {
                    kind: executor.kind.clone(),
                });
            }
        }
        validate_labels(&self.labels)?;
        self.demand.validate()?;
        if self.demand.epoch.supersedes(&self.epoch)
            || self.reservation.epoch.supersedes(&self.epoch)
        {
            return Err(SnapshotError::EpochAhead.into());
        }
        self.urls.validate()
    }

    /// Whether this advertisement replaces `stored`. Equal epochs do not
    /// supersede, so a replayed advertisement never rewrites observed facts.
    pub fn supersedes(&self, stored: &Self) -> bool {
        self.node_id == stored.node_id && self.epoch.supersedes(&stored.epoch)
    }

    /// Whether this advertisement may still receive new execution offers.
    /// Removal from membership revokes future eligibility only: the document,
    /// its executors, and any receipted execution stay untouched.
    pub fn offers_compute(&self, is_member: bool) -> bool {
        is_member && !self.leaving && !self.compute_draining
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, ConversionError> {
        self.validate()?;
        Ok(postcard::to_allocvec(self)?)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ConversionError> {
        let document: Self = postcard::from_bytes(bytes)?;
        document.validate()?;
        Ok(document)
    }
}

fn validate_labels(labels: &BTreeMap<String, String>) -> Result<(), AdvertisementError> {
    if labels.len() > MAX_NODE_INFO_LABELS {
        return Err(AdvertisementError::LabelCount);
    }
    for (key, value) in labels {
        let key = key.trim();
        if key.is_empty() || key.len() > MAX_LABEL_KEY_LEN || value.len() > MAX_LABEL_VALUE_LEN {
            return Err(AdvertisementError::InvalidLabel);
        }
    }
    Ok(())
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct NodeUrls {
    pub api: Option<String>,
    pub s3: Option<String>,
}

impl NodeUrls {
    pub fn validate(&self) -> Result<(), AdvertisementError> {
        match [self.api.as_deref(), self.s3.as_deref()]
            .iter()
            .flatten()
            .all(|url| !url.is_empty() && url.len() <= MAX_NODE_URL_LEN)
        {
            true => Ok(()),
            false => Err(AdvertisementError::InvalidUrl),
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct NodeUtilization {
    pub storage_bytes_used: u64,
    pub documents_held: Option<u64>,
    pub load_permille: Option<u32>,
    pub heartbeat_at_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::PlacementSubject;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn subject(node_id: NodeId, generation: u64) -> PlacementSubject {
        PlacementSubject {
            node_id,
            generation,
            location: "eu-west".to_string(),
            labels: BTreeMap::new(),
            executor_kind: None,
            local_to_controller: true,
        }
    }

    fn document(node_id: NodeId, epoch: AdvertisementEpoch) -> NodeInfoDocument {
        NodeInfoDocument {
            node_id,
            executors: vec![
                ExecutorCapability::new("docker".to_string(), subject(node_id, 3))
                    .expect("subject is valid"),
            ],
            labels: BTreeMap::from([(KIND_LABEL_KEY.to_string(), "server".to_string())]),
            urls: NodeUrls {
                api: Some("https://api.example".to_string()),
                s3: None,
            },
            utilization: NodeUtilization {
                storage_bytes_used: 1_024,
                documents_held: Some(7),
                load_permille: Some(250),
                heartbeat_at_ms: 1_700_000_000_000,
            },
            updated_at_ms: 1_700_000_000_500,
            epoch,
            compute_draining: false,
            leaving: false,
            demand: ComputeDemandSnapshot {
                epoch,
                ..Default::default()
            },
            reservation: ComputeReservationSnapshot {
                epoch,
                ..Default::default()
            },
        }
    }

    fn epoch(membership: u64, publisher: u64) -> AdvertisementEpoch {
        AdvertisementEpoch {
            membership_generation: membership,
            publisher_generation: publisher,
            observed_at_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn document_round_trips() {
        let document = document(node(1), epoch(4, 9));
        let bytes = document.to_bytes().expect("document encodes");
        assert_eq!(
            NodeInfoDocument::from_bytes(&bytes).expect("document decodes"),
            document
        );
    }

    #[test]
    fn rejects_foreign_subject() {
        // A publisher may only advertise execution sites of its own node.
        let mut document = document(node(1), epoch(1, 1));
        document.executors[0].subject.node_id = node(2);
        assert!(document.validate().is_err());
        assert!(NodeInfoDocument::from_bytes(&postcard::to_allocvec(&document).unwrap()).is_err());
    }

    #[test]
    fn rejects_subject_drift() {
        let mut document = document(node(1), epoch(1, 1));
        document.executors[0].subject.generation += 1;
        assert_eq!(
            document.validate(),
            Err(crate::compute::AdvertisementError::SubjectDrift)
        );
    }

    #[test]
    fn rejects_duplicate_kind() {
        let mut document = document(node(1), epoch(1, 1));
        let duplicate = document.executors[0].clone();
        document.executors.push(duplicate);
        assert!(document.validate().is_err());
    }

    #[test]
    fn rejects_snapshot_ahead() {
        // A snapshot may not claim a newer epoch than the advertisement that
        // carries it, otherwise it would outlive its own supersession order.
        let mut ahead = document(node(1), epoch(4, 9));
        ahead.demand.epoch = epoch(5, 1);
        assert_eq!(
            ahead.validate(),
            Err(AdvertisementError::Snapshot(SnapshotError::EpochAhead))
        );

        let mut reserved = document(node(1), epoch(4, 9));
        reserved.reservation.epoch = epoch(4, 10);
        assert!(reserved.validate().is_err());
    }

    #[test]
    fn rejects_unbounded_demand() {
        use crate::compute_quota::{DemandFamily, DemandGroup};
        use crate::structs::{EffectiveResources, SubmissionId};

        let mut document = document(node(1), epoch(1, 1));
        document.demand.groups = vec![DemandGroup {
            group_id: ulid::Ulid::from_bytes([1; 16]),
            families: (0..2)
                .map(|seed| DemandFamily {
                    submission_id: SubmissionId([1; 32]),
                    request_digest: [seed; 32],
                    resources: EffectiveResources {
                        cpu_cores: 1,
                        ram_bytes: 0,
                        disk_bytes: 0,
                        max_walltime_ms: 1,
                        preemptible: false,
                    },
                })
                .rev()
                .collect(),
            truncated: false,
        }];
        assert!(document.validate().is_err());
        assert!(NodeInfoDocument::from_bytes(&postcard::to_allocvec(&document).unwrap()).is_err());
    }

    #[test]
    fn removal_revokes_offers() {
        // Removal from membership stops new offers without erasing anything the
        // node advertised or claiming its executions ended.
        let document = document(node(1), epoch(1, 1));
        assert!(document.offers_compute(true));
        assert!(!document.offers_compute(false));
        assert_eq!(document.executors.len(), 1);

        let mut leaving = document.clone();
        leaving.leaving = true;
        assert!(!leaving.offers_compute(true));

        let mut drained = document.clone();
        drained.compute_draining = true;
        assert!(!drained.offers_compute(true));
    }

    #[test]
    fn supersedes_by_epoch() {
        // A rejoin advertises a newer membership generation, so the delayed
        // pre-rejoin advertisement with a higher local counter cannot win.
        let node_id = node(1);
        let current = document(node_id, epoch(7, 1));
        let stale = document(node_id, epoch(6, 900));

        assert!(current.supersedes(&stale));
        assert!(!stale.supersedes(&current));
        assert!(!current.supersedes(&current));
        assert!(document(node_id, epoch(7, 2)).supersedes(&current));
        assert!(!current.supersedes(&document(node(2), epoch(1, 1))));
    }
}
