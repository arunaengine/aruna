//! Selects the members a coordinator publishes one range to and admits every incoming
//! publication against the receiver's own record, so foreign or stale input is refused.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

use super::{
    DataStrategy, DistributionBinding, DistributionMember, DistributionSnapshot, KeyRange,
    MemberState, MetadataCoverage, PublicationEpoch,
};
use crate::NodeId;

/// What a coordinator's publication to one member asserts besides the version it carries.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Publication {
    pub set_id: Ulid,
    /// The range the coordinator published under; a split resolves it to the key's child.
    pub range_id: Ulid,
    /// The record revision number the coordinator published under.
    pub revision: u64,
    pub epoch: PublicationEpoch,
    /// The receiving member and the local bucket incarnation the record admitted.
    pub member_id: Ulid,
    pub incarnation: u128,
    /// Whether the member keeps the bytes; otherwise it receives the record only.
    pub bytes: bool,
}

/// One member a coordinator publishes a range to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Delivery {
    pub member_id: Ulid,
    pub node_id: NodeId,
    pub incarnation: u128,
    pub bytes: bool,
}

/// Why a member refuses a publication.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum AdmissionError {
    #[error("the publication names distributed bucket {0}, not this member's")]
    ForeignSet(Ulid),
    #[error("the publication is for member {0}, not this bucket")]
    ForeignMember(Ulid),
    #[error("the publication names another local bucket incarnation than this one")]
    Incarnation,
    #[error("this member's record is at revision {local}, behind the sender's {sent}")]
    RecordBehind { local: u64, sent: u64 },
    #[error("node {0} does not coordinate the key's range")]
    NotCoordinator(NodeId),
    #[error("the publication's range or epoch is stale; the range is {range_id} at epoch {epoch}")]
    Stale { range_id: Ulid, epoch: u64 },
    #[error("this member does not hold the key's range")]
    NotHolder,
    #[error("this member holds the key's range as records only")]
    RecordsOnly,
    #[error("this member refuses publications while it is leaving or failed")]
    Fenced,
    #[error("distributed bucket {0} is being deleted")]
    Deleting(Ulid),
}

impl AdmissionError {
    /// Refusals a sender retries after either side read a newer record.
    pub fn retryable(&self) -> bool {
        matches!(self, Self::RecordBehind { .. } | Self::Stale { .. })
    }
}

/// Whether `member` keeps records of `range`, and if so whether it keeps the bytes too.
fn holds(range: &KeyRange, member: &DistributionMember) -> Option<bool> {
    match range.replicas.contains(&member.member_id) {
        true => Some(member.strategy == DataStrategy::Copy),
        false => (member.coverage == MetadataCoverage::All).then_some(false),
    }
}

/// An admitted member that still accepts publications.
fn receiving(member: &DistributionMember) -> Option<u128> {
    match member.state {
        MemberState::CatchingUp | MemberState::Ready => member.incarnation,
        MemberState::Created | MemberState::Leaving | MemberState::Failed => None,
    }
}

impl DistributionSnapshot {
    /// The members `coordinator` publishes `range` to: listed replicas, with bytes only for
    /// `copy` ones, and records only for every other member with `all` coverage.
    pub fn range_deliveries(&self, coordinator: Ulid, range: &KeyRange) -> Vec<Delivery> {
        if range.coordinator != coordinator || self.lifecycle.deletion.is_some() {
            return Vec::new();
        }
        let members = self.config.members.iter();
        members
            .filter(|member| member.member_id != coordinator)
            .filter_map(|member| {
                let incarnation = receiving(member)?;
                Some(Delivery {
                    member_id: member.member_id,
                    node_id: member.node_id,
                    incarnation,
                    bytes: holds(range, member)?,
                })
            })
            .collect()
    }

    /// The range of `key` and its deliveries, while `coordinator` coordinates it.
    pub fn key_deliveries(
        &self,
        coordinator: Ulid,
        key: &str,
    ) -> Option<(&KeyRange, Vec<Delivery>)> {
        let range = self.range_for(key)?;
        (range.coordinator == coordinator)
            .then(|| (range, self.range_deliveries(coordinator, range)))
    }

    /// The publication of `range` to one delivery at this revision.
    pub fn publication(&self, range: &KeyRange, delivery: &Delivery) -> Publication {
        Publication {
            set_id: self.set_id(),
            range_id: range.range_id,
            revision: self.number,
            epoch: range.publication_epoch(),
            member_id: delivery.member_id,
            incarnation: delivery.incarnation,
            bytes: delivery.bytes,
        }
    }

    /// Admits a publication of `key` from `sender` to the bucket bound by `binding`: the
    /// sender coordinates the key's range at the publication epoch, this member is the
    /// admitted incarnation and holds the range, and only a `copy` replica takes bytes.
    pub fn admit(
        &self,
        binding: &DistributionBinding,
        sender: NodeId,
        key: &str,
        publication: &Publication,
    ) -> Result<(), AdmissionError> {
        let set_id = self.set_id();
        if publication.set_id != set_id || binding.set_id != set_id {
            return Err(AdmissionError::ForeignSet(publication.set_id));
        }
        if publication.member_id != binding.member_id {
            return Err(AdmissionError::ForeignMember(publication.member_id));
        }
        let member = self
            .member(binding.member_id)
            .ok_or(AdmissionError::ForeignMember(binding.member_id))?;
        if member.incarnation != Some(binding.incarnation)
            || publication.incarnation != binding.incarnation
        {
            return Err(AdmissionError::Incarnation);
        }
        if receiving(member).is_none() {
            return Err(AdmissionError::Fenced);
        }
        if self.lifecycle.deletion.is_some() {
            return Err(AdmissionError::Deleting(set_id));
        }
        if publication.revision > self.number {
            return Err(AdmissionError::RecordBehind {
                local: self.number,
                sent: publication.revision,
            });
        }
        let range = self.range_for(key).ok_or(AdmissionError::NotHolder)?;
        let coordinator = self
            .member(range.coordinator)
            .ok_or(AdmissionError::NotCoordinator(sender))?;
        if coordinator.node_id != sender {
            return Err(AdmissionError::NotCoordinator(sender));
        }
        let named =
            publication.range_id == range.range_id || range.lineage == Some(publication.range_id);
        if !named || publication.epoch != range.publication_epoch() {
            return Err(AdmissionError::Stale {
                range_id: range.range_id,
                epoch: range.epoch,
            });
        }
        match holds(range, member) {
            None => Err(AdmissionError::NotHolder),
            Some(false) if publication.bytes => Err(AdmissionError::RecordsOnly),
            Some(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::storage::distribution::presets::{add_edges, shard_layout};
    use crate::structs::storage::distribution::snapshot::{Deletion, Lifecycle};
    use crate::structs::storage::distribution::{
        DistributionConfig, DistributionRevision, DistributionSettings, SetPhase, SetRoot,
    };

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn member(seed: u8) -> Ulid {
        Ulid::from_bytes([seed; 16])
    }

    /// Two ranges split at "m": member 1 coordinates the lower with member 2 as `copy`
    /// replica, member 2 the upper with member 3; member 4 is a stream edge with `all`.
    fn snapshot() -> DistributionSnapshot {
        let root = SetRoot {
            publisher: node(1),
            owner_group: Ulid::from_bytes([90; 16]),
            nonce: [7; 16],
        };
        let nodes: Vec<_> = (1..=3).map(|seed| (member(seed), node(seed))).collect();
        let mut layout = shard_layout(root.set_id(), &nodes, &["m".to_string()], 1).unwrap();
        add_edges(&mut layout, &[(member(4), node(4))], DataStrategy::Stream).unwrap();
        for (index, member) in layout.members.iter_mut().enumerate() {
            member.incarnation = Some(index as u128 + 10);
            member.state = MemberState::Ready;
            member.intent = [1; 32];
        }
        DistributionSnapshot {
            root: root.clone(),
            number: 5,
            previous: Some([0; 32]),
            phase: SetPhase::Active,
            config: DistributionConfig {
                bucket: "spire".to_string(),
                owner_group: root.owner_group,
                node_set: None,
                members: layout.members,
                ranges: layout.ranges,
                settings: DistributionSettings::default(),
            },
            lifecycle: Lifecycle::default(),
            statements: Vec::new(),
            tombstones: Vec::new(),
            admin_number: 1,
        }
    }

    fn binding(head: &DistributionSnapshot, seed: u8) -> DistributionBinding {
        let member = head.member(member(seed)).unwrap();
        DistributionBinding {
            set_id: head.set_id(),
            member_id: member.member_id,
            incarnation: member.incarnation.unwrap(),
            revision: DistributionRevision::default(),
        }
    }

    fn sent(head: &DistributionSnapshot, key: &str, seed: u8) -> Publication {
        let (range, deliveries) = head
            .key_deliveries(head.range_for(key).unwrap().coordinator, key)
            .unwrap();
        let delivery = deliveries
            .iter()
            .find(|delivery| delivery.member_id == member(seed))
            .unwrap();
        head.publication(range, delivery)
    }

    #[test]
    fn selects_range_holders() {
        let head = snapshot();
        let (_, lower) = head.key_deliveries(member(1), "a").unwrap();
        let picked: Vec<_> = lower
            .iter()
            .map(|delivery| (delivery.member_id, delivery.bytes))
            .collect();
        assert_eq!(picked, vec![(member(2), true), (member(4), false)]);
        assert!(
            head.key_deliveries(member(1), "x").is_none(),
            "member 2 coordinates x"
        );
        let (_, upper) = head.key_deliveries(member(2), "x").unwrap();
        assert!(upper.iter().all(|delivery| delivery.member_id != member(1)));
    }

    #[test]
    fn skips_unadmitted_members() {
        let mut head = snapshot();
        head.config.members[1].state = MemberState::Leaving;
        head.config.members[3].incarnation = None;
        head.config.members[3].state = MemberState::Created;
        let (_, lower) = head.key_deliveries(member(1), "a").unwrap();
        assert!(lower.is_empty());
        head.lifecycle.deletion = Some(Deletion {
            members: Vec::new(),
            fenced: Vec::new(),
            committed: false,
        });
        let (_, upper) = head.key_deliveries(member(2), "x").unwrap();
        assert!(upper.is_empty(), "a deletion fences every publication");
    }

    #[test]
    fn admits_coordinator_publication() {
        let head = snapshot();
        let publication = sent(&head, "a", 2);
        assert_eq!(
            head.admit(&binding(&head, 2), node(1), "a", &publication),
            Ok(())
        );
        let edge = sent(&head, "a", 4);
        assert!(!edge.bytes);
        assert_eq!(head.admit(&binding(&head, 4), node(1), "a", &edge), Ok(()));
    }

    #[test]
    fn refuses_foreign_input() {
        let head = snapshot();
        let publication = sent(&head, "a", 2);
        let receiver = binding(&head, 2);
        assert_eq!(
            head.admit(&receiver, node(3), "a", &publication),
            Err(AdmissionError::NotCoordinator(node(3)))
        );
        assert_eq!(
            head.admit(&binding(&head, 3), node(1), "a", &publication),
            Err(AdmissionError::ForeignMember(member(2)))
        );
        let mut replaced = receiver.clone();
        replaced.incarnation += 1;
        assert_eq!(
            head.admit(&replaced, node(1), "a", &publication),
            Err(AdmissionError::Incarnation)
        );
        let mut bytes = sent(&head, "a", 4);
        bytes.bytes = true;
        assert_eq!(
            head.admit(&binding(&head, 4), node(1), "a", &bytes),
            Err(AdmissionError::RecordsOnly)
        );
    }

    #[test]
    fn refuses_stale_input() {
        let head = snapshot();
        let receiver = binding(&head, 2);
        let mut old = sent(&head, "a", 2);
        old.epoch = PublicationEpoch(0);
        let refused = head.admit(&receiver, node(1), "a", &old).unwrap_err();
        assert!(refused.retryable());
        let mut ahead = sent(&head, "a", 2);
        ahead.revision = 6;
        assert_eq!(
            head.admit(&receiver, node(1), "a", &ahead),
            Err(AdmissionError::RecordBehind { local: 5, sent: 6 })
        );
        let mut leaving = head.clone();
        leaving.config.members[1].state = MemberState::Leaving;
        let publication = sent(&head, "a", 2);
        assert_eq!(
            leaving.admit(&receiver, node(1), "a", &publication),
            Err(AdmissionError::Fenced)
        );
    }

    #[test]
    fn resolves_split_children() {
        let mut head = snapshot();
        let publication = sent(&head, "a", 2);
        let parent = head.config.ranges[0].range_id;
        head.config.ranges[0].range_id = Ulid::from_bytes([77; 16]);
        head.config.ranges[0].lineage = Some(parent);
        assert_eq!(
            head.admit(&binding(&head, 2), node(1), "a", &publication),
            Ok(())
        );
    }
}
