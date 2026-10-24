//! Decides where a write to one key of a distributed bucket goes: the member that coordinates
//! the key's range writes it at the range's epoch, every other member forwards it there.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use thiserror::Error;
use ulid::Ulid;

use super::{
    DistributionMember, DistributionSnapshot, KeyRange, MemberState, MetadataCoverage,
    OriginalEpoch,
};
use crate::NodeId;

/// Where one write goes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteRoute {
    /// This member coordinates the key's range; its versions carry this epoch.
    Local {
        range_id: Ulid,
        epoch: OriginalEpoch,
    },
    /// Another member coordinates the range; the write goes to its node.
    Forward {
        range_id: Ulid,
        coordinator: Ulid,
        node_id: NodeId,
    },
}

/// Why a distributed bucket accepts no write for a key now.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum RouteError {
    #[error("distributed bucket {0} is not active yet; its members are still being admitted")]
    Pending(Ulid),
    #[error("distributed bucket {0} is being deleted")]
    Deleting(Ulid),
    #[error("range {0} is moving to another coordinator; retry shortly")]
    Moving(Ulid),
    #[error("this bucket is not a current member of distributed bucket {0}")]
    NotMember(Ulid),
    #[error("no range of distributed bucket {0} holds the key")]
    NoRange(Ulid),
    #[error(
        "range {0} no longer leads to the key's range or epoch; retry after the record updates"
    )]
    StaleRange(Ulid),
}

/// Where a read is answered.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadRoute {
    /// This member keeps the records; while it catches up, a miss proves nothing.
    Local { catching_up: bool },
    /// This member keeps no records of the range; its coordinator does.
    Holder { range_id: Ulid, node_id: NodeId },
}

/// Whether `member` keeps the records of `range`.
fn keeps_records(range: &KeyRange, member: &DistributionMember) -> bool {
    member.coverage == MetadataCoverage::All
        || range.coordinator == member.member_id
        || range.replicas.contains(&member.member_id)
}

/// Whether some key starting with `prefix` lies in `range`.
fn reaches(range: &KeyRange, prefix: &str) -> bool {
    let below_upper = range.upper.as_deref().is_none_or(|upper| prefix < upper);
    below_upper && (range.lower.as_str() <= prefix || range.lower.starts_with(prefix))
}

impl DistributionSnapshot {
    /// Routes a read of `key`, or with `listing` of every key starting with it, arriving at
    /// `member_id`: local when this member keeps the records of every range the read reaches.
    pub fn read_route(
        &self,
        member_id: Ulid,
        key: &str,
        listing: bool,
    ) -> Result<ReadRoute, RouteError> {
        let set_id = self.set_id();
        let member = self
            .member(member_id)
            .ok_or(RouteError::NotMember(set_id))?;
        let reached: Vec<&KeyRange> = match listing {
            true => self
                .config
                .ranges
                .iter()
                .filter(|range| reaches(range, key))
                .collect(),
            false => self.range_for(key).into_iter().collect(),
        };
        if reached.is_empty() {
            return Err(RouteError::NoRange(set_id));
        }
        match reached
            .into_iter()
            .find(|range| !keeps_records(range, member))
        {
            None => Ok(ReadRoute::Local {
                catching_up: member.state == MemberState::CatchingUp,
            }),
            Some(range) => {
                let coordinator = self
                    .member(range.coordinator)
                    .ok_or(RouteError::NoRange(set_id))?;
                Ok(ReadRoute::Holder {
                    range_id: range.range_id,
                    node_id: coordinator.node_id,
                })
            }
        }
    }

    /// Routes a write to `key` arriving at `member_id`. Only the coordinator writes, and only
    /// while the set serves and no fence covers the range.
    pub fn write_route(&self, member_id: Ulid, key: &str) -> Result<WriteRoute, RouteError> {
        let set_id = self.set_id();
        if self.member(member_id).is_none() {
            return Err(RouteError::NotMember(set_id));
        }
        if !self.serving() {
            return Err(RouteError::Pending(set_id));
        }
        if self.lifecycle.deletion.is_some() {
            return Err(RouteError::Deleting(set_id));
        }
        let range = self.range_for(key).ok_or(RouteError::NoRange(set_id))?;
        let moving = self
            .lifecycle
            .transfers
            .iter()
            .any(|transfer| transfer.range_id == range.range_id && transfer.fenced.is_some());
        if moving {
            return Err(RouteError::Moving(range.range_id));
        }
        if range.coordinator == member_id {
            return Ok(WriteRoute::Local {
                range_id: range.range_id,
                epoch: range.original_epoch(),
            });
        }
        let coordinator = self
            .member(range.coordinator)
            .ok_or(RouteError::NoRange(set_id))?;
        Ok(WriteRoute::Forward {
            range_id: range.range_id,
            coordinator: range.coordinator,
            node_id: coordinator.node_id,
        })
    }

    /// The ready members that keep the records of `range`, its coordinator last, so reads of
    /// the range go to replicas and `all` coverage members first.
    pub fn record_holders(&self, range: &KeyRange) -> Vec<&DistributionMember> {
        let ready = |member: &&DistributionMember| member.state == MemberState::Ready;
        let mut holders: Vec<&DistributionMember> = (self.config.members.iter())
            .filter(|member| member.member_id != range.coordinator)
            .filter(|member| keeps_records(range, member))
            .filter(ready)
            .collect();
        holders.extend(self.member(range.coordinator).filter(ready));
        holders
    }

    /// The ranges whose records `member_id` holds: those it coordinates or replicates, or
    /// every range with `all` coverage.
    pub fn held_ranges(&self, member_id: Ulid) -> Vec<&KeyRange> {
        let all = self
            .member(member_id)
            .is_some_and(|member| member.coverage == MetadataCoverage::All);
        self.config
            .ranges
            .iter()
            .filter(|range| {
                all || range.coordinator == member_id || range.replicas.contains(&member_id)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{RouteError, WriteRoute};
    use crate::structs::identity::realm::RealmId;
    use crate::structs::storage::distribution::presets::shard_layout;
    use crate::structs::storage::distribution::snapshot::{Deletion, OpenTransfer};
    use crate::structs::storage::distribution::{
        DistributionConfig, DistributionRequest, DistributionSettings, DistributionSnapshot,
        OriginalEpoch, SetRoot, Statement, TransferRole, next_snapshot,
    };
    use ulid::Ulid;

    fn secret(seed: u8) -> iroh::SecretKey {
        iroh::SecretKey::from_bytes(&[seed; 32])
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([9; 32])
    }

    fn member(seed: u8) -> Ulid {
        Ulid::from_bytes([seed; 16])
    }

    /// Two shards split at "m", member 1 coordinates the lower one, both admitted and active.
    fn active() -> DistributionSnapshot {
        let root = SetRoot {
            publisher: secret(1).public(),
            owner_group: Ulid::from_bytes([90; 16]),
            nonce: [7; 16],
        };
        let nodes = [
            (member(1), secret(1).public()),
            (member(2), secret(2).public()),
        ];
        let layout = shard_layout(root.set_id(), &nodes, &["m".to_string()], 0).unwrap();
        let create = DistributionRequest::Create {
            root: root.clone(),
            config: DistributionConfig {
                bucket: "spire".to_string(),
                owner_group: root.owner_group,
                node_set: None,
                members: layout.members,
                ranges: layout.ranges,
                settings: DistributionSettings::default(),
            },
        };
        let mut head = next_snapshot(realm(), None, &create).unwrap();
        for seed in [1, 2] {
            let intent = head.member(member(seed)).unwrap().intent;
            let statement = Statement::admit(realm(), head.set_id(), member(seed), 5, intent);
            let request = DistributionRequest::Statement(Box::new(statement.sign(&secret(seed))));
            head = next_snapshot(realm(), Some(&head), &request).unwrap();
        }
        next_snapshot(realm(), Some(&head), &DistributionRequest::Activate).unwrap()
    }

    #[test]
    fn coordinator_writes_locally() {
        let head = active();
        let lower = head.config.ranges[0].range_id;
        assert_eq!(
            head.write_route(member(1), "a/b"),
            Ok(WriteRoute::Local {
                range_id: lower,
                epoch: OriginalEpoch(1),
            })
        );
        let Ok(WriteRoute::Forward { node_id, .. }) = head.write_route(member(1), "x") else {
            panic!("the upper range belongs to member 2");
        };
        assert_eq!(node_id, secret(2).public());
        assert_eq!(
            head.write_route(member(9), "a"),
            Err(RouteError::NotMember(head.set_id()))
        );
    }

    #[test]
    fn fences_refuse_writes() {
        let mut head = active();
        let lower = head.config.ranges[0].range_id;
        let mut pending = head.clone();
        pending.phase = crate::structs::storage::distribution::SetPhase::Pending;
        assert_eq!(
            pending.write_route(member(1), "a"),
            Err(RouteError::Pending(head.set_id()))
        );
        head.lifecycle.transfers.push(OpenTransfer {
            transfer_id: Ulid::from_bytes([3; 16]),
            range_id: lower,
            from: member(1),
            to: member(2),
            epoch: 2,
            role: TransferRole::Replica,
            fenced: Some([1; 32]),
            completed: None,
        });
        assert_eq!(
            head.write_route(member(1), "a"),
            Err(RouteError::Moving(lower))
        );
        assert!(head.write_route(member(2), "x").is_ok());
        head.lifecycle.deletion = Some(Deletion {
            members: Vec::new(),
            fenced: Vec::new(),
            committed: false,
        });
        assert_eq!(
            head.write_route(member(2), "x"),
            Err(RouteError::Deleting(head.set_id()))
        );
    }

    #[test]
    fn reads_held_ranges() {
        use super::ReadRoute;
        let head = active();
        let upper = &head.config.ranges[1];
        assert_eq!(
            head.read_route(member(1), "a/b", false),
            Ok(ReadRoute::Local { catching_up: false })
        );
        assert_eq!(
            head.read_route(member(1), "x", false),
            Ok(ReadRoute::Holder {
                range_id: upper.range_id,
                node_id: secret(2).public(),
            })
        );
        assert_eq!(
            head.read_route(member(1), "a", true),
            Ok(ReadRoute::Local { catching_up: false }),
            "every key under a/ is below m"
        );
        assert!(matches!(
            head.read_route(member(1), "", true),
            Ok(ReadRoute::Holder { .. })
        ));
        assert!(matches!(
            head.read_route(member(2), "m", true),
            Ok(ReadRoute::Local { .. })
        ));
        let lower = head.read_route(member(2), "l", true);
        assert!(
            matches!(lower, Ok(ReadRoute::Holder { .. })),
            "keys under l stay below m"
        );
    }

    #[test]
    fn assigned_holds_own() {
        let head = active();
        let held: Vec<Ulid> = head
            .held_ranges(member(2))
            .iter()
            .map(|range| range.range_id)
            .collect();
        assert_eq!(held, vec![head.config.ranges[1].range_id]);
    }
}
