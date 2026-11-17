//! Lists the destinations an enabled outbound sync relationship will replicate a version to.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::LocationSummaryError;
use crate::replication::version_replication::map_sync_key;
use aruna_core::NodeId;
use aruna_core::calls::{IoResult, Next};
use aruna_core::keyspaces::RELATIONSHIP_OUT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::structs::{SyncMode, SyncRelationship, SyncState, sync_relationship_prefix};
use std::collections::BTreeSet;

const RELATIONSHIP_PAGE_SIZE: usize = 256;

/// One destination a copy of this version lands on: the node, plus the bucket
/// and key it is stored under there. Two relationships to one node with
/// different mappings are two destinations, not one.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReplicaTarget {
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
}

/// Destinations an enabled outbound sync relationship will replicate this
/// version to, each carrying the path the copy is stored under. Between the
/// local commit and the queue job that follows it, no other source names them.
#[derive(Debug, PartialEq)]
pub struct RelationshipNodesOperation {
    local_node: NodeId,
    bucket: String,
    key: String,
    delete_marker: bool,
    pages: PrefixPages,
    found: BTreeSet<ReplicaTarget>,
}

type Targets = BTreeSet<ReplicaTarget>;

impl RelationshipNodesOperation {
    pub fn new(local_node: NodeId, bucket: String, key: String, delete_marker: bool) -> Self {
        let prefix = sync_relationship_prefix(&bucket);
        Self {
            local_node,
            bucket,
            key,
            delete_marker,
            pages: PrefixPages::new(RELATIONSHIP_OUT_KEYSPACE, prefix, RELATIONSHIP_PAGE_SIZE),
            found: BTreeSet::new(),
        }
    }

    /// The live queue's admission rule minus loop guards needing a write origin this
    /// query lacks. The key runs through replication's mapping, so a prefix rewrite is
    /// asked about where the copy actually lands.
    fn target_of(&self, relationship: &SyncRelationship) -> Option<ReplicaTarget> {
        if !matches!(
            relationship.mode,
            SyncMode::Continuous | SyncMode::Reference
        ) || relationship.state != SyncState::Enabled
            || relationship.source.node_id != self.local_node
            || relationship.source.bucket() != Some(self.bucket.as_str())
            || (self.delete_marker && !relationship.replicate_deletes)
            || relationship.target.node_id == self.local_node
        {
            return None;
        }
        Some(ReplicaTarget {
            node_id: relationship.target.node_id,
            bucket: relationship.target.bucket()?.to_string(),
            key: map_sync_key(
                &self.key,
                relationship.source.key_prefix(),
                relationship.target.key_prefix(),
            )?,
        })
    }
}

impl Operation for RelationshipNodesOperation {
    type Output = Targets;
    type Error = LocationSummaryError;

    fn first(&mut self) -> Result<Next<Targets>, LocationSummaryError> {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Targets>, LocationSummaryError> {
        for (_, value) in self.pages.take(result)? {
            let relationship = SyncRelationship::from_bytes(value.as_ref())?;
            if let Some(target) = self.target_of(&relationship) {
                self.found.insert(target);
            }
        }
        if !self.pages.done() {
            return Ok(Next::Call(self.pages.call()));
        }
        Ok(Next::Done(std::mem::take(&mut self.found)))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::{LocationSummaryError, Targets};
    use super::{RelationshipNodesOperation, ReplicaTarget};
    use crate::tests::locations::{node_id, realm_id};
    use aruna_core::calls::{IoResult, Next};
    use aruna_core::operation::Operation;
    use aruna_core::structs::storage::replication::ArunaArn;
    use aruna_core::structs::{
        ReferenceHandling, SyncMode, SyncRelationship, SyncState, SyncStatusSnapshot,
    };
    use ulid::Ulid;

    fn link(source: &str, target: &str, deletes: bool) -> SyncRelationship {
        SyncRelationship {
            id: Ulid::from_bytes([1u8; 16]),
            source: ArunaArn::s3_bucket(realm_id(), node_id(4), source).unwrap(),
            target: ArunaArn::s3_bucket(realm_id(), node_id(6), target).unwrap(),
            mode: SyncMode::Continuous,
            reference_handling: ReferenceHandling::default(),
            reference_serving: false,
            replicate_deletes: deletes,
            created_by: Default::default(),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            state: SyncState::Enabled,
            status: SyncStatusSnapshot::default(),
        }
    }

    fn page(relationship: &SyncRelationship) -> IoResult {
        IoResult::Page {
            rows: vec![(
                b"k".to_vec().into(),
                relationship.to_bytes().unwrap().into(),
            )],
            next: None,
        }
    }

    fn finished(done: Result<Next<Targets>, LocationSummaryError>) -> Targets {
        match done {
            Ok(Next::Done(targets)) => targets,
            other => panic!("expected the targets, got {other:?}"),
        }
    }

    fn operation(delete_marker: bool) -> RelationshipNodesOperation {
        RelationshipNodesOperation::new(
            node_id(4),
            "raw".to_string(),
            "run1.tar".to_string(),
            delete_marker,
        )
    }

    #[test]
    fn names_destination_bucket() {
        // The copy lands in the relationship's target bucket, so that is the
        // bucket the destination has to be asked about.
        let mut op = operation(false);
        let _ = op.first();

        let done = op.next(page(&link("raw", "mirror", true)));

        assert_eq!(
            finished(done).into_iter().collect::<Vec<_>>(),
            vec![ReplicaTarget {
                node_id: node_id(6),
                bucket: "mirror".to_string(),
                key: "run1.tar".to_string(),
            }]
        );
    }

    #[test]
    fn maps_destination_key() {
        // A prefix rewrite stores the copy under the mapped key, so asking the
        // destination about the source key would miss it.
        let mut op = RelationshipNodesOperation::new(
            node_id(4),
            "raw".to_string(),
            "photos/a.jpg".to_string(),
            false,
        );
        let _ = op.first();
        let mut relationship = link("raw", "archive", true);
        relationship.source =
            ArunaArn::s3_object_prefix(realm_id(), node_id(4), "raw", "photos/").unwrap();
        relationship.target =
            ArunaArn::s3_object_prefix(realm_id(), node_id(6), "archive", "images/").unwrap();

        let done = op.next(page(&relationship));

        assert_eq!(
            finished(done).into_iter().collect::<Vec<_>>(),
            vec![ReplicaTarget {
                node_id: node_id(6),
                bucket: "archive".to_string(),
                key: "images/a.jpg".to_string(),
            }]
        );
    }

    #[test]
    fn keeps_both_mappings() {
        // Two relationships to one node place two copies; collapsing them to
        // one destination loses whichever mapping came second.
        let mut op = RelationshipNodesOperation::new(
            node_id(4),
            "raw".to_string(),
            "photos/a.jpg".to_string(),
            false,
        );
        let _ = op.first();
        let mut second = link("raw", "second", true);
        second.id = Ulid::from_bytes([2u8; 16]);

        let done = op.next(IoResult::Page {
            rows: vec![
                (
                    b"a".to_vec().into(),
                    link("raw", "mirror", true).to_bytes().unwrap().into(),
                ),
                (b"b".to_vec().into(), second.to_bytes().unwrap().into()),
            ],
            next: None,
        });

        let targets = finished(done);
        assert_eq!(targets.len(), 2);
        assert!(targets.iter().all(|target| target.node_id == node_id(6)));
    }

    #[test]
    fn skips_declined_markers() {
        let mut op = operation(true);
        let _ = op.first();

        let done = op.next(page(&link("raw", "mirror", false)));

        assert!(finished(done).is_empty());
    }

    #[test]
    fn skips_paused_link() {
        let mut op = operation(false);
        let _ = op.first();
        let mut relationship = link("raw", "mirror", true);
        relationship.state = SyncState::Paused;

        let done = op.next(page(&relationship));

        assert!(finished(done).is_empty());
    }

    #[test]
    fn skips_other_prefix() {
        // A relationship scoped to a prefix the key is not under places no copy.
        let mut op = operation(false);
        let _ = op.first();
        let mut relationship = link("raw", "mirror", true);
        relationship.source =
            ArunaArn::s3_object_prefix(realm_id(), node_id(4), "raw", "other/").unwrap();

        let done = op.next(page(&relationship));

        assert!(finished(done).is_empty());
    }
}
