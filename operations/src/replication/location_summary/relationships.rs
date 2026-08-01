use super::LocationSummaryError;
use crate::replication::version_replication::map_sync_key;
use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::SYNC_RELATIONSHIP_OUT_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::{SyncMode, SyncRelationship, SyncState, sync_relationship_prefix};
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use std::collections::BTreeSet;

const RELATIONSHIP_PAGE_SIZE: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
enum RelationshipState {
    Init,
    Scan,
    Finish,
    Error,
}

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
pub struct RelationshipReplicaNodesOperation {
    local_node: NodeId,
    bucket: String,
    key: String,
    delete_marker: bool,
    found: BTreeSet<ReplicaTarget>,
    state: RelationshipState,
    output: Option<Result<BTreeSet<ReplicaTarget>, LocationSummaryError>>,
}

impl RelationshipReplicaNodesOperation {
    pub fn new(local_node: NodeId, bucket: String, key: String, delete_marker: bool) -> Self {
        Self {
            local_node,
            bucket,
            key,
            delete_marker,
            found: BTreeSet::new(),
            state: RelationshipState::Init,
            output: None,
        }
    }

    fn scan(&mut self, start_after: Option<Key>) -> Effects {
        self.state = RelationshipState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: SYNC_RELATIONSHIP_OUT_KEYSPACE.to_string(),
            prefix: Some(sync_relationship_prefix(&self.bucket).into()),
            start: start_after.map(IterStart::After),
            limit: RELATIONSHIP_PAGE_SIZE,
            txn_id: None,
        })]
    }

    /// The live queue's own admission rule, minus the loop guards that need the
    /// inbound origin of a write this query does not have. The key runs through
    /// replication's own mapping, so a prefix rewrite is asked about where the
    /// copy actually lands.
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

    fn fail(&mut self, error: LocationSummaryError) -> Effects {
        self.state = RelationshipState::Error;
        self.output = Some(Err(error));
        smallvec![]
    }
}

impl Operation for RelationshipReplicaNodesOperation {
    type Output = BTreeSet<ReplicaTarget>;
    type Error = LocationSummaryError;

    fn start(&mut self) -> Effects {
        self.scan(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            RelationshipState::Init => self.start(),
            RelationshipState::Scan => {
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    return self.fail(LocationSummaryError::Unexpected {
                        state: "relationship_scan",
                        event: format!("{event:?}"),
                    });
                };
                for (_, value) in values {
                    let relationship = match SyncRelationship::from_bytes(value.as_ref()) {
                        Ok(relationship) => relationship,
                        Err(error) => return self.fail(error.into()),
                    };
                    if let Some(target) = self.target_of(&relationship) {
                        self.found.insert(target);
                    }
                }
                match next_start_after {
                    Some(start) => self.scan(Some(start)),
                    None => {
                        self.state = RelationshipState::Finish;
                        self.output = Some(Ok(std::mem::take(&mut self.found)));
                        smallvec![]
                    }
                }
            }
            RelationshipState::Finish | RelationshipState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            RelationshipState::Finish | RelationshipState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(LocationSummaryError::Unexpected {
            state: "finalize",
            event: "relationship scan ended without an answer".to_string(),
        }))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::{RelationshipReplicaNodesOperation, ReplicaTarget};
    use crate::replication::location_summary::fixtures::{node_id, realm_id};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::{
        ArunaArn, ReferenceHandling, SyncMode, SyncRelationship, SyncState, SyncStatusSnapshot,
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

    fn page(relationship: &SyncRelationship) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values: vec![(
                b"k".to_vec().into(),
                relationship.to_bytes().unwrap().into(),
            )],
            next_start_after: None,
        })
    }

    fn operation(delete_marker: bool) -> RelationshipReplicaNodesOperation {
        RelationshipReplicaNodesOperation::new(
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
        op.start();

        op.step(page(&link("raw", "mirror", true)));

        assert_eq!(
            op.finalize().unwrap().into_iter().collect::<Vec<_>>(),
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
        let mut op = RelationshipReplicaNodesOperation::new(
            node_id(4),
            "raw".to_string(),
            "photos/a.jpg".to_string(),
            false,
        );
        op.start();
        let mut relationship = link("raw", "archive", true);
        relationship.source =
            ArunaArn::s3_object_prefix(realm_id(), node_id(4), "raw", "photos/").unwrap();
        relationship.target =
            ArunaArn::s3_object_prefix(realm_id(), node_id(6), "archive", "images/").unwrap();

        op.step(page(&relationship));

        assert_eq!(
            op.finalize().unwrap().into_iter().collect::<Vec<_>>(),
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
        let mut op = RelationshipReplicaNodesOperation::new(
            node_id(4),
            "raw".to_string(),
            "photos/a.jpg".to_string(),
            false,
        );
        op.start();
        let mut second = link("raw", "second", true);
        second.id = Ulid::from_bytes([2u8; 16]);

        op.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                (
                    b"a".to_vec().into(),
                    link("raw", "mirror", true).to_bytes().unwrap().into(),
                ),
                (b"b".to_vec().into(), second.to_bytes().unwrap().into()),
            ],
            next_start_after: None,
        }));

        let targets = op.finalize().unwrap();
        assert_eq!(targets.len(), 2);
        assert!(targets.iter().all(|target| target.node_id == node_id(6)));
    }

    #[test]
    fn skips_declined_markers() {
        let mut op = operation(true);
        op.start();

        op.step(page(&link("raw", "mirror", false)));

        assert!(op.finalize().unwrap().is_empty());
    }

    #[test]
    fn skips_paused_link() {
        let mut op = operation(false);
        op.start();
        let mut relationship = link("raw", "mirror", true);
        relationship.state = SyncState::Paused;

        op.step(page(&relationship));

        assert!(op.finalize().unwrap().is_empty());
    }

    #[test]
    fn skips_other_prefix() {
        // A relationship scoped to a prefix the key is not under places no copy.
        let mut op = operation(false);
        op.start();
        let mut relationship = link("raw", "mirror", true);
        relationship.source =
            ArunaArn::s3_object_prefix(realm_id(), node_id(4), "raw", "other/").unwrap();

        op.step(page(&relationship));

        assert!(op.finalize().unwrap().is_empty());
    }
}
