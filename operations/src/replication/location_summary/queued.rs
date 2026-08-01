use super::LocationSummaryError;
use crate::replication::queue::BlobReplicationJobRecord;
use crate::replication::version_replication::{ReplicateScopeInput, ReplicateScopeTarget};
use aruna_core::NodeId;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::BLOB_REPLICATION_JOB_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::types::{Effects, Key};
use smallvec::smallvec;
use std::collections::BTreeSet;
use ulid::Ulid;

const QUEUED_JOB_PAGE_SIZE: usize = 256;
const QUEUED_JOB_MAX_PAGES: usize = 4;

#[derive(Clone, Debug, Eq, PartialEq)]
enum QueuedState {
    Init,
    Scan,
    Finish,
    Error,
}

/// Nodes with a queued replication job, plus what the scan could not see: a
/// page cap reached before the keyspace ended, and records that would not
/// decode. Either one means a queued copy may be missing from `nodes`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueuedReplicas {
    pub nodes: BTreeSet<NodeId>,
    pub truncated: bool,
    pub skipped: usize,
}

/// Collects nodes with a queued replication job for one version. They are the
/// copies a caller must see as `pending`: no location record for them exists
/// anywhere yet, so nothing else would report them.
#[derive(Debug, PartialEq)]
pub struct QueuedReplicaNodesOperation {
    bucket: String,
    key: String,
    version_id: Ulid,
    delete_marker: bool,
    pages: usize,
    found: QueuedReplicas,
    state: QueuedState,
    output: Option<Result<QueuedReplicas, LocationSummaryError>>,
}

impl QueuedReplicaNodesOperation {
    pub fn new(bucket: String, key: String, version_id: Ulid, delete_marker: bool) -> Self {
        Self {
            bucket,
            key,
            version_id,
            delete_marker,
            pages: 0,
            found: QueuedReplicas::default(),
            state: QueuedState::Init,
            output: None,
        }
    }

    fn scan(&mut self, start_after: Option<Key>) -> Effects {
        self.pages = self.pages.saturating_add(1);
        self.state = QueuedState::Scan;
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: BLOB_REPLICATION_JOB_KEYSPACE.to_string(),
            prefix: None,
            start: start_after.map(IterStart::After),
            limit: QUEUED_JOB_PAGE_SIZE,
            txn_id: None,
        })]
    }

    /// Mirrors `ReplicateScopeOperation::should_enqueue_version`: a job that
    /// declines delete markers will skip this version rather than copy it.
    fn covers(&self, input: &ReplicateScopeInput) -> bool {
        if input.bucket != self.bucket || (self.delete_marker && !input.replicate_delete_markers) {
            return false;
        }
        match &input.target {
            ReplicateScopeTarget::Bucket => true,
            ReplicateScopeTarget::Prefix(prefix) => self.key.starts_with(prefix),
            ReplicateScopeTarget::Object { key } => key == &self.key,
            ReplicateScopeTarget::Version { key, version_id } => {
                key == &self.key && *version_id == self.version_id
            }
        }
    }

    fn finish(&mut self, truncated: bool) -> Effects {
        self.state = QueuedState::Finish;
        self.found.truncated = truncated;
        self.output = Some(Ok(std::mem::take(&mut self.found)));
        smallvec![]
    }
}

impl Operation for QueuedReplicaNodesOperation {
    type Output = QueuedReplicas;
    type Error = LocationSummaryError;

    fn start(&mut self) -> Effects {
        self.scan(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.state {
            QueuedState::Init => self.start(),
            QueuedState::Scan => {
                let Event::Storage(StorageEvent::IterResult {
                    values,
                    next_start_after,
                }) = event
                else {
                    self.state = QueuedState::Error;
                    self.output = Some(Err(LocationSummaryError::Unexpected {
                        state: "queued_scan",
                        event: format!("{event:?}"),
                    }));
                    return smallvec![];
                };
                for (_, value) in values {
                    let Ok(record) = BlobReplicationJobRecord::from_bytes(value.as_ref()) else {
                        self.found.skipped = self.found.skipped.saturating_add(1);
                        continue;
                    };
                    if self.covers(&record.input) {
                        self.found.nodes.insert(record.input.target_node_id);
                    }
                }
                match next_start_after {
                    Some(start) if self.pages < QUEUED_JOB_MAX_PAGES => self.scan(Some(start)),
                    Some(_) => self.finish(true),
                    None => self.finish(false),
                }
            }
            QueuedState::Finish | QueuedState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, QueuedState::Finish | QueuedState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.unwrap_or(Err(LocationSummaryError::Unexpected {
            state: "finalize",
            event: "queued replica scan ended without an answer".to_string(),
        }))
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod tests {
    use super::QueuedReplicaNodesOperation;
    use crate::replication::location_summary::fixtures::{auth, node_id};
    use crate::replication::queue::BlobReplicationJobRecord;
    use crate::replication::version_replication::{ReplicateScopeInput, ReplicateScopeTarget};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::types::NodeId;
    use ulid::Ulid;

    fn job(target: ReplicateScopeTarget, target_node: NodeId) -> BlobReplicationJobRecord {
        marker_job(target, target_node, true)
    }

    fn marker_job(
        target: ReplicateScopeTarget,
        target_node: NodeId,
        markers: bool,
    ) -> BlobReplicationJobRecord {
        BlobReplicationJobRecord::new(
            ReplicateScopeInput {
                bucket: "raw".to_string(),
                target,
                target_node_id: target_node,
                auth_context: auth(),
                replicate_delete_markers: markers,
                mode: crate::replication::protocol::ReplicationMode::OnDemand,
            },
            None,
            0,
        )
    }

    #[test]
    fn skips_declined_markers() {
        // A scoped job that does not replicate delete markers will skip this
        // version, so reporting its target as pending would promise a copy
        // that is never coming.
        let mut operation = QueuedReplicaNodesOperation::new(
            "raw".to_string(),
            "run1.tar".to_string(),
            Ulid::from_bytes([3u8; 16]),
            true,
        );
        operation.start();

        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(
                b"a".to_vec().into(),
                marker_job(ReplicateScopeTarget::Bucket, node_id(6), false)
                    .to_bytes()
                    .unwrap()
                    .into(),
            )],
            next_start_after: None,
        }));

        assert!(operation.finalize().unwrap().nodes.is_empty());
    }

    #[test]
    fn names_queued_nodes() {
        let version_id = Ulid::from_bytes([3u8; 16]);
        let wanted = node_id(6);
        let mut operation = QueuedReplicaNodesOperation::new(
            "raw".to_string(),
            "run1.tar".to_string(),
            version_id,
            false,
        );
        operation.start();

        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![
                (
                    b"a".to_vec().into(),
                    job(ReplicateScopeTarget::Bucket, wanted)
                        .to_bytes()
                        .unwrap()
                        .into(),
                ),
                (
                    b"b".to_vec().into(),
                    job(
                        ReplicateScopeTarget::Object {
                            key: "other".to_string(),
                        },
                        wanted,
                    )
                    .to_bytes()
                    .unwrap()
                    .into(),
                ),
            ],
            next_start_after: None,
        }));

        let queued = operation.finalize().unwrap();
        assert_eq!(queued.nodes.len(), 1);
        assert!(queued.nodes.contains(&wanted));
        assert!(!queued.truncated);
    }

    #[test]
    fn counts_skipped_records() {
        // A record that will not decode may name a node; the scan is then not
        // an exhaustive answer and must say so.
        let mut operation = QueuedReplicaNodesOperation::new(
            "raw".to_string(),
            "run1.tar".to_string(),
            Ulid::from_bytes([3u8; 16]),
            false,
        );
        operation.start();

        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(b"a".to_vec().into(), vec![0xffu8; 8].into())],
            next_start_after: None,
        }));

        let queued = operation.finalize().unwrap();
        assert_eq!(queued.skipped, 1);
        assert!(queued.nodes.is_empty());
    }

    #[test]
    fn signals_truncated_scan() {
        // A capped scan must not look like an exhausted one: a queued copy past
        // the cap is otherwise indistinguishable from absent.
        let wanted = node_id(6);
        let mut operation = QueuedReplicaNodesOperation::new(
            "raw".to_string(),
            "run1.tar".to_string(),
            Ulid::from_bytes([3u8; 16]),
            false,
        );
        operation.start();

        for _ in 0..super::QUEUED_JOB_MAX_PAGES {
            operation.step(Event::Storage(StorageEvent::IterResult {
                values: vec![(
                    b"a".to_vec().into(),
                    job(ReplicateScopeTarget::Bucket, wanted)
                        .to_bytes()
                        .unwrap()
                        .into(),
                )],
                next_start_after: Some(b"a".to_vec().into()),
            }));
        }

        let queued = operation.finalize().unwrap();
        assert!(queued.truncated);
        assert!(queued.nodes.contains(&wanted));
    }
}
