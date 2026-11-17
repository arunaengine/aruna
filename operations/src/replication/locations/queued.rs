//! Scans the replication job keyspace for nodes that still have a queued copy of a version.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::LocationSummaryError;
use aruna_core::NodeId;
use aruna_core::calls::{IoResult, Next};
use aruna_core::keyspaces::REPLICATION_JOB_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::rows::PrefixPages;
use aruna_core::structs::storage::replication_queue::BlobJobRecord;
use aruna_core::structs::storage::replication_queue::{ReplicateScopeInput, ReplicateScopeTarget};
use std::collections::BTreeSet;
use ulid::Ulid;

const JOB_PAGE_SIZE: usize = 256;
const JOB_MAX_PAGES: usize = 4;

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
pub struct QueuedNodesOperation {
    bucket: String,
    key: String,
    version_id: Ulid,
    delete_marker: bool,
    pages: usize,
    scan: PrefixPages,
    found: QueuedReplicas,
}

impl QueuedNodesOperation {
    pub fn new(bucket: String, key: String, version_id: Ulid, delete_marker: bool) -> Self {
        Self {
            bucket,
            key,
            version_id,
            delete_marker,
            pages: 0,
            scan: PrefixPages::new(REPLICATION_JOB_KEYSPACE, Vec::new(), JOB_PAGE_SIZE),
            found: QueuedReplicas::default(),
        }
    }

    fn read_page(&mut self) -> Next<QueuedReplicas> {
        self.pages = self.pages.saturating_add(1);
        Next::Call(self.scan.call())
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
}

impl Operation for QueuedNodesOperation {
    type Output = QueuedReplicas;
    type Error = LocationSummaryError;

    fn first(&mut self) -> Result<Next<QueuedReplicas>, LocationSummaryError> {
        Ok(self.read_page())
    }

    fn next(&mut self, result: IoResult) -> Result<Next<QueuedReplicas>, LocationSummaryError> {
        for (_, value) in self.scan.take(result)? {
            let Ok(record) = BlobJobRecord::from_bytes(value.as_ref()) else {
                self.found.skipped = self.found.skipped.saturating_add(1);
                continue;
            };
            if self.covers(&record.input) {
                self.found.nodes.insert(record.input.target_node_id);
            }
        }
        if !self.scan.done() && self.pages < JOB_MAX_PAGES {
            return Ok(self.read_page());
        }
        self.found.truncated = !self.scan.done();
        Ok(Next::Done(std::mem::take(&mut self.found)))
    }
}

#[cfg(test)]
mod pure_tests {
    use super::QueuedNodesOperation;
    use super::QueuedReplicas;
    use crate::tests::locations::{auth, node_id};
    use aruna_core::calls::{IoResult, Next};
    use aruna_core::id::NodeId;
    use aruna_core::operation::Operation;
    use aruna_core::structs::storage::replication_queue::BlobJobRecord;
    use aruna_core::structs::storage::replication_queue::{
        ReplicateScopeInput, ReplicateScopeTarget,
    };
    use ulid::Ulid;

    fn done(next: Result<Next<QueuedReplicas>, super::LocationSummaryError>) -> QueuedReplicas {
        match next {
            Ok(Next::Done(queued)) => queued,
            other => panic!("expected the queued nodes, got {other:?}"),
        }
    }

    fn job(target: ReplicateScopeTarget, target_node: NodeId) -> BlobJobRecord {
        marker_job(target, target_node, true)
    }

    fn marker_job(
        target: ReplicateScopeTarget,
        target_node: NodeId,
        markers: bool,
    ) -> BlobJobRecord {
        BlobJobRecord::new(
            ReplicateScopeInput {
                bucket: "raw".to_string(),
                target,
                target_node_id: target_node,
                auth_context: auth(),
                replicate_delete_markers: markers,
                mode: aruna_core::structs::storage::replication_queue::ReplicationMode::OnDemand,
            },
            None,
            0,
        )
    }

    #[test]
    fn skips_declined_markers() {
        // A scoped job that skips delete markers will skip this version, so
        // reporting its target pending would promise a copy never coming.
        let mut operation = QueuedNodesOperation::new(
            "raw".to_string(),
            "run1.tar".to_string(),
            Ulid::from_bytes([3u8; 16]),
            true,
        );
        let _ = operation.first();

        let next = operation.next(IoResult::Page {
            rows: vec![(
                b"a".to_vec().into(),
                marker_job(ReplicateScopeTarget::Bucket, node_id(6), false)
                    .to_bytes()
                    .unwrap()
                    .into(),
            )],
            next: None,
        });

        assert!(done(next).nodes.is_empty());
    }

    #[test]
    fn names_queued_nodes() {
        let version_id = Ulid::from_bytes([3u8; 16]);
        let wanted = node_id(6);
        let mut operation =
            QueuedNodesOperation::new("raw".to_string(), "run1.tar".to_string(), version_id, false);
        let _ = operation.first();

        let next = operation.next(IoResult::Page {
            rows: vec![
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
            next: None,
        });

        let queued = done(next);
        assert_eq!(queued.nodes.len(), 1);
        assert!(queued.nodes.contains(&wanted));
        assert!(!queued.truncated);
    }

    #[test]
    fn counts_skipped_records() {
        // A record that will not decode may name a node; the scan is then not
        // an exhaustive answer and must say so.
        let mut operation = QueuedNodesOperation::new(
            "raw".to_string(),
            "run1.tar".to_string(),
            Ulid::from_bytes([3u8; 16]),
            false,
        );
        let _ = operation.first();

        let next = operation.next(IoResult::Page {
            rows: vec![(b"a".to_vec().into(), vec![0xffu8; 8].into())],
            next: None,
        });

        let queued = done(next);
        assert_eq!(queued.skipped, 1);
        assert!(queued.nodes.is_empty());
    }

    #[test]
    fn signals_truncated_scan() {
        // A capped scan must not look like an exhausted one: a queued copy past
        // the cap is otherwise indistinguishable from absent.
        let wanted = node_id(6);
        let mut operation = QueuedNodesOperation::new(
            "raw".to_string(),
            "run1.tar".to_string(),
            Ulid::from_bytes([3u8; 16]),
            false,
        );
        let _ = operation.first();

        let mut next = Ok(Next::Done(QueuedReplicas::default()));
        for _ in 0..super::JOB_MAX_PAGES {
            next = operation.next(IoResult::Page {
                rows: vec![(
                    b"a".to_vec().into(),
                    job(ReplicateScopeTarget::Bucket, wanted)
                        .to_bytes()
                        .unwrap()
                        .into(),
                )],
                next: Some(b"a".to_vec().into()),
            });
        }

        let queued = done(next);
        assert!(queued.truncated);
        assert!(queued.nodes.contains(&wanted));
    }
}
