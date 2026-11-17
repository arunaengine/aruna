//! Refreshes the realm usage summary for the usage targets a document sync touched.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::usage::{SUMMARY_GLOBAL_KEY, SUMMARY_GROUP_PREFIX};
use aruna_core::types::{GroupId, Key};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::node::realm_usage::{RealmUsage, RealmUsageError, RealmUsageOperation};
use crate::s3::usage::{dirty_rows, schedule_snapshot_publish};

/// Rows one scan page reads.
const PAGE_LIMIT: usize = 1_000;

#[derive(Debug, Error, PartialEq)]
pub enum RefreshUsageError {
    #[error(transparent)]
    Io(#[from] IoError),
}

#[derive(Debug, PartialEq)]
enum Stage {
    Idle,
    ClearScan(Vec<(String, Key)>),
    ClearDelete,
    BucketScan(BTreeSet<GroupId>),
    MarkWrite,
    Wake,
    Summary,
}

type Turn = Result<Next<()>, RefreshUsageError>;

/// A storage step whose failure the refresh logs and skips, so it sees the error event itself.
fn best_effort(effect: StorageEffect) -> Turn {
    Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(effect)))))
}

/// The realm summary sum's outcome as the event its parent receives.
fn summed(result: Result<Vec<RealmUsage>, RealmUsageError>) -> Event {
    let result = result.map(drop).map_err(|error| error.to_string());
    Event::SubOperation(SubOperationEvent::DocumentSyncResult { result })
}

/// Refreshes the realm summary for the `NodeUsage` scopes a reconcile touched. A config change
/// first clears the summary and marks local distributed bucket groups for a publish, since a
/// new coordinator moves replica shares; a failed step is logged and skipped.
#[derive(Debug, PartialEq)]
pub(crate) struct RefreshUsageOperation {
    local_node_id: NodeId,
    realm_id: Option<RealmId>,
    include_global: bool,
    groups: Vec<GroupId>,
    config_changed: bool,
    /// The generation of the dirty markers this refresh writes.
    generation: Ulid,
    stage: Stage,
}

impl RefreshUsageOperation {
    pub fn new(
        local_node_id: NodeId,
        realm_id: Option<RealmId>,
        targets: &[DocumentTarget],
        generation: Ulid,
    ) -> Self {
        let mut include_global = false;
        let mut groups = BTreeSet::new();
        let mut config_changed = false;
        for target in targets {
            match target {
                DocumentTarget::NodeUsage {
                    group_id: Some(group_id),
                    ..
                } => {
                    groups.insert(*group_id);
                }
                DocumentTarget::NodeUsage { group_id: None, .. } => include_global = true,
                DocumentTarget::RealmConfig { .. } => config_changed = true,
                _ => {}
            }
        }
        Self {
            local_node_id,
            realm_id,
            include_global,
            groups: groups.into_iter().collect(),
            config_changed,
            generation,
            stage: Stage::Idle,
        }
    }

    fn summary_page(start: Option<Key>) -> Turn {
        best_effort(StorageEffect::Iter {
            key_space: NODE_STATS_KEYSPACE.to_string(),
            prefix: Some(Key::from(SUMMARY_GROUP_PREFIX.to_vec())),
            start: start.map(IterStart::After),
            limit: PAGE_LIMIT,
            txn_id: None,
        })
    }

    fn bucket_page(start: Option<Key>) -> Turn {
        best_effort(StorageEffect::Iter {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(IterStart::After),
            limit: PAGE_LIMIT,
            txn_id: None,
        })
    }

    fn begin_buckets(&mut self) -> Turn {
        self.stage = Stage::BucketScan(BTreeSet::new());
        Self::bucket_page(None)
    }

    fn begin_summary(&mut self) -> Turn {
        if !self.include_global && self.groups.is_empty() {
            return Ok(Next::Done(()));
        }
        let groups = std::mem::take(&mut self.groups);
        let (local, realm, global) = (self.local_node_id, self.realm_id, self.include_global);
        let summary = RealmUsageOperation::summary(local, realm, global, groups);
        self.stage = Stage::Summary;
        Ok(Next::Call(IoCall::Child(boxed_suboperation(
            summary, summed,
        ))))
    }

    fn clear_failed(&mut self, error: &dyn std::fmt::Display) -> Turn {
        warn!(%error, "Failed to clear realm usage summary after realm config change");
        self.begin_buckets()
    }

    fn mark_failed(&mut self, error: &dyn std::fmt::Display) -> Turn {
        warn!(%error, "Failed to mark distributed bucket groups for a usage publish");
        self.begin_summary()
    }

    fn scan_summary(&mut self, mut deletes: Vec<(String, Key)>, event: StorageEvent) -> Turn {
        match event {
            StorageEvent::IterResult {
                values,
                next_start_after,
            } => {
                let keys = values.into_iter().map(|(key, _)| key);
                deletes.extend(keys.map(|key| (NODE_STATS_KEYSPACE.to_string(), key)));
                if let Some(start) = next_start_after {
                    self.stage = Stage::ClearScan(deletes);
                    return Self::summary_page(Some(start));
                }
                self.stage = Stage::ClearDelete;
                best_effort(StorageEffect::BatchDelete {
                    deletes,
                    txn_id: None,
                })
            }
            StorageEvent::Error { error } => self.clear_failed(&error),
            other => Err(unexpected(Event::Storage(other))),
        }
    }

    fn scan_buckets(&mut self, mut groups: BTreeSet<GroupId>, event: StorageEvent) -> Turn {
        let (values, next_start_after) = match event {
            StorageEvent::IterResult {
                values,
                next_start_after,
            } => (values, next_start_after),
            StorageEvent::Error { error } => return self.mark_failed(&error),
            other => return Err(unexpected(Event::Storage(other))),
        };
        for (_, value) in values {
            match BucketInfo::from_bytes(value.as_ref()) {
                Ok(info) if info.distribution.is_some() => {
                    groups.insert(info.group_id);
                }
                Ok(_) => {}
                Err(error) => return self.mark_failed(&error),
            }
        }
        if let Some(start) = next_start_after {
            self.stage = Stage::BucketScan(groups);
            return Self::bucket_page(Some(start));
        }
        if groups.is_empty() {
            return self.begin_summary();
        }
        self.stage = Stage::MarkWrite;
        best_effort(StorageEffect::BatchWrite {
            writes: dirty_rows(groups, self.generation),
            txn_id: None,
        })
    }
}

fn unexpected(event: Event) -> RefreshUsageError {
    let expected = "the answer to the usage refresh step";
    IoError::Unexpected {
        expected,
        got: event.label(),
    }
    .into()
}

impl Operation for RefreshUsageOperation {
    type Output = ();
    type Error = RefreshUsageError;

    fn first(&mut self) -> Turn {
        if !self.config_changed {
            return self.begin_summary();
        }
        let global = Key::from(SUMMARY_GLOBAL_KEY.to_vec());
        self.stage = Stage::ClearScan(vec![(NODE_STATS_KEYSPACE.to_string(), global)]);
        Self::summary_page(None)
    }

    fn next(&mut self, result: IoResult) -> Turn {
        let IoResult::Event(event) = result else {
            return Err(IoError::Unexpected {
                expected: "the answer to the usage refresh step",
                got: "another result",
            }
            .into());
        };
        match (std::mem::replace(&mut self.stage, Stage::Idle), *event) {
            (Stage::ClearScan(deletes), Event::Storage(event)) => self.scan_summary(deletes, event),
            (Stage::ClearDelete, Event::Storage(StorageEvent::BatchDeleteResult { .. })) => {
                self.begin_buckets()
            }
            (Stage::ClearDelete, Event::Storage(StorageEvent::Error { error })) => {
                self.clear_failed(&error)
            }
            (Stage::BucketScan(groups), Event::Storage(event)) => self.scan_buckets(groups, event),
            (Stage::MarkWrite, Event::Storage(StorageEvent::BatchWriteResult { .. })) => {
                self.stage = Stage::Wake;
                Ok(Next::Call(IoCall::Effect(Box::new(schedule_snapshot_publish()))))
            }
            (Stage::MarkWrite, Event::Storage(StorageEvent::Error { error })) => {
                self.mark_failed(&error)
            }
            // The publish timer is best effort: a later marker write schedules it again.
            (Stage::Wake, Event::Task(_)) => self.begin_summary(),
            (
                Stage::Summary,
                Event::SubOperation(SubOperationEvent::DocumentSyncResult { result }),
            ) => {
                if let Err(error) = result {
                    warn!(
                        %error,
                        "Failed to refresh realm usage summary after document sync reconciliation"
                    );
                }
                Ok(Next::Done(()))
            }
            (_, event) => Err(unexpected(event)),
        }
    }
}

#[cfg(test)]
mod state_machine_tests {
    use std::time::SystemTime;

    use aruna_core::errors::StorageError;
    use aruna_core::structs::storage::distribution::DistributionBinding;
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::task::{TaskEvent, TaskKey};
    use aruna_core::types::Value;

    use super::*;

    const REALM: RealmId = RealmId([4; 32]);
    const GROUP: GroupId = Ulid::from_parts(6, 6);
    const GENERATION: Ulid = Ulid::from_parts(1, 1);

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn bucket(group_id: GroupId, distributed: bool) -> Value {
        let info = BucketInfo {
            group_id,
            created_at: SystemTime::UNIX_EPOCH,
            created_by: Default::default(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: distributed.then(|| DistributionBinding {
                set_id: Ulid::from_parts(8, 8),
                member_id: Ulid::from_parts(9, 9),
                incarnation: 1,
                revision: Default::default(),
            }),
        };
        Value::from(info.to_bytes().unwrap())
    }

    fn storage(event: StorageEvent) -> IoResult {
        IoResult::Event(Box::new(Event::Storage(event)))
    }

    fn page(values: Vec<(Key, Value)>, next: Option<Key>) -> IoResult {
        storage(StorageEvent::IterResult {
            values,
            next_start_after: next,
        })
    }

    fn deleted() -> IoResult {
        storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        })
    }

    fn written() -> IoResult {
        storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        })
    }

    fn failed() -> IoResult {
        storage(StorageEvent::Error {
            error: StorageError::Timeout,
        })
    }

    fn spire() -> Vec<(Key, Value)> {
        vec![(Key::from(b"spire".to_vec()), bucket(GROUP, true))]
    }

    fn config_changed() -> RefreshUsageOperation {
        let target = DocumentTarget::RealmConfig { realm_id: REALM };
        RefreshUsageOperation::new(node(1), None, &[target], GENERATION)
    }

    fn done() -> Turn {
        Ok(Next::Done(()))
    }

    #[test]
    fn config_marks_groups() {
        let mut operation = config_changed();
        let summary = Key::from(aruna_core::structs::storage::usage::usage_summary_key(
            GROUP,
        ));
        let deletes = vec![
            (
                NODE_STATS_KEYSPACE.to_string(),
                Key::from(SUMMARY_GLOBAL_KEY.to_vec()),
            ),
            (NODE_STATS_KEYSPACE.to_string(), summary.clone()),
        ];
        let plain = bucket(Ulid::from_parts(7, 7), false);
        let buckets = vec![(Key::from(b"plain".to_vec()), plain), spire()[0].clone()];
        let marks = best_effort(StorageEffect::BatchWrite {
            writes: dirty_rows([GROUP], GENERATION),
            txn_id: None,
        });
        assert_eq!(operation.first(), RefreshUsageOperation::summary_page(None));
        let first = page(
            vec![(summary.clone(), Value::from(vec![1]))],
            Some(summary.clone()),
        );
        let next = RefreshUsageOperation::summary_page(Some(summary));
        assert_eq!(operation.next(first), next);
        let delete = best_effort(StorageEffect::BatchDelete {
            deletes,
            txn_id: None,
        });
        assert_eq!(operation.next(page(Vec::new(), None)), delete);
        assert_eq!(
            operation.next(deleted()),
            RefreshUsageOperation::bucket_page(None)
        );
        assert_eq!(operation.next(page(buckets, None)), marks);
        let wake = Ok(Next::Call(IoCall::Effect(Box::new(schedule_snapshot_publish()))));
        assert_eq!(operation.next(written()), wake);
        let scheduled = TaskEvent::TimerScheduled {
            key: TaskKey::PublishUsageSnapshots,
            after: std::time::Duration::from_secs(2),
        };
        assert_eq!(
            operation.next(IoResult::Event(Box::new(Event::Task(scheduled)))),
            done()
        );
    }

    #[test]
    fn usage_targets_sum() {
        let target = DocumentTarget::NodeUsage {
            realm_id: REALM,
            node_id: node(2),
            group_id: None,
        };
        let targets = [target];
        let mut operation = RefreshUsageOperation::new(node(1), None, &targets, GENERATION);
        let summary = RealmUsageOperation::summary(node(1), None, true, Vec::new());
        let child = IoCall::Child(boxed_suboperation(summary, summed));
        assert_eq!(operation.first(), Ok(Next::Call(child)));
        let finished = summed(Ok(Vec::new()));
        assert_eq!(operation.next(IoResult::Event(Box::new(finished))), done());
        // A failed sum is only logged.
        let mut failing = RefreshUsageOperation::new(node(1), None, &targets, GENERATION);
        failing.first().unwrap();
        let failed = summed(Err(IoError::Conflict.into()));
        assert_eq!(failing.next(IoResult::Event(Box::new(failed))), done());

        // Targets that touch no usage scope and no config refresh nothing.
        let other = DocumentTarget::NodeInfo {
            realm_id: REALM,
            node_id: node(2),
        };
        let mut idle = RefreshUsageOperation::new(node(1), None, &[other], GENERATION);
        assert_eq!(idle.first(), done());
    }

    #[test]
    fn failed_steps_continue() {
        // A failed summary scan skips to the bucket scan, a failed bucket scan to the end.
        let mut operation = config_changed();
        operation.first().unwrap();
        let buckets = RefreshUsageOperation::bucket_page(None);
        assert_eq!(operation.next(failed()), buckets);
        assert_eq!(operation.next(failed()), done());

        // A failed delete still scans the buckets; a failed marker write ends the refresh.
        let mut operation = config_changed();
        operation.first().unwrap();
        operation.next(page(Vec::new(), None)).unwrap();
        assert_eq!(operation.next(failed()), buckets);
        operation.next(page(spire(), None)).unwrap();
        assert_eq!(operation.next(failed()), done());

        // An unreadable bucket row and a refused timer end the marking, not the refresh.
        let mut operation = config_changed();
        operation.first().unwrap();
        operation.next(page(Vec::new(), None)).unwrap();
        operation.next(deleted()).unwrap();
        let garbled = (Key::from(b"bad".to_vec()), Value::from(vec![0xff; 3]));
        assert_eq!(operation.next(page(vec![garbled], None)), done());
        let mut operation = config_changed();
        operation.first().unwrap();
        operation.next(page(Vec::new(), None)).unwrap();
        operation.next(deleted()).unwrap();
        operation.next(page(spire(), None)).unwrap();
        operation.next(written()).unwrap();
        let refused = TaskEvent::Error {
            key: Some(TaskKey::PublishUsageSnapshots),
            message: "no task handle".to_string(),
        };
        assert_eq!(
            operation.next(IoResult::Event(Box::new(Event::Task(refused)))),
            done()
        );
    }

    #[test]
    fn rejects_stray_results() {
        let mut scanning = config_changed();
        scanning.first().unwrap();
        assert!(matches!(
            scanning.next(deleted()),
            Err(RefreshUsageError::Io(_))
        ));
        let mut values = config_changed();
        values.first().unwrap();
        let answer = values.next(IoResult::Values(Vec::new()));
        assert!(matches!(answer, Err(RefreshUsageError::Io(_))));
        let mut waking = config_changed();
        waking.first().unwrap();
        waking.next(page(Vec::new(), None)).unwrap();
        waking.next(deleted()).unwrap();
        waking.next(page(spire(), None)).unwrap();
        waking.next(written()).unwrap();
        assert!(matches!(
            waking.next(written()),
            Err(RefreshUsageError::Io(_))
        ));
    }
}
