//! Refreshes the realm usage summary for the usage targets a document sync touched.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;

use aruna_core::NodeId;
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, IterStart, StorageEffect};
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, S3_BUCKET_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::usage::{SUMMARY_GLOBAL_KEY, SUMMARY_GROUP_PREFIX};
use aruna_core::types::{Effects, GroupId, Key};
use smallvec::smallvec;
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::node::realm_usage::{RealmUsageError, RealmUsageOperation};
use crate::s3::usage::{dirty_rows, schedule_snapshot_publish};

/// Rows one scan page reads.
const PAGE_LIMIT: usize = 1_000;

#[derive(Debug, Error, PartialEq)]
pub enum RefreshUsageError {
    #[error("unexpected event while {0}")]
    UnexpectedEvent(&'static str),
    #[error("the usage refresh has not finished")]
    Unfinished,
}

#[derive(Debug, PartialEq)]
enum Stage {
    Idle,
    ClearScan(Vec<(String, Key)>),
    ClearDelete,
    BucketScan(BTreeSet<GroupId>),
    MarkWrite,
    Wake,
    Summary(Box<RealmUsageOperation>),
    Done(Result<(), RefreshUsageError>),
}

impl Stage {
    fn label(&self) -> &'static str {
        match self {
            Stage::Idle => "idle",
            Stage::ClearScan(_) => "scanning the realm summary",
            Stage::ClearDelete => "clearing the realm summary",
            Stage::BucketScan(_) => "scanning the distributed buckets",
            Stage::MarkWrite => "marking groups for a publish",
            Stage::Wake => "scheduling the publish",
            Stage::Summary(_) => "summing the realm usage",
            Stage::Done(_) => "done",
        }
    }
}

/// Refreshes the persisted realm summary for exactly the `NodeUsage` scopes a reconcile touched.
/// A realm config change first clears the whole summary and marks the groups of local
/// distributed buckets for a publish, since a new coordinator moves replica shares. Each step
/// that fails is logged and skipped, so one failure never blocks the others.
#[derive(Debug, PartialEq)]
pub struct RefreshUsageOperation {
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

    fn summary_page(start: Option<Key>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: NODE_STATS_KEYSPACE.to_string(),
            prefix: Some(Key::from(SUMMARY_GROUP_PREFIX.to_vec())),
            start: start.map(IterStart::After),
            limit: PAGE_LIMIT,
            txn_id: None,
        })]
    }

    fn bucket_page(start: Option<Key>) -> Effects {
        smallvec![Effect::Storage(StorageEffect::Iter {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            prefix: None,
            start: start.map(IterStart::After),
            limit: PAGE_LIMIT,
            txn_id: None,
        })]
    }

    fn begin_buckets(&mut self) -> Effects {
        self.stage = Stage::BucketScan(BTreeSet::new());
        Self::bucket_page(None)
    }

    fn begin_summary(&mut self) -> Effects {
        if !self.include_global && self.groups.is_empty() {
            self.stage = Stage::Done(Ok(()));
            return smallvec![];
        }
        let groups = std::mem::take(&mut self.groups);
        let mut summary = Box::new(RealmUsageOperation::summary(
            self.local_node_id,
            self.realm_id,
            self.include_global,
            groups,
        ));
        let effects = summary.start();
        self.stage = Stage::Summary(summary);
        effects
    }

    fn clear_failed(&mut self, error: &dyn std::fmt::Display) -> Effects {
        warn!(%error, "Failed to clear realm usage summary after realm config change");
        self.begin_buckets()
    }

    fn mark_failed(&mut self, error: &dyn std::fmt::Display) -> Effects {
        warn!(%error, "Failed to mark distributed bucket groups for a usage publish");
        self.begin_summary()
    }

    fn scan_summary(&mut self, event: StorageEvent) -> Result<Effects, RefreshUsageError> {
        match event {
            StorageEvent::IterResult {
                values,
                next_start_after,
            } => {
                let Stage::ClearScan(deletes) = &mut self.stage else {
                    return Err(RefreshUsageError::UnexpectedEvent("scanning the summary"));
                };
                deletes.extend(
                    (values.into_iter()).map(|(key, _)| (NODE_STATS_KEYSPACE.to_string(), key)),
                );
                if let Some(start) = next_start_after {
                    return Ok(Self::summary_page(Some(start)));
                }
                let Stage::ClearScan(deletes) = std::mem::replace(&mut self.stage, Stage::ClearDelete)
                else {
                    return Err(RefreshUsageError::UnexpectedEvent("scanning the summary"));
                };
                Ok(smallvec![Effect::Storage(StorageEffect::BatchDelete {
                    deletes,
                    txn_id: None,
                })])
            }
            StorageEvent::Error { error } => Ok(self.clear_failed(&error)),
            _ => Err(RefreshUsageError::UnexpectedEvent(self.stage.label())),
        }
    }

    fn scan_buckets(&mut self, event: StorageEvent) -> Result<Effects, RefreshUsageError> {
        let (values, next_start_after) = match event {
            StorageEvent::IterResult {
                values,
                next_start_after,
            } => (values, next_start_after),
            StorageEvent::Error { error } => return Ok(self.mark_failed(&error)),
            _ => return Err(RefreshUsageError::UnexpectedEvent(self.stage.label())),
        };
        let Stage::BucketScan(groups) = &mut self.stage else {
            return Err(RefreshUsageError::UnexpectedEvent("scanning the buckets"));
        };
        for (_, value) in values {
            match BucketInfo::from_bytes(value.as_ref()) {
                Ok(info) if info.distribution.is_some() => {
                    groups.insert(info.group_id);
                }
                Ok(_) => {}
                Err(error) => return Ok(self.mark_failed(&error)),
            }
        }
        if let Some(start) = next_start_after {
            return Ok(Self::bucket_page(Some(start)));
        }
        let Stage::BucketScan(groups) = std::mem::replace(&mut self.stage, Stage::MarkWrite) else {
            return Err(RefreshUsageError::UnexpectedEvent("scanning the buckets"));
        };
        if groups.is_empty() {
            return Ok(self.begin_summary());
        }
        Ok(smallvec![Effect::Storage(StorageEffect::BatchWrite {
            writes: dirty_rows(groups, self.generation),
            txn_id: None,
        })])
    }

    fn sum(&mut self, event: Event) -> Result<Effects, RefreshUsageError> {
        let Stage::Summary(summary) = &mut self.stage else {
            return Err(RefreshUsageError::UnexpectedEvent("summing the realm usage"));
        };
        let effects = summary.step(event);
        if !summary.is_complete() {
            return Ok(effects);
        }
        let Stage::Summary(summary) = std::mem::replace(&mut self.stage, Stage::Done(Ok(())))
        else {
            return Err(RefreshUsageError::UnexpectedEvent("summing the realm usage"));
        };
        match summary.finalize() {
            Ok(_) => {}
            Err(RealmUsageError::UnexpectedEvent(state)) => {
                return Err(RefreshUsageError::UnexpectedEvent(state));
            }
            Err(error) => warn!(
                %error,
                "Failed to refresh realm usage summary after document sync reconciliation"
            ),
        }
        Ok(effects)
    }

    fn advance(&mut self, event: Event) -> Result<Effects, RefreshUsageError> {
        let unexpected = RefreshUsageError::UnexpectedEvent(self.stage.label());
        match self.stage {
            Stage::Summary(_) => self.sum(event),
            Stage::ClearScan(_) => match event {
                Event::Storage(event) => self.scan_summary(event),
                _ => Err(unexpected),
            },
            Stage::ClearDelete => match event {
                Event::Storage(StorageEvent::BatchDeleteResult { .. }) => Ok(self.begin_buckets()),
                Event::Storage(StorageEvent::Error { error }) => Ok(self.clear_failed(&error)),
                _ => Err(unexpected),
            },
            Stage::BucketScan(_) => match event {
                Event::Storage(event) => self.scan_buckets(event),
                _ => Err(unexpected),
            },
            Stage::MarkWrite => match event {
                Event::Storage(StorageEvent::BatchWriteResult { .. }) => {
                    self.stage = Stage::Wake;
                    Ok(smallvec![schedule_snapshot_publish()])
                }
                Event::Storage(StorageEvent::Error { error }) => Ok(self.mark_failed(&error)),
                _ => Err(unexpected),
            },
            // The publish timer is best effort: a later marker write schedules it again.
            Stage::Wake => match event {
                Event::Task(_) => Ok(self.begin_summary()),
                _ => Err(unexpected),
            },
            Stage::Idle | Stage::Done(_) => Err(unexpected),
        }
    }
}

impl Operation for RefreshUsageOperation {
    type Output = ();
    type Error = RefreshUsageError;

    fn start(&mut self) -> Effects {
        if !self.config_changed {
            return self.begin_summary();
        }
        self.stage = Stage::ClearScan(vec![(
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(SUMMARY_GLOBAL_KEY.to_vec()),
        )]);
        Self::summary_page(None)
    }

    fn step(&mut self, event: Event) -> Effects {
        match self.advance(event) {
            Ok(effects) => effects,
            Err(error) => {
                self.stage = Stage::Done(Err(error));
                smallvec![]
            }
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.stage, Stage::Done(_))
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        match self.stage {
            Stage::Done(result) => result,
            _ => Err(RefreshUsageError::Unfinished),
        }
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[cfg(test)]
mod state_machine_tests {
    use std::time::SystemTime;

    use aruna_core::errors::StorageError;
    use aruna_core::operation_check::{
        check_abort, check_trace, rejects_events, rejects_finalize, sample_events,
    };
    use aruna_core::structs::storage::distribution::DistributionBinding;
    use aruna_core::structs::storage::format::Compression;
    use aruna_core::structs::storage::usage::{UsageCounters, global_shard_keys, usage_summary_key};
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

    fn page(values: Vec<(Key, Value)>, next: Option<Key>) -> Event {
        Event::Storage(StorageEvent::IterResult {
            values,
            next_start_after: next,
        })
    }

    fn failed() -> Event {
        Event::Storage(StorageEvent::Error {
            error: StorageError::Timeout,
        })
    }

    fn config_changed() -> RefreshUsageOperation {
        let target = DocumentTarget::RealmConfig { realm_id: REALM };
        RefreshUsageOperation::new(node(1), None, &[target], GENERATION)
    }

    fn summary_key(group_id: GroupId) -> Key {
        Key::from(usage_summary_key(group_id))
    }

    fn global_read() -> Effect {
        Effect::Storage(StorageEffect::BatchRead {
            reads: global_shard_keys()
                .into_iter()
                .map(|key| (aruna_core::keyspaces::USAGE_STATS_KEYSPACE.to_string(), Key::from(key)))
                .collect(),
            txn_id: None,
        })
    }

    #[test]
    fn config_change_marks_groups() {
        let mut operation = config_changed();
        let summary = summary_key(GROUP);
        let deletes = vec![
            (NODE_STATS_KEYSPACE.to_string(), Key::from(SUMMARY_GLOBAL_KEY.to_vec())),
            (NODE_STATS_KEYSPACE.to_string(), summary.clone()),
        ];
        let buckets = vec![
            (Key::from(b"plain".to_vec()), bucket(Ulid::from_parts(7, 7), false)),
            (Key::from(b"spire".to_vec()), bucket(GROUP, true)),
        ];
        check_trace(
            &mut operation,
            &RefreshUsageOperation::summary_page(None),
            vec![
                (
                    page(vec![(summary.clone(), Value::from(vec![1]))], Some(summary.clone())),
                    RefreshUsageOperation::summary_page(Some(summary)).to_vec(),
                ),
                (
                    page(Vec::new(), None),
                    vec![Effect::Storage(StorageEffect::BatchDelete {
                        deletes,
                        txn_id: None,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::BatchDeleteResult {
                        entries: Vec::new(),
                    }),
                    RefreshUsageOperation::bucket_page(None).to_vec(),
                ),
                (
                    page(buckets, None),
                    vec![Effect::Storage(StorageEffect::BatchWrite {
                        writes: dirty_rows([GROUP], GENERATION),
                        txn_id: None,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::BatchWriteResult {
                        entries: Vec::new(),
                    }),
                    vec![schedule_snapshot_publish()],
                ),
                (
                    Event::Task(TaskEvent::TimerScheduled {
                        key: TaskKey::PublishUsageSnapshots,
                        after: std::time::Duration::from_secs(2),
                    }),
                    vec![],
                ),
            ],
        );
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn usage_targets_sum() {
        let target = DocumentTarget::NodeUsage {
            realm_id: REALM,
            node_id: node(2),
            group_id: None,
        };
        let mut operation = RefreshUsageOperation::new(node(1), None, &[target], GENERATION);
        let total = UsageCounters::default().to_bytes().unwrap();
        check_trace(
            &mut operation,
            &[global_read()],
            vec![
                (
                    Event::Storage(StorageEvent::BatchReadResult { values: Vec::new() }),
                    vec![Effect::Storage(StorageEffect::Iter {
                        key_space: NODE_STATS_KEYSPACE.to_string(),
                        prefix: Some(Key::from(
                            aruna_core::structs::storage::usage::USAGE_GLOBAL_PREFIX.to_vec(),
                        )),
                        start: None,
                        limit: 1_000,
                        txn_id: None,
                    })],
                ),
                (
                    page(Vec::new(), None),
                    vec![Effect::Storage(StorageEffect::BatchWrite {
                        writes: vec![(
                            NODE_STATS_KEYSPACE.to_string(),
                            Key::from(SUMMARY_GLOBAL_KEY.to_vec()),
                            Value::from(total),
                        )],
                        txn_id: None,
                    })],
                ),
                (
                    Event::Storage(StorageEvent::BatchWriteResult {
                        entries: Vec::new(),
                    }),
                    vec![],
                ),
            ],
        );
        assert_eq!(operation.finalize(), Ok(()));

        // Targets that touch no usage scope and no config refresh nothing.
        let other = DocumentTarget::NodeInfo {
            realm_id: REALM,
            node_id: node(2),
        };
        let mut idle = RefreshUsageOperation::new(node(1), None, &[other], GENERATION);
        check_trace(&mut idle, &[], vec![]);
        assert_eq!(idle.finalize(), Ok(()));
    }

    #[test]
    fn failed_steps_continue() {
        // A failed summary scan skips to the bucket scan, a failed bucket scan to the end.
        let mut operation = config_changed();
        operation.start();
        assert_eq!(
            operation.step(failed()).to_vec(),
            RefreshUsageOperation::bucket_page(None).to_vec()
        );
        assert!(operation.step(failed()).is_empty());
        assert_eq!(operation.finalize(), Ok(()));

        // A failed delete still scans the buckets; a failed marker write ends the refresh.
        let mut operation = config_changed();
        operation.start();
        operation.step(page(Vec::new(), None));
        assert_eq!(
            operation.step(failed()).to_vec(),
            RefreshUsageOperation::bucket_page(None).to_vec()
        );
        operation.step(page(vec![(Key::from(b"spire".to_vec()), bucket(GROUP, true))], None));
        assert!(operation.step(failed()).is_empty());
        assert_eq!(operation.finalize(), Ok(()));

        // An unreadable bucket row and a refused timer end the marking, not the refresh.
        let mut operation = config_changed();
        operation.start();
        operation.step(page(Vec::new(), None));
        operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        let garbled = (Key::from(b"bad".to_vec()), Value::from(vec![0xff; 3]));
        assert!(operation.step(page(vec![garbled], None)).is_empty());
        assert_eq!(operation.finalize(), Ok(()));
        let mut operation = config_changed();
        operation.start();
        operation.step(page(Vec::new(), None));
        operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
            entries: Vec::new(),
        }));
        operation.step(page(vec![(Key::from(b"spire".to_vec()), bucket(GROUP, true))], None));
        operation.step(Event::Storage(StorageEvent::BatchWriteResult {
            entries: Vec::new(),
        }));
        let refused = Event::Task(TaskEvent::Error {
            key: Some(TaskKey::PublishUsageSnapshots),
            message: "no task handle".to_string(),
        });
        assert!(operation.step(refused).is_empty());
        assert_eq!(operation.finalize(), Ok(()));
    }

    #[test]
    fn rejects_stray_events() {
        let started = || {
            let mut operation = config_changed();
            operation.start();
            operation
        };
        let scan = |event: &Event| {
            matches!(
                event,
                Event::Storage(StorageEvent::IterResult { .. } | StorageEvent::Error { .. })
            )
        };
        for (label, effects, error) in rejects_events(started, sample_events, scan) {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(matches!(error, RefreshUsageError::UnexpectedEvent(_)), "{label}");
        }
        let waking = || {
            let mut operation = started();
            operation.step(page(Vec::new(), None));
            operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
                entries: Vec::new(),
            }));
            operation.step(page(vec![(Key::from(b"spire".to_vec()), bucket(GROUP, true))], None));
            operation.step(Event::Storage(StorageEvent::BatchWriteResult {
                entries: Vec::new(),
            }));
            operation
        };
        let task = |event: &Event| matches!(event, Event::Task(_));
        for (label, effects, error) in rejects_events(waking, sample_events, task) {
            assert!(effects.is_empty(), "{label} emitted effects");
            assert!(matches!(error, RefreshUsageError::UnexpectedEvent(_)), "{label}");
        }
        let fresh = || config_changed();
        assert_eq!(rejects_events(fresh, sample_events, |_| false).len(), sample_events().len());
    }

    #[test]
    fn abort_and_finalize() {
        let mut operation = config_changed();
        check_abort(&mut operation, &[]);
        operation.start();
        check_abort(&mut operation, &[]);
        operation.step(page(Vec::new(), None));
        check_abort(&mut operation, &[]);
        rejects_finalize(operation);
    }
}
