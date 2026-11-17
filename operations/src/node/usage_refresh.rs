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

