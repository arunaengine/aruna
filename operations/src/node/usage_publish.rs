//! Publishes this node's usage snapshots, refreshes the realm summary and keeps dirty markers.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, HashSet};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::events::Event;
use aruna_core::keyspaces::{NODE_STATS_KEYSPACE, USAGE_STATS_KEYSPACE};
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::rows::{PrefixPages, done};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::usage::{
    DIRTY_GLOBAL_KEY, DIRTY_PREFIX, NodeUsageSnapshot, USAGE_GROUP_PREFIX, UsageCounters,
    dirty_group_id, dirty_group_key, dirty_share_id, usage_global_key, usage_group_id,
    usage_group_key, usage_node_id, usage_snapshot_key,
};
use aruna_core::task::TaskEvent;
use aruna_core::types::{GroupId, Key, Value};
use thiserror::Error;
use tracing::warn;
use ulid::Ulid;

use crate::distribution::replica_shares::{GroupShare, ReplicaSharesOperation};
use crate::driver::DriverContext;
use crate::node::realm_usage::{RealmUsage, RealmUsageError, RealmUsageOperation};
use crate::s3::usage::{
    LoadCountersError, global_shard_rows, global_sum, schedule_snapshot_publish,
};
use crate::sync::replicate_documents::{
    ReplicateDocumentsConfig, ReplicateDocumentsError, ReplicateDocumentsOperation,
};

/// Rows one marker or counter scan reads per page.
const SCAN_PAGE: usize = 1_000;
const GROUP_COUNTER_PREFIX: &[u8] = b"group/";

#[derive(Debug, Error, PartialEq)]
pub enum UsagePublishError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("{0}")]
    Failed(String),
}

fn failed(error: impl std::fmt::Display) -> UsagePublishError {
    UsagePublishError::Failed(error.to_string())
}

/// Groups/global scope touched by a publish run, so callers can refresh exactly
/// the affected realm summary cache entries.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublishedUsage {
    pub global: bool,
    pub groups: Vec<GroupId>,
}

impl PublishedUsage {
    pub fn is_empty(&self) -> bool {
        !self.global && self.groups.is_empty()
    }

    fn targets(&self, realm_id: RealmId, node_id: NodeId) -> Vec<DocumentTarget> {
        let mut targets = Vec::with_capacity(self.groups.len() + usize::from(self.global));
        if self.global {
            targets.push(DocumentTarget::NodeUsage {
                realm_id,
                node_id,
                group_id: None,
            });
        }
        for group_id in &self.groups {
            targets.push(DocumentTarget::NodeUsage {
                realm_id,
                node_id,
                group_id: Some(*group_id),
            });
        }
        targets
    }
}

/// The groups and global scope one publish reports.
#[derive(Debug, Default, PartialEq)]
struct UsageScope {
    global: bool,
    groups: BTreeSet<GroupId>,
}

impl UsageScope {
    fn add_markers(&mut self, rows: &[(Key, Value)]) {
        for (key, _) in rows {
            if key.as_ref() == DIRTY_GLOBAL_KEY {
                self.global = true;
            } else if let Some(group_id) = dirty_group_id(key.as_ref()) {
                self.groups.insert(group_id);
            } else if let Some(group_id) = dirty_share_id(key.as_ref()) {
                // A share marker the scan saw without its group's marker still needs a publish.
                self.groups.insert(group_id);
            }
        }
    }

    fn add_counters(&mut self, rows: &[(Key, Value)]) {
        for (key, _) in rows {
            if let Some(rest) = key.as_ref().strip_prefix(GROUP_COUNTER_PREFIX)
                && let Ok(bytes) = <[u8; 16]>::try_from(rest)
            {
                self.groups.insert(GroupId::from_bytes(bytes));
            }
        }
    }

    /// Snapshots missing a live counter republish as zero so peers stop summing stale data.
    fn add_snapshots(&mut self, rows: &[(Key, Value)], node_id: NodeId) {
        for (key, _) in rows {
            if usage_node_id(key.as_ref()) == Some(node_id)
                && let Some(group_id) = usage_group_id(key.as_ref())
            {
                self.groups.insert(group_id);
            }
        }
    }
}

/// The live counters and replica shares of one publish.
#[derive(Debug, PartialEq)]
struct UsageRead {
    shares: BTreeMap<GroupId, GroupShare>,
    global: UsageCounters,
    groups: Vec<UsageCounters>,
}

/// Reads counters and replica shares from one snapshot, as a purge debits both together.
#[derive(Debug, PartialEq)]
struct UsageReadOperation {
    global: bool,
    groups: Vec<GroupId>,
    shares: ReplicaSharesOperation,
    read: Option<BTreeMap<GroupId, GroupShare>>,
}

impl UsageReadOperation {
    fn new(global: bool, groups: Vec<GroupId>) -> Self {
        Self {
            global,
            groups,
            shares: ReplicaSharesOperation::new(),
            read: None,
        }
    }

    fn share_turn(
        &mut self,
        turn: Next<BTreeMap<GroupId, GroupShare>>,
    ) -> Result<Next<UsageRead>, UsagePublishError> {
        match turn {
            Next::Call(call) => Ok(Next::Call(call)),
            Next::Done(shares) => {
                self.read = Some(shares);
                let mut rows = Vec::with_capacity(self.groups.len());
                if self.global {
                    rows.extend(global_shard_rows());
                }
                rows.extend(self.groups.iter().map(|group_id| {
                    let key = Key::from(usage_group_key(*group_id));
                    (USAGE_STATS_KEYSPACE.to_string(), key)
                }));
                Ok(Next::Call(IoCall::Read(rows)))
            }
            Next::Wait | Next::Then { .. } => Err(IoError::Unfinished.into()),
        }
    }

    fn counters(&mut self, values: Vec<Option<Value>>) -> Result<UsageRead, UsagePublishError> {
        let shares = self.read.take().ok_or(IoError::Unfinished)?;
        let split = values.len().saturating_sub(self.groups.len());
        let mut values = values;
        let group_values = values.split_off(split);
        let global = match self.global {
            true => global_sum::<LoadCountersError>(values).map_err(failed)?,
            false => UsageCounters::default(),
        };
        let groups = (group_values.into_iter())
            .map(|value| match value {
                Some(bytes) => UsageCounters::from_bytes(bytes.as_ref()).map_err(failed),
                None => Ok(UsageCounters::default()),
            })
            .collect::<Result<_, _>>()?;
        Ok(UsageRead {
            shares,
            global,
            groups,
        })
    }
}

impl Operation for UsageReadOperation {
    type Output = UsageRead;
    type Error = UsagePublishError;

    fn first(&mut self) -> Result<Next<UsageRead>, UsagePublishError> {
        let turn = self.shares.first().map_err(failed)?;
        self.share_turn(turn)
    }

    fn next(&mut self, result: IoResult) -> Result<Next<UsageRead>, UsagePublishError> {
        if self.read.is_some() {
            return Ok(Next::Done(self.counters(result.values()?)?));
        }
        let turn = self.shares.next(result).map_err(failed)?;
        self.share_turn(turn)
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::ReadSnapshot
    }
}

/// The snapshot rows of one publish. Each key has a single writer, this node.
fn snapshot_rows(
    node_id: NodeId,
    scope: &PublishedUsage,
    read: UsageRead,
) -> Result<Vec<(String, Key, Value)>, UsagePublishError> {
    let row = |key: Vec<u8>, snapshot: NodeUsageSnapshot| {
        let bytes = snapshot.to_bytes().map_err(failed)?;
        Ok::<_, UsagePublishError>((
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(key),
            Value::from(bytes),
        ))
    };
    let UsageRead {
        shares,
        global,
        groups,
    } = read;
    let mut writes = Vec::with_capacity(scope.groups.len() + 1);
    if scope.global {
        let (mut replicas, mut revisions) = (UsageCounters::default(), BTreeMap::new());
        for share in shares.values() {
            replicas.add(&share.replicas).map_err(failed)?;
            revisions.extend(&share.revisions);
        }
        let snapshot = NodeUsageSnapshot {
            node_id,
            counters: global,
            replicas,
            revisions,
        };
        writes.push(row(usage_global_key(node_id), snapshot)?);
    }
    for (group_id, counters) in scope.groups.iter().zip(groups) {
        let share = shares.get(group_id).cloned().unwrap_or_default();
        let snapshot = NodeUsageSnapshot {
            node_id,
            counters,
            replicas: share.replicas,
            revisions: share.revisions,
        };
        writes.push(row(usage_snapshot_key(*group_id, node_id), snapshot)?);
    }
    Ok(writes)
}

#[derive(Debug, PartialEq)]
enum PublishStep {
    Markers,
    Counters,
    Snapshots,
    Read,
    Write,
    Replicate,
    Summary,
    Clear,
    /// The retry signal runs; the run then fails with this error.
    Retry(String),
}

type PublishTurn = Result<Next<PublishedUsage>, UsagePublishError>;

/// Rebuilds this node's snapshots from live counters and replicates them: dirty mode refreshes
/// marked groups, full mode all groups plus the global. Markers clear only after replication (and
/// with `refresh` the realm summary), never if re-dirtied; a later failure marks a retry.
#[derive(Debug, PartialEq)]
pub struct PublishUsageOperation {
    node_id: NodeId,
    realm_id: RealmId,
    /// The realm of this node's net transport, which the summary is computed for.
    serving_realm: Option<RealmId>,
    full: bool,
    refresh: bool,
    retry_generation: Ulid,
    pages: PrefixPages,
    observed: Vec<(Key, Value)>,
    scope: UsageScope,
    published: PublishedUsage,
    step: PublishStep,
}

impl PublishUsageOperation {
    pub fn new(
        context: &DriverContext,
        (node_id, realm_id): (NodeId, RealmId),
        full: bool,
        refresh: bool,
    ) -> Self {
        let serving_realm = context.node_and_realm().map(|(_, realm_id)| realm_id);
        Self::with_generation(
            (node_id, realm_id),
            serving_realm,
            full,
            refresh,
            Ulid::generate(),
        )
    }

    fn with_generation(
        (node_id, realm_id): (NodeId, RealmId),
        serving_realm: Option<RealmId>,
        full: bool,
        refresh: bool,
        retry_generation: Ulid,
    ) -> Self {
        Self {
            node_id,
            realm_id,
            serving_realm,
            full,
            refresh,
            retry_generation,
            pages: marker_pages(),
            observed: Vec::new(),
            scope: UsageScope::default(),
            published: PublishedUsage::default(),
            step: PublishStep::Markers,
        }
    }

    fn scan(&mut self, step: PublishStep, pages: PrefixPages) -> PublishTurn {
        self.step = step;
        self.pages = pages;
        Ok(Next::Call(self.pages.call()))
    }

    /// Reads the live values once the scope is known; nothing to report ends the run.
    fn read_scope(&mut self) -> PublishTurn {
        let scope = std::mem::take(&mut self.scope);
        self.published = PublishedUsage {
            global: scope.global,
            groups: scope.groups.into_iter().collect(),
        };
        if self.published.is_empty() {
            self.observed.clear();
            return Ok(Next::Done(PublishedUsage::default()));
        }
        self.step = PublishStep::Read;
        let read = UsageReadOperation::new(self.published.global, self.published.groups.clone());
        Ok(Next::Call(IoCall::child(read)))
    }

    fn replicate(&mut self) -> PublishTurn {
        self.step = PublishStep::Replicate;
        let replicate = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id: self.realm_id,
            local_node_id: self.node_id,
            excluded_peers: Vec::new(),
            documents: self.published.targets(self.realm_id, self.node_id),
            // Steady-state publishers must not mint the shared node-usage
            // genesis; bootstrap/placement-origin paths do that authoritatively.
            allow_genesis: false,
        });
        Ok(Next::Call(IoCall::child(replicate)))
    }

    fn after_replication(&mut self) -> PublishTurn {
        if !self.refresh {
            return self.clear();
        }
        self.step = PublishStep::Summary;
        let summary = RealmUsageOperation::summary(
            self.node_id,
            self.serving_realm,
            self.published.global,
            self.published.groups.clone(),
        );
        Ok(Next::Call(IoCall::child(summary)))
    }

    /// Replication, and a refresh's summary, succeeded: only now the markers are consumed.
    fn clear(&mut self) -> PublishTurn {
        self.step = PublishStep::Clear;
        let observed = std::mem::take(&mut self.observed);
        Ok(Next::Call(IoCall::child(ClearMarkersOperation::new(
            observed,
        ))))
    }

    fn retry(&mut self, error: String) -> PublishTurn {
        let generation = Value::from(self.retry_generation.to_bytes().to_vec());
        let markers = retry_markers(&self.published, &self.observed, generation);
        self.step = PublishStep::Retry(error);
        Ok(Next::Call(IoCall::child(RetrySignalOperation::new(
            markers,
        ))))
    }
}

fn marker_pages() -> PrefixPages {
    PrefixPages::new(NODE_STATS_KEYSPACE, DIRTY_PREFIX.to_vec(), SCAN_PAGE)
}

impl Operation for PublishUsageOperation {
    type Output = PublishedUsage;
    type Error = UsagePublishError;

    fn first(&mut self) -> PublishTurn {
        Ok(Next::Call(self.pages.call()))
    }

    fn next(&mut self, result: IoResult) -> PublishTurn {
        match std::mem::replace(&mut self.step, PublishStep::Read) {
            PublishStep::Markers => {
                let rows = self.pages.take(result)?;
                self.scope.add_markers(&rows);
                self.observed.extend(rows);
                if !self.pages.done() {
                    self.step = PublishStep::Markers;
                    return Ok(Next::Call(self.pages.call()));
                }
                if !self.full {
                    return self.read_scope();
                }
                self.scope.global = true;
                let pages = PrefixPages::new(
                    USAGE_STATS_KEYSPACE,
                    GROUP_COUNTER_PREFIX.to_vec(),
                    SCAN_PAGE,
                );
                self.scan(PublishStep::Counters, pages)
            }
            PublishStep::Counters => {
                let rows = self.pages.take(result)?;
                self.scope.add_counters(&rows);
                if !self.pages.done() {
                    self.step = PublishStep::Counters;
                    return Ok(Next::Call(self.pages.call()));
                }
                let pages =
                    PrefixPages::new(NODE_STATS_KEYSPACE, USAGE_GROUP_PREFIX.to_vec(), SCAN_PAGE);
                self.scan(PublishStep::Snapshots, pages)
            }
            PublishStep::Snapshots => {
                let rows = self.pages.take(result)?;
                self.scope.add_snapshots(&rows, self.node_id);
                if !self.pages.done() {
                    self.step = PublishStep::Snapshots;
                    return Ok(Next::Call(self.pages.call()));
                }
                self.read_scope()
            }
            PublishStep::Read => {
                let read = result.child::<UsageRead, UsagePublishError>()??;
                // Persisted now, while the markers stay until replication accepted the documents.
                let writes = snapshot_rows(self.node_id, &self.published, read)?;
                self.step = PublishStep::Write;
                Ok(Next::Call(IoCall::Write(writes)))
            }
            PublishStep::Write => {
                done(result).map_err(|error| failed(format!("snapshot write failed: {error}")))?;
                self.replicate()
            }
            PublishStep::Replicate => match result.child::<(), ReplicateDocumentsError>()? {
                Ok(()) => self.after_replication(),
                Err(error) => {
                    self.retry(format!("node usage snapshot replication failed: {error}"))
                }
            },
            PublishStep::Summary => match result.child::<Vec<RealmUsage>, RealmUsageError>()? {
                Ok(_) => self.clear(),
                Err(error) => self.retry(error.to_string()),
            },
            PublishStep::Clear => match result.child::<(), IoError>()? {
                // A lost conflict means a counter update re-dirtied a marker, which stays.
                Ok(()) | Err(IoError::Conflict) => {
                    Ok(Next::Done(std::mem::take(&mut self.published)))
                }
                Err(error) => Err(failed(format!("marker cleanup failed: {error}"))),
            },
            PublishStep::Retry(error) => match result.child::<(), IoError>()? {
                Ok(()) => Err(UsagePublishError::Failed(error)),
                Err(retry) => Err(failed(format!(
                    "{error}; additionally failed to mark usage snapshot retry: \
                     retry marker write failed: {retry}"
                ))),
            },
        }
    }
}

/// Marker rows for the published scopes a concurrent write did not mark already, so the next
/// publish retries them; none when nothing was published.
fn retry_markers(
    published: &PublishedUsage,
    observed_markers: &[(Key, Value)],
    generation: Value,
) -> Vec<(String, Key, Value)> {
    let observed_keys: HashSet<&[u8]> = (observed_markers.iter())
        .map(|(key, _)| key.as_ref())
        .collect();
    let mut keys = Vec::with_capacity(published.groups.len() + usize::from(published.global));
    if published.global {
        keys.push(DIRTY_GLOBAL_KEY.to_vec());
    }
    keys.extend(
        published
            .groups
            .iter()
            .map(|group_id| dirty_group_key(*group_id)),
    );
    (keys.into_iter())
        .filter(|key| !observed_keys.contains(key.as_slice()))
        .map(|key| {
            (
                NODE_STATS_KEYSPACE.to_string(),
                Key::from(key),
                generation.clone(),
            )
        })
        .collect()
}

/// Writes the retry markers, then moves the debounced publish timer up. A refused timer is only
/// logged: the markers alone keep the retry for the next run.
#[derive(Debug, PartialEq)]
struct RetrySignalOperation {
    markers: Vec<(String, Key, Value)>,
    scheduling: bool,
}

impl RetrySignalOperation {
    fn new(markers: Vec<(String, Key, Value)>) -> Self {
        Self {
            markers,
            scheduling: false,
        }
    }

    fn schedule(&mut self) -> Result<Next<()>, IoError> {
        self.scheduling = true;
        Ok(Next::Call(IoCall::Effect(Box::new(
            schedule_snapshot_publish(),
        ))))
    }
}

impl Operation for RetrySignalOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        if self.markers.is_empty() {
            return self.schedule();
        }
        Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.markers))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if !self.scheduling {
            done(result)?;
            return self.schedule();
        }
        match result.event()? {
            Event::Task(TaskEvent::TimerScheduled { .. }) => {}
            Event::Task(TaskEvent::Error { message, .. }) => {
                warn!(message = %message, "Failed to schedule usage snapshot retry");
            }
            other => warn!(
                event = other.label(),
                "Unexpected usage snapshot retry schedule result"
            ),
        }
        Ok(Next::Done(()))
    }
}

/// Deletes each observed dirty marker only if its stored generation still matches the one seen
/// at publish start. The re-read runs in the deleting transaction, so a racing counter update
/// makes the commit conflict and its retry signal is never lost.
#[derive(Debug, PartialEq)]
pub struct ClearMarkersOperation {
    observed: Vec<(Key, Value)>,
    deleting: bool,
}

impl ClearMarkersOperation {
    pub fn new(observed: Vec<(Key, Value)>) -> Self {
        Self {
            observed,
            deleting: false,
        }
    }
}

impl Operation for ClearMarkersOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        if self.observed.is_empty() {
            return Ok(Next::Done(()));
        }
        let reads = (self.observed.iter())
            .map(|(key, _)| (NODE_STATS_KEYSPACE.to_string(), key.clone()))
            .collect();
        Ok(Next::Call(IoCall::Read(reads)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.deleting {
            return done(result).map(Next::Done);
        }
        let current = result.values()?;
        let deletes: Vec<_> = (self.observed.iter().zip(current))
            .filter(|((_, seen), stored)| stored.as_ref() == Some(seen))
            .map(|((key, _), _)| (NODE_STATS_KEYSPACE.to_string(), key.clone()))
            .collect();
        if deletes.is_empty() {
            return Ok(Next::Done(()));
        }
        self.deleting = true;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    fn transaction(&self) -> aruna_core::operation::TransactionMode {
        aruna_core::operation::TransactionMode::Write
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(seed: u8) -> (Key, Value) {
        (Key::from(vec![b'd', seed]), Value::from(vec![seed]))
    }

    const REALM: RealmId = RealmId([4; 32]);
    const GROUP: GroupId = Ulid::from_parts(9, 9);

    fn node() -> NodeId {
        iroh::SecretKey::from_bytes(&[1; 32]).public()
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    fn publish(full: bool, refresh: bool) -> PublishUsageOperation {
        let generation = Ulid::from_parts(1, 1);
        PublishUsageOperation::with_generation((node(), REALM), None, full, refresh, generation)
    }

    fn dirty_group() -> (Key, Value) {
        (Key::from(dirty_group_key(GROUP)), Value::from(vec![1]))
    }

    fn page(rows: Vec<(Key, Value)>) -> IoResult {
        IoResult::Page { rows, next: None }
    }

    fn read_result() -> UsageRead {
        UsageRead {
            shares: BTreeMap::new(),
            global: UsageCounters::default(),
            groups: vec![UsageCounters::default()],
        }
    }

    fn published() -> PublishedUsage {
        PublishedUsage {
            global: false,
            groups: vec![GROUP],
        }
    }

    /// A publish standing at its replication step after reading and writing one group.
    fn replicating(refresh: bool) -> PublishUsageOperation {
        let mut operation = publish(false, refresh);
        operation.first().unwrap();
        operation.next(page(vec![dirty_group()])).unwrap();
        operation
            .next(child::<_, UsagePublishError>(Ok(read_result())))
            .unwrap();
        operation.next(IoResult::Done).unwrap();
        operation
    }

    #[test]
    fn nothing_dirty_ends() {
        let mut operation = publish(false, false);
        assert_eq!(operation.first(), Ok(Next::Call(marker_pages().call())));
        let answer = operation.next(page(Vec::new()));
        assert_eq!(answer, Ok(Next::Done(PublishedUsage::default())));
    }

    #[test]
    fn publishes_dirty_group() {
        let mut operation = publish(false, false);
        operation.first().unwrap();
        let read = IoCall::child(UsageReadOperation::new(false, vec![GROUP]));
        assert_eq!(
            operation.next(page(vec![dirty_group()])),
            Ok(Next::Call(read))
        );
        let answer = operation.next(child::<_, UsagePublishError>(Ok(read_result())));
        assert!(matches!(answer, Ok(Next::Call(IoCall::Write(rows))) if rows.len() == 1));
        let answer = operation.next(IoResult::Done);
        assert!(matches!(answer, Ok(Next::Call(IoCall::Child(_)))));
        let clear = IoCall::child(ClearMarkersOperation::new(vec![dirty_group()]));
        let answer = operation.next(child::<(), ReplicateDocumentsError>(Ok(())));
        assert_eq!(answer, Ok(Next::Call(clear)));
        // A lost conflict leaves the re-dirtied marker for the next run.
        let answer = operation.next(child::<(), _>(Err(IoError::Conflict)));
        assert_eq!(answer, Ok(Next::Done(published())));
    }

    #[test]
    fn full_scans_counters() {
        let mut operation = publish(true, false);
        operation.first().unwrap();
        let counters = PrefixPages::new(
            USAGE_STATS_KEYSPACE,
            GROUP_COUNTER_PREFIX.to_vec(),
            SCAN_PAGE,
        );
        let answer = operation.next(page(Vec::new()));
        assert_eq!(answer, Ok(Next::Call(counters.call())));
        let key = [GROUP_COUNTER_PREFIX, &GROUP.to_bytes()].concat();
        let row = (Key::from(key), Value::from(vec![0]));
        let snapshots =
            PrefixPages::new(NODE_STATS_KEYSPACE, USAGE_GROUP_PREFIX.to_vec(), SCAN_PAGE);
        let answer = operation.next(page(vec![row]));
        assert_eq!(answer, Ok(Next::Call(snapshots.call())));
        let read = IoCall::child(UsageReadOperation::new(true, vec![GROUP]));
        assert_eq!(operation.next(page(Vec::new())), Ok(Next::Call(read)));
    }

    #[test]
    fn failed_replication_retries() {
        let mut operation = replicating(false);
        let failed = child::<(), _>(Err(ReplicateDocumentsError::ConfigMissing));
        let generation = Value::from(Ulid::from_parts(1, 1).to_bytes().to_vec());
        // The observed group marker already carries the retry, so none is written again.
        let markers = retry_markers(&published(), &[dirty_group()], generation);
        let signal = IoCall::child(RetrySignalOperation::new(markers));
        assert_eq!(operation.next(failed), Ok(Next::Call(signal)));
        let answer = operation.next(child::<(), IoError>(Ok(())));
        let message = "node usage snapshot replication failed: realm config document not found";
        assert_eq!(answer, Err(UsagePublishError::Failed(message.to_string())));
    }

    #[test]
    fn refresh_updates_summary() {
        let mut operation = replicating(true);
        let summary = RealmUsageOperation::summary(node(), None, false, vec![GROUP]);
        let answer = operation.next(child::<(), ReplicateDocumentsError>(Ok(())));
        assert_eq!(answer, Ok(Next::Call(IoCall::child(summary))));
        let clear = IoCall::child(ClearMarkersOperation::new(vec![dirty_group()]));
        let answer = operation.next(child::<_, RealmUsageError>(Ok(Vec::<RealmUsage>::new())));
        assert_eq!(answer, Ok(Next::Call(clear)));
    }

    #[test]
    fn reads_shares_counters() {
        let mut read = UsageReadOperation::new(false, vec![GROUP]);
        assert_eq!(read.transaction(), TransactionMode::ReadSnapshot);
        assert!(matches!(read.first(), Ok(Next::Call(IoCall::Page { .. }))));
        let rows = vec![(
            USAGE_STATS_KEYSPACE.to_string(),
            Key::from(usage_group_key(GROUP)),
        )];
        assert_eq!(
            read.next(page(Vec::new())),
            Ok(Next::Call(IoCall::Read(rows)))
        );
        let answer = read.next(IoResult::Values(vec![None]));
        assert_eq!(answer, Ok(Next::Done(read_result())));
    }

    #[test]
    fn snapshots_name_scopes() {
        let scope = PublishedUsage {
            global: true,
            groups: vec![GROUP],
        };
        let read = UsageRead {
            shares: BTreeMap::new(),
            global: UsageCounters::default(),
            groups: vec![UsageCounters::default()],
        };
        let rows = snapshot_rows(node(), &scope, read).unwrap();
        let keys: Vec<_> = rows.iter().map(|(_, key, _)| key.to_vec()).collect();
        let expected = vec![usage_global_key(node()), usage_snapshot_key(GROUP, node())];
        assert_eq!(keys, expected);
    }

    #[test]
    fn clears_unchanged_markers() {
        // Only markers still at their observed generation are deleted, in one transaction.
        let mut operation = ClearMarkersOperation::new(vec![marker(1), marker(2)]);
        assert_eq!(
            operation.transaction(),
            aruna_core::operation::TransactionMode::Write
        );
        let reads = vec![
            (NODE_STATS_KEYSPACE.to_string(), marker(1).0),
            (NODE_STATS_KEYSPACE.to_string(), marker(2).0),
        ];
        assert_eq!(operation.first(), Ok(Next::Call(IoCall::Read(reads))));
        let stored = IoResult::Values(vec![Some(marker(1).1), Some(Value::from(vec![9]))]);
        let deletes = vec![(NODE_STATS_KEYSPACE.to_string(), marker(1).0)];
        assert_eq!(
            operation.next(stored),
            Ok(Next::Call(IoCall::Delete(deletes)))
        );
        assert_eq!(operation.next(IoResult::Done), Ok(Next::Done(())));
    }

    #[test]
    fn retry_marks_schedules() {
        // Scopes a concurrent write marked already keep their marker; then the timer moves up.
        let group = GroupId::from_bytes([3; 16]);
        let published = PublishedUsage {
            global: true,
            groups: vec![group],
        };
        let seen = vec![(Key::from(DIRTY_GLOBAL_KEY.to_vec()), Value::from(vec![1]))];
        let generation = Value::from(vec![2]);
        let markers = retry_markers(&published, &seen, generation.clone());
        let marker = (
            NODE_STATS_KEYSPACE.to_string(),
            Key::from(dirty_group_key(group)),
            generation,
        );
        assert_eq!(markers, vec![marker.clone()]);
        let mut operation = RetrySignalOperation::new(markers);
        let write = Ok(Next::Call(IoCall::Write(vec![marker])));
        assert_eq!(operation.first(), write);
        let timer = Ok(Next::Call(IoCall::Effect(Box::new(
            schedule_snapshot_publish(),
        ))));
        assert_eq!(operation.next(IoResult::Done), timer);
        let refused = Event::Task(TaskEvent::Error {
            key: None,
            message: "full".to_string(),
        });
        let answer = operation.next(IoResult::Event(Box::new(refused)));
        assert_eq!(answer, Ok(Next::Done(())));
    }

    #[test]
    fn redirtied_markers_kept() {
        // Nothing observed, or every marker re-dirtied or gone, ends without a delete.
        let mut empty = ClearMarkersOperation::new(Vec::new());
        assert_eq!(empty.first(), Ok(Next::Done(())));
        let mut moved = ClearMarkersOperation::new(vec![marker(1), marker(2)]);
        moved.first().unwrap();
        let stored = IoResult::Values(vec![Some(Value::from(vec![7])), None]);
        assert_eq!(moved.next(stored), Ok(Next::Done(())));
    }
}
