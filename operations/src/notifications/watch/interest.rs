//! Publishes this node's watch interest digests and refreshes the interest cache from the
//! digests other nodes replicated.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::{Effect, NetEffect};
use aruna_core::keyspaces::WATCH_INTEREST_KEYSPACE;
use aruna_core::operation::{Operation, TransactionMode};
use aruna_core::structs::execution::notification_watch::{
    WATCH_DIRTY_PREFIX, WatchInterestDigest, WatchInterestTable, dirty_interest_realm,
    interest_node_key, interest_pending_key,
};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::types::{Key, KeySpace, Value};
use thiserror::Error;
use tracing::warn;

use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::notifications::protocol::DIRTY_REALM_CAP;
use crate::notifications::watch::check::{
    AuthorizedWatchSubscriptions, WatchCheckError, WatchFilterOperation,
};
use crate::notifications::watch::digest::{
    InterestError, InterestOperation, catchall_digest, digest_over,
};
use crate::notifications::watch::expansion::{RetryDrainError, RetryDrainOperation};
use crate::notifications::watch::listing::{ListWatchesOperation, WatchPage, WatchScope};
use crate::notifications::watch::records::WatchSubscriptionError;
use crate::notifications::watch::table::RebuildTableOperation;
use crate::sync::replicate_documents::{
    ReplicateDocumentsConfig, ReplicateDocumentsError, ReplicateDocumentsOperation,
};

/// Why a publish run stopped; the timer runs it again after the debounce.
#[derive(Debug, Error, PartialEq)]
#[error("{0}")]
pub struct PublishError(pub String);

impl From<IoError> for PublishError {
    fn from(error: IoError) -> Self {
        Self(error.to_string())
    }
}

fn failed(error: impl ToString) -> PublishError {
    PublishError(error.to_string())
}

/// Deletes each observed dirty marker only if its stored generation still matches the one the
/// publish run read. A marker dirtied again meanwhile makes the commit conflict, which keeps
/// every marker for the next run.
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
        let space = || WATCH_INTEREST_KEYSPACE.to_string();
        let reads = self.observed.iter().map(|(key, _)| (space(), key.clone()));
        Ok(Next::Call(IoCall::Read(reads.collect())))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        if self.deleting {
            result.done()?;
            return Ok(Next::Done(()));
        }
        let current = result.values()?;
        let deletes: Vec<(KeySpace, Key)> = std::mem::take(&mut self.observed)
            .into_iter()
            .zip(current)
            .filter(|((_, observed), current)| current.as_ref() == Some(observed))
            .map(|((key, _), _)| (WATCH_INTEREST_KEYSPACE.to_string(), key))
            .collect();
        self.deleting = true;
        Ok(Next::Call(IoCall::Delete(deletes)))
    }

    fn transaction(&self) -> TransactionMode {
        TransactionMode::Write
    }
}

/// The digest of one realm and whether an authorization check failed while building it.
#[derive(Clone, Debug, PartialEq)]
struct RealmDigest {
    realm_id: RealmId,
    config: Option<RealmConfigDocument>,
    digest: Option<WatchInterestDigest>,
    check_failed: bool,
}

#[derive(Clone, Debug, Default, PartialEq)]
enum PublishState {
    #[default]
    Markers,
    Config,
    Drain,
    List,
    Filter,
    Current,
    Write,
    Replicate,
    Pending,
    Retry,
    Clear,
    Again,
    Table,
}

/// One publish run over up to `DIRTY_REALM_CAP` dirty realms: drains each realm's retry queue,
/// rebuilds its digest from the watches held and authorized here, writes and replicates changed
/// digests, then consumes the markers. Its output is whether it published a digest.
#[derive(Clone, Debug, PartialEq)]
pub struct PublishInterestOperation {
    node: Option<NodeId>,
    state: PublishState,
    markers: Vec<(Key, Value)>,
    more: bool,
    realms: VecDeque<RealmId>,
    current: Option<RealmDigest>,
    writes: Vec<(KeySpace, Key, Value)>,
    targets: VecDeque<RealmId>,
    replicated: Option<RealmId>,
    retries: VecDeque<RealmId>,
    published: bool,
}

type PublishTurn = Result<Next<bool>, PublishError>;

impl PublishInterestOperation {
    /// `node` is this node, `None` without a net transport.
    pub fn new(node: Option<NodeId>) -> Self {
        Self {
            node,
            state: PublishState::Markers,
            markers: Vec::new(),
            more: false,
            realms: VecDeque::new(),
            current: None,
            writes: Vec::new(),
            targets: VecDeque::new(),
            replicated: None,
            retries: VecDeque::new(),
            published: false,
        }
    }

    fn node(&self) -> Result<NodeId, PublishError> {
        self.node
            .ok_or_else(|| failed("watch interest publish without a node"))
    }

    fn markers_read(&mut self, rows: Vec<(Key, Value)>, next: Option<Key>) -> PublishTurn {
        self.more = next.is_some() || rows.len() > DIRTY_REALM_CAP;
        self.markers = rows.into_iter().take(DIRTY_REALM_CAP).collect();
        if self.markers.is_empty() {
            return Ok(Next::Done(false));
        }
        let realms: BTreeSet<RealmId> = (self.markers.iter())
            .filter_map(|(key, _)| dirty_interest_realm(key.as_ref()))
            .collect();
        // Malformed markers only are dropped, so they cannot loop forever.
        self.realms = realms.into_iter().collect();
        self.next_realm()
    }

    fn next_realm(&mut self) -> PublishTurn {
        let Some(realm_id) = self.realms.pop_front() else {
            return self.write();
        };
        self.current = Some(RealmDigest {
            realm_id,
            config: None,
            digest: None,
            check_failed: false,
        });
        self.state = PublishState::Config;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn realm(&mut self) -> Result<&mut RealmDigest, PublishError> {
        self.current
            .as_mut()
            .ok_or_else(|| failed(IoError::Unfinished))
    }

    fn configured(&mut self, config: Result<RealmConfigDocument, GetConfigError>) -> PublishTurn {
        let config = config.map_err(|error| {
            failed(format!(
                "failed to read realm config for watch interest: {error}"
            ))
        })?;
        let node = self.node()?;
        let current = self.realm()?;
        let drain = RetryDrainOperation::new(current.realm_id, config.clone(), node);
        current.config = Some(config);
        self.state = PublishState::Drain;
        Ok(Next::Call(IoCall::child(drain)))
    }

    fn list(&mut self) -> PublishTurn {
        let realm_id = self.realm()?.realm_id;
        self.state = PublishState::List;
        let list = ListWatchesOperation::new(WatchScope::Realm(realm_id));
        Ok(Next::Call(IoCall::child(list)))
    }

    /// Filters the realm's watches; an oversized scan publishes a catch-all entry instead.
    fn listed(&mut self, listed: Result<WatchPage, WatchSubscriptionError>) -> PublishTurn {
        let node = self.node()?;
        let subscriptions = match listed {
            Ok(page) => page.subscriptions,
            Err(WatchSubscriptionError::Storage(error))
                if error.contains("subscription scan cap reached") =>
            {
                self.realm()?.digest = Some(catchall_digest(node));
                return self.read_current();
            }
            Err(error) => return Err(failed(error)),
        };
        let current = self.realm()?;
        let config = current
            .config
            .as_ref()
            .ok_or_else(|| failed(IoError::Unfinished))?;
        let filter = WatchFilterOperation::realm(current.realm_id, config, node, subscriptions)
            .map_err(failed)?;
        self.state = PublishState::Filter;
        Ok(Next::Call(IoCall::child(filter)))
    }

    fn filtered(&mut self, filtered: AuthorizedWatchSubscriptions) -> PublishTurn {
        let node = self.node()?;
        let watches = filtered.subscriptions.into_iter();
        let entries = watches.map(|watch| (watch.path_prefix, watch.event_mask));
        let digest = WatchInterestDigest::from_subscriptions(node, entries);
        let current = self.realm()?;
        match digest_over(&digest).map_err(failed)? {
            true => current.digest = Some(catchall_digest(node)),
            false => {
                current.digest = Some(digest);
                current.check_failed = filtered.check_failed;
            }
        }
        self.read_current()
    }

    fn read_current(&mut self) -> PublishTurn {
        let node = self.node()?;
        let realm_id = self.realm()?.realm_id;
        let space = || WATCH_INTEREST_KEYSPACE.to_string();
        let reads = vec![
            (space(), Key::from(interest_node_key(realm_id, node))),
            (space(), Key::from(interest_pending_key(realm_id))),
        ];
        self.state = PublishState::Current;
        Ok(Next::Call(IoCall::Read(reads)))
    }

    /// Queues the realm's changed digest with its pending marker, and the realm for replication
    /// while a digest or an earlier pending marker waits.
    fn compared(&mut self, values: Vec<Option<Value>>) -> PublishTurn {
        let node = self.node()?;
        let current = self
            .current
            .take()
            .ok_or_else(|| failed(IoError::Unfinished))?;
        let realm_id = current.realm_id;
        if current.check_failed {
            self.retries.push_back(realm_id);
        }
        let [stored, pending] = <[Option<Value>; 2]>::try_from(values)
            .map_err(|_| failed("watch interest digest read count mismatch"))?;
        let digest = current.digest.ok_or_else(|| failed(IoError::Unfinished))?;
        let digest = Value::from(digest.to_bytes().map_err(failed)?);
        let changed = stored.as_ref() != Some(&digest);
        if changed {
            let space = || WATCH_INTEREST_KEYSPACE.to_string();
            let digest_key = Key::from(interest_node_key(realm_id, node));
            let pending_key = Key::from(interest_pending_key(realm_id));
            self.writes.push((space(), digest_key, digest));
            self.writes
                .push((space(), pending_key, Value::from(Vec::new())));
        }
        if changed || pending.is_some() {
            self.targets.push_back(realm_id);
        }
        self.next_realm()
    }

    /// Persists the changed digests with their pending markers in one batch.
    fn write(&mut self) -> PublishTurn {
        self.published = !self.targets.is_empty();
        self.state = PublishState::Write;
        Ok(Next::Call(IoCall::Write(std::mem::take(&mut self.writes))))
    }

    /// Replicates the next realm's digest on its realm topic.
    fn replicate(&mut self) -> PublishTurn {
        let Some(realm_id) = self.targets.pop_front() else {
            return self.retry();
        };
        let node_id = self.node()?;
        self.replicated = Some(realm_id);
        self.state = PublishState::Replicate;
        let replicate = ReplicateDocumentsOperation::new(ReplicateDocumentsConfig {
            realm_id,
            local_node_id: node_id,
            excluded_peers: Vec::new(),
            documents: vec![DocumentTarget::WatchInterest { realm_id, node_id }],
            // Watch-interest digests ride one shared realm topic; steady-state digest
            // publishers must not fork genesis.
            allow_genesis: false,
        });
        Ok(Next::Call(IoCall::child(replicate)))
    }

    /// Keeps a retry generation for a realm whose permission lookup failed; its fail-closed
    /// digest still retracts stale interest at once.
    fn retry(&mut self) -> PublishTurn {
        let Some(realm_id) = self.retries.pop_front() else {
            self.state = PublishState::Clear;
            let clear = ClearMarkersOperation::new(std::mem::take(&mut self.markers));
            return Ok(Next::Call(IoCall::child(clear)));
        };
        self.state = PublishState::Retry;
        Ok(Next::Call(IoCall::child(InterestOperation::mark(realm_id))))
    }

    /// Asks for another run while markers wait, then refreshes the cache after a publish.
    fn again(&mut self) -> PublishTurn {
        if self.more {
            self.more = false;
            self.state = PublishState::Again;
            return Ok(Next::Call(IoCall::child(InterestOperation::publish())));
        }
        if !self.published {
            return Ok(Next::Done(false));
        }
        // The local write bypasses the reconcile path that refreshes the cache for remotes.
        self.state = PublishState::Table;
        Ok(Next::Call(IoCall::child(RebuildTableOperation::default())))
    }
}

impl Operation for PublishInterestOperation {
    type Output = bool;
    type Error = PublishError;

    fn first(&mut self) -> PublishTurn {
        if self.node.is_none() {
            warn!("Cannot publish watch interest without net handle");
            return Ok(Next::Done(false));
        }
        Ok(Next::Call(IoCall::Page {
            space: WATCH_INTEREST_KEYSPACE.to_string(),
            prefix: Key::from(WATCH_DIRTY_PREFIX.to_vec()),
            after: None,
            limit: DIRTY_REALM_CAP.saturating_add(1),
        }))
    }

    fn next(&mut self, result: IoResult) -> PublishTurn {
        match std::mem::take(&mut self.state) {
            PublishState::Markers => {
                let (rows, next) = result.page()?;
                self.markers_read(rows, next)
            }
            PublishState::Config => {
                self.configured(result.child::<RealmConfigDocument, GetConfigError>()?)
            }
            PublishState::Drain => {
                if let Err(error) = result.child::<(), RetryDrainError>()? {
                    let realm_id = self.realm()?.realm_id;
                    warn!(%realm_id, %error, "Watch event retry remains pending");
                }
                self.list()
            }
            PublishState::List => self.listed(result.child::<WatchPage, WatchSubscriptionError>()?),
            PublishState::Filter => {
                let filtered = result.child::<AuthorizedWatchSubscriptions, WatchCheckError>()?;
                self.filtered(filtered.map_err(failed)?)
            }
            PublishState::Current => self.compared(result.values()?),
            PublishState::Write => {
                result.done()?;
                self.replicate()
            }
            PublishState::Replicate => {
                let replicated = result.child::<(), ReplicateDocumentsError>()?;
                replicated.map_err(|error| {
                    failed(format!("watch interest replication failed: {error}"))
                })?;
                let realm_id = self
                    .replicated
                    .take()
                    .ok_or_else(|| failed(IoError::Unfinished))?;
                self.state = PublishState::Pending;
                let pending = (
                    WATCH_INTEREST_KEYSPACE.to_string(),
                    interest_pending_key(realm_id),
                );
                Ok(Next::Call(IoCall::Delete(vec![(
                    pending.0,
                    Key::from(pending.1),
                )])))
            }
            PublishState::Pending => {
                result.done()?;
                self.replicate()
            }
            PublishState::Retry => {
                result.child::<bool, InterestError>()?.map_err(failed)?;
                self.retry()
            }
            PublishState::Clear => {
                // A conflict means a marker was dirtied again; it stays for the next run.
                match result.child::<(), IoError>()? {
                    Ok(()) | Err(IoError::Conflict) => self.again(),
                    Err(error) => Err(failed(error)),
                }
            }
            PublishState::Again => {
                let _ = result.child::<bool, InterestError>()?;
                self.again()
            }
            PublishState::Table => {
                let table = result.child::<WatchInterestTable, InterestError>()?;
                let table = table.unwrap_or_else(|error| {
                    warn!(%error, "Failed to scan watch interest digests");
                    WatchInterestTable::default()
                });
                let replace = NetEffect::ReplaceInterest(Box::new(table));
                Ok(Next::Then {
                    output: true,
                    calls: vec![IoCall::from(Effect::Net(replace))],
                })
            }
        }
    }
}

/// What refreshing the interest cache waits for.
#[derive(Debug, Default, PartialEq)]
enum RefreshState {
    #[default]
    Mark,
    Table,
}

/// After document reconciliation: marks realms whose watches, authorization or config changed,
/// and rebuilds the cached interest of realms whose digests or membership changed. A failed
/// rebuild clears that realm's interest, since membership is a security boundary.
#[derive(Debug, PartialEq)]
pub struct RefreshInterestOperation {
    state: RefreshState,
    marks: VecDeque<RealmId>,
    realms: VecDeque<RealmId>,
    current: Option<RealmId>,
    updates: Vec<IoCall>,
}

impl RefreshInterestOperation {
    /// `local_realm` is this node's realm, which a group authorization change belongs to.
    pub fn new(targets: &[DocumentTarget], local_realm: RealmId) -> Self {
        let mut realms: BTreeSet<RealmId> = BTreeSet::new();
        let mut dirty: BTreeSet<RealmId> = BTreeSet::new();
        for target in targets {
            match target {
                DocumentTarget::WatchInterest { realm_id, .. } => {
                    realms.insert(*realm_id);
                }
                DocumentTarget::WatchSubscription { owner, .. } => {
                    dirty.insert(owner.realm_id);
                }
                DocumentTarget::GroupAuthorization { .. } => {
                    dirty.insert(local_realm);
                }
                DocumentTarget::RealmAuthorization { realm_id } => {
                    dirty.insert(*realm_id);
                }
                DocumentTarget::RealmConfig { realm_id } => {
                    realms.insert(*realm_id);
                    dirty.insert(*realm_id);
                }
                _ => {}
            }
        }
        Self {
            state: RefreshState::Mark,
            marks: dirty.into_iter().collect(),
            realms: realms.into_iter().collect(),
            current: None,
            updates: Vec::new(),
        }
    }

    fn next_step(&mut self) -> Result<Next<()>, IoError> {
        if let Some(realm_id) = self.marks.pop_front() {
            self.current = Some(realm_id);
            self.state = RefreshState::Mark;
            return Ok(Next::Call(IoCall::child(InterestOperation::mark(realm_id))));
        }
        if let Some(realm_id) = self.realms.pop_front() {
            self.current = Some(realm_id);
            self.state = RefreshState::Table;
            return Ok(Next::Call(IoCall::child(RebuildTableOperation::realm(
                realm_id,
            ))));
        }
        let calls = std::mem::take(&mut self.updates);
        if calls.is_empty() {
            return Ok(Next::Done(()));
        }
        Ok(Next::Then { output: (), calls })
    }
}

impl Operation for RefreshInterestOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> Result<Next<()>, IoError> {
        self.next_step()
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, IoError> {
        let realm_id = self.current.take().ok_or(IoError::Unfinished)?;
        match self.state {
            RefreshState::Mark => {
                if let Err(error) = result.child::<bool, InterestError>()? {
                    warn!(%realm_id, %error, "Failed to schedule watch interest rebuild after document reconciliation");
                }
            }
            RefreshState::Table => {
                let nodes = match result.child::<WatchInterestTable, InterestError>()? {
                    Ok(mut table) => table.take_realm(realm_id),
                    Err(error) => {
                        warn!(%realm_id, %error, "Cleared watch interest after failed realm refresh");
                        Default::default()
                    }
                };
                let update = NetEffect::UpdateRealmInterest { realm_id, nodes };
                self.updates.push(IoCall::from(Effect::Net(update)));
            }
        }
        self.next_step()
    }
}

#[cfg(test)]
#[path = "interest_tests.rs"]
mod call_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use crate::notifications::watch::digest::dirty_marker_write;
    use crate::notifications::watch::listing::{ListWatchesOperation, WatchScope};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::structs::execution::notification_watch::{
        INTEREST_ENTRY_CAP, interest_dirty_key,
    };
    use aruna_core::structs::execution::notification_watch::{
        WatchEventKind, WatchEventMask, WatchInterestEntry,
    };
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmId, RealmNodeKind,
    };
    use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
    use aruna_storage::FjallStorage;
    use tempfile::{TempDir, tempdir};
    use ulid::Ulid;

    use crate::tests::notifications::{create_local_watch, delete_local_watch};

    async fn publish_watch_interest(ctx: &DriverContext, node_id: NodeId) -> Result<bool, String> {
        let publish = PublishInterestOperation::new(Some(node_id));
        drive(publish, ctx).await.map_err(|error| error.to_string())
    }

    async fn refresh_target_interest(ctx: &DriverContext, targets: &[DocumentTarget]) {
        let Some(net_handle) = ctx.net_handle.as_ref() else {
            return;
        };
        let refresh = RefreshInterestOperation::new(targets, *net_handle.realm_id());
        drive(refresh, ctx)
            .await
            .expect("the interest refresh runs");
    }

    async fn clear_consumed_markers(
        ctx: &DriverContext,
        observed: Vec<(Key, Value)>,
    ) -> Result<(), IoError> {
        match drive(ClearMarkersOperation::new(observed), ctx).await {
            Ok(()) | Err(IoError::Conflict) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// The digest the publish run builds for `realm_id` from the watches held and authorized here.
    async fn build_realm_digest(
        ctx: &DriverContext,
        node_id: NodeId,
        realm_id: RealmId,
        config: &RealmConfigDocument,
    ) -> Result<(WatchInterestDigest, bool), String> {
        let list = ListWatchesOperation::new(WatchScope::Realm(realm_id));
        let page = drive(list, ctx).await.map_err(|error| error.to_string())?;
        let filter = WatchFilterOperation::realm(realm_id, config, node_id, page.subscriptions)
            .map_err(|error| error.to_string())?;
        let filtered = drive(filter, ctx)
            .await
            .map_err(|error| error.to_string())?;
        let watches = filtered.subscriptions.into_iter();
        let entries = watches.map(|watch| (watch.path_prefix, watch.event_mask));
        let digest = WatchInterestDigest::from_subscriptions(node_id, entries);
        Ok((digest, filtered.check_failed))
    }

    fn test_ctx(root: &str) -> DriverContext {
        DriverContext {
            storage_handle: FjallStorage::open(root).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        }
    }

    async fn ctx_with_net(
        realm_id: RealmId,
        secret: [u8; 32],
    ) -> (TempDir, DriverContext, NetHandle) {
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let net = NetHandle::new(
            NetConfig {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                secret_key: Some(iroh::SecretKey::from_bytes(&secret)),
                realm_id,
                discovery_method: DiscoveryMethod::None,
                relay_method: RelayMethod::None,
                ..NetConfig::default()
            },
            storage.clone(),
        )
        .await
        .unwrap();
        let ctx = DriverContext {
            storage_handle: storage,
            net_handle: Some(net.clone()),
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        (dir, ctx, net)
    }

    async fn write_digest(ctx: &DriverContext, realm_id: RealmId, digest: &WatchInterestDigest) {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                key: Key::from(interest_node_key(realm_id, digest.node_id)),
                value: Value::from(digest.to_bytes().unwrap()),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected digest write event: {other:?}"),
        }
    }

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn user(realm: u8, seed: u8) -> UserId {
        UserId::new(Ulid::from_bytes([seed; 16]), RealmId([realm; 32]))
    }

    fn mask() -> WatchEventMask {
        WatchEventMask::from_kinds([WatchEventKind::MetadataCreated])
    }

    fn metadata_prefix(group_id: Ulid, suffix: &str) -> String {
        format!("meta/{group_id}/{suffix}")
    }

    async fn install_authorization(
        ctx: &DriverContext,
        realm_id: RealmId,
        node_id: NodeId,
        group_id: Ulid,
        owner: UserId,
        readers: &[UserId],
    ) {
        let actor = Actor {
            node_id,
            user_id: owner,
            realm_id,
        };
        let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let mut group_auth =
            GroupAuthorizationDocument::default_group_doc(owner, realm_id, group_id);
        group_auth
            .roles
            .values_mut()
            .find(|role| role.name == "viewer")
            .unwrap()
            .assigned_users
            .extend(readers.iter().copied());
        // Policy loading resolves the group record before group policies apply.
        let group = Group {
            display_name: "watch".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner,
        };
        for (key_space, key, value) in [
            (
                AUTH_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                realm_auth.to_bytes(&actor).unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group_auth.to_bytes(&actor).unwrap(),
            ),
            (
                GROUP_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
        ] {
            assert!(matches!(
                ctx.storage_handle
                    .send_storage_effect(StorageEffect::Write {
                        key_space: key_space.to_string(),
                        key: key.into(),
                        value: value.into(),
                        txn_id: None,
                    })
                    .await,
                Event::Storage(StorageEvent::WriteResult { .. })
            ));
        }
    }

    async fn install_realm_config(
        ctx: &DriverContext,
        realm_id: RealmId,
        nodes: &[NodeId],
    ) -> RealmConfigDocument {
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        for node_id in nodes {
            config.ensure_node(*node_id, RealmNodeKind::Server);
        }
        let actor = Actor {
            node_id: nodes[0],
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: aruna_core::keyspaces::REALM_CONFIG_KEYSPACE.to_string(),
                key: realm_id.as_bytes().to_vec().into(),
                value: config.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => config,
            other => panic!("unexpected realm config write result: {other:?}"),
        }
    }

    fn user_for_holder(realm_id: RealmId, config: &RealmConfigDocument, holder: NodeId) -> UserId {
        for seed in 1..50_000u128 {
            let candidate = UserId::new(Ulid::from_bytes(seed.to_be_bytes()), realm_id);
            if crate::notifications::placement::resolve_inbox_holder(&candidate, config).unwrap()
                == Some(holder)
            {
                return candidate;
            }
        }
        panic!("no user resolved to holder {holder}");
    }

    async fn read_marker(ctx: &DriverContext, realm_id: RealmId) -> Option<Value> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                key: Key::from(interest_dirty_key(realm_id)),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("unexpected marker read event: {other:?}"),
        }
    }

    async fn dirty_count(ctx: &DriverContext) -> usize {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Iter {
                key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                prefix: Some(Key::from(WATCH_DIRTY_PREFIX.to_vec())),
                start: None,
                limit: 1024,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::IterResult { values, .. }) => values.len(),
            other => panic!("unexpected marker scan event: {other:?}"),
        }
    }

    async fn read_pending(ctx: &DriverContext, realm_id: RealmId) -> Option<Value> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                key: Key::from(interest_pending_key(realm_id)),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => value,
            other => panic!("unexpected pending marker read event: {other:?}"),
        }
    }

    async fn read_digest(
        ctx: &DriverContext,
        realm_id: RealmId,
        node_id: NodeId,
    ) -> Option<WatchInterestDigest> {
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Read {
                key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                key: Key::from(interest_node_key(realm_id, node_id)),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult {
                value: Some(bytes), ..
            }) => Some(WatchInterestDigest::from_bytes(bytes.as_ref()).unwrap()),
            Event::Storage(StorageEvent::ReadResult { value: None, .. }) => None,
            other => panic!("unexpected digest read event: {other:?}"),
        }
    }

    #[test]
    fn catchall_covers_events() {
        let digest = catchall_digest(node(1));
        let entry = &digest.entries[0];
        assert!(entry.path_prefix.is_empty());
        assert_eq!(entry.event_mask.bits(), 15);
    }

    #[test]
    fn digest_caps_work() {
        let digest = WatchInterestDigest {
            node_id: node(1),
            entries: (0..=INTEREST_ENTRY_CAP)
                .map(|index| WatchInterestEntry {
                    path_prefix: format!("p/{index}"),
                    event_mask: mask(),
                })
                .collect(),
        };
        assert!(digest_over(&digest).expect("digest serializes"));
    }

    #[tokio::test]
    async fn dirty_page_caps() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        for index in 0..=DIRTY_REALM_CAP {
            assert!(matches!(
                ctx.storage_handle
                    .send_storage_effect(StorageEffect::Write {
                        key_space: WATCH_INTEREST_KEYSPACE.to_string(),
                        key: format!("dirty/bad/{index}").into_bytes().into(),
                        value: vec![index as u8].into(),
                        txn_id: None,
                    })
                    .await,
                Event::Storage(StorageEvent::WriteResult { .. })
            ));
        }

        assert!(!publish_watch_interest(&ctx, node(1)).await.unwrap());
        assert_eq!(dirty_count(&ctx).await, 1);
        assert!(!publish_watch_interest(&ctx, node(1)).await.unwrap());
        assert_eq!(dirty_count(&ctx).await, 0);
    }

    #[tokio::test]
    async fn create_marks_dirty() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let owner = user(1, 2);
        let group_id = Ulid::generate();

        create_local_watch(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "a"),
            mask(),
            1,
        )
        .await
        .expect("create succeeds");

        assert!(read_marker(&ctx, owner.realm_id).await.is_some());
    }

    #[tokio::test]
    async fn publish_clears_markers() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(5);
        let owner = user(1, 2);
        install_realm_config(&ctx, owner.realm_id, &[node_id]).await;
        let group_id = Ulid::generate();
        install_authorization(&ctx, owner.realm_id, node_id, group_id, owner, &[]).await;

        create_local_watch(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "a"),
            mask(),
            1,
        )
        .await
        .expect("create a");
        create_local_watch(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "b"),
            mask(),
            2,
        )
        .await
        .expect("create b");

        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("publish")
        );

        let digest = read_digest(&ctx, owner.realm_id, node_id)
            .await
            .expect("digest written");
        assert_eq!(digest.node_id, node_id);
        let prefixes: Vec<&str> = digest
            .entries
            .iter()
            .map(|entry| entry.path_prefix.as_str())
            .collect();
        assert_eq!(
            prefixes,
            vec![
                metadata_prefix(group_id, "a"),
                metadata_prefix(group_id, "b")
            ]
        );

        // Markers consumed once the digest is durable.
        assert!(read_marker(&ctx, owner.realm_id).await.is_none());
        // Nothing left to publish.
        assert!(
            !publish_watch_interest(&ctx, node_id)
                .await
                .expect("republish")
        );
    }

    // An acknowledged unchanged digest consumes a fresh dirty marker without
    // another replication handoff.
    #[tokio::test]
    async fn unchanged_skips_publish() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let realm_id = RealmId([7u8; 32]);
        let node_id = node(5);
        let owner = user(7, 2);
        let group_id = Ulid::generate();
        install_realm_config(&ctx, realm_id, &[node_id]).await;
        install_authorization(&ctx, realm_id, node_id, group_id, owner, &[]).await;
        create_local_watch(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "a"),
            mask(),
            1,
        )
        .await
        .expect("create");

        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("publish")
        );
        let before = read_digest(&ctx, realm_id, node_id)
            .await
            .expect("digest")
            .to_bytes()
            .unwrap();
        assert!(read_pending(&ctx, realm_id).await.is_none());

        drive(InterestOperation::mark(realm_id), &ctx)
            .await
            .expect("mark dirty");
        assert!(
            !publish_watch_interest(&ctx, node_id)
                .await
                .expect("republish")
        );

        let after = read_digest(&ctx, realm_id, node_id)
            .await
            .expect("digest")
            .to_bytes()
            .unwrap();
        assert_eq!(after, before);
        assert!(read_marker(&ctx, realm_id).await.is_none());
        assert!(read_pending(&ctx, realm_id).await.is_none());
    }

    // Persistent authorization unavailability retains retries while identical
    // empty digests stop producing replicated events.
    #[tokio::test]
    async fn retry_stays_dirty() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let realm_id = RealmId([8u8; 32]);
        let node_id = node(6);
        let owner = user(8, 2);
        let group_id = Ulid::generate();
        install_realm_config(&ctx, realm_id, &[node_id]).await;
        create_local_watch(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "a"),
            mask(),
            1,
        )
        .await
        .expect("create");

        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("publish")
        );
        assert!(read_marker(&ctx, realm_id).await.is_some());
        let digest = read_digest(&ctx, realm_id, node_id)
            .await
            .expect("empty digest");
        assert!(digest.entries.is_empty());
        let before = digest.to_bytes().unwrap();

        assert!(
            !publish_watch_interest(&ctx, node_id)
                .await
                .expect("republish")
        );
        let after = read_digest(&ctx, realm_id, node_id)
            .await
            .expect("empty digest")
            .to_bytes()
            .unwrap();
        assert_eq!(after, before);
        assert!(read_marker(&ctx, realm_id).await.is_some());
    }

    #[tokio::test]
    async fn delete_publishes_empty() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let node_id = node(5);
        let owner = user(1, 2);
        install_realm_config(&ctx, owner.realm_id, &[node_id]).await;
        let group_id = Ulid::generate();
        install_authorization(&ctx, owner.realm_id, node_id, group_id, owner, &[]).await;

        let created = create_local_watch(
            &ctx.storage_handle,
            owner,
            metadata_prefix(group_id, "a"),
            mask(),
            1,
        )
        .await
        .expect("create");
        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("publish")
        );

        delete_local_watch(&ctx.storage_handle, owner, created.watch_id)
            .await
            .expect("delete");
        assert!(read_marker(&ctx, owner.realm_id).await.is_some());

        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("republish")
        );
        let digest = read_digest(&ctx, owner.realm_id, node_id)
            .await
            .expect("empty digest is still written");
        assert!(digest.entries.is_empty());
    }

    #[tokio::test]
    async fn digest_omits_moved() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let realm_id = RealmId([2u8; 32]);
        let local_node_id = node(5);
        let remote_node_id = node(6);
        let config = install_realm_config(&ctx, realm_id, &[local_node_id, remote_node_id]).await;
        let local_owner = user_for_holder(realm_id, &config, local_node_id);
        let stale_owner = user_for_holder(realm_id, &config, remote_node_id);
        let group_id = Ulid::generate();
        install_authorization(
            &ctx,
            realm_id,
            local_node_id,
            group_id,
            local_owner,
            &[stale_owner],
        )
        .await;

        create_local_watch(
            &ctx.storage_handle,
            local_owner,
            metadata_prefix(group_id, "local"),
            mask(),
            1,
        )
        .await
        .unwrap();
        create_local_watch(
            &ctx.storage_handle,
            stale_owner,
            metadata_prefix(group_id, "stale"),
            mask(),
            2,
        )
        .await
        .unwrap();

        let digest = build_realm_digest(&ctx, local_node_id, realm_id, &config)
            .await
            .unwrap()
            .0;

        assert_eq!(digest.entries.len(), 1);
        assert_eq!(
            digest.entries[0].path_prefix,
            metadata_prefix(group_id, "local")
        );
    }

    #[tokio::test]
    async fn marker_respects_generation() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let realm_id = RealmId([1u8; 32]);

        // Two distinct generations for the same realm marker: only the observed
        // generation should be cleared.
        let (space, key, first) = dirty_marker_write(realm_id);
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: space,
                key: key.clone(),
                value: first.clone(),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write: {other:?}"),
        }

        // A concurrent CRUD bumped the generation after we observed `first`.
        let observed = vec![(key.clone(), first)];
        let (space2, key2, second) = dirty_marker_write(realm_id);
        assert_eq!(key2, key);
        match ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: space2,
                key: key2,
                value: second,
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected write: {other:?}"),
        }

        clear_consumed_markers(&ctx, observed)
            .await
            .expect("clear runs");

        // The re-dirtied marker survived because its generation moved on.
        assert!(read_marker(&ctx, realm_id).await.is_some());
    }

    #[tokio::test]
    async fn rebuild_reads_digests() {
        let temp = tempdir().unwrap();
        let ctx = test_ctx(temp.path().to_str().unwrap());
        let realm_id = RealmId([3u8; 32]);
        let holder = node(11);
        install_realm_config(&ctx, realm_id, &[holder]).await;

        write_digest(
            &ctx,
            realm_id,
            &WatchInterestDigest {
                node_id: holder,
                entries: vec![WatchInterestEntry {
                    path_prefix: "bucket/".to_string(),
                    event_mask: mask(),
                }],
            },
        )
        .await;
        // A positive digest from a node outside current membership must not add
        // a matchable holder.
        write_digest(
            &ctx,
            realm_id,
            &WatchInterestDigest {
                node_id: node(12),
                entries: vec![WatchInterestEntry {
                    path_prefix: "bucket/".to_string(),
                    event_mask: mask(),
                }],
            },
        )
        .await;

        let table = drive(RebuildTableOperation::default(), &ctx)
            .await
            .expect("table rebuilds");
        assert_eq!(
            table.matching_nodes(realm_id, "bucket/object", WatchEventKind::MetadataCreated),
            vec![holder]
        );
        assert_eq!(table.nodes(realm_id).map(|nodes| nodes.len()), Some(1));
    }

    // A consumed empty digest must be re-dirtied by group authorization
    // reconciliation so a later READ grant restores remote interest.
    #[tokio::test]
    async fn regrant_restores_digest() {
        let realm_id = RealmId([6u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm_id, [72u8; 32]).await;
        let node_id = net.node_id();
        install_realm_config(&ctx, realm_id, &[node_id]).await;
        let group_id = Ulid::generate();
        let auth_owner = user(6, 3);
        let watch_owner = user(6, 2);
        let prefix = metadata_prefix(group_id, "a");
        install_authorization(&ctx, realm_id, node_id, group_id, auth_owner, &[]).await;
        create_local_watch(&ctx.storage_handle, watch_owner, prefix.clone(), mask(), 1)
            .await
            .expect("create");

        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("publish")
        );
        assert!(
            read_digest(&ctx, realm_id, node_id)
                .await
                .expect("empty digest")
                .entries
                .is_empty()
        );
        assert!(read_marker(&ctx, realm_id).await.is_none());

        install_authorization(
            &ctx,
            realm_id,
            node_id,
            group_id,
            auth_owner,
            &[watch_owner],
        )
        .await;
        refresh_target_interest(&ctx, &[DocumentTarget::GroupAuthorization { group_id }]).await;

        assert!(read_marker(&ctx, realm_id).await.is_some());
        assert!(
            publish_watch_interest(&ctx, node_id)
                .await
                .expect("republish")
        );
        let digest = read_digest(&ctx, realm_id, node_id)
            .await
            .expect("restored digest");
        assert_eq!(digest.entries.len(), 1);
        assert_eq!(digest.entries[0].path_prefix, prefix);
    }

    #[tokio::test]
    async fn refresh_updates_network() {
        let realm_id = RealmId([4u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm_id, [70u8; 32]).await;
        let holder = node(13);
        install_realm_config(&ctx, realm_id, &[holder]).await;

        assert!(net.watch_interest_snapshot().is_empty());

        write_digest(
            &ctx,
            realm_id,
            &WatchInterestDigest {
                node_id: holder,
                entries: vec![WatchInterestEntry {
                    path_prefix: "data/".to_string(),
                    event_mask: mask(),
                }],
            },
        )
        .await;

        let target = DocumentTarget::WatchInterest {
            realm_id,
            node_id: holder,
        };
        refresh_target_interest(&ctx, &[target]).await;

        let snapshot = net.watch_interest_snapshot();
        assert_eq!(
            snapshot.matching_nodes(realm_id, "data/x", WatchEventKind::MetadataCreated),
            vec![holder]
        );
    }

    #[tokio::test]
    async fn refresh_retracts_holder() {
        let realm_id = RealmId([5u8; 32]);
        let (_dir, ctx, net) = ctx_with_net(realm_id, [71u8; 32]).await;
        let retained = node(14);
        let removed = node(15);
        install_realm_config(&ctx, realm_id, &[retained, removed]).await;
        for holder in [retained, removed] {
            write_digest(
                &ctx,
                realm_id,
                &WatchInterestDigest {
                    node_id: holder,
                    entries: vec![WatchInterestEntry {
                        path_prefix: "data/".to_string(),
                        event_mask: mask(),
                    }],
                },
            )
            .await;
        }
        refresh_target_interest(
            &ctx,
            &[DocumentTarget::WatchInterest {
                realm_id,
                node_id: retained,
            }],
        )
        .await;
        assert_eq!(
            net.watch_interest_snapshot().matching_nodes(
                realm_id,
                "data/x",
                WatchEventKind::MetadataCreated,
            ),
            vec![retained, removed]
        );

        install_realm_config(&ctx, realm_id, &[retained]).await;
        refresh_target_interest(&ctx, &[DocumentTarget::RealmConfig { realm_id }]).await;

        assert_eq!(
            net.watch_interest_snapshot().matching_nodes(
                realm_id,
                "data/x",
                WatchEventKind::MetadataCreated,
            ),
            vec![retained]
        );
        assert!(read_digest(&ctx, realm_id, removed).await.is_some());
    }
}
