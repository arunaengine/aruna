//! Reads the metadata audit trail for a group, one page at a time.
//! Audit rows are node-local, so pages from each eligible realm node are merged by storage key.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use aruna_core::audit::{
    AUDIT_KEY_BYTES, AuditPageBatch, AuditPageEntry, AuditPageRequest, AuditPageResponse,
    MAX_AUDIT_PEERS, MAX_AUDIT_RECORDS, validate_page,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::cursor;
use aruna_core::effects::{
    AuditPageEffect, Effect, EffectGroup, GroupForm, IterStart, NetEffect, StorageEffect,
};
use aruna_core::events::{Event, GroupEvent, GroupResult, NetEvent, StorageEvent};
use aruna_core::id::NodeId;
use aruna_core::keyspaces::METADATA_AUDIT_KEYSPACE;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::metadata_registry::MetadataAuditRecord;
use aruna_core::types::{GroupId, Key, Value};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use ulid::Ulid;

use super::call_results::page_rows;
use aruna_core::placement::selector::select_top_peers;

pub const AUDIT_PAGE_LIMIT: usize = MAX_AUDIT_RECORDS;
pub const AUDIT_PAGE_SIZE: usize = 50;
const AUDIT_CURSOR_VERSION: u8 = 1;
const AUDIT_CURSOR_BYTES: usize = 256;
const AUDIT_CURSOR_CHARS: usize = 384;
const AUDIT_INBOUND_LIMIT: usize = 16;
const AUDIT_OUTBOUND_LIMIT: usize = 16;
pub const AUDIT_DEADLINE_SECS: u64 = 30;
pub(super) static AUDIT_INBOUND_ADMISSION: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(AUDIT_INBOUND_LIMIT)));
pub(super) static AUDIT_OUTBOUND_ADMISSION: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(AUDIT_OUTBOUND_LIMIT)));

#[derive(Debug, Clone)]
pub struct ListAuditRequest {
    pub group_id: GroupId,
    pub document_id: Option<Ulid>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
    pub local_authorized: bool,
}

/// A partial result has no cursor because records may be missing or conflicting.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditAggregate {
    pub records: Vec<MetadataAuditRecord>,
    pub next_cursor: Option<String>,
    pub partial: bool,
    pub missing_nodes: Vec<NodeId>,
    pub missing_overflow: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ListAuditError {
    #[error("audit read unavailable")]
    Unavailable,
    #[error("audit authorization requires a bearer token")]
    Unauthorized,
    #[error("invalid audit cursor")]
    InvalidCursor,
    #[error("audit read failed: {0}")]
    Storage(String),
}

impl From<IoError> for ListAuditError {
    fn from(error: IoError) -> Self {
        Self::Storage(error.to_string())
    }
}

#[derive(Serialize, Deserialize)]
struct AuditCursor {
    version: u8,
    realm_id: RealmId,
    config_digest: [u8; 32],
    group_id: GroupId,
    document_id: Option<Ulid>,
    key: Vec<u8>,
}

fn encode_cursor(
    realm_id: RealmId,
    config_digest: [u8; 32],
    group_id: GroupId,
    document_id: Option<Ulid>,
    key: Vec<u8>,
) -> String {
    cursor::encode_value(&AuditCursor {
        version: AUDIT_CURSOR_VERSION,
        realm_id,
        config_digest,
        group_id,
        document_id,
        key,
    })
    .unwrap_or_default()
}

pub(super) fn decode_cursor(
    cursor: &str,
    realm_id: RealmId,
    config_digest: [u8; 32],
    group_id: GroupId,
    document_id: Option<Ulid>,
) -> Result<Vec<u8>, ListAuditError> {
    if cursor.len() > AUDIT_CURSOR_CHARS {
        return Err(ListAuditError::InvalidCursor);
    }
    let bytes = cursor::decode(cursor).map_err(|_| ListAuditError::InvalidCursor)?;
    if bytes.len() > AUDIT_CURSOR_BYTES {
        return Err(ListAuditError::InvalidCursor);
    }
    let (cursor, rest) = postcard::take_from_bytes::<AuditCursor>(&bytes)
        .map_err(|_| ListAuditError::InvalidCursor)?;
    if cursor.version != AUDIT_CURSOR_VERSION
        || !rest.is_empty()
        || cursor.realm_id != realm_id
        || cursor.config_digest != config_digest
        || cursor.group_id != group_id
        || cursor.document_id != document_id
        || cursor.key.len() != AUDIT_KEY_BYTES
    {
        return Err(ListAuditError::InvalidCursor);
    }
    if !cursor.key.starts_with(&audit_prefix(group_id, document_id)) {
        return Err(ListAuditError::InvalidCursor);
    }
    Ok(cursor.key)
}

/// The audit rows of a group, or of one document in it.
fn audit_prefix(group_id: GroupId, document_id: Option<Ulid>) -> Vec<u8> {
    let mut prefix = group_id.to_bytes().to_vec();
    if let Some(document_id) = document_id {
        prefix.extend_from_slice(&document_id.to_bytes());
    }
    prefix
}

fn audit_iter_effect(
    group_id: GroupId,
    document_id: Option<Ulid>,
    start_after: &Option<Vec<u8>>,
    limit: usize,
) -> Effect {
    Effect::Storage(StorageEffect::Iter {
        key_space: METADATA_AUDIT_KEYSPACE.to_string(),
        prefix: Some(audit_prefix(group_id, document_id).into()),
        start: start_after.clone().map(|key| IterStart::After(key.into())),
        limit,
        txn_id: None,
    })
}

fn parse_audit_page(
    values: Vec<(Key, Value)>,
    next_start_after: Option<Key>,
    request: &AuditPageRequest,
) -> Result<AuditPageResponse, ListAuditError> {
    let mut records = Vec::with_capacity(values.len());
    for (key, value) in values {
        let record = postcard::from_bytes::<MetadataAuditRecord>(&value)
            .map_err(|error| ListAuditError::Storage(error.to_string()))?;
        records.push(AuditPageEntry {
            key: key.to_vec(),
            record,
        });
    }
    let page = AuditPageResponse {
        records,
        next_start_after: next_start_after.map(|key| key.to_vec()),
    };
    validate_page(request, &page).map_err(|error| ListAuditError::Storage(error.to_string()))?;
    Ok(page)
}

/// One node's local audit page as a sans-I/O operation: it reads one page of the audit
/// keyspace and returns it raw. The serving node runs it to answer a peer.
#[derive(Debug, PartialEq)]
pub struct LocalPageOperation {
    realm_id: RealmId,
    group_id: GroupId,
    document_id: Option<Ulid>,
    start_after: Option<Vec<u8>>,
    limit: usize,
}

impl LocalPageOperation {
    pub fn new(
        realm_id: RealmId,
        group_id: GroupId,
        document_id: Option<Ulid>,
        start_after: Option<Vec<u8>>,
        limit: usize,
    ) -> Self {
        let limit = limit.clamp(1, AUDIT_PAGE_LIMIT);
        Self {
            realm_id,
            group_id,
            document_id,
            start_after,
            limit,
        }
    }

    fn request(&self) -> AuditPageRequest {
        AuditPageRequest {
            auth_token: None,
            config_digest: [0u8; 32],
            realm_id: self.realm_id,
            group_id: self.group_id,
            document_id: self.document_id,
            start_after: self.start_after.clone(),
            limit: self.limit,
        }
    }
}

impl Operation for LocalPageOperation {
    type Output = AuditPageResponse;
    type Error = ListAuditError;

    fn first(&mut self) -> Result<Next<AuditPageResponse>, ListAuditError> {
        Ok(Next::Call(IoCall::Page {
            space: METADATA_AUDIT_KEYSPACE.to_string(),
            prefix: audit_prefix(self.group_id, self.document_id).into(),
            after: self.start_after.clone().map(Key::from),
            limit: self.limit,
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<AuditPageResponse>, ListAuditError> {
        let (values, next) = page_rows(result)?;
        Ok(Next::Done(parse_audit_page(values, next, &self.request())?))
    }
}

fn remember_peer(peers: &mut BTreeSet<NodeId>, node: NodeId) {
    if peers.len() < MAX_AUDIT_PEERS {
        peers.insert(node);
    }
}

#[derive(Default)]
struct PeerSelection {
    selected: Vec<NodeId>,
    omitted: BTreeSet<NodeId>,
    missing_count: usize,
}

fn audit_scope(
    realm_id: RealmId,
    local_node: NodeId,
    group_id: GroupId,
    document_id: Option<Ulid>,
    start_after: Option<&[u8]>,
    limit: usize,
    config_digest: [u8; 32],
) -> Vec<u8> {
    let mut scope = Vec::with_capacity(32 + 32 + 16 + 1 + 16 + 1 + 48 + 8 + 32);
    scope.extend_from_slice(realm_id.as_bytes());
    scope.extend_from_slice(local_node.as_bytes());
    scope.extend_from_slice(&group_id.to_bytes());
    match document_id {
        Some(document_id) => {
            scope.push(1);
            scope.extend_from_slice(&document_id.to_bytes());
        }
        None => scope.push(0),
    }
    match start_after {
        Some(start_after) => {
            scope.push(1);
            scope.extend_from_slice(start_after);
        }
        None => scope.push(0),
    }
    scope.extend_from_slice(&limit.to_be_bytes());
    scope.extend_from_slice(&config_digest);
    scope
}

fn select_peers<I>(peers: I, limit: usize, scope: &[u8]) -> PeerSelection
where
    I: IntoIterator<Item = NodeId>,
{
    let mut omitted = BTreeSet::new();
    let mut missing_count = 0usize;
    let selected = select_top_peers(peers, scope, limit.min(MAX_AUDIT_PEERS), |node| {
        remember_peer(&mut omitted, node);
        missing_count = missing_count.saturating_add(1);
    });
    PeerSelection {
        selected,
        omitted,
        missing_count,
    }
}

/// Gathers a group's audit trail across realm nodes as a sans-I/O operation: it scans the
/// local keyspace and asks every selected peer in one group, then merges the pages. A member
/// still running at the group's deadline marks its nodes missing. Node selection is the caller's.
#[derive(Debug, PartialEq)]
pub struct ListAuditOperation {
    realm_id: RealmId,
    local_node: NodeId,
    include_local: bool,
    group_id: GroupId,
    document_id: Option<Ulid>,
    peers: Vec<NodeId>,
    start_after: Option<Vec<u8>>,
    limit: usize,
    auth_token: Option<AuthToken>,
    config_digest: [u8; 32],
    wait: Option<Duration>,
    local_failed: bool,
    batch: AuditPageBatch,
}

type AuditTurn = Result<Next<AuditAggregate>, ListAuditError>;

impl ListAuditOperation {
    #[allow(clippy::too_many_arguments)]
    pub fn new<I>(
        realm_id: RealmId,
        local_node: NodeId,
        include_local: bool,
        group_id: GroupId,
        document_id: Option<Ulid>,
        peers: I,
        start_after: Option<Vec<u8>>,
        limit: usize,
        auth_token: Option<AuthToken>,
        config_digest: [u8; 32],
    ) -> Self
    where
        I: IntoIterator<Item = NodeId>,
    {
        let limit = limit.clamp(1, AUDIT_PAGE_LIMIT);
        let scope = audit_scope(
            realm_id,
            local_node,
            group_id,
            document_id,
            start_after.as_deref(),
            limit,
            config_digest,
        );
        let PeerSelection {
            selected,
            omitted,
            missing_count,
        } = select_peers(peers, MAX_AUDIT_PEERS, &scope);
        let mut batch = AuditPageBatch::with_limit(limit);
        for node in omitted {
            batch.mark_missing(node);
        }
        batch.missing_overflow = batch
            .missing_overflow
            .saturating_add(missing_count.saturating_sub(batch.missing_nodes.len()));
        Self {
            realm_id,
            local_node,
            include_local,
            group_id,
            document_id,
            peers: selected,
            start_after,
            limit,
            auth_token,
            config_digest,
            wait: None,
            local_failed: false,
            batch,
        }
    }

    /// Answers within `wait`: the local page or the peer pages not read by then are missing.
    pub fn within(mut self, wait: Duration) -> Self {
        self.wait = Some(wait);
        self
    }

    fn request(&self) -> AuditPageRequest {
        AuditPageRequest {
            auth_token: self.auth_token.clone(),
            config_digest: self.config_digest,
            realm_id: self.realm_id,
            group_id: self.group_id,
            document_id: self.document_id,
            start_after: self.start_after.clone(),
            limit: self.limit,
        }
    }

    fn peers_effect(&self) -> Effect {
        Effect::Net(NetEffect::AuditPage(Box::new(AuditPageEffect {
            nodes: self.peers.clone(),
            request: self.request(),
        })))
    }

    fn local_result(&mut self, result: GroupResult) -> Result<(), ListAuditError> {
        let event = match result {
            GroupResult::Done(event) => *event,
            GroupResult::Unfinished | GroupResult::Refused => {
                self.local_failed = true;
                self.batch.mark_missing(self.local_node);
                return Ok(());
            }
        };
        let (values, next_start_after) = match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => (values, next_start_after),
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(ListAuditError::Storage(error.to_string()));
            }
            other => return Err(unexpected_event(other.label())),
        };
        let page = parse_audit_page(values, next_start_after, &self.request())?;
        if page.records.is_empty() {
            return Ok(());
        }
        let mut local_batch = AuditPageBatch::with_limit(self.limit);
        local_batch
            .add_page(self.local_node, page, &self.request())
            .map_err(|error| ListAuditError::Storage(error.to_string()))?;
        local_batch.completed_nodes.clear();
        self.batch.merge(local_batch);
        Ok(())
    }

    fn peers_result(&mut self, result: GroupResult) -> Result<(), ListAuditError> {
        let event = match result {
            GroupResult::Done(event) => *event,
            GroupResult::Unfinished | GroupResult::Refused => {
                for node in &self.peers {
                    self.batch.mark_missing(*node);
                }
                return Ok(());
            }
        };
        let Event::Net(NetEvent::AuditPages(mut batch)) = event else {
            return Err(unexpected_event(event.label()));
        };
        if batch.completed_nodes.len() > MAX_AUDIT_PEERS
            || batch.missing_nodes.len() > MAX_AUDIT_PEERS
        {
            return Err(failure("audit peer response exceeds its bound"));
        }
        let expected: BTreeSet<NodeId> = self.peers.iter().copied().collect();
        let accounted: BTreeSet<NodeId> = batch
            .completed_nodes
            .union(&batch.missing_nodes)
            .copied()
            .collect();
        if accounted.iter().any(|node| !expected.contains(node)) {
            return Err(failure("unexpected audit peer response"));
        }
        if batch.records.values().any(|candidate| {
            !batch.completed_nodes.contains(&candidate.source)
                || !expected.contains(&candidate.source)
        }) {
            return Err(failure("unexpected audit peer record"));
        }
        for node in expected.difference(&accounted) {
            batch.mark_missing(*node);
        }
        self.batch.merge(batch);
        Ok(())
    }

    fn aggregate(&mut self) -> AuditAggregate {
        let batch = std::mem::take(&mut self.batch);
        let records: Vec<MetadataAuditRecord> = batch
            .records
            .values()
            .take(self.limit)
            .map(|candidate| candidate.entry.record.clone())
            .collect();
        let cutoff = if batch.records.len() > self.limit {
            batch
                .records
                .keys()
                .nth(self.limit.saturating_sub(1))
                .cloned()
        } else {
            batch.horizon.clone()
        };
        let missing: Vec<NodeId> = batch.missing_nodes.iter().copied().collect();
        let partial = !missing.is_empty()
            || batch.missing_overflow > 0
            || batch.conflict
            || self.local_failed;
        let next_cursor = match partial {
            true => None,
            false => cutoff.map(|key| {
                encode_cursor(
                    self.realm_id,
                    self.config_digest,
                    self.group_id,
                    self.document_id,
                    key,
                )
            }),
        };
        AuditAggregate {
            records,
            next_cursor,
            partial,
            missing_nodes: missing,
            missing_overflow: batch.missing_overflow,
        }
    }
}

fn failure(message: &str) -> ListAuditError {
    ListAuditError::Storage(message.to_string())
}

fn unexpected_event(label: &str) -> ListAuditError {
    ListAuditError::Storage(format!("unexpected audit event {label}"))
}

impl Operation for ListAuditOperation {
    type Output = AuditAggregate;
    type Error = ListAuditError;

    fn first(&mut self) -> AuditTurn {
        let mut members = Vec::new();
        if self.include_local {
            let local = audit_iter_effect(
                self.group_id,
                self.document_id,
                &self.start_after,
                self.limit,
            );
            members.push(local);
        }
        if !self.peers.is_empty() {
            members.push(self.peers_effect());
        }
        if members.is_empty() {
            return Ok(Next::Done(self.aggregate()));
        }
        let limit = members.len();
        let form = GroupForm::All {
            deadline: self.wait,
        };
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit,
            form,
        })))
    }

    fn next(&mut self, result: IoResult) -> AuditTurn {
        let IoResult::Group(GroupEvent::Finished { results }) = result else {
            return Err(unexpected_event(result.label()));
        };
        let mut results = results.into_iter();
        let mut member = || {
            results
                .next()
                .ok_or_else(|| failure("audit group result missing"))
        };
        if self.include_local {
            let local = member()?;
            self.local_result(local)?;
        }
        if !self.peers.is_empty() {
            let peers = member()?;
            self.peers_result(peers)?;
        }
        Ok(Next::Done(self.aggregate()))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AUDIT_CURSOR_CHARS, AUDIT_DEADLINE_SECS, AUDIT_INBOUND_ADMISSION, AUDIT_INBOUND_LIMIT,
        AUDIT_OUTBOUND_ADMISSION, AUDIT_OUTBOUND_LIMIT, AUDIT_PAGE_LIMIT, AuditAggregate,
        AuditPageEntry, AuditPageResponse, Effect, Event, GroupEvent, GroupForm, GroupResult,
        IoCall, IoResult, Key, ListAuditError, ListAuditOperation, ListAuditRequest,
        LocalPageOperation, MAX_AUDIT_PEERS, METADATA_AUDIT_KEYSPACE, NetEffect, NetEvent, Next,
        Operation, StorageEvent, Value, audit_scope, decode_cursor, encode_cursor, select_peers,
    };
    use crate::driver::DriverContext;
    use crate::metadata::audit_entry::{audit_member, authorize_admin, list_audit};
    use crate::metadata::protocol::MetadataReadError;
    use crate::metadata::repository::{audit_entry, metadata_audit_key};
    use aruna_core::UserId;
    use aruna_core::audit::AUDIT_KEY_BYTES;
    use aruna_core::audit::{AuditPageBatch, AuditPageRequest};
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE};
    use aruna_core::metadata::AuthToken;
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::identity::auth::{Actor, AuthContext};
    use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
    use aruna_core::structs::identity::realm::RealmId;
    use aruna_core::structs::identity::realm::{
        RealmAuthorizationDocument, RealmConfigDocument, RealmNodeKind,
    };
    use aruna_core::structs::storage::metadata_registry::{
        MetadataAuditOperation, MetadataAuditRecord,
    };
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use std::collections::BTreeSet;
    use std::time::Duration;
    use tempfile::tempdir;
    use ulid::Ulid;

    fn record(group_id: Ulid, document_id: Ulid, realm_id: RealmId) -> MetadataAuditRecord {
        MetadataAuditRecord {
            realm_id,
            group_id,
            document_id,
            graph_iri: "urn:test".to_string(),
            user_id: UserId::local(Ulid::from_bytes([1u8; 16]), realm_id),
            node_id: iroh::SecretKey::from_bytes(&[2u8; 32]).public(),
            operation: MetadataAuditOperation::Create,
            occurred_at_ms: 1,
            details: None,
        }
    }

    #[test]
    fn checks_audit_member() {
        let realm_id = RealmId([1u8; 32]);
        let node = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let mut config =
            aruna_core::structs::identity::realm::RealmConfigDocument::new(realm_id, Vec::new(), 3);
        config.ensure_node(node, RealmNodeKind::Server);

        assert!(audit_member(&config, realm_id, node));
        assert!(!audit_member(&config, RealmId([3u8; 32]), node));
        assert!(!audit_member(
            &config,
            realm_id,
            iroh::SecretKey::from_bytes(&[4u8; 32]).public()
        ));
    }

    fn entry(
        group_id: Ulid,
        document_id: Ulid,
        audit_id: Ulid,
        realm_id: RealmId,
    ) -> AuditPageEntry {
        AuditPageEntry {
            key: metadata_audit_key(group_id, document_id, audit_id).to_vec(),
            record: record(group_id, document_id, realm_id),
        }
    }

    fn batch(
        node: aruna_core::id::NodeId,
        realm_id: RealmId,
        group_id: Ulid,
        document_id: Option<Ulid>,
        page: AuditPageResponse,
    ) -> AuditPageBatch {
        let mut batch = AuditPageBatch::new();
        batch
            .add_page(
                node,
                page,
                &AuditPageRequest {
                    auth_token: None,
                    config_digest: [0u8; 32],
                    realm_id,
                    group_id,
                    document_id,
                    start_after: None,
                    limit: 200,
                },
            )
            .unwrap();
        batch
    }

    fn members(operation: &mut ListAuditOperation) -> Vec<Effect> {
        let Ok(Next::Call(IoCall::Group(group))) = operation.first() else {
            panic!("expected one audit group");
        };
        group.members
    }

    fn local_page(values: Vec<(Key, Value)>) -> GroupResult {
        let page = StorageEvent::IterResult {
            values,
            next_start_after: None,
        };
        GroupResult::Done(Box::new(Event::Storage(page)))
    }

    fn empty_local() -> GroupResult {
        local_page(Vec::new())
    }

    fn peer_pages(batch: AuditPageBatch) -> GroupResult {
        GroupResult::Done(Box::new(Event::Net(NetEvent::AuditPages(batch))))
    }

    /// Runs the operation's one group with `results` and returns the aggregate.
    fn aggregate(
        operation: &mut ListAuditOperation,
        results: Vec<GroupResult>,
    ) -> Result<AuditAggregate, ListAuditError> {
        operation.first()?;
        match operation.next(IoResult::Group(GroupEvent::Finished { results }))? {
            Next::Done(aggregate) => Ok(aggregate),
            _ => panic!("expected the aggregate"),
        }
    }

    #[test]
    fn peers_share_effect() {
        // Every peer is asked in one adapter round: with one effect per peer the
        // runner contacts them serially and unreachable nodes add up to a timeout.
        let unique: Vec<aruna_core::id::NodeId> = (1u8..=3)
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let mut peers = unique.clone();
        peers.insert(1, unique[0]);
        let mut operation = ListAuditOperation::new(
            RealmId([0u8; 32]),
            unique[0],
            true,
            Ulid::from_bytes([3u8; 16]),
            None,
            peers,
            None,
            10,
            None,
            [0u8; 32],
        );
        let members = members(&mut operation);
        assert_eq!(members.len(), 2);
        let Effect::Net(NetEffect::AuditPage(fan_out)) = &members[1] else {
            panic!("the peer fan-out must be a single net effect");
        };
        assert_eq!(
            fan_out.nodes.iter().copied().collect::<BTreeSet<_>>(),
            unique.iter().copied().collect::<BTreeSet<_>>()
        );
        let mut batch = AuditPageBatch::new();
        for node in &unique {
            batch.mark_missing(*node);
        }
        let aggregate = aggregate(&mut operation, vec![empty_local(), peer_pages(batch)]).unwrap();
        assert!(aggregate.partial);
        assert_eq!(aggregate.missing_nodes.len(), unique.len());
        assert!(aggregate.records.is_empty());
    }

    #[test]
    fn selection_ignores_order() {
        let peers: Vec<_> = (1u8..=130)
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let scope = audit_scope(
            RealmId([1u8; 32]),
            peers[0],
            Ulid::from_bytes([2u8; 16]),
            None,
            None,
            10,
            [3u8; 32],
        );
        let first = select_peers(peers.clone(), MAX_AUDIT_PEERS, &scope);
        let mut reversed = peers;
        reversed.reverse();
        let second = select_peers(reversed, MAX_AUDIT_PEERS, &scope);
        assert_eq!(first.selected, second.selected);
        assert_eq!(first.missing_count, second.missing_count);
        assert!(first.missing_count > 0);
    }

    #[test]
    fn selection_local_seed() {
        let peers: Vec<_> = (1u8..=130)
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let first = ListAuditOperation::new(
            RealmId([1u8; 32]),
            peers[0],
            false,
            Ulid::from_bytes([2u8; 16]),
            None,
            peers.clone(),
            None,
            10,
            None,
            [3u8; 32],
        );
        let second = ListAuditOperation::new(
            RealmId([1u8; 32]),
            peers[1],
            false,
            Ulid::from_bytes([2u8; 16]),
            None,
            peers,
            None,
            10,
            None,
            [3u8; 32],
        );
        assert_ne!(first.peers, second.peers);
    }

    #[test]
    fn peer_budget() {
        let realm_id = RealmId([1u8; 32]);
        let local = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let peers: Vec<_> = (2u8..=u8::try_from(MAX_AUDIT_PEERS + 2).unwrap())
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        for include_local in [true, false] {
            let mut operation = ListAuditOperation::new(
                realm_id,
                local,
                include_local,
                Ulid::from_bytes([2u8; 16]),
                None,
                peers.clone(),
                None,
                10,
                None,
                [0u8; 32],
            );
            let members = members(&mut operation);
            let Some(Effect::Net(NetEffect::AuditPage(effect))) = members.last() else {
                panic!("the peer fan-out must be the last member");
            };
            assert_eq!(effect.nodes.len(), MAX_AUDIT_PEERS);
        }
    }

    #[test]
    fn local_plus_peers() {
        let realm_id = RealmId([1u8; 32]);
        let local = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let group = Ulid::from_bytes([2u8; 16]);
        let doc = Ulid::from_bytes([3u8; 16]);
        let peers: Vec<_> = (2u8..=u8::try_from(MAX_AUDIT_PEERS + 1).unwrap())
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let mut operation = ListAuditOperation::new(
            realm_id,
            local,
            true,
            group,
            Some(doc),
            peers.clone(),
            None,
            10,
            None,
            [0u8; 32],
        );
        let record = record(group, doc, realm_id);
        let local_page = local_page(vec![(
            metadata_audit_key(group, doc, Ulid::from_bytes([4u8; 16])),
            postcard::to_allocvec(&record).unwrap().into(),
        )]);
        let mut batch = AuditPageBatch::new();
        batch.completed_nodes.extend(peers);
        let result = aggregate(&mut operation, vec![local_page, peer_pages(batch)]).unwrap();
        assert!(!result.partial);
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.missing_overflow, 0);
    }

    #[test]
    fn limit_clamps() {
        let node = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let mut operation = ListAuditOperation::new(
            RealmId([1u8; 32]),
            node,
            true,
            Ulid::from_bytes([2u8; 16]),
            None,
            Vec::new(),
            None,
            usize::MAX,
            None,
            [0u8; 32],
        )
        .within(Duration::from_secs(AUDIT_DEADLINE_SECS));
        let Ok(Next::Call(IoCall::Group(group))) = operation.first() else {
            panic!("expected one audit group");
        };
        let deadline = Some(Duration::from_secs(AUDIT_DEADLINE_SECS));
        assert_eq!(group.form, GroupForm::All { deadline });
        let [Effect::Storage(StorageEffect::Iter { limit, .. })] = group.members.as_slice() else {
            panic!("local page must use storage iteration");
        };
        assert_eq!(*limit, AUDIT_PAGE_LIMIT);
    }

    #[test]
    fn unfinished_local_partial() {
        // A local page still running at the deadline marks this node missing.
        let node = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let mut operation = ListAuditOperation::new(
            RealmId([1u8; 32]),
            node,
            true,
            Ulid::from_bytes([2u8; 16]),
            None,
            Vec::new(),
            None,
            10,
            None,
            [0u8; 32],
        );
        let result = aggregate(&mut operation, vec![GroupResult::Unfinished]).unwrap();
        assert!(result.partial);
        assert!(result.next_cursor.is_none());
        assert_eq!(result.missing_nodes, vec![node]);
    }

    #[tokio::test]
    async fn outbound_admission() {
        let permits: Vec<_> = (0..AUDIT_OUTBOUND_LIMIT)
            .map(|_| {
                AUDIT_OUTBOUND_ADMISSION
                    .clone()
                    .try_acquire_owned()
                    .unwrap()
            })
            .collect();
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: FjallStorage::open(directory.path().to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        };
        let node = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let result = list_audit(
            &context,
            RealmId([1u8; 32]),
            node,
            None,
            ListAuditRequest {
                group_id: Ulid::from_bytes([2u8; 16]),
                document_id: None,
                cursor: None,
                limit: Some(10),
                local_authorized: true,
            },
            tokio::time::Instant::now() + Duration::from_secs(AUDIT_DEADLINE_SECS),
        )
        .await;
        assert!(matches!(result, Err(ListAuditError::Unavailable)));
        drop(permits);
    }

    #[test]
    fn admissions_independent() {
        let outbound: Vec<_> = (0..AUDIT_OUTBOUND_LIMIT)
            .map(|_| {
                AUDIT_OUTBOUND_ADMISSION
                    .clone()
                    .try_acquire_owned()
                    .unwrap()
            })
            .collect();
        assert!(AUDIT_INBOUND_ADMISSION.clone().try_acquire_owned().is_ok());
        let inbound: Vec<_> = (0..AUDIT_INBOUND_LIMIT)
            .map(|_| AUDIT_INBOUND_ADMISSION.clone().try_acquire_owned().unwrap())
            .collect();
        assert!(
            AUDIT_OUTBOUND_ADMISSION
                .clone()
                .try_acquire_owned()
                .is_err()
        );
        assert!(AUDIT_INBOUND_ADMISSION.clone().try_acquire_owned().is_err());
        drop(inbound);
        drop(outbound);
    }

    #[test]
    fn success_has_cursor() {
        let realm_id = RealmId([9u8; 32]);
        let group = Ulid::from_bytes([3u8; 16]);
        let doc = Ulid::from_bytes([4u8; 16]);
        let node = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let page = AuditPageResponse {
            records: vec![entry(group, doc, Ulid::from_bytes([1u8; 16]), realm_id)],
            next_start_after: Some(
                metadata_audit_key(group, doc, Ulid::from_bytes([1u8; 16])).to_vec(),
            ),
        };
        let mut operation = ListAuditOperation::new(
            realm_id,
            node,
            true,
            group,
            Some(doc),
            vec![node],
            None,
            10,
            None,
            [0u8; 32],
        );
        let mut batch = AuditPageBatch::new();
        batch
            .add_page(
                node,
                page,
                &AuditPageRequest {
                    auth_token: None,
                    config_digest: [0u8; 32],
                    realm_id,
                    group_id: group,
                    document_id: Some(doc),
                    start_after: None,
                    limit: 10,
                },
            )
            .unwrap();
        let aggregate = aggregate(&mut operation, vec![empty_local(), peer_pages(batch)]).unwrap();
        assert!(!aggregate.partial);
        assert!(aggregate.next_cursor.is_some());
    }

    #[test]
    fn silent_peer_missing() {
        // A peer the adapter did not answer for must still be reported missing
        // instead of leaving the page looking complete.
        let peers: Vec<aruna_core::id::NodeId> = (4u8..=5)
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let mut operation = ListAuditOperation::new(
            RealmId([0u8; 32]),
            peers[0],
            true,
            Ulid::from_bytes([3u8; 16]),
            None,
            peers.clone(),
            None,
            10,
            None,
            [0u8; 32],
        );

        let mut batch = AuditPageBatch::new();
        batch.completed_nodes.insert(peers[0]);
        let aggregate = aggregate(&mut operation, vec![empty_local(), peer_pages(batch)]).unwrap();
        assert!(aggregate.partial);
        assert_eq!(aggregate.missing_nodes, vec![peers[1]]);
    }

    #[test]
    fn partial_page_retries() {
        let realm_id = RealmId([9u8; 32]);
        let group = Ulid::from_bytes([3u8; 16]);
        let doc = Ulid::from_bytes([4u8; 16]);
        let first = Ulid::from_bytes([1u8; 16]);
        let second = Ulid::from_bytes([2u8; 16]);
        let third = Ulid::from_bytes([3u8; 16]);
        let live = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let missing = iroh::SecretKey::from_bytes(&[7u8; 32]).public();

        let mut first_entry = entry(group, doc, first, realm_id);
        first_entry.record.occurred_at_ms = 1;
        let mut second_entry = entry(group, doc, second, realm_id);
        second_entry.record.occurred_at_ms = 2;
        let mut third_entry = entry(group, doc, third, realm_id);
        third_entry.record.occurred_at_ms = 3;
        let live_page = AuditPageResponse {
            records: vec![second_entry, third_entry],
            next_start_after: Some(metadata_audit_key(group, doc, third).to_vec()),
        };

        let mut operation = ListAuditOperation::new(
            realm_id,
            live,
            true,
            group,
            Some(doc),
            vec![live, missing],
            None,
            2,
            None,
            [0u8; 32],
        );
        let mut first_batch = batch(live, realm_id, group, Some(doc), live_page.clone());
        first_batch.mark_missing(missing);
        let results = vec![empty_local(), peer_pages(first_batch)];
        let partial = aggregate(&mut operation, results).unwrap();
        assert!(partial.partial);
        assert!(partial.next_cursor.is_none());

        let mut retry = ListAuditOperation::new(
            realm_id,
            live,
            true,
            group,
            Some(doc),
            vec![live, missing],
            None,
            2,
            None,
            [0u8; 32],
        );
        let mut retry_batch = batch(live, realm_id, group, Some(doc), live_page);
        retry_batch.merge(batch(
            missing,
            realm_id,
            group,
            Some(doc),
            AuditPageResponse {
                records: vec![first_entry],
                next_start_after: None,
            },
        ));
        let results = vec![empty_local(), peer_pages(retry_batch)];
        let recovered = aggregate(&mut retry, results).unwrap();
        assert_eq!(
            recovered
                .records
                .iter()
                .map(|record| record.occurred_at_ms)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn cursor_round_trips() {
        let realm_id = RealmId([6u8; 32]);
        let config_digest = [5u8; 32];
        let group = Ulid::from_bytes([7u8; 16]);
        let key = metadata_audit_key(
            group,
            Ulid::from_bytes([8u8; 16]),
            Ulid::from_bytes([9u8; 16]),
        )
        .to_vec();
        let encoded = encode_cursor(realm_id, config_digest, group, None, key.clone());
        assert_eq!(
            decode_cursor(&encoded, realm_id, config_digest, group, None).unwrap(),
            key
        );
        assert!(decode_cursor(&encoded, RealmId([4u8; 32]), config_digest, group, None).is_err());
        assert!(decode_cursor(&encoded, realm_id, [4u8; 32], group, None).is_err());
        assert!(
            decode_cursor(
                &encoded,
                realm_id,
                config_digest,
                Ulid::from_bytes([6u8; 16]),
                None
            )
            .is_err()
        );
        assert!(decode_cursor("!!not-base64!!", realm_id, config_digest, group, None).is_err());
        assert!(
            decode_cursor(
                &"a".repeat(AUDIT_CURSOR_CHARS + 1),
                realm_id,
                config_digest,
                group,
                None
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_bad_doc() {
        let realm_id = RealmId([9u8; 32]);
        let group = Ulid::from_bytes([3u8; 16]);
        let doc = Ulid::from_bytes([4u8; 16]);
        let other = Ulid::from_bytes([5u8; 16]);
        let node = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let mut row = entry(group, doc, Ulid::from_bytes([1u8; 16]), realm_id);
        row.record.document_id = other;
        let mut batch = AuditPageBatch::new();
        let error = batch.add_page(
            node,
            AuditPageResponse {
                records: vec![row],
                next_start_after: None,
            },
            &AuditPageRequest {
                auth_token: None,
                config_digest: [0u8; 32],
                realm_id,
                group_id: group,
                document_id: None,
                start_after: None,
                limit: 10,
            },
        );
        assert!(error.is_err());
    }

    #[test]
    fn caps_missing_peers() {
        let realm_id = RealmId([1u8; 32]);
        let local = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let peers: Vec<_> = (2u8..=u8::try_from(MAX_AUDIT_PEERS * 2 + 3).unwrap())
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let mut operation = ListAuditOperation::new(
            realm_id,
            local,
            false,
            Ulid::from_bytes([2u8; 16]),
            None,
            peers,
            None,
            10,
            Some(AuthToken::bearer("test").unwrap()),
            [3u8; 32],
        );
        let result = aggregate(&mut operation, vec![GroupResult::Unfinished]).unwrap();
        assert!(result.partial);
        assert!(result.next_cursor.is_none());
        assert!(result.missing_overflow > 0);
        assert!(result.missing_nodes.len() <= MAX_AUDIT_PEERS);
    }

    #[test]
    fn rejects_foreign_peer() {
        let node = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let stranger = iroh::SecretKey::from_bytes(&[9u8; 32]).public();
        let mut operation = ListAuditOperation::new(
            RealmId([1u8; 32]),
            node,
            true,
            Ulid::from_bytes([2u8; 16]),
            None,
            vec![node],
            None,
            10,
            None,
            [0u8; 32],
        );
        let mut batch = AuditPageBatch::new();
        batch.completed_nodes.insert(stranger);
        let results = vec![empty_local(), peer_pages(batch)];
        assert!(aggregate(&mut operation, results).is_err());
        assert!(operation.next(IoResult::Done).is_err());
    }

    #[test]
    fn rejects_conflicting_rows() {
        let realm_id = RealmId([9u8; 32]);
        let group = Ulid::from_bytes([3u8; 16]);
        let doc = Ulid::from_bytes([4u8; 16]);
        let first = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let second = iroh::SecretKey::from_bytes(&[2u8; 32]).public();
        let id = Ulid::from_bytes([8u8; 16]);
        let mut left = batch(
            first,
            realm_id,
            group,
            Some(doc),
            AuditPageResponse {
                records: vec![entry(group, doc, id, realm_id)],
                next_start_after: None,
            },
        );
        let mut changed = entry(group, doc, id, realm_id);
        changed.record.occurred_at_ms = 2;
        left.merge(batch(
            second,
            realm_id,
            group,
            Some(doc),
            AuditPageResponse {
                records: vec![changed],
                next_start_after: None,
            },
        ));
        assert!(left.conflict);
        assert_eq!(
            left.records[&metadata_audit_key(group, doc, id).to_vec()].source,
            first
        );
    }

    #[test]
    fn user_skips_local() {
        let node = iroh::SecretKey::from_bytes(&[3u8; 32]).public();
        let mut operation = ListAuditOperation::new(
            RealmId([9u8; 32]),
            node,
            false,
            Ulid::from_bytes([4u8; 16]),
            None,
            vec![node],
            None,
            10,
            None,
            [0u8; 32],
        );
        assert!(matches!(
            members(&mut operation).as_slice(),
            [Effect::Net(NetEffect::AuditPage(_))]
        ));
    }

    #[tokio::test]
    async fn forwarded_policy_deny() {
        // A serving peer must apply the same policy context as the REST gate.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = std::sync::Arc::new(DriverContext {
            storage_handle: storage.clone(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        });
        let realm_id = RealmId([8u8; 32]);
        let group_id = Ulid::from_bytes([9u8; 16]);
        let user_id = UserId::local(Ulid::from_bytes([10u8; 16]), realm_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[11u8; 32]).public(),
            user_id,
            realm_id,
        };
        let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
        let mut group_auth =
            GroupAuthorizationDocument::default_group_doc(user_id, realm_id, group_id);
        group_auth.policies.push(RequestPolicy {
            policy_id: Ulid::from_bytes([12u8; 16]),
            name: "deny-audit".to_string(),
            kind: PolicyKind::Deny,
            when: None,
            expression: "operation == 'rest'".to_string(),
            enabled: true,
        });
        let group = Group {
            display_name: "audit".to_string(),
            group_id,
            realm_id,
            roles: group_auth.roles.keys().copied().collect(),
            owner: user_id,
        };
        for (key_space, key, value) in [
            // Policy loading fails closed without the realm config document, so
            // the denial below must come from the group policy, not absent state.
            (
                REALM_CONFIG_KEYSPACE,
                realm_id.as_bytes().to_vec(),
                RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                    .to_bytes(&actor)
                    .unwrap(),
            ),
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
                storage
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

        let result = authorize_admin(
            &context,
            AuthContext {
                user_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            format!("/{realm_id}/g/{group_id}/admin"),
        )
        .await;
        assert!(matches!(result, Err(MetadataReadError::Forbidden)));
    }

    #[test]
    fn merges_holder_projections() {
        // The same record on two holders appears once; a truncated node bounds
        // the page so no key past the horizon leaks before the next request.
        let realm_id = RealmId([9u8; 32]);
        let group = Ulid::from_bytes([3u8; 16]);
        let doc = Ulid::from_bytes([4u8; 16]);
        let first = Ulid::from_bytes([1u8; 16]);
        let second = Ulid::from_bytes([2u8; 16]);
        let third = Ulid::from_bytes([3u8; 16]);
        let holder_a = AuditPageResponse {
            records: vec![
                entry(group, doc, first, realm_id),
                entry(group, doc, third, realm_id),
            ],
            next_start_after: Some(metadata_audit_key(group, doc, third).to_vec()),
        };
        let holder_b = AuditPageResponse {
            records: vec![
                entry(group, doc, first, realm_id),
                entry(group, doc, second, realm_id),
            ],
            next_start_after: None,
        };

        let node_a = iroh::SecretKey::from_bytes(&[5u8; 32]).public();
        let node_b = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let mut merged = batch(node_a, realm_id, group, Some(doc), holder_a);
        merged.merge(batch(node_b, realm_id, group, Some(doc), holder_b));
        // horizon is holder_a's last key (third); every key <= third is complete.
        assert_eq!(merged.records.len(), 3);
        assert_eq!(
            merged.horizon,
            Some(metadata_audit_key(group, doc, third).to_vec())
        );
    }

    #[test]
    fn limit_bounds_page() {
        let realm_id = RealmId([9u8; 32]);
        let group = Ulid::from_bytes([3u8; 16]);
        let doc = Ulid::from_bytes([4u8; 16]);
        let ids = [
            Ulid::from_bytes([1u8; 16]),
            Ulid::from_bytes([2u8; 16]),
            Ulid::from_bytes([3u8; 16]),
        ];
        let page = AuditPageResponse {
            records: ids
                .iter()
                .map(|id| entry(group, doc, *id, realm_id))
                .collect(),
            next_start_after: None,
        };

        let node = iroh::SecretKey::from_bytes(&[7u8; 32]).public();
        let mut operation = ListAuditOperation::new(
            realm_id,
            node,
            true,
            group,
            Some(doc),
            vec![node],
            None,
            2,
            None,
            [0u8; 32],
        );
        let pages = peer_pages(batch(node, realm_id, group, Some(doc), page));
        let aggregate = aggregate(&mut operation, vec![empty_local(), pages]).unwrap();
        assert_eq!(aggregate.records.len(), 2);
        assert!(aggregate.next_cursor.is_some());
    }

    #[tokio::test]
    async fn local_page_paging() {
        // Cursor paging walks the group's records; a document filter narrows.
        let dir = tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
            outbound_handle: None,
        };
        let realm_id = RealmId([9u8; 32]);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let first_doc = Ulid::from_bytes([4u8; 16]);
        let second_doc = Ulid::from_bytes([5u8; 16]);
        for (document_id, audit_id) in [
            (first_doc, Ulid::from_bytes([6u8; 16])),
            (first_doc, Ulid::from_bytes([7u8; 16])),
            (second_doc, Ulid::from_bytes([8u8; 16])),
        ] {
            let row = audit_entry(&record(group_id, document_id, realm_id), audit_id).unwrap();
            crate::testing::write_entries(&context.storage_handle, vec![row]).await;
        }

        let first = crate::driver::drive(
            LocalPageOperation::new(realm_id, group_id, None, None, 2),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(first.records.len(), 2);
        let cursor = first.next_start_after.expect("more records");

        let second = crate::driver::drive(
            LocalPageOperation::new(realm_id, group_id, None, Some(cursor), 2),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(second.records.len(), 1);
        assert_eq!(second.records[0].record.document_id, second_doc);

        let filtered = crate::driver::drive(
            LocalPageOperation::new(realm_id, group_id, Some(second_doc), None, 50),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(filtered.records.len(), 1);

        let foreign = crate::driver::drive(
            LocalPageOperation::new(realm_id, Ulid::from_bytes([99u8; 16]), None, None, 50),
            &context,
        )
        .await
        .unwrap();
        assert!(foreign.records.is_empty());
    }

    #[test]
    fn local_page_calls() {
        let realm_id = RealmId([3u8; 32]);
        let group_id = Ulid::from_bytes([4u8; 16]);
        let document_id = Ulid::from_bytes([5u8; 16]);
        let mut prefix = group_id.to_bytes().to_vec();
        prefix.extend_from_slice(&document_id.to_bytes());
        let mut cursor = prefix.clone();
        cursor.resize(AUDIT_KEY_BYTES, 7);
        let after = Some(cursor);
        let mut operation =
            LocalPageOperation::new(realm_id, group_id, Some(document_id), after.clone(), 0);
        assert_eq!(
            operation.first(),
            Ok(Next::Call(IoCall::Page {
                space: METADATA_AUDIT_KEYSPACE.to_string(),
                prefix: prefix.into(),
                after: after.map(Key::from),
                limit: 1,
            }))
        );
        let empty = IoResult::Page {
            rows: Vec::new(),
            next: None,
        };
        let Ok(Next::Done(page)) = operation.next(empty) else {
            panic!("expected the page");
        };
        assert!(page.records.is_empty() && page.next_start_after.is_none());
        let broken = IoResult::Page {
            rows: vec![(vec![1u8].into(), vec![0xff].into())],
            next: None,
        };
        assert!(matches!(
            operation.next(broken),
            Err(ListAuditError::Storage(_))
        ));
    }
}
