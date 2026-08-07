//! Read surface over the metadata audit trail. Audit rows are node-local
//! projections, so a group's complete trail is gathered by asking every eligible
//! realm node for its local page and merging the pages by their raw storage key.

use std::collections::BTreeSet;
use std::sync::{Arc, LazyLock};

use aruna_core::audit::{
    AUDIT_KEY_BYTES, AuditPageBatch, AuditPageEntry, AuditPageRequest, AuditPageResponse,
    MAX_AUDIT_PEERS, MAX_AUDIT_RECORDS, validate_page, validate_request,
};
use aruna_core::effects::{AuditPageEffect, Effect, IterStart, NetEffect, StorageEffect};
use aruna_core::events::{Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::METADATA_AUDIT_KEYSPACE;
use aruna_core::metadata::MetadataAuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::{AuthContext, MetadataAuditRecord, Permission, RealmId};
use aruna_core::types::{Effects, GroupId, Key, NodeId, Value};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use tokio::sync::Semaphore;
use ulid::Ulid;

use super::api::load_realm_config;
use super::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::driver::{DriverContext, drive_until};
use crate::placement::selector::{ROLE_NODE, neg_log2_q48, selector_hash};
use crate::request_authorization::{AuthorizeError, authorize};
use crate::request_policy::PolicyRequestExtras;

pub const MAX_AUDIT_PAGE_SIZE: usize = MAX_AUDIT_RECORDS;
pub const DEFAULT_AUDIT_PAGE_SIZE: usize = 50;
const AUDIT_CURSOR_VERSION: u8 = 1;
const MAX_AUDIT_CURSOR_BYTES: usize = 256;
const MAX_AUDIT_CURSOR_CHARS: usize = 384;
const AUDIT_INBOUND_LIMIT: usize = 16;
const AUDIT_OUTBOUND_LIMIT: usize = 16;
pub const AUDIT_DEADLINE_SECS: u64 = 30;
static AUDIT_INBOUND_ADMISSION: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(AUDIT_INBOUND_LIMIT)));
static AUDIT_OUTBOUND_ADMISSION: LazyLock<Arc<Semaphore>> =
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
    let bytes = postcard::to_allocvec(&AuditCursor {
        version: AUDIT_CURSOR_VERSION,
        realm_id,
        config_digest,
        group_id,
        document_id,
        key,
    })
    .unwrap_or_default();
    URL_SAFE_NO_PAD.encode(bytes)
}

fn decode_cursor(
    cursor: &str,
    realm_id: RealmId,
    config_digest: [u8; 32],
    group_id: GroupId,
    document_id: Option<Ulid>,
) -> Result<Vec<u8>, ListAuditError> {
    if cursor.len() > MAX_AUDIT_CURSOR_CHARS {
        return Err(ListAuditError::InvalidCursor);
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| ListAuditError::InvalidCursor)?;
    if bytes.len() > MAX_AUDIT_CURSOR_BYTES {
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
    let mut prefix = group_id.to_bytes().to_vec();
    if let Some(document_id) = document_id {
        prefix.extend_from_slice(&document_id.to_bytes());
    }
    if !cursor.key.starts_with(&prefix) {
        return Err(ListAuditError::InvalidCursor);
    }
    Ok(cursor.key)
}

fn audit_iter_effect(
    group_id: GroupId,
    document_id: Option<Ulid>,
    start_after: &Option<Vec<u8>>,
    limit: usize,
) -> Effect {
    let mut prefix = group_id.to_bytes().to_vec();
    if let Some(document_id) = document_id {
        prefix.extend_from_slice(&document_id.to_bytes());
    }
    Effect::Storage(StorageEffect::Iter {
        key_space: METADATA_AUDIT_KEYSPACE.to_string(),
        prefix: Some(prefix.into()),
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

/// One node's local audit page as a sans-I/O operation: it emits a single
/// keyspace scan and returns the raw page. The serving node runs it to answer a
/// peer, and the aggregator reuses the same scan for its own local page.
#[derive(Debug, PartialEq)]
pub struct LocalAuditPageOperation {
    realm_id: RealmId,
    group_id: GroupId,
    document_id: Option<Ulid>,
    start_after: Option<Vec<u8>>,
    limit: usize,
    done: bool,
    output: Option<Result<AuditPageResponse, ListAuditError>>,
}

impl LocalAuditPageOperation {
    pub fn new(
        realm_id: RealmId,
        group_id: GroupId,
        document_id: Option<Ulid>,
        start_after: Option<Vec<u8>>,
        limit: usize,
    ) -> Self {
        let limit = limit.clamp(1, MAX_AUDIT_PAGE_SIZE);
        Self {
            realm_id,
            group_id,
            document_id,
            start_after,
            limit,
            done: false,
            output: None,
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

impl Operation for LocalAuditPageOperation {
    type Output = AuditPageResponse;
    type Error = ListAuditError;

    fn start(&mut self) -> Effects {
        smallvec![audit_iter_effect(
            self.group_id,
            self.document_id,
            &self.start_after,
            self.limit
        )]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.done {
            self.output = Some(Err(ListAuditError::Storage(
                "audit event after completion".to_string(),
            )));
            return smallvec![];
        }
        self.output = Some(match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => parse_audit_page(values, next_start_after, &self.request()),
            Event::Storage(StorageEvent::Error { error }) => {
                Err(ListAuditError::Storage(error.to_string()))
            }
            other => Err(ListAuditError::Storage(format!(
                "unexpected audit event {other:?}"
            ))),
        });
        self.done = true;
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        self.done
    }

    fn finalize(self) -> Result<AuditPageResponse, ListAuditError> {
        self.output.unwrap_or_else(|| {
            Err(ListAuditError::Storage(
                "audit page did not complete".to_string(),
            ))
        })
    }

    fn abort(&mut self) -> Effects {
        smallvec![]
    }
}

#[derive(Debug, PartialEq)]
enum FanState {
    Collecting,
    Done,
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
    let limit = limit.min(MAX_AUDIT_PEERS);
    let mut selected = BTreeSet::new();
    let mut omitted = BTreeSet::new();
    let mut missing_count = 0usize;
    for node in peers {
        if selected.iter().any(|(_, candidate)| *candidate == node) {
            continue;
        }
        let score = neg_log2_q48(selector_hash(ROLE_NODE, scope, node.as_bytes()));
        if selected.len() < limit {
            selected.insert((score, node));
            continue;
        }
        let Some(worst) = selected.last().copied() else {
            remember_peer(&mut omitted, node);
            missing_count = missing_count.saturating_add(1);
            continue;
        };
        if (score, node) < worst {
            selected.remove(&worst);
            remember_peer(&mut omitted, worst.1);
            missing_count = missing_count.saturating_add(1);
            selected.insert((score, node));
        } else {
            remember_peer(&mut omitted, node);
            missing_count = missing_count.saturating_add(1);
        }
    }
    PeerSelection {
        selected: selected.into_iter().map(|(_, node)| node).collect(),
        omitted,
        missing_count,
    }
}

/// Gathers a group's audit trail across realm nodes as a sans-I/O operation: it
/// scans the local keyspace and emits one audit-page request per peer, then
/// merges the pages. Node selection is supplied by the caller.
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
    auth_token: Option<MetadataAuthToken>,
    config_digest: [u8; 32],
    state: FanState,
    started: bool,
    pending: usize,
    local_done: bool,
    local_failed: bool,
    responded: BTreeSet<NodeId>,
    batch: AuditPageBatch,
    error: Option<ListAuditError>,
}

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
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
    ) -> Self
    where
        I: IntoIterator<Item = NodeId>,
    {
        let limit = limit.clamp(1, MAX_AUDIT_PAGE_SIZE);
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
            state: FanState::Collecting,
            started: false,
            pending: 0,
            local_done: !include_local,
            local_failed: false,
            responded: BTreeSet::new(),
            batch,
            error: None,
        }
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

    fn fail(&mut self, message: &str) -> Effects {
        self.error = Some(ListAuditError::Storage(message.to_string()));
        self.state = FanState::Done;
        smallvec![]
    }
}

impl Operation for ListAuditOperation {
    type Output = AuditAggregate;
    type Error = ListAuditError;

    fn start(&mut self) -> Effects {
        if self.state != FanState::Collecting || self.started {
            return self.fail("audit operation already started");
        }
        self.started = true;
        if self.include_local && !self.local_done {
            self.pending = 1;
            return smallvec![audit_iter_effect(
                self.group_id,
                self.document_id,
                &self.start_after,
                self.limit
            )];
        }
        if self.peers.is_empty() {
            self.state = FanState::Done;
            return smallvec![];
        }
        self.pending = 1;
        smallvec![self.peers_effect()]
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state != FanState::Collecting || self.pending != 1 {
            return self.fail("audit event after aggregation completed");
        }
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                if !self.include_local || self.local_done {
                    return self.fail("duplicate local audit page");
                }
                match parse_audit_page(values, next_start_after, &self.request()) {
                    Ok(page) => {
                        if !page.records.is_empty() {
                            let source = self.local_node;
                            let mut local_batch = AuditPageBatch::with_limit(self.limit);
                            if let Err(error) = local_batch.add_page(source, page, &self.request())
                            {
                                self.error = Some(ListAuditError::Storage(error.to_string()));
                                self.state = FanState::Done;
                                return smallvec![];
                            }
                            local_batch.completed_nodes.clear();
                            self.batch.merge(local_batch);
                        }
                    }
                    Err(error) => {
                        self.error = Some(error);
                        self.state = FanState::Done;
                        return smallvec![];
                    }
                }
                self.local_done = true;
                self.pending = 0;
                if !self.peers.is_empty() {
                    self.pending = 1;
                    return smallvec![self.peers_effect()];
                }
            }
            Event::Storage(StorageEvent::Error { error }) => {
                if !self.include_local || self.local_done {
                    return self.fail("unexpected local audit error");
                }
                self.error = Some(ListAuditError::Storage(error.to_string()));
                self.state = FanState::Done;
                return smallvec![];
            }
            Event::Net(NetEvent::AuditPages(mut batch)) => {
                if !self.local_done || self.peers.is_empty() {
                    return self.fail("audit peers responded before local page");
                }
                if batch.completed_nodes.len() > MAX_AUDIT_PEERS
                    || batch.missing_nodes.len() > MAX_AUDIT_PEERS
                {
                    return self.fail("audit peer response exceeds its bound");
                }
                let expected: BTreeSet<NodeId> = self.peers.iter().copied().collect();
                let accounted: BTreeSet<NodeId> = batch
                    .completed_nodes
                    .union(&batch.missing_nodes)
                    .copied()
                    .collect();
                if accounted.iter().any(|node| !expected.contains(node)) {
                    return self.fail("unexpected audit peer response");
                }
                if batch.records.values().any(|candidate| {
                    !batch.completed_nodes.contains(&candidate.source)
                        || !expected.contains(&candidate.source)
                }) {
                    return self.fail("unexpected audit peer record");
                }
                for node in expected.difference(&accounted) {
                    batch.mark_missing(*node);
                }
                self.responded.extend(batch.completed_nodes.iter().copied());
                self.responded.extend(batch.missing_nodes.iter().copied());
                self.batch.merge(batch);
                self.pending = 0;
            }
            other => return self.fail(&format!("unexpected audit event {other:?}")),
        }
        if self.pending == 0 {
            self.state = FanState::Done;
        }
        smallvec![]
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, FanState::Done)
    }

    fn finalize(self) -> Result<AuditAggregate, ListAuditError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        let records: Vec<MetadataAuditRecord> = self
            .batch
            .records
            .values()
            .take(self.limit)
            .map(|candidate| candidate.entry.record.clone())
            .collect();
        let cutoff = if self.batch.records.len() > self.limit {
            self.batch
                .records
                .keys()
                .nth(self.limit.saturating_sub(1))
                .cloned()
        } else {
            self.batch.horizon.clone()
        };
        let missing: Vec<NodeId> = self.batch.missing_nodes.iter().copied().collect();
        let partial = !missing.is_empty() || self.batch.missing_overflow > 0 || self.batch.conflict;
        let partial = partial || self.local_failed;
        Ok(AuditAggregate {
            records,
            next_cursor: if partial {
                None
            } else {
                cutoff.map(|key| {
                    encode_cursor(
                        self.realm_id,
                        self.config_digest,
                        self.group_id,
                        self.document_id,
                        key,
                    )
                })
            },
            partial,
            missing_nodes: missing,
            missing_overflow: self.batch.missing_overflow,
        })
    }

    fn abort(&mut self) -> Effects {
        if self.include_local && !self.local_done && !self.local_failed {
            self.local_failed = true;
            self.batch.mark_missing(self.local_node);
        }
        let pending: Vec<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|node| !self.responded.contains(node))
            .collect();
        for node in pending {
            self.batch.mark_missing(node);
        }
        self.state = FanState::Done;
        smallvec![]
    }
}

/// Gathers the group's audit trail from every eligible realm node and merges the
/// pages. `local_node` reads its own keyspace directly; the remaining nodes are
/// queried over the metadata control transport.
pub async fn list_audit(
    context: &DriverContext,
    realm_id: RealmId,
    local_node: NodeId,
    auth_token: Option<MetadataAuthToken>,
    request: ListAuditRequest,
    deadline: tokio::time::Instant,
) -> Result<AuditAggregate, ListAuditError> {
    let _admission = AUDIT_OUTBOUND_ADMISSION
        .clone()
        .try_acquire_owned()
        .map_err(|_| ListAuditError::Unavailable)?;
    let limit = request
        .limit
        .unwrap_or(DEFAULT_AUDIT_PAGE_SIZE)
        .clamp(1, MAX_AUDIT_PAGE_SIZE);

    // Membership and its digest are eventual candidates; each peer revalidates them.
    let mut partial = false;
    let mut config_document = None;
    let mut config_digest = [0u8; 32];
    let mut digest_ready = false;
    let mut include_local = false;
    let mut user_origin = false;
    let config = tokio::time::timeout_at(deadline, load_realm_config(context, realm_id))
        .await
        .map_err(|_| ListAuditError::Unavailable)?;
    match config {
        None => partial = true,
        Some(config) => {
            match config.digest() {
                Ok(digest) => {
                    config_digest = digest;
                    digest_ready = true;
                }
                Err(_) => partial = true,
            }
            match config
                .nodes
                .iter()
                .find(|node| node.node_id == local_node.to_string())
            {
                Some(node) => {
                    let current_local_authorized =
                        !matches!(&node.kind, aruna_core::structs::RealmNodeKind::User);
                    user_origin = !current_local_authorized;
                    include_local = request.local_authorized && current_local_authorized;
                    if request.local_authorized != current_local_authorized {
                        partial = true;
                    }
                }
                None => partial = true,
            }
            config_document = Some(config);
        }
    }
    if request.cursor.is_some() && !digest_ready {
        return Err(ListAuditError::InvalidCursor);
    }
    if user_origin && auth_token.is_none() {
        return Err(ListAuditError::Unauthorized);
    }
    let start_after = request
        .cursor
        .as_deref()
        .map(|cursor| {
            decode_cursor(
                cursor,
                realm_id,
                config_digest,
                request.group_id,
                request.document_id,
            )
        })
        .transpose()?;

    let mut invalid_peer = false;
    let peer_nodes = config_document
        .as_ref()
        .into_iter()
        .flat_map(|config| config.nodes.iter())
        .filter(|node| node.kind.is_sync_eligible())
        .filter_map(|node| match node.node_id.parse::<NodeId>() {
            Ok(node) if node != local_node && digest_ready => Some(node),
            Ok(_) => None,
            Err(_) => {
                invalid_peer = true;
                None
            }
        });
    let operation = ListAuditOperation::new(
        realm_id,
        local_node,
        include_local,
        request.group_id,
        request.document_id,
        peer_nodes,
        start_after,
        limit,
        auth_token,
        config_digest,
    );
    partial |= invalid_peer;
    let mut aggregate = drive_until(operation, context, deadline).await?;
    aggregate.partial |= partial;
    if aggregate.partial {
        aggregate.next_cursor = None;
    }
    Ok(aggregate)
}

/// Serves a peer's request for this node's local audit page after re-checking the
/// caller's group-admin authority under the forwarded token.
pub(crate) async fn serve_local_audit(
    context: &Arc<DriverContext>,
    peer: NodeId,
    request: AuditPageRequest,
    deadline: tokio::time::Instant,
) -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardedAuditPage {
        result: local_audit_result(context, peer, request, deadline).await,
    }
}

async fn local_audit_result(
    context: &Arc<DriverContext>,
    peer: NodeId,
    request: AuditPageRequest,
    deadline: tokio::time::Instant,
) -> Result<AuditPageResponse, MetadataReadError> {
    let _admission = AUDIT_INBOUND_ADMISSION
        .clone()
        .try_acquire_owned()
        .map_err(|_| MetadataReadError::Unavailable)?;
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    if request.realm_id != realm_id {
        return Err(MetadataReadError::Unavailable);
    }
    validate_request(&request).map_err(|_| MetadataReadError::Unavailable)?;
    let config = tokio::time::timeout_at(deadline, load_realm_config(context.as_ref(), realm_id))
        .await
        .map_err(|_| MetadataReadError::Unavailable)?
        .ok_or(MetadataReadError::Unavailable)?;
    if config.digest().ok() != Some(request.config_digest) {
        return Err(MetadataReadError::Unavailable);
    }
    let local_node = net_handle.node_id();
    if !config
        .nodes
        .iter()
        .any(|node| node.node_id == local_node.to_string() && node.kind.is_sync_eligible())
    {
        return Err(MetadataReadError::Unavailable);
    }
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let auth_result = tokio::time::timeout_at(
        deadline,
        metadata.authorize_read_peer(peer, request.auth_token.clone(), false),
    )
    .await
    .map_err(|_| MetadataReadError::Unavailable)?;
    let auth = auth_result?.ok_or(MetadataReadError::Unauthorized)?;
    let path = format!("/{realm_id}/g/{}/admin", request.group_id);
    let auth_result = tokio::time::timeout_at(deadline, authorize_admin(context, auth, path))
        .await
        .map_err(|_| MetadataReadError::Unavailable)?;
    auth_result?;

    let operation = LocalAuditPageOperation::new(
        realm_id,
        request.group_id,
        request.document_id,
        request.start_after,
        request.limit.clamp(1, MAX_AUDIT_PAGE_SIZE),
    );
    drive_until(operation, context.as_ref(), deadline)
        .await
        .map_err(|_| MetadataReadError::Unavailable)
}

async fn authorize_admin(
    context: &Arc<DriverContext>,
    auth_context: AuthContext,
    path: String,
) -> Result<(), MetadataReadError> {
    match authorize(
        context.as_ref(),
        auth_context.realm_id,
        &auth_context,
        &path,
        &Permission::WRITE,
        PolicyRequestExtras::rest(),
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
            Err(MetadataReadError::Forbidden)
        }
        Err(AuthorizeError::CheckFailed(_)) => Err(MetadataReadError::Unavailable),
    }
}

/// Adapter I/O for [`AuditPageEffect`]: requests one node's local page over the
/// metadata control transport. Kept out of the sans-I/O operation.
pub(crate) async fn send_audit_request(
    context: &DriverContext,
    node: NodeId,
    request: AuditPageRequest,
) -> Result<AuditPageResponse, MetadataReadError> {
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    match metadata
        .request_forwarded_write(node, MetadataTransportMessage::ForwardAuditPage { request })
        .await
    {
        Ok(MetadataTransportMessage::ForwardedAuditPage { result }) => result,
        Ok(_) | Err(_) => Err(MetadataReadError::Unavailable),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AUDIT_INBOUND_ADMISSION, AUDIT_INBOUND_LIMIT, AUDIT_OUTBOUND_ADMISSION,
        AUDIT_OUTBOUND_LIMIT, AuditPageEntry, AuditPageResponse, Effect, Event, ListAuditError,
        ListAuditOperation, ListAuditRequest, LocalAuditPageOperation, MAX_AUDIT_CURSOR_CHARS,
        MAX_AUDIT_PAGE_SIZE, MAX_AUDIT_PEERS, MetadataReadError, NetEffect, NetEvent, Operation,
        StorageEvent, audit_scope, authorize_admin, decode_cursor, drive_until, encode_cursor,
        list_audit, select_peers,
    };
    use crate::driver::DriverContext;
    use crate::metadata::repository::{metadata_audit_key, write_audit_effect};
    use aruna_core::UserId;
    use aruna_core::audit::{AuditPageBatch, AuditPageRequest};
    use aruna_core::effects::StorageEffect;
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::metadata::MetadataAuthToken;
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::{
        Actor, AuthContext, Group, GroupAuthorizationDocument, RealmAuthorizationDocument,
    };
    use aruna_core::structs::{MetadataAuditOperation, MetadataAuditRecord, RealmId};
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
        node: aruna_core::types::NodeId,
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

    #[test]
    fn peers_share_effect() {
        // Every peer is asked in one adapter round: with one effect per peer the
        // runner contacts them serially and unreachable nodes add up to a timeout.
        let unique: Vec<aruna_core::types::NodeId> = (1u8..=3)
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let mut peers = unique.clone();
        peers.insert(1, unique[0]);
        let group_id = Ulid::from_bytes([3u8; 16]);
        let mut operation = ListAuditOperation::new(
            RealmId([0u8; 32]),
            unique[0],
            true,
            group_id,
            None,
            peers.clone(),
            None,
            10,
            None,
            [0u8; 32],
        );

        let effects = operation.start();
        assert_eq!(effects.len(), 1);

        let effects = operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        let Effect::Net(NetEffect::AuditPage(fan_out)) = &effects[0] else {
            panic!("the peer fan-out must be a single net effect");
        };
        assert_eq!(
            fan_out.nodes.iter().copied().collect::<BTreeSet<_>>(),
            unique.iter().copied().collect::<BTreeSet<_>>()
        );
        assert!(!operation.is_complete());
        let mut batch = AuditPageBatch::new();
        for node in &unique {
            batch.mark_missing(*node);
        }
        operation.step(Event::Net(NetEvent::AuditPages(batch)));

        assert!(operation.is_complete());
        let aggregate = operation.finalize().unwrap();
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
        let mut local_operation = ListAuditOperation::new(
            realm_id,
            local,
            true,
            Ulid::from_bytes([2u8; 16]),
            None,
            peers.clone(),
            None,
            10,
            None,
            [0u8; 32],
        );
        local_operation.start();
        let effects = local_operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        let Effect::Net(NetEffect::AuditPage(effect)) = &effects[0] else {
            panic!("local page must precede peer fan-out");
        };
        assert_eq!(effect.nodes.len(), MAX_AUDIT_PEERS);

        let mut remote_operation = ListAuditOperation::new(
            realm_id,
            local,
            false,
            Ulid::from_bytes([2u8; 16]),
            None,
            peers,
            None,
            10,
            None,
            [0u8; 32],
        );
        let effects = remote_operation.start();
        let Effect::Net(NetEffect::AuditPage(effect)) = &effects[0] else {
            panic!("remote page must fan out directly");
        };
        assert_eq!(effect.nodes.len(), MAX_AUDIT_PEERS);
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
        operation.start();
        let record = record(group, doc, realm_id);
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: vec![(
                metadata_audit_key(group, doc, Ulid::from_bytes([4u8; 16])),
                postcard::to_allocvec(&record).unwrap().into(),
            )],
            next_start_after: None,
        }));
        let mut batch = AuditPageBatch::new();
        batch.completed_nodes.extend(peers);
        operation.step(Event::Net(NetEvent::AuditPages(batch)));
        let result = operation.finalize().unwrap();
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
        );
        let effects = operation.start();
        let Effect::Storage(StorageEffect::Iter { limit, .. }) = &effects[0] else {
            panic!("local page must use storage iteration");
        };
        assert_eq!(*limit, MAX_AUDIT_PAGE_SIZE);
    }

    #[test]
    fn local_abort_partial() {
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
        operation.start();
        operation.abort();
        operation.abort();
        let result = operation.finalize().unwrap();
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
            task_handle: None,
            compute_handle: None,
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

    #[tokio::test]
    async fn local_deadline() {
        let directory = tempdir().unwrap();
        let context = DriverContext {
            storage_handle: FjallStorage::open(directory.path().to_str().unwrap()).unwrap(),
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        };
        let node = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
        let operation = ListAuditOperation::new(
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
        let result = drive_until(
            operation,
            &context,
            tokio::time::Instant::now() - Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert!(result.partial);
        assert!(result.next_cursor.is_none());
        assert_eq!(result.missing_nodes, vec![node]);
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
        operation.start();
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
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
        operation.step(Event::Net(NetEvent::AuditPages(batch)));

        let aggregate = operation.finalize().unwrap();
        assert!(!aggregate.partial);
        assert!(aggregate.next_cursor.is_some());
    }

    #[test]
    fn silent_peer_missing() {
        // A peer the adapter did not answer for must still be reported missing
        // instead of leaving the page looking complete.
        let peers: Vec<aruna_core::types::NodeId> = (4u8..=5)
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

        operation.start();
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        let mut batch = AuditPageBatch::new();
        batch.completed_nodes.insert(peers[0]);
        operation.step(Event::Net(NetEvent::AuditPages(batch)));

        let aggregate = operation.finalize().unwrap();
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
        operation.start();
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        let mut first_batch = batch(live, realm_id, group, Some(doc), live_page.clone());
        first_batch.mark_missing(missing);
        operation.step(Event::Net(NetEvent::AuditPages(first_batch)));

        let partial = operation.finalize().unwrap();
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
        retry.start();
        retry.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
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
        retry.step(Event::Net(NetEvent::AuditPages(retry_batch)));

        let recovered = retry.finalize().unwrap();
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
                &"a".repeat(MAX_AUDIT_CURSOR_CHARS + 1),
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
        let peers = (2u8..=u8::try_from(MAX_AUDIT_PEERS * 2 + 3).unwrap())
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
            Some(MetadataAuthToken::bearer("test").unwrap()),
            [3u8; 32],
        );
        operation.start();
        operation.step(Event::Net(NetEvent::AuditPages(AuditPageBatch::new())));
        let result = operation.finalize().unwrap();
        assert!(result.partial);
        assert!(result.next_cursor.is_none());
        assert!(result.missing_overflow > 0);
        assert!(result.missing_nodes.len() <= MAX_AUDIT_PEERS);
    }

    #[test]
    fn rejects_peer_first() {
        let node = iroh::SecretKey::from_bytes(&[1u8; 32]).public();
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
        operation.start();
        operation.step(Event::Net(NetEvent::AuditPages(AuditPageBatch::new())));
        assert!(operation.finalize().is_err());
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
        let operation = ListAuditOperation::new(
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
        let effects = operation.start();
        assert!(matches!(
            effects.as_slice(),
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
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        });
        let realm_id = RealmId([8u8; 32]);
        let group_id = Ulid::from_bytes([9u8; 16]);
        let user_id = UserId::local(Ulid::from_bytes([10u8; 16]), realm_id);
        let actor = Actor {
            node_id: iroh::SecretKey::from_bytes(&[11u8; 32]).public(),
            user_id,
            realm_id,
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
        let mut group_auth =
            GroupAuthorizationDocument::new_default_group_doc(user_id, realm_id, group_id);
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
        operation.start();
        operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        operation.step(Event::Net(NetEvent::AuditPages(batch(
            node,
            realm_id,
            group,
            Some(doc),
            page,
        ))));
        let aggregate = operation.finalize().unwrap();
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
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
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
            let effect =
                write_audit_effect(&record(group_id, document_id, realm_id), audit_id, None)
                    .unwrap();
            context.storage_handle.send_effect(effect).await;
        }

        let first = crate::driver::drive(
            LocalAuditPageOperation::new(realm_id, group_id, None, None, 2),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(first.records.len(), 2);
        let cursor = first.next_start_after.expect("more records");

        let second = crate::driver::drive(
            LocalAuditPageOperation::new(realm_id, group_id, None, Some(cursor), 2),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(second.records.len(), 1);
        assert_eq!(second.records[0].record.document_id, second_doc);

        let filtered = crate::driver::drive(
            LocalAuditPageOperation::new(realm_id, group_id, Some(second_doc), None, 50),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(filtered.records.len(), 1);

        let foreign = crate::driver::drive(
            LocalAuditPageOperation::new(realm_id, Ulid::from_bytes([99u8; 16]), None, None, 50),
            &context,
        )
        .await
        .unwrap();
        assert!(foreign.records.is_empty());
    }
}
