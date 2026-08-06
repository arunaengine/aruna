//! Read surface over the metadata audit trail. Audit rows are node-local
//! projections, so a group's complete trail is gathered by asking every eligible
//! realm node for its local page and merging the pages by their raw storage key.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use aruna_core::audit::{AuditPageEntry, AuditPageRequest, AuditPageResponse};
use aruna_core::effects::{AuditPageEffect, Effect, IterStart, NetEffect, StorageEffect};
use aruna_core::events::{AuditPageEvent, Event, NetEvent, StorageEvent};
use aruna_core::keyspaces::METADATA_AUDIT_KEYSPACE;
use aruna_core::metadata::MetadataAuthToken;
use aruna_core::operation::Operation;
use aruna_core::structs::{AuthContext, MetadataAuditRecord, Permission, RealmId};
use aruna_core::types::{Effects, GroupId, Key, NodeId, Value};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use ulid::Ulid;

use super::api::load_realm_config;
use super::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::driver::{DriverContext, drive};
use crate::request_authorization::{AuthorizeError, authorize};
use crate::request_policy::PolicyRequestExtras;

pub const MAX_AUDIT_PAGE_SIZE: usize = 200;
pub const DEFAULT_AUDIT_PAGE_SIZE: usize = 50;

#[derive(Debug, Clone)]
pub struct ListAuditRequest {
    pub group_id: GroupId,
    pub document_id: Option<Ulid>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

/// A merged audit page. `partial` is set with `missing_nodes` when a required
/// node could not be reached or attested, so a gap is never a silent 200.
#[derive(Debug, Clone, PartialEq)]
pub struct AuditAggregate {
    pub records: Vec<MetadataAuditRecord>,
    pub next_cursor: Option<String>,
    pub partial: bool,
    pub missing_nodes: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ListAuditError {
    #[error("invalid audit cursor")]
    InvalidCursor,
    #[error("audit read failed: {0}")]
    Storage(String),
}

#[derive(Serialize, Deserialize)]
struct AuditCursor {
    key: Vec<u8>,
}

fn encode_cursor(key: Vec<u8>) -> String {
    let bytes = postcard::to_allocvec(&AuditCursor { key }).unwrap_or_default();
    URL_SAFE_NO_PAD.encode(bytes)
}

fn decode_cursor(cursor: &str) -> Result<Vec<u8>, ListAuditError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| ListAuditError::InvalidCursor)?;
    let cursor: AuditCursor =
        postcard::from_bytes(&bytes).map_err(|_| ListAuditError::InvalidCursor)?;
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
    Ok(AuditPageResponse {
        records,
        next_start_after: next_start_after.map(|key| key.to_vec()),
    })
}

/// Merges node pages into one deterministic page. Records are deduplicated by
/// their raw key; the horizon is the smallest last-key of any truncated page, so
/// only keys every node has fully covered are emitted before the next request.
fn merge_pages(
    pages: &[AuditPageResponse],
    limit: usize,
) -> (Vec<MetadataAuditRecord>, Option<Vec<u8>>) {
    let mut horizon: Option<Vec<u8>> = None;
    let mut any_truncated = false;
    for page in pages {
        if page.next_start_after.is_some() {
            any_truncated = true;
            if let Some(last) = page.records.last() {
                horizon = Some(match horizon {
                    Some(current) => current.min(last.key.clone()),
                    None => last.key.clone(),
                });
            }
        }
    }

    let mut by_key: BTreeMap<Vec<u8>, MetadataAuditRecord> = BTreeMap::new();
    for page in pages {
        for entry in &page.records {
            if horizon.as_ref().is_some_and(|bound| &entry.key > bound) {
                continue;
            }
            by_key
                .entry(entry.key.clone())
                .or_insert_with(|| entry.record.clone());
        }
    }

    let candidates: Vec<(Vec<u8>, MetadataAuditRecord)> = by_key.into_iter().collect();
    if candidates.len() > limit {
        let cutoff = candidates[limit - 1].0.clone();
        let records = candidates
            .into_iter()
            .take(limit)
            .map(|(_, record)| record)
            .collect();
        (records, Some(cutoff))
    } else if any_truncated && horizon.is_some() {
        let records = candidates.into_iter().map(|(_, record)| record).collect();
        (records, horizon)
    } else {
        let records = candidates.into_iter().map(|(_, record)| record).collect();
        (records, None)
    }
}

/// One node's local audit page as a sans-I/O operation: it emits a single
/// keyspace scan and returns the raw page. The serving node runs it to answer a
/// peer, and the aggregator reuses the same scan for its own local page.
#[derive(Debug, PartialEq)]
pub struct LocalAuditPageOperation {
    group_id: GroupId,
    document_id: Option<Ulid>,
    start_after: Option<Vec<u8>>,
    limit: usize,
    done: bool,
    output: Option<Result<AuditPageResponse, ListAuditError>>,
}

impl LocalAuditPageOperation {
    pub fn new(
        group_id: GroupId,
        document_id: Option<Ulid>,
        start_after: Option<Vec<u8>>,
        limit: usize,
    ) -> Self {
        Self {
            group_id,
            document_id,
            start_after,
            limit,
            done: false,
            output: None,
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
            }) => parse_audit_page(values, next_start_after),
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

/// Gathers a group's audit trail across realm nodes as a sans-I/O operation: it
/// scans the local keyspace and emits one audit-page request per peer, then
/// merges the pages. Node selection is supplied by the caller.
#[derive(Debug, PartialEq)]
pub struct ListAuditOperation {
    group_id: GroupId,
    document_id: Option<Ulid>,
    peers: Vec<NodeId>,
    start_after: Option<Vec<u8>>,
    limit: usize,
    auth_token: Option<MetadataAuthToken>,
    config_digest: [u8; 32],
    state: FanState,
    pending: usize,
    local_done: bool,
    responded: BTreeSet<NodeId>,
    pages: Vec<AuditPageResponse>,
    missing: Vec<NodeId>,
    error: Option<ListAuditError>,
}

impl ListAuditOperation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        group_id: GroupId,
        document_id: Option<Ulid>,
        peers: Vec<NodeId>,
        start_after: Option<Vec<u8>>,
        limit: usize,
        auth_token: Option<MetadataAuthToken>,
        config_digest: [u8; 32],
    ) -> Self {
        Self {
            group_id,
            document_id,
            peers,
            start_after,
            limit,
            auth_token,
            config_digest,
            state: FanState::Collecting,
            pending: 0,
            local_done: false,
            responded: BTreeSet::new(),
            pages: Vec::new(),
            missing: Vec::new(),
            error: None,
        }
    }

    fn accept_peer(&mut self, node: NodeId) -> bool {
        self.peers.contains(&node) && self.responded.insert(node)
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
        let mut effects: Effects = smallvec![audit_iter_effect(
            self.group_id,
            self.document_id,
            &self.start_after,
            self.limit
        )];
        self.pending = 1;
        if self.peers.is_empty() {
            return effects;
        }
        // One fan-out effect: the adapter asks every peer concurrently, so an
        // unreachable node costs its own timeout, not the sum of them.
        effects.push(Effect::Net(NetEffect::AuditPage(Box::new(
            AuditPageEffect {
                nodes: self.peers.clone(),
                request: AuditPageRequest {
                    auth_token: self.auth_token.clone(),
                    config_digest: self.config_digest,
                    group_id: self.group_id,
                    document_id: self.document_id,
                    start_after: self.start_after.clone(),
                    limit: self.limit,
                },
            },
        ))));
        self.pending += 1;
        effects
    }

    fn step(&mut self, event: Event) -> Effects {
        if self.state != FanState::Collecting {
            return self.fail("audit event after aggregation completed");
        }
        match event {
            Event::Storage(StorageEvent::IterResult {
                values,
                next_start_after,
            }) => {
                if self.local_done {
                    return self.fail("duplicate local audit page");
                }
                match parse_audit_page(values, next_start_after) {
                    Ok(page) => self.pages.push(page),
                    Err(error) => {
                        self.error = Some(error);
                        self.state = FanState::Done;
                        return smallvec![];
                    }
                }
                self.local_done = true;
                self.pending -= 1;
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.error = Some(ListAuditError::Storage(error.to_string()));
                self.state = FanState::Done;
                return smallvec![];
            }
            Event::Net(NetEvent::AuditPages(pages)) => {
                for page in pages {
                    match page {
                        AuditPageEvent::Page { node, response } => {
                            if !self.accept_peer(node) {
                                return self.fail("unexpected audit peer response");
                            }
                            self.pages.push(*response);
                        }
                        AuditPageEvent::Unavailable { node, .. } => {
                            if !self.accept_peer(node) {
                                return self.fail("unexpected audit peer response");
                            }
                            self.missing.push(node);
                        }
                    }
                }
                // Every configured peer must be accounted for before the merge
                // can claim completeness.
                let unanswered: Vec<NodeId> = self
                    .peers
                    .iter()
                    .copied()
                    .filter(|node| !self.responded.contains(node))
                    .collect();
                for node in unanswered {
                    self.responded.insert(node);
                    self.missing.push(node);
                }
                self.pending -= 1;
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
        let (records, cutoff) = merge_pages(&self.pages, self.limit);
        let mut missing = self.missing;
        missing.sort_by_key(|node| node.to_string());
        let partial = !missing.is_empty();
        Ok(AuditAggregate {
            records,
            next_cursor: if partial {
                None
            } else {
                cutoff.map(encode_cursor)
            },
            partial,
            missing_nodes: missing,
        })
    }

    fn abort(&mut self) -> Effects {
        let pending: Vec<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|node| !self.responded.contains(node))
            .collect();
        self.missing.extend(pending);
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
) -> Result<AuditAggregate, ListAuditError> {
    let start_after = request.cursor.as_deref().map(decode_cursor).transpose()?;
    let limit = request
        .limit
        .unwrap_or(DEFAULT_AUDIT_PAGE_SIZE)
        .clamp(1, MAX_AUDIT_PAGE_SIZE);

    // The completeness set is the realm's configured sync-eligible membership,
    // not who is currently reachable: an offline configured holder must surface
    // as a missing node with partial results rather than being silently dropped.
    let mut partial = false;
    let mut peers: Vec<NodeId> = Vec::new();
    let mut config_digest = [0u8; 32];
    match load_realm_config(context, realm_id).await {
        None => partial = true,
        Some(config) => match config.sync_eligible_node_ids() {
            Err(_) => partial = true,
            Ok(nodes) => {
                peers = nodes
                    .into_iter()
                    .filter(|node| *node != local_node)
                    .collect();
                if !peers.is_empty() {
                    match config.digest() {
                        Ok(digest) => config_digest = digest,
                        Err(_) => {
                            partial = true;
                            peers.clear();
                        }
                    }
                }
            }
        },
    }

    let operation = ListAuditOperation::new(
        request.group_id,
        request.document_id,
        peers,
        start_after,
        limit,
        auth_token,
        config_digest,
    );
    let mut aggregate = drive(operation, context).await?;
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
) -> MetadataTransportMessage {
    MetadataTransportMessage::ForwardedAuditPage {
        result: local_audit_result(context, peer, request).await,
    }
}

async fn local_audit_result(
    context: &Arc<DriverContext>,
    peer: NodeId,
    request: AuditPageRequest,
) -> Result<AuditPageResponse, MetadataReadError> {
    let net_handle = context
        .net_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let realm_id = *net_handle.realm_id();
    let config = load_realm_config(context.as_ref(), realm_id)
        .await
        .ok_or(MetadataReadError::Unavailable)?;
    if config.digest().ok() != Some(request.config_digest) {
        return Err(MetadataReadError::Unavailable);
    }
    let metadata = context
        .metadata_handle
        .as_ref()
        .ok_or(MetadataReadError::Unavailable)?;
    let auth = metadata
        .authorize_read_peer(peer, request.auth_token.clone(), false)
        .await?
        .ok_or(MetadataReadError::Unauthorized)?;
    let path = format!("/{realm_id}/g/{}/admin", request.group_id);
    authorize_admin(context, auth, path).await?;

    let operation = LocalAuditPageOperation::new(
        request.group_id,
        request.document_id,
        request.start_after,
        request.limit.clamp(1, MAX_AUDIT_PAGE_SIZE),
    );
    drive(operation, context.as_ref())
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
        AuditPageEntry, AuditPageEvent, AuditPageResponse, Effect, Event, ListAuditOperation,
        LocalAuditPageOperation, MetadataReadError, NetEffect, NetEvent, Operation, StorageEvent,
        authorize_admin, decode_cursor, encode_cursor, merge_pages,
    };
    use crate::driver::DriverContext;
    use crate::metadata::repository::{metadata_audit_key, write_audit_effect};
    use aruna_core::UserId;
    use aruna_core::effects::StorageEffect;
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE};
    use aruna_core::request_policy::{PolicyKind, RequestPolicy};
    use aruna_core::structs::{
        Actor, AuthContext, Group, GroupAuthorizationDocument, RealmAuthorizationDocument,
    };
    use aruna_core::structs::{MetadataAuditOperation, MetadataAuditRecord, RealmId};
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
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

    #[test]
    fn peers_share_effect() {
        // Every peer is asked in one adapter round: with one effect per peer the
        // runner contacts them serially and unreachable nodes add up to a timeout.
        let peers: Vec<aruna_core::types::NodeId> = (1u8..=3)
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let group_id = Ulid::from_bytes([3u8; 16]);
        let mut operation =
            ListAuditOperation::new(group_id, None, peers.clone(), None, 10, None, [0u8; 32]);

        let effects = operation.start();
        assert_eq!(effects.len(), 2);
        let Effect::Net(NetEffect::AuditPage(fan_out)) = &effects[1] else {
            panic!("the peer fan-out must be a single net effect");
        };
        assert_eq!(fan_out.nodes, peers);

        operation.step(Event::Storage(StorageEvent::IterResult {
            values: Vec::new(),
            next_start_after: None,
        }));
        assert!(!operation.is_complete());
        operation.step(Event::Net(NetEvent::AuditPages(
            peers
                .iter()
                .map(|node| AuditPageEvent::Unavailable {
                    node: *node,
                    message: "unreachable".to_string(),
                })
                .collect(),
        )));

        assert!(operation.is_complete());
        let aggregate = operation.finalize().unwrap();
        assert!(aggregate.partial);
        assert_eq!(aggregate.missing_nodes.len(), peers.len());
        assert!(aggregate.records.is_empty());
    }

    #[test]
    fn silent_peer_missing() {
        // A peer the adapter did not answer for must still be reported missing
        // instead of leaving the page looking complete.
        let peers: Vec<aruna_core::types::NodeId> = (4u8..=5)
            .map(|seed| iroh::SecretKey::from_bytes(&[seed; 32]).public())
            .collect();
        let mut operation = ListAuditOperation::new(
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
        operation.step(Event::Net(NetEvent::AuditPages(vec![
            AuditPageEvent::Page {
                node: peers[0],
                response: Box::new(AuditPageResponse {
                    records: Vec::new(),
                    next_start_after: None,
                }),
            },
        ])));

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
        operation.step(Event::Net(NetEvent::AuditPages(vec![
            AuditPageEvent::Page {
                node: live,
                response: Box::new(live_page.clone()),
            },
            AuditPageEvent::Unavailable {
                node: missing,
                message: "unreachable".to_string(),
            },
        ])));

        let partial = operation.finalize().unwrap();
        assert!(partial.partial);
        assert!(partial.next_cursor.is_none());

        let mut retry = ListAuditOperation::new(
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
        retry.step(Event::Net(NetEvent::AuditPages(vec![
            AuditPageEvent::Page {
                node: live,
                response: Box::new(live_page),
            },
            AuditPageEvent::Page {
                node: missing,
                response: Box::new(AuditPageResponse {
                    records: vec![first_entry],
                    next_start_after: None,
                }),
            },
        ])));

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
        let key = vec![7u8, 9, 11];
        let encoded = encode_cursor(key.clone());
        assert_eq!(decode_cursor(&encoded).unwrap(), key);
        assert!(decode_cursor("!!not-base64!!").is_err());
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

        let (records, cutoff) = merge_pages(&[holder_a, holder_b], 10);
        // horizon is holder_a's last key (third); every key <= third is complete.
        assert_eq!(records.len(), 3);
        assert_eq!(cutoff, Some(metadata_audit_key(group, doc, third).to_vec()));
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

        let (records, cutoff) = merge_pages(&[page], 2);
        assert_eq!(records.len(), 2);
        assert_eq!(
            cutoff,
            Some(metadata_audit_key(group, doc, ids[1]).to_vec())
        );
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
            LocalAuditPageOperation::new(group_id, None, None, 2),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(first.records.len(), 2);
        let cursor = first.next_start_after.expect("more records");

        let second = crate::driver::drive(
            LocalAuditPageOperation::new(group_id, None, Some(cursor), 2),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(second.records.len(), 1);
        assert_eq!(second.records[0].record.document_id, second_doc);

        let filtered = crate::driver::drive(
            LocalAuditPageOperation::new(group_id, Some(second_doc), None, 50),
            &context,
        )
        .await
        .unwrap();
        assert_eq!(filtered.records.len(), 1);

        let foreign = crate::driver::drive(
            LocalAuditPageOperation::new(Ulid::from_bytes([99u8; 16]), None, None, 50),
            &context,
        )
        .await
        .unwrap();
        assert!(foreign.records.is_empty());
    }
}
