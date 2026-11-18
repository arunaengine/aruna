//! Serves the realm documents and document graphs a device asks for, and applies its edits.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::str::FromStr;

use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::admin_documents::{AdminDocumentClock, AdminDocumentTarget};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::document::DocumentTarget;
use aruna_core::effects::Effect;
use aruna_core::events::{Event, PeerAuthRefusal};
use aruna_core::keyspaces::DOCUMENT_STATE_KEYSPACE;
use aruna_core::metadata::{
    AuthToken, MetadataEffect, MetadataError, MetadataEvent, graph_version,
};
use aruna_core::node_state::{NodeInfoError, ReadInfoOperation};
use aruna_core::operation::Operation;
use aruna_core::reducer::AdminDocumentState;
use aruna_core::rows::{outside_answer, outside_call, read_row, value, values};
use aruna_core::storage_entries::reducer_state_key;
use aruna_core::structs::SyncRefusal;
use aruna_core::structs::identity::auth::{Actor, AuthContext};
use aruna_core::structs::identity::group::{Group, GroupAuthorizationDocument};
use aruna_core::structs::identity::realm::{
    NodeRealm, RealmConfigDocument, RealmId, RealmNodeKind,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::node_info::NodeInfoDocument;
use aruna_core::types::GroupId;
use craqle::GraphReplicaSnapshot;
use tracing::warn;
use ulid::Ulid;

use crate::auth::error::AuthorizeError;
use crate::document_repository::{
    GetConfigError, GetConfigOperation, GetGroupConfig, GetGroupError, GetGroupOperation,
};
use crate::forward::authorize::{ForwardAuthError, ForwardedCallerOperation, write_check};
use crate::forward::replay::{HeldRecordError, HeldRecordOperation};
use crate::forward::routing::holds_metadata_id;
use crate::forward::transport::reject;
use crate::groups::list_groups::{ListGroupError, ListGroupOperation};
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::readable::RecordReadableOperation;
use crate::metadata::live_records::{LiveRecordOperation, LiveRecordsError};
use crate::metadata::protocol::{
    DeviceGroupDocuments, GraphState, MAX_DEVICE_GROUPS, MetadataTransportMessage, RealmDocuments,
};
use crate::metadata::raw_events::RawReadError;
use crate::metadata::record_reads::RecordMaterializedOperation;
use crate::metadata::revision_read::{MetadataRawView, RawDisplayOperation};
use crate::metadata::update_document::{
    UpdateDocumentConfig, UpdateDocumentError, UpdateDocumentMutation, UpdateDocumentOperation,
};
use crate::metadata::update_entry::UpdateEntryOperation;
use crate::metadata::write_peer::WritePeerOperation;
use crate::realm::load_config::config_unavailable;
use crate::realm::node_name::{NodeDisplays, NodeNameError, ReadNamesOperation};
use crate::realm::peer_trust::{is_sync_eligible, peer_acts_for};
use aruna_core::errors::StorageReadError;

pub(super) const GROUP_SCAN_PAGE: usize = 10_000;

type Document = MetadataRegistryRecord;

fn config_call(realm_id: RealmId) -> IoCall {
    IoCall::child(GetConfigOperation::new(realm_id))
}

fn config_answer(result: IoResult) -> Result<RealmConfigDocument, SyncRefusal> {
    let config = result.child::<RealmConfigDocument, GetConfigError>()?;
    config.map_err(|error| {
        config_unavailable(&error);
        SyncRefusal::Unavailable
    })
}

fn node_ids(ids: NodeRealm) -> Result<(NodeId, RealmId), SyncRefusal> {
    ids.ok_or(SyncRefusal::Unavailable)
}

/// The caller a device's token stands for, once the realm config binds the device to it.
fn peer_caller(
    result: IoResult,
    config: &RealmConfigDocument,
    (peer, realm_id): (NodeId, RealmId),
) -> Result<AuthContext, SyncRefusal> {
    let checked = result.child::<AuthContext, PeerAuthRefusal>()?;
    let auth = checked.map_err(|refusal| match refusal {
        PeerAuthRefusal::Unauthorized => SyncRefusal::Unauthorized,
        PeerAuthRefusal::Unavailable(_) => SyncRefusal::Unavailable,
    })?;
    // The documents are this realm's own; nothing about another realm is served.
    if auth.realm_id != realm_id || !peer_acts_for(config, peer, auth.user_id) {
        return Err(SyncRefusal::Unauthorized);
    }
    Ok(auth)
}

fn metadata_call(effect: MetadataEffect) -> IoCall {
    IoCall::from(Effect::Metadata(effect))
}

fn metadata_event(result: IoResult) -> Result<MetadataEvent, SyncRefusal> {
    match result.event()? {
        Event::Metadata(event) => Ok(event),
        _ => Err(SyncRefusal::Unavailable),
    }
}

/// Which request a device sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ServeKind {
    Documents,
    Graph,
    Batch,
}

/// Answers one device request with its reply message; a refusal travels in the reply.
#[derive(Debug, PartialEq)]
pub(crate) struct ServeDeviceOperation {
    kind: ServeKind,
    call: Result<IoCall, Option<MetadataTransportMessage>>,
}

impl ServeDeviceOperation {
    fn unexpected(kind: ServeKind) -> Self {
        let message = reject("unexpected metadata control message");
        Self {
            kind,
            call: Err(Some(message)),
        }
    }
}

/// Serves the realm-wide documents to one of the realm's devices. A device runs no document
/// sync, so this is how it sees the realm config it is judged by.
pub(crate) fn serve_realm_documents(
    ids: NodeRealm,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> ServeDeviceOperation {
    let MetadataTransportMessage::FetchRealmDocuments { auth_token } = message else {
        return ServeDeviceOperation::unexpected(ServeKind::Documents);
    };
    ServeDeviceOperation {
        kind: ServeKind::Documents,
        call: Ok(IoCall::child(ReadDocumentsOperation::new(
            ids, peer, auth_token,
        ))),
    }
}

/// Serves one document's graph state to a device that keeps a replica of it. The device joins
/// the snapshot locally, so state travels, never authority.
pub(crate) fn serve_graph_state(
    ids: NodeRealm,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> ServeDeviceOperation {
    let MetadataTransportMessage::FetchGraphState {
        auth_token,
        document_id,
    } = message
    else {
        return ServeDeviceOperation::unexpected(ServeKind::Graph);
    };
    ServeDeviceOperation {
        kind: ServeKind::Graph,
        call: Ok(IoCall::child(ReadGraphOperation::new(
            ids,
            peer,
            auth_token,
            document_id,
        ))),
    }
}

/// Applies an edit a device already made on its replica. The batch is appended unchanged as an
/// ordinary update event, so every holder materializes the change set the owner saw.
pub(crate) fn apply_device_batch(
    ids: NodeRealm,
    peer: NodeId,
    message: MetadataTransportMessage,
) -> ServeDeviceOperation {
    ServeDeviceOperation {
        kind: ServeKind::Batch,
        call: Ok(IoCall::child(DeviceBatchOperation::new(ids, peer, message))),
    }
}

impl Operation for ServeDeviceOperation {
    type Output = MetadataTransportMessage;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<MetadataTransportMessage>, IoError> {
        let call = std::mem::replace(&mut self.call, Err(None));
        match call {
            Ok(call) => Ok(Next::Call(call)),
            Err(reply) => Ok(Next::Done(reply.ok_or(IoError::Unfinished)?)),
        }
    }

    fn next(&mut self, result: IoResult) -> Result<Next<MetadataTransportMessage>, IoError> {
        Ok(Next::Done(match self.kind {
            ServeKind::Documents => MetadataTransportMessage::FetchedRealmDocuments {
                result: result.child::<RealmDocuments, SyncRefusal>()?,
            },
            ServeKind::Graph => MetadataTransportMessage::FetchedGraphState {
                result: result.child::<GraphState, SyncRefusal>()?.map(Box::new),
            },
            ServeKind::Batch => MetadataTransportMessage::ForwardedApplyBatch {
                result: result.child::<Document, SyncRefusal>()?.map(Box::new),
            },
        }))
    }
}

/// What the pending call of reading the realm documents answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DocumentsStep {
    Config,
    Peer,
    Rows,
    Infos,
    Names,
    Groups,
    Urls,
    Clock,
}

/// The realm documents for one device. Realm infrastructure only, bound to the device's owner.
#[derive(Debug, PartialEq)]
struct ReadDocumentsOperation {
    ids: NodeRealm,
    peer: NodeId,
    auth_token: Option<AuthToken>,
    step: DocumentsStep,
    config: Option<RealmConfigDocument>,
    user_id: Option<UserId>,
    realm_config: Vec<u8>,
    realm_authorization: Option<Vec<u8>>,
    owner: Option<Vec<u8>>,
    node_infos: Vec<NodeInfoDocument>,
    displays: Option<NodeDisplays>,
    groups: Vec<DeviceGroupDocuments>,
    urls: Vec<String>,
}

type DocumentsTurn = Result<Next<RealmDocuments>, SyncRefusal>;

impl ReadDocumentsOperation {
    fn new(ids: NodeRealm, peer: NodeId, auth_token: AuthToken) -> Self {
        Self {
            ids,
            peer,
            auth_token: Some(auth_token),
            step: DocumentsStep::Config,
            config: None,
            user_id: None,
            realm_config: Vec::new(),
            realm_authorization: None,
            owner: None,
            node_infos: Vec::new(),
            displays: None,
            groups: Vec::new(),
            urls: Vec::new(),
        }
    }

    fn config(&self) -> Result<&RealmConfigDocument, IoError> {
        self.config.as_ref().ok_or(IoError::Unfinished)
    }

    fn configured(&mut self, config: RealmConfigDocument) -> DocumentsTurn {
        // Only realm infrastructure answers: a device holds no realm state to serve.
        if !is_sync_eligible(&config, node_ids(self.ids)?.0) {
            return Err(SyncRefusal::Unavailable);
        }
        self.config = Some(config);
        self.step = DocumentsStep::Peer;
        let token = self.auth_token.take().ok_or(SyncRefusal::Unauthorized)?;
        Ok(Next::Call(IoCall::child(WritePeerOperation::new(
            self.peer, token,
        ))))
    }

    /// The realm config, its authorization, and the token subject's own user document: for a
    /// device that is its owner by the peer check.
    fn read_rows(&mut self, user_id: UserId) -> DocumentsTurn {
        let realm_id = node_ids(self.ids)?.1;
        let row = |target: DocumentTarget| {
            let space = target.storage_keyspace().to_string();
            (space, target.storage_key())
        };
        self.user_id = Some(user_id);
        self.step = DocumentsStep::Rows;
        Ok(Next::Call(IoCall::Read(vec![
            row(DocumentTarget::RealmConfig { realm_id }),
            row(DocumentTarget::RealmAuthorization { realm_id }),
            row(DocumentTarget::User { user_id }),
        ])))
    }

    fn management_call(&self) -> Result<IoCall, IoError> {
        let nodes = (self.config()?.nodes.iter())
            .filter(|node| matches!(node.kind, RealmNodeKind::Management))
            .filter_map(|node| NodeId::from_str(&node.node_id).ok())
            .collect();
        Ok(IoCall::child(ManagementUrlsOperation::new(nodes)))
    }

    fn finish(&mut self, clock: AdminDocumentClock) -> DocumentsTurn {
        let displays = self.displays.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Done(RealmDocuments {
            realm_config: std::mem::take(&mut self.realm_config),
            realm_authorization: self.realm_authorization.take(),
            owner: self.owner.take(),
            groups: std::mem::take(&mut self.groups),
            node_infos: std::mem::take(&mut self.node_infos),
            management_urls: std::mem::take(&mut self.urls),
            clock,
            node_names: displays.names,
            node_places: displays.places,
        }))
    }
}

impl Operation for ReadDocumentsOperation {
    type Output = RealmDocuments;
    type Error = SyncRefusal;

    fn first(&mut self) -> DocumentsTurn {
        Ok(Next::Call(config_call(node_ids(self.ids)?.1)))
    }

    fn next(&mut self, result: IoResult) -> DocumentsTurn {
        let (_, realm_id) = node_ids(self.ids)?;
        match self.step {
            DocumentsStep::Config => self.configured(config_answer(result)?),
            DocumentsStep::Peer => {
                let scope = (self.peer, realm_id);
                let auth = peer_caller(result, self.config()?, scope)?;
                self.read_rows(auth.user_id)
            }
            DocumentsStep::Rows => {
                let [config, authorization, owner] = values::<3>(result)?;
                let bytes = |row: aruna_core::types::Value| row.as_ref().to_vec();
                self.realm_config = bytes(config.ok_or(SyncRefusal::NotFound)?);
                self.realm_authorization = authorization.map(bytes);
                self.owner = owner.map(bytes);
                let nodes =
                    (self.config()?.sync_eligible_nodes()).map_err(|_| SyncRefusal::Unavailable)?;
                self.step = DocumentsStep::Infos;
                Ok(Next::Call(IoCall::child(ReadInfoOperation::new(nodes))))
            }
            DocumentsStep::Infos => {
                let infos = result.child::<NodeInfos, NodeInfoError>()?;
                let infos = infos.map_err(|error| {
                    warn!(%error, "Failed to read the node info documents for a device");
                    SyncRefusal::Unavailable
                })?;
                self.node_infos = infos.into_values().collect();
                self.step = DocumentsStep::Names;
                let names = ReadNamesOperation::new(realm_id);
                Ok(Next::Call(IoCall::child(names)))
            }
            DocumentsStep::Names => {
                let names = result.child::<NodeDisplays, NodeNameError>()?;
                self.displays = Some(names.map_err(|error| {
                    warn!(%error, "Failed to read the node display names for a device");
                    SyncRefusal::Unavailable
                })?);
                let user_id = self.user_id.ok_or(IoError::Unfinished)?;
                self.step = DocumentsStep::Groups;
                Ok(Next::Call(IoCall::child(DeviceGroupsOperation::new(
                    user_id,
                ))))
            }
            DocumentsStep::Groups => {
                self.groups = result.child::<Vec<DeviceGroupDocuments>, IoError>()??;
                self.step = DocumentsStep::Urls;
                Ok(Next::Call(self.management_call()?))
            }
            DocumentsStep::Urls => {
                self.urls = result.child::<Vec<String>, IoError>()??;
                self.step = DocumentsStep::Clock;
                let target = AdminDocumentTarget::RealmConfig { realm_id };
                let key = reducer_state_key(&target);
                Ok(Next::Call(outside_call(read_row(
                    DOCUMENT_STATE_KEYSPACE,
                    key,
                ))))
            }
            // What this node applied to the realm configuration, as the reducer keeps it. A
            // device compares it with its own copy's and never accepts less.
            DocumentsStep::Clock => {
                let state = outside_answer(result).and_then(value).ok().flatten();
                let state =
                    state.and_then(|bytes| postcard::from_bytes::<AdminDocumentState>(&bytes).ok());
                self.finish(state.map(|state| state.clock).unwrap_or_default())
            }
        }
    }
}

type NodeInfos = BTreeMap<NodeId, NodeInfoDocument>;

/// The api urls the realm's management nodes published, in node-id order so repeated answers
/// pin the same peer. A device relays its management-only routes to these.
#[derive(Debug, PartialEq)]
struct ManagementUrlsOperation {
    nodes: Vec<NodeId>,
}

impl ManagementUrlsOperation {
    fn new(nodes: Vec<NodeId>) -> Self {
        Self { nodes }
    }
}

impl Operation for ManagementUrlsOperation {
    type Output = Vec<String>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Vec<String>>, IoError> {
        let read = ReadInfoOperation::new(std::mem::take(&mut self.nodes));
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Vec<String>>, IoError> {
        let documents = match result.child::<NodeInfos, NodeInfoError>()? {
            Ok(documents) => documents,
            Err(error) => {
                warn!(%error, "Failed to read the management urls for a device");
                return Ok(Next::Done(Vec::new()));
            }
        };
        let mut urls: Vec<String> = Vec::new();
        for url in documents.values().filter_map(|info| info.urls.api.clone()) {
            if !urls.contains(&url) {
                urls.push(url);
            }
        }
        Ok(Next::Done(urls))
    }
}

/// The caller's own groups, as the documents this node stores. A list that fails yields
/// nothing: one unreadable group must not cost the device the realm configuration.
#[derive(Debug, PartialEq)]
pub(super) struct DeviceGroupsOperation {
    user_id: UserId,
    offset: usize,
    page_len: usize,
    queue: VecDeque<GroupId>,
    asked: Option<GroupId>,
    documents: Vec<DeviceGroupDocuments>,
}

type GroupsTurn = Result<Next<Vec<DeviceGroupDocuments>>, IoError>;

impl DeviceGroupsOperation {
    pub(super) fn new(user_id: UserId) -> Self {
        Self {
            user_id,
            offset: 0,
            page_len: 0,
            queue: VecDeque::new(),
            asked: None,
            documents: Vec::new(),
        }
    }

    fn page(&self) -> Next<Vec<DeviceGroupDocuments>> {
        let list = ListGroupOperation::with_pagination(GROUP_SCAN_PAGE, self.offset);
        Next::Call(IoCall::child(list))
    }

    fn finish(&mut self) -> GroupsTurn {
        Ok(Next::Done(std::mem::take(&mut self.documents)))
    }

    fn advance(&mut self) -> GroupsTurn {
        if let Some(group_id) = self.queue.pop_front() {
            self.asked = Some(group_id);
            let read = GetGroupOperation::new(GetGroupConfig { group_id });
            return Ok(Next::Call(IoCall::child(read)));
        }
        self.asked = None;
        if self.page_len < GROUP_SCAN_PAGE {
            return self.finish();
        }
        self.offset = self.offset.saturating_add(self.page_len);
        Ok(self.page())
    }
}

impl Operation for DeviceGroupsOperation {
    type Output = Vec<DeviceGroupDocuments>;
    type Error = IoError;

    fn first(&mut self) -> GroupsTurn {
        Ok(self.page())
    }

    fn next(&mut self, result: IoResult) -> GroupsTurn {
        let Some(group_id) = self.asked else {
            let groups = match result.child::<Vec<Group>, ListGroupError>()? {
                Ok(groups) => groups,
                Err(error) => {
                    warn!(%error, "Failed to list groups for a device");
                    return Ok(Next::Done(Vec::new()));
                }
            };
            self.page_len = groups.len();
            self.queue = groups.into_iter().map(|group| group.group_id).collect();
            return self.advance();
        };
        let read = result.child::<(Group, GroupAuthorizationDocument), GetGroupError>()?;
        let Ok((group, authorization)) = read else {
            warn!(%group_id, "Failed to read a group for a device");
            return self.advance();
        };
        if holds_any_role(&authorization, self.user_id) {
            self.documents.push(DeviceGroupDocuments {
                group,
                authorization,
            });
            if self.documents.len() >= MAX_DEVICE_GROUPS {
                return self.finish();
            }
        }
        self.advance()
    }
}

/// Whether the owner holds a role in this group. A device caches its owner's groups only; a
/// realm-scale group list is not theirs to hold.
fn holds_any_role(authorization: &GroupAuthorizationDocument, user_id: UserId) -> bool {
    (authorization.roles.values()).any(|role| role.assigned_users.contains(&user_id))
}

fn sync_refusal(error: MetadataApiError) -> SyncRefusal {
    match error {
        MetadataApiError::Unauthorized => SyncRefusal::Unauthorized,
        MetadataApiError::Forbidden => SyncRefusal::Forbidden,
        MetadataApiError::NotFound => SyncRefusal::NotFound,
        _ => SyncRefusal::Unavailable,
    }
}

fn live_answer(result: IoResult) -> Result<Document, SyncRefusal> {
    match result.child::<Option<Document>, LiveRecordsError>()? {
        Ok(Some(record)) => Ok(record),
        Ok(None) => Err(SyncRefusal::NotFound),
        Err(_) => Err(SyncRefusal::Unavailable),
    }
}

/// What the pending call of reading a graph answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GraphStep {
    Config,
    Peer,
    Live,
    Readable,
    Snapshot,
    Raw,
    Materialized,
    Versioned,
    Current,
}

/// One document's graph state for a device. Only a holder answers, for the owner the realm
/// config binds the device to.
#[derive(Debug, PartialEq)]
struct ReadGraphOperation {
    ids: NodeRealm,
    peer: NodeId,
    auth_token: Option<AuthToken>,
    document_id: Ulid,
    step: GraphStep,
    config: Option<RealmConfigDocument>,
    auth: Option<AuthContext>,
    record: Option<Document>,
    snapshot: Option<GraphReplicaSnapshot>,
    scaffold: Option<String>,
}

type GraphTurn = Result<Next<GraphState>, SyncRefusal>;

impl ReadGraphOperation {
    fn new(ids: NodeRealm, peer: NodeId, auth_token: AuthToken, document_id: Ulid) -> Self {
        Self {
            ids,
            peer,
            auth_token: Some(auth_token),
            document_id,
            step: GraphStep::Config,
            config: None,
            auth: None,
            record: None,
            snapshot: None,
            scaffold: None,
        }
    }

    fn record(&self) -> Result<&Document, IoError> {
        self.record.as_ref().ok_or(IoError::Unfinished)
    }

    fn live_call(&mut self, step: GraphStep) -> GraphTurn {
        self.step = step;
        let live = LiveRecordOperation::new(self.document_id);
        Ok(Next::Call(IoCall::child(live)))
    }

    fn finish(
        &mut self,
        displayed_jsonld: String,
        dataset_digest: Option<[u8; 32]>,
        findings: u32,
    ) -> GraphTurn {
        Ok(Next::Done(GraphState {
            record: self.record.take().ok_or(IoError::Unfinished)?,
            snapshot: self.snapshot.take().ok_or(IoError::Unfinished)?,
            displayed_jsonld,
            dataset_digest,
            findings,
        }))
    }

    /// A document created as a scaffold has no authored text yet; its graph is what it shows.
    fn raw_read(&mut self, raw: Option<MetadataRawView>) -> GraphTurn {
        let Some(raw) = raw else {
            self.step = GraphStep::Materialized;
            let check = RecordMaterializedOperation::new(self.record()?);
            return Ok(Next::Call(IoCall::child(check)));
        };
        let revision = raw.revision;
        let findings = revision.merged.map_or(0, |merged| merged.findings);
        self.finish(revision.jsonld, revision.dataset_digest, findings)
    }

    /// Anything that may have moved between the snapshot and the render is refused, so the
    /// device asks again.
    fn versioned(&mut self, event: MetadataEvent) -> GraphTurn {
        let MetadataEvent::VersionedExport {
            jsonld, version, ..
        } = event
        else {
            return Err(SyncRefusal::Unavailable);
        };
        let snapshot = self.snapshot.as_ref().ok_or(IoError::Unfinished)?;
        if version != graph_version(&snapshot.clock) {
            return Err(SyncRefusal::Unavailable);
        }
        self.scaffold = Some(jsonld);
        self.live_call(GraphStep::Current)
    }
}

impl Operation for ReadGraphOperation {
    type Output = GraphState;
    type Error = SyncRefusal;

    fn first(&mut self) -> GraphTurn {
        Ok(Next::Call(config_call(node_ids(self.ids)?.1)))
    }

    fn next(&mut self, result: IoResult) -> GraphTurn {
        let (node_id, realm_id) = node_ids(self.ids)?;
        match self.step {
            GraphStep::Config => {
                let config = config_answer(result)?;
                if !holds_metadata_id(&config, realm_id, node_id, self.document_id) {
                    return Err(SyncRefusal::Unavailable);
                }
                self.config = Some(config);
                self.step = GraphStep::Peer;
                let token = self.auth_token.take().ok_or(SyncRefusal::Unauthorized)?;
                Ok(Next::Call(IoCall::child(WritePeerOperation::new(
                    self.peer, token,
                ))))
            }
            GraphStep::Peer => {
                let config = self.config.as_ref().ok_or(IoError::Unfinished)?;
                self.auth = Some(peer_caller(result, config, (self.peer, realm_id))?);
                self.live_call(GraphStep::Live)
            }
            GraphStep::Live => {
                let record = live_answer(result)?;
                let auth = self.auth.clone();
                let check = RecordReadableOperation::new(realm_id, auth, record.clone());
                self.record = Some(record);
                self.step = GraphStep::Readable;
                Ok(Next::Call(IoCall::child(check)))
            }
            GraphStep::Readable => {
                result
                    .child::<(), MetadataApiError>()?
                    .map_err(sync_refusal)?;
                let graph_iri = self.record()?.graph_iri.clone();
                self.step = GraphStep::Snapshot;
                let snapshot = MetadataEffect::GraphSnapshot { graph_iri };
                Ok(Next::Call(metadata_call(snapshot)))
            }
            GraphStep::Snapshot => match metadata_event(result)? {
                MetadataEvent::GraphSnapshotResult { snapshot, .. } => {
                    self.snapshot = Some(*snapshot);
                    self.step = GraphStep::Raw;
                    let raw = RawDisplayOperation::new(self.document_id);
                    Ok(Next::Call(IoCall::child(raw)))
                }
                other => {
                    let document_id = self.document_id;
                    warn!(%document_id, event = ?other, "Could not snapshot a graph for a device");
                    Err(SyncRefusal::Unavailable)
                }
            },
            GraphStep::Raw => {
                let raw = result.child::<Option<MetadataRawView>, RawReadError>()?;
                self.raw_read(raw.map_err(|_| SyncRefusal::Unavailable)?)
            }
            GraphStep::Materialized => {
                let ready = result.child::<bool, StorageReadError>()?;
                if !ready.map_err(|_| SyncRefusal::Unavailable)? {
                    return Err(SyncRefusal::Unavailable);
                }
                self.step = GraphStep::Versioned;
                let export = MetadataEffect::ExportVersioned {
                    graph_iri: self.record()?.graph_iri.clone(),
                };
                Ok(Next::Call(metadata_call(export)))
            }
            GraphStep::Versioned => self.versioned(metadata_event(result)?),
            GraphStep::Current => {
                let current = live_answer(result)?;
                if current.last_event_id != self.record()?.last_event_id {
                    return Err(SyncRefusal::Unavailable);
                }
                let jsonld = self.scaffold.take().ok_or(IoError::Unfinished)?;
                let digest =
                    (craqle::canonicalize_jsonld(&jsonld).ok()).map(|canonical| canonical.digest);
                self.finish(jsonld, digest, 0)
            }
        }
    }
}

fn caller_refusal(error: ForwardAuthError) -> SyncRefusal {
    match error {
        ForwardAuthError::Unauthorized => SyncRefusal::Unauthorized,
        ForwardAuthError::Forbidden => SyncRefusal::Forbidden,
        ForwardAuthError::Unavailable(_) => SyncRefusal::Unavailable,
    }
}

/// What the pending call of applying a device batch answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BatchStep {
    Config,
    Caller,
    Held,
    Write,
    Dots,
    Update,
}

/// Checks and applies one edit a device made on its replica.
#[derive(Debug, PartialEq)]
struct DeviceBatchOperation {
    ids: NodeRealm,
    peer: NodeId,
    message: MetadataTransportMessage,
    step: BatchStep,
    config: Option<RealmConfigDocument>,
    auth: Option<AuthContext>,
    record: Option<Document>,
    dots: VecDeque<([u8; 32], u64)>,
}

type BatchTurn = Result<Next<Document>, SyncRefusal>;

impl DeviceBatchOperation {
    fn new(ids: NodeRealm, peer: NodeId, message: MetadataTransportMessage) -> Self {
        Self {
            ids,
            peer,
            message,
            step: BatchStep::Config,
            config: None,
            auth: None,
            record: None,
            dots: VecDeque::new(),
        }
    }

    fn record(&self) -> Result<&Document, IoError> {
        self.record.as_ref().ok_or(IoError::Unfinished)
    }

    fn held(&mut self) -> BatchTurn {
        let MetadataTransportMessage::ForwardApplyBatch {
            config_digest,
            document_id,
            ..
        } = &self.message
        else {
            return Err(SyncRefusal::Invalid(
                "unexpected metadata control message".to_string(),
            ));
        };
        let config = self.config.as_ref().ok_or(IoError::Unfinished)?;
        if config.digest().ok() != Some(*config_digest) {
            return Err(SyncRefusal::Unavailable);
        }
        let node_id = node_ids(self.ids)?.0;
        let held = HeldRecordOperation::new(config.clone(), node_id, *document_id);
        self.step = BatchStep::Held;
        Ok(Next::Call(IoCall::child(held)))
    }

    /// The batch names the graph it was planned against; another graph's change set must never
    /// be applied here.
    fn checked_graph(&mut self, record: Document) -> BatchTurn {
        let MetadataTransportMessage::ForwardApplyBatch { batch, .. } = &self.message else {
            return Err(IoError::Unfinished.into());
        };
        if batch.graph_iri != record.graph_iri {
            return Err(SyncRefusal::Invalid(
                "the batch was planned against another document".to_string(),
            ));
        }
        self.dots = (batch.base_clock.0.iter())
            .map(|(actor, counter)| (actor.0, *counter))
            .collect();
        let auth = self.auth.as_ref().ok_or(IoError::Unfinished)?;
        let check = write_check(auth, &record.permission_path);
        self.record = Some(record);
        self.step = BatchStep::Write;
        Ok(Next::Call(IoCall::child(check)))
    }

    /// An edit whose dependencies never reached this holder would wait in its log forever; the
    /// device retries it instead, or learns the dependency was refused.
    fn next_dot(&mut self) -> BatchTurn {
        let Some((actor, counter)) = self.dots.pop_front() else {
            return self.update();
        };
        self.step = BatchStep::Dots;
        Ok(Next::Call(metadata_call(MetadataEffect::ContainsDot {
            graph_iri: self.record()?.graph_iri.clone(),
            actor,
            counter,
        })))
    }

    fn update(&mut self) -> BatchTurn {
        let (node_id, realm_id) = node_ids(self.ids)?;
        let record = self.record()?;
        let (group_id, public) = (record.group_id, record.public);
        let user_id = self.auth.as_ref().ok_or(IoError::Unfinished)?.user_id;
        let placeholder = MetadataTransportMessage::Reject(String::new());
        let MetadataTransportMessage::ForwardApplyBatch {
            document_id,
            batch,
            authored,
            ..
        } = std::mem::replace(&mut self.message, placeholder)
        else {
            return Err(IoError::Unfinished.into());
        };
        let operation = UpdateDocumentOperation::new(UpdateDocumentConfig {
            actor: Actor {
                node_id,
                user_id,
                realm_id,
            },
            group_id,
            document_id,
            public,
            mutation: UpdateDocumentMutation::ApplyBatch { batch, authored },
            expected_revision: None,
        });
        self.step = BatchStep::Update;
        let update = UpdateEntryOperation::new(operation, Some(node_id));
        Ok(Next::Call(IoCall::child(update)))
    }
}

impl Operation for DeviceBatchOperation {
    type Output = Document;
    type Error = SyncRefusal;

    fn first(&mut self) -> BatchTurn {
        Ok(Next::Call(config_call(node_ids(self.ids)?.1)))
    }

    fn next(&mut self, result: IoResult) -> BatchTurn {
        let (node_id, realm_id) = node_ids(self.ids)?;
        match self.step {
            BatchStep::Config => {
                let config = config_answer(result)?;
                if !is_sync_eligible(&config, node_id) {
                    return Err(SyncRefusal::Unavailable);
                }
                self.config = Some(config);
                self.step = BatchStep::Caller;
                let caller = ForwardedCallerOperation::new(self.peer, realm_id, &self.message);
                Ok(Next::Call(IoCall::child(caller)))
            }
            BatchStep::Caller => {
                let auth = result.child::<AuthContext, ForwardAuthError>()?;
                self.auth = Some(auth.map_err(caller_refusal)?);
                self.held()
            }
            BatchStep::Held => match result.child::<Document, HeldRecordError>()? {
                Ok(record) => self.checked_graph(record),
                Err(HeldRecordError::NotFound) => Err(SyncRefusal::NotFound),
                Err(HeldRecordError::Unavailable(_)) => Err(SyncRefusal::Unavailable),
            },
            BatchStep::Write => {
                let allowed = result.child::<(), AuthorizeError>()?;
                allowed
                    .map_err(ForwardAuthError::from)
                    .map_err(caller_refusal)?;
                self.next_dot()
            }
            BatchStep::Dots => match metadata_event(result)? {
                MetadataEvent::ContainsDotResult { contains: true, .. } => self.next_dot(),
                _ => Err(SyncRefusal::Unavailable),
            },
            BatchStep::Update => match result.child::<Document, UpdateDocumentError>()? {
                Ok(record) => Ok(Next::Done(record)),
                Err(UpdateDocumentError::MetadataError(MetadataError::InvalidInput(message))) => {
                    Err(SyncRefusal::Invalid(message))
                }
                Err(error) => {
                    let document_id = self.record()?.document_id;
                    warn!(%document_id, %error, "A device edit did not apply");
                    Err(SyncRefusal::Unavailable)
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::driver::{DriverContext, drive};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::StorageEvent;
    use std::sync::Arc;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    #[tokio::test]
    async fn finds_later_membership() {
        // The only matching group sits just beyond the legacy default page.
        let dir = tempfile::tempdir().unwrap();
        let storage = aruna_storage::FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            graph_handle: None,
            task_handle: None,
            compute_handle: None,
            outbound_handle: None,
        });
        let realm_id = RealmId::from_bytes([7u8; 32]);
        let member = UserId::local(Ulid::from_bytes([8u8; 16]), realm_id);
        let other = UserId::local(Ulid::from_bytes([9u8; 16]), realm_id);
        let actor = Actor {
            node_id: node(1),
            user_id: member,
            realm_id,
        };
        let mut writes = Vec::with_capacity((GROUP_SCAN_PAGE + 1) * 2);
        for seed in 1..=GROUP_SCAN_PAGE + 1 {
            let group_id = Ulid::from(seed as u128);
            let group = Group {
                display_name: seed.to_string(),
                group_id,
                realm_id,
                roles: Default::default(),
                owner: other,
            };
            let authorization = GroupAuthorizationDocument::default_group_doc(
                if seed > GROUP_SCAN_PAGE {
                    member
                } else {
                    other
                },
                realm_id,
                group_id,
            );
            for (target, bytes) in [
                (
                    DocumentTarget::Group { group_id },
                    group.to_bytes(&actor).unwrap(),
                ),
                (
                    DocumentTarget::GroupAuthorization { group_id },
                    authorization.to_bytes(&actor).unwrap(),
                ),
            ] {
                writes.push((
                    target.storage_keyspace().to_string(),
                    target.storage_key(),
                    aruna_core::types::Value::from(bytes),
                ));
            }
        }
        assert!(matches!(
            context
                .storage_handle
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes,
                    txn_id: None,
                })
                .await,
            Event::Storage(StorageEvent::BatchWriteResult { .. })
        ));

        let documents = drive(DeviceGroupsOperation::new(member), &context)
            .await
            .expect("the groups read");
        assert_eq!(documents.len(), 1);
        assert_eq!(
            documents[0].group.group_id,
            Ulid::from((GROUP_SCAN_PAGE + 1) as u128)
        );
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    fn realm() -> RealmId {
        RealmId::from_bytes([7; 32])
    }

    fn token() -> AuthToken {
        AuthToken::internal(AuthContext {
            user_id: UserId::local(Ulid::from_bytes([8; 16]), realm()),
            realm_id: realm(),
            path_restrictions: None,
            session: None,
        })
    }

    fn config() -> RealmConfigDocument {
        RealmConfigDocument::new(realm(), Vec::new(), 1)
    }

    // A message of another kind is answered with a rejection and reads nothing.
    #[test]
    fn wrong_message_rejected() {
        let other = MetadataTransportMessage::Reject("other".to_string());
        let mut serve = serve_realm_documents(None, node(1), other);
        assert_eq!(
            serve.first(),
            Ok(Next::Done(reject("unexpected metadata control message")))
        );
    }

    // A refusal reaches the device inside the reply.
    #[test]
    fn refusal_travels_reply() {
        let asked = MetadataTransportMessage::FetchRealmDocuments {
            auth_token: token(),
        };
        let mut serve = serve_realm_documents(None, node(1), asked);
        serve.first().unwrap();
        let refused = child(Err::<RealmDocuments, _>(SyncRefusal::Unauthorized));
        let reply = MetadataTransportMessage::FetchedRealmDocuments {
            result: Err(SyncRefusal::Unauthorized),
        };
        assert_eq!(serve.next(refused), Ok(Next::Done(reply)));
    }

    // Without a network identity no realm state is served.
    #[test]
    fn documents_need_ids() {
        let mut read = ReadDocumentsOperation::new(None, node(1), token());
        assert_eq!(read.first(), Err(SyncRefusal::Unavailable));
    }

    // A device is no realm infrastructure and answers for nobody else.
    #[test]
    fn device_serves_nothing() {
        let ids = Some((node(2), realm()));
        let mut read = ReadDocumentsOperation::new(ids, node(1), token());
        read.first().unwrap();
        let answer = read.next(child(Ok::<_, GetConfigError>(config())));
        assert_eq!(answer, Err(SyncRefusal::Unavailable));
    }

    // Only a holder of the document answers for its graph.
    #[test]
    fn graph_needs_holder() {
        let ids = Some((node(2), realm()));
        let mut read = ReadGraphOperation::new(ids, node(1), token(), Ulid::from_bytes([3; 16]));
        read.first().unwrap();
        let answer = read.next(child(Ok::<_, GetConfigError>(config())));
        assert_eq!(answer, Err(SyncRefusal::Unavailable));
    }

    // A dependency this holder has not seen makes the device retry the edit.
    #[test]
    fn missing_dot_retries() {
        let ids = Some((node(2), realm()));
        let message = MetadataTransportMessage::Reject(String::new());
        let mut batch = DeviceBatchOperation::new(ids, node(1), message);
        batch.step = BatchStep::Dots;
        let event = MetadataEvent::ContainsDotResult {
            graph_iri: "graph".to_string(),
            contains: false,
        };
        let answer = batch.next(IoResult::Event(Box::new(Event::Metadata(event))));
        assert_eq!(answer, Err(SyncRefusal::Unavailable));
    }

    // A token of another realm is refused even when the peer check passes.
    #[test]
    fn foreign_realm_refused() {
        let ids = Some((node(2), realm()));
        let mut read = ReadDocumentsOperation::new(ids, node(1), token());
        read.config = Some(config());
        read.step = DocumentsStep::Peer;
        let foreign = RealmId::from_bytes([9; 32]);
        let auth = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([8; 16]), foreign),
            realm_id: foreign,
            path_restrictions: None,
            session: None,
        };
        let checked = Ok::<_, PeerAuthRefusal>(auth);
        let answer = read.next(IoResult::Child(ChildOutcome::new(checked)));
        assert_eq!(answer, Err(SyncRefusal::Unauthorized));
    }

    // A failed peer check keeps its reason.
    #[test]
    fn peer_refusal_mapped() {
        let ids = Some((node(2), realm()));
        let mut read = ReadDocumentsOperation::new(ids, node(1), token());
        read.config = Some(config());
        read.step = DocumentsStep::Peer;
        let checked = Err::<AuthContext, _>(PeerAuthRefusal::Unauthorized);
        let answer = read.next(IoResult::Child(ChildOutcome::new(checked)));
        assert_eq!(answer, Err(SyncRefusal::Unauthorized));
    }

    // Any other message than a batch is invalid once the caller is known.
    #[test]
    fn foreign_message_invalid() {
        let ids = Some((node(2), realm()));
        let message = MetadataTransportMessage::Reject(String::new());
        let mut batch = DeviceBatchOperation::new(ids, node(1), message);
        batch.config = Some(config());
        batch.step = BatchStep::Caller;
        let AuthToken::Internal(auth) = token() else {
            panic!("the test token is internal");
        };
        let answer = batch.next(child(Ok::<_, ForwardAuthError>(auth)));
        let reason = "unexpected metadata control message".to_string();
        assert_eq!(answer, Err(SyncRefusal::Invalid(reason)));
    }

    // An empty listing ends the scan with no groups.
    #[test]
    fn empty_listing_finishes() {
        let member = UserId::local(Ulid::from_bytes([8; 16]), realm());
        let mut groups = DeviceGroupsOperation::new(member);
        groups.first().unwrap();
        let answer = groups.next(child(Ok::<_, ListGroupError>(Vec::<Group>::new())));
        assert_eq!(answer, Ok(Next::Done(Vec::new())));
    }

    // A full page means more groups may follow, so the next page is asked for.
    #[test]
    fn full_page_continues() {
        let member = UserId::local(Ulid::from_bytes([8; 16]), realm());
        let mut groups = DeviceGroupsOperation::new(member);
        groups.page_len = GROUP_SCAN_PAGE;
        assert!(matches!(groups.advance(), Ok(Next::Call(IoCall::Child(_)))));
        assert_eq!(groups.offset, GROUP_SCAN_PAGE);
    }
}
