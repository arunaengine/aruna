//! Exports one metadata document wherever it is held: here, at its holders, or from this
//! device's replica while the realm is out of reach.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::metadata::{AuthToken, MaterializationState, MetadataEffect, MetadataEvent};
use aruna_core::operation::Operation;
use aruna_core::placement::read_holder_sets;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};

use super::holder_reads::{AuthFailure, ReadDecision, reduce_holder_reads};
use super::holder_reads::{READ_FANOUT_LIMIT, READ_PEER_TIMEOUT, device_raw_revision};
use crate::device::replica::{ReplicaRecord, read_answer, replica_read};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::fetch::{FanoutOperation, FetchError, PeerAnswer};
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export::ExportOperation;
use crate::metadata::api::export_types::{
    ENTRY_ROOM, ExportMetadataRequest, ExportMetadataResult, PAGE_DEFAULT, PAGE_MAX,
    RoCrateExportView,
};
use crate::metadata::api::readable::RecordReadableOperation;
use crate::metadata::call_results::effect_event;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage};
use crate::metadata::revision_read::MetadataRawView;
use crate::realm::mutate_placement::node_kind;
use crate::realm::peer_trust::is_sync_eligible;

type ExportTurn = Result<Next<ExportMetadataResult>, MetadataApiError>;

/// Refuses an export larger than `metadata_bytes`.
pub fn ensure_export_limit(
    export: &ExportMetadataResult,
    metadata_bytes: u64,
) -> Result<(), MetadataApiError> {
    let length = match export {
        ExportMetadataResult::Full { jsonld, .. }
        | ExportMetadataResult::Summary { jsonld, .. } => jsonld.len(),
        // A page is read with its cursor, and a reader adds a few bytes around each entity.
        ExportMetadataResult::Page { page, .. } => {
            let cursor = page.next_cursor.as_ref().map_or(0, String::len);
            page.jsonld.len() + cursor + ENTRY_ROOM * page.returned_data_entities
        }
        ExportMetadataResult::Raw { raw, .. } => raw.revision.jsonld.len(),
    };
    if u64::try_from(length).unwrap_or(u64::MAX) > metadata_bytes {
        return Err(MetadataApiError::ServiceUnavailable);
    }
    Ok(())
}

/// Exports one document from this node's own copy and halves a page until it fits the byte
/// bound; a page of one entity that is still too large is refused.
#[derive(Debug, PartialEq)]
pub struct ExportWithinOperation {
    realm_id: RealmId,
    request: ExportMetadataRequest,
    metadata_bytes: u64,
}

impl ExportWithinOperation {
    pub fn new(realm_id: RealmId, request: ExportMetadataRequest, metadata_bytes: u64) -> Self {
        Self {
            realm_id,
            request,
            metadata_bytes,
        }
    }

    fn export(&self) -> IoCall {
        IoCall::child(ExportOperation::new(self.realm_id, self.request.clone()))
    }
}

impl Operation for ExportWithinOperation {
    type Output = ExportMetadataResult;
    type Error = MetadataApiError;

    fn first(&mut self) -> ExportTurn {
        Ok(Next::Call(self.export()))
    }

    fn next(&mut self, result: IoResult) -> ExportTurn {
        let export = result.child::<ExportMetadataResult, MetadataApiError>()??;
        let over = ensure_export_limit(&export, self.metadata_bytes);
        let page = matches!(export, ExportMetadataResult::Page { .. });
        let limit = self
            .request
            .limit
            .unwrap_or(PAGE_DEFAULT)
            .clamp(1, PAGE_MAX);
        match over {
            Ok(()) => Ok(Next::Done(export)),
            Err(_) if page && limit > 1 => {
                self.request.limit = Some(limit / 2);
                Ok(Next::Call(self.export()))
            }
            Err(_) if page => Err(MetadataApiError::TooLarge),
            Err(error) => Err(error),
        }
    }
}

/// Exports one document from this device's replica. The displayed render is what the device
/// holds, and the graph views come from the local graph the replica installed.
#[derive(Debug, PartialEq)]
pub struct DeviceExportOperation {
    replica: ReplicaRecord,
    request: ExportMetadataRequest,
    checked: bool,
}

impl DeviceExportOperation {
    pub fn new(replica: ReplicaRecord, request: ExportMetadataRequest) -> Self {
        Self {
            replica,
            request,
            checked: false,
        }
    }

    fn record(
        &self,
    ) -> Result<
        aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord,
        MetadataApiError,
    > {
        (self.replica.record.as_deref().cloned()).ok_or(MetadataApiError::ServiceUnavailable)
    }

    fn graph_view(&mut self) -> ExportTurn {
        let record = self.record()?;
        let graph_iri = record.graph_iri.clone();
        let effect = match self.request.view {
            RoCrateExportView::Full => {
                return Ok(Next::Done(ExportMetadataResult::Full {
                    jsonld: self.replica.displayed_jsonld.clone(),
                    record,
                }));
            }
            RoCrateExportView::Raw if self.replica.findings == 0 => {
                return Ok(Next::Done(self.raw(record, None)));
            }
            RoCrateExportView::Raw => MetadataEffect::ExportRoCrate { graph_iri },
            RoCrateExportView::Summary => MetadataEffect::ExportRoCrateSummary { graph_iri },
            RoCrateExportView::Page => MetadataEffect::ExportRoCratePage {
                graph_iri,
                limit: self.request.limit.unwrap_or(100).clamp(1, 1_000),
                offset: self.request.offset,
                after: self.request.after.clone(),
            },
        };
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Metadata(
            effect,
        )))))
    }

    fn raw(
        &self,
        record: aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord,
        merged: Option<String>,
    ) -> ExportMetadataResult {
        ExportMetadataResult::Raw {
            raw: MetadataRawView {
                revision: device_raw_revision(
                    &self.replica.displayed_jsonld,
                    self.replica.dataset_digest,
                    self.replica.findings,
                    record.last_event_id,
                    merged,
                ),
                projection_state: MaterializationState::Materialized,
                projected_event_id: Some(record.last_event_id),
            },
            dataset_digest: self.replica.dataset_digest,
            record,
        }
    }
}

impl Operation for DeviceExportOperation {
    type Output = ExportMetadataResult;
    type Error = MetadataApiError;

    fn first(&mut self) -> ExportTurn {
        let record = self.record()?;
        let readable =
            RecordReadableOperation::new(record.realm_id, self.request.auth.clone(), record);
        Ok(Next::Call(IoCall::child(readable)))
    }

    fn next(&mut self, result: IoResult) -> ExportTurn {
        if !self.checked {
            result.child::<(), MetadataApiError>()??;
            self.checked = true;
            return self.graph_view();
        }
        let record = self.record()?;
        match effect_event(result)? {
            Event::Metadata(MetadataEvent::RoCrateExportResult { jsonld, .. }) => {
                Ok(Next::Done(self.raw(record, Some(jsonld))))
            }
            Event::Metadata(MetadataEvent::RoCrateSummaryResult { jsonld, .. }) => {
                Ok(Next::Done(ExportMetadataResult::Summary { jsonld, record }))
            }
            Event::Metadata(MetadataEvent::RoCratePageResult { page, .. }) => {
                Ok(Next::Done(ExportMetadataResult::Page { page, record }))
            }
            _ => Err(MetadataApiError::ServiceUnavailable),
        }
    }
}

/// Whether a read fills this device's cache: only the owner's own unrestricted read does.
pub(crate) fn owner_reads(
    config: &RealmConfigDocument,
    local_node: Option<NodeId>,
    auth: Option<&AuthContext>,
) -> bool {
    let owner = local_node
        .and_then(|node| node_kind(config, node))
        .and_then(|kind| kind.owner());
    auth.is_some_and(|auth| {
        Some(auth.user_id) == owner && auth.path_restrictions.is_none() && !auth.federated()
    })
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RoutedStage {
    Local,
    Config,
    Replica,
    Holder,
    Peers,
    Device,
}

/// Exports one document on its holders: this node's own copy when it holds the document, the
/// other holders' answers otherwise. A device answers from its pinned replica while the realm is
/// out of reach.
#[derive(Debug, PartialEq)]
pub struct RoutedExportOperation {
    local_node: Option<NodeId>,
    realm_id: RealmId,
    request: ExportMetadataRequest,
    forward_token: Option<AuthToken>,
    metadata_bytes: u64,
    stage: RoutedStage,
    config: Option<RealmConfigDocument>,
    replica: Option<ReplicaRecord>,
    peers: Vec<NodeId>,
    holder_count: usize,
    answers: Vec<(NodeId, Result<ExportMetadataResult, MetadataReadError>)>,
    /// The other holders' requests, sent once this node's own copy answered.
    pending_fanout: Option<Vec<(NodeId, MetadataTransportMessage)>>,
}

impl RoutedExportOperation {
    /// `local_node` is this node on the network; without one the node exports its own copy.
    pub fn new(
        local_node: Option<NodeId>,
        realm_id: RealmId,
        request: ExportMetadataRequest,
        forward_token: Option<AuthToken>,
        metadata_bytes: u64,
    ) -> Self {
        Self {
            local_node,
            realm_id,
            request,
            forward_token,
            metadata_bytes,
            stage: RoutedStage::Config,
            config: None,
            replica: None,
            peers: Vec::new(),
            holder_count: 0,
            answers: Vec::new(),
            pending_fanout: None,
        }
    }

    fn within(&self) -> IoCall {
        IoCall::child(ExportWithinOperation::new(
            self.realm_id,
            self.request.clone(),
            self.metadata_bytes,
        ))
    }

    fn config_read(&mut self, result: IoResult) -> ExportTurn {
        let config = result
            .child::<RealmConfigDocument, GetConfigError>()?
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let device = self
            .local_node
            .is_some_and(|node| !is_sync_eligible(&config, node));
        self.config = Some(config);
        if !device {
            return self.route();
        }
        self.stage = RoutedStage::Replica;
        let read = replica_read(self.request.document_id);
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(read)))))
    }

    /// Like a read, an unselected replica answers only while the realm is out of reach.
    fn replica_read(&mut self, result: IoResult) -> ExportTurn {
        let replica = read_answer(&effect_event(result)?).filter(ReplicaRecord::serves_reads);
        let pinned = replica
            .as_ref()
            .is_some_and(|replica| replica.selected || replica.pending_edits > 0);
        self.replica = replica;
        if pinned {
            return self.fall_back(MetadataApiError::ServiceUnavailable);
        }
        self.route()
    }

    fn route(&mut self) -> ExportTurn {
        let config = self
            .config
            .as_ref()
            .ok_or(MetadataApiError::ServiceUnavailable)?;
        let config_digest = config
            .digest()
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let placement = resolve_metadata_id(config, self.realm_id, None, self.request.document_id)
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let holders =
            read_holder_sets(config, &placement).map_err(MetadataApiError::PlacementUnavailable)?;
        self.holder_count = holders.len();
        let holds = self.local_node.is_some_and(|node| holders.contains(&node));
        let local_node = self.local_node;
        let message = MetadataTransportMessage::ForwardExportDocument {
            auth_token: self.forward_token.clone(),
            config_digest,
            document_id: self.request.document_id,
            view: self.request.view,
            metadata_bytes: self.metadata_bytes,
            limit: self.request.limit,
            offset: self.request.offset,
            after: self.request.after.clone(),
        };
        self.peers = holders
            .into_iter()
            .filter(|holder| Some(*holder) != local_node)
            .collect();
        let requests = self
            .peers
            .iter()
            .map(|peer| (*peer, message.clone()))
            .collect();
        if holds {
            self.stage = RoutedStage::Holder;
            self.pending_fanout = Some(requests);
            return Ok(Next::Call(self.within()));
        }
        self.ask(requests)
    }

    fn ask(&mut self, requests: Vec<(NodeId, MetadataTransportMessage)>) -> ExportTurn {
        if requests.is_empty() {
            return self.collect();
        }
        self.stage = RoutedStage::Peers;
        let fanout = FanoutOperation::new(requests, READ_FANOUT_LIMIT, READ_PEER_TIMEOUT);
        Ok(Next::Call(IoCall::child(fanout)))
    }

    fn collect(&mut self) -> ExportTurn {
        let mut not_found = 0usize;
        let mut success = None;
        let mut auth_error = None;
        let (mut unavailable, mut too_large) = (false, false);
        for (_, response) in std::mem::take(&mut self.answers) {
            match response {
                Ok(export) => {
                    success.get_or_insert(export);
                }
                Err(error @ (MetadataReadError::Unauthorized | MetadataReadError::Forbidden)) => {
                    auth_error.get_or_insert(error);
                }
                Err(MetadataReadError::NotFound) => not_found += 1,
                Err(MetadataReadError::Unavailable) => unavailable = true,
                Err(MetadataReadError::TooLarge) => too_large = true,
            }
        }
        let conflict = success.is_some() && not_found > 0;
        let all_not_found = self.holder_count > 0 && not_found == self.holder_count;
        let routed = match reduce_holder_reads(
            success,
            auth_error,
            all_not_found,
            conflict,
            unavailable,
            AuthFailure::Fatal,
        ) {
            ReadDecision::Success(export) => return Ok(Next::Done(export)),
            ReadDecision::NotFound => MetadataApiError::NotFound,
            ReadDecision::Auth(MetadataReadError::Unauthorized) => MetadataApiError::Unauthorized,
            ReadDecision::Auth(MetadataReadError::Forbidden) => MetadataApiError::Forbidden,
            // A holder that found the page too large answered for every holder of the graph.
            ReadDecision::Unavailable if too_large => MetadataApiError::TooLarge,
            ReadDecision::Auth(_) | ReadDecision::Unavailable => {
                MetadataApiError::ServiceUnavailable
            }
        };
        self.fall_back(routed)
    }

    /// An unavailable realm leaves this device's replica to answer.
    fn fall_back(&mut self, error: MetadataApiError) -> ExportTurn {
        match (error, self.replica.take()) {
            (MetadataApiError::ServiceUnavailable, Some(replica)) => {
                self.stage = RoutedStage::Device;
                let export = DeviceExportOperation::new(replica, self.request.clone());
                Ok(Next::Call(IoCall::child(export)))
            }
            (error, _) => Err(error),
        }
    }
}

impl Operation for RoutedExportOperation {
    type Output = ExportMetadataResult;
    type Error = MetadataApiError;

    fn first(&mut self) -> ExportTurn {
        if self.local_node.is_none() {
            self.stage = RoutedStage::Local;
            return Ok(Next::Call(self.within()));
        }
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> ExportTurn {
        match self.stage {
            RoutedStage::Local => Ok(Next::Done(
                result.child::<ExportMetadataResult, MetadataApiError>()??,
            )),
            RoutedStage::Config => self.config_read(result),
            RoutedStage::Replica => self.replica_read(result),
            RoutedStage::Holder => {
                let local = result
                    .child::<ExportMetadataResult, MetadataApiError>()?
                    .map_err(crate::forward::transport::read_error);
                if let Some(node) = self.local_node {
                    self.answers.push((node, local));
                }
                let requests = self.pending_fanout.take().unwrap_or_default();
                self.ask(requests)
            }
            RoutedStage::Peers => {
                let answers = result
                    .child::<Vec<PeerAnswer>, FetchError>()?
                    .map_err(|_| MetadataApiError::ServiceUnavailable)?;
                for (peer, answer) in answers {
                    let export = match answer {
                        Ok(MetadataTransportMessage::ExportAnswer { result }) => {
                            result.map(|export| *export)
                        }
                        _ => Err(MetadataReadError::Unavailable),
                    };
                    self.answers.push((peer, export));
                }
                self.collect()
            }
            RoutedStage::Device => {
                let export = result.child::<ExportMetadataResult, MetadataApiError>()??;
                ensure_export_limit(&export, self.metadata_bytes)?;
                Ok(Next::Done(export))
            }
        }
    }
}
