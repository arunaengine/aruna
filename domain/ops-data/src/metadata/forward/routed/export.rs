//! Exports one metadata document wherever it is held: here, at its holders, or from this
//! device's replica while the realm is out of reach.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::id::NodeId;
use aruna_core::metadata::{AuthToken, MaterializationState, MetadataEffect, MetadataEvent};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::placement::read_holder_sets;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};

use super::super::holder_reads::{
    AuthFailure, READ_FANOUT_LIMIT, READ_PEER_TIMEOUT, ReadDecision, avoid_holders_call,
    device_raw_revision, peer_transport_fault, reduce_holder_reads,
};
use super::super::read::METADATA_READ_DEADLINE;
use crate::device::replica::{ReplicaRecord, read_answer as replica_answer, replica_read};
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export::ExportOperation;
use crate::metadata::api::export_types::{
    ENTRY_ROOM, ExportMetadataRequest, PAGE_DEFAULT, PAGE_MAX,
};
use crate::metadata::api::readable::RecordReadableOperation;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::deletion_guard::metadata_call;
use crate::metadata::device::selection::keep_read;
use crate::metadata::forward::write::read_error;
use aruna_core::metadata::api::{ExportMetadataResult, MetadataRawView, RoCrateExportView};
use aruna_core::metadata::protocol::{
    MetadataReadError, MetadataTransportMessage, encode_message, metadata_request,
};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_ops_identity::forward::fetch::read_answer;
use aruna_ops_identity::realm::mutate_placement::node_kind;
use aruna_ops_identity::realm::peer_trust::is_sync_eligible;

type ExportTurn = Result<Next<ExportMetadataResult>, MetadataApiError>;
type ExportOutcome = Result<ExportMetadataResult, MetadataApiError>;

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
        Ok(Next::Call(metadata_call(effect)))
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
        match result.event()? {
            Event::Metadata(MetadataEvent::RoCrateExportResult { jsonld, .. }) => {
                Ok(Next::Done(self.raw(record, Some(jsonld))))
            }
            Event::Metadata(MetadataEvent::RoCrateSummaryResult { jsonld, .. }) => {
                Ok(Next::Done(ExportMetadataResult::Summary { jsonld, record }))
            }
            Event::Metadata(MetadataEvent::RoCratePageResult { page, .. }) => {
                Ok(Next::Done(ExportMetadataResult::Page {
                    page: *page,
                    record,
                }))
            }
            _ => Err(MetadataApiError::ServiceUnavailable),
        }
    }
}

/// Whether a read fills this device's cache: only the owner's own unrestricted read does.
pub fn owner_reads(
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

#[derive(Debug, PartialEq)]
enum RoutedStage {
    Local,
    Config,
    Replica,
    SoleLocal,
    Holders,
    Avoid(Box<ExportOutcome>),
    Kept,
    Device,
}

/// Exports one document directly when this node is the sole holder, or on all holders at once
/// under one deadline so denials and deletions are never overridden by a lagging replica. A
/// device answers from its pinned replica while the realm is out of reach.
#[derive(Debug, PartialEq)]
pub struct RoutedExportOperation {
    local_node: Option<NodeId>,
    realm_id: RealmId,
    request: ExportMetadataRequest,
    forward_token: Option<AuthToken>,
    metadata_bytes: u64,
    now_ms: u64,
    stage: RoutedStage,
    config: Option<RealmConfigDocument>,
    replica: Option<ReplicaRecord>,
    holders: Vec<NodeId>,
    /// The export that answers once this device kept its document.
    kept: Option<ExportMetadataResult>,
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
            now_ms: 0,
            stage: RoutedStage::Config,
            config: None,
            replica: None,
            holders: Vec::new(),
            kept: None,
        }
    }

    fn export_within(&self) -> ExportWithinOperation {
        ExportWithinOperation::new(self.realm_id, self.request.clone(), self.metadata_bytes)
    }

    fn within(&self) -> IoCall {
        IoCall::child(self.export_within())
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
        let replica = replica_answer(&result.event()?).filter(ReplicaRecord::serves_reads);
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
        let placement = resolve_metadata_id(config, self.realm_id, None, self.request.document_id)
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let holders =
            read_holder_sets(config, &placement).map_err(MetadataApiError::PlacementUnavailable)?;
        if holders.len() == 1 && Some(holders[0]) == self.local_node {
            self.stage = RoutedStage::SoleLocal;
            return Ok(Next::Call(self.within()));
        }
        let config_digest = config
            .digest()
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
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
        let frame = encode_message(&message).map_err(MetadataApiError::Internal)?;
        let members = holders
            .iter()
            .map(|holder| match Some(*holder) == self.local_node {
                true => Effect::SubOperation(boxed_suboperation(self.export_within(), |result| {
                    Event::Child(ChildOutcome::new(result))
                })),
                false => Effect::Net(NetEffect::MetadataRequest(Box::new(metadata_request(
                    *holder,
                    frame.clone(),
                    READ_PEER_TIMEOUT,
                )))),
            })
            .collect();
        self.holders = holders;
        self.stage = RoutedStage::Holders;
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: READ_FANOUT_LIMIT,
            form: GroupForm::All {
                deadline: Some(METADATA_READ_DEADLINE),
            },
        })))
    }

    fn sole_local_answered(&mut self, result: IoResult) -> ExportTurn {
        match result.child::<ExportMetadataResult, MetadataApiError>()? {
            Ok(export) => self.found(export),
            Err(
                error @ (MetadataApiError::Unauthorized
                | MetadataApiError::Forbidden
                | MetadataApiError::NotFound
                | MetadataApiError::TooLarge),
            ) => Err(error),
            Err(_) => self.fall_back(MetadataApiError::ServiceUnavailable),
        }
    }

    fn holder_export(
        &self,
        holder: NodeId,
        member: GroupResult,
    ) -> Result<ExportMetadataResult, MetadataReadError> {
        match (Some(holder) == self.local_node, member) {
            (true, GroupResult::Done(event)) => match *event {
                Event::Child(outcome) => outcome
                    .take::<ExportMetadataResult, MetadataApiError>()
                    .ok_or(MetadataReadError::Unavailable)?
                    .map_err(read_error),
                _ => Err(MetadataReadError::Unavailable),
            },
            (true, _) => Err(MetadataReadError::Unavailable),
            (false, member) => match read_answer(holder, Some(member)) {
                Ok(MetadataTransportMessage::ExportAnswer { result }) => result.map(|e| *e),
                _ => Err(MetadataReadError::Unavailable),
            },
        }
    }

    fn collect(&mut self, results: Vec<GroupResult>) -> ExportTurn {
        let holder_count = self.holders.len();
        let mut not_found = 0usize;
        let mut success = None;
        let mut auth_error = None;
        let (mut unavailable, mut too_large) = (false, false);
        let mut faulted = Vec::new();
        let mut results = results.into_iter();
        for holder in std::mem::take(&mut self.holders) {
            let member = results.next().unwrap_or(GroupResult::Unfinished);
            if Some(holder) != self.local_node && peer_transport_fault(&member) {
                faulted.push(holder);
            }
            unavailable |= matches!(member, GroupResult::Unfinished);
            match self.holder_export(holder, member) {
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
        let all_not_found = holder_count > 0 && not_found == holder_count;
        let outcome = match reduce_holder_reads(
            success,
            auth_error,
            all_not_found,
            conflict,
            unavailable,
            AuthFailure::Fatal,
        ) {
            ReadDecision::Success(export) => Ok(export),
            ReadDecision::NotFound => Err(MetadataApiError::NotFound),
            ReadDecision::Auth(MetadataReadError::Unauthorized) => {
                Err(MetadataApiError::Unauthorized)
            }
            ReadDecision::Auth(MetadataReadError::Forbidden) => Err(MetadataApiError::Forbidden),
            // A holder that found the page too large answered for every holder of the graph.
            ReadDecision::Unavailable if too_large => Err(MetadataApiError::TooLarge),
            ReadDecision::Auth(_) | ReadDecision::Unavailable => {
                Err(MetadataApiError::ServiceUnavailable)
            }
        };
        if !faulted.is_empty() {
            self.stage = RoutedStage::Avoid(Box::new(outcome));
            return Ok(Next::Call(avoid_holders_call(faulted, self.now_ms)));
        }
        self.finish_routed(outcome)
    }

    fn finish_routed(&mut self, outcome: ExportOutcome) -> ExportTurn {
        match outcome {
            Ok(export) => self.found(export),
            Err(error) => self.fall_back(error),
        }
    }

    /// The owner's own online export is kept on this device before it answers, like a read.
    fn found(&mut self, export: ExportMetadataResult) -> ExportTurn {
        let config = (self.config.as_ref()).ok_or(MetadataApiError::ServiceUnavailable)?;
        if !owner_reads(config, self.local_node, self.request.auth.as_ref()) {
            return Ok(Next::Done(export));
        }
        let (ExportMetadataResult::Full { record, .. }
        | ExportMetadataResult::Summary { record, .. }
        | ExportMetadataResult::Page { record, .. }
        | ExportMetadataResult::Raw { record, .. }) = &export;
        let ids = self.local_node.map(|node| (node, self.realm_id));
        let keep = keep_read(ids, record);
        self.kept = Some(export);
        self.stage = RoutedStage::Kept;
        Ok(Next::Call(IoCall::child(keep)))
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

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

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
        match std::mem::replace(&mut self.stage, RoutedStage::Config) {
            RoutedStage::Local => Ok(Next::Done(
                result.child::<ExportMetadataResult, MetadataApiError>()??,
            )),
            RoutedStage::Config => self.config_read(result),
            RoutedStage::Replica => self.replica_read(result),
            RoutedStage::SoleLocal => self.sole_local_answered(result),
            RoutedStage::Holders => {
                let IoResult::Group(GroupEvent::Finished { results }) = result else {
                    return Err(MetadataApiError::ServiceUnavailable);
                };
                self.collect(results)
            }
            RoutedStage::Avoid(outcome) => self.finish_routed(*outcome),
            // Keeping is best effort: a full cache or a failed write never fails the export.
            RoutedStage::Kept => (self.kept.take())
                .map(Next::Done)
                .ok_or(MetadataApiError::ServiceUnavailable),
            RoutedStage::Device => {
                let export = result.child::<ExportMetadataResult, MetadataApiError>()??;
                ensure_export_limit(&export, self.metadata_bytes)?;
                Ok(Next::Done(export))
            }
        }
    }
}

#[cfg(test)]
#[path = "export_tests.rs"]
mod call_tests;
