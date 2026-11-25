//! Reads one metadata registry record wherever it is held: here, at its holders, or from this
//! device's replica while the realm is out of reach.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::id::NodeId;
use aruna_core::metadata::AuthToken;
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::placement::read_holder_sets;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::placement::record::PlacementRef;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;

use super::super::holder_reads::{
    READ_FANOUT_LIMIT, READ_PEER_TIMEOUT, avoid_holders_call, peer_transport_fault,
};
use super::super::read::METADATA_READ_DEADLINE;
use super::export::owner_reads;
use crate::device::replica::{ReplicaRecord, read_answer as replica_answer, replica_read};
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export::VisibleRecordOperation;
use crate::metadata::api::export_types::GetVisibleRequest;
use crate::metadata::api::readable::RecordReadableOperation;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::device::selection::keep_read;
use crate::metadata::forward::replay::routed_record_matches;
use crate::metadata::forward::write::read_error;
use aruna_core::metadata::protocol::{
    MetadataReadError, MetadataTransportMessage, encode_message, metadata_request,
};
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_ops_identity::forward::fetch::read_answer;
use aruna_ops_identity::realm::peer_trust::is_sync_eligible;

type ReadTurn = Result<Next<MetadataRegistryRecord>, MetadataApiError>;
type ReadOutcome = Result<MetadataRegistryRecord, MetadataApiError>;

#[derive(Debug, PartialEq)]
enum ReadStage {
    Local,
    Config,
    Replica,
    SoleLocal,
    Holders,
    Avoid(Box<ReadOutcome>),
    /// The replica's record waits for the caller's own READ check.
    Readable(MetadataRegistryRecord),
    /// The record read online is kept as an unselected replica on this device.
    Kept(MetadataRegistryRecord),
}

/// Reads one registry record directly when this node is the sole holder, or on all holders under
/// one deadline so denials and deletions hold. A device answers from its pinned replica, or an
/// unselected replica while offline, and keeps the owner's online read.
#[derive(Debug, PartialEq)]
pub struct RoutedReadOperation {
    local_node: Option<NodeId>,
    realm_id: RealmId,
    request: GetVisibleRequest,
    forward_token: Option<AuthToken>,
    now_ms: u64,
    stage: ReadStage,
    config: Option<RealmConfigDocument>,
    replica: Option<ReplicaRecord>,
    placement: Option<PlacementRef>,
    holders: Vec<NodeId>,
}

impl RoutedReadOperation {
    /// `local_node` is this node on the network; without one the node reads its own copy.
    pub fn new(
        local_node: Option<NodeId>,
        realm_id: RealmId,
        request: GetVisibleRequest,
        forward_token: Option<AuthToken>,
    ) -> Self {
        Self {
            local_node,
            realm_id,
            request,
            forward_token,
            now_ms: 0,
            stage: ReadStage::Config,
            config: None,
            replica: None,
            placement: None,
            holders: Vec::new(),
        }
    }

    fn visible(&self) -> VisibleRecordOperation {
        VisibleRecordOperation::new(self.realm_id, self.request.clone())
    }

    fn config(&self) -> Result<&RealmConfigDocument, MetadataApiError> {
        (self.config.as_ref()).ok_or(MetadataApiError::ServiceUnavailable)
    }

    fn configured(&mut self, result: IoResult) -> ReadTurn {
        let config = (result.child::<RealmConfigDocument, GetConfigError>()?)
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let device = (self.local_node).is_some_and(|node| !is_sync_eligible(&config, node));
        self.config = Some(config);
        if !device {
            return self.route();
        }
        self.stage = ReadStage::Replica;
        let read = replica_read(self.request.document_id);
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Storage(read)))))
    }

    fn replica_read(&mut self, result: IoResult) -> ReadTurn {
        let replica = replica_answer(&result.event()?).filter(ReplicaRecord::serves_reads);
        let pinned =
            (replica.as_ref()).is_some_and(|replica| replica.selected || replica.pending_edits > 0);
        self.replica = replica;
        if pinned {
            return self.fall_back(MetadataApiError::ServiceUnavailable);
        }
        self.route()
    }

    /// Reads locally without frame encoding when this node is the sole holder; otherwise asks
    /// every holder at once under one overall deadline.
    fn route(&mut self) -> ReadTurn {
        let config = self.config()?;
        let placement = resolve_metadata_id(config, self.realm_id, None, self.request.document_id)
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let holders =
            read_holder_sets(config, &placement).map_err(MetadataApiError::PlacementUnavailable)?;
        if holders.len() == 1 && Some(holders[0]) == self.local_node {
            self.placement = Some(placement);
            self.stage = ReadStage::SoleLocal;
            return Ok(Next::Call(IoCall::child(self.visible())));
        }
        let config_digest = (config.digest()).map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let message = MetadataTransportMessage::ForwardReadDocument {
            auth_token: self.forward_token.clone(),
            config_digest,
            document_id: self.request.document_id,
        };
        let frame = encode_message(&message).map_err(MetadataApiError::Internal)?;
        let members = (holders.iter())
            .map(|holder| match Some(*holder) == self.local_node {
                true => Effect::SubOperation(boxed_suboperation(self.visible(), |result| {
                    Event::Child(ChildOutcome::new(result))
                })),
                false => Effect::Net(NetEffect::MetadataRequest(Box::new(metadata_request(
                    *holder,
                    frame.clone(),
                    READ_PEER_TIMEOUT,
                )))),
            })
            .collect();
        self.placement = Some(placement);
        self.holders = holders;
        self.stage = ReadStage::Holders;
        Ok(Next::Call(IoCall::Group(EffectGroup {
            members,
            limit: READ_FANOUT_LIMIT,
            form: GroupForm::All {
                deadline: Some(METADATA_READ_DEADLINE),
            },
        })))
    }

    fn record_valid(&self, record: &MetadataRegistryRecord) -> bool {
        let Ok(config) = self.config() else {
            return false;
        };
        let Some(placement) = self.placement.as_ref() else {
            return false;
        };
        routed_record_matches(config, self.realm_id, record.document_id, placement, record)
    }

    fn sole_local_answered(&mut self, result: IoResult) -> ReadTurn {
        match result.child::<MetadataRegistryRecord, MetadataApiError>()? {
            Ok(record) if self.record_valid(&record) => self.found(record),
            Err(
                error @ (MetadataApiError::Unauthorized
                | MetadataApiError::Forbidden
                | MetadataApiError::NotFound),
            ) => Err(error),
            Ok(_) | Err(_) => self.fall_back(MetadataApiError::ServiceUnavailable),
        }
    }

    /// One holder's record, when it matches the placement it was read for.
    fn holder_record(
        &self,
        holder: NodeId,
        member: GroupResult,
    ) -> Result<MetadataRegistryRecord, MetadataReadError> {
        let record = match (Some(holder) == self.local_node, member) {
            (true, GroupResult::Done(event)) => match *event {
                Event::Child(outcome) => outcome
                    .take::<MetadataRegistryRecord, MetadataApiError>()
                    .ok_or(MetadataReadError::Unavailable)?
                    .map_err(read_error)?,
                _ => return Err(MetadataReadError::Unavailable),
            },
            (true, _) => return Err(MetadataReadError::Unavailable),
            (false, member) => match read_answer(holder, Some(member)) {
                Ok(MetadataTransportMessage::ForwardedRead { result }) => *result?,
                _ => return Err(MetadataReadError::Unavailable),
            },
        };
        match self.record_valid(&record) {
            true => Ok(record),
            false => Err(MetadataReadError::Unavailable),
        }
    }

    fn collect(&mut self, results: Vec<GroupResult>) -> ReadTurn {
        let holder_count = self.holders.len();
        let mut not_found = 0usize;
        let mut success = None;
        let mut auth_error = None;
        let mut unavailable = false;
        let mut faulted = Vec::new();
        let mut results = results.into_iter();
        for holder in std::mem::take(&mut self.holders) {
            let member = results.next().unwrap_or(GroupResult::Unfinished);
            if Some(holder) != self.local_node && peer_transport_fault(&member) {
                faulted.push(holder);
            }
            unavailable |= matches!(member, GroupResult::Unfinished);
            match self.holder_record(holder, member) {
                Ok(record) => {
                    success.get_or_insert(record);
                }
                Err(MetadataReadError::Unauthorized) => {
                    auth_error.get_or_insert(MetadataApiError::Unauthorized);
                }
                Err(MetadataReadError::Forbidden) => {
                    auth_error.get_or_insert(MetadataApiError::Forbidden);
                }
                Err(MetadataReadError::NotFound) => not_found += 1,
                Err(MetadataReadError::Unavailable | MetadataReadError::TooLarge) => {
                    unavailable = true;
                }
            }
        }
        let outcome = match (auth_error, success) {
            (Some(error), _) => Err(error),
            (None, Some(_)) if not_found > 0 => Err(MetadataApiError::ServiceUnavailable),
            (None, Some(record)) => Ok(record),
            (None, None) if !unavailable && holder_count > 0 && not_found == holder_count => {
                Err(MetadataApiError::NotFound)
            }
            (None, None) => Err(MetadataApiError::ServiceUnavailable),
        };
        if !faulted.is_empty() {
            self.stage = ReadStage::Avoid(Box::new(outcome));
            return Ok(Next::Call(avoid_holders_call(faulted, self.now_ms)));
        }
        self.finish_routed(outcome)
    }

    fn finish_routed(&mut self, outcome: ReadOutcome) -> ReadTurn {
        match outcome {
            Ok(record) => self.found(record),
            Err(error) => self.fall_back(error),
        }
    }

    /// The owner's own online read is kept on this device before it answers.
    fn found(&mut self, record: MetadataRegistryRecord) -> ReadTurn {
        let config = self.config()?;
        if !owner_reads(config, self.local_node, self.request.auth.as_ref()) {
            return Ok(Next::Done(record));
        }
        let ids = self.local_node.map(|node| (node, self.realm_id));
        let keep = keep_read(ids, &record);
        self.stage = ReadStage::Kept(record);
        Ok(Next::Call(IoCall::child(keep)))
    }

    /// An unavailable realm leaves this device's replica to answer, after the caller's own
    /// READ check.
    fn fall_back(&mut self, error: MetadataApiError) -> ReadTurn {
        let replica = self.replica.take();
        let record = replica
            .and_then(|replica| replica.record)
            .map(|record| *record);
        match (error, record) {
            (MetadataApiError::ServiceUnavailable, Some(record)) => {
                let auth = self.request.auth.clone();
                let readable = RecordReadableOperation::new(self.realm_id, auth, record.clone());
                self.stage = ReadStage::Readable(record);
                Ok(Next::Call(IoCall::child(readable)))
            }
            (error, _) => Err(error),
        }
    }
}

impl Operation for RoutedReadOperation {
    type Output = MetadataRegistryRecord;
    type Error = MetadataApiError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> ReadTurn {
        if self.local_node.is_none() {
            self.stage = ReadStage::Local;
            return Ok(Next::Call(IoCall::child(self.visible())));
        }
        let config = GetConfigOperation::new(self.realm_id);
        Ok(Next::Call(IoCall::child(config)))
    }

    fn next(&mut self, result: IoResult) -> ReadTurn {
        match std::mem::replace(&mut self.stage, ReadStage::Config) {
            ReadStage::Local => Ok(Next::Done(
                result.child::<MetadataRegistryRecord, MetadataApiError>()??,
            )),
            ReadStage::Config => self.configured(result),
            ReadStage::Replica => self.replica_read(result),
            ReadStage::SoleLocal => self.sole_local_answered(result),
            ReadStage::Holders => {
                let IoResult::Group(GroupEvent::Finished { results }) = result else {
                    return Err(MetadataApiError::ServiceUnavailable);
                };
                self.collect(results)
            }
            ReadStage::Avoid(outcome) => self.finish_routed(*outcome),
            ReadStage::Readable(record) => {
                result.child::<(), MetadataApiError>()??;
                Ok(Next::Done(record))
            }
            // Keeping is best effort: a full cache or a failed write never fails the read.
            ReadStage::Kept(record) => Ok(Next::Done(record)),
        }
    }
}

#[cfg(test)]
#[path = "read_tests.rs"]
mod call_tests;
