//! Reads or recomputes the Profile status of one document on its holders.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{ChildOutcome, IoCall, IoResult, Next};
use aruna_core::effects::{Effect, EffectGroup, GroupForm, MetadataRequestEffect, NetEffect};
use aruna_core::events::{Event, GroupEvent, GroupResult};
use aruna_core::metadata::{AuthToken, MetadataError, ProfileValidationStatus};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::placement::read_holder_sets;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use ulid::Ulid;

use super::holder_reads::{READ_FANOUT_LIMIT, READ_PEER_TIMEOUT};
use super::read::{METADATA_READ_DEADLINE, keep_status};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::fetch::read_answer;
use crate::forward::transport::read_error;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::export_types::GetVisibleRequest;
use crate::metadata::api::readable::RecordReadableOperation;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::live_records::{LiveRecordOperation, LiveRecordsError};
use crate::metadata::profile::validation::{RevalidateOperation, ValidationStatusOperation};
use crate::metadata::protocol::{MetadataReadError, MetadataTransportMessage, encode_message};
use crate::realm::load_config::config_unavailable;

type StatusTurn = Result<Next<ProfileValidationStatus>, MetadataApiError>;

/// The live record of `result`, or not found.
fn live_record(result: IoResult) -> Result<MetadataRegistryRecord, MetadataApiError> {
    let record = result.child::<Option<MetadataRegistryRecord>, LiveRecordsError>()??;
    record.ok_or(MetadataApiError::NotFound)
}

#[derive(Debug, PartialEq)]
enum LocalStage {
    Record,
    Readable,
    Status,
}

/// This node's Profile status of one live document the caller may read, recomputed when asked.
#[derive(Debug, PartialEq)]
pub(crate) struct LocalStatusOperation {
    realm_id: RealmId,
    document_id: Ulid,
    auth: Option<AuthContext>,
    local_node: Option<NodeId>,
    revalidate: bool,
    record: Option<MetadataRegistryRecord>,
    stage: LocalStage,
}

impl LocalStatusOperation {
    /// A `record` already read is checked as it is; without one the document is read first.
    pub(crate) fn new(
        (realm_id, local_node): (RealmId, Option<NodeId>),
        request: GetVisibleRequest,
        record: Option<MetadataRegistryRecord>,
        revalidate: bool,
    ) -> Self {
        Self {
            realm_id,
            document_id: request.document_id,
            auth: request.auth,
            local_node,
            revalidate,
            record,
            stage: LocalStage::Record,
        }
    }

    fn readable(&mut self, record: MetadataRegistryRecord) -> StatusTurn {
        let readable =
            RecordReadableOperation::new(self.realm_id, self.auth.clone(), record.clone());
        self.record = Some(record);
        self.stage = LocalStage::Readable;
        Ok(Next::Call(IoCall::child(readable)))
    }
}

impl Operation for LocalStatusOperation {
    type Output = ProfileValidationStatus;
    type Error = MetadataApiError;

    fn first(&mut self) -> StatusTurn {
        if let Some(record) = self.record.take() {
            return self.readable(record);
        }
        Ok(Next::Call(IoCall::child(LiveRecordOperation::new(
            self.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> StatusTurn {
        match self.stage {
            LocalStage::Record => {
                let record = live_record(result)?;
                self.readable(record)
            }
            LocalStage::Readable => {
                result.child::<(), MetadataApiError>()??;
                let record = self
                    .record
                    .take()
                    .ok_or(MetadataApiError::ServiceUnavailable)?;
                self.stage = LocalStage::Status;
                Ok(Next::Call(match self.revalidate {
                    true => IoCall::child(RevalidateOperation::new(self.local_node, record)),
                    false => IoCall::child(ValidationStatusOperation::new(record)),
                }))
            }
            LocalStage::Status => {
                let status = result.child::<ProfileValidationStatus, MetadataError>()?;
                Ok(Next::Done(
                    status.map_err(|_| MetadataApiError::ServiceUnavailable)?,
                ))
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum RoutedStage {
    Record,
    Local,
    Config,
    Holders,
}

/// Reads or recomputes Profile status on the document's holders, this node among them, all at
/// once under one deadline. Each holder performs the same READ check as the ordinary metadata
/// route, and a status of the current revision wins over an older one.
#[derive(Debug, PartialEq)]
pub struct RoutedStatusOperation {
    local_node: Option<NodeId>,
    realm_id: RealmId,
    request: GetVisibleRequest,
    forward_token: Option<AuthToken>,
    revalidate: bool,
    stage: RoutedStage,
    expected_revision: Ulid,
    holders: Vec<NodeId>,
}

impl RoutedStatusOperation {
    /// `local_node` is this node on the network; without one the node answers itself.
    pub fn new(
        (local_node, realm_id): (Option<NodeId>, RealmId),
        request: GetVisibleRequest,
        forward_token: Option<AuthToken>,
        revalidate: bool,
    ) -> Self {
        Self {
            local_node,
            realm_id,
            request,
            forward_token,
            revalidate,
            stage: RoutedStage::Record,
            expected_revision: Ulid::nil(),
            holders: Vec::new(),
        }
    }

    fn local(&self, record: Option<MetadataRegistryRecord>) -> LocalStatusOperation {
        let node = (self.realm_id, self.local_node);
        LocalStatusOperation::new(node, self.request.clone(), record, self.revalidate)
    }

    fn route(&mut self, config: RealmConfigDocument) -> StatusTurn {
        let config_digest = (config.digest()).map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let placement = resolve_metadata_id(&config, self.realm_id, None, self.request.document_id)
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let holders = read_holder_sets(&config, &placement)
            .map_err(MetadataApiError::PlacementUnavailable)?;
        let message = MetadataTransportMessage::ForwardValidationStatus {
            auth_token: self.forward_token.clone(),
            config_digest,
            document_id: self.request.document_id,
            revalidate: self.revalidate,
        };
        let frame = encode_message(&message).map_err(MetadataApiError::Internal)?;
        let members = (holders.iter())
            .map(|holder| match Some(*holder) == self.local_node {
                true => Effect::SubOperation(boxed_suboperation(self.local(None), |result| {
                    Event::Child(ChildOutcome::new(result))
                })),
                false => Effect::Net(NetEffect::MetadataRequest(Box::new(
                    MetadataRequestEffect {
                        peer: *holder,
                        frame: frame.clone(),
                        timeout: READ_PEER_TIMEOUT,
                    },
                ))),
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

    fn holder_status(
        &self,
        holder: NodeId,
        member: GroupResult,
    ) -> Result<ProfileValidationStatus, MetadataReadError> {
        if Some(holder) != self.local_node {
            return match read_answer(holder, Some(member)) {
                Ok(MetadataTransportMessage::ForwardedValidationStatus { result }) => {
                    result.map(|status| *status)
                }
                _ => Err(MetadataReadError::Unavailable),
            };
        }
        let GroupResult::Done(event) = member else {
            return Err(MetadataReadError::Unavailable);
        };
        let Event::Child(outcome) = *event else {
            return Err(MetadataReadError::Unavailable);
        };
        let status = outcome.take::<ProfileValidationStatus, MetadataApiError>();
        status
            .ok_or(MetadataReadError::Unavailable)?
            .map_err(read_error)
    }

    fn collect(&mut self, results: Vec<GroupResult>) -> StatusTurn {
        let holder_count = self.holders.len();
        let mut not_found = 0usize;
        let mut success = None;
        let mut auth_error = None;
        let mut unavailable = false;
        let mut results = results.into_iter();
        for holder in std::mem::take(&mut self.holders) {
            let member = results.next().unwrap_or(GroupResult::Unfinished);
            unavailable |= matches!(member, GroupResult::Unfinished);
            match self.holder_status(holder, member) {
                Ok(status) => keep_status(&mut success, status, self.expected_revision),
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
        match (auth_error, success) {
            (Some(error), _) => Err(error),
            (None, Some(_)) if not_found > 0 => Err(MetadataApiError::ServiceUnavailable),
            (None, Some(status)) => Ok(Next::Done(status)),
            (None, None) if !unavailable && holder_count > 0 && not_found == holder_count => {
                Err(MetadataApiError::NotFound)
            }
            (None, None) => Err(MetadataApiError::ServiceUnavailable),
        }
    }
}

impl Operation for RoutedStatusOperation {
    type Output = ProfileValidationStatus;
    type Error = MetadataApiError;

    fn first(&mut self) -> StatusTurn {
        let record = LiveRecordOperation::new(self.request.document_id);
        Ok(Next::Call(IoCall::child(record)))
    }

    fn next(&mut self, result: IoResult) -> StatusTurn {
        match self.stage {
            RoutedStage::Record => {
                let record = live_record(result)?;
                if self.local_node.is_none() {
                    self.stage = RoutedStage::Local;
                    return Ok(Next::Call(IoCall::child(self.local(Some(record)))));
                }
                self.expected_revision = record.last_event_id;
                self.stage = RoutedStage::Config;
                Ok(Next::Call(IoCall::child(GetConfigOperation::new(
                    self.realm_id,
                ))))
            }
            RoutedStage::Local => Ok(Next::Done(
                result.child::<ProfileValidationStatus, MetadataApiError>()??,
            )),
            RoutedStage::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|error| {
                    config_unavailable(&error);
                    MetadataApiError::ServiceUnavailable
                })?;
                self.route(config)
            }
            RoutedStage::Holders => {
                let IoResult::Group(GroupEvent::Finished { results }) = result else {
                    return Err(MetadataApiError::ServiceUnavailable);
                };
                self.collect(results)
            }
        }
    }
}

#[cfg(test)]
#[path = "routed_status_tests.rs"]
mod call_tests;
