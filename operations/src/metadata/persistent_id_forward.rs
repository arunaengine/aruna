//! Routes persistent identifier requests to the one node that owns that document's PID state.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::MetaResourceId;
use aruna_core::NodeId;
use aruna_core::StructuredId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::metadata::AuthToken;
use aruna_core::operation::Operation;
use aruna_core::placement::resolve_shard_holders;
use aruna_core::structs::MintPersistentSpec;
use aruna_core::structs::PersistentIdMapping;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::binding_directory::BindingError;
use aruna_core::structs::placement::record::{
    DocumentClass, FIRST_GRANTABLE_HANDLE, PlacementScope,
};
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use tracing::warn;
use ulid::Ulid;

use super::{MappingReadOperation, PidChange, PidChangeOperation};
use crate::auth::error::AuthorizeError;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::forward::authorize::ForwardAuthError;
use crate::forward::authorize::ForwardedCallerOperation;
use crate::forward::authorize::forward_auth_error;
use crate::forward::authorize::write_check;
use crate::forward::replay::DeletedHereOperation;
use crate::forward::routing::{ForwardHoldersOperation, HolderWriteError};
use crate::forward::transport::MetadataWriteError;
use crate::forward::transport::read_error;
use crate::forward::transport::reject;
use crate::forward::transport::write_error;
use crate::jobs::submit::{SubmitJobError, SubmitJobResult};
use crate::jobs::submit_local::SubmitLocalOperation;
use crate::metadata::api::MetadataApiError;
use crate::metadata::api::readable::RecordReadableOperation;
use crate::metadata::create_document::resolve_metadata_id;
use crate::metadata::id_mapping::PersistentIdError;
use crate::metadata::pid_transition::PidTransition;
use crate::metadata::protocol::MetadataReadError;
use crate::metadata::protocol::MetadataTransportMessage;
use crate::metadata::protocol::PersistentIdOutcome;
use crate::metadata::protocol::PersistentIdRequest;
use crate::metadata::protocol::PersistentIdResolution;
use crate::metadata::record_reads::DocumentRecordOperation;
use crate::metadata::repository::StorageReadError;
use crate::realm::load_config::config_unavailable;

/// The one node that owns a document's PID state: rank-0 holder of the placement
/// derived from the structured id, never the registry row (a delete removes it
/// while the mapping must survive for a permanent 410). All nodes derive the same.
pub(crate) fn pid_authority_node(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    document_id: Ulid,
) -> Option<NodeId> {
    let placement = resolve_metadata_id(config, realm_id, None, document_id).ok()?;
    resolve_shard_holders(config, &placement).first().copied()
}

/// True only when the id provably names no metadata placement of this realm: it is no
/// structured id, or its binding has another class or realm. Unknown, conflicting or partial
/// bindings may still be replicating, so they stay an unreachable authority.
fn outside_realm(config: &RealmConfigDocument, realm_id: RealmId, document_id: Ulid) -> bool {
    let Ok(id) = MetaResourceId::from_bytes(document_id.to_bytes()) else {
        return true;
    };
    let bucket_count = |strategy_id: Ulid| {
        config
            .strategy(&strategy_id)
            .and_then(|strategy| u16::try_from(strategy.shard_count).ok())
    };
    match config.binding_directory().resolve_id(&id, bucket_count) {
        Ok(binding) => {
            binding.document_class != DocumentClass::Metadata
                || matches!(binding.scope, PlacementScope::Realm(owner) if owner != realm_id)
        }
        Err(_) => false,
    }
}

/// True when the id's handle has no binding here and lies in no granted handle range, so no
/// node can have bound it yet; a covered handle may still be replicating its binding.
fn unplaced(config: &RealmConfigDocument, document_id: Ulid) -> bool {
    let Ok(id) = MetaResourceId::from_bytes(document_id.to_bytes()) else {
        return false;
    };
    let handle = id.placement_handle();
    let unbound = matches!(
        config.binding_directory().resolve(handle),
        Err(BindingError::Unknown(_))
    );
    unbound
        && handle.get() >= FIRST_GRANTABLE_HANDLE
        && !config
            .placement_handle_ranges
            .iter()
            .any(|range| range.contains(handle.get()))
}

/// The realm config a PID route needs, or why it has none.
fn pid_config(result: IoResult) -> Result<RealmConfigDocument, MetadataApiError> {
    (result.child::<RealmConfigDocument, GetConfigError>()?).map_err(|error| {
        config_unavailable(&error);
        MetadataApiError::ServiceUnavailable
    })
}

/// The mapping and whether this call changed it, from a mapping outcome.
pub fn outcome_mapping(
    outcome: PersistentIdOutcome,
) -> Result<(PersistentIdMapping, bool), MetadataApiError> {
    match outcome {
        PersistentIdOutcome::Mapping { mapping, changed } => Ok((*mapping, changed)),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// The stored mapping, from a status outcome.
pub fn outcome_status(
    outcome: PersistentIdOutcome,
) -> Result<Option<PersistentIdMapping>, MetadataApiError> {
    match outcome {
        PersistentIdOutcome::Status(mapping) => Ok(mapping.map(|mapping| *mapping)),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// The landing answer, from a resolution outcome.
pub fn outcome_resolution(
    outcome: PersistentIdOutcome,
) -> Result<PersistentIdResolution, MetadataApiError> {
    match outcome {
        PersistentIdOutcome::Resolution(resolution) => Ok(resolution),
        _ => Err(MetadataApiError::ServiceUnavailable),
    }
}

/// The authority's mint job, from a submission outcome.
pub fn outcome_submission(outcome: PersistentIdOutcome) -> Result<SubmitJobResult, SubmitJobError> {
    match outcome {
        PersistentIdOutcome::Submission { job_id, created } => {
            Ok(SubmitJobResult { job_id, created })
        }
        _ => Err(SubmitJobError::PlacementUnavailable(
            "the authority answered without a job".to_string(),
        )),
    }
}

/// A job submission's failure as the routed mint reports it.
pub fn submission_error(error: MetadataApiError) -> SubmitJobError {
    match error {
        MetadataApiError::NotFound => SubmitJobError::DocumentMissing,
        MetadataApiError::Unauthorized | MetadataApiError::Forbidden => {
            SubmitJobError::AuthorityDenied
        }
        error => SubmitJobError::PlacementUnavailable(error.to_string()),
    }
}

fn write_denied(error: ForwardAuthError) -> MetadataApiError {
    match error {
        ForwardAuthError::Unauthorized => MetadataApiError::Unauthorized,
        ForwardAuthError::Forbidden => MetadataApiError::Forbidden,
        ForwardAuthError::Unavailable(_) => MetadataApiError::ServiceUnavailable,
    }
}

#[derive(Debug, PartialEq)]
enum ResolveStage {
    Mapping,
    Record(PersistentIdMapping),
    Deleted(PersistentIdMapping),
    Readable,
}

/// The authority's own landing answer: a withdrawn mapping of a public intent is a permanent
/// 410, an active one redirects only while the document is anonymously readable now, and
/// everything else is indistinguishable from unminted.
#[derive(Debug, PartialEq)]
pub(crate) struct LocalResolveOperation {
    realm_id: RealmId,
    document_id: Ulid,
    expected_pid: String,
    stage: ResolveStage,
}

impl LocalResolveOperation {
    pub(crate) fn new(realm_id: RealmId, document_id: Ulid, expected_pid: String) -> Self {
        Self {
            realm_id,
            document_id,
            expected_pid,
            stage: ResolveStage::Mapping,
        }
    }

    fn mapped(&mut self, mapping: Option<PersistentIdMapping>) -> Next<PersistentIdResolution> {
        let Some(mapping) = mapping.filter(|mapping| mapping.pid == self.expected_pid) else {
            return Next::Done(PersistentIdResolution::Missing);
        };
        if mapping.is_retired() {
            // The record may be gone, so a withdrawal keeps the visibility frozen at creation.
            return Next::Done(match mapping.public {
                Some(false) => PersistentIdResolution::Missing,
                _ => PersistentIdResolution::Gone { pid: mapping.pid },
            });
        }
        if !mapping.is_active() {
            return Next::Done(PersistentIdResolution::Missing);
        }
        self.stage = ResolveStage::Record(mapping);
        Next::Call(IoCall::child(DocumentRecordOperation::new(
            self.document_id,
        )))
    }
}

impl Operation for LocalResolveOperation {
    type Output = PersistentIdResolution;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<PersistentIdResolution>, MetadataApiError> {
        Ok(Next::Call(IoCall::child(MappingReadOperation::new(
            self.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PersistentIdResolution>, MetadataApiError> {
        let unavailable = |_| MetadataApiError::ServiceUnavailable;
        match std::mem::replace(&mut self.stage, ResolveStage::Readable) {
            ResolveStage::Mapping => {
                let mapping = result.child::<Option<PersistentIdMapping>, PersistentIdError>()?;
                Ok(self.mapped(mapping.map_err(pid_error)?))
            }
            ResolveStage::Record(mapping) => {
                let record = result.child::<Option<MetadataRegistryRecord>, StorageReadError>()?;
                match record.map_err(unavailable)? {
                    Some(record) => {
                        let readable = RecordReadableOperation::new(self.realm_id, None, record);
                        Ok(Next::Call(IoCall::child(readable)))
                    }
                    // An active mapping without a registry row is a permanent 410 only once
                    // this node has evidence the document was created here and is gone.
                    None => {
                        self.stage = ResolveStage::Deleted(mapping);
                        let deleted = DeletedHereOperation::new(self.document_id);
                        Ok(Next::Call(IoCall::child(deleted)))
                    }
                }
            }
            ResolveStage::Deleted(mapping) => {
                match result.child::<bool, aruna_core::calls::IoError>()? {
                    Ok(true) => Ok(Next::Done(PersistentIdResolution::Gone {
                        pid: mapping.pid,
                    })),
                    _ => Err(MetadataApiError::ServiceUnavailable),
                }
            }
            ResolveStage::Readable => match result.child::<(), MetadataApiError>()? {
                Ok(()) => Ok(Next::Done(PersistentIdResolution::Redirect)),
                Err(_) => Ok(Next::Done(PersistentIdResolution::Missing)),
            },
        }
    }
}

/// The change a transition request asks for.
fn transition(request: &PersistentIdRequest) -> Option<PidChange> {
    let transition = match request.clone() {
        PersistentIdRequest::Mint {
            minted_by,
            minted_at_ms,
        } => PidTransition::Mint {
            minted_by,
            minted_at_ms,
        },
        PersistentIdRequest::Withdraw {
            withdrawn_by,
            reason,
            withdrawn_at_ms,
        } => PidTransition::Withdraw {
            withdrawn_by,
            reason,
            withdrawn_at_ms,
        },
        PersistentIdRequest::Fail { failure } => PidTransition::Fail { failure },
        PersistentIdRequest::AddIdentifiers {
            identifiers,
            occurred_at_ms,
        } => {
            return Some(PidChange::AddIdentifiers {
                identifiers,
                occurred_at_ms,
            });
        }
        PersistentIdRequest::SubmitMint { .. }
        | PersistentIdRequest::Resolve { .. }
        | PersistentIdRequest::Status => return None,
    };
    Some(PidChange::Transition(transition))
}

/// The realm a PID request is for and this node on the network, if it has one.
pub type PidNode = (RealmId, Option<NodeId>);

#[derive(Debug, PartialEq)]
enum LocalStage {
    Answer,
    Record,
    Checked,
}

/// Answers one PID request on this node as its authority. Adding identifiers needs the
/// caller's WRITE on the document first.
#[derive(Debug, PartialEq)]
pub(crate) struct LocalPidOperation {
    node: PidNode,
    document_id: Ulid,
    request: PersistentIdRequest,
    caller: Option<AuthContext>,
    owner: Option<NodeId>,
    now_ms: u64,
    stage: LocalStage,
}

impl LocalPidOperation {
    pub(crate) fn new(
        node: PidNode,
        document_id: Ulid,
        request: PersistentIdRequest,
        caller: Option<AuthContext>,
    ) -> Self {
        Self {
            node,
            document_id,
            request,
            caller,
            owner: None,
            now_ms: 0,
            stage: LocalStage::Answer,
        }
    }

    /// The node that owns a mint job submitted here; a networked node owns its own jobs.
    pub(crate) fn with_owner(mut self, owner: Option<NodeId>) -> Self {
        self.owner = owner;
        self
    }

    fn answer(&mut self) -> Result<Next<PersistentIdOutcome>, MetadataApiError> {
        let (realm_id, local_node) = self.node;
        self.stage = LocalStage::Answer;
        let call = match &self.request {
            PersistentIdRequest::Status => {
                IoCall::child(MappingReadOperation::new(self.document_id))
            }
            PersistentIdRequest::Resolve { pid } => IoCall::child(LocalResolveOperation::new(
                realm_id,
                self.document_id,
                pid.clone(),
            )),
            PersistentIdRequest::SubmitMint {
                minted_by,
                retention_ms,
            } => {
                let spec = MintPersistentSpec {
                    document_id: self.document_id,
                    minted_by: *minted_by,
                };
                let serving = local_node.map(|node| (node, realm_id));
                let owner =
                    (local_node.or(self.owner)).ok_or(MetadataApiError::ServiceUnavailable)?;
                IoCall::child(SubmitLocalOperation::mint_pid(
                    spec,
                    realm_id,
                    owner,
                    *retention_ms,
                    serving,
                    self.now_ms,
                ))
            }
            request => {
                let change = transition(request).ok_or(MetadataApiError::BadRequest)?;
                let change =
                    PidChangeOperation::new((realm_id, local_node), self.document_id, change);
                IoCall::child(change)
            }
        };
        Ok(Next::Call(call))
    }

    fn answered(&self, result: IoResult) -> Result<PersistentIdOutcome, MetadataApiError> {
        Ok(match &self.request {
            PersistentIdRequest::Status => {
                let mapping = result.child::<Option<PersistentIdMapping>, PersistentIdError>()?;
                PersistentIdOutcome::Status(mapping.map_err(pid_error)?.map(Box::new))
            }
            PersistentIdRequest::Resolve { .. } => PersistentIdOutcome::Resolution(
                result.child::<PersistentIdResolution, MetadataApiError>()??,
            ),
            PersistentIdRequest::SubmitMint { .. } => {
                let submitted = result.child::<SubmitJobResult, SubmitJobError>()?;
                let submitted =
                    submitted.map_err(|error| MetadataApiError::Internal(error.to_string()))?;
                PersistentIdOutcome::Submission {
                    job_id: submitted.job_id,
                    created: submitted.created,
                }
            }
            _ => {
                let changed = result.child::<(PersistentIdMapping, bool), PersistentIdError>()?;
                let (mapping, changed) = changed.map_err(pid_error)?;
                PersistentIdOutcome::Mapping {
                    mapping: Box::new(mapping),
                    changed,
                }
            }
        })
    }
}

impl Operation for LocalPidOperation {
    type Output = PersistentIdOutcome;
    type Error = MetadataApiError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<PersistentIdOutcome>, MetadataApiError> {
        if !matches!(self.request, PersistentIdRequest::AddIdentifiers { .. }) {
            return self.answer();
        }
        self.stage = LocalStage::Record;
        Ok(Next::Call(IoCall::child(DocumentRecordOperation::new(
            self.document_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PersistentIdOutcome>, MetadataApiError> {
        match self.stage {
            LocalStage::Record => {
                let record = result.child::<Option<MetadataRegistryRecord>, StorageReadError>()?;
                let record = record
                    .map_err(|error| {
                        MetadataApiError::Internal(format!(
                            "metadata registry read failed: {error:?}"
                        ))
                    })?
                    .ok_or(MetadataApiError::NotFound)?;
                let caller = self.caller.as_ref().ok_or(MetadataApiError::Unauthorized)?;
                self.stage = LocalStage::Checked;
                Ok(Next::Call(IoCall::child(write_check(
                    caller,
                    &record.permission_path,
                ))))
            }
            LocalStage::Checked => {
                let checked = result.child::<(), AuthorizeError>()?;
                checked
                    .map_err(ForwardAuthError::from)
                    .map_err(write_denied)?;
                self.answer()
            }
            LocalStage::Answer => Ok(Next::Done(self.answered(result)?)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum RouteStage {
    Known,
    Config,
    Local,
    Forward,
}

/// Runs one PID request on the document's authority: here when this node is it or has no
/// network, otherwise forwarded to it. A mint job submission first asks the authority for a
/// job the mapping already names, so alternating ingress nodes join one job.
#[derive(Debug, PartialEq)]
pub struct PidRouteOperation {
    node: PidNode,
    document_id: Ulid,
    request: PersistentIdRequest,
    auth_token: Option<AuthToken>,
    caller: Option<AuthContext>,
    owner: Option<NodeId>,
    stage: RouteStage,
}

impl PidRouteOperation {
    /// `caller` is checked for WRITE on the document before identifiers are added.
    pub fn new(
        node: PidNode,
        document_id: Ulid,
        request: PersistentIdRequest,
        (auth_token, caller): (Option<AuthToken>, Option<AuthContext>),
    ) -> Self {
        Self {
            node,
            document_id,
            request,
            auth_token,
            caller,
            owner: None,
            stage: RouteStage::Config,
        }
    }

    /// The node that owns a mint job submitted on a node without a network.
    pub fn with_owner(mut self, owner: NodeId) -> Self {
        self.owner = Some(owner);
        self
    }

    fn local(&mut self) -> Result<Next<PersistentIdOutcome>, MetadataApiError> {
        self.stage = RouteStage::Local;
        let local = LocalPidOperation::new(
            self.node,
            self.document_id,
            self.request.clone(),
            self.caller.clone(),
        )
        .with_owner(self.owner);
        Ok(Next::Call(IoCall::child(local)))
    }

    fn route(&mut self) -> Result<Next<PersistentIdOutcome>, MetadataApiError> {
        let (realm_id, local_node) = self.node;
        if local_node.is_none() {
            return self.local();
        }
        self.stage = RouteStage::Config;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn configured(
        &mut self,
        config: RealmConfigDocument,
    ) -> Result<Next<PersistentIdOutcome>, MetadataApiError> {
        let (realm_id, local_node) = self.node;
        let node_id = local_node.ok_or(MetadataApiError::ServiceUnavailable)?;
        let resolve = matches!(self.request, PersistentIdRequest::Resolve { .. });
        let missing = PersistentIdOutcome::Resolution(PersistentIdResolution::Missing);
        if resolve && outside_realm(&config, realm_id, self.document_id) {
            return Ok(Next::Done(missing));
        }
        let authority = match pid_authority_node(&config, realm_id, self.document_id) {
            Some(authority) => authority,
            None if resolve && unplaced(&config, self.document_id) => {
                let unplaced = PersistentIdResolution::Unplaced;
                return Ok(Next::Done(PersistentIdOutcome::Resolution(unplaced)));
            }
            None => return Err(MetadataApiError::ServiceUnavailable),
        };
        if authority == node_id {
            return self.local();
        }
        let config_digest = config
            .digest()
            .map_err(|_| MetadataApiError::ServiceUnavailable)?;
        let auth_token = match &self.request {
            // Identifiers carry the caller's own authority to the authority's WRITE check.
            PersistentIdRequest::AddIdentifiers { .. } => {
                self.caller.clone().map(AuthToken::internal)
            }
            _ => self.auth_token.clone(),
        };
        let message = MetadataTransportMessage::ForwardPersistentId {
            auth_token,
            config_digest,
            document_id: self.document_id,
            request: self.request.clone(),
        };
        self.stage = RouteStage::Forward;
        let forward =
            ForwardHoldersOperation::new(Some(node_id), &[authority], &message, None, false);
        Ok(Next::Call(IoCall::child(forward)))
    }

    fn forwarded(&self, result: IoResult) -> Result<PersistentIdOutcome, MetadataApiError> {
        let response = result.child::<MetadataTransportMessage, HolderWriteError>()?;
        let response = response
            .map_err(MetadataWriteError::from)
            .map_err(write_error)?;
        match response {
            MetadataTransportMessage::ForwardedPersistentId {
                result: Ok(outcome),
            } => Ok(outcome),
            MetadataTransportMessage::ForwardedPersistentId { result: Err(error) } => {
                Err(match error {
                    MetadataReadError::Unauthorized => MetadataApiError::Unauthorized,
                    MetadataReadError::Forbidden => MetadataApiError::Forbidden,
                    MetadataReadError::NotFound => MetadataApiError::NotFound,
                    MetadataReadError::Unavailable | MetadataReadError::TooLarge => {
                        MetadataApiError::ServiceUnavailable
                    }
                })
            }
            _ => Err(MetadataApiError::ServiceUnavailable),
        }
    }
}

impl Operation for PidRouteOperation {
    type Output = PersistentIdOutcome;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<PersistentIdOutcome>, MetadataApiError> {
        if !matches!(self.request, PersistentIdRequest::SubmitMint { .. }) {
            return self.route();
        }
        self.stage = RouteStage::Known;
        let known = Self::new(
            self.node,
            self.document_id,
            PersistentIdRequest::Status,
            (self.auth_token.clone(), None),
        );
        Ok(Next::Call(IoCall::child(known)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<PersistentIdOutcome>, MetadataApiError> {
        match self.stage {
            RouteStage::Known => {
                let known =
                    outcome_status(result.child::<PersistentIdOutcome, MetadataApiError>()??)?;
                if let Some(job_id) = known.and_then(|mapping| mapping.job_id) {
                    let joined = PersistentIdOutcome::Submission {
                        job_id,
                        created: false,
                    };
                    return Ok(Next::Done(joined));
                }
                self.route()
            }
            RouteStage::Config => {
                let config = pid_config(result)?;
                self.configured(config)
            }
            RouteStage::Local => Ok(Next::Done(
                result.child::<PersistentIdOutcome, MetadataApiError>()??,
            )),
            RouteStage::Forward => Ok(Next::Done(self.forwarded(result)?)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum ServeStage {
    Config,
    Read,
    Caller,
    Record(AuthContext),
    Checked,
    Answer,
}

/// Applies a PID transition or answers a landing resolution or status read forwarded to this
/// node as the document's authority. Transitions carry the caller's authority, so the WRITE
/// check of the origin's handler runs here again; a mint is attributed only to the token's own
/// subject.
#[derive(Debug, PartialEq)]
pub(crate) struct ServePidOperation {
    peer: NodeId,
    node: PidNode,
    config_digest: [u8; 32],
    document_id: Ulid,
    request: PersistentIdRequest,
    auth_token: Option<AuthToken>,
    stage: ServeStage,
}

impl ServePidOperation {
    /// `None` when `message` is no forwarded PID request.
    pub(crate) fn new(
        peer: NodeId,
        (realm_id, node_id): (RealmId, NodeId),
        message: MetadataTransportMessage,
    ) -> Option<Self> {
        let MetadataTransportMessage::ForwardPersistentId {
            auth_token,
            config_digest,
            document_id,
            request,
        } = message
        else {
            return None;
        };
        Some(Self {
            peer,
            node: (realm_id, Some(node_id)),
            config_digest,
            document_id,
            request,
            auth_token,
            stage: ServeStage::Config,
        })
    }

    fn local(&self, caller: Option<AuthContext>) -> IoCall {
        let local =
            LocalPidOperation::new(self.node, self.document_id, self.request.clone(), caller);
        IoCall::child(local)
    }

    fn served(
        result: Result<PersistentIdOutcome, MetadataReadError>,
    ) -> Next<MetadataTransportMessage> {
        Next::Done(MetadataTransportMessage::ForwardedPersistentId { result })
    }

    fn configured(
        &mut self,
        config: Option<RealmConfigDocument>,
    ) -> Next<MetadataTransportMessage> {
        let (realm_id, local_node) = self.node;
        let Some(node_id) = local_node else {
            return Next::Done(MetadataTransportMessage::ForwardedWriteUnavailable);
        };
        let Some(config) = config.filter(|config| config.digest().ok() == Some(self.config_digest))
        else {
            return Next::Done(MetadataTransportMessage::ForwardedWriteUnavailable);
        };
        if pid_authority_node(&config, realm_id, self.document_id) != Some(node_id) {
            return Next::Done(MetadataTransportMessage::ForwardedWriteUnavailable);
        }
        if matches!(
            self.request,
            PersistentIdRequest::Resolve { .. } | PersistentIdRequest::Status
        ) {
            self.stage = ServeStage::Read;
            return Next::Call(self.local(None));
        }
        self.stage = ServeStage::Caller;
        let caller =
            ForwardedCallerOperation::for_token(self.peer, realm_id, self.auth_token.clone());
        Next::Call(IoCall::child(caller))
    }

    fn caller(&mut self, auth: AuthContext) -> Next<MetadataTransportMessage> {
        // The minting subject is the token's own subject: a routing hop may not attribute a
        // mint to a user it merely relays for.
        let subject = match &self.request {
            PersistentIdRequest::Mint { minted_by, .. }
            | PersistentIdRequest::SubmitMint { minted_by, .. } => Some(*minted_by),
            PersistentIdRequest::Withdraw { withdrawn_by, .. } => Some(*withdrawn_by),
            _ => None,
        };
        if subject.is_some_and(|subject| subject != auth.user_id) {
            return Next::Done(forward_auth_error(ForwardAuthError::Forbidden));
        }
        self.stage = ServeStage::Record(auth);
        Next::Call(IoCall::child(DocumentRecordOperation::new(
            self.document_id,
        )))
    }

    fn recorded(
        &mut self,
        auth: AuthContext,
        record: Option<MetadataRegistryRecord>,
    ) -> Next<MetadataTransportMessage> {
        let (realm_id, _) = self.node;
        let internal = matches!(&self.auth_token, Some(AuthToken::Internal(_)));
        let path = match (&self.request, record.as_ref()) {
            (PersistentIdRequest::Fail { .. }, _) if !internal => {
                return Next::Done(forward_auth_error(ForwardAuthError::Forbidden));
            }
            (PersistentIdRequest::Mint { .. } | PersistentIdRequest::Fail { .. }, _)
                if internal =>
            {
                return self.apply();
            }
            (PersistentIdRequest::Withdraw { .. }, Some(_)) => {
                format!("/{realm_id}/admin/pids/{}", self.document_id)
            }
            (_, Some(record)) => record.permission_path.clone(),
            // Without a registry row there is no permission path that can authorize it.
            (_, None) => return Next::Done(MetadataTransportMessage::WriteNotFound),
        };
        self.stage = ServeStage::Checked;
        Next::Call(IoCall::child(write_check(&auth, &path)))
    }

    /// The change itself; identifiers were checked above, so the local answer needs no caller.
    fn apply(&mut self) -> Next<MetadataTransportMessage> {
        self.stage = ServeStage::Answer;
        if let PersistentIdRequest::AddIdentifiers { .. } = &self.request {
            let change = transition(&self.request);
            if let Some(change) = change {
                let change = PidChangeOperation::new(self.node, self.document_id, change);
                return Next::Call(IoCall::child(change));
            }
        }
        Next::Call(self.local(None))
    }

    fn answered(
        &self,
        result: IoResult,
    ) -> Result<Next<MetadataTransportMessage>, MetadataApiError> {
        let outcome = match &self.request {
            PersistentIdRequest::AddIdentifiers { .. } => {
                let changed = result.child::<(PersistentIdMapping, bool), PersistentIdError>()?;
                changed
                    .map(|(mapping, changed)| PersistentIdOutcome::Mapping {
                        mapping: Box::new(mapping),
                        changed,
                    })
                    .map_err(pid_error)
            }
            _ => result.child::<PersistentIdOutcome, MetadataApiError>()?,
        };
        Ok(Next::Done(match outcome {
            Ok(outcome) => MetadataTransportMessage::ForwardedPersistentId {
                result: Ok(outcome),
            },
            Err(MetadataApiError::NotFound) => MetadataTransportMessage::WriteNotFound,
            Err(error) => {
                warn!(document_id = %self.document_id, ?error, "Forwarded persistent id request failed");
                MetadataTransportMessage::ForwardedWriteUnavailable
            }
        }))
    }
}

impl Operation for ServePidOperation {
    type Output = MetadataTransportMessage;
    type Error = MetadataApiError;

    fn first(&mut self) -> Result<Next<MetadataTransportMessage>, MetadataApiError> {
        let (realm_id, _) = self.node;
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(realm_id))))
    }

    fn next(
        &mut self,
        result: IoResult,
    ) -> Result<Next<MetadataTransportMessage>, MetadataApiError> {
        match std::mem::replace(&mut self.stage, ServeStage::Answer) {
            ServeStage::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.inspect_err(config_unavailable).ok();
                Ok(self.configured(config))
            }
            ServeStage::Read => {
                let outcome = result.child::<PersistentIdOutcome, MetadataApiError>()?;
                Ok(Self::served(outcome.map_err(|error| match self.request {
                    PersistentIdRequest::Status => MetadataReadError::Unavailable,
                    _ => read_error(error),
                })))
            }
            ServeStage::Caller => match result.child::<AuthContext, ForwardAuthError>()? {
                Ok(auth) => Ok(self.caller(auth)),
                Err(error) => Ok(Next::Done(forward_auth_error(error))),
            },
            ServeStage::Record(auth) => {
                match result.child::<Option<MetadataRegistryRecord>, StorageReadError>()? {
                    Ok(record) => Ok(self.recorded(auth, record)),
                    Err(error) => Ok(Next::Done(reject(format!(
                        "metadata registry read failed: {error:?}"
                    )))),
                }
            }
            ServeStage::Checked => match result.child::<(), AuthorizeError>()? {
                Ok(()) => Ok(self.apply()),
                Err(error) => Ok(Next::Done(forward_auth_error(error.into()))),
            },
            ServeStage::Answer => self.answered(result),
        }
    }
}

pub(crate) fn pid_error(error: PersistentIdError) -> MetadataApiError {
    match error {
        PersistentIdError::DocumentMissing | PersistentIdError::IntentMissing => {
            MetadataApiError::NotFound
        }
        error => MetadataApiError::Internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::structs::placement::record::{
        FIRST_GRANTABLE_HANDLE, HANDLE_RANGE_SIZE, HandleRange, METADATA_HANDLE, PlacementBinding,
    };
    use aruna_core::{BucketId, PlacementHandle, StructuredId};

    fn minted(handle: u32) -> Ulid {
        let handle = PlacementHandle::new(handle).unwrap();
        let bucket = BucketId::new(0).unwrap();
        MetaResourceId::from_parts(1, handle, bucket, 7)
            .unwrap()
            .as_ulid()
    }

    #[test]
    fn outside_only_provably() {
        let realm_id = RealmId::from_bytes([5; 32]);
        let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
        config.seed_default_placement();
        let strategy_id = config.default_strategy_id.unwrap();
        let owner = iroh::SecretKey::from_bytes(&[8; 32]).public();
        let range_id = Ulid::from_bytes([19; 16]);
        config.placement_handle_ranges.push(HandleRange {
            range_id,
            owner,
            start: FIRST_GRANTABLE_HANDLE,
            end: FIRST_GRANTABLE_HANDLE + HANDLE_RANGE_SIZE,
        });
        let bind = |handle, scope, document_class| PlacementBinding {
            handle: PlacementHandle::new(handle).unwrap(),
            scope,
            document_class,
            strategy_id,
            allocator_range_id: Some(range_id),
            allocated_by: Some(owner),
            allocated_at_ms: Some(1),
        };
        let other = RealmId::from_bytes([6; 32]);
        config.placement_bindings.extend([
            bind(
                500,
                PlacementScope::Realm(realm_id),
                DocumentClass::JobControl,
            ),
            bind(501, PlacementScope::Realm(other), DocumentClass::Metadata),
            bind(
                502,
                PlacementScope::Group(Ulid::from_bytes([1; 16])),
                DocumentClass::Metadata,
            ),
            bind(
                504,
                PlacementScope::Realm(realm_id),
                DocumentClass::Metadata,
            ),
            bind(504, PlacementScope::Realm(other), DocumentClass::Metadata),
        ]);
        // No structured id, another class or another realm: never minted here, a 404.
        let reserved = Ulid::from_parts(1_700_000_000_000, 0x1234_5678);
        assert!(outside_realm(&config, realm_id, reserved));
        assert!(outside_realm(&config, realm_id, minted(500)));
        assert!(outside_realm(&config, realm_id, minted(501)));
        assert!(!outside_realm(&config, realm_id, minted(METADATA_HANDLE)));
        assert!(!outside_realm(&config, realm_id, minted(502)));
        // An unbound, conflicting or out of range placement, as a v2 id carries, may still be
        // replicating: unavailable, not 404.
        let legacy = Ulid::from_string("01H8Y3R7Z5X9C2V4B6N8M0Q1W3").unwrap();
        assert!(!outside_realm(&config, realm_id, legacy));
        assert!(!outside_realm(&config, realm_id, minted(503)));
        assert!(!outside_realm(&config, realm_id, minted(504)));
        let handle = PlacementHandle::new(METADATA_HANDLE).unwrap();
        let far = MetaResourceId::from_parts(1, handle, BucketId::new(4095).unwrap(), 7).unwrap();
        assert!(!outside_realm(&config, realm_id, far.as_ulid()));
        // Only an unbound handle that no granted range covers is unplaced; the rest may replicate.
        assert!(unplaced(
            &config,
            minted(FIRST_GRANTABLE_HANDLE + HANDLE_RANGE_SIZE)
        ));
        for handle in [METADATA_HANDLE, 2, 503, 504] {
            assert!(!unplaced(&config, minted(handle)), "{handle}");
        }
        assert!(!unplaced(&config, reserved));
        // A strategy unknown here is indeterminate too.
        config
            .strategies
            .retain(|strategy| strategy.strategy_id != strategy_id);
        assert!(!outside_realm(&config, realm_id, minted(METADATA_HANDLE)));
    }
}
