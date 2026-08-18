use std::collections::{HashSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
use aruna_core::errors::{BlobError, ConversionError};
use aruna_core::events::{BlobEvent, Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{
    AUTH_KEYSPACE, BLOB_VERSIONS_KEYSPACE, REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
};
use aruna_core::operation::{Operation, boxed_suboperation};
use aruna_core::request_policy::{CompiledPolicySet, PolicyDecision, PolicyFunctions};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::{
    BackendLocation, BlobLocationKey, BlobVersion, BucketInfo, GroupAuthorizationDocument,
    HashPathIndexKey, ManagedCopyKey, Permission, PlacementPolicyRef, RealmConfigDocument, RealmId,
    VersionKey, VersionedObjectArn, blob_object_permission_path,
};
use aruna_core::types::{Effects, GroupId, TxnId};
use bytes::Bytes;
use byteview::ByteView;
use smallvec::smallvec;
use thiserror::Error;
use ulid::Ulid;

use crate::blob::managed_copy::{
    CopyRequest, serve_reads, split_serve_reads, validate_registration,
};
use crate::driver::{DriverContext, GateContextError, drive, gate_context, now_ms};
use crate::placement_policy::{
    GateContext, PolicyGateError, PolicyGateOperation, gate_decision, union_refs, write_gate,
};

use super::protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget, VersionReplicationMessage};
use crate::blob::blob_keyspace_helper::blob_location_read;
use crate::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use crate::realm_peer::ensure_realm_peer;
use crate::request_policy::{PolicyRequestExtras, policy_request_with};

#[derive(Debug, PartialEq)]
pub enum BaoReadOutput {
    Metadata {
        size: u64,
        blake3: [u8; 32],
    },
    Stream {
        blob: BackendStream<Result<Bytes, StreamError>>,
        size: u64,
        blake3: [u8; 32],
    },
}

#[derive(Debug, Error, PartialEq)]
pub enum BaoReadError {
    #[error("bao read was refused: {0:?}")]
    Refused(BaoReadRefusal),
    #[error(transparent)]
    Blob(#[from] BlobError),
    #[error(transparent)]
    Conversion(#[from] ConversionError),
    #[error("unexpected event in state {state}: {event}")]
    Unexpected { state: &'static str, event: String },
    #[error("bao read did not finish")]
    NotFinished,
    #[error(transparent)]
    ManagedCopy(#[from] crate::blob::managed_copy::ManagedCopyError),
    /// The requester must resolve these rules and ask again.
    #[error("the destination has not resolved every required placement policy")]
    PolicyRequired { refs: Vec<PlacementPolicyRef> },
    #[error("placement policy denies this destination")]
    PolicyDenied { policy_ids: Vec<Ulid> },
    #[error(transparent)]
    Gate(#[from] PolicyGateError),
    /// This node advertises no subject, or stopped admitting while in
    /// transition, so nothing governed may land here.
    #[error("this node is not a legal destination for governed data")]
    NoDestination,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BaoReadState {
    Init,
    Open,
    Send,
    ReadResponse,
    Receive,
    CloseMetadata,
    Close,
    Finish,
    Error,
}

#[derive(Debug, PartialEq)]
pub struct BaoReadOperation {
    node_id: NodeId,
    request: BaoReadRequest,
    stream_id: Option<Ulid>,
    state: BaoReadState,
    output: Option<Result<BaoReadOutput, BaoReadError>>,
    close_error: Option<BaoReadError>,
    accepted_blake3: Option<[u8; 32]>,
}

impl BaoReadOperation {
    pub fn new(node_id: NodeId, request: BaoReadRequest) -> Self {
        Self {
            node_id,
            request,
            stream_id: None,
            state: BaoReadState::Init,
            output: None,
            close_error: None,
            accepted_blake3: None,
        }
    }

    fn fail(&mut self, error: BaoReadError) -> Effects {
        let Some(stream_id) = self.stream_id else {
            self.state = BaoReadState::Error;
            self.output = Some(Err(error));
            return smallvec![];
        };
        self.state = BaoReadState::Close;
        self.close_error = Some(error);
        smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        self.fail(BaoReadError::Unexpected {
            state: self.state_name(),
            event: format!("{event:?}"),
        })
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            BaoReadState::Init => "init",
            BaoReadState::Open => "open",
            BaoReadState::Send => "send",
            BaoReadState::ReadResponse => "read_response",
            BaoReadState::Receive => "receive",
            BaoReadState::CloseMetadata => "close_metadata",
            BaoReadState::Close => "close",
            BaoReadState::Finish => "finish",
            BaoReadState::Error => "error",
        }
    }
}

impl Operation for BaoReadOperation {
    type Output = BaoReadOutput;
    type Error = BaoReadError;

    fn start(&mut self) -> Effects {
        self.state = BaoReadState::Open;
        smallvec![Effect::Blob(BlobEffect::OpenConnection {
            node_id: self.node_id,
        })]
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Blob(BlobEvent::Error(error)) => return self.fail(error.into()),
            event => event,
        };

        match self.state {
            BaoReadState::Open => {
                let Event::Blob(BlobEvent::ConnectionEstablished { stream_id }) = event else {
                    return self.unexpected(event);
                };
                self.stream_id = Some(stream_id);
                self.state = BaoReadState::Send;
                let payload = match VersionReplicationMessage::BaoReadRequest(self.request.clone())
                    .to_bytes()
                {
                    Ok(payload) => payload,
                    Err(error) => return self.fail(error.into()),
                };
                smallvec![Effect::Blob(BlobEffect::SendMessage { stream_id, payload })]
            }
            BaoReadState::Send => {
                let Event::Blob(BlobEvent::MessageSent { stream_id }) = event else {
                    return self.unexpected(event);
                };
                self.state = BaoReadState::ReadResponse;
                smallvec![Effect::Blob(BlobEffect::ReadMessage { stream_id })]
            }
            BaoReadState::ReadResponse => {
                let Event::Blob(BlobEvent::MessageReceived { stream_id, payload }) = event else {
                    return self.unexpected(event);
                };
                match VersionReplicationMessage::from_bytes(&payload) {
                    Ok(VersionReplicationMessage::BaoReadAccepted { size, blake3 }) => {
                        if self
                            .request
                            .expected_blake3
                            .is_some_and(|expected| expected != blake3)
                        {
                            return self.fail(BaoReadError::Refused(BaoReadRefusal::HashMismatch));
                        }
                        if self.request.metadata_only {
                            self.output = Some(Ok(BaoReadOutput::Metadata { size, blake3 }));
                            self.state = BaoReadState::CloseMetadata;
                            return smallvec![Effect::Blob(BlobEffect::CloseConnection {
                                stream_id,
                            })];
                        }
                        self.accepted_blake3 = Some(blake3);
                        self.state = BaoReadState::Receive;
                        smallvec![Effect::Blob(BlobEffect::ReceiveRead {
                            stream_id,
                            size,
                            expected_blake3: blake3,
                        })]
                    }
                    Ok(VersionReplicationMessage::BaoReadRefused(reason)) => {
                        self.fail(BaoReadError::Refused(reason))
                    }
                    // The source teaches the rule before bytes: the caller
                    // resolves it, caches it, and retries only when compliant.
                    Ok(VersionReplicationMessage::PlacementPolicyRequired { refs }) => {
                        self.fail(BaoReadError::PolicyRequired { refs })
                    }
                    Ok(VersionReplicationMessage::PlacementPolicyDenied { policy_ids }) => {
                        self.fail(BaoReadError::PolicyDenied { policy_ids })
                    }
                    Ok(_) => self.fail(BaoReadError::Unexpected {
                        state: self.state_name(),
                        event: "unexpected bao read response".to_string(),
                    }),
                    Err(error) => self.fail(error.into()),
                }
            }
            BaoReadState::Receive => {
                let Event::Blob(BlobEvent::ReadFinished { blob, stream_size }) = event else {
                    return self.unexpected(event);
                };
                let Some(blake3) = self.accepted_blake3.take() else {
                    return self.fail(BaoReadError::NotFinished);
                };
                self.output = Some(Ok(BaoReadOutput::Stream {
                    blob,
                    size: stream_size,
                    blake3,
                }));
                self.state = BaoReadState::Finish;
                smallvec![]
            }
            BaoReadState::CloseMetadata => {
                let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
                    return self.unexpected(event);
                };
                self.state = BaoReadState::Finish;
                smallvec![]
            }
            BaoReadState::Close => {
                let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
                    return self.unexpected(event);
                };
                self.output = Some(Err(self
                    .close_error
                    .take()
                    .unwrap_or(BaoReadError::NotFinished)));
                self.state = BaoReadState::Error;
                smallvec![]
            }
            BaoReadState::Init => self.unexpected(event),
            BaoReadState::Finish | BaoReadState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(self.state, BaoReadState::Finish | BaoReadState::Error)
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(BaoReadError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        match self.stream_id {
            Some(stream_id) => {
                self.state = BaoReadState::Close;
                self.close_error = Some(BaoReadError::NotFinished);
                smallvec![Effect::Blob(BlobEffect::CloseConnection { stream_id })]
            }
            None => {
                self.state = BaoReadState::Error;
                self.output = Some(Err(BaoReadError::NotFinished));
                smallvec![]
            }
        }
    }
}

/// One teach-then-retry round. The source only ever teaches the refs it needs,
/// so a second `Required` for the same set is a protocol dead end, not a loop.
const CHALLENGE_ATTEMPTS: usize = 2;

/// A governed remote read with the plan's destination challenge (5.6/10).
///
/// The request carries this node's advertised subject, so the source can
/// evaluate it independently. On `PlacementPolicyRequired` the refs are
/// resolved through the ordinary policy resolver, which verifies publication
/// authority and caches the result, then evaluated locally; the read is retried
/// only when the local subject complies. Echoed refs are never authority.
pub async fn managed_read(
    context: &DriverContext,
    node_id: NodeId,
    mut request: BaoReadRequest,
) -> Result<BaoReadOutput, BaoReadError> {
    let destination = match gate_context(context, request.realm_id, now_ms()).await {
        Ok(destination) => destination,
        Err(GateContextError::AdmissionStopped) => {
            return Err(PolicyGateError::AdmissionStopped.into());
        }
        Err(GateContextError::Routing(_)) => return Err(BaoReadError::NoDestination),
    };
    request.destination = destination.as_ref().map(|gate| gate.subject.clone());
    let mut taught: Vec<PlacementPolicyRef> = Vec::new();
    for _ in 0..CHALLENGE_ATTEMPTS {
        let refs = match drive(BaoReadOperation::new(node_id, request.clone()), context).await {
            Err(BaoReadError::PolicyRequired { refs }) => refs,
            other => return other,
        };
        let refs = PlacementPolicyRef::canonical_set(&refs).map_err(ConversionError::from)?;
        if refs.is_empty() || refs == taught {
            return Err(BaoReadError::PolicyRequired { refs });
        }
        // The refs are only a hint: this node decides on its own resolution,
        // which also caches the verified publication for every later read.
        let Some(gate) = write_gate(destination.as_ref(), &refs)? else {
            return Err(BaoReadError::NoDestination);
        };
        let outcome = drive(gate, context).await.map_err(PolicyGateError::from)?;
        gate_decision(outcome.decision)?;
        request.known_refs = union_refs(&request.known_refs, &refs)?;
        taught = refs;
    }
    Err(BaoReadError::PolicyRequired { refs: taught })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncomingBaoReadResult {
    Served,
    Probed,
    Refused(BaoReadRefusal),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IncomingBaoReadState {
    Init,
    StartTransaction,
    ReadRealm,
    ReadExactVersion,
    ReadExactBucket,
    ReadPolicy,
    CheckPermission,
    ReadHashVersion,
    ReadLocation,
    CheckManagedCopy,
    PolicyChallenge,
    SendAccepted,
    ServeRead,
    CloseMetadata,
    CommitTransaction,
    SendRefusal,
    CloseRefusal,
    AbortTransaction,
    Finish,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PolicyNext {
    Exact,
    Hash,
}

#[derive(Debug, PartialEq)]
pub struct IncomingBaoReadOperation {
    peer: NodeId,
    local_node: NodeId,
    local_realm: RealmId,
    stream_id: Ulid,
    request: BaoReadRequest,
    state: IncomingBaoReadState,
    candidates: VecDeque<HashPathIndexKey>,
    candidate: Option<HashPathIndexKey>,
    blob_hash: Option<[u8; 32]>,
    location_key: Option<BlobLocationKey>,
    location: Option<BackendLocation>,
    candidates_ready: bool,
    had_denial: bool,
    refusal: Option<BaoReadRefusal>,
    output: Option<Result<IncomingBaoReadResult, BaoReadError>>,
    policy_paths: HashSet<String>,
    snapshot: bool,
    txn_id: Option<TxnId>,
    result: Option<IncomingBaoReadResult>,
    policy_path: Option<String>,
    policy_group: Option<GroupId>,
    policy_next: Option<PolicyNext>,
    policy_current: bool,
    /// The version this serve resolved and the refs it carries. A governed copy
    /// answers the destination challenge before a single byte is offered.
    version_key: Option<VersionKey>,
    version_refs: Vec<PlacementPolicyRef>,
    pending_location: Option<BackendLocation>,
    gate: Option<PolicyGateOperation>,
    now_ms: u64,
}

impl IncomingBaoReadOperation {
    pub fn new(
        peer: NodeId,
        local_node: NodeId,
        local_realm: RealmId,
        stream_id: Ulid,
        request: BaoReadRequest,
    ) -> Self {
        Self {
            peer,
            local_node,
            local_realm,
            stream_id,
            request,
            state: IncomingBaoReadState::Init,
            candidates: VecDeque::new(),
            candidate: None,
            blob_hash: None,
            location_key: None,
            location: None,
            candidates_ready: false,
            had_denial: false,
            refusal: None,
            output: None,
            policy_paths: HashSet::new(),
            snapshot: false,
            txn_id: None,
            result: None,
            policy_path: None,
            policy_group: None,
            policy_next: None,
            policy_current: false,
            version_key: None,
            version_refs: Vec::new(),
            pending_location: None,
            gate: None,
            now_ms: 0,
        }
    }

    /// Cache freshness for the challenge; the operation stays sans-I/O by
    /// taking the clock as configuration.
    pub fn with_now(mut self, now_ms: u64) -> Self {
        self.now_ms = now_ms;
        self
    }

    pub fn with_policy_paths(mut self, paths: HashSet<String>) -> Self {
        self.policy_paths = paths;
        self
    }

    pub fn with_policy_candidates(
        mut self,
        candidates: Vec<HashPathIndexKey>,
        had_denial: bool,
    ) -> Self {
        self.candidates = candidates.into();
        self.candidates_ready = true;
        self.had_denial = had_denial;
        self
    }

    pub fn with_snapshot(mut self) -> Self {
        self.snapshot = true;
        self
    }

    fn exact_target(&self) -> Option<&VersionedObjectArn> {
        match &self.request.target {
            BaoReadTarget::ExactVersion(target) => Some(target),
            BaoReadTarget::Blake3(_) => None,
        }
    }

    fn hash_target(&self) -> Option<[u8; 32]> {
        match &self.request.target {
            BaoReadTarget::Blake3(hash) => Some(*hash),
            BaoReadTarget::ExactVersion(_) => None,
        }
    }

    fn read_realm(&mut self) -> Effects {
        self.state = IncomingBaoReadState::ReadRealm;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(self.request.realm_id.as_bytes().to_vec()),
            txn_id: self.txn_id,
        })]
    }

    fn read_exact_version(&mut self) -> Effects {
        let target = self
            .exact_target()
            .expect("exact target required in exact-version state");
        let key = match VersionKey::new(&target.bucket, &target.key, target.version).to_bytes() {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.state = IncomingBaoReadState::ReadExactVersion;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
            key: key.into(),
            txn_id: self.txn_id,
        })]
    }

    fn read_exact_bucket(&mut self) -> Effects {
        let bucket = self
            .exact_target()
            .expect("exact target required while reading bucket")
            .bucket
            .clone();
        self.state = IncomingBaoReadState::ReadExactBucket;
        smallvec![Effect::Storage(StorageEffect::Read {
            key_space: S3_BUCKET_KEYSPACE.to_string(),
            key: bucket.as_bytes().into(),
            txn_id: self.txn_id,
        })]
    }

    fn start_policy(&mut self, group_id: GroupId, path: String, next: PolicyNext) -> Effects {
        self.policy_group = Some(group_id);
        self.policy_path = Some(path.clone());
        self.policy_next = Some(next);
        let Some(txn_id) = self.txn_id else {
            return self.continue_policy(self.policy_paths.contains(&path));
        };
        self.state = IncomingBaoReadState::ReadPolicy;
        smallvec![Effect::Storage(StorageEffect::BatchRead {
            reads: vec![
                (
                    REALM_CONFIG_KEYSPACE.to_string(),
                    self.request.realm_id.as_bytes().to_vec().into(),
                ),
                (AUTH_KEYSPACE.to_string(), group_id.to_bytes().into()),
            ],
            txn_id: Some(txn_id),
        })]
    }

    fn continue_policy(&mut self, allowed: bool) -> Effects {
        let Some(next) = self.policy_next else {
            return self.fail(BaoReadError::NotFinished);
        };
        if !allowed {
            return match next {
                PolicyNext::Exact => self.send_refusal(BaoReadRefusal::ReadDenied),
                PolicyNext::Hash => {
                    self.had_denial = true;
                    self.next_candidate()
                }
            };
        }
        match next {
            PolicyNext::Exact => self.read_exact_version(),
            PolicyNext::Hash => {
                let Some(candidate) = self.candidate.as_ref() else {
                    return self.fail(BaoReadError::NotFinished);
                };
                let key =
                    match VersionKey::new(&candidate.bucket, &candidate.key, candidate.version_id)
                        .to_bytes()
                    {
                        Ok(key) => key,
                        Err(error) => return self.fail(error.into()),
                    };
                self.state = IncomingBaoReadState::ReadHashVersion;
                smallvec![Effect::Storage(StorageEffect::Read {
                    key_space: BLOB_VERSIONS_KEYSPACE.to_string(),
                    key: key.into(),
                    txn_id: self.txn_id,
                })]
            }
        }
    }

    fn handle_policy(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(event);
        };
        let Some((_, Some(realm_value))) = values.first() else {
            return self.continue_policy(false);
        };
        let Some((_, Some(group_value))) = values.get(1) else {
            return self.continue_policy(false);
        };
        let Ok(realm) = RealmConfigDocument::from_bytes(realm_value) else {
            return self.continue_policy(false);
        };
        let Ok(group) = GroupAuthorizationDocument::from_bytes(group_value) else {
            return self.continue_policy(false);
        };
        let Some(group_id) = self.policy_group else {
            return self.continue_policy(false);
        };
        if group.group_id != group_id {
            return self.continue_policy(false);
        }
        let Some(path) = self.policy_path.as_deref() else {
            return self.continue_policy(false);
        };
        let request = policy_request_with(
            path,
            &Permission::READ,
            Some(&self.request.auth_context.user_id),
            PolicyRequestExtras::operation("s3.GetObject"),
        );
        let realm_set = match CompiledPolicySet::compile(&realm.request_policies) {
            Ok(set) => set,
            Err(_) => return self.continue_policy(false),
        };
        let group_set = match CompiledPolicySet::compile(&group.policies) {
            Ok(set) => set,
            Err(_) => return self.continue_policy(false),
        };
        self.policy_current = matches!(
            realm_set.evaluate(&request, &PolicyFunctions::default()),
            PolicyDecision::Allowed
        ) && matches!(
            group_set.evaluate(&request, &PolicyFunctions::default()),
            PolicyDecision::Allowed
        );
        let Some(txn_id) = self.txn_id else {
            return self.continue_policy(self.policy_current);
        };
        self.state = IncomingBaoReadState::CheckPermission;
        smallvec![Effect::SubOperation(boxed_suboperation(
            CheckPermissionsOperation::new_with_txn(
                CheckPermissionsConfig {
                    auth_context: self.request.auth_context.clone(),
                    path: path.to_string(),
                    required_permission: Permission::READ,
                },
                txn_id,
            ),
            |allowed| Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }),
        ))]
    }

    fn handle_permission(&mut self, event: Event) -> Effects {
        let Event::SubOperation(SubOperationEvent::AuthorizationResult { allowed }) = event else {
            return self.unexpected(event);
        };
        self.continue_policy(allowed.is_ok_and(|allowed| allowed && self.policy_current))
    }

    fn next_candidate(&mut self) -> Effects {
        let Some(candidate) = self.candidates.pop_front() else {
            return self.send_refusal(if self.had_denial {
                BaoReadRefusal::ReadDenied
            } else {
                BaoReadRefusal::NotFound
            });
        };
        let path = candidate.permission_path();
        let group_id = candidate.group_id;
        self.candidate = Some(candidate);
        self.start_policy(group_id, path, PolicyNext::Hash)
    }

    fn read_location(&mut self) -> Effects {
        let Some(key) = self.location_key.clone() else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        self.state = IncomingBaoReadState::ReadLocation;
        smallvec![blob_location_read(&key, self.txn_id)]
    }

    fn send_accepted(&mut self, location: BackendLocation) -> Effects {
        let Some(blake3) = self.blob_hash else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let payload = match (VersionReplicationMessage::BaoReadAccepted {
            size: location.blob_size,
            blake3,
        })
        .to_bytes()
        {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.location = Some(location);
        self.state = IncomingBaoReadState::SendAccepted;
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn send_refusal(&mut self, refusal: BaoReadRefusal) -> Effects {
        let payload = match VersionReplicationMessage::BaoReadRefused(refusal).to_bytes() {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.refusal = Some(refusal);
        self.state = IncomingBaoReadState::SendRefusal;
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }

    fn handle_start(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionStarted { txn_id }) = event else {
            return self.unexpected(event);
        };
        self.txn_id = Some(txn_id);
        self.read_realm()
    }

    fn commit_result(&mut self, result: IncomingBaoReadResult) -> Effects {
        self.result = Some(result);
        let Some(txn_id) = self.txn_id else {
            self.output = Some(Ok(result));
            self.state = IncomingBaoReadState::Finish;
            return smallvec![];
        };
        self.state = IncomingBaoReadState::CommitTransaction;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    fn handle_commit(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionCommitted { .. }) = event else {
            return self.unexpected(event);
        };
        self.txn_id = None;
        self.state = IncomingBaoReadState::Finish;
        self.output = Some(Ok(self
            .result
            .take()
            .unwrap_or(IncomingBaoReadResult::Probed)));
        smallvec![]
    }

    fn handle_abort(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::TransactionAborted { .. }) = event else {
            return self.unexpected(event);
        };
        self.txn_id = None;
        self.state = IncomingBaoReadState::Finish;
        let refusal = self
            .refusal
            .take()
            .unwrap_or(BaoReadRefusal::BackendFailure);
        self.output = Some(Ok(IncomingBaoReadResult::Refused(refusal)));
        smallvec![]
    }

    fn fail(&mut self, error: BaoReadError) -> Effects {
        self.state = IncomingBaoReadState::Error;
        self.output = Some(Err(error));
        smallvec![Effect::Blob(BlobEffect::CloseConnection {
            stream_id: self.stream_id,
        })]
    }

    fn unexpected(&mut self, event: Event) -> Effects {
        self.fail(BaoReadError::Unexpected {
            state: self.state_name(),
            event: format!("{event:?}"),
        })
    }

    fn state_name(&self) -> &'static str {
        match self.state {
            IncomingBaoReadState::Init => "init",
            IncomingBaoReadState::StartTransaction => "start_transaction",
            IncomingBaoReadState::ReadRealm => "read_realm",
            IncomingBaoReadState::ReadExactVersion => "read_exact_version",
            IncomingBaoReadState::ReadExactBucket => "read_exact_bucket",
            IncomingBaoReadState::ReadPolicy => "read_policy",
            IncomingBaoReadState::CheckPermission => "check_permission",
            IncomingBaoReadState::ReadHashVersion => "read_hash_version",
            IncomingBaoReadState::ReadLocation => "read_location",
            IncomingBaoReadState::CheckManagedCopy => "check_managed_copy",
            IncomingBaoReadState::PolicyChallenge => "policy_challenge",
            IncomingBaoReadState::SendAccepted => "send_accepted",
            IncomingBaoReadState::ServeRead => "serve_read",
            IncomingBaoReadState::CloseMetadata => "close_metadata",
            IncomingBaoReadState::CommitTransaction => "commit_transaction",
            IncomingBaoReadState::SendRefusal => "send_refusal",
            IncomingBaoReadState::CloseRefusal => "close_refusal",
            IncomingBaoReadState::AbortTransaction => "abort_transaction",
            IncomingBaoReadState::Finish => "finish",
            IncomingBaoReadState::Error => "error",
        }
    }

    fn handle_realm(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.send_refusal(BaoReadRefusal::RealmPeerDenied);
        };
        let document = match RealmConfigDocument::from_bytes(&value) {
            Ok(document) => document,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        if ensure_realm_peer(&document, self.peer, self.request.realm_id, true).is_err() {
            return self.send_refusal(BaoReadRefusal::RealmPeerDenied);
        }
        match &self.request.target {
            BaoReadTarget::ExactVersion(target) => {
                if target.realm_id != self.request.realm_id || target.node_id != self.local_node {
                    self.send_refusal(BaoReadRefusal::InvalidTarget)
                } else {
                    self.read_exact_bucket()
                }
            }
            BaoReadTarget::Blake3(hash) => {
                if self
                    .request
                    .expected_blake3
                    .is_some_and(|expected| expected != *hash)
                {
                    self.send_refusal(BaoReadRefusal::HashMismatch)
                } else if self.candidates_ready {
                    self.next_candidate()
                } else {
                    self.send_refusal(BaoReadRefusal::BackendFailure)
                }
            }
        }
    }

    fn handle_exact_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let version = match BlobVersion::from_bytes(&value) {
            Ok(version) => version,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        let Some(blob_hash) = version.blob_hash().copied() else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        if self
            .request
            .expected_blake3
            .is_some_and(|expected| expected != blob_hash)
        {
            return self.send_refusal(BaoReadRefusal::HashMismatch);
        }
        self.blob_hash = Some(blob_hash);
        self.location_key = version.location_key();
        self.version_refs = version.placement_policies.clone();
        self.version_key = self
            .exact_target()
            .map(|target| VersionKey::new(&target.bucket, &target.key, target.version));
        self.read_location()
    }

    fn handle_exact_bucket(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let bucket = match BucketInfo::from_bytes(&value) {
            Ok(bucket) => bucket,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        let target = self
            .exact_target()
            .expect("exact target required after exact bucket read");
        let path = blob_object_permission_path(
            self.request.realm_id,
            bucket.group_id,
            self.local_node,
            &target.bucket,
            &target.key,
        );
        self.start_policy(bucket.group_id, path, PolicyNext::Exact)
    }

    fn handle_hash_version(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return self.next_candidate();
        };
        let version = match BlobVersion::from_bytes(&value) {
            Ok(version) => version,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        let Some(hash) = self.hash_target() else {
            return self.send_refusal(BaoReadRefusal::InvalidTarget);
        };
        if version.blob_hash() != Some(&hash) {
            return self.next_candidate();
        }
        self.blob_hash = Some(hash);
        self.location_key = version.location_key();
        self.version_refs = version.placement_policies.clone();
        self.version_key = self.candidate.as_ref().map(|candidate| {
            VersionKey::new(&candidate.bucket, &candidate.key, candidate.version_id)
        });
        self.read_location()
    }

    fn handle_location(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        let Some(value) = value else {
            return if matches!(&self.request.target, BaoReadTarget::Blake3(_)) {
                self.next_candidate()
            } else {
                self.send_refusal(BaoReadRefusal::NotFound)
            };
        };
        let location = match BackendLocation::from_bytes(&value) {
            Ok(location) => location,
            Err(_) => return self.send_refusal(BaoReadRefusal::BackendFailure),
        };
        let Some(blake3) = self.blob_hash else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        if location.get_blake3() != Some(blake3.as_slice()) {
            return self.send_refusal(BaoReadRefusal::HashMismatch);
        }
        if self.version_refs.is_empty() {
            return self.send_accepted(location);
        }
        self.check_managed_copy(location)
    }

    /// A governed copy is only offered from a registration this node can still
    /// serve, and only to a destination it evaluated itself.
    fn check_managed_copy(&mut self, location: BackendLocation) -> Effects {
        let Some(version) = self.version_key.clone() else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let key = ManagedCopyKey::new(version, location.backend.clone());
        let effect = match serve_reads(&key, self.txn_id) {
            Ok(effect) => effect,
            Err(error) => return self.fail(error.into()),
        };
        self.pending_location = Some(location);
        self.state = IncomingBaoReadState::CheckManagedCopy;
        smallvec![effect]
    }

    fn handle_managed_copy(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchReadResult { values }) = event else {
            return self.unexpected(event);
        };
        let Some((copy, subject)) = split_serve_reads(values).ok() else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let (Some(location), Some(version)) =
            (self.pending_location.clone(), self.version_key.clone())
        else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let key = ManagedCopyKey::new(version, location.backend.clone());
        if validate_registration(
            copy.as_deref(),
            &CopyRequest {
                key: &key,
                node_id: Some(self.local_node),
                blake3: self.blob_hash,
                refs: &self.version_refs,
                subject_generation: Some(subject.subject.generation),
            },
        )
        .is_err()
        {
            return self.send_refusal(BaoReadRefusal::NotFound);
        }
        self.challenge_destination(location)
    }

    /// Teaches the requester every rule it has not resolved, then evaluates the
    /// authenticated destination independently. Authorization has already
    /// passed here, so the refs may be disclosed; echoing one is never
    /// authority.
    fn challenge_destination(&mut self, location: BackendLocation) -> Effects {
        let missing: Vec<PlacementPolicyRef> = self
            .version_refs
            .iter()
            .filter(|policy_ref| !self.request.known_refs.contains(policy_ref))
            .copied()
            .collect();
        if !missing.is_empty() {
            return self.send_required(missing);
        }
        let Some(destination) = self.request.destination.clone() else {
            return self.send_denied(Vec::new());
        };
        let context = GateContext {
            realm_id: self.local_realm,
            subject: destination,
            now_ms: self.now_ms,
        };
        match write_gate(Some(&context), &self.version_refs) {
            Ok(None) => self.send_accepted(location),
            Ok(Some(mut gate)) => {
                let effects = gate.start();
                let complete = gate.is_complete();
                self.gate = Some(gate);
                self.state = IncomingBaoReadState::PolicyChallenge;
                match complete {
                    true => self.finish_challenge(),
                    false => effects,
                }
            }
            Err(_) => self.send_denied(Vec::new()),
        }
    }

    fn finish_challenge(&mut self) -> Effects {
        let (Some(gate), Some(location)) = (self.gate.take(), self.pending_location.clone()) else {
            return self.send_refusal(BaoReadRefusal::NotFound);
        };
        let decision = gate
            .finalize()
            .map_err(PolicyGateError::from)
            .and_then(|outcome| gate_decision(outcome.decision));
        match decision {
            Ok(()) => self.send_accepted(location),
            Err(PolicyGateError::Denied { policy_ids })
            | Err(PolicyGateError::Unavailable { policy_ids }) => self.send_denied(policy_ids),
            Err(PolicyGateError::Required { refs }) => self.send_required(refs),
            Err(_) => self.send_denied(Vec::new()),
        }
    }

    fn send_required(&mut self, refs: Vec<PlacementPolicyRef>) -> Effects {
        self.send_policy(VersionReplicationMessage::PlacementPolicyRequired { refs })
    }

    fn send_denied(&mut self, policy_ids: Vec<Ulid>) -> Effects {
        self.send_policy(VersionReplicationMessage::PlacementPolicyDenied { policy_ids })
    }

    fn send_policy(&mut self, message: VersionReplicationMessage) -> Effects {
        let payload = match message.to_bytes() {
            Ok(payload) => payload,
            Err(error) => return self.fail(error.into()),
        };
        self.refusal = Some(BaoReadRefusal::ReadDenied);
        self.state = IncomingBaoReadState::SendRefusal;
        smallvec![Effect::Blob(BlobEffect::SendMessage {
            stream_id: self.stream_id,
            payload,
        })]
    }
}

impl Operation for IncomingBaoReadOperation {
    type Output = IncomingBaoReadResult;
    type Error = BaoReadError;

    fn start(&mut self) -> Effects {
        if self.request.realm_id != self.local_realm
            || self.request.auth_context.realm_id != self.request.realm_id
            || self.request.auth_context.user_id.realm_id != self.request.realm_id
        {
            return self.send_refusal(BaoReadRefusal::RealmPeerDenied);
        }
        if self.snapshot {
            self.state = IncomingBaoReadState::StartTransaction;
            return smallvec![Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })];
        }
        self.read_realm()
    }

    fn step(&mut self, event: Event) -> Effects {
        let event = match event {
            Event::Storage(StorageEvent::Error { .. })
                if !matches!(
                    self.state,
                    IncomingBaoReadState::CommitTransaction
                        | IncomingBaoReadState::AbortTransaction
                ) =>
            {
                return self.send_refusal(BaoReadRefusal::BackendFailure);
            }
            Event::Blob(BlobEvent::Error(error)) => return self.fail(error.into()),
            event => event,
        };

        match self.state {
            IncomingBaoReadState::StartTransaction => self.handle_start(event),
            IncomingBaoReadState::ReadRealm => self.handle_realm(event),
            IncomingBaoReadState::ReadExactVersion => self.handle_exact_version(event),
            IncomingBaoReadState::ReadExactBucket => self.handle_exact_bucket(event),
            IncomingBaoReadState::ReadPolicy => self.handle_policy(event),
            IncomingBaoReadState::CheckPermission => self.handle_permission(event),
            IncomingBaoReadState::ReadHashVersion => self.handle_hash_version(event),
            IncomingBaoReadState::ReadLocation => self.handle_location(event),
            IncomingBaoReadState::CheckManagedCopy => self.handle_managed_copy(event),
            IncomingBaoReadState::PolicyChallenge => {
                let Some(gate) = self.gate.as_mut() else {
                    return self.unexpected(event);
                };
                let effects = gate.step(event);
                match gate.is_complete() {
                    true => self.finish_challenge(),
                    false => effects,
                }
            }
            IncomingBaoReadState::SendAccepted => {
                let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
                    return self.unexpected(event);
                };
                if self.request.metadata_only {
                    self.state = IncomingBaoReadState::CloseMetadata;
                    return smallvec![Effect::Blob(BlobEffect::CloseConnection {
                        stream_id: self.stream_id,
                    })];
                }
                let Some(location) = self.location.clone() else {
                    return self.fail(BaoReadError::NotFinished);
                };
                let Some(expected_blake3) = self.blob_hash else {
                    return self.fail(BaoReadError::NotFinished);
                };
                self.state = IncomingBaoReadState::ServeRead;
                smallvec![Effect::Blob(BlobEffect::ServeRead {
                    stream_id: self.stream_id,
                    location,
                    expected_blake3,
                })]
            }
            IncomingBaoReadState::ServeRead => {
                let Event::Blob(BlobEvent::ReadServed { .. }) = event else {
                    return self.unexpected(event);
                };
                self.commit_result(IncomingBaoReadResult::Served)
            }
            IncomingBaoReadState::CloseMetadata => {
                let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
                    return self.unexpected(event);
                };
                self.commit_result(IncomingBaoReadResult::Probed)
            }
            IncomingBaoReadState::SendRefusal => {
                let Event::Blob(BlobEvent::MessageSent { .. }) = event else {
                    return self.unexpected(event);
                };
                self.state = IncomingBaoReadState::CloseRefusal;
                smallvec![Effect::Blob(BlobEffect::CloseConnection {
                    stream_id: self.stream_id,
                })]
            }
            IncomingBaoReadState::CloseRefusal => {
                let Event::Blob(BlobEvent::ConnectionClosed { .. }) = event else {
                    return self.unexpected(event);
                };
                let refusal = self
                    .refusal
                    .take()
                    .unwrap_or(BaoReadRefusal::BackendFailure);
                self.refusal = Some(refusal);
                let Some(txn_id) = self.txn_id else {
                    self.state = IncomingBaoReadState::Finish;
                    self.output = Some(Ok(IncomingBaoReadResult::Refused(refusal)));
                    return smallvec![];
                };
                self.state = IncomingBaoReadState::AbortTransaction;
                smallvec![Effect::Storage(StorageEffect::AbortTransaction { txn_id })]
            }
            IncomingBaoReadState::CommitTransaction => self.handle_commit(event),
            IncomingBaoReadState::AbortTransaction => self.handle_abort(event),
            IncomingBaoReadState::Init => self.unexpected(event),
            IncomingBaoReadState::Finish | IncomingBaoReadState::Error => smallvec![],
        }
    }

    fn is_complete(&self) -> bool {
        matches!(
            self.state,
            IncomingBaoReadState::Finish | IncomingBaoReadState::Error
        )
    }

    fn finalize(self) -> Result<Self::Output, Self::Error> {
        self.output.ok_or(BaoReadError::NotFinished)?
    }

    fn abort(&mut self) -> Effects {
        let effects = self.fail(BaoReadError::NotFinished);
        if let Some(txn_id) = self.txn_id.take() {
            let mut effects = effects;
            effects.push(Effect::Storage(StorageEffect::AbortTransaction { txn_id }));
            return effects;
        }
        effects
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::time::SystemTime;

    use aruna_core::UserId;
    use aruna_core::effects::{BlobEffect, Effect, StorageEffect};
    use aruna_core::events::{BlobEvent, Event, StorageEvent};
    use aruna_core::operation::Operation;
    use aruna_core::structs::checksum::HASH_BLAKE3;
    use aruna_core::structs::{
        AuthContext, BackendLocation, BackendRef, BlobVersion, BucketInfo, RealmConfigDocument,
        RealmId, RealmNodeKind, VersionedObjectArn,
    };
    use aruna_core::types::Effects;
    use ulid::Ulid;

    use super::{BaoReadOperation, BaoReadOutput, IncomingBaoReadOperation, IncomingBaoReadResult};
    use crate::replication::protocol::{
        BaoReadRefusal, BaoReadRequest, BaoReadTarget, VersionReplicationMessage,
    };

    fn test_realm() -> RealmId {
        RealmId::from_bytes([7u8; 32])
    }

    fn node_from_seed(seed: u8) -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn read_request(local_node: aruna_core::NodeId, hash: [u8; 32]) -> BaoReadRequest {
        let realm_id = test_realm();
        BaoReadRequest {
            auth_context: AuthContext {
                user_id: UserId::nil(realm_id),
                realm_id,
                path_restrictions: None,
            },
            realm_id,
            target: BaoReadTarget::ExactVersion(
                VersionedObjectArn::new(
                    realm_id,
                    local_node,
                    "bucket",
                    "path/file.txt",
                    Ulid::from(4u128),
                )
                .unwrap(),
            ),
            expected_blake3: Some(hash),
            metadata_only: false,
            destination: None,
            known_refs: Vec::new(),
        }
    }

    fn read_path(local_node: aruna_core::NodeId) -> String {
        aruna_core::structs::blob_object_permission_path(
            test_realm(),
            Ulid::from(5u128),
            local_node,
            "bucket",
            "path/file.txt",
        )
    }

    fn realm_value(peer: aruna_core::NodeId) -> byteview::ByteView {
        let mut config = RealmConfigDocument::default_for_realm(test_realm(), Vec::new());
        config.ensure_node(peer, RealmNodeKind::Server);
        postcard::to_allocvec(&config).unwrap().into()
    }

    fn version_value(hash: [u8; 32]) -> byteview::ByteView {
        BlobVersion::materialized(
            hash,
            BackendRef::node_default(),
            SystemTime::UNIX_EPOCH,
            UserId::nil(test_realm()),
            None,
        )
        .to_bytes()
        .unwrap()
        .into()
    }

    fn bucket_value() -> byteview::ByteView {
        BucketInfo {
            group_id: Ulid::from(5u128),
            created_at: SystemTime::UNIX_EPOCH,
            created_by: UserId::nil(test_realm()),
            cors_configuration: None,
            replication: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
        }
        .to_bytes()
        .unwrap()
        .into()
    }

    fn location_value(hash: [u8; 32]) -> (BackendLocation, byteview::ByteView) {
        let location = BackendLocation {
            backend: BackendRef::node_default(),
            storage_class: None,
            root: "/data".to_string(),
            storage_bucket: "blob-0".to_string(),
            backend_path: "object".to_string(),
            ulid: Ulid::from(6u128),
            compressed: false,
            encrypted: false,
            created_by: UserId::nil(test_realm()),
            created_at: SystemTime::UNIX_EPOCH,
            staging: false,
            partial: false,
            blob_size: 42,
            hashes: HashMap::from([(HASH_BLAKE3.to_string(), hash.to_vec())]),
        };
        let value = location.to_bytes().unwrap().into();
        (location, value)
    }

    fn refusal_from(effects: &Effects) -> BaoReadRefusal {
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected refusal frame")
        };
        let VersionReplicationMessage::BaoReadRefused(reason) =
            VersionReplicationMessage::from_bytes(payload).unwrap()
        else {
            panic!("expected typed refusal")
        };
        reason
    }

    #[test]
    fn exact_probe_hash() {
        let remote_node = node_from_seed(1);
        let hash = [4u8; 32];
        let stream_id = Ulid::from(9u128);
        let mut request = read_request(remote_node, hash);
        request.expected_blake3 = None;
        request.metadata_only = true;
        let mut operation = BaoReadOperation::new(remote_node, request);

        operation.start();
        operation.step(Event::Blob(BlobEvent::ConnectionEstablished { stream_id }));
        operation.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        let payload = VersionReplicationMessage::BaoReadAccepted {
            size: 42,
            blake3: hash,
        }
        .to_bytes()
        .unwrap();
        let effects = operation.step(Event::Blob(BlobEvent::MessageReceived {
            stream_id,
            payload,
        }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::CloseConnection { stream_id: id })] if *id == stream_id
        ));
        operation.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));

        assert_eq!(
            operation.finalize().unwrap(),
            BaoReadOutput::Metadata {
                size: 42,
                blake3: hash,
            }
        );
    }

    /// Drives a requester read to the response frame and returns its effects.
    fn respond(message: VersionReplicationMessage) -> BaoReadOperation {
        let remote_node = node_from_seed(1);
        let stream_id = Ulid::from(9u128);
        let mut operation =
            BaoReadOperation::new(remote_node, read_request(remote_node, [4u8; 32]));
        operation.start();
        operation.step(Event::Blob(BlobEvent::ConnectionEstablished { stream_id }));
        operation.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        operation.step(Event::Blob(BlobEvent::MessageReceived {
            stream_id,
            payload: message.to_bytes().unwrap(),
        }));
        operation.step(Event::Blob(BlobEvent::ConnectionClosed { stream_id }));
        operation
    }

    #[test]
    fn required_teaches_refs() {
        // The source teaches the rule instead of streaming, so the requester can
        // resolve it and ask again.
        let policy_ref = PlacementPolicyRef {
            policy_id: Ulid::from(3u128),
            digest: [2u8; 32],
        };
        let operation = respond(VersionReplicationMessage::PlacementPolicyRequired {
            refs: vec![policy_ref],
        });
        assert_eq!(
            operation.finalize(),
            Err(BaoReadError::PolicyRequired {
                refs: vec![policy_ref]
            })
        );
    }

    #[test]
    fn denied_never_streams() {
        let operation = respond(VersionReplicationMessage::PlacementPolicyDenied {
            policy_ids: vec![Ulid::from(3u128)],
        });
        assert_eq!(
            operation.finalize(),
            Err(BaoReadError::PolicyDenied {
                policy_ids: vec![Ulid::from(3u128)]
            })
        );
    }

    #[test]
    fn echoed_refs_never_grant() {
        // The requester claims to know the rule, but the source still evaluates
        // the destination itself and refuses it.
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let policy_ref = PlacementPolicyRef {
            policy_id: Ulid::from(3u128),
            digest: [2u8; 32],
        };
        let mut request = read_request(local_node, [4u8; 32]);
        request.known_refs = vec![policy_ref];
        request.destination = None;
        let mut operation = IncomingBaoReadOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            request,
        );
        operation.version_refs = vec![policy_ref];
        operation.request.known_refs = vec![policy_ref];

        // No destination subject means nothing governed may be served, even
        // though every ref was echoed back.
        let (location, _) = location_value([4u8; 32]);
        let effects = operation.challenge_destination(location);
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected a policy frame")
        };
        assert!(matches!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::PlacementPolicyDenied { .. }
        ));
    }

    #[test]
    fn rejects_unknown_peer() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let configured_peer = node_from_seed(3);
        let mut operation = IncomingBaoReadOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, [4u8; 32]),
        );

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(configured_peer)),
        }));

        assert_eq!(refusal_from(&effects), BaoReadRefusal::RealmPeerDenied);
    }

    #[test]
    fn snapshot_reads_txn() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let mut operation = IncomingBaoReadOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, [4u8; 32]),
        )
        .with_snapshot();

        assert!(matches!(
            operation.start().as_slice(),
            [Effect::Storage(StorageEffect::StartTransaction {
                read: true
            })]
        ));
        let txn_id = Ulid::from(10u128);
        assert!(matches!(
            operation
                .step(Event::Storage(StorageEvent::TransactionStarted { txn_id }))
                .as_slice(),
            [Effect::Storage(StorageEffect::Read {
                txn_id: Some(read_txn),
                ..
            })] if *read_txn == txn_id
        ));
    }

    #[test]
    fn rejects_user_peer() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let mut config = RealmConfigDocument::default_for_realm(test_realm(), Vec::new());
        config.ensure_node(peer, RealmNodeKind::User);
        let mut operation = IncomingBaoReadOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, [4u8; 32]),
        );

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(postcard::to_allocvec(&config).unwrap().into()),
        }));

        assert_eq!(refusal_from(&effects), BaoReadRefusal::RealmPeerDenied);
    }

    #[test]
    fn denies_wire_read() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let hash = [4u8; 32];
        let mut operation = IncomingBaoReadOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, hash),
        );

        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(peer)),
        }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(bucket_value()),
        }));
        assert_eq!(refusal_from(&effects), BaoReadRefusal::ReadDenied);
    }

    #[test]
    fn serves_exact_blob() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let hash = [4u8; 32];
        let stream_id = Ulid::from(9u128);
        let mut request = read_request(local_node, hash);
        request.expected_blake3 = None;
        let mut operation =
            IncomingBaoReadOperation::new(peer, local_node, test_realm(), stream_id, request)
                .with_policy_paths(HashSet::from([read_path(local_node)]));

        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(peer)),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(bucket_value()),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(version_value(hash)),
        }));
        let (location, value) = location_value(hash);
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(value),
        }));
        let [Effect::Blob(BlobEffect::SendMessage { payload, .. })] = effects.as_slice() else {
            panic!("expected accepted frame")
        };
        assert_eq!(
            VersionReplicationMessage::from_bytes(payload).unwrap(),
            VersionReplicationMessage::BaoReadAccepted {
                size: 42,
                blake3: hash,
            }
        );

        let effects = operation.step(Event::Blob(BlobEvent::MessageSent { stream_id }));
        assert!(matches!(
            effects.as_slice(),
            [Effect::Blob(BlobEffect::ServeRead {
                stream_id: id,
                location: selected,
                expected_blake3,
            })] if *id == stream_id && selected == &location && expected_blake3 == &hash
        ));
        assert!(
            operation
                .step(Event::Blob(BlobEvent::ReadServed { stream_id }))
                .is_empty()
        );
        assert_eq!(operation.finalize().unwrap(), IncomingBaoReadResult::Served);
    }

    #[test]
    fn rejects_hash_mismatch() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let mut operation = IncomingBaoReadOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            read_request(local_node, [4u8; 32]),
        )
        .with_policy_paths(HashSet::from([read_path(local_node)]));

        operation.start();
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(peer)),
        }));
        operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(bucket_value()),
        }));
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(version_value([5u8; 32])),
        }));

        assert_eq!(refusal_from(&effects), BaoReadRefusal::HashMismatch);
    }

    #[test]
    fn hash_requires_candidates() {
        let local_node = node_from_seed(1);
        let peer = node_from_seed(2);
        let hash = [4u8; 32];
        let mut request = read_request(local_node, hash);
        request.target = BaoReadTarget::Blake3(hash);
        let mut operation = IncomingBaoReadOperation::new(
            peer,
            local_node,
            test_realm(),
            Ulid::from(9u128),
            request,
        );

        operation.start();
        let effects = operation.step(Event::Storage(StorageEvent::ReadResult {
            key: Vec::<u8>::new().into(),
            value: Some(realm_value(peer)),
        }));

        assert_eq!(refusal_from(&effects), BaoReadRefusal::BackendFailure);
    }
}
