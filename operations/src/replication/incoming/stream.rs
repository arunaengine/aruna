//! Answers inbound bao streams: peer admission, manifests, reads, summaries and index lookups.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, HashSet};

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::{BlobEffect, Effect};
use aruna_core::errors::AuthorizationError;
use aruna_core::events::{BlobEvent, Event};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::RoCrateLimits;
use aruna_core::structs::execution::notification_watch::{
    WatchEvent, WatchEventDetail, WatchEventKind, watch_resource_path,
};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId};
use aruna_core::structs::storage::blob::{
    BucketInfo, HashIndex, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::replication::ReplicationItemKind;
use aruna_core::structs::storage::routing::NodeRouting;
use thiserror::Error;
use tracing::{debug, error, warn};
use ulid::Ulid;

use super::{IncomingVersionError, IncomingVersionOperation, IncomingVersionResult};
use crate::auth::enforcement::{PolicyBulkOperation, PolicyEvaluators};
use crate::auth::error::{AuthorizeError, PolicyEnforcementError};
use crate::auth::permission_rules::GroupPermissionRules;
use crate::auth::request_authorization::{AuthorizeOperation, GroupRulesOperation};
use crate::auth::request_policy::{PolicyRequestExtras, policy_request_with};
use crate::blob::permission_paths::{ResolvePathsError, ResolvePathsOperation};
use crate::distribution::resolve::{
    MAX_CONTENT_VERSIONS, ResolveError, content_versions, indexed_key,
};
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::groups::routing::{QuotaOperation, RoutingInputsError};
use crate::notifications::watch::origin::EmitEventOperation;
use crate::placement::policy::{GateContext, GateContextError, GateOperation};
use crate::realm::mutate_placement::node_kind;
use crate::replication::bao_read::{BaoReadError, IncomingBaoOperation, IncomingBaoResult};
use crate::replication::locations::{LocalSummary, LocationSummaryError, LocationSummaryOperation};
use crate::replication::protocol::{
    BaoReadRequest, BaoReadTarget, LocationSummaryRequest, VersionReplicationManifest,
    VersionReplicationMessage,
};
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::staging::bucket_guard::{BucketGuardError, BucketGuardOperation};
use crate::staging::offered_directory::OfferedDirectoryError;

type StreamTurn = Result<Next<()>, IoError>;
type ManifestPaths = (Option<String>, Option<String>);
type ReadPolicy = (HashSet<String>, Vec<HashIndex>, bool);
type ContentPage = (Vec<(Ulid, String)>, bool);

fn close_call(stream_id: Ulid) -> IoCall {
    Effect::Blob(BlobEffect::CloseConnection { stream_id }).into()
}

fn closed(result: IoResult) -> StreamTurn {
    if let Event::Blob(BlobEvent::Error(error)) = result.event()? {
        error!(error = ?error, "Failed to close rejected inbound bao stream");
    }
    Ok(Next::Done(()))
}

/// What the bao plane admitted a peer as. A device reaches the read branch and
/// nothing else, so replication stays infrastructure-only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BaoAdmission {
    Infra,
    DeviceRead,
}

/// What the bao plane admits this peer as. Replication is trusted from sync-eligible realm
/// nodes only; an owner-bound device gets bao reads alone from a sync-eligible node, never
/// becoming a replication source. Identity comes from the realm config. Fails closed.
#[derive(Debug, PartialEq)]
pub(crate) struct BaoAdmissionOperation {
    realm_id: RealmId,
    local: NodeId,
    peer: NodeId,
}

impl BaoAdmissionOperation {
    pub(crate) fn new(realm_id: RealmId, local: NodeId, peer: NodeId) -> Self {
        Self {
            realm_id,
            local,
            peer,
        }
    }
}

impl Operation for BaoAdmissionOperation {
    type Output = Option<BaoAdmission>;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<Option<BaoAdmission>>, IoError> {
        Ok(Next::Call(IoCall::child(GetConfigOperation::new(
            self.realm_id,
        ))))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Option<BaoAdmission>>, IoError> {
        let peer = self.peer;
        let config = match result.child::<RealmConfigDocument, GetConfigError>()? {
            Ok(config) => config,
            Err(error) => {
                warn!(peer = %peer, error = %error, "Failed to read realm config for replication gate");
                return Ok(Next::Done(None));
            }
        };
        let eligible = match config.sync_eligible_nodes() {
            Ok(ids) => ids,
            Err(error) => {
                warn!(peer = %peer, error = %error, "Failed to resolve replication peers");
                return Ok(Next::Done(None));
            }
        };
        if eligible.contains(&peer) {
            return Ok(Next::Done(Some(BaoAdmission::Infra)));
        }
        // Only a realm node answers a device, so a device never reaches another device's plane.
        let device = node_kind(&config, peer)
            .and_then(|kind| kind.owner())
            .is_some();
        let read = device && eligible.contains(&self.local);
        Ok(Next::Done(read.then_some(BaoAdmission::DeviceRead)))
    }
}

/// A registered bao stream and the nodes on its two ends.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct BaoPeerStream {
    pub(crate) realm_id: RealmId,
    pub(crate) local_node: NodeId,
    pub(crate) peer: NodeId,
    pub(crate) stream_id: Ulid,
}

/// Answers the first frame of an admitted stream. Every family closes the stream when it
/// refuses, so an error here means an answer could not be read at all.
#[derive(Debug, PartialEq)]
pub(crate) struct ServeBaoOperation {
    stream: BaoPeerStream,
    admission: BaoAdmission,
    payload: Vec<u8>,
    routing: Option<NodeRouting>,
    limits: RoCrateLimits,
    closing: bool,
}

impl ServeBaoOperation {
    pub(crate) fn new(
        stream: BaoPeerStream,
        admission: BaoAdmission,
        payload: Vec<u8>,
        routing: NodeRouting,
        limits: &RoCrateLimits,
    ) -> Self {
        Self {
            stream,
            admission,
            payload,
            routing: Some(routing),
            limits: limits.clone(),
            closing: false,
        }
    }

    fn refuse(&mut self) -> StreamTurn {
        self.closing = true;
        Ok(Next::Call(close_call(self.stream.stream_id)))
    }

    /// Lookups of a set are told to realm infrastructure only.
    fn lookup(&mut self, asked: RealmId, lookup: SetLookup) -> StreamTurn {
        if self.admission != BaoAdmission::Infra || asked != self.stream.realm_id {
            warn!(peer = %self.stream.peer, "Refusing a set index lookup");
            return self.refuse();
        }
        let answer = AnswerLookupOperation::new(self.stream.stream_id, lookup);
        Ok(Next::Call(IoCall::child(answer)))
    }
}

impl Operation for ServeBaoOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> StreamTurn {
        let stream = self.stream;
        let (peer, stream_id) = (stream.peer, stream.stream_id);
        let infra = self.admission == BaoAdmission::Infra;
        match VersionReplicationMessage::from_bytes(&self.payload) {
            Ok(VersionReplicationMessage::VersionManifest(manifest))
            | Ok(VersionReplicationMessage::ReferenceAdvance { manifest, .. }) => {
                // Only infrastructure may publish a self-identifying manifest.
                if !infra {
                    warn!(peer = %peer, stream_id = %stream_id, "Refusing a replication manifest from a device");
                    return self.refuse();
                }
                let routing = self.routing.take().ok_or(IoError::Unfinished)?;
                let accept = AcceptVersionOperation::new(stream, manifest, routing, &self.limits);
                Ok(Next::Call(IoCall::child(accept)))
            }
            Ok(VersionReplicationMessage::BaoReadRequest(request)) => {
                let answer = AnswerReadOperation::new(stream, request);
                Ok(Next::Call(IoCall::child(answer)))
            }
            Ok(VersionReplicationMessage::LocationSummaryRequest(request)) => {
                // The summary describes what this node holds; a device is never told.
                if !infra {
                    warn!(peer = %peer, stream_id = %stream_id, "Refusing a location summary to a device");
                    return self.refuse();
                }
                let answer = AnswerSummaryOperation::new(stream, request);
                Ok(Next::Call(IoCall::child(answer)))
            }
            Ok(VersionReplicationMessage::VersionKeyRequest {
                realm_id,
                set_id,
                version_id,
            }) => self.lookup(realm_id, SetLookup::Key { set_id, version_id }),
            Ok(VersionReplicationMessage::ContentRequest {
                realm_id,
                set_id,
                identity,
            }) => self.lookup(realm_id, SetLookup::Content { set_id, identity }),
            _ => {
                error!(peer = %peer, stream_id = %stream_id, "Unsupported inbound bao payload");
                self.refuse()
            }
        }
    }

    fn next(&mut self, result: IoResult) -> StreamTurn {
        if self.closing {
            return closed(result);
        }
        result.child::<(), IoError>()??;
        Ok(Next::Done(()))
    }
}

#[derive(Debug, PartialEq)]
enum AcceptStep {
    Routing,
    Policy,
    Gate,
    Incoming,
    Emit,
    Close,
}

/// Executes the manifest and reference-advance family for an admitted infra peer: routing,
/// request policy, placement gate, and the watch emitted on success. Every refusal closes the
/// stream.
#[derive(Debug, PartialEq)]
struct AcceptVersionOperation {
    stream: BaoPeerStream,
    manifest: VersionReplicationManifest,
    routing: NodeRouting,
    limits: RoCrateLimits,
    paths: ManifestPaths,
    event_id: Ulid,
    now_ms: u64,
    step: AcceptStep,
}

impl AcceptVersionOperation {
    fn new(
        stream: BaoPeerStream,
        manifest: VersionReplicationManifest,
        routing: NodeRouting,
        limits: &RoCrateLimits,
    ) -> Self {
        Self {
            stream,
            manifest,
            routing,
            limits: limits.clone(),
            paths: (None, None),
            event_id: Ulid::generate(),
            now_ms: 0,
            step: AcceptStep::Routing,
        }
    }

    fn refuse(&mut self) -> StreamTurn {
        self.step = AcceptStep::Close;
        Ok(Next::Call(close_call(self.stream.stream_id)))
    }

    fn read_policy(&mut self) -> StreamTurn {
        let stream = self.stream;
        let policy =
            ManifestPolicyOperation::new(stream.realm_id, stream.local_node, &self.manifest);
        self.step = AcceptStep::Policy;
        Ok(Next::Call(IoCall::child(policy)))
    }

    fn incoming(&mut self, gate: Option<GateContext>) -> StreamTurn {
        let stream = self.stream;
        let (manifest_path, writer_path) = std::mem::take(&mut self.paths);
        let mut incoming = IncomingVersionOperation::new(
            stream.stream_id,
            stream.local_node,
            stream.realm_id,
            self.manifest.clone(),
        )
        .with_routing(self.routing.clone())
        .with_rocrate_limits(self.limits.clone())
        .with_publisher_node(stream.peer)
        .with_manifest_policy(manifest_path)
        .with_writer_policy(writer_path);
        if let Some(gate) = gate {
            incoming = incoming.with_gate(gate);
        }
        self.step = AcceptStep::Incoming;
        Ok(Next::Call(IoCall::child(incoming)))
    }

    /// The event of an applied materialized version; nothing else is announced.
    fn watch_event(&self, result: &IncomingVersionResult) -> Option<WatchEvent> {
        let manifest = &self.manifest;
        let group_id = result.group_id?;
        if !result.applied || manifest.kind != ReplicationItemKind::Materialized {
            return None;
        }
        let node_id = self.stream.local_node;
        Some(WatchEvent {
            event_id: self.event_id,
            realm_id: manifest.auth_context.realm_id,
            kind: WatchEventKind::DataUploaded,
            path: watch_resource_path(group_id, node_id, &manifest.bucket, &manifest.key),
            actor: manifest.auth_context.user_id,
            occurred_at_ms: self.now_ms,
            detail: WatchEventDetail::DataUploaded {
                group_id,
                node_id,
                bucket: manifest.bucket.clone(),
                key: manifest.key.clone(),
                size_bytes: manifest.blob.as_ref().map_or(0, |blob| blob.size),
            },
        })
    }
}

impl Operation for AcceptVersionOperation {
    type Output = ();
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> StreamTurn {
        let (stream, manifest) = (self.stream, &self.manifest);
        debug!(
            peer = %stream.peer,
            stream_id = %stream.stream_id,
            bucket = %manifest.bucket,
            key = %manifest.key,
            version_id = %manifest.version_id,
            kind = ?manifest.kind,
            "Received inbound version replication manifest"
        );
        // Only a materialized item can place a blob, so only it reads the caps.
        if manifest.kind != ReplicationItemKind::Materialized {
            return self.read_policy();
        }
        let quota = QuotaOperation::new(self.routing.clone());
        Ok(Next::Call(IoCall::child(quota)))
    }

    fn next(&mut self, result: IoResult) -> StreamTurn {
        let (peer, stream_id) = (self.stream.peer, self.stream.stream_id);
        match self.step {
            AcceptStep::Routing => match result.child::<NodeRouting, RoutingInputsError>()? {
                Ok(routing) => {
                    self.routing = routing;
                    self.read_policy()
                }
                Err(error) => {
                    error!(peer = %peer, error = %error, "Refusing inbound replication with unreadable routing inputs");
                    self.refuse()
                }
            },
            AcceptStep::Policy => match result.child::<ManifestPaths, StreamPolicyError>()? {
                Ok(paths) => {
                    self.paths = paths;
                    self.step = AcceptStep::Gate;
                    let gate = GateOperation::new(self.stream.realm_id, self.now_ms);
                    Ok(Next::Call(IoCall::child(gate)))
                }
                Err(error) => {
                    error!(peer = %peer, stream_id = %stream_id, error = %error, "Refusing inbound replication with unavailable request policy");
                    self.refuse()
                }
            },
            AcceptStep::Gate => match result.child::<Option<GateContext>, GateContextError>()? {
                Ok(gate) => self.incoming(gate),
                Err(error) => {
                    error!(peer = %peer, stream_id = %stream_id, error = %error, "Refusing inbound replication with unreadable placement subject");
                    self.refuse()
                }
            },
            AcceptStep::Incoming => {
                match result.child::<IncomingVersionResult, IncomingVersionError>()? {
                    Ok(applied) => match self.watch_event(&applied) {
                        Some(event) => {
                            self.step = AcceptStep::Emit;
                            Ok(Next::Call(IoCall::child(EmitEventOperation::new(event))))
                        }
                        None => Ok(Next::Done(())),
                    },
                    Err(error) => {
                        error!(error = ?error, "Failed to process inbound version replication stream");
                        self.refuse()
                    }
                }
            }
            // Failures only warn inside the operation.
            AcceptStep::Emit => Ok(Next::Done(())),
            AcceptStep::Close => closed(result),
        }
    }
}

#[derive(Debug, PartialEq)]
enum ReadAnswerStep {
    Policy,
    Read,
    Close,
}

/// Answers the bao read family: policy resolution, then the read; a failure refuses the stream.
#[derive(Debug, PartialEq)]
struct AnswerReadOperation {
    stream: BaoPeerStream,
    request: Option<BaoReadRequest>,
    now_ms: u64,
    step: ReadAnswerStep,
}

impl AnswerReadOperation {
    fn new(stream: BaoPeerStream, request: BaoReadRequest) -> Self {
        Self {
            stream,
            request: Some(request),
            now_ms: 0,
            step: ReadAnswerStep::Policy,
        }
    }

    fn refuse(&mut self) -> StreamTurn {
        self.step = ReadAnswerStep::Close;
        Ok(Next::Call(close_call(self.stream.stream_id)))
    }
}

impl Operation for AnswerReadOperation {
    type Output = ();
    type Error = IoError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> StreamTurn {
        let request = self.request.as_ref().ok_or(IoError::Unfinished)?;
        let stream = self.stream;
        let policy = ReadPolicyOperation::new(stream.realm_id, stream.local_node, request);
        Ok(Next::Call(IoCall::child(policy)))
    }

    fn next(&mut self, result: IoResult) -> StreamTurn {
        let stream = self.stream;
        let (peer, stream_id) = (stream.peer, stream.stream_id);
        match self.step {
            ReadAnswerStep::Policy => match result.child::<ReadPolicy, StreamPolicyError>()? {
                Ok((paths, candidates, had_denial)) => {
                    let request = self.request.take().ok_or(IoError::Unfinished)?;
                    let read = IncomingBaoOperation::new(
                        peer,
                        stream.local_node,
                        stream.realm_id,
                        stream_id,
                        request,
                    )
                    .with_policy_paths(paths)
                    .with_policy_candidates(candidates, had_denial)
                    .with_now(self.now_ms)
                    .with_snapshot();
                    self.step = ReadAnswerStep::Read;
                    Ok(Next::Call(IoCall::child(read)))
                }
                Err(error) => {
                    error!(peer = %peer, stream_id = %stream_id, error = %error, "Refusing inbound bao read with unavailable request policy");
                    self.refuse()
                }
            },
            ReadAnswerStep::Read => match result.child::<IncomingBaoResult, BaoReadError>()? {
                Ok(_) => Ok(Next::Done(())),
                Err(error) => {
                    error!(peer = %peer, stream_id = %stream_id, error = ?error, "Failed to process inbound bao read");
                    self.refuse()
                }
            },
            ReadAnswerStep::Close => closed(result),
        }
    }
}

/// Answers the location summary family. The summary describes what this node holds, so the
/// request's own authenticated context decides whether anything is disclosed.
#[derive(Debug, PartialEq)]
struct AnswerSummaryOperation {
    stream: BaoPeerStream,
    request: Option<LocationSummaryRequest>,
    closing: bool,
}

impl AnswerSummaryOperation {
    fn new(stream: BaoPeerStream, request: LocationSummaryRequest) -> Self {
        Self {
            stream,
            request: Some(request),
            closing: false,
        }
    }
}

impl Operation for AnswerSummaryOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> StreamTurn {
        let stream = self.stream;
        let request = self.request.take().ok_or(IoError::Unfinished)?;
        let allowed = request.realm_id == stream.realm_id
            && auth_matches(&request.auth_context, stream.realm_id);
        let summary = LocationSummaryOperation::new_incoming(
            stream.peer,
            stream.local_node,
            stream.stream_id,
            request,
        )
        .with_policy(allowed);
        Ok(Next::Call(IoCall::child(summary)))
    }

    fn next(&mut self, result: IoResult) -> StreamTurn {
        if self.closing {
            return closed(result);
        }
        let (peer, stream_id) = (self.stream.peer, self.stream.stream_id);
        match result.child::<LocalSummary, LocationSummaryError>()? {
            Ok(_) => Ok(Next::Done(())),
            Err(error) => {
                error!(peer = %peer, stream_id = %stream_id, error = ?error, "Failed to answer inbound location summary");
                self.closing = true;
                Ok(Next::Call(close_call(stream_id)))
            }
        }
    }
}

/// One question about the index of a set this member belongs to.
#[derive(Debug, PartialEq)]
enum SetLookup {
    Key { set_id: Ulid, version_id: Ulid },
    Content { set_id: Ulid, identity: [u8; 32] },
}

#[derive(Debug, PartialEq)]
enum LookupStep {
    Find,
    Send,
    Close,
}

/// Answers which key or which versions this member's index holds, then ends the stream.
#[derive(Debug, PartialEq)]
struct AnswerLookupOperation {
    stream_id: Ulid,
    lookup: SetLookup,
    step: LookupStep,
}

impl AnswerLookupOperation {
    fn new(stream_id: Ulid, lookup: SetLookup) -> Self {
        Self {
            stream_id,
            lookup,
            step: LookupStep::Find,
        }
    }

    fn close(&mut self) -> StreamTurn {
        self.step = LookupStep::Close;
        Ok(Next::Call(close_call(self.stream_id)))
    }

    fn send(&mut self, answer: VersionReplicationMessage) -> StreamTurn {
        let Ok(payload) = answer.to_bytes() else {
            return self.close();
        };
        self.step = LookupStep::Send;
        let stream_id = self.stream_id;
        let send = BlobEffect::SendMessage { stream_id, payload };
        Ok(Next::Call(Effect::Blob(send).into()))
    }
}

impl Operation for AnswerLookupOperation {
    type Output = ();
    type Error = IoError;

    fn first(&mut self) -> StreamTurn {
        Ok(Next::Call(match &self.lookup {
            SetLookup::Key { set_id, version_id } => {
                IoCall::child(indexed_key(*set_id, *version_id))
            }
            SetLookup::Content { set_id, identity } => {
                IoCall::child(content_versions(*set_id, identity, MAX_CONTENT_VERSIONS))
            }
        }))
    }

    fn next(&mut self, result: IoResult) -> StreamTurn {
        match self.step {
            LookupStep::Find => match self.lookup {
                SetLookup::Key { .. } => {
                    let key = (result.child::<Option<String>, ResolveError>()?)
                        .ok()
                        .flatten();
                    self.send(VersionReplicationMessage::VersionKeyResponse(key))
                }
                SetLookup::Content { .. } => match result.child::<ContentPage, ResolveError>()? {
                    Ok((versions, more)) => {
                        self.send(VersionReplicationMessage::ContentResponse { versions, more })
                    }
                    Err(_) => self.close(),
                },
            },
            LookupStep::Send => {
                if let Event::Blob(BlobEvent::Error(error)) = result.event()? {
                    debug!(error = ?error, "Failed to answer a set index lookup");
                }
                self.close()
            }
            LookupStep::Close => closed(result),
        }
    }
}

/// Why the request policy of an inbound stream could not be decided.
#[derive(Debug, Error, PartialEq)]
pub(crate) enum StreamPolicyError {
    #[error(transparent)]
    Io(#[from] IoError),
    #[error("{0}")]
    Failed(String),
}

fn policy_failed(error: impl std::fmt::Display) -> StreamPolicyError {
    StreamPolicyError::Failed(error.to_string())
}

/// A user of another realm signed in here matches; an anonymous identity only in its own realm.
fn auth_matches(auth: &AuthContext, realm_id: RealmId) -> bool {
    auth.realm_id == realm_id && (auth.user_id.realm_id == realm_id || !auth.user_id.is_nil())
}

async fn manifest_policy(
    context: &DriverContext,
    local_realm: RealmId,
    local_node: NodeId,
    manifest: &VersionReplicationManifest,
) -> Result<(Option<String>, Option<String>), String> {
    // Denied like a missing policy, so the sender stops and no destination is created. An unbound
    // folder stays `Deleting` until its row is gone, so checking before the create suffices.
    match drive(BucketGuardOperation::new(manifest.bucket.as_str()), context)
        .await
        .map_err(OfferedDirectoryError::from)
    {
        Ok(()) => {}
        Err(OfferedDirectoryError::ReadOnly(_)) => return Ok((None, None)),
        Err(error) => return Err(error.to_string()),
    }
    let group_id = match drive(GetBucketOperation::new(manifest.bucket.clone()), context).await {
        Ok(info) => info.group_id,
        Err(GetBucketError::NotFound) => manifest.group_id,
        Err(error) => return Err(error.to_string()),
    };
    // The member admits a publication against its record of the set, not a user's grants.
    if manifest.distribution.is_some() {
        return Ok((None, None));
    }
    let path = if manifest.key.is_empty() {
        bucket_permission_path(local_realm, group_id, local_node, &manifest.bucket)
    } else {
        object_permission_path(
            local_realm,
            group_id,
            local_node,
            &manifest.bucket,
            &manifest.key,
        )
    };
    if manifest.reference_advance.is_some() {
        // The sync-eligible publisher attests source READ for this advance.
        return Ok(if auth_matches(&manifest.auth_context, local_realm) {
            (Some(path), None)
        } else {
            (None, None)
        });
    }
    let operation = if manifest.kind == ReplicationItemKind::DeleteMarker {
        "s3.DeleteObject"
    } else {
        "s3.PutObject"
    };
    if !auth_matches(&manifest.auth_context, local_realm)
        || !allow_policy(
            context,
            &manifest.auth_context,
            &path,
            &Permission::WRITE,
            operation,
        )
        .await?
    {
        return Ok((None, None));
    }
    let manifest_path = Some(path.clone());
    let writer_path = if let Some(auth) = manifest.writer_auth_context.as_ref()
        && auth_matches(auth, local_realm)
    {
        if allow_policy(context, auth, &path, &Permission::WRITE, operation).await? {
            Some(path)
        } else {
            None
        }
    } else {
        None
    };
    Ok((manifest_path, writer_path))
}

async fn bao_policy(
    context: &DriverContext,
    local_realm: RealmId,
    local_node: NodeId,
    request: &crate::replication::protocol::BaoReadRequest,
) -> Result<(HashSet<String>, Vec<HashIndex>, bool), String> {
    let mut paths = HashSet::new();
    if request.realm_id != local_realm || !auth_matches(&request.auth_context, request.realm_id) {
        return Ok((paths, Vec::new(), false));
    }
    match &request.target {
        crate::replication::protocol::BaoReadTarget::ExactVersion(target) => {
            if target.realm_id != request.realm_id || target.node_id != local_node {
                return Ok((paths, Vec::new(), false));
            }
            let group_id =
                match drive(GetBucketOperation::new(target.bucket.clone()), context).await {
                    Ok(info) => info.group_id,
                    Err(GetBucketError::NotFound) => {
                        return Ok((paths, Vec::new(), false));
                    }
                    Err(error) => return Err(error.to_string()),
                };
            let path = object_permission_path(
                request.realm_id,
                group_id,
                local_node,
                &target.bucket,
                &target.key,
            );
            if allow_policy(
                context,
                &request.auth_context,
                &path,
                &Permission::READ,
                "s3.GetObject",
            )
            .await?
            {
                paths.insert(path);
            }
            Ok((paths, Vec::new(), false))
        }
        crate::replication::protocol::BaoReadTarget::Blake3(hash) => {
            let candidates = drive(ResolvePathsOperation::new(*hash), context)
                .await
                .map_err(|error| error.to_string())?;
            let mut unique = BTreeMap::new();
            for candidate in candidates {
                if candidate.realm_id != request.realm_id
                    || candidate.node_id != local_node
                    || candidate.blake3_hash != *hash
                {
                    continue;
                }
                let key = candidate.to_bytes().map_err(|error| error.to_string())?;
                unique.entry(key).or_insert(candidate);
            }
            let candidates = unique.into_values().collect::<Vec<_>>();
            let evaluators = drive(
                PolicyBulkOperation::new(
                    candidates
                        .iter()
                        .map(|candidate| (candidate.realm_id, candidate.group_id)),
                ),
                context,
            )
            .await
            .map_err(|error| error.to_string())?;
            let permissions = drive(
                GroupRulesOperation::new(
                    Some(&request.auth_context),
                    candidates.iter().map(|candidate| candidate.group_id),
                ),
                context,
            )
            .await
            .unwrap_or_default();
            let mut allowed = Vec::with_capacity(candidates.len());
            let mut had_denial = false;
            for candidate in candidates {
                let path = candidate.permission_path();
                if !permissions.allows(candidate.group_id, &path, &Permission::READ) {
                    had_denial = true;
                    continue;
                }
                let request = policy_request_with(
                    &path,
                    &Permission::READ,
                    Some(&request.auth_context),
                    PolicyRequestExtras::operation("s3.GetObject"),
                );
                let evaluator = evaluators
                    .get(&(candidate.realm_id, candidate.group_id))
                    .ok_or_else(|| "hash policy evaluator is unavailable".to_string())?;
                match evaluator.evaluate(&request) {
                    Ok(()) => {
                        paths.insert(path);
                        allowed.push(candidate);
                    }
                    Err(PolicyEnforcementError::Denied { .. }) => had_denial = true,
                    Err(error) => return Err(error.to_string()),
                }
            }
            Ok((paths, allowed, had_denial))
        }
        // The serve finds the version in its member bucket and checks the caller in its
        // snapshot transaction, as for every target.
        crate::replication::protocol::BaoReadTarget::Version { .. } => {
            Ok((paths, Vec::new(), false))
        }
    }
}

#[cfg(test)]
#[path = "stream_tests.rs"]
mod tests;
