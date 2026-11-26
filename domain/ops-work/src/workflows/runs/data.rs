//! The data calls of a run's coordinator: stat, list, read, upload and delivery. Each is decided
//! as the run's creator, never with the node's own rights; only the run's own area is exempt.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::bucket_read::{GetBucketError, GetBucketOperation};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::id_mint::IdMint;
use aruna_core::jobs::{JobEffect, JobEvent};
use aruna_core::operation::Operation;
use aruna_core::request_policy::PolicyRequestExtras;
use aruna_core::stream::BackendStream;
use aruna_core::structs::checksum::HASH_SHA256;
use aruna_core::structs::execution::workflow_run::{RunFile, RunOrigin, WorkflowRunRecord};
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::{
    BackendLocation, BucketInfo, bucket_permission_path, object_permission_path,
};
use aruna_core::structs::storage::object_read::ObjectRangeRequest;
use aruna_core::structs::storage::origin::VersionOrigin;
use aruna_core::structs::storage::routing::{NodeRouting, RoutingSnapshot};
use aruna_ops_data::replication::bao_read::BaoReadOutput;
use aruna_ops_data::s3::object::get::{
    GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult,
};
use aruna_ops_data::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use aruna_ops_data::s3::object::list::{
    ListBucketError, ListBucketInput, ListBucketOperation, ListBucketResult, ListContinuationToken,
};
use aruna_ops_data::s3::object::put::{
    PutObjectConfig, PutObjectError, PutObjectInput, PutObjectOperation, PutObjectResult,
};
use aruna_ops_identity::auth::error::{AuthorizeError, PolicyEnforcementError};
use aruna_ops_identity::auth::request_authorization::AuthorizeOperation;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_ops_identity::groups::routing::{BucketRules, RoutingInputsError, SnapshotOperation};
use aruna_ops_identity::placement::policy::gate::{GateContext, GateContextError, GateOperation};
use aruna_workflow_contract::host::{ListEntry, StorageEntry, UploadReceipt};
use aruna_workflow_contract::preflight::check_path;
use aruna_workflow_contract::{ContractError, DATA_CHUNK_BYTES, HostRpcResult};
use bytes::Bytes;
use ulid::Ulid;

use super::remote::{self, RemoteReadOperation};
use super::{RunChange, change_call, changed, creator_auth};
use crate::workflows::CatalogError;

/// The scheme of uploads only the coordinator held; they land in the run's area.
pub const STAGING_SCHEME: &str = "aruna-staging://";
/// The folder below the output prefix that holds the task files of every run.
pub const AREA_FOLDER: &str = ".uwe/runs";
/// Keys one listing page of a data call reads.
const LIST_KEYS: usize = 1000;

/// One object a data call names, resolved against the run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
    pub bucket: String,
    pub key: String,
    pub version: Option<Ulid>,
    /// The object lies in the run's own area, which the run wrote itself.
    pub owned: bool,
    /// The node that holds the exact version, when the address names one; `None` is this node.
    pub node: Option<NodeId>,
}

impl Target {
    /// The node that holds the object, when it is not `local`.
    pub fn remote(&self, local: NodeId) -> Option<NodeId> {
        self.node.filter(|node| *node != local)
    }
}

/// The bucket and key prefix of a run's area, ending in `/`; `None` without an output bucket.
pub fn area_of(record: &WorkflowRunRecord) -> Option<(String, String)> {
    let output = record.output.as_ref()?;
    let base = output.prefix.trim_end_matches('/');
    let folder = match base.is_empty() {
        true => AREA_FOLDER.to_owned(),
        false => format!("{base}/{AREA_FOLDER}"),
    };
    Some((
        output.bucket.clone(),
        format!("{folder}/{}/", record.run_id),
    ))
}

/// The address the node gives the coordinator for a stored object version.
pub fn address_of(bucket: &str, key: &str, version: Option<Ulid>) -> String {
    format_address("aruna://", bucket, key, version, None)
}

/// The address of an object that `node` holds, like a task output in its run area there.
pub fn address_on(bucket: &str, key: &str, version: Option<Ulid>, node: NodeId) -> String {
    format_address("aruna://", bucket, key, version, Some(node))
}

/// Formats an `aruna://` or `s3://` address with percent-encoded key and optional version/node.
/// Unversioned remote addresses place `node` in the RFC 3986 authority (`node@bucket`) so CWL
/// `loadListing` and IRI joins preserve the holder across child paths.
pub(crate) fn format_address(
    scheme: &str,
    bucket: &str,
    key: &str,
    version: Option<Ulid>,
    node: Option<NodeId>,
) -> String {
    let key = aruna_workflow_contract::host::encode_key(key);
    match (version, node) {
        (Some(version), Some(node)) => {
            format!("{scheme}{bucket}/{key}?version={version}&node={node}")
        }
        (Some(version), None) => format!("{scheme}{bucket}/{key}?version={version}"),
        (None, Some(node)) => format!("{scheme}{node}@{bucket}/{key}"),
        (None, None) => format!("{scheme}{bucket}/{key}"),
    }
}

/// Parses an `aruna://` or `s3://` address into `(bucket, decoded_key, version, node)`.
pub(crate) fn parse_address(
    address: &str,
) -> Result<(String, String, Option<Ulid>, Option<NodeId>), ContractError> {
    let rest = (address.strip_prefix("aruna://"))
        .or_else(|| address.strip_prefix("s3://"))
        .ok_or_else(|| {
            ContractError::unsupported(format!("the address {address} is not served"))
        })?;
    let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (mut version, mut query_node) = (None, None);
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        match pair.split_once('=') {
            Some(("version", text)) => {
                let parsed = Ulid::from_string(text)
                    .map_err(|_| ContractError::invalid(format!("{text} is no version id")))?;
                version = Some(parsed);
            }
            Some(("node", text)) => {
                let parsed = (text.parse::<NodeId>())
                    .map_err(|_| ContractError::invalid(format!("{text} is no node id")))?;
                query_node = Some(parsed);
            }
            _ => return Err(ContractError::invalid(format!("{address} names no object"))),
        }
    }
    let (authority, raw_key) = path.split_once('/').unwrap_or((path, ""));
    let (auth_node, bucket) = match authority.split_once('@') {
        Some((node_text, bucket)) => {
            let parsed = (node_text.parse::<NodeId>())
                .map_err(|_| ContractError::invalid(format!("{node_text} is no node id")))?;
            (Some(parsed), bucket)
        }
        None => (None, authority),
    };
    let node = match (auth_node, query_node) {
        (Some(a), Some(b)) if a != b => {
            return Err(ContractError::invalid(format!(
                "{address} names conflicting nodes"
            )));
        }
        (a, b) => a.or(b),
    };
    let key = aruna_workflow_contract::host::decode_key(raw_key)
        .ok_or_else(|| ContractError::invalid(format!("{address} names no object")))?;
    if bucket.is_empty() || key.split('/').any(|part| part == "..") {
        return Err(ContractError::invalid(format!("{address} names no object")));
    }
    Ok((bucket.to_owned(), key, version, node))
}

/// The address of a file of a run as seen from `local`: it names its node only when another
/// node holds the exact version.
pub fn file_address(local: NodeId, file: &RunFile) -> Option<String> {
    let (bucket, key) = (file.bucket.as_deref()?, file.key.as_deref()?);
    let version = (file.version_id.as_deref()).and_then(|version| version.parse().ok());
    Some(match file.node_id.filter(|node| *node != local) {
        Some(node) => address_on(bucket, key, version, node),
        None => address_of(bucket, key, version),
    })
}

fn denied(message: impl Into<String>) -> ContractError {
    ContractError::Denied {
        message: message.into(),
    }
}

fn not_found(message: impl Into<String>) -> ContractError {
    ContractError::NotFound {
        message: message.into(),
    }
}

/// Resolves an address of the contract: `aruna://` and `s3://` name a bucket and key with an
/// optional `?version=`, `aruna-staging://` a file in the run's area.
pub fn resolve(record: &WorkflowRunRecord, address: &str) -> Result<Target, ContractError> {
    let area = area_of(record);
    if let Some(rest) = address.strip_prefix(STAGING_SCHEME) {
        check_path(rest)?;
        let (bucket, prefix) = area.ok_or_else(no_area)?;
        return Ok(Target {
            bucket,
            key: format!("{prefix}staging/{rest}"),
            version: None,
            owned: true,
            node: None,
        });
    }
    let (bucket, key, version, node) = parse_address(address)?;
    let owned = node.is_none()
        && area
            .is_some_and(|(area_bucket, prefix)| area_bucket == bucket && key.starts_with(&prefix));
    Ok(Target {
        bucket,
        key,
        version,
        owned,
        node,
    })
}

fn no_area() -> ContractError {
    ContractError::unsupported("the run names no output bucket to keep its task files in")
}

/// Finds the group of a bucket and checks `permission` on an object path for the caller; the
/// check is skipped for the run's own area. Answers the bucket.
#[derive(Debug, PartialEq)]
pub struct ObjectAccessOperation {
    auth: AuthContext,
    node_id: NodeId,
    bucket: String,
    key: String,
    permission: Permission,
    check: bool,
    info: Option<BucketInfo>,
}

impl ObjectAccessOperation {
    pub fn new(
        auth: &AuthContext,
        node_id: NodeId,
        (bucket, key): (&str, &str),
        permission: Permission,
    ) -> Self {
        Self {
            auth: auth.clone(),
            node_id,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            permission,
            check: true,
            info: None,
        }
    }

    /// Skips the permission check for objects the run wrote itself.
    pub fn owned(mut self, owned: bool) -> Self {
        self.check = !owned;
        self
    }
}

impl Operation for ObjectAccessOperation {
    type Output = BucketInfo;
    type Error = CatalogError;

    fn first(&mut self) -> Result<Next<BucketInfo>, CatalogError> {
        let bucket = GetBucketOperation::new(self.bucket.clone());
        Ok(Next::Call(IoCall::child(bucket)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<BucketInfo>, CatalogError> {
        let Some(info) = self.info.take() else {
            let info = match result.child::<BucketInfo, GetBucketError>()? {
                Ok(info) => info,
                Err(GetBucketError::NotFound) => return Err(CatalogError::NotFound),
                Err(error) => return Err(CatalogError::Storage(error.to_string())),
            };
            if !self.check {
                return Ok(Next::Done(info));
            }
            let (realm, group) = (self.auth.realm_id, info.group_id);
            let path = match self.key.is_empty() {
                true => bucket_permission_path(realm, group, self.node_id, &self.bucket),
                false => {
                    object_permission_path(realm, group, self.node_id, &self.bucket, &self.key)
                }
            };
            self.info = Some(info);
            let check = AuthorizeOperation::new(
                realm,
                &self.auth,
                &path,
                &self.permission,
                PolicyRequestExtras::rest(),
            );
            return Ok(Next::Call(IoCall::child(check)));
        };
        match result.child::<(), AuthorizeError>()? {
            Ok(()) => Ok(Next::Done(info)),
            Err(AuthorizeError::PermissionDenied | AuthorizeError::Policy(_)) => {
                Err(CatalogError::Forbidden)
            }
            Err(error) => Err(CatalogError::Storage(error.to_string())),
        }
    }

    fn expected_error(error: &CatalogError) -> bool {
        matches!(error, CatalogError::NotFound | CatalogError::Forbidden)
    }
}

pub(crate) fn bucket_answer(result: IoResult) -> Result<BucketInfo, CatalogError> {
    result.child::<BucketInfo, CatalogError>()?
}

/// A refusal of an access check, as the coordinator reads it.
pub(crate) fn refusal(error: CatalogError, address: &str) -> Result<ContractError, CatalogError> {
    match error {
        CatalogError::NotFound => Ok(not_found(format!("{address} does not exist"))),
        CatalogError::Forbidden => Ok(denied(format!("the run's creator may not use {address}"))),
        other => Err(other),
    }
}

/// A read-only data call of the coordinator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataCall {
    Stat { address: String },
    List { address: String },
    Download { address: String, offset: u64 },
}

impl DataCall {
    fn address(&self) -> &str {
        match self {
            Self::Stat { address } | Self::List { address } | Self::Download { address, .. } => {
                address
            }
        }
    }
}

#[derive(Debug, PartialEq)]
struct ListState {
    target: Target,
    info: BucketInfo,
    stat: bool,
    pending: VecDeque<(String, ListEntry)>,
    checking: Option<ListEntry>,
    next_token: Option<ListContinuationToken>,
}

#[derive(Debug, PartialEq)]
enum ReadStep {
    Access(Target),
    Encryption(Target, Box<BucketInfo>),
    Head(Target, Box<BucketInfo>),
    Folder(Target, Box<BucketInfo>),
    List(Target, Box<BucketInfo>),
    Filter(Box<ListState>),
    Get(Target),
    Collect(Box<GetObjectResult>),
    Remote,
}

/// Answers one stat, listing or chunk read of the coordinator as the run's creator.
#[derive(Debug, PartialEq)]
pub struct ReadDataOperation {
    run: WorkflowRunRecord,
    node_id: NodeId,
    call: DataCall,
    entries: Vec<ListEntry>,
    step: Option<ReadStep>,
}

type ReadNext = Result<Next<HostRpcResult>, CatalogError>;

impl ReadDataOperation {
    pub fn new(run: WorkflowRunRecord, node_id: NodeId, call: DataCall) -> Self {
        Self {
            run,
            node_id,
            call,
            entries: Vec::new(),
            step: None,
        }
    }

    fn failed(error: ContractError) -> ReadNext {
        Ok(Next::Done(HostRpcResult::Failed(error)))
    }

    fn accessed(&mut self, target: Target, info: BucketInfo) -> ReadNext {
        match self.call.clone() {
            DataCall::Stat { .. } => {
                let head = HeadObjectOperation::new(HeadObjectInput {
                    bucket: target.bucket.clone(),
                    key: target.key.clone(),
                    version_id: target.version,
                });
                self.step = Some(ReadStep::Head(target, Box::new(info)));
                Ok(Next::Call(IoCall::child(head)))
            }
            DataCall::List { .. } => {
                if !target.owned
                    && remote::has_captured_dir(
                        &self.run,
                        self.node_id,
                        self.node_id,
                        &target.bucket,
                        &target.key,
                    )
                {
                    let pending = remote::captured_pins(
                        &self.run,
                        self.node_id,
                        self.node_id,
                        &target.bucket,
                        &target.key,
                    );
                    return self.check_next(ListState {
                        target,
                        info,
                        stat: false,
                        pending: pending.into(),
                        checking: None,
                        next_token: None,
                    });
                }
                self.list(target, info, None)
            }
            DataCall::Download { offset, .. } => {
                let get = GetObjectOperation::new(GetObjectInput {
                    bucket: target.bucket.clone(),
                    key: target.key.clone(),
                    version_id: target.version,
                    range: Some(ObjectRangeRequest::StartEnd {
                        start: offset,
                        end: offset + DATA_CHUNK_BYTES as u64 - 1,
                    }),
                    group_id: info.group_id,
                    user_identity: self.run.created_by,
                    realm_id: self.run.realm_id,
                    node_id: self.node_id,
                })
                .with_disallow_encrypted(!self.run.protected && !target.owned);
                self.step = Some(ReadStep::Get(target));
                Ok(Next::Call(IoCall::child(get)))
            }
        }
    }

    /// One listing page below the target, as a folder; a stat lists keys to check for a visible entry.
    fn list(
        &mut self,
        target: Target,
        info: BucketInfo,
        token: Option<ListContinuationToken>,
    ) -> ReadNext {
        let prefix = folder(&target.key);
        let stat = matches!(self.call, DataCall::Stat { .. });
        let list = ListBucketOperation::new(ListBucketInput {
            bucket: target.bucket.clone(),
            group_id: info.group_id,
            continuation_token: token,
            max_keys: Some(if stat && target.owned { 1 } else { LIST_KEYS }),
            prefix: (!prefix.is_empty()).then_some(prefix),
            delimiter: Some("/".into()),
            start_after: None,
        });
        self.step = Some(match stat {
            true => ReadStep::Folder(target, Box::new(info)),
            false => ReadStep::List(target, Box::new(info)),
        });
        Ok(Next::Call(IoCall::child(list)))
    }

    fn headed(&mut self, target: Target, info: BucketInfo, result: IoResult) -> ReadNext {
        match result.child::<HeadObjectResult, HeadObjectError>()? {
            Ok(_) if target.key.ends_with('/') => Ok(Next::Done(HostRpcResult::StorageStat {
                entry: Some(Self::folder_entry()),
            })),
            Ok(head) => {
                let version = head.resolved_version_id.or(head.version_id);
                let entry = StorageEntry {
                    directory: false,
                    size: head
                        .location
                        .as_ref()
                        .map_or(0, |location| location.blob_size),
                    modified_ms: (head.version_created_at)
                        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok()),
                    version: version.map(|version| version.to_string()),
                    sha256: head.location.as_ref().and_then(sha256_of),
                    aruna_id: version.map(|version| version.to_string()),
                    endpoints: Vec::new(),
                };
                Ok(Next::Done(HostRpcResult::StorageStat {
                    entry: Some(entry),
                }))
            }
            Err(
                HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker,
            ) if target.version.is_none() => {
                if !target.owned
                    && remote::has_captured_dir(
                        &self.run,
                        self.node_id,
                        self.node_id,
                        &target.bucket,
                        &target.key,
                    )
                {
                    let pending = remote::captured_pins(
                        &self.run,
                        self.node_id,
                        self.node_id,
                        &target.bucket,
                        &target.key,
                    );
                    if pending.is_empty() {
                        // Admitted empty root; ObjectAccessOperation already checked folder READ.
                        return Ok(Next::Done(HostRpcResult::StorageStat {
                            entry: Some(Self::folder_entry()),
                        }));
                    }
                    return self.check_next(ListState {
                        target,
                        info,
                        stat: true,
                        pending: pending.into(),
                        checking: None,
                        next_token: None,
                    });
                }
                self.list(target, info, None)
            }
            Err(
                HeadObjectError::NoSuchKey
                | HeadObjectError::NoSuchVersion
                | HeadObjectError::DeleteMarker,
            ) => Ok(Next::Done(HostRpcResult::StorageStat { entry: None })),
            Err(error) => Err(CatalogError::Storage(error.to_string())),
        }
    }

    fn folder_entry() -> StorageEntry {
        StorageEntry {
            directory: true,
            size: 0,
            modified_ms: None,
            version: None,
            sha256: None,
            aruna_id: None,
            endpoints: Vec::new(),
        }
    }

    fn listed(
        &mut self,
        target: Target,
        info: BucketInfo,
        stat: bool,
        result: IoResult,
    ) -> ReadNext {
        let listed = result.child::<ListBucketResult, ListBucketError>()?;
        let listed = listed.map_err(|error| CatalogError::Storage(error.to_string()))?;
        if target.owned {
            if stat {
                let exists = !(listed.objects.is_empty() && listed.common_prefixes.is_empty());
                return Ok(Next::Done(HostRpcResult::StorageStat {
                    entry: exists.then(Self::folder_entry),
                }));
            }
            let prefix = folder(&target.key);
            let name = |key: &str| {
                key.strip_prefix(&prefix)
                    .unwrap_or(key)
                    .trim_end_matches('/')
                    .to_owned()
            };
            for object in &listed.objects {
                if object.head.key == prefix {
                    continue;
                }
                self.entries.push(ListEntry {
                    name: name(&object.head.key),
                    directory: false,
                });
            }
            for common in &listed.common_prefixes {
                self.entries.push(ListEntry {
                    name: name(common),
                    directory: true,
                });
            }
            return match listed.continuation_token {
                Some(token) => self.list(target, info, Some(token)),
                None => Ok(Next::Done(HostRpcResult::StorageList {
                    entries: std::mem::take(&mut self.entries),
                })),
            };
        }
        let prefix = folder(&target.key);
        let name = |key: &str| {
            key.strip_prefix(&prefix)
                .unwrap_or(key)
                .trim_end_matches('/')
                .to_owned()
        };
        let mut pending =
            VecDeque::with_capacity(listed.objects.len() + listed.common_prefixes.len());
        for object in listed.objects {
            if !stat && object.head.key == prefix {
                continue;
            }
            let display = name(&object.head.key);
            pending.push_back((
                object.head.key,
                ListEntry {
                    name: display,
                    directory: false,
                },
            ));
        }
        for common in listed.common_prefixes {
            let display = name(&common);
            pending.push_back((
                common,
                ListEntry {
                    name: display,
                    directory: true,
                },
            ));
        }
        self.check_next(ListState {
            target,
            info,
            stat,
            pending,
            checking: None,
            next_token: listed.continuation_token,
        })
    }

    fn check_next(&mut self, mut state: ListState) -> ReadNext {
        if let Some((key, entry)) = state.pending.pop_front() {
            let auth = creator_auth(&self.run);
            let path = object_permission_path(
                auth.realm_id,
                state.info.group_id,
                self.node_id,
                &state.target.bucket,
                &key,
            );
            let check = AuthorizeOperation::new(
                auth.realm_id,
                &auth,
                &path,
                &Permission::READ,
                PolicyRequestExtras::rest(),
            );
            state.checking = Some(entry);
            self.step = Some(ReadStep::Filter(Box::new(state)));
            return Ok(Next::Call(IoCall::child(check)));
        }
        if let Some(token) = state.next_token.take() {
            return self.list(state.target, state.info, Some(token));
        }
        if state.stat {
            return Ok(Next::Done(HostRpcResult::StorageStat { entry: None }));
        }
        Ok(Next::Done(HostRpcResult::StorageList {
            entries: std::mem::take(&mut self.entries),
        }))
    }

    fn filtered(&mut self, mut state: ListState, result: IoResult) -> ReadNext {
        let entry = state.checking.take();
        match result.child::<(), AuthorizeError>()? {
            Ok(()) => {
                if state.stat {
                    return Ok(Next::Done(HostRpcResult::StorageStat {
                        entry: Some(Self::folder_entry()),
                    }));
                }
                if let Some(entry) = entry
                    && !self.entries.contains(&entry)
                {
                    self.entries.push(entry);
                }
            }
            Err(
                AuthorizeError::PermissionDenied
                | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }),
            ) => {}
            Err(error) => return Err(CatalogError::Storage(error.to_string())),
        }
        self.check_next(state)
    }

    fn got(&mut self, target: Target, result: IoResult) -> ReadNext {
        let object = match result.child::<GetObjectResult, GetObjectError>()? {
            Ok(object) => object,
            Err(GetObjectError::InvalidRange) => {
                let version = target.version.map(|version| version.to_string());
                return Ok(Next::Done(HostRpcResult::StorageChunk {
                    bytes: Vec::new(),
                    eof: true,
                    version,
                }));
            }
            Err(GetObjectError::NoSuchKey | GetObjectError::NoSuchVersion) => {
                return Self::failed(not_found(format!("{} does not exist", target.key)));
            }
            Err(GetObjectError::EncryptedDisallowed) => {
                return Self::failed(denied("encrypted inputs require a protected workflow run"));
            }
            Err(error) => return Err(CatalogError::Storage(error.to_string())),
        };
        let mut object = Box::new(object);
        let stream = std::mem::replace(&mut object.blob, BackendStream::empty());
        let collect = JobEffect::CollectUpTo {
            stream,
            limit: DATA_CHUNK_BYTES as u64,
        };
        self.step = Some(ReadStep::Collect(object));
        Ok(Next::Call(IoCall::Effect(Box::new(Effect::Job(collect)))))
    }

    fn collected(&mut self, object: GetObjectResult, result: IoResult) -> ReadNext {
        let DataCall::Download { offset, .. } = self.call else {
            return Err(IoError::Unfinished.into());
        };
        let bytes = match result.event()? {
            Event::Job(JobEvent::CollectedUpTo(Ok(Some(bytes)))) => bytes,
            Event::Job(JobEvent::CollectedUpTo(Ok(None))) => {
                return Err(CatalogError::Storage("a chunk exceeded its bound".into()));
            }
            Event::Job(JobEvent::CollectedUpTo(Err(error))) => {
                return Err(CatalogError::Storage(error.to_string()));
            }
            _ => return Err(crate::workflows::unexpected("JobEvent::CollectedUpTo")),
        };
        let eof = offset + bytes.len() as u64 >= object.info.size;
        let version =
            (object.resolved_version_id.or(object.version_id)).map(|version| version.to_string());
        Ok(Next::Done(HostRpcResult::StorageChunk {
            bytes,
            eof,
            version,
        }))
    }
}

impl Operation for ReadDataOperation {
    type Output = HostRpcResult;
    type Error = CatalogError;

    fn first(&mut self) -> ReadNext {
        let target = match resolve(&self.run, self.call.address()) {
            Ok(target) => remote::resolve_precaptured(&self.run, target, self.node_id),
            Err(error) => return Self::failed(error),
        };
        if let Some(node) = target.remote(self.node_id) {
            let read = RemoteReadOperation::new(self.run.clone(), target, node, self.call.clone());
            self.step = Some(ReadStep::Remote);
            return Ok(Next::Call(IoCall::child(read)));
        }
        let access = ObjectAccessOperation::new(
            &creator_auth(&self.run),
            self.node_id,
            (&target.bucket, &target.key),
            Permission::READ,
        )
        .owned(target.owned);
        self.step = Some(ReadStep::Access(target));
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> ReadNext {
        match self.step.take().ok_or(IoError::Unfinished)? {
            ReadStep::Access(target) => match bucket_answer(result) {
                Ok(info) if !self.run.protected && !target.owned => {
                    let key = aruna_core::types::Key::from(target.bucket.as_bytes().to_vec());
                    self.step = Some(ReadStep::Encryption(target, Box::new(info)));
                    Ok(Next::Call(IoCall::Read(vec![(
                        aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                        key,
                    )])))
                }
                Ok(info) => self.accessed(target, info),
                Err(CatalogError::NotFound) if matches!(self.call, DataCall::Stat { .. }) => {
                    Ok(Next::Done(HostRpcResult::StorageStat { entry: None }))
                }
                Err(error) => Self::failed(refusal(error, self.call.address())?),
            },
            ReadStep::Encryption(target, info) => {
                let row = result.value()?;
                let settings =
                    aruna_core::structs::storage::encryption::BucketEncryption::from_row(
                        row.as_deref(),
                    )
                    .map_err(|error| CatalogError::Storage(error.to_string()))?;
                if settings.is_encrypted() {
                    return Self::failed(denied(
                        "encrypted inputs require a protected workflow run",
                    ));
                }
                self.accessed(target, *info)
            }
            ReadStep::Head(target, info) => self.headed(target, *info, result),
            ReadStep::Folder(target, info) => self.listed(target, *info, true, result),
            ReadStep::List(target, info) => self.listed(target, *info, false, result),
            ReadStep::Filter(state) => self.filtered(*state, result),
            ReadStep::Get(target) => self.got(target, result),
            ReadStep::Collect(object) => self.collected(*object, result),
            ReadStep::Remote => Ok(Next::Done(result.child::<HostRpcResult, CatalogError>()??)),
        }
    }
}

/// A key as a folder prefix: empty, or ending in `/`.
fn folder(key: &str) -> String {
    match key.is_empty() || key.ends_with('/') {
        true => key.to_owned(),
        false => format!("{key}/"),
    }
}

fn sha256_of(location: &BackendLocation) -> Option<String> {
    location.hashes.get(HASH_SHA256).map(hex::encode)
}

/// What a write stores: bytes the coordinator uploaded, or a copy of an object of the run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteBody {
    Bytes(Vec<u8>),
    Copy(Target),
}

/// Where an upload goes, resolved and checked before its chunks arrive.
pub fn upload_target(
    record: &WorkflowRunRecord,
    local: NodeId,
    address: &str,
) -> Result<Target, ContractError> {
    let target = resolve(record, address)?;
    if let Some(node) = target.remote(local) {
        return Err(ContractError::unsupported(format!(
            "uploads cannot target remote node {node}"
        )));
    }
    if target.version.is_some() || target.key.is_empty() || target.key.ends_with('/') {
        return Err(ContractError::invalid(format!(
            "{address} names no new object"
        )));
    }
    Ok(target)
}

/// Where a delivery goes: the path below the run's output prefix.
pub fn delivery_target(record: &WorkflowRunRecord, path: &str) -> Result<Target, ContractError> {
    let output = record.output.as_ref().ok_or_else(no_area)?;
    let folder = path.ends_with('/');
    let clean = path.trim_start_matches('/').trim_end_matches('/');
    check_path(clean)?;
    let prefix = output.prefix.trim_end_matches('/');
    let mut key = match prefix.is_empty() {
        true => clean.to_owned(),
        false => format!("{prefix}/{clean}"),
    };
    if folder {
        key.push('/');
    }
    Ok(Target {
        bucket: output.bucket.clone(),
        key,
        version: None,
        owned: false,
        node: None,
    })
}

#[derive(Debug, PartialEq)]
enum WriteStep {
    Target,
    Source(Box<BucketInfo>),
    SourceEncryption(Box<BucketInfo>),
    Config(Box<BucketInfo>),
    Routing(Box<BucketInfo>, Option<u64>),
    Gate(Box<(BucketInfo, Option<u64>, RoutingSnapshot)>),
    Read(
        Box<(
            BucketInfo,
            Option<u64>,
            RoutingSnapshot,
            Option<GateContext>,
        )>,
    ),
    Put,
    Record(Box<UploadReceipt>),
    /// A copy whose source another node holds is read through that node.
    Remote(
        Box<(
            BucketInfo,
            Option<u64>,
            RoutingSnapshot,
            Option<GateContext>,
        )>,
    ),
}

/// Writes one object of a run as its creator: an upload's bytes into the area or a bucket the
/// creator may write, or a delivery that copies an output below the run's output prefix.
#[derive(Debug, PartialEq)]
pub struct WriteObjectOperation {
    run: WorkflowRunRecord,
    node_id: NodeId,
    routing: NodeRouting,
    target: Target,
    body: Option<WriteBody>,
    /// The path a delivery names, recorded with the run's outputs.
    delivered: Option<String>,
    source: Option<BucketInfo>,
    stream: Option<(
        BackendStream<Result<Bytes, aruna_core::stream::StreamError>>,
        u64,
    )>,
    ids: IdMint,
    now_ms: u64,
    step: WriteStep,
}

type WriteNext = Result<Next<HostRpcResult>, CatalogError>;

impl WriteObjectOperation {
    pub fn new(
        (run, node_id): (WorkflowRunRecord, NodeId),
        routing: NodeRouting,
        target: Target,
        body: WriteBody,
    ) -> Self {
        let body = match body {
            WriteBody::Copy(source) => {
                WriteBody::Copy(remote::resolve_precaptured(&run, source, node_id))
            }
            other => other,
        };
        Self {
            run,
            node_id,
            routing,
            target,
            body: Some(body),
            delivered: None,
            source: None,
            stream: None,
            ids: IdMint::default(),
            now_ms: 0,
            step: WriteStep::Target,
        }
    }

    /// Records the written object as the delivered output `path` of the run.
    pub fn delivery(mut self, path: impl Into<String>) -> Self {
        self.delivered = Some(path.into());
        self
    }

    fn failed(error: ContractError) -> WriteNext {
        Ok(Next::Done(HostRpcResult::Failed(error)))
    }

    fn access(&self, target: &Target, permission: Permission) -> IoCall {
        IoCall::child(
            ObjectAccessOperation::new(
                &creator_auth(&self.run),
                self.node_id,
                (&target.bucket, &target.key),
                permission,
            )
            .owned(target.owned),
        )
    }

    fn config(&mut self, info: BucketInfo) -> WriteNext {
        self.step = WriteStep::Config(Box::new(info));
        let read = GetConfigOperation::new(self.run.realm_id);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn put(
        &mut self,
        (info, ceiling, routing, gate): (
            BucketInfo,
            Option<u64>,
            RoutingSnapshot,
            Option<GateContext>,
        ),
    ) -> WriteNext {
        let (body, length) = match self.body.take().ok_or(IoError::Unfinished)? {
            WriteBody::Bytes(bytes) => {
                let length = bytes.len() as u64;
                (BackendStream::once(Bytes::from(bytes)), length)
            }
            WriteBody::Copy(_) => self.stream.take().ok_or(IoError::Unfinished)?,
        };
        let version = self.ids.mint(self.now_ms);
        let mut put = PutObjectOperation::new(PutObjectConfig {
            user_id: self.run.created_by,
            group_id: info.group_id,
            realm_id: self.run.realm_id,
            node_id: self.node_id,
            request: PutObjectInput {
                bucket: self.target.bucket.clone(),
                key: self.target.key.clone(),
                content_length: Some(length),
                body: Some(body),
            },
            expected_checksums: Vec::new(),
            checksum_type: None,
            exists: false,
            version_source: None,
            preassigned_version_id: Some(version),
            quota_ceiling: ceiling,
            routing,
        })
        .with_version_origin(Some(VersionOrigin::Job {
            run_id: self.run.run_id,
            started_by: Some(self.run.created_by),
        }));
        if let Some(gate) = gate {
            put = put.with_gate(gate);
        }
        self.step = WriteStep::Put;
        Ok(Next::Call(IoCall::child(put)))
    }

    fn written(&mut self, put: PutObjectResult) -> WriteNext {
        let version = (!self.target.key.ends_with('/')).then_some(put.version_id);
        let receipt = UploadReceipt {
            address: address_of(&self.target.bucket, &self.target.key, version),
            size: put.location.blob_size,
            version: version.map(|v| v.to_string()),
            sha256: sha256_of(&put.location),
        };
        let Some(path) = self.delivered.clone() else {
            return Ok(Next::Done(HostRpcResult::UploadAck {
                seq: 0,
                receipt: Some(receipt),
            }));
        };
        let version_id = put.version_id.to_string();
        let file = RunFile {
            name: path,
            bucket: Some(self.target.bucket.clone()),
            key: Some(self.target.key.clone()),
            version_id: Some(version_id.clone()),
            node_id: Some(self.node_id),
            size: Some(put.location.blob_size),
            digest: put.location.get_blake3().map(hex::encode),
        };
        let origin = RunOrigin {
            bucket: self.target.bucket.clone(),
            key: self.target.key.clone(),
            version_id,
        };
        let change = RunChange::Delivered {
            file,
            origin: Some(origin),
        };
        self.step = WriteStep::Record(Box::new(receipt));
        Ok(Next::Call(change_call(self.run.run_id, vec![change])))
    }
}

impl Operation for WriteObjectOperation {
    type Output = HostRpcResult;
    type Error = CatalogError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn seed(&mut self, entropy: u128) {
        self.ids = IdMint::new(entropy);
    }

    fn first(&mut self) -> WriteNext {
        if let Some(node) = self.target.remote(self.node_id) {
            return Self::failed(ContractError::unsupported(format!(
                "writes cannot target remote node {node}"
            )));
        }
        self.step = WriteStep::Target;
        Ok(Next::Call(self.access(&self.target, Permission::WRITE)))
    }

    fn next(&mut self, result: IoResult) -> WriteNext {
        match std::mem::replace(&mut self.step, WriteStep::Put) {
            WriteStep::Target => {
                let address = address_of(&self.target.bucket, &self.target.key, None);
                let info = match bucket_answer(result) {
                    Ok(info) => info,
                    Err(error) => return Self::failed(refusal(error, &address)?),
                };
                match &self.body {
                    // The holder authorizes the creator's read of its own copy.
                    Some(WriteBody::Copy(source)) if source.remote(self.node_id).is_some() => {
                        self.config(info)
                    }
                    Some(WriteBody::Copy(source)) => {
                        let call = self.access(source, Permission::READ);
                        self.step = WriteStep::Source(Box::new(info));
                        Ok(Next::Call(call))
                    }
                    _ => self.config(info),
                }
            }
            WriteStep::Source(info) => {
                let Some(WriteBody::Copy(source)) = &self.body else {
                    return Err(IoError::Unfinished.into());
                };
                let address = address_of(&source.bucket, &source.key, source.version);
                match bucket_answer(result) {
                    Ok(source_info) => self.source = Some(source_info),
                    Err(error) => return Self::failed(refusal(error, &address)?),
                }
                if !self.run.protected && !source.owned {
                    let key = aruna_core::types::Key::from(source.bucket.as_bytes().to_vec());
                    self.step = WriteStep::SourceEncryption(info);
                    return Ok(Next::Call(IoCall::Read(vec![(
                        aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                        key,
                    )])));
                }
                self.config(*info)
            }
            WriteStep::SourceEncryption(info) => {
                let row = result.value()?;
                let settings =
                    aruna_core::structs::storage::encryption::BucketEncryption::from_row(
                        row.as_deref(),
                    )
                    .map_err(|error| CatalogError::Storage(error.to_string()))?;
                if settings.is_encrypted() {
                    return Self::failed(denied(
                        "an unprotected run cannot copy from an encrypted bucket",
                    ));
                }
                self.config(*info)
            }
            WriteStep::Config(info) => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|error| CatalogError::Storage(error.to_string()))?;
                let ceiling = config.quota.effective_group_ceiling(&info.group_id);
                let rules = BucketRules::Named(self.target.bucket.clone());
                let routing = SnapshotOperation::new(&self.routing, info.group_id, rules);
                self.step = WriteStep::Routing(info, ceiling);
                Ok(Next::Call(IoCall::child(routing)))
            }
            WriteStep::Routing(info, ceiling) => {
                let routing = result.child::<RoutingSnapshot, RoutingInputsError>()?;
                let routing = routing.map_err(|error| CatalogError::Storage(error.to_string()))?;
                self.step = WriteStep::Gate(Box::new((*info, ceiling, routing)));
                let gate = GateOperation::new(self.run.realm_id, self.now_ms);
                Ok(Next::Call(IoCall::child(gate)))
            }
            WriteStep::Gate(parts) => {
                let (info, ceiling, routing) = *parts;
                let gate = result.child::<Option<GateContext>, GateContextError>()?;
                let gate = gate.map_err(|error| CatalogError::Storage(error.to_string()))?;
                let Some(WriteBody::Copy(source)) = &self.body else {
                    return self.put((info, ceiling, routing, gate));
                };
                if let Some(node) = source.remote(self.node_id) {
                    let Some(read) = remote::read_call(&self.run, source, node, (false, None))
                    else {
                        let message = "an object on another node is copied by its exact version";
                        return Self::failed(ContractError::invalid(message));
                    };
                    self.step = WriteStep::Remote(Box::new((info, ceiling, routing, gate)));
                    return Ok(Next::Call(read));
                }
                let group_id = (self.source.as_ref())
                    .map(|source| source.group_id)
                    .ok_or(IoError::Unfinished)?;
                let get = GetObjectOperation::new(GetObjectInput {
                    bucket: source.bucket.clone(),
                    key: source.key.clone(),
                    version_id: source.version,
                    range: None,
                    group_id,
                    user_identity: self.run.created_by,
                    realm_id: self.run.realm_id,
                    node_id: self.node_id,
                })
                .with_disallow_encrypted(!self.run.protected && !source.owned);
                self.step = WriteStep::Read(Box::new((info, ceiling, routing, gate)));
                Ok(Next::Call(IoCall::child(get)))
            }
            WriteStep::Read(parts) => {
                let object = match result.child::<GetObjectResult, GetObjectError>()? {
                    Ok(object) => object,
                    Err(GetObjectError::NoSuchKey | GetObjectError::NoSuchVersion) => {
                        return Self::failed(not_found("the output to deliver does not exist"));
                    }
                    Err(GetObjectError::EncryptedDisallowed) => {
                        return Self::failed(denied(
                            "an unprotected run cannot copy from an encrypted bucket",
                        ));
                    }
                    Err(error) => return Err(CatalogError::Storage(error.to_string())),
                };
                self.stream = Some((object.blob, object.info.size));
                self.put(*parts)
            }
            WriteStep::Remote(parts) => match remote::read_answer(result)? {
                Ok(BaoReadOutput::Stream { blob, size, .. }) => {
                    self.stream = Some((blob, size));
                    self.put(*parts)
                }
                Ok(BaoReadOutput::Metadata { .. }) => Err(IoError::Unfinished.into()),
                Err(error) => Self::failed(error),
            },
            WriteStep::Put => {
                let put = result.child::<PutObjectResult, PutObjectError>()?;
                match put {
                    Ok(put) => self.written(put),
                    Err(error) => Self::failed(ContractError::internal(error.to_string())),
                }
            }
            WriteStep::Record(receipt) => {
                changed(result)?;
                Ok(Next::Done(HostRpcResult::Delivered {
                    location: receipt.address,
                }))
            }
        }
    }
}
