//! Reads of a run's objects that another node holds, like task outputs in its run area there:
//! a stat or a chunk of the exact version, read through the holder, which authorizes the creator.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::jobs::{JobEffect, JobEvent};
use aruna_core::keyspaces::BUCKET_ENCRYPTION_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::replication_protocol::{BaoReadRefusal, BaoReadRequest, BaoReadTarget};
use aruna_core::structs::checksum::HASH_SHA256;
use aruna_core::structs::execution::job::input::InputSource;
use aruna_core::structs::execution::workflow_run::WorkflowRunRecord;
use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::storage::encryption::BucketEncryption;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::Key;
use aruna_ops_data::replication::bao_read::{BaoReadError, BaoReadOutput};
use aruna_ops_data::replication::managed_read::ManagedReadOperation;
use aruna_ops_data::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use aruna_workflow_contract::host::{ListEntry, StorageEntry};
use aruna_workflow_contract::{ContractError, DATA_CHUNK_BYTES, HostRpcResult};
use ulid::Ulid;

use super::creator_auth;
use super::data::{DataCall, ObjectAccessOperation, Target, bucket_answer};
use crate::workflows::CatalogError;

/// Attaches the admitted version for an unversioned directory child while keeping the original
/// pin identity so child grant lookup still matches precaptured links.
pub fn pin_directory_child(run: &WorkflowRunRecord, mut target: Target, local: NodeId) -> Target {
    if target.version.is_none() {
        let holder = target.node.unwrap_or(local);
        if let Some(pin) = run.directory_pins.iter().find(|pin| {
            pin.source_node_id.unwrap_or(local) == holder
                && pin.bucket == target.bucket
                && pin.key == target.key
        }) {
            target.version = Some(pin.version_id);
        }
    }
    target
}

/// Resolves a precaptured link or directory child, returning `(admitted_pin, resolved_target)`.
/// `admitted_pin` keeps the original `(node, bucket, key)` with its admitted `pin_version_id`.
pub fn resolve_pin(run: &WorkflowRunRecord, target: Target, local: NodeId) -> (Target, Target) {
    let mut pin = pin_directory_child(run, target, local);
    let holder = pin.node.unwrap_or(local);
    if pin.version.is_none()
        && let Some(entry) = run.captured_remote.iter().find(|entry| {
            entry.pin_node_id == holder
                && entry.pin_bucket == pin.bucket
                && entry.pin_key == pin.key
        })
    {
        pin.version = Some(entry.pin_version_id);
    }
    let Some(version) = pin.version else {
        return (pin.clone(), pin);
    };
    let Some(entry) = run.captured_remote.iter().find(|entry| {
        entry.pin_node_id == holder
            && entry.pin_bucket == pin.bucket
            && entry.pin_key == pin.key
            && entry.pin_version_id == version
    }) else {
        return (pin.clone(), pin);
    };
    let mut resolved = pin.clone();
    if let InputSource::S3 {
        bucket,
        key,
        version_id,
    } = &entry.resolved.source
        && let Some(resolved_ver) = version_id
            .as_deref()
            .and_then(|v| Ulid::from_string(v).ok())
    {
        resolved.bucket = bucket.clone();
        resolved.key = key.clone();
        resolved.version = Some(resolved_ver);
        resolved.node = entry.resolved.source_node_id.or(Some(holder));
        resolved.owned = false;
    }
    (pin, resolved)
}

/// Rewrites a precaptured link or captured directory child to its resolved versioned object so
/// coordinator reads work both locally and after cross-node resume.
pub fn resolve_precaptured(run: &WorkflowRunRecord, target: Target, local: NodeId) -> Target {
    resolve_pin(run, target, local).1
}

/// Whether `resolved` points to a different `(holder, bucket, key, version)` than `pin`.
pub(super) fn is_link_alias(pin: &Target, resolved: &Target, local: NodeId) -> bool {
    let pin_node = pin.node.unwrap_or(local);
    let resolved_node = resolved.node.unwrap_or(local);
    resolved_node != pin_node
        || resolved.bucket != pin.bucket
        || resolved.key != pin.key
        || resolved.version != pin.version
}

/// Whether `(holder, bucket, key)` lies under an admitted directory root or has captured pins.
pub(super) fn has_captured_dir(
    run: &WorkflowRunRecord,
    local: NodeId,
    holder: NodeId,
    bucket: &str,
    key: &str,
) -> bool {
    let prefix = match key.is_empty() || key.ends_with('/') {
        true => key.to_owned(),
        false => format!("{key}/"),
    };
    let under_root = run.directory_roots.iter().any(|root| {
        root.source_node_id.unwrap_or(local) == holder
            && root.bucket == bucket
            && (root.prefix == prefix || root.prefix.starts_with(&prefix))
    });
    if under_root {
        return true;
    }
    run.directory_pins.iter().any(|pin| {
        pin.source_node_id.unwrap_or(local) == holder
            && pin.bucket == bucket
            && pin.key.strip_prefix(&prefix).is_some_and(|r| !r.is_empty())
    })
}

/// Captures `(object_key, immediate_entry)` pairs for per-entry READ authorization of local
/// captured listings, including nested empty directory roots.
pub(super) fn captured_pins(
    run: &WorkflowRunRecord,
    local: NodeId,
    holder: NodeId,
    bucket: &str,
    key: &str,
) -> Vec<(String, ListEntry)> {
    let prefix = match key.is_empty() || key.ends_with('/') {
        true => key.to_owned(),
        false => format!("{key}/"),
    };
    let mut out = Vec::new();
    for pin in &run.directory_pins {
        if pin.source_node_id.unwrap_or(local) != holder || pin.bucket != bucket {
            continue;
        }
        let Some(rest) = pin.key.strip_prefix(&prefix) else {
            continue;
        };
        if rest.is_empty() {
            continue;
        }
        let entry = match rest.split_once('/') {
            Some((dir, _)) if !dir.is_empty() => ListEntry {
                name: dir.to_owned(),
                directory: true,
            },
            None => ListEntry {
                name: rest.to_owned(),
                directory: false,
            },
            _ => continue,
        };
        out.push((pin.key.clone(), entry));
    }
    for root in &run.directory_roots {
        if root.source_node_id.unwrap_or(local) != holder || root.bucket != bucket {
            continue;
        }
        let Some(rest) = root.prefix.strip_prefix(&prefix) else {
            continue;
        };
        let rest = rest.trim_end_matches('/');
        if rest.is_empty() {
            continue;
        }
        let dir = rest.split('/').next().unwrap_or_default();
        if !dir.is_empty() {
            out.push((
                root.prefix.clone(),
                ListEntry {
                    name: dir.to_owned(),
                    directory: true,
                },
            ));
        }
    }
    out
}

/// Remote `(object_key, version_id, Option<ListEntry>)` entries that must pass live READ
/// authorization on `holder` before `RemoteReadOperation` exposes `List` or folder `Stat`.
fn remote_dir_checks(
    run: &WorkflowRunRecord,
    holder: NodeId,
    bucket: &str,
    key: &str,
) -> Vec<(String, Ulid, Option<ListEntry>)> {
    let prefix = match key.is_empty() || key.ends_with('/') {
        true => key.to_owned(),
        false => format!("{key}/"),
    };
    let mut out = Vec::new();
    for pin in &run.directory_pins {
        if pin.source_node_id != Some(holder) || pin.bucket != bucket {
            continue;
        }
        let Some(rest) = pin.key.strip_prefix(&prefix) else {
            continue;
        };
        if rest.is_empty() {
            continue;
        }
        let entry = match rest.split_once('/') {
            Some((dir, _)) if !dir.is_empty() => ListEntry {
                name: dir.to_owned(),
                directory: true,
            },
            None => ListEntry {
                name: rest.to_owned(),
                directory: false,
            },
            _ => continue,
        };
        out.push((pin.key.clone(), pin.version_id, Some(entry)));
    }
    for root in &run.directory_roots {
        if root.source_node_id != Some(holder) || root.bucket != bucket {
            continue;
        }
        let Some(version_id) = root.version_id else {
            continue;
        };
        if root.prefix == prefix {
            out.push((root.prefix.clone(), version_id, None));
            continue;
        }
        let Some(rest) = root.prefix.strip_prefix(&prefix) else {
            continue;
        };
        let rest = rest.trim_end_matches('/');
        if rest.is_empty() {
            continue;
        }
        let dir = rest.split('/').next().unwrap_or_default();
        if !dir.is_empty() {
            out.push((
                root.prefix.clone(),
                version_id,
                Some(ListEntry {
                    name: dir.to_owned(),
                    directory: true,
                }),
            ));
        }
    }
    out
}

/// Builds a holder read for the exact `(bucket, key, version)` without resolving precaptured links.
fn exact_read(
    run: &WorkflowRunRecord,
    holder: NodeId,
    target: Target,
    (metadata_only, range): (bool, Option<Range<u64>>),
) -> Option<IoCall> {
    let request = BaoReadRequest {
        auth_context: creator_auth(run),
        realm_id: run.realm_id,
        target: BaoReadTarget::ExactVersion(VersionedObjectArn {
            realm_id: run.realm_id,
            node_id: holder,
            bucket: target.bucket,
            key: target.key,
            version: target.version?,
        }),
        expected_blake3: None,
        metadata_only,
        destination: None,
        known_refs: Vec::new(),
        range,
        disallow_encrypted: !run.protected,
    };
    Some(IoCall::child(ManagedReadOperation::new(holder, request)))
}

/// Checks READ on an exact link version at `holder` via `BaoReadTarget::LinkAccess`.
pub(super) fn link_check(run: &WorkflowRunRecord, holder: NodeId, pin: &Target) -> Option<IoCall> {
    let request = BaoReadRequest {
        auth_context: creator_auth(run),
        realm_id: run.realm_id,
        target: BaoReadTarget::LinkAccess(VersionedObjectArn {
            realm_id: run.realm_id,
            node_id: holder,
            bucket: pin.bucket.clone(),
            key: pin.key.clone(),
            version: pin.version?,
        }),
        expected_blake3: None,
        metadata_only: true,
        destination: None,
        known_refs: Vec::new(),
        range: None,
        disallow_encrypted: !run.protected,
    };
    Some(IoCall::child(ManagedReadOperation::new(holder, request)))
}

/// The read of an exact version `node` holds, as the run's creator; `None` without a version.
pub fn read_call(
    run: &WorkflowRunRecord,
    target: &Target,
    node: NodeId,
    (metadata_only, range): (bool, Option<Range<u64>>),
) -> Option<IoCall> {
    let target = resolve_precaptured(run, target.clone(), node);
    let holder = target.node.unwrap_or(node);
    exact_read(run, holder, target, (metadata_only, range))
}

/// Maps a holder refusal onto the coordinator error class without hiding backend faults.
pub(super) fn refusal_error(refusal: BaoReadRefusal) -> ContractError {
    let message = format!("the holder refused the read: {refusal:?}");
    match refusal {
        BaoReadRefusal::ReadDenied | BaoReadRefusal::BucketLocked(_) => {
            ContractError::Denied { message }
        }
        BaoReadRefusal::NotFound => ContractError::NotFound { message },
        BaoReadRefusal::InvalidTarget => ContractError::Invalid { message },
        BaoReadRefusal::BackendFailure
        | BaoReadRefusal::HashMismatch
        | BaoReadRefusal::RealmPeerDenied
        | BaoReadRefusal::HeldBy { .. } => ContractError::Internal { message },
    }
}

/// The answer of a [`read_call`], or the refusal the coordinator reads.
pub fn read_answer(result: IoResult) -> Result<Result<BaoReadOutput, ContractError>, CatalogError> {
    Ok(match result.child::<BaoReadOutput, BaoReadError>()? {
        Ok(output) => Ok(output),
        Err(BaoReadError::Refused(refusal)) => Err(refusal_error(refusal)),
        Err(error) => Err(ContractError::internal(error.to_string())),
    })
}

#[derive(Debug, PartialEq)]
enum CheckPhase {
    LinkPin,
    Remote,
    LocalAccess(Target),
    LocalEncrypt(Target),
    LocalHead,
}

#[derive(Debug, PartialEq)]
struct DirCheck {
    entry: Option<ListEntry>,
    follow_up: Option<Target>,
    phase: CheckPhase,
}

#[derive(Debug, PartialEq)]
struct AuthDir {
    stat: bool,
    holder: NodeId,
    bucket: String,
    pending: VecDeque<(String, Ulid, Option<ListEntry>)>,
    checking: Option<DirCheck>,
    allowed: BTreeMap<String, bool>,
    any_allowed: bool,
}

#[derive(Debug, PartialEq)]
enum PinPhase {
    Remote,
    LocalAccess(Target),
    LocalEncrypt(Target),
    LocalHead,
}

#[derive(Debug, PartialEq)]
enum RemoteStep {
    PinCheck(PinPhase),
    Head,
    Read,
    Collect,
    AuthList(Box<AuthDir>),
}

/// Answers a stat or a chunk read of an object another node holds; a listing there is refused.
#[derive(Debug, PartialEq)]
pub struct RemoteReadOperation {
    run: WorkflowRunRecord,
    target: Target,
    /// Original link pin that must pass READ before reading a resolved target.
    link_pin: Option<Target>,
    /// Requested directory `(holder, bucket, key)` kept before resolving precaptured markers.
    dir_target: Option<(NodeId, String, String)>,
    /// Admitted version of the requested address (preserves link version across multi-chunk reads).
    reported_version: Option<Ulid>,
    node: NodeId,
    call: DataCall,
    size: u64,
    step: RemoteStep,
}

type RemoteNext = Result<Next<HostRpcResult>, CatalogError>;

impl RemoteReadOperation {
    pub fn new(run: WorkflowRunRecord, target: Target, node: NodeId, call: DataCall) -> Self {
        let pinned = pin_directory_child(&run, target, run.node_id);
        let (pin, target) = resolve_pin(&run, pinned.clone(), run.node_id);
        let is_alias = is_link_alias(&pin, &target, run.node_id);
        // A resolved standalone file or link takes precedence over folder fallback on unversioned
        // Stat, while explicit trailing-slash directory markers stay on the folder auth path.
        let dir_target = match &call {
            DataCall::List { .. } => Some((
                pinned.node.unwrap_or(node),
                pinned.bucket.clone(),
                pinned.key.clone(),
            )),
            DataCall::Stat { .. }
                if pinned.version.is_none()
                    && (pinned.key.ends_with('/') || pin.version.is_none()) =>
            {
                Some((
                    pinned.node.unwrap_or(node),
                    pinned.bucket.clone(),
                    pinned.key.clone(),
                ))
            }
            _ => None,
        };
        let reported_version = pin.version;
        // Keep `link_pin` whenever `is_alias` is true; folder Stat returns early if checks exist.
        let link_pin = is_alias.then_some(pin);
        let node = target.node.unwrap_or(node);
        Self {
            run,
            target,
            link_pin,
            dir_target,
            reported_version,
            node,
            call,
            size: 0,
            step: RemoteStep::Head,
        }
    }

    fn start_head(&mut self) -> RemoteNext {
        let Some(head) = exact_read(&self.run, self.node, self.target.clone(), (true, None)) else {
            return Self::failed(ContractError::invalid(
                "an object on another node is read by its exact version",
            ));
        };
        self.step = RemoteStep::Head;
        Ok(Next::Call(head))
    }

    fn head_pin(&mut self, pin: Target) -> RemoteNext {
        let head = HeadObjectOperation::new(HeadObjectInput {
            bucket: pin.bucket,
            key: pin.key,
            version_id: pin.version,
        });
        self.step = RemoteStep::PinCheck(PinPhase::LocalHead);
        Ok(Next::Call(IoCall::child(head)))
    }

    fn failed(error: ContractError) -> RemoteNext {
        Ok(Next::Done(HostRpcResult::Failed(error)))
    }

    fn version(&self) -> Option<String> {
        self.reported_version
            .or(self.target.version)
            .map(|version| version.to_string())
    }

    fn headed(
        &mut self,
        size: u64,
        hashes: &std::collections::HashMap<String, Vec<u8>>,
    ) -> RemoteNext {
        self.size = size;
        let offset = match self.call {
            DataCall::Stat { .. } => {
                let entry = StorageEntry {
                    directory: false,
                    size,
                    modified_ms: None,
                    version: self.version(),
                    sha256: hashes.get(HASH_SHA256).map(hex::encode),
                    aruna_id: None,
                    endpoints: Vec::new(),
                };
                return Ok(Next::Done(HostRpcResult::StorageStat {
                    entry: Some(entry),
                }));
            }
            DataCall::Download { offset, .. } => offset,
            DataCall::List { .. } => return Err(IoError::Unfinished.into()),
        };
        if offset >= size {
            return Ok(Next::Done(HostRpcResult::StorageChunk {
                bytes: Vec::new(),
                eof: true,
                version: self.version(),
            }));
        }
        let end = size.min(offset.saturating_add(DATA_CHUNK_BYTES as u64));
        let read = read_call(
            &self.run,
            &self.target,
            self.node,
            (false, Some(offset..end)),
        );
        self.step = RemoteStep::Read;
        Ok(Next::Call(read.ok_or(IoError::Unfinished)?))
    }

    fn next_dir_auth(&mut self, mut state: AuthDir) -> RemoteNext {
        if let Some((key, version, entry)) = state.pending.pop_front() {
            let pin = Target {
                bucket: state.bucket.clone(),
                key,
                version: Some(version),
                owned: false,
                node: Some(state.holder),
            };
            let resolved = resolve_precaptured(&self.run, pin.clone(), self.run.node_id);
            let is_alias = is_link_alias(&pin, &resolved, self.run.node_id);
            // Check READ on the original pin at its holder before accepting any link target.
            let (call, phase) = match is_alias {
                true => (
                    link_check(&self.run, state.holder, &pin).ok_or(IoError::Unfinished)?,
                    CheckPhase::LinkPin,
                ),
                false => (
                    exact_read(&self.run, state.holder, pin, (true, None))
                        .ok_or(IoError::Unfinished)?,
                    CheckPhase::Remote,
                ),
            };
            state.checking = Some(DirCheck {
                entry,
                follow_up: is_alias.then_some(resolved),
                phase,
            });
            self.step = RemoteStep::AuthList(Box::new(state));
            return Ok(Next::Call(call));
        }
        if state.stat {
            return Ok(Next::Done(HostRpcResult::StorageStat {
                entry: state.any_allowed.then(super::data::folder_entry),
            }));
        }
        if !state.any_allowed {
            return Self::failed(ContractError::Denied {
                message: "the holder refused access to the folder".into(),
            });
        }
        let entries = state
            .allowed
            .into_iter()
            .map(|(name, directory)| ListEntry { name, directory })
            .collect();
        Ok(Next::Done(HostRpcResult::StorageList { entries }))
    }

    fn accept_entry(&mut self, mut state: AuthDir, entry: Option<ListEntry>) -> RemoteNext {
        state.any_allowed = true;
        if state.stat {
            return Ok(Next::Done(HostRpcResult::StorageStat {
                entry: Some(super::data::folder_entry()),
            }));
        }
        if let Some(entry) = entry {
            state
                .allowed
                .entry(entry.name)
                .and_modify(|is_dir| *is_dir |= entry.directory)
                .or_insert(entry.directory);
        }
        self.next_dir_auth(state)
    }

    fn check_target(
        &mut self,
        mut state: AuthDir,
        entry: Option<ListEntry>,
        target: Target,
    ) -> RemoteNext {
        let holder = target.node.unwrap_or(state.holder);
        if holder == self.run.node_id {
            let access = ObjectAccessOperation::new(
                &creator_auth(&self.run),
                self.run.node_id,
                (&target.bucket, &target.key),
                Permission::READ,
            )
            .owned(target.owned);
            state.checking = Some(DirCheck {
                entry,
                follow_up: None,
                phase: CheckPhase::LocalAccess(target),
            });
            self.step = RemoteStep::AuthList(Box::new(state));
            return Ok(Next::Call(IoCall::child(access)));
        }
        let call =
            exact_read(&self.run, holder, target, (true, None)).ok_or(IoError::Unfinished)?;
        state.checking = Some(DirCheck {
            entry,
            follow_up: None,
            phase: CheckPhase::Remote,
        });
        self.step = RemoteStep::AuthList(Box::new(state));
        Ok(Next::Call(call))
    }

    fn head_local(
        &mut self,
        mut state: AuthDir,
        entry: Option<ListEntry>,
        target: Target,
    ) -> RemoteNext {
        let head = HeadObjectOperation::new(HeadObjectInput {
            bucket: target.bucket,
            key: target.key,
            version_id: target.version,
        });
        state.checking = Some(DirCheck {
            entry,
            follow_up: None,
            phase: CheckPhase::LocalHead,
        });
        self.step = RemoteStep::AuthList(Box::new(state));
        Ok(Next::Call(IoCall::child(head)))
    }
}

impl Operation for RemoteReadOperation {
    type Output = HostRpcResult;
    type Error = CatalogError;

    fn first(&mut self) -> RemoteNext {
        if matches!(self.call, DataCall::List { .. }) {
            let Some((holder, bucket, key)) = self.dir_target.clone() else {
                return Err(IoError::Unfinished.into());
            };
            let checks = remote_dir_checks(&self.run, holder, &bucket, &key);
            if checks.is_empty() {
                return Self::failed(ContractError::unsupported(format!(
                    "a folder on node {holder} is not listed from here"
                )));
            }
            return self.next_dir_auth(AuthDir {
                stat: false,
                holder,
                bucket,
                pending: checks.into(),
                checking: None,
                allowed: BTreeMap::new(),
                any_allowed: false,
            });
        }
        if matches!(self.call, DataCall::Stat { .. })
            && let Some((holder, bucket, key)) = self.dir_target.clone()
        {
            let checks = remote_dir_checks(&self.run, holder, &bucket, &key);
            if !checks.is_empty() {
                return self.next_dir_auth(AuthDir {
                    stat: true,
                    holder,
                    bucket,
                    pending: checks.into(),
                    checking: None,
                    allowed: BTreeMap::new(),
                    any_allowed: false,
                });
            }
        }
        if let Some(pin) = self.link_pin.clone() {
            let holder = pin.node.unwrap_or(self.run.node_id);
            if holder == self.run.node_id {
                let access = ObjectAccessOperation::new(
                    &creator_auth(&self.run),
                    self.run.node_id,
                    (&pin.bucket, &pin.key),
                    Permission::READ,
                )
                .owned(pin.owned);
                self.step = RemoteStep::PinCheck(PinPhase::LocalAccess(pin));
                return Ok(Next::Call(IoCall::child(access)));
            }
            let Some(check) = link_check(&self.run, holder, &pin) else {
                return Self::failed(ContractError::invalid(
                    "an object on another node is read by its exact version",
                ));
            };
            self.step = RemoteStep::PinCheck(PinPhase::Remote);
            return Ok(Next::Call(check));
        }
        self.start_head()
    }

    fn next(&mut self, result: IoResult) -> RemoteNext {
        match std::mem::replace(&mut self.step, RemoteStep::Head) {
            RemoteStep::PinCheck(phase) => match phase {
                PinPhase::Remote => match result.child::<BaoReadOutput, BaoReadError>()? {
                    Ok(BaoReadOutput::Authorized) => self.start_head(),
                    Ok(_) => Err(IoError::Unfinished.into()),
                    Err(BaoReadError::Refused(BaoReadRefusal::NotFound))
                        if matches!(self.call, DataCall::Stat { .. }) =>
                    {
                        Ok(Next::Done(HostRpcResult::StorageStat { entry: None }))
                    }
                    Err(BaoReadError::Refused(refusal)) => Self::failed(refusal_error(refusal)),
                    Err(error) => Self::failed(ContractError::internal(error.to_string())),
                },
                PinPhase::LocalAccess(pin) => match bucket_answer(result) {
                    Ok(_) if !self.run.protected && !pin.owned => {
                        let key = Key::from(pin.bucket.as_bytes().to_vec());
                        self.step = RemoteStep::PinCheck(PinPhase::LocalEncrypt(pin));
                        Ok(Next::Call(IoCall::Read(vec![(
                            BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                            key,
                        )])))
                    }
                    Ok(_) => self.head_pin(pin),
                    Err(CatalogError::NotFound) if matches!(self.call, DataCall::Stat { .. }) => {
                        Ok(Next::Done(HostRpcResult::StorageStat { entry: None }))
                    }
                    Err(error) => Self::failed(super::data::refusal(error, self.call.address())?),
                },
                PinPhase::LocalEncrypt(pin) => {
                    let row = result.value()?;
                    let settings = BucketEncryption::from_row(row.as_deref())
                        .map_err(|error| CatalogError::Storage(error.to_string()))?;
                    if settings.is_encrypted() {
                        Self::failed(ContractError::Denied {
                            message: "encrypted inputs require a protected workflow run".into(),
                        })
                    } else {
                        self.head_pin(pin)
                    }
                }
                PinPhase::LocalHead => match result.child::<HeadObjectResult, HeadObjectError>()? {
                    Ok(_) => self.start_head(),
                    Err(
                        HeadObjectError::NoSuchKey
                        | HeadObjectError::NoSuchVersion
                        | HeadObjectError::DeleteMarker,
                    ) if matches!(self.call, DataCall::Stat { .. }) => {
                        Ok(Next::Done(HostRpcResult::StorageStat { entry: None }))
                    }
                    Err(
                        HeadObjectError::NoSuchKey
                        | HeadObjectError::NoSuchVersion
                        | HeadObjectError::DeleteMarker,
                    ) => Self::failed(ContractError::NotFound {
                        message: format!("{} does not exist", self.call.address()),
                    }),
                    Err(error) => Err(CatalogError::Storage(error.to_string())),
                },
            },
            RemoteStep::Head => match read_answer(result)? {
                Ok(BaoReadOutput::Metadata { size, hashes, .. })
                | Ok(BaoReadOutput::Stream { size, hashes, .. }) => self.headed(size, &hashes),
                Ok(BaoReadOutput::Authorized) => Err(IoError::Unfinished.into()),
                Err(error) => Self::failed(error),
            },
            RemoteStep::Read => match read_answer(result)? {
                Ok(BaoReadOutput::Stream { blob, .. }) => {
                    let collect = JobEffect::CollectUpTo {
                        stream: blob,
                        limit: DATA_CHUNK_BYTES as u64,
                    };
                    self.step = RemoteStep::Collect;
                    Ok(Next::Call(IoCall::Effect(Box::new(Effect::Job(collect)))))
                }
                Ok(BaoReadOutput::Metadata { .. } | BaoReadOutput::Authorized) => {
                    Err(IoError::Unfinished.into())
                }
                Err(error) => Self::failed(error),
            },
            RemoteStep::Collect => {
                let DataCall::Download { offset, .. } = self.call else {
                    return Err(IoError::Unfinished.into());
                };
                let bytes = match result.event()? {
                    Event::Job(JobEvent::CollectedUpTo(Ok(Some(bytes)))) => bytes,
                    Event::Job(JobEvent::CollectedUpTo(other)) => {
                        let message = format!("a remote chunk was not read: {other:?}");
                        return Err(CatalogError::Storage(message));
                    }
                    _ => return Err(crate::workflows::unexpected("JobEvent::CollectedUpTo")),
                };
                let eof = offset + bytes.len() as u64 >= self.size;
                Ok(Next::Done(HostRpcResult::StorageChunk {
                    bytes,
                    eof,
                    version: self.version(),
                }))
            }
            RemoteStep::AuthList(mut state) => {
                let check = state.checking.take().ok_or(IoError::Unfinished)?;
                match check.phase {
                    CheckPhase::LinkPin => match result.child::<BaoReadOutput, BaoReadError>()? {
                        Ok(BaoReadOutput::Authorized) => match check.follow_up {
                            Some(target) => self.check_target(*state, check.entry, target),
                            None => self.accept_entry(*state, check.entry),
                        },
                        Ok(_) => Err(IoError::Unfinished.into()),
                        Err(BaoReadError::Refused(BaoReadRefusal::ReadDenied)) => {
                            self.next_dir_auth(*state)
                        }
                        Err(BaoReadError::Refused(refusal)) => Self::failed(refusal_error(refusal)),
                        Err(error) => Self::failed(ContractError::internal(error.to_string())),
                    },
                    CheckPhase::Remote => match result.child::<BaoReadOutput, BaoReadError>()? {
                        Ok(BaoReadOutput::Metadata { .. }) | Ok(BaoReadOutput::Stream { .. }) => {
                            match check.follow_up {
                                Some(target) => self.check_target(*state, check.entry, target),
                                None => self.accept_entry(*state, check.entry),
                            }
                        }
                        Ok(BaoReadOutput::Authorized) => Err(IoError::Unfinished.into()),
                        Err(BaoReadError::Refused(BaoReadRefusal::ReadDenied)) => {
                            self.next_dir_auth(*state)
                        }
                        Err(BaoReadError::Refused(refusal)) => Self::failed(refusal_error(refusal)),
                        Err(error) => Self::failed(ContractError::internal(error.to_string())),
                    },
                    CheckPhase::LocalAccess(target) => match bucket_answer(result) {
                        Ok(_) if !self.run.protected && !target.owned => {
                            let key = Key::from(target.bucket.as_bytes().to_vec());
                            state.checking = Some(DirCheck {
                                entry: check.entry,
                                follow_up: None,
                                phase: CheckPhase::LocalEncrypt(target),
                            });
                            self.step = RemoteStep::AuthList(state);
                            Ok(Next::Call(IoCall::Read(vec![(
                                BUCKET_ENCRYPTION_KEYSPACE.to_string(),
                                key,
                            )])))
                        }
                        Ok(_) => self.head_local(*state, check.entry, target),
                        Err(CatalogError::Forbidden) => self.next_dir_auth(*state),
                        Err(CatalogError::NotFound) => Self::failed(ContractError::NotFound {
                            message: format!("{} does not exist", target.key),
                        }),
                        Err(error) => Err(error),
                    },
                    CheckPhase::LocalEncrypt(target) => {
                        let row = result.value()?;
                        let settings = BucketEncryption::from_row(row.as_deref())
                            .map_err(|error| CatalogError::Storage(error.to_string()))?;
                        if settings.is_encrypted() {
                            self.next_dir_auth(*state)
                        } else {
                            self.head_local(*state, check.entry, target)
                        }
                    }
                    CheckPhase::LocalHead => {
                        match result.child::<HeadObjectResult, HeadObjectError>()? {
                            Ok(_) => self.accept_entry(*state, check.entry),
                            Err(
                                HeadObjectError::NoSuchKey
                                | HeadObjectError::NoSuchVersion
                                | HeadObjectError::DeleteMarker,
                            ) => Self::failed(ContractError::NotFound {
                                message: "the link target does not exist".into(),
                            }),
                            Err(error) => Err(CatalogError::Storage(error.to_string())),
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{REALM, captured_input, node, user};
    use aruna_core::structs::execution::job::input::{InputMode, InputSelection};
    use aruna_core::structs::execution::workflow_run::{
        CacheScope, PrecapturedInput, RunState, WorkflowRunRecord,
    };

    // Coordinator remote reads resolve precaptured link pins to their target object version.
    #[test]
    fn resolves_precaptured_link() {
        let link_ver = Ulid::from_parts(2, 1);
        let target_ver = Ulid::from_parts(3, 1);
        let resolved_sel = InputSelection {
            source: InputSource::S3 {
                bucket: "data".into(),
                key: "real.dat".into(),
                version_id: Some(target_ver.to_string()),
            },
            source_node_id: Some(node(2)),
            dest_key: "data/alias.link".into(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        };
        let mut cap = captured_input(&resolved_sel, node(2));
        cap.version_id = target_ver;
        let run = WorkflowRunRecord {
            run_id: Ulid::from_parts(1, 1),
            lineage: Ulid::from_parts(1, 1),
            group_id: aruna_core::types::GroupId::from_bytes([1; 16]),
            workflow_id: Ulid::from_parts(1, 2),
            workflow_name: "demo".into(),
            version: 1,
            realm_id: REALM,
            node_id: node(1),
            local_only: false,
            last_event_seq: 0,
            state: RunState::Running,
            created_by: user(),
            created_at_ms: 1,
            started_at_ms: None,
            finished_at_ms: None,
            error: None,
            values_json: "{}".into(),
            inputs_json: None,
            output: None,
            provenance_crate: false,
            provenance_status: None,
            provenance_document_id: None,
            outputs: Vec::new(),
            origin: None,
            parent_job_id: None,
            claim_token_digest: None,
            attempts: 1,
            steps: Vec::new(),
            resumes_run_id: None,
            repeats_run_id: None,
            cache_scope: CacheScope::Group,
            placement_site: None,
            protected: false,
            secrets: Default::default(),
            path_restrictions: None,
            captured_remote: vec![PrecapturedInput {
                pin_node_id: node(2),
                pin_bucket: "data".into(),
                pin_key: "alias.link".into(),
                pin_version_id: link_ver,
                resolved: resolved_sel,
                captured: cap,
            }],
            directory_pins: Vec::new(),
            directory_roots: Vec::new(),
            used_tools: Vec::new(),
            limits: None,
            automation: None,
        };
        let link_target = Target {
            bucket: "data".into(),
            key: "alias.link".into(),
            version: Some(link_ver),
            owned: false,
            node: Some(node(2)),
        };
        let op = RemoteReadOperation::new(
            run.clone(),
            link_target,
            node(2),
            DataCall::Stat {
                address: "aruna://data/alias.link".into(),
            },
        );
        assert_eq!(op.target.key, "real.dat");
        assert_eq!(op.target.version, Some(target_ver));
        assert_eq!(op.node, node(2));

        // After resume onto the former remote holder (`node(2)`), omitting `node` still resolves.
        let implicit_local = Target {
            bucket: "data".into(),
            key: "alias.link".into(),
            version: Some(link_ver),
            owned: true,
            node: None,
        };
        let resolved_local = resolve_precaptured(&run, implicit_local, node(2));
        assert_eq!(resolved_local.key, "real.dat");
        assert_eq!(resolved_local.version, Some(target_ver));
        assert!(!resolved_local.owned);
    }

    // An unversioned directory child resolves via `directory_pins` before an earlier explicit pin.
    #[test]
    fn prefers_directory_pin() {
        use aruna_core::structs::execution::workflow_run::DirectoryPin;
        let old_ver = Ulid::from_parts(2, 1);
        let new_ver = Ulid::from_parts(4, 1);
        let target_ver = Ulid::from_parts(5, 1);
        let old_sel = InputSelection {
            source: InputSource::S3 {
                bucket: "data".into(),
                key: "dir/a.txt".into(),
                version_id: Some(old_ver.to_string()),
            },
            source_node_id: Some(node(2)),
            dest_key: "data/dir/a.txt".into(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        };
        let new_sel = InputSelection {
            source: InputSource::S3 {
                bucket: "data".into(),
                key: "dir/real.txt".into(),
                version_id: Some(target_ver.to_string()),
            },
            source_node_id: Some(node(2)),
            dest_key: "data/dir/a.txt".into(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        };
        let mut old_cap = captured_input(&old_sel, node(2));
        old_cap.version_id = old_ver;
        let mut new_cap = captured_input(&new_sel, node(2));
        new_cap.version_id = target_ver;
        let run = WorkflowRunRecord {
            run_id: Ulid::from_parts(1, 1),
            lineage: Ulid::from_parts(1, 1),
            group_id: aruna_core::types::GroupId::from_bytes([1; 16]),
            workflow_id: Ulid::from_parts(1, 2),
            workflow_name: "demo".into(),
            version: 1,
            realm_id: REALM,
            node_id: node(1),
            local_only: false,
            last_event_seq: 0,
            state: RunState::Running,
            created_by: user(),
            created_at_ms: 1,
            started_at_ms: None,
            finished_at_ms: None,
            error: None,
            values_json: "{}".into(),
            inputs_json: None,
            output: None,
            provenance_crate: false,
            provenance_status: None,
            provenance_document_id: None,
            outputs: Vec::new(),
            origin: None,
            parent_job_id: None,
            claim_token_digest: None,
            attempts: 1,
            steps: Vec::new(),
            resumes_run_id: None,
            repeats_run_id: None,
            cache_scope: CacheScope::Group,
            placement_site: None,
            protected: false,
            secrets: Default::default(),
            path_restrictions: None,
            captured_remote: vec![
                PrecapturedInput {
                    pin_node_id: node(2),
                    pin_bucket: "data".into(),
                    pin_key: "dir/a.txt".into(),
                    pin_version_id: old_ver,
                    resolved: old_sel,
                    captured: old_cap,
                },
                PrecapturedInput {
                    pin_node_id: node(2),
                    pin_bucket: "data".into(),
                    pin_key: "dir/a.txt".into(),
                    pin_version_id: new_ver,
                    resolved: new_sel,
                    captured: new_cap,
                },
            ],
            directory_pins: vec![DirectoryPin {
                bucket: "data".into(),
                key: "dir/a.txt".into(),
                version_id: new_ver,
                source_node_id: Some(node(2)),
            }],
            directory_roots: Vec::new(),
            used_tools: Vec::new(),
            limits: None,
            automation: None,
        };
        let child = Target {
            bucket: "data".into(),
            key: "dir/a.txt".into(),
            version: None,
            owned: false,
            node: Some(node(2)),
        };
        let pinned = pin_directory_child(&run, child.clone(), node(1));
        assert_eq!(pinned.key, "dir/a.txt");
        assert_eq!(pinned.version, Some(new_ver));

        let resolved = resolve_precaptured(&run, child, node(1));
        assert_eq!(resolved.key, "dir/real.txt");
        assert_eq!(resolved.version, Some(target_ver));

        let explicit = Target {
            bucket: "data".into(),
            key: "dir/a.txt".into(),
            version: Some(old_ver),
            owned: false,
            node: Some(node(2)),
        };
        let explicit_res = resolve_precaptured(&run, explicit, node(1));
        assert_eq!(explicit_res.key, "dir/a.txt");
        assert_eq!(explicit_res.version, Some(old_ver));

        // RemoteReadOperation reports the admitted link version (`new_ver`) so chunk 2 pins the
        // link key with `new_ver` and still resolves to `dir/real.txt@target_ver`.
        let child_target = Target {
            bucket: "data".into(),
            key: "dir/a.txt".into(),
            version: None,
            owned: false,
            node: Some(node(2)),
        };
        let op1 = RemoteReadOperation::new(
            run.clone(),
            child_target,
            node(2),
            DataCall::Download {
                address: "s3://data/dir/a.txt".into(),
                offset: 0,
            },
        );
        assert_eq!(op1.version().as_deref(), Some(new_ver.to_string().as_str()));
        assert_eq!(op1.target.key, "dir/real.txt");
        assert_eq!(op1.target.version, Some(target_ver));

        let chunk2_target = Target {
            bucket: "data".into(),
            key: "dir/a.txt".into(),
            version: Some(new_ver),
            owned: false,
            node: Some(node(2)),
        };
        let op2 = RemoteReadOperation::new(
            run,
            chunk2_target,
            node(2),
            DataCall::Download {
                address: format!("s3://data/dir/a.txt?version={new_ver}"),
                offset: 1024,
            },
        );
        assert_eq!(op2.version().as_deref(), Some(new_ver.to_string().as_str()));
        assert_eq!(op2.target.key, "dir/real.txt");
        assert_eq!(op2.target.version, Some(target_ver));
    }

    fn base_run() -> WorkflowRunRecord {
        WorkflowRunRecord {
            run_id: Ulid::from_parts(1, 1),
            lineage: Ulid::from_parts(1, 1),
            group_id: aruna_core::types::GroupId::from_bytes([1; 16]),
            workflow_id: Ulid::from_parts(1, 2),
            workflow_name: "demo".into(),
            version: 1,
            realm_id: REALM,
            node_id: node(1),
            local_only: false,
            last_event_seq: 0,
            state: RunState::Running,
            created_by: user(),
            created_at_ms: 1,
            started_at_ms: None,
            finished_at_ms: None,
            error: None,
            values_json: "{}".into(),
            inputs_json: None,
            output: None,
            provenance_crate: false,
            provenance_status: None,
            provenance_document_id: None,
            outputs: Vec::new(),
            origin: None,
            parent_job_id: None,
            claim_token_digest: None,
            attempts: 1,
            steps: Vec::new(),
            resumes_run_id: None,
            repeats_run_id: None,
            cache_scope: CacheScope::Group,
            placement_site: None,
            protected: false,
            secrets: Default::default(),
            path_restrictions: None,
            captured_remote: Vec::new(),
            directory_pins: Vec::new(),
            directory_roots: Vec::new(),
            used_tools: Vec::new(),
        }
    }

    fn ok_meta() -> IoResult {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(Ok::<
            BaoReadOutput,
            BaoReadError,
        >(
            BaoReadOutput::Metadata {
                size: 4,
                blake3: [7; 32],
                etag: None,
                hashes: Default::default(),
            },
        )))
    }

    fn ok_auth() -> IoResult {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(Ok::<
            BaoReadOutput,
            BaoReadError,
        >(
            BaoReadOutput::Authorized,
        )))
    }

    fn refused_meta(reason: aruna_core::replication_protocol::BaoReadRefusal) -> IoResult {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(Err::<
            BaoReadOutput,
            BaoReadError,
        >(
            BaoReadError::Refused(reason),
        )))
    }

    // Listing and stat authorize the original remote link pin at its holder before the target.
    #[test]
    fn checks_link_pin() {
        use aruna_core::calls::ChildOutcome;
        use aruna_core::replication_protocol::BaoReadRefusal;
        use aruna_core::structs::execution::workflow_run::DirectoryPin;
        use aruna_core::structs::storage::blob::BucketInfo;
        use aruna_core::structs::storage::multipart::MultipartChecksumType;

        let link_ver = Ulid::from_parts(2, 1);
        let real_ver = Ulid::from_parts(3, 1);
        let make_run = |target_node: NodeId| {
            let sel = InputSelection {
                source: InputSource::S3 {
                    bucket: "other".into(),
                    key: "real.txt".into(),
                    version_id: Some(real_ver.to_string()),
                },
                source_node_id: Some(target_node),
                dest_key: "data/folder/alias.link".into(),
                mode: InputMode::Snapshot,
                container_path: None,
                name: None,
                description: None,
            };
            let mut cap = captured_input(&sel, target_node);
            cap.version_id = real_ver;
            let mut run = base_run();
            run.directory_pins.push(DirectoryPin {
                bucket: "data".into(),
                key: "folder/alias.link".into(),
                version_id: link_ver,
                source_node_id: Some(node(2)),
            });
            run.captured_remote.push(PrecapturedInput {
                pin_node_id: node(2),
                pin_bucket: "data".into(),
                pin_key: "folder/alias.link".into(),
                pin_version_id: link_ver,
                resolved: sel,
                captured: cap,
            });
            run
        };
        let dir_target = Target {
            bucket: "data".into(),
            key: "folder/".into(),
            version: None,
            owned: false,
            node: Some(node(2)),
        };

        // Revoked READ on the link omits it from List and makes folder Stat return None in 1 step.
        let run = make_run(node(3));
        let mut list = RemoteReadOperation::new(
            run.clone(),
            dir_target.clone(),
            node(2),
            DataCall::List {
                address: "s3://data/folder/".into(),
            },
        );
        let first = list.first().unwrap();
        assert!(format!("{first:?}").contains("LinkAccess"));
        assert!(format!("{first:?}").contains("folder/alias.link"));
        assert!(matches!(
            list.next(refused_meta(BaoReadRefusal::ReadDenied)).unwrap(),
            Next::Done(HostRpcResult::Failed(ContractError::Denied { .. }))
        ));

        let mut stat = RemoteReadOperation::new(
            run.clone(),
            dir_target.clone(),
            node(2),
            DataCall::Stat {
                address: "s3://data/folder/".into(),
            },
        );
        let first = stat.first().unwrap();
        assert!(format!("{first:?}").contains("LinkAccess"));
        assert!(format!("{first:?}").contains("folder/alias.link"));
        assert_eq!(
            stat.next(refused_meta(BaoReadRefusal::ReadDenied)).unwrap(),
            Next::Done(HostRpcResult::StorageStat { entry: None })
        );

        // Allowed pin followed by denied target still omits the link.
        let mut list = RemoteReadOperation::new(
            run.clone(),
            dir_target.clone(),
            node(2),
            DataCall::List {
                address: "s3://data/folder/".into(),
            },
        );
        assert!(matches!(
            list.first().unwrap(),
            Next::Call(IoCall::Child(_))
        ));
        let second = list.next(ok_auth()).unwrap();
        assert!(format!("{second:?}").contains("real.txt"));
        assert!(matches!(
            list.next(refused_meta(BaoReadRefusal::ReadDenied)).unwrap(),
            Next::Done(HostRpcResult::Failed(ContractError::Denied { .. }))
        ));

        // Allowed pin and allowed target expose the original link name.
        let mut list = RemoteReadOperation::new(
            run,
            dir_target.clone(),
            node(2),
            DataCall::List {
                address: "s3://data/folder/".into(),
            },
        );
        assert!(matches!(
            list.first().unwrap(),
            Next::Call(IoCall::Child(_))
        ));
        assert!(matches!(
            list.next(ok_auth()).unwrap(),
            Next::Call(IoCall::Child(_))
        ));
        assert_eq!(
            list.next(ok_meta()).unwrap(),
            Next::Done(HostRpcResult::StorageList {
                entries: vec![ListEntry {
                    name: "alias.link".into(),
                    directory: false,
                }],
            })
        );

        // A remote link resolving to a local target checks the remote pin then authorizes locally.
        let local_run = make_run(node(1));
        let mut list = RemoteReadOperation::new(
            local_run,
            dir_target,
            node(2),
            DataCall::List {
                address: "s3://data/folder/".into(),
            },
        );
        let first = list.first().unwrap();
        assert!(format!("{first:?}").contains("LinkAccess"));
        assert!(format!("{first:?}").contains("folder/alias.link"));
        let second = list.next(ok_auth()).unwrap();
        assert!(format!("{second:?}").contains("ObjectAccessOperation"));
        let info = BucketInfo {
            group_id: aruna_core::types::GroupId::from_bytes([1; 16]),
            created_at: std::time::UNIX_EPOCH,
            created_by: user(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Default::default(),
            distribution: None,
        };
        let third = list
            .next(IoResult::Child(ChildOutcome::new(Ok::<_, CatalogError>(
                info,
            ))))
            .unwrap();
        assert!(matches!(third, Next::Call(IoCall::Read(_))));
        let fourth = list.next(IoResult::Values(vec![None])).unwrap();
        assert!(format!("{fourth:?}").contains("HeadObjectOperation"));
        let head = HeadObjectResult {
            location: None,
            metadata: Default::default(),
            source_metadata: None,
            last_refresh: None,
            version_created_at: None,
            version_id: Some(real_ver),
            resolved_version_id: Some(real_ver),
            checksum_type: MultipartChecksumType::FullObject,
            composite_hashes: Default::default(),
            part_count: None,
            source_policies: Vec::new(),
            source_binding: None,
        };
        assert_eq!(
            list.next(IoResult::Child(ChildOutcome::new(
                Ok::<_, HeadObjectError>(head)
            )))
            .unwrap(),
            Next::Done(HostRpcResult::StorageList {
                entries: vec![ListEntry {
                    name: "alias.link".into(),
                    directory: false,
                }],
            })
        );
    }

    // Direct remote file stat/download checks READ on the original link pin before the target.
    #[test]
    fn checks_direct_link() {
        use aruna_core::calls::ChildOutcome;
        use aruna_core::structs::storage::blob::BucketInfo;

        let link_ver = Ulid::from_parts(2, 1);
        let real_ver = Ulid::from_parts(3, 1);
        let make_run = |pin_node: NodeId| {
            let sel = InputSelection {
                source: InputSource::S3 {
                    bucket: "other".into(),
                    key: "real.txt".into(),
                    version_id: Some(real_ver.to_string()),
                },
                source_node_id: Some(node(3)),
                dest_key: "data/standalone.link".into(),
                mode: InputMode::Snapshot,
                container_path: None,
                name: None,
                description: None,
            };
            let mut cap = captured_input(&sel, node(3));
            cap.version_id = real_ver;
            let mut run = base_run();
            run.captured_remote.push(PrecapturedInput {
                pin_node_id: pin_node,
                pin_bucket: "data".into(),
                pin_key: "standalone.link".into(),
                pin_version_id: link_ver,
                resolved: sel,
                captured: cap,
            });
            run
        };

        // Standalone unversioned remote link preserves pin_version_id and checks LinkAccess first.
        let run = make_run(node(2));
        let req_target = Target {
            bucket: "data".into(),
            key: "standalone.link".into(),
            version: None,
            owned: false,
            node: Some(node(2)),
        };
        let mut denied = RemoteReadOperation::new(
            run.clone(),
            req_target.clone(),
            node(3),
            DataCall::Stat {
                address: "s3://data/standalone.link".into(),
            },
        );
        assert_eq!(
            denied.version().as_deref(),
            Some(link_ver.to_string().as_str())
        );
        let first = denied.first().unwrap();
        assert!(format!("{first:?}").contains("LinkAccess"));
        assert!(format!("{first:?}").contains("standalone.link"));
        assert!(matches!(
            denied
                .next(refused_meta(BaoReadRefusal::ReadDenied))
                .unwrap(),
            Next::Done(HostRpcResult::Failed(ContractError::Denied { .. }))
        ));

        let mut allowed = RemoteReadOperation::new(
            run,
            req_target,
            node(3),
            DataCall::Stat {
                address: "s3://data/standalone.link".into(),
            },
        );
        assert!(matches!(
            allowed.first().unwrap(),
            Next::Call(IoCall::Child(_))
        ));
        let head = allowed.next(ok_auth()).unwrap();
        assert!(format!("{head:?}").contains("ExactVersion"));
        assert!(format!("{head:?}").contains("real.txt"));
        match allowed.next(ok_meta()).unwrap() {
            Next::Done(HostRpcResult::StorageStat { entry: Some(entry) }) => {
                assert_eq!(
                    entry.version.as_deref(),
                    Some(link_ver.to_string().as_str())
                );
            }
            other => panic!("expected file stat, got {other:?}"),
        }

        // Local link resolving to a remote target checks local ObjectAccess before remote Head.
        let local_run = make_run(node(1));
        let local_target = Target {
            bucket: "data".into(),
            key: "standalone.link".into(),
            version: None,
            owned: false,
            node: None,
        };
        let mut local_denied = RemoteReadOperation::new(
            local_run,
            local_target,
            node(3),
            DataCall::Stat {
                address: "s3://data/standalone.link".into(),
            },
        );
        let first = local_denied.first().unwrap();
        assert!(format!("{first:?}").contains("ObjectAccessOperation"));
        assert!(format!("{first:?}").contains("standalone.link"));
        assert!(matches!(
            local_denied
                .next(IoResult::Child(ChildOutcome::new(Err::<
                    BucketInfo,
                    CatalogError,
                >(
                    CatalogError::Forbidden
                ))))
                .unwrap(),
            Next::Done(HostRpcResult::Failed(ContractError::Denied { .. }))
        ));
    }

    // Unversioned Stat on a standalone link runs LinkAccess before any folder-child fallback.
    #[test]
    fn prefers_link_stat() {
        use aruna_core::structs::execution::workflow_run::DirectoryPin;

        let link_ver = Ulid::from_parts(2, 1);
        let real_ver = Ulid::from_parts(3, 1);
        let idx_ver = Ulid::from_parts(4, 1);
        let sel = InputSelection {
            source: InputSource::S3 {
                bucket: "other".into(),
                key: "real.txt".into(),
                version_id: Some(real_ver.to_string()),
            },
            source_node_id: Some(node(2)),
            dest_key: "data/sample".into(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        };
        let mut cap = captured_input(&sel, node(2));
        cap.version_id = real_ver;
        let mut run = base_run();
        run.captured_remote.push(PrecapturedInput {
            pin_node_id: node(2),
            pin_bucket: "data".into(),
            pin_key: "sample".into(),
            pin_version_id: link_ver,
            resolved: sel,
            captured: cap,
        });
        run.directory_pins.push(DirectoryPin {
            bucket: "data".into(),
            key: "sample/index".into(),
            version_id: idx_ver,
            source_node_id: Some(node(2)),
        });

        let file_target = Target {
            bucket: "data".into(),
            key: "sample".into(),
            version: None,
            owned: false,
            node: Some(node(2)),
        };
        let mut denied = RemoteReadOperation::new(
            run.clone(),
            file_target.clone(),
            node(2),
            DataCall::Stat {
                address: "s3://data/sample".into(),
            },
        );
        let first = denied.first().unwrap();
        assert!(format!("{first:?}").contains("LinkAccess"));
        assert!(format!("{first:?}").contains("\"sample\""));
        assert!(matches!(
            denied
                .next(refused_meta(BaoReadRefusal::ReadDenied))
                .unwrap(),
            Next::Done(HostRpcResult::Failed(ContractError::Denied { .. }))
        ));

        let mut allowed = RemoteReadOperation::new(
            run.clone(),
            file_target.clone(),
            node(2),
            DataCall::Stat {
                address: "s3://data/sample".into(),
            },
        );
        assert!(matches!(
            allowed.first().unwrap(),
            Next::Call(IoCall::Child(_))
        ));
        let head = allowed.next(ok_auth()).unwrap();
        assert!(format!("{head:?}").contains("real.txt"));
        match allowed.next(ok_meta()).unwrap() {
            Next::Done(HostRpcResult::StorageStat { entry: Some(entry) }) => {
                assert!(!entry.directory);
                assert_eq!(
                    entry.version.as_deref(),
                    Some(link_ver.to_string().as_str())
                );
            }
            other => panic!("expected file stat, got {other:?}"),
        }

        // Trailing-slash Stat and List still resolve the folder children under `sample/`.
        let slash_target = Target {
            bucket: "data".into(),
            key: "sample/".into(),
            version: None,
            owned: false,
            node: Some(node(2)),
        };
        let mut dir_stat = RemoteReadOperation::new(
            run.clone(),
            slash_target,
            node(2),
            DataCall::Stat {
                address: "s3://data/sample/".into(),
            },
        );
        let first = dir_stat.first().unwrap();
        assert!(format!("{first:?}").contains("sample/index"));
        match dir_stat.next(ok_meta()).unwrap() {
            Next::Done(HostRpcResult::StorageStat { entry: Some(entry) }) => {
                assert!(entry.directory);
            }
            other => panic!("expected folder stat, got {other:?}"),
        }

        let mut list = RemoteReadOperation::new(
            run,
            file_target,
            node(2),
            DataCall::List {
                address: "s3://data/sample".into(),
            },
        );
        let first = list.first().unwrap();
        assert!(format!("{first:?}").contains("sample/index"));
        assert_eq!(
            list.next(ok_meta()).unwrap(),
            Next::Done(HostRpcResult::StorageList {
                entries: vec![ListEntry {
                    name: "index".into(),
                    directory: false,
                }],
            })
        );

        // Trailing-slash standalone link without directory entries still runs LinkAccess on Stat.
        let slash_sel = InputSelection {
            source: InputSource::S3 {
                bucket: "other".into(),
                key: "slash_real.txt".into(),
                version_id: Some(real_ver.to_string()),
            },
            source_node_id: Some(node(2)),
            dest_key: "data/slash_link/".into(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        };
        let mut slash_cap = captured_input(&slash_sel, node(2));
        slash_cap.version_id = real_ver;
        let mut slash_run = base_run();
        slash_run.captured_remote.push(PrecapturedInput {
            pin_node_id: node(2),
            pin_bucket: "data".into(),
            pin_key: "slash_link/".into(),
            pin_version_id: link_ver,
            resolved: slash_sel,
            captured: slash_cap,
        });
        let mut slash_denied = RemoteReadOperation::new(
            slash_run,
            Target {
                bucket: "data".into(),
                key: "slash_link/".into(),
                version: None,
                owned: false,
                node: Some(node(2)),
            },
            node(2),
            DataCall::Stat {
                address: "s3://data/slash_link/".into(),
            },
        );
        let first = slash_denied.first().unwrap();
        assert!(format!("{first:?}").contains("LinkAccess"));
        assert!(format!("{first:?}").contains("slash_link/"));
        assert!(matches!(
            slash_denied
                .next(refused_meta(BaoReadRefusal::ReadDenied))
                .unwrap(),
            Next::Done(HostRpcResult::Failed(ContractError::Denied { .. }))
        ));
    }

    // Non-permission holder refusals fail remote directory listings instead of dropping files.
    #[test]
    fn propagates_backend_failure() {
        use aruna_core::structs::execution::workflow_run::DirectoryPin;

        let mut run = base_run();
        run.directory_pins.push(DirectoryPin {
            bucket: "data".into(),
            key: "folder/a.txt".into(),
            version_id: Ulid::from_parts(2, 1),
            source_node_id: Some(node(2)),
        });
        run.directory_pins.push(DirectoryPin {
            bucket: "data".into(),
            key: "folder/b.txt".into(),
            version_id: Ulid::from_parts(2, 2),
            source_node_id: Some(node(2)),
        });
        let dir_target = Target {
            bucket: "data".into(),
            key: "folder/".into(),
            version: None,
            owned: false,
            node: Some(node(2)),
        };

        let mut list = RemoteReadOperation::new(
            run.clone(),
            dir_target.clone(),
            node(2),
            DataCall::List {
                address: "s3://data/folder/".into(),
            },
        );
        assert!(matches!(
            list.first().unwrap(),
            Next::Call(IoCall::Child(_))
        ));
        assert!(matches!(
            list.next(refused_meta(BaoReadRefusal::BackendFailure))
                .unwrap(),
            Next::Done(HostRpcResult::Failed(ContractError::Internal { .. }))
        ));

        let mut locked = RemoteReadOperation::new(
            run.clone(),
            dir_target.clone(),
            node(2),
            DataCall::List {
                address: "s3://data/folder/".into(),
            },
        );
        assert!(matches!(
            locked.first().unwrap(),
            Next::Call(IoCall::Child(_))
        ));
        assert!(matches!(
            locked
                .next(refused_meta(BaoReadRefusal::BucketLocked(
                    Ulid::from_parts(9, 1)
                )))
                .unwrap(),
            Next::Done(HostRpcResult::Failed(ContractError::Denied { .. }))
        ));

        let mut stat = RemoteReadOperation::new(
            run,
            dir_target,
            node(2),
            DataCall::Stat {
                address: "s3://data/folder/".into(),
            },
        );
        assert!(matches!(
            stat.first().unwrap(),
            Next::Call(IoCall::Child(_))
        ));
        assert!(matches!(
            stat.next(refused_meta(BaoReadRefusal::BackendFailure))
                .unwrap(),
            Next::Done(HostRpcResult::Failed(ContractError::Internal { .. }))
        ));

        assert!(matches!(
            read_answer(refused_meta(BaoReadRefusal::BackendFailure)).unwrap(),
            Err(ContractError::Internal { .. })
        ));
        assert!(matches!(
            read_answer(refused_meta(BaoReadRefusal::NotFound)).unwrap(),
            Err(ContractError::NotFound { .. })
        ));
        assert!(matches!(
            read_answer(refused_meta(BaoReadRefusal::InvalidTarget)).unwrap(),
            Err(ContractError::Invalid { .. })
        ));
        assert!(matches!(
            read_answer(refused_meta(BaoReadRefusal::ReadDenied)).unwrap(),
            Err(ContractError::Denied { .. })
        ));
    }

    // Stat on a precaptured remote directory marker routes through folder auth and reports a dir.
    #[test]
    fn stats_captured_marker() {
        use aruna_core::structs::execution::workflow_run::DirectoryRoot;

        let marker_ver = Ulid::from_parts(6, 1);
        let sel = InputSelection {
            source: InputSource::S3 {
                bucket: "data".into(),
                key: "empty_dir/".into(),
                version_id: Some(marker_ver.to_string()),
            },
            source_node_id: Some(node(2)),
            dest_key: "data/empty_dir/".into(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        };
        let mut cap = captured_input(&sel, node(2));
        cap.version_id = marker_ver;
        let mut run = base_run();
        run.directory_roots.push(DirectoryRoot {
            bucket: "data".into(),
            prefix: "empty_dir/".into(),
            version_id: Some(marker_ver),
            source_node_id: Some(node(2)),
        });
        run.captured_remote.push(PrecapturedInput {
            pin_node_id: node(2),
            pin_bucket: "data".into(),
            pin_key: "empty_dir/".into(),
            pin_version_id: marker_ver,
            resolved: sel,
            captured: cap,
        });

        for key in ["empty_dir/", "empty_dir"] {
            let dir_target = Target {
                bucket: "data".into(),
                key: key.into(),
                version: None,
                owned: false,
                node: Some(node(2)),
            };
            let mut stat = RemoteReadOperation::new(
                run.clone(),
                dir_target.clone(),
                node(2),
                DataCall::Stat {
                    address: format!("s3://data/{key}"),
                },
            );
            assert!(matches!(
                stat.first().unwrap(),
                Next::Call(IoCall::Child(_))
            ));
            match stat.next(ok_meta()).unwrap() {
                Next::Done(HostRpcResult::StorageStat { entry: Some(entry) }) => {
                    assert!(entry.directory, "{key}: {entry:?}");
                }
                other => panic!("{key}: expected folder stat, got {other:?}"),
            }

            let mut denied = RemoteReadOperation::new(
                run.clone(),
                dir_target,
                node(2),
                DataCall::Stat {
                    address: format!("s3://data/{key}"),
                },
            );
            assert!(matches!(
                denied.first().unwrap(),
                Next::Call(IoCall::Child(_))
            ));
            assert_eq!(
                denied
                    .next(refused_meta(BaoReadRefusal::ReadDenied))
                    .unwrap(),
                Next::Done(HostRpcResult::StorageStat { entry: None })
            );
        }

        let mut list = RemoteReadOperation::new(
            run,
            Target {
                bucket: "data".into(),
                key: "empty_dir/".into(),
                version: None,
                owned: false,
                node: Some(node(2)),
            },
            node(2),
            DataCall::List {
                address: "s3://data/empty_dir/".into(),
            },
        );
        assert!(matches!(
            list.first().unwrap(),
            Next::Call(IoCall::Child(_))
        ));
        assert_eq!(
            list.next(ok_meta()).unwrap(),
            Next::Done(HostRpcResult::StorageList {
                entries: Vec::new(),
            })
        );
    }
}
