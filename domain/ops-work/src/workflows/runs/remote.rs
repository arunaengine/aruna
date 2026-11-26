//! Reads of a run's objects that another node holds, like task outputs in its run area there:
//! a stat or a chunk of the exact version, read through the holder, which authorizes the creator.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::ops::Range;

use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::effects::Effect;
use aruna_core::events::Event;
use aruna_core::id::NodeId;
use aruna_core::jobs::{JobEffect, JobEvent};
use aruna_core::operation::Operation;
use aruna_core::replication_protocol::{BaoReadRequest, BaoReadTarget};
use aruna_core::structs::checksum::HASH_SHA256;
use aruna_core::structs::execution::job::input::InputSource;
use aruna_core::structs::execution::workflow_run::WorkflowRunRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_ops_data::replication::bao_read::{BaoReadError, BaoReadOutput};
use aruna_ops_data::replication::managed_read::ManagedReadOperation;
use aruna_workflow_contract::host::{ListEntry, StorageEntry};
use aruna_workflow_contract::{ContractError, DATA_CHUNK_BYTES, HostRpcResult};
use ulid::Ulid;

use super::creator_auth;
use super::data::{DataCall, Target};
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

/// Rewrites a precaptured link or captured directory child to its resolved versioned object so
/// coordinator reads work both locally and after cross-node resume.
pub fn resolve_precaptured(run: &WorkflowRunRecord, target: Target, local: NodeId) -> Target {
    let mut target = pin_directory_child(run, target, local);
    let holder = target.node.unwrap_or(local);
    if target.version.is_none()
        && let Some(entry) = run.captured_remote.iter().find(|entry| {
            entry.pin_node_id == holder
                && entry.pin_bucket == target.bucket
                && entry.pin_key == target.key
        })
    {
        target.version = Some(entry.pin_version_id);
    }
    let Some(version) = target.version else {
        return target;
    };
    let Some(entry) = run.captured_remote.iter().find(|entry| {
        entry.pin_node_id == holder
            && entry.pin_bucket == target.bucket
            && entry.pin_key == target.key
            && entry.pin_version_id == version
    }) else {
        return target;
    };
    if let InputSource::S3 {
        bucket,
        key,
        version_id,
    } = &entry.resolved.source
        && let Some(resolved_ver) = version_id
            .as_deref()
            .and_then(|v| Ulid::from_string(v).ok())
    {
        target.bucket = bucket.clone();
        target.key = key.clone();
        target.version = Some(resolved_ver);
        target.node = entry.resolved.source_node_id.or(Some(holder));
        target.owned = false;
    }
    target
}

/// Immediate directory entries captured for `(holder, bucket, key)` from admitted directory pins,
/// keeping directory membership fixed across local and cross-node resumes.
pub(super) fn captured_entries(
    run: &WorkflowRunRecord,
    local: NodeId,
    holder: NodeId,
    bucket: &str,
    key: &str,
) -> Vec<ListEntry> {
    let prefix = match key.is_empty() || key.ends_with('/') {
        true => key.to_owned(),
        false => format!("{key}/"),
    };
    let mut entries: BTreeMap<String, bool> = BTreeMap::new();
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
        match rest.split_once('/') {
            Some((dir, _)) if !dir.is_empty() => {
                entries.insert(dir.to_owned(), true);
            }
            None => {
                entries.entry(rest.to_owned()).or_insert(false);
            }
            _ => {}
        }
    }
    entries
        .into_iter()
        .map(|(name, directory)| ListEntry { name, directory })
        .collect()
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

/// The answer of a [`read_call`], or the refusal the coordinator reads.
pub fn read_answer(result: IoResult) -> Result<Result<BaoReadOutput, ContractError>, CatalogError> {
    Ok(match result.child::<BaoReadOutput, BaoReadError>()? {
        Ok(output) => Ok(output),
        Err(BaoReadError::Refused(refusal)) => Err(ContractError::Denied {
            message: format!("the holder refused the read: {refusal:?}"),
        }),
        Err(error) => Err(ContractError::internal(error.to_string())),
    })
}

#[derive(Debug, PartialEq)]
enum RemoteStep {
    Head,
    Read,
    Collect,
}

/// Answers a stat or a chunk read of an object another node holds; a listing there is refused.
#[derive(Debug, PartialEq)]
pub struct RemoteReadOperation {
    run: WorkflowRunRecord,
    target: Target,
    node: NodeId,
    call: DataCall,
    size: u64,
    step: RemoteStep,
}

type RemoteNext = Result<Next<HostRpcResult>, CatalogError>;

impl RemoteReadOperation {
    pub fn new(run: WorkflowRunRecord, target: Target, node: NodeId, call: DataCall) -> Self {
        let target = resolve_precaptured(&run, target, node);
        let node = target.node.unwrap_or(node);
        Self {
            run,
            target,
            node,
            call,
            size: 0,
            step: RemoteStep::Head,
        }
    }

    fn failed(error: ContractError) -> RemoteNext {
        Ok(Next::Done(HostRpcResult::Failed(error)))
    }

    fn version(&self) -> Option<String> {
        self.target.version.map(|version| version.to_string())
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
}

impl Operation for RemoteReadOperation {
    type Output = HostRpcResult;
    type Error = CatalogError;

    fn first(&mut self) -> RemoteNext {
        if matches!(self.call, DataCall::List { .. }) {
            let entries = captured_entries(
                &self.run,
                self.run.node_id,
                self.node,
                &self.target.bucket,
                &self.target.key,
            );
            if !entries.is_empty() {
                return Ok(Next::Done(HostRpcResult::StorageList { entries }));
            }
            return Self::failed(ContractError::unsupported(format!(
                "a folder on node {} is not listed from here",
                self.node
            )));
        }
        if matches!(self.call, DataCall::Stat { .. }) && self.target.version.is_none() {
            let entries = captured_entries(
                &self.run,
                self.run.node_id,
                self.node,
                &self.target.bucket,
                &self.target.key,
            );
            if !entries.is_empty() {
                return Ok(Next::Done(HostRpcResult::StorageStat {
                    entry: Some(StorageEntry {
                        directory: true,
                        size: 0,
                        modified_ms: None,
                        version: None,
                        sha256: None,
                        aruna_id: None,
                        endpoints: Vec::new(),
                    }),
                }));
            }
        }
        let Some(head) = read_call(&self.run, &self.target, self.node, (true, None)) else {
            return Self::failed(ContractError::invalid(
                "an object on another node is read by its exact version",
            ));
        };
        self.step = RemoteStep::Head;
        Ok(Next::Call(head))
    }

    fn next(&mut self, result: IoResult) -> RemoteNext {
        match self.step {
            RemoteStep::Head => match read_answer(result)? {
                Ok(BaoReadOutput::Metadata { size, hashes, .. })
                | Ok(BaoReadOutput::Stream { size, hashes, .. }) => self.headed(size, &hashes),
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
                Ok(BaoReadOutput::Metadata { .. }) => Err(IoError::Unfinished.into()),
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
            used_tools: Vec::new(),
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
            used_tools: Vec::new(),
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
    }
}
