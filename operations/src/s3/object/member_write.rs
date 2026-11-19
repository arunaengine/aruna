//! Decides which member a write goes to: the coordinator of its key's range, unless this node
//! is that member or the request already arrived with a delegation (D33).
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{HashMap, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::member_read::{MemberBody, MemberMethod};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::placement::policy::PlacementPolicyRef;
use aruna_core::structs::storage::blob::CONTENT_TYPE_KEY;
use aruna_core::structs::storage::distribution::delegation::{COPY_CONTEXT_HEADER, CopyContext};
use aruna_core::structs::storage::distribution::{
    DistributionBinding, DistributionView, ReadRoute, RouteError, WriteRoute,
};
use thiserror::Error;
use ulid::Ulid;

use crate::distribution::guard::GuardError;
use crate::distribution::routing::{
    bound_view, read_coordinator, route_read, route_upload, route_write,
};
use crate::s3::object::delete::bulk::BulkDeleteEntry;
use crate::s3::object::member_read::{
    Header, MemberCallOperation, MemberFailure, MemberPayload, MemberRefusal, MemberRequest,
    MemberTarget,
};
use crate::s3::object::range_list::DelegatedCaller;

/// The copy conditions of a source as the holder's own read sees them.
const COPY_CONDITIONS: &[(&str, &str)] = &[
    ("x-amz-copy-source-if-match", "if-match"),
    ("x-amz-copy-source-if-none-match", "if-none-match"),
    ("x-amz-copy-source-if-modified-since", "if-modified-since"),
    (
        "x-amz-copy-source-if-unmodified-since",
        "if-unmodified-since",
    ),
    ("x-amz-copy-source-range", "range"),
];

/// The coordinator a write to `key` goes to, or `None` when this node writes it. A range fence
/// in its upload grace period keeps a request on an open upload here.
#[derive(Debug, PartialEq)]
pub struct WriteTargetOperation {
    binding: DistributionBinding,
    key: String,
    upload: bool,
    delegated: bool,
}

impl WriteTargetOperation {
    pub fn new(binding: &DistributionBinding, key: &str, upload: bool, delegated: bool) -> Self {
        Self {
            binding: binding.clone(),
            key: key.to_string(),
            upload,
            delegated,
        }
    }
}

impl Operation for WriteTargetOperation {
    type Output = Option<MemberTarget>;
    type Error = GuardError;

    fn first(&mut self) -> Result<Next<Self::Output>, GuardError> {
        if self.delegated {
            return Ok(Next::Done(None));
        }
        let (binding, key) = (&self.binding, self.key.as_str());
        Ok(Next::Call(match self.upload {
            true => IoCall::child(route_upload(binding, key)),
            false => IoCall::child(route_write(binding, key)),
        }))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Self::Output>, GuardError> {
        Ok(Next::Done(
            match result.child::<WriteRoute, GuardError>()?? {
                WriteRoute::Local { .. } => None,
                WriteRoute::Forward {
                    range_id, node_id, ..
                } => Some(MemberTarget {
                    set_id: self.binding.set_id,
                    range_id,
                    node_id,
                }),
            },
        ))
    }
}

/// What a bulk delete leaves for this node, and what other members answered.
#[derive(Debug, PartialEq)]
pub struct ForwardedDeletes {
    pub local: Vec<BulkDeleteEntry>,
    pub answers: Vec<(String, Result<MemberBody, MemberFailure>)>,
}

#[derive(Debug, PartialEq)]
enum DeleteStep {
    Target(BulkDeleteEntry),
    Sent(String),
}

/// Forwards each delete marker whose key another member coordinates, one delegated request per
/// key, and keeps the other entries for this node.
#[derive(Debug, PartialEq)]
pub struct ForwardDeletesOperation {
    ids: Option<(NodeId, RealmId)>,
    caller: DelegatedCaller,
    binding: Option<DistributionBinding>,
    bucket: String,
    delegated: bool,
    entries: VecDeque<BulkDeleteEntry>,
    done: ForwardedDeletes,
    step: Option<DeleteStep>,
}

type DeleteTurn = Result<Next<ForwardedDeletes>, IoError>;

impl ForwardDeletesOperation {
    pub fn new(
        ids: Option<(NodeId, RealmId)>,
        caller: DelegatedCaller,
        (bucket, binding, delegated): (&str, Option<&DistributionBinding>, bool),
        entries: Vec<BulkDeleteEntry>,
    ) -> Self {
        Self {
            ids,
            caller,
            binding: binding.cloned(),
            bucket: bucket.to_string(),
            delegated,
            entries: entries.into(),
            done: ForwardedDeletes {
                local: Vec::new(),
                answers: Vec::new(),
            },
            step: None,
        }
    }

    fn advance(&mut self) -> DeleteTurn {
        while let Some(entry) = self.entries.pop_front() {
            let binding = self.binding.as_ref().filter(|_| entry.version_id.is_none());
            let Some(binding) = binding else {
                self.done.local.push(entry);
                continue;
            };
            let target = WriteTargetOperation::new(binding, &entry.key, false, self.delegated);
            self.step = Some(DeleteStep::Target(entry));
            return Ok(Next::Call(IoCall::child(target)));
        }
        let done = ForwardedDeletes {
            local: std::mem::take(&mut self.done.local),
            answers: std::mem::take(&mut self.done.answers),
        };
        Ok(Next::Done(done))
    }
}

impl Operation for ForwardDeletesOperation {
    type Output = ForwardedDeletes;
    type Error = IoError;

    fn first(&mut self) -> DeleteTurn {
        self.advance()
    }

    fn next(&mut self, result: IoResult) -> DeleteTurn {
        match self.step.take().ok_or(IoError::Unfinished)? {
            DeleteStep::Target(entry) => {
                match result.child::<Option<MemberTarget>, GuardError>()? {
                    Ok(None) => self.done.local.push(entry),
                    Ok(Some(target)) => {
                        let request = MemberRequest {
                            method: MemberMethod::Delete,
                            operation: "DeleteObject",
                            bucket: self.bucket.clone(),
                            key: entry.key.clone(),
                            query: None,
                            headers: Vec::new(),
                            body: MemberPayload::Empty,
                        };
                        let caller = self.caller.clone();
                        let call = MemberCallOperation::write(self.ids, caller, target, request);
                        self.step = Some(DeleteStep::Sent(entry.key));
                        return Ok(Next::Call(IoCall::child(call)));
                    }
                    Err(error) => {
                        let failure = MemberRefusal::Route(error).into();
                        self.done.answers.push((entry.key, Err(failure)));
                    }
                }
            }
            DeleteStep::Sent(key) => {
                let answer = result.child::<MemberBody, MemberRefusal>()?;
                self.done.answers.push((key, answer.map_err(Into::into)));
            }
        }
        self.advance()
    }
}

/// How a copy reaches the coordinator of its destination key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CopyPath {
    /// This member keeps the source and streams it as a forwarded write.
    Stream,
    /// The coordinator keeps the source's records and copies itself.
    Delegate,
    /// Only another member keeps the source: this member reads it through that holder.
    Relay(MemberTarget),
}

#[derive(Debug, Error, PartialEq)]
pub enum CopyPathError {
    #[error(transparent)]
    Route(#[from] GuardError),
    /// Neither this member nor the destination's coordinator keeps the source.
    #[error("the copy source is kept elsewhere in distributed bucket {set_id}")]
    Elsewhere { set_id: Ulid, holders: Vec<NodeId> },
}

impl From<IoError> for CopyPathError {
    fn from(error: IoError) -> Self {
        Self::Route(error.into())
    }
}

#[derive(Debug, PartialEq)]
enum CopyStep {
    Coordinator,
    Route,
    View,
}

/// Decides how a copy reaches its source. With `dest` the destination's coordinator is another
/// member; without it this node writes the copy and must keep the source itself.
#[derive(Debug, PartialEq)]
pub struct CopyPathOperation {
    binding: DistributionBinding,
    key: String,
    pinned: bool,
    local: NodeId,
    dest: Option<NodeId>,
    step: CopyStep,
}

type CopyTurn = Result<Next<CopyPath>, CopyPathError>;

impl CopyPathOperation {
    pub fn new(
        binding: &DistributionBinding,
        (key, pinned): (&str, bool),
        local: NodeId,
        dest: Option<NodeId>,
    ) -> Self {
        Self {
            binding: binding.clone(),
            key: key.to_string(),
            pinned,
            local,
            dest,
            step: CopyStep::Coordinator,
        }
    }

    fn holders_known(&mut self) -> CopyTurn {
        self.step = CopyStep::View;
        Ok(Next::Call(IoCall::child(bound_view(&self.binding))))
    }

    /// The range of the source and the nodes that may answer its read: only the coordinator
    /// for the current version, since a copy may lag behind it.
    fn decide(&self, view: &DistributionView) -> Result<CopyPath, CopyPathError> {
        let set_id = self.binding.set_id;
        let snapshot = view.snapshot();
        let range = (snapshot.range_for(&self.key))
            .ok_or_else(|| GuardError::from(RouteError::NoRange(set_id)))?;
        let holders = match self.pinned {
            true => snapshot.record_holders(range),
            false => snapshot.list_holder(range).into_iter().collect(),
        };
        let holders: Vec<NodeId> = holders.into_iter().map(|member| member.node_id).collect();
        let elsewhere = |holders: Vec<NodeId>| CopyPathError::Elsewhere { set_id, holders };
        let Some(dest) = self.dest else {
            return Err(elsewhere(holders));
        };
        if holders.contains(&dest) {
            return Ok(CopyPath::Delegate);
        }
        match holders.iter().find(|node_id| **node_id != self.local) {
            Some(node_id) => Ok(CopyPath::Relay(MemberTarget {
                set_id,
                range_id: range.range_id,
                node_id: *node_id,
            })),
            None => Err(elsewhere(holders)),
        }
    }
}

impl Operation for CopyPathOperation {
    type Output = CopyPath;
    type Error = CopyPathError;

    fn first(&mut self) -> CopyTurn {
        let (binding, key) = (&self.binding, self.key.as_str());
        if self.pinned {
            self.step = CopyStep::Route;
            return Ok(Next::Call(IoCall::child(route_read(binding, key, false))));
        }
        Ok(Next::Call(IoCall::child(read_coordinator(binding, key))))
    }

    fn next(&mut self, result: IoResult) -> CopyTurn {
        match self.step {
            CopyStep::Coordinator => {
                let coordinator = result.child::<Option<(Ulid, NodeId)>, GuardError>()??;
                match coordinator {
                    None => Ok(Next::Done(CopyPath::Stream)),
                    Some(_) => self.holders_known(),
                }
            }
            CopyStep::Route => match result.child::<ReadRoute, GuardError>()?? {
                ReadRoute::Local { catching_up: false } => Ok(Next::Done(CopyPath::Stream)),
                ReadRoute::Local { .. } | ReadRoute::Holder { .. } => self.holders_known(),
            },
            CopyStep::View => {
                let view = result.child::<DistributionView, GuardError>()??;
                Ok(Next::Done(self.decide(&view)?))
            }
        }
    }
}

/// The holder's read of a copy source; `headers` are those of the copy request.
pub fn source_read(
    (bucket, key, version): (&str, &str, Option<Ulid>),
    headers: &[Header],
) -> MemberRequest {
    let value = |name: &str| {
        (headers.iter())
            .find(|(header, _)| header == name)
            .map(|(_, value)| value.clone())
    };
    let headers = (COPY_CONDITIONS.iter())
        .filter_map(|(from, to)| Some((to.to_string(), value(from)?)))
        .collect();
    MemberRequest {
        method: MemberMethod::Get,
        operation: "GetObject",
        bucket: bucket.to_string(),
        key: key.to_string(),
        query: version.map(|version| format!("versionId={version}")),
        headers,
        body: MemberPayload::Empty,
    }
}

/// The metadata a holder answered with, as a copy keeps it.
pub fn answered_metadata(body: &MemberBody) -> HashMap<String, String> {
    let mut metadata: HashMap<String, String> = (body.headers.iter())
        .filter_map(|(name, value)| {
            Some((name.strip_prefix("x-amz-meta-")?.to_string(), value.clone()))
        })
        .collect();
    if let Some(content_type) = body.header("content-type") {
        metadata.insert(CONTENT_TYPE_KEY.to_string(), content_type.to_string());
    }
    metadata
}

/// The placement refs a holder answered a copy source read with; `None` when unreadable.
pub fn answered_policies(body: &MemberBody) -> Option<Vec<PlacementPolicyRef>> {
    match body.header(COPY_CONTEXT_HEADER) {
        Some(value) => CopyContext::decode(value)
            .ok()
            .map(|context| context.policies),
        None => Some(Vec::new()),
    }
}

/// The headers that store a streamed copy with the metadata it keeps.
pub fn copy_headers(metadata: &HashMap<String, String>, length: u64) -> Vec<Header> {
    let mut headers = vec![("content-length".to_string(), length.to_string())];
    for (name, value) in metadata {
        match name.as_str() {
            CONTENT_TYPE_KEY => headers.push(("content-type".to_string(), value.clone())),
            // Internal tags of the source stay with the source.
            name if name.starts_with("aruna") => {}
            name => headers.push((format!("x-amz-meta-{name}"), value.clone())),
        }
    }
    headers
}

#[cfg(test)]
mod tests {
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::storage::distribution::DistributionRevision;

    use super::*;

    fn node(seed: u8) -> NodeId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn binding() -> DistributionBinding {
        DistributionBinding {
            set_id: Ulid::from_bytes([3; 16]),
            member_id: Ulid::from_bytes([4; 16]),
            incarnation: 0,
            revision: DistributionRevision::default(),
        }
    }

    fn child<T, E>(answer: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(answer))
    }

    fn caller() -> DelegatedCaller {
        DelegatedCaller {
            user_id: aruna_core::UserId::nil(RealmId::from_bytes([1; 32])),
            group_id: Ulid::from_bytes([2; 16]),
            credential_id: String::new(),
            restrictions: None,
            credential_expiry: 0,
        }
    }

    fn entry(key: &str, version_id: Option<Ulid>) -> BulkDeleteEntry {
        BulkDeleteEntry {
            key: key.to_string(),
            version_id,
        }
    }

    #[test]
    fn delegated_write_stays() {
        let mut target = WriteTargetOperation::new(&binding(), "a", false, true);
        assert_eq!(target.first(), Ok(Next::Done(None)));
    }

    #[test]
    fn forward_route_targets() {
        let mut target = WriteTargetOperation::new(&binding(), "a", true, false);
        assert!(matches!(target.first(), Ok(Next::Call(IoCall::Child(_)))));
        let range_id = Ulid::from_bytes([5; 16]);
        let route = WriteRoute::Forward {
            range_id,
            node_id: node(2),
            coordinator: Ulid::from_bytes([6; 16]),
        };
        let expected = MemberTarget {
            set_id: binding().set_id,
            range_id,
            node_id: node(2),
        };
        let answer = target.next(child::<_, GuardError>(Ok(route)));
        assert_eq!(answer, Ok(Next::Done(Some(expected))));
    }

    #[test]
    fn versioned_deletes_stay() {
        let version = Some(Ulid::from_bytes([9; 16]));
        let entries = vec![entry("a", version)];
        let held = binding();
        let mut deletes =
            ForwardDeletesOperation::new(None, caller(), ("b", Some(&held), false), entries);
        let Ok(Next::Done(done)) = deletes.first() else {
            panic!("nothing is forwarded");
        };
        assert_eq!(done.local, [entry("a", version)]);
    }

    #[test]
    fn route_failure_reported() {
        let held = binding();
        let entries = vec![entry("a", None)];
        let mut deletes =
            ForwardDeletesOperation::new(None, caller(), ("b", Some(&held), false), entries);
        deletes.first().unwrap();
        let refused = GuardError::Unbound(held.set_id);
        let Ok(Next::Done(done)) = deletes.next(child::<Option<MemberTarget>, _>(Err(refused)))
        else {
            panic!("the delete ends");
        };
        assert_eq!(done.answers.len(), 1);
        assert!(done.answers[0].1.is_err());
    }

    #[test]
    fn own_coordinator_streams() {
        let mut path = CopyPathOperation::new(&binding(), ("a", false), node(1), Some(node(2)));
        path.first().unwrap();
        let answer = path.next(child::<Option<(Ulid, NodeId)>, GuardError>(Ok(None)));
        assert_eq!(answer, Ok(Next::Done(CopyPath::Stream)));
    }

    #[test]
    fn read_keeps_conditions() {
        let headers = vec![
            (
                "x-amz-copy-source-if-match".to_string(),
                "\"abc\"".to_string(),
            ),
            (
                "x-amz-copy-source-range".to_string(),
                "bytes=0-9".to_string(),
            ),
            ("content-type".to_string(), "text/plain".to_string()),
        ];
        let version = Ulid::from_bytes([5; 16]);
        let read = source_read(("spire", "a/b", Some(version)), &headers);
        assert_eq!(read.operation, "GetObject");
        assert_eq!(read.query, Some(format!("versionId={version}")));
        assert_eq!(
            read.headers,
            vec![
                ("if-match".to_string(), "\"abc\"".to_string()),
                ("range".to_string(), "bytes=0-9".to_string()),
            ]
        );
    }

    #[test]
    fn copy_keeps_metadata() {
        let metadata = HashMap::from([
            (CONTENT_TYPE_KEY.to_string(), "text/csv".to_string()),
            ("owner".to_string(), "lab".to_string()),
            ("aruna.sync.version".to_string(), "x".to_string()),
        ]);
        let mut headers = copy_headers(&metadata, 12);
        headers.sort();
        assert_eq!(
            headers,
            vec![
                ("content-length".to_string(), "12".to_string()),
                ("content-type".to_string(), "text/csv".to_string()),
                ("x-amz-meta-owner".to_string(), "lab".to_string()),
            ]
        );
    }
}
