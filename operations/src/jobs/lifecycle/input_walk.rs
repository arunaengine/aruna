//! Walks the inputs of one submission to the objects they end at: links, directories and the
//! range coordinator of a distributed bucket.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use aruna_core::UserId;
use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::id::NodeId;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::{InputSelection, InputSource};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_core::structs::storage::link::LinkTarget;
use aruna_core::structs::storage::read_grant::READ_OPERATION;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::GroupId;
use ulid::Ulid;

use super::submission_rules::{input_reference, source_error};
use crate::distribution::guard::GuardError;
use crate::distribution::route_query::coordinator_read;
use crate::jobs::read_access::{SourceObject, SourceReadError, SourceReadOperation};
use crate::jobs::submit::SubmitJobError;
use crate::s3::bucket::get::{GetBucketError, GetBucketOperation};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::s3::object::list::{
    ListBucketError, ListBucketInput, ListBucketOperation, ListBucketResult, ListContinuationToken,
};

/// Who walks the inputs, in the realm this node serves, and where.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct InputCaller {
    pub(super) created_by: UserId,
    pub(super) realm_id: RealmId,
    pub(super) local: NodeId,
}

impl InputCaller {
    pub(super) fn auth(&self) -> AuthContext {
        AuthContext {
            user_id: self.created_by,
            realm_id: self.realm_id,
            path_restrictions: None,
            session: None,
        }
    }

    /// The check that the caller may read `key` like an S3 GET of it.
    pub(super) fn read_check(&self, bucket: &str, key: &str) -> SourceReadOperation {
        let object = SourceObject {
            node_id: self.local,
            bucket: bucket.to_string(),
            key: key.to_string(),
            operation: READ_OPERATION.to_string(),
        };
        SourceReadOperation::new(self.auth(), object)
    }
}

pub(super) fn source_parts(input: &InputSelection) -> (&str, &str, Option<&str>) {
    let InputSource::S3 {
        bucket,
        key,
        version_id,
    } = &input.source;
    (bucket, key, version_id.as_deref())
}

pub(super) fn reference_of(input: &InputSelection) -> String {
    let (bucket, key, version_id) = source_parts(input);
    input_reference(bucket, key, version_id)
}

/// A link met on the walk, with the target it names.
#[derive(Debug, PartialEq)]
struct LinkStep {
    link: VersionedObjectArn,
    target: VersionedObjectArn,
}

/// Follows links an input names on this node to the version they end at, which the caller must
/// be able to read like any input. Answers the input of the target and the first link. A link
/// to a link is followed; a cycle is refused; a target elsewhere ends the walk here.
#[derive(Debug, PartialEq)]
pub(super) struct FollowLinkOperation {
    caller: InputCaller,
    input: Option<InputSelection>,
    first: Option<VersionedObjectArn>,
    seen: BTreeSet<String>,
    step: Option<LinkStep>,
}

type Followed = (InputSelection, Option<VersionedObjectArn>);
type FollowNext = Result<Next<Followed>, SubmitJobError>;

impl FollowLinkOperation {
    pub(super) fn new(caller: InputCaller, input: InputSelection) -> Self {
        Self {
            caller,
            input: Some(input),
            first: None,
            seen: BTreeSet::new(),
            step: None,
        }
    }

    fn walked(&self) -> Result<&InputSelection, SubmitJobError> {
        let lost = || SubmitJobError::PlacementUnavailable("the input was lost".to_string());
        self.input.as_ref().ok_or_else(lost)
    }

    fn finish(&mut self) -> FollowNext {
        let lost = || SubmitJobError::PlacementUnavailable("the input was lost".to_string());
        let input = self.input.take().ok_or_else(lost)?;
        Ok(Next::Done((input, self.first.take())))
    }

    fn head(&mut self) -> FollowNext {
        let input = self.walked()?;
        if input
            .source_node_id
            .is_some_and(|node| node != self.caller.local)
        {
            return self.finish();
        }
        let (bucket, key, version_id) = source_parts(input);
        let version_id = version_id.and_then(|version| Ulid::from_string(version).ok());
        Ok(Next::Call(IoCall::child(HeadObjectOperation::new(
            HeadObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id,
            },
        ))))
    }

    fn headed(&mut self, head: HeadObjectResult) -> FollowNext {
        let realm_id = self.caller.realm_id;
        let binding = head.source_binding.as_ref();
        let Some(target) = binding.and_then(|binding| LinkTarget::of(binding, realm_id)) else {
            return self.finish();
        };
        let (bucket, key, _) = source_parts(self.walked()?);
        let (bucket, key) = (bucket.to_string(), key.to_string());
        // The link itself must be readable by the caller before its target is.
        let check = self.caller.read_check(&bucket, &key);
        let version = head
            .resolved_version_id
            .or(head.version_id)
            .unwrap_or(target.arn.version);
        self.step = Some(LinkStep {
            link: VersionedObjectArn {
                realm_id,
                node_id: self.caller.local,
                bucket,
                key,
                version,
            },
            target: target.arn,
        });
        Ok(Next::Call(IoCall::child(check)))
    }

    fn follow(&mut self, step: LinkStep) -> FollowNext {
        let reference = reference_of(self.walked()?);
        let LinkStep { link, target } = step;
        if !self.seen.insert(link.to_w3id()) {
            return Err(SubmitJobError::InvalidWorkspace(format!(
                "{reference}: the links form a cycle"
            )));
        }
        self.first.get_or_insert(link);
        let local = self.caller.local;
        if let Some(input) = self.input.as_mut() {
            input.source_node_id = (target.node_id != local).then_some(target.node_id);
            input.source = InputSource::S3 {
                version_id: Some(target.version.to_string()),
                bucket: target.bucket,
                key: target.key,
            };
        }
        self.head()
    }
}

impl Operation for FollowLinkOperation {
    type Output = Followed;
    type Error = SubmitJobError;

    fn first(&mut self) -> FollowNext {
        self.head()
    }

    fn next(&mut self, result: IoResult) -> FollowNext {
        let Some(step) = self.step.take() else {
            return match result.child::<HeadObjectResult, HeadObjectError>()? {
                Ok(head) => self.headed(head),
                Err(_) => self.finish(),
            };
        };
        match result.child::<GroupId, SourceReadError>()? {
            Ok(_) => self.follow(step),
            Err(error) => Err(source_error(&reference_of(self.walked()?), error)),
        }
    }
}

#[derive(Debug, PartialEq)]
enum DirectoryStep {
    Bucket,
    List,
    Read { key: String, version_id: Ulid },
}

/// One pinned input per object below a directory input's prefix, at the current version, each
/// below the directory's container path. An object the caller may not read refuses the whole
/// directory without naming it; a distributed bucket's prefix spans other members' ranges.
#[derive(Debug, PartialEq)]
pub(super) struct ExpandDirectoryOperation {
    caller: InputCaller,
    input: InputSelection,
    bucket: String,
    prefix: String,
    group_id: Option<GroupId>,
    token: Option<ListContinuationToken>,
    objects: VecDeque<(String, Ulid)>,
    files: Vec<InputSelection>,
    step: DirectoryStep,
}

type ExpandNext = Result<Next<Vec<InputSelection>>, SubmitJobError>;

impl ExpandDirectoryOperation {
    pub(super) fn new(caller: InputCaller, input: InputSelection) -> Self {
        let (bucket, prefix, _) = source_parts(&input);
        let (bucket, prefix) = (bucket.to_string(), prefix.to_string());
        Self {
            caller,
            input,
            bucket,
            prefix,
            group_id: None,
            token: None,
            objects: VecDeque::new(),
            files: Vec::new(),
            step: DirectoryStep::Bucket,
        }
    }

    fn reference(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }

    fn list(&mut self, group_id: GroupId) -> ExpandNext {
        self.group_id = Some(group_id);
        self.step = DirectoryStep::List;
        Ok(Next::Call(IoCall::child(ListBucketOperation::new(
            ListBucketInput {
                bucket: self.bucket.clone(),
                group_id,
                continuation_token: self.token.take(),
                max_keys: Some(ListBucketOperation::DEFAULT_MAX_KEYS),
                prefix: Some(self.prefix.clone()),
                delimiter: None,
                start_after: None,
            },
        ))))
    }

    /// Checks the next listed object, then the next page, then ends the walk.
    fn advance(&mut self) -> ExpandNext {
        while let Some((key, version_id)) = self.objects.pop_front() {
            let relative = &key[self.prefix.len()..];
            if relative.is_empty() || relative.ends_with('/') {
                continue;
            }
            let check = self.caller.read_check(&self.bucket, &key);
            self.step = DirectoryStep::Read { key, version_id };
            return Ok(Next::Call(IoCall::child(check)));
        }
        if let (Some(_), Some(group_id)) = (&self.token, self.group_id) {
            return self.list(group_id);
        }
        if self.files.is_empty() {
            return Err(SubmitJobError::InvalidWorkspace(format!(
                "{}: the directory input holds no objects",
                self.reference()
            )));
        }
        Ok(Next::Done(std::mem::take(&mut self.files)))
    }

    fn file(&self, key: &str, version_id: Ulid) -> InputSelection {
        let relative = &key[self.prefix.len()..];
        let input = &self.input;
        let directory = input.container_path.as_deref().unwrap_or("/");
        let directory = directory.trim_end_matches('/');
        InputSelection {
            source: InputSource::S3 {
                bucket: self.bucket.clone(),
                key: key.to_string(),
                version_id: Some(version_id.to_string()),
            },
            source_node_id: input.source_node_id,
            dest_key: format!("{}{relative}", input.dest_key),
            mode: input.mode,
            container_path: Some(format!("{directory}/{relative}")),
            name: None,
            description: None,
        }
    }
}

impl Operation for ExpandDirectoryOperation {
    type Output = Vec<InputSelection>;
    type Error = SubmitJobError;

    fn first(&mut self) -> ExpandNext {
        Ok(Next::Call(IoCall::child(GetBucketOperation::new(
            self.bucket.clone(),
        ))))
    }

    fn next(&mut self, result: IoResult) -> ExpandNext {
        let reference = self.reference();
        match std::mem::replace(&mut self.step, DirectoryStep::List) {
            DirectoryStep::Bucket => {
                let Ok(info) = result.child::<BucketInfo, GetBucketError>()? else {
                    return Err(SubmitJobError::InvalidWorkspace(format!(
                        "{reference}: input bucket not found"
                    )));
                };
                if info.distribution.is_some() {
                    return Err(SubmitJobError::InvalidWorkspace(format!(
                        "{reference}: directory inputs of a distributed bucket are not supported"
                    )));
                }
                self.list(info.group_id)
            }
            DirectoryStep::List => {
                let page = result
                    .child::<ListBucketResult, ListBucketError>()?
                    .map_err(|error| {
                        SubmitJobError::PlacementUnavailable(format!("{reference}: {error}"))
                    })?;
                let objects = page.objects.into_iter();
                self.objects = objects
                    .map(|object| (object.head.key, object.version_id))
                    .collect();
                self.token = page.continuation_token;
                self.advance()
            }
            DirectoryStep::Read { key, version_id } => {
                result
                    .child::<GroupId, SourceReadError>()?
                    .map_err(|error| source_error(&reference, error))?;
                self.files.push(self.file(&key, version_id));
                self.advance()
            }
        }
    }
}

#[derive(Debug, PartialEq)]
enum HolderStep {
    Bucket,
    Coordinator,
}

/// Points a local input of a distributed bucket at the member that coordinates its key, so it
/// is captured and pinned there, as an S3 read of the key is served there. A missing or
/// unreadable bucket is refused by the capture with its own message.
#[derive(Debug, PartialEq)]
pub(super) struct RangeHolderOperation {
    input: Option<InputSelection>,
    local: NodeId,
    step: HolderStep,
}

type HolderNext = Result<Next<InputSelection>, SubmitJobError>;

impl RangeHolderOperation {
    pub(super) fn new(input: InputSelection, local: NodeId) -> Self {
        Self {
            input: Some(input),
            local,
            step: HolderStep::Bucket,
        }
    }

    fn input(&self) -> Result<&InputSelection, SubmitJobError> {
        let lost = || SubmitJobError::PlacementUnavailable("the input was lost".to_string());
        self.input.as_ref().ok_or_else(lost)
    }

    fn finish(&mut self) -> HolderNext {
        let lost = || SubmitJobError::PlacementUnavailable("the input was lost".to_string());
        self.input.take().map(Next::Done).ok_or_else(lost)
    }
}

impl Operation for RangeHolderOperation {
    type Output = InputSelection;
    type Error = SubmitJobError;

    fn first(&mut self) -> HolderNext {
        let input = self.input()?;
        if input.source_node_id.is_some_and(|node| node != self.local) {
            return self.finish();
        }
        let (bucket, _, _) = source_parts(input);
        Ok(Next::Call(IoCall::child(GetBucketOperation::new(
            bucket.to_string(),
        ))))
    }

    fn next(&mut self, result: IoResult) -> HolderNext {
        match self.step {
            HolderStep::Bucket => {
                let info = result.child::<BucketInfo, GetBucketError>()?;
                let Some(binding) = info.ok().and_then(|info| info.distribution) else {
                    return self.finish();
                };
                let (_, key, _) = source_parts(self.input()?);
                let route = coordinator_read(binding, key.to_string());
                self.step = HolderStep::Coordinator;
                Ok(Next::Call(IoCall::child(route)))
            }
            HolderStep::Coordinator => {
                let route = result.child::<Option<(Ulid, NodeId)>, GuardError>()?;
                let coordinator = route.map_err(|error| {
                    let reference = self.input().map(reference_of).unwrap_or_default();
                    SubmitJobError::PlacementUnavailable(format!("{reference}: {error}"))
                })?;
                if let (Some((_, node)), Some(input)) = (coordinator, self.input.as_mut()) {
                    input.source_node_id = Some(node);
                }
                self.finish()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::records::{node, user};
    use aruna_core::calls::ChildOutcome;
    use aruna_core::structs::execution::job::InputMode;
    use aruna_core::structs::storage::distribution::{DistributionBinding, DistributionRevision};
    use aruna_core::structs::storage::format::Compression;

    fn caller() -> InputCaller {
        InputCaller {
            created_by: user(),
            realm_id: RealmId([1u8; 32]),
            local: node(1),
        }
    }

    fn input(key: &str, dest_key: &str) -> InputSelection {
        InputSelection {
            source: InputSource::S3 {
                bucket: "b".to_string(),
                key: key.to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: dest_key.to_string(),
            mode: InputMode::Snapshot,
            container_path: Some("/in/".to_string()),
            name: None,
            description: None,
        }
    }

    fn bucket(distribution: Option<DistributionBinding>) -> BucketInfo {
        BucketInfo {
            group_id: Ulid::from_parts(2, 2),
            created_at: std::time::SystemTime::UNIX_EPOCH,
            created_by: user(),
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution,
        }
    }

    fn child<T, E>(result: Result<T, E>) -> IoResult
    where
        T: Send + std::fmt::Debug + PartialEq + 'static,
        E: Send + std::fmt::Debug + PartialEq + 'static,
    {
        IoResult::Child(ChildOutcome::new(result))
    }

    // An input on another node ends the walk at once; an unreadable head ends it unchanged.
    #[test]
    fn walk_stops_without_link() {
        let mut remote = input("a", "a");
        remote.source_node_id = Some(node(2));
        let mut elsewhere = FollowLinkOperation::new(caller(), remote.clone());
        assert_eq!(elsewhere.first(), Ok(Next::Done((remote, None))));

        let mut missing = FollowLinkOperation::new(caller(), input("a", "a"));
        assert!(matches!(missing.first(), Ok(Next::Call(IoCall::Child(_)))));
        let head = child(Err::<HeadObjectResult, _>(HeadObjectError::NoSuchKey));
        assert_eq!(missing.next(head), Ok(Next::Done((input("a", "a"), None))));
    }

    fn directory() -> ExpandDirectoryOperation {
        ExpandDirectoryOperation::new(caller(), input("dir/", "out/"))
    }

    fn listed(token: Option<ListContinuationToken>) -> IoResult {
        child(Ok::<_, ListBucketError>(ListBucketResult {
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            continuation_token: token,
        }))
    }

    // A directory lists its bucket; an empty one and a distributed one are refused.
    #[test]
    fn directory_needs_objects() {
        let mut empty = directory();
        empty.first().expect("bucket read");
        let found = child(Ok::<_, GetBucketError>(bucket(None)));
        assert!(matches!(
            empty.next(found),
            Ok(Next::Call(IoCall::Child(_)))
        ));
        assert!(matches!(
            empty.next(listed(None)),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));

        let binding = DistributionBinding {
            set_id: Ulid::from_parts(3, 3),
            member_id: Ulid::from_parts(4, 4),
            incarnation: 1,
            revision: DistributionRevision::default(),
        };
        let mut distributed = directory();
        distributed.first().expect("bucket read");
        let found = child(Ok::<_, GetBucketError>(bucket(Some(binding))));
        assert!(matches!(
            distributed.next(found),
            Err(SubmitJobError::InvalidWorkspace(_))
        ));
    }

    // Every listed file is read-checked before it becomes an input below the directory path;
    // folder markers are skipped and a denied file refuses the directory.
    #[test]
    fn directory_checks_each_file() {
        let version = Ulid::from_parts(5, 5);
        let mut walk = directory();
        walk.group_id = Some(Ulid::from_parts(2, 2));
        walk.objects = VecDeque::from([
            ("dir/".to_string(), version),
            ("dir/sub/".to_string(), version),
            ("dir/a".to_string(), version),
        ]);
        assert!(matches!(walk.advance(), Ok(Next::Call(IoCall::Child(_)))));
        let allowed = child(Ok::<GroupId, SourceReadError>(Ulid::from_parts(2, 2)));
        let Ok(Next::Done(files)) = walk.next(allowed) else {
            panic!("the only file is an input");
        };
        assert_eq!(files[0].dest_key, "out/a");
        assert_eq!(files[0].container_path.as_deref(), Some("/in/a"));

        let mut denied = directory();
        denied.objects = VecDeque::from([("dir/a".to_string(), version)]);
        denied.advance().expect("read check");
        let refused = child(Err::<GroupId, _>(SourceReadError::Denied));
        assert!(matches!(
            denied.next(refused),
            Err(SubmitJobError::InputDenied(_))
        ));
    }

    // A plain bucket keeps the input here; a distributed one points it at the coordinator.
    #[test]
    fn holder_follows_coordinator() {
        let mut plain = RangeHolderOperation::new(input("a", "a"), node(1));
        plain.first().expect("bucket read");
        let found = child(Ok::<_, GetBucketError>(bucket(None)));
        assert_eq!(plain.next(found), Ok(Next::Done(input("a", "a"))));

        let binding = DistributionBinding {
            set_id: Ulid::from_parts(3, 3),
            member_id: Ulid::from_parts(4, 4),
            incarnation: 1,
            revision: DistributionRevision::default(),
        };
        let mut routed = RangeHolderOperation::new(input("a", "a"), node(1));
        routed.first().expect("bucket read");
        let found = child(Ok::<_, GetBucketError>(bucket(Some(binding))));
        routed.next(found).expect("coordinator read");
        let route = Some((Ulid::from_parts(6, 6), node(2)));
        let Ok(Next::Done(pinned)) = routed.next(child(Ok::<_, GuardError>(route))) else {
            panic!("the coordinator answers");
        };
        assert_eq!(pinned.source_node_id, Some(node(2)));
    }
}
