//! Follows links for reads: authorizes the target for the caller, like an S3 read of it, and
//! reads its exact version here or, through a delegation, at its node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::ops::Range;
use std::time::{Duration, UNIX_EPOCH};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::errors::StagingSourceError;
use aruna_core::native_reference::DelegatedBody;
use aruna_core::operation::Operation;
use aruna_core::structs::execution::source_access::SourceMetadata;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::storage::blob::{CONTENT_TYPE_KEY, UserAccess, object_permission_path};
use aruna_core::structs::storage::distribution::delegation::{MAX_VALID_SECS, Payload};
use aruna_core::structs::storage::link::LinkTarget;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::types::GroupId;
use thiserror::Error;
use ulid::Ulid;

use crate::auth::error::AuthorizeError;
use crate::auth::error::PolicyEnforcementError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::delegate::DelegationInput;
use crate::jobs::read_access::SourceReadError;
use crate::jobs::read_access::SourceReadOperation;
use crate::jobs::workflow::input_view::ViewStream;
use crate::s3::object::get::{
    GetObjectError, GetObjectInput, GetObjectOperation, GetObjectResult, ObjectRangeRequest,
};
use crate::s3::object::head::{
    HeadObjectError, HeadObjectInput, HeadObjectOperation, HeadObjectResult,
};
use crate::staging::native_source::delegated_read;
use crate::staging::reference::{
    MaterializeReferenceError, ReferenceWrite, WriteReferenceOperation,
};
use aruna_core::structs::storage::read_grant::{HEAD_OPERATION, READ_OPERATION};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LinkError {
    #[error("the link target is denied")]
    Denied,
    #[error("the link target is not found")]
    NotFound,
    #[error("the link target is unavailable: {0}")]
    Unavailable(String),
    #[error("the link request is invalid: {0}")]
    Invalid(String),
}

impl From<IoError> for LinkError {
    fn from(error: IoError) -> Self {
        Self::Unavailable(error.to_string())
    }
}

/// Who reads through a link, as the node that verified the request's credential saw them.
#[derive(Clone, Debug, PartialEq)]
pub struct LinkCaller {
    pub auth: AuthContext,
    /// The verified credential's id, group and expiry in Unix seconds, for delegations.
    pub credential: (String, GroupId, u64),
}

impl LinkCaller {
    /// The caller of an S3 request signed with `access`.
    pub fn of_access(auth: AuthContext, access: &UserAccess) -> Self {
        let expiry = access
            .expiry
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs());
        Self {
            auth,
            credential: (access.access_key.clone(), access.group_id, expiry),
        }
    }
}

/// This node as a link read needs it: its id, and the ids a delegation is signed under.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LinkNode {
    pub local: NodeId,
    pub ids: Option<(NodeId, RealmId)>,
}

type LinkTurn<T> = Result<Next<T>, LinkError>;

fn staging_error(error: StagingSourceError) -> LinkError {
    match error {
        StagingSourceError::AccessDenied => LinkError::Denied,
        StagingSourceError::NotFound => LinkError::NotFound,
        other => LinkError::Unavailable(other.to_string()),
    }
}

fn unavailable(error: impl std::fmt::Display) -> LinkError {
    LinkError::Unavailable(error.to_string())
}

/// The verified caller a delegation carries; the read fills in what it binds. A signed-out
/// caller has no credential to bound it, so its own validity does and the target checks READ.
fn delegation(caller: &LinkCaller, target: NodeId, now_secs: u64) -> DelegationInput {
    let (credential_id, group_id, expiry) = caller.credential.clone();
    let expiry = match caller.auth.user_id.is_nil() {
        true => now_secs.saturating_add(MAX_VALID_SECS),
        false => expiry,
    };
    DelegationInput {
        set_id: Ulid::nil(),
        range_id: Ulid::nil(),
        target,
        user_id: caller.auth.user_id,
        group_id,
        credential_id,
        restrictions: caller.auth.path_restrictions.clone(),
        credential_expiry: expiry,
        operation: String::new(),
        bucket: String::new(),
        key: String::new(),
        copy_source: None,
        request_digest: [0; 32],
        payload: Payload::Empty,
    }
}

/// Whether the caller may read the target, evaluated here from the target's recorded group and
/// the replicated roles and policies; the target's node checks again on every byte read.
#[derive(Debug, PartialEq)]
pub struct AuthorizeTargetOperation {
    check: Option<AuthorizeOperation>,
}

impl AuthorizeTargetOperation {
    pub fn new(auth: &AuthContext, target: &LinkTarget, operation: &str) -> Self {
        let arn = &target.arn;
        let path = object_permission_path(
            auth.realm_id,
            target.group_id,
            arn.node_id,
            &arn.bucket,
            &arn.key,
        );
        let extras = PolicyRequestExtras::operation(operation);
        let check = AuthorizeOperation::new(auth.realm_id, auth, &path, &Permission::READ, extras);
        Self { check: Some(check) }
    }
}

impl Operation for AuthorizeTargetOperation {
    type Output = ();
    type Error = LinkError;

    fn first(&mut self) -> LinkTurn<()> {
        let check = self.check.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> LinkTurn<()> {
        match result.child::<(), AuthorizeError>()? {
            Ok(()) => Ok(Next::Done(())),
            Err(
                AuthorizeError::PermissionDenied
                | AuthorizeError::Policy(PolicyEnforcementError::Denied { .. }),
            ) => Err(LinkError::Denied),
            Err(error) => Err(unavailable(error)),
        }
    }
}

/// Whether the caller may HEAD the target: checked here and, for a target on another node, by
/// that node in one delegated HEAD.
#[derive(Debug, PartialEq)]
pub struct HeadTargetOperation {
    caller: LinkCaller,
    target: LinkTarget,
    node: LinkNode,
    now_ms: u64,
    asking: bool,
}

impl HeadTargetOperation {
    pub fn new(caller: &LinkCaller, target: &LinkTarget, node: LinkNode) -> Self {
        Self {
            caller: caller.clone(),
            target: target.clone(),
            node,
            now_ms: 0,
            asking: false,
        }
    }
}

impl Operation for HeadTargetOperation {
    type Output = ();
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> LinkTurn<()> {
        let check = AuthorizeTargetOperation::new(&self.caller.auth, &self.target, HEAD_OPERATION);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> LinkTurn<()> {
        if self.asking {
            let answer = result.child::<DelegatedBody, StagingSourceError>()?;
            answer.map_err(staging_error)?;
            return Ok(Next::Done(()));
        }
        result.child::<(), LinkError>()??;
        let arn = &self.target.arn;
        if arn.node_id == self.node.local {
            return Ok(Next::Done(()));
        }
        let caller = delegation(&self.caller, arn.node_id, self.now_ms / 1000);
        let read = delegated_read(self.node.ids, caller, arn, None, true);
        self.asking = true;
        Ok(Next::Call(IoCall::child(read)))
    }
}

/// Whether a listing may show a link: the caller may read its target.
#[derive(Debug, PartialEq)]
pub struct TargetVisibleOperation {
    head: Option<HeadTargetOperation>,
}

impl TargetVisibleOperation {
    pub fn new(caller: &LinkCaller, target: &LinkTarget, node: LinkNode) -> Self {
        Self {
            head: Some(HeadTargetOperation::new(caller, target, node)),
        }
    }
}

impl Operation for TargetVisibleOperation {
    type Output = bool;
    type Error = LinkError;

    fn first(&mut self) -> LinkTurn<bool> {
        let head = self.head.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(head)))
    }

    fn next(&mut self, result: IoResult) -> LinkTurn<bool> {
        match result.child::<(), LinkError>()? {
            Ok(()) => Ok(Next::Done(true)),
            Err(LinkError::Denied | LinkError::NotFound) => Ok(Next::Done(false)),
            Err(error) => Err(error),
        }
    }
}

#[derive(Debug, PartialEq)]
enum ReadTargetStep {
    Authorize,
    Local,
    Remote,
}

/// The target's bytes, or `range` of them, at its exact version. A target on another node is
/// read through one delegation that carries the caller this node verified.
#[derive(Debug, PartialEq)]
pub struct ReadTargetOperation {
    caller: LinkCaller,
    target: LinkTarget,
    node: LinkNode,
    range: Option<Range<u64>>,
    now_ms: u64,
    step: ReadTargetStep,
}

impl ReadTargetOperation {
    pub fn new(
        caller: &LinkCaller,
        target: &LinkTarget,
        node: LinkNode,
        range: Option<Range<u64>>,
    ) -> Self {
        Self {
            caller: caller.clone(),
            target: target.clone(),
            node,
            range,
            now_ms: 0,
            step: ReadTargetStep::Authorize,
        }
    }

    fn read(&mut self) -> LinkTurn<ViewStream> {
        let arn = &self.target.arn;
        let range = self.range.take();
        if arn.node_id != self.node.local {
            let caller = delegation(&self.caller, arn.node_id, self.now_ms / 1000);
            let read = delegated_read(self.node.ids, caller, arn, range, false);
            self.step = ReadTargetStep::Remote;
            return Ok(Next::Call(IoCall::child(read)));
        }
        let get = GetObjectOperation::new(GetObjectInput {
            bucket: arn.bucket.clone(),
            key: arn.key.clone(),
            version_id: Some(arn.version),
            range: range.map(|range| ObjectRangeRequest::StartEnd {
                start: range.start,
                end: range.end.saturating_sub(1),
            }),
            group_id: self.target.group_id,
            user_identity: self.caller.auth.user_id,
            realm_id: self.caller.auth.realm_id,
            node_id: self.node.local,
        });
        self.step = ReadTargetStep::Local;
        Ok(Next::Call(IoCall::child(get)))
    }
}

impl Operation for ReadTargetOperation {
    type Output = ViewStream;
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> LinkTurn<ViewStream> {
        let check = AuthorizeTargetOperation::new(&self.caller.auth, &self.target, READ_OPERATION);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> LinkTurn<ViewStream> {
        match self.step {
            ReadTargetStep::Authorize => {
                result.child::<(), LinkError>()??;
                self.read()
            }
            ReadTargetStep::Local => {
                let read = result.child::<GetObjectResult, GetObjectError>()?;
                Ok(Next::Done(read.map_err(unavailable)?.blob))
            }
            ReadTargetStep::Remote => {
                let read = result.child::<DelegatedBody, StagingSourceError>()?;
                Ok(Next::Done(read.map_err(staging_error)?.blob))
            }
        }
    }
}

/// Where a link should point: a node, a bucket and key there, and maybe an exact version.
#[derive(Clone, Debug, PartialEq)]
pub struct LinkSource {
    pub node_id: NodeId,
    pub bucket: String,
    pub key: String,
    pub version: Option<Ulid>,
}

/// A source on this node as a link records it.
fn local_metadata(head: &HeadObjectResult, version: Ulid) -> SourceMetadata {
    let (location, source) = (head.location.as_ref(), head.source_metadata.as_ref());
    SourceMetadata {
        content_length: (location.map(|location| location.blob_size))
            .or_else(|| source.map(|source| source.content_length))
            .unwrap_or_default(),
        content_type: (head.metadata.get(CONTENT_TYPE_KEY).cloned())
            .or_else(|| source.and_then(|source| source.content_type.clone())),
        etag: Some(version.to_string()),
        last_modified: head.version_created_at,
        source_version: None,
        digests: Default::default(),
    }
}

#[derive(Debug, PartialEq)]
enum CreateLinkStep {
    Access,
    Head(GroupId),
    Remote(VersionedObjectArn),
    Write,
}

/// Writes a link at a destination to the source's exact version, for a caller who may read that
/// version now. A source on another node must name its version.
#[derive(Debug, PartialEq)]
pub struct CreateLinkOperation {
    caller: LinkCaller,
    bucket: String,
    key: String,
    group_id: GroupId,
    node: LinkNode,
    source: LinkSource,
    fresh: Ulid,
    now_ms: u64,
    step: CreateLinkStep,
}

impl CreateLinkOperation {
    pub fn new(
        caller: &LinkCaller,
        (bucket, key, group_id): (&str, &str, GroupId),
        node: LinkNode,
        source: LinkSource,
    ) -> Self {
        Self {
            caller: caller.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            group_id,
            node,
            source,
            fresh: Ulid::generate(),
            now_ms: 0,
            step: CreateLinkStep::Access,
        }
    }

    /// A source on another node, described by that node for the caller in one delegated HEAD.
    fn ask_remote(&mut self) -> LinkTurn<Ulid> {
        let source = &self.source;
        let version = source.version.ok_or_else(|| {
            LinkError::Invalid("a link to another node names the exact version".to_string())
        })?;
        let arn = VersionedObjectArn {
            realm_id: self.caller.auth.realm_id,
            node_id: source.node_id,
            bucket: source.bucket.clone(),
            key: source.key.clone(),
            version,
        };
        let caller = delegation(&self.caller, arn.node_id, self.now_ms / 1000);
        let read = delegated_read(self.node.ids, caller, &arn, None, true);
        self.step = CreateLinkStep::Remote(arn);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn write(&mut self, target: LinkTarget, metadata: SourceMetadata) -> LinkTurn<Ulid> {
        let caller = &self.caller;
        let write = ReferenceWrite {
            job_id: None,
            preassigned_version_id: None,
            group_id: self.group_id,
            user_id: caller.auth.user_id,
            realm_id: caller.auth.realm_id,
            node_id: self.node.local,
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            expected_bucket: None,
            version_source: target.binding(),
            metadata,
            inherited_policies: Vec::new(),
            connector_guard: None,
            restrictions: caller.auth.path_restrictions.clone(),
            mirror_guards: Vec::new(),
        };
        let now = UNIX_EPOCH + Duration::from_millis(self.now_ms);
        self.step = CreateLinkStep::Write;
        let write = WriteReferenceOperation::new(write, now, self.fresh);
        Ok(Next::Call(IoCall::child(write)))
    }
}

impl Operation for CreateLinkOperation {
    type Output = Ulid;
    type Error = LinkError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> LinkTurn<Ulid> {
        let source = &self.source;
        if source.node_id != self.node.local {
            return self.ask_remote();
        }
        let object = (source.bucket.as_str(), source.key.as_str());
        let read = SourceReadOperation::of_object(
            &self.caller.auth,
            source.node_id,
            object,
            HEAD_OPERATION,
        );
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> LinkTurn<Ulid> {
        match std::mem::replace(&mut self.step, CreateLinkStep::Write) {
            CreateLinkStep::Access => {
                let group_id =
                    (result.child::<GroupId, SourceReadError>()?).map_err(|error| match error {
                        SourceReadError::NotFound => LinkError::NotFound,
                        SourceReadError::Denied => LinkError::Denied,
                        SourceReadError::Unavailable(error) => LinkError::Unavailable(error),
                    })?;
                let head = HeadObjectOperation::new(HeadObjectInput {
                    bucket: self.source.bucket.clone(),
                    key: self.source.key.clone(),
                    version_id: self.source.version,
                });
                self.step = CreateLinkStep::Head(group_id);
                Ok(Next::Call(IoCall::child(head)))
            }
            CreateLinkStep::Head(group_id) => {
                let head =
                    (result.child::<HeadObjectResult, HeadObjectError>()?).map_err(|error| {
                        match error {
                            HeadObjectError::NoSuchKey
                            | HeadObjectError::NoSuchVersion
                            | HeadObjectError::DeleteMarker => LinkError::NotFound,
                            other => unavailable(other),
                        }
                    })?;
                let version =
                    (head.resolved_version_id.or(head.version_id)).ok_or(LinkError::NotFound)?;
                let metadata = local_metadata(&head, version);
                let source = &self.source;
                let target = LinkTarget {
                    arn: VersionedObjectArn {
                        realm_id: self.caller.auth.realm_id,
                        node_id: source.node_id,
                        bucket: source.bucket.clone(),
                        key: source.key.clone(),
                        version,
                    },
                    group_id,
                };
                self.write(target, metadata)
            }
            CreateLinkStep::Remote(arn) => {
                let answer = result.child::<DelegatedBody, StagingSourceError>()?;
                let answer = answer.map_err(staging_error)?;
                let group_id = answer
                    .group_id
                    .ok_or_else(|| unavailable("the source node did not name its bucket group"))?;
                let metadata = SourceMetadata {
                    etag: Some(arn.version.to_string()),
                    ..answer.metadata
                };
                self.write(LinkTarget { arn, group_id }, metadata)
            }
            CreateLinkStep::Write => {
                let written = result.child::<(Ulid, bool), MaterializeReferenceError>()?;
                Ok(Next::Done(written.map_err(unavailable)?.0))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use aruna_core::structs::identity::realm::RealmId;

    use super::*;
    use crate::driver::drive;
    use crate::testing::test_context;
    use crate::testing_access::seed_access;
    use crate::tests::s3::test_storage;

    #[tokio::test]
    async fn head_asks_target() {
        // Local roles allow both targets, but one on another node shows only once that node
        // answers a delegated HEAD, which it cannot without a network here.
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let (realm_id, group_id) = (RealmId::from_bytes([1; 32]), Ulid::from_parts(2, 2));
        let local = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let remote = iroh::SecretKey::from_bytes(&[5; 32]).public();
        let auth = seed_access(&context, (realm_id, group_id, local), "/denied").await;
        let caller = LinkCaller {
            auth,
            credential: ("key".to_string(), group_id, u64::MAX),
        };
        let target = |node_id| LinkTarget {
            arn: VersionedObjectArn {
                realm_id,
                node_id,
                bucket: "db".to_string(),
                key: "bakta.db".to_string(),
                version: Ulid::from_parts(3, 3),
            },
            group_id,
        };
        let node = LinkNode {
            local,
            ids: context.node_and_realm(),
        };
        let visible = |target| TargetVisibleOperation::new(&caller, &target, node);
        let here = drive(visible(target(local)), &context).await;
        assert_eq!(here, Ok(true));
        let there = drive(visible(target(remote)), &context).await;
        assert!(matches!(there, Err(LinkError::Unavailable(_))), "{there:?}");
    }

    #[test]
    fn signed_out_delegation() {
        // A signed-out caller has no credential expiry; its delegation must still be valid now.
        let realm_id = RealmId::from_bytes([1; 32]);
        let target = iroh::SecretKey::from_bytes(&[5; 32]).public();
        let caller = |auth| LinkCaller {
            auth,
            credential: (String::new(), Ulid::from_parts(2, 2), 0),
        };
        let now = 1_000;
        let visitor = delegation(&caller(AuthContext::anonymous(realm_id)), target, now);
        assert_eq!(visitor.credential_expiry, now + MAX_VALID_SECS);
        let member = AuthContext {
            user_id: aruna_core::UserId::new(Ulid::from_parts(7, 7), realm_id),
            ..AuthContext::anonymous(realm_id)
        };
        assert_eq!(
            delegation(&caller(member), target, now).credential_expiry,
            0
        );
    }

    #[tokio::test]
    async fn remote_needs_version() {
        // A link to another node without a version is a bad request, not an unavailable target.
        let (_directory, storage) = test_storage();
        let context = test_context(storage);
        let (realm_id, group_id) = (RealmId::from_bytes([1; 32]), Ulid::from_parts(2, 2));
        let local = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let remote = iroh::SecretKey::from_bytes(&[5; 32]).public();
        let auth = seed_access(&context, (realm_id, group_id, local), "/denied").await;
        let caller = LinkCaller {
            auth,
            credential: ("key".to_string(), group_id, u64::MAX),
        };
        let source = LinkSource {
            node_id: remote,
            bucket: "work3".to_string(),
            key: "n3.txt".to_string(),
            version: None,
        };
        let node = LinkNode {
            local,
            ids: context.node_and_realm(),
        };
        let created = CreateLinkOperation::new(&caller, ("work", "n3.txt", group_id), node, source);
        let error = drive(created, &context).await.unwrap_err();
        assert!(matches!(&error, LinkError::Invalid(_)), "{error:?}");
        assert!(error.to_string().contains("names the exact version"));
    }

    fn step_target() -> (LinkCaller, LinkTarget, LinkNode) {
        let realm_id = RealmId::from_bytes([1; 32]);
        let node = iroh::SecretKey::from_bytes(&[4; 32]).public();
        let caller = LinkCaller {
            auth: AuthContext::anonymous(realm_id),
            credential: (String::new(), Ulid::from_parts(2, 2), 0),
        };
        let target = LinkTarget {
            arn: VersionedObjectArn {
                realm_id,
                node_id: node,
                bucket: "db".to_string(),
                key: "file".to_string(),
                version: Ulid::from_parts(3, 3),
            },
            group_id: Ulid::from_parts(2, 2),
        };
        let here = LinkNode {
            local: node,
            ids: None,
        };
        (caller, target, here)
    }

    fn outcome<T>(result: Result<T, LinkError>) -> IoResult
    where
        T: std::fmt::Debug + PartialEq + Send + 'static,
    {
        IoResult::Child(aruna_core::calls::ChildOutcome::new(result))
    }

    #[test]
    fn local_head_ends() {
        let (caller, target, node) = step_target();
        let mut head = HeadTargetOperation::new(&caller, &target, node);
        head.first().unwrap();
        assert_eq!(head.next(outcome(Ok(()))), Ok(Next::Done(())));
    }

    #[test]
    fn denied_target_hidden() {
        let (caller, target, node) = step_target();
        let mut visible = TargetVisibleOperation::new(&caller, &target, node);
        visible.first().unwrap();
        let denied = outcome::<()>(Err(LinkError::Denied));
        assert_eq!(visible.next(denied), Ok(Next::Done(false)));
    }

    #[test]
    fn failed_head_surfaces() {
        let (caller, target, node) = step_target();
        let mut visible = TargetVisibleOperation::new(&caller, &target, node);
        visible.first().unwrap();
        let failed = LinkError::Unavailable("down".to_string());
        let answer = visible.next(outcome::<()>(Err(failed.clone())));
        assert_eq!(answer, Err(failed));
    }

    #[test]
    fn denied_read_stops() {
        let (caller, target, node) = step_target();
        let mut read = ReadTargetOperation::new(&caller, &target, node, None);
        read.first().unwrap();
        let answer = read.next(outcome::<()>(Err(LinkError::Denied)));
        assert_eq!(answer, Err(LinkError::Denied));
    }
}
