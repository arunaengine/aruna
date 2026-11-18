//! Transfers LFS content through Aruna object operations on any document holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitRecord, LfsObject, StoredObject};
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::storage::blob::object_permission_path;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use aruna_core::structs::storage::routing::NodeRouting;
use tokio::time::Instant;
use ulid::Ulid;

use super::access::{Repository, RepositoryOperation};
use super::calls::node_of;
use super::objects::{self, Blob, StoreObjectOperation};
use super::state::{Ancestry, reduce};
use super::{GitError, publish, records, repository};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::distribution::resolve::{RESOLVE_BUDGET, resolve_version, version_found};
use crate::driver::{DriverContext, drive, drive_until, node_routing};
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::managed_read::ManagedReadOperation;
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};

type Document = MetadataRegistryRecord;

fn key(id: Ulid, oid: &str) -> String {
    format!("git-lfs/{id}/{oid}")
}

/// Where content was originally stored: a replicated record, or an upload not pushed yet.
pub(super) fn locate(document: &Document, oid: &str) -> LocateObjectOperation {
    LocateObjectOperation {
        document: document.clone(),
        oid: oid.to_string(),
        scanned: false,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct LocateObjectOperation {
    document: Document,
    oid: String,
    scanned: bool,
}

type Located = Option<StoredObject>;

impl Operation for LocateObjectOperation {
    type Output = Located;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Located>, GitError> {
        let scan = records::scan(self.document.document_id);
        Ok(Next::Call(IoCall::child(scan)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Located>, GitError> {
        if self.scanned {
            return Ok(Next::Done(result.child::<Located, GitError>()??));
        }
        let records = result.child::<Vec<GitRecord>, GitError>()??;
        let mut state = reduce(&records, &Ancestry::new()).0;
        if let Some(object) = state.lfs.remove(&self.oid) {
            return Ok(Next::Done(Some(object)));
        }
        self.scanned = true;
        let copy = objects::copy(&self.document, &self.oid);
        Ok(Next::Call(IoCall::child(copy)))
    }
}

/// A metadata-only read of the exact version on its own node.
fn described(auth: &AuthContext, object: &StoredObject) -> BaoReadRequest {
    BaoReadRequest {
        auth_context: auth.clone(),
        realm_id: auth.realm_id,
        target: BaoReadTarget::ExactVersion(VersionedObjectArn {
            realm_id: auth.realm_id,
            node_id: object.node_id,
            bucket: object.bucket.clone(),
            key: object.key.clone(),
            version: object.version_id,
        }),
        expected_blake3: Some(object.blake3),
        metadata_only: true,
        destination: None,
        known_refs: Vec::new(),
        range: None,
    }
}

fn described_call(auth: &AuthContext, object: &StoredObject) -> IoCall {
    let read = ManagedReadOperation::new(object.node_id, described(auth, object));
    IoCall::child(read)
}

/// Access is always decided for the original object, never for a node's copy of it. With a
/// known bucket group the path policy decides here; otherwise the source node decides.
#[derive(Debug, PartialEq)]
pub(super) struct PermitObjectOperation {
    auth: AuthContext,
    object: StoredObject,
    permission: Permission,
}

impl PermitObjectOperation {
    fn new(auth: &AuthContext, object: &StoredObject, permission: Permission) -> Self {
        Self {
            auth: auth.clone(),
            object: object.clone(),
            permission,
        }
    }
}

impl Operation for PermitObjectOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> Result<Next<()>, GitError> {
        let (auth, object) = (&self.auth, &self.object);
        let Some(group_id) = object.group_id else {
            if self.permission != Permission::READ {
                return Err(GitError::Invalid);
            }
            return Ok(Next::Call(described_call(auth, object)));
        };
        let path = object_permission_path(
            auth.realm_id,
            group_id,
            object.node_id,
            &object.bucket,
            &object.key,
        );
        let authorize = AuthorizeOperation::new(
            auth.realm_id,
            auth,
            &path,
            &self.permission,
            PolicyRequestExtras::rest(),
        );
        Ok(Next::Call(IoCall::child(authorize)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<()>, GitError> {
        if self.object.group_id.is_some() {
            result.child::<(), AuthorizeError>()??;
            return Ok(Next::Done(()));
        }
        match result.child::<BaoReadOutput, BaoReadError>()? {
            Ok(_) => Ok(Next::Done(())),
            Err(BaoReadError::Refused(_)) => Err(GitError::NotFound),
            Err(_) => Err(GitError::Unavailable),
        }
    }
}

/// Whether the node holding the object still serves that exact version to `auth`.
pub(super) fn readable(
    context: &DriverContext,
    auth: &AuthContext,
    object: &StoredObject,
) -> ReadableOperation {
    ReadableOperation {
        node: node_of(context),
        auth: auth.clone(),
        object: object.clone(),
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct ReadableOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    object: StoredObject,
}

impl ReadableOperation {
    fn local(&self) -> bool {
        self.node == Some(self.object.node_id)
    }
}

impl Operation for ReadableOperation {
    type Output = bool;
    type Error = IoError;

    fn first(&mut self) -> Result<Next<bool>, IoError> {
        let object = &self.object;
        Ok(match (self.local(), object.group_id) {
            (true, Some(group_id)) => Next::Call(IoCall::child(objects::describe_call(
                self.node,
                (&object.bucket, group_id),
                &object.key,
                Some(object.version_id),
            ))),
            (true, None) => Next::Done(false),
            (false, _) => Next::Call(described_call(&self.auth, object)),
        })
    }

    fn next(&mut self, result: IoResult) -> Result<Next<bool>, IoError> {
        Ok(Next::Done(match self.local() {
            true => matches!(result.child::<Located, GitError>()?, Ok(Some(_))),
            false => result.child::<BaoReadOutput, BaoReadError>()?.is_ok(),
        }))
    }
}

/// The stored location of an LFS object the caller may reach with `permission`.
pub fn inspect(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    object: &LfsObject,
    permission: Permission,
) -> InspectObjectOperation {
    InspectObjectOperation {
        object: object.clone(),
        open: Some(RepositoryOperation::new(
            node_of(context),
            auth,
            id,
            permission,
        )),
        locating: false,
    }
}

#[derive(Debug, PartialEq)]
pub struct InspectObjectOperation {
    object: LfsObject,
    open: Option<RepositoryOperation>,
    locating: bool,
}

impl Operation for InspectObjectOperation {
    type Output = Located;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Located>, GitError> {
        if !self.object.valid() {
            return Err(GitError::Invalid);
        }
        let open = self.open.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(open)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Located>, GitError> {
        if !self.locating {
            let (document, _) = result.child::<Repository, GitError>()??;
            self.locating = true;
            let locate = locate(&document, &self.object.oid);
            return Ok(Next::Call(IoCall::child(locate)));
        }
        let location = result.child::<Located, GitError>()??;
        if (location.as_ref()).is_some_and(|location| location.size != self.object.size) {
            return Err(GitError::Invalid);
        }
        Ok(Next::Done(location))
    }
}

/// What the pending call of an upload answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UploadStep {
    Open,
    Copy,
    Permit,
    Store,
}

/// Stores LFS content the caller may write, unless this node already has it.
pub fn upload(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    object: LfsObject,
    body: Blob,
) -> UploadObjectOperation {
    let node = node_of(context);
    UploadObjectOperation {
        node,
        routing: node_routing(context),
        auth: auth.clone(),
        id,
        object,
        body: Some(body),
        open: Some(RepositoryOperation::new(node, auth, id, Permission::WRITE)),
        step: UploadStep::Open,
        document: None,
        bucket: None,
        target: None,
    }
}

#[derive(Debug, PartialEq)]
pub struct UploadObjectOperation {
    node: Option<NodeId>,
    routing: NodeRouting,
    auth: AuthContext,
    id: Ulid,
    object: LfsObject,
    body: Option<Blob>,
    open: Option<RepositoryOperation>,
    step: UploadStep,
    document: Option<Document>,
    bucket: Option<String>,
    target: Option<StoredObject>,
}

type UploadTurn = Result<Next<()>, GitError>;

impl UploadObjectOperation {
    fn permit(&mut self) -> UploadTurn {
        let bucket = self.bucket.take().ok_or(IoError::Unfinished)?;
        let document = self.document.as_ref().ok_or(IoError::Unfinished)?;
        let target = StoredObject {
            node_id: self.node.ok_or(GitError::Unavailable)?,
            group_id: Some(document.group_id),
            bucket,
            key: key(self.id, &self.object.oid),
            version_id: Ulid::nil(),
            size: self.object.size,
            sha256: self.object.oid.clone(),
            blake3: [0; 32],
            global: None,
        };
        let permit = PermitObjectOperation::new(&self.auth, &target, Permission::WRITE);
        self.target = Some(target);
        self.step = UploadStep::Permit;
        Ok(Next::Call(IoCall::child(permit)))
    }

    fn store(&mut self) -> UploadTurn {
        let target = self.target.take().ok_or(IoError::Unfinished)?;
        let document = self.document.as_ref().ok_or(IoError::Unfinished)?;
        let body = self.body.take().ok_or(IoError::Unfinished)?;
        let store = StoreObjectOperation::new(
            (self.node, self.routing.clone()),
            &self.auth,
            document,
            (target.key, self.object.size, &self.object.oid),
            body,
        );
        self.step = UploadStep::Store;
        Ok(Next::Call(IoCall::child(store)))
    }
}

impl Operation for UploadObjectOperation {
    type Output = ();
    type Error = GitError;

    fn first(&mut self) -> UploadTurn {
        if !self.object.valid() {
            return Err(GitError::Invalid);
        }
        let open = self.open.take().ok_or(IoError::Unfinished)?;
        Ok(Next::Call(IoCall::child(open)))
    }

    fn next(&mut self, result: IoResult) -> UploadTurn {
        match self.step {
            UploadStep::Open => {
                let (document, repository) = result.child::<Repository, GitError>()??;
                let copy = objects::copy(&document, &self.object.oid);
                self.document = Some(document);
                self.bucket = Some(repository.bucket);
                self.step = UploadStep::Copy;
                Ok(Next::Call(IoCall::child(copy)))
            }
            UploadStep::Copy => {
                if result.child::<Located, GitError>()??.is_some() {
                    return Ok(Next::Done(()));
                }
                self.permit()
            }
            UploadStep::Permit => {
                result.child::<(), GitError>()??;
                self.store()
            }
            UploadStep::Store => {
                result.child::<StoredObject, GitError>()??;
                Ok(Next::Done(()))
            }
        }
    }
}

/// The members that keep a pinned distributed version, found through the resolver.
async fn set_holders(context: &DriverContext, object: &StoredObject) -> Vec<NodeId> {
    match &object.global {
        Some(GlobalId::Version { version_id, .. }) => {
            let deadline = Instant::now() + RESOLVE_BUDGET;
            let resolve = resolve_version(context, *version_id, deadline);
            version_found(drive_until(resolve, context, deadline).await)
                .map(|resolved| resolved.holders)
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

/// Streams LFS content after authorizing the original object. Content held elsewhere is
/// relayed from its node and not stored here.
pub async fn download(
    context: &DriverContext,
    auth: &AuthContext,
    id: Ulid,
    oid: &str,
) -> Result<(Blob, u64), GitError> {
    let (document, _) = drive(repository(context, auth, id, Permission::READ), context).await?;
    let original = drive(locate(&document, oid), context)
        .await?
        .ok_or(GitError::NotFound)?;
    let permit = PermitObjectOperation::new(auth, &original, Permission::READ);
    drive(permit, context).await?;
    let node = node_of(context).ok_or(GitError::Unavailable)?;
    let local = match drive(objects::copy(&document, oid), context).await? {
        Some(copy) => copy,
        None if original.node_id == node => original,
        None => {
            let mut holders = drive(publish::holders(context, &document), context).await?;
            holders.extend(set_holders(context, &original).await);
            let blob = objects::open(context, auth, &document, &original, &holders).await?;
            return Ok((blob, original.size));
        }
    };
    let blob = objects::open(context, auth, &document, &local, &[]).await?;
    Ok((blob, local.size))
}

#[cfg(test)]
#[path = "lfs_tests.rs"]
mod tests;
