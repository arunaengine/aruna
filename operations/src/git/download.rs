//! Reads stored Git packs and LFS content from this node or from any document holder.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::VecDeque;

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitPack, GitPackRecord, GitRecord, StoredObject, git_pack_key};
use aruna_core::keyspaces::GIT_PACK_KEYSPACE;
use aruna_core::operation::Operation;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::NodeRealm;
use aruna_core::structs::storage::global_id::GlobalId;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::structs::storage::replication::VersionedObjectArn;
use bytes::Bytes;
use ulid::Ulid;

use super::access::Repository;
use super::calls::{
    child_member, collect_call, collected, group_call, group_results, member_outcome,
};
use super::lfs::{PermitObjectOperation, locate};
use super::objects::{self, Blob};
use super::{GitError, publish, records, repository};
use crate::distribution::resolve::{
    GlobalResolution, RESOLVE_BUDGET, ResolvedVersion, resolve_bounded, version_found,
};
use crate::replication::bao_read::{BaoReadError, BaoReadOutput};
use crate::replication::managed_read::ManagedReadOperation;
use crate::replication::protocol::{BaoReadRequest, BaoReadTarget};
use crate::s3::object::get::{GetObjectError, GetObjectInput, GetObjectResult};
use crate::s3::object::read::GetRoutedOperation;

type Document = MetadataRegistryRecord;

/// What the pending call of reading a pack answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PackStep {
    Stored,
    Copy,
    Open,
    Collect,
    Publish,
}

/// The bytes of a pack `owner` names first. A pack this node still keeps only as an object
/// from before packs moved into Fjall is moved there once and replicated.
pub(super) fn pack_bytes(
    node: Option<NodeId>,
    document: &Document,
    pack: &GitPack,
    owner: &GitRecord,
) -> PackBytesOperation {
    PackBytesOperation {
        node,
        document: document.clone(),
        pack: pack.clone(),
        owner: owner.clone(),
        step: PackStep::Stored,
        bytes: None,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct PackBytesOperation {
    node: Option<NodeId>,
    document: Document,
    pack: GitPack,
    owner: GitRecord,
    step: PackStep,
    bytes: Option<Bytes>,
}

impl Operation for PackBytesOperation {
    type Output = Bytes;
    type Error = GitError;

    fn first(&mut self) -> Result<Next<Bytes>, GitError> {
        let digest = self.pack.digest().ok_or(GitError::Invalid)?;
        let key = git_pack_key(self.document.document_id, &digest).to_vec();
        let load = records::load::<GitPackRecord>(GIT_PACK_KEYSPACE, key);
        Ok(Next::Call(IoCall::child(load)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Bytes>, GitError> {
        match self.step {
            PackStep::Stored => {
                if let Some(stored) = result.child::<Option<GitPackRecord>, GitError>()?? {
                    return Ok(Next::Done(stored.bytes));
                }
                self.step = PackStep::Copy;
                let copy = objects::copy(&self.document, &self.pack.sha256);
                Ok(Next::Call(IoCall::child(copy)))
            }
            PackStep::Copy => {
                let object = result.child::<Option<StoredObject>, GitError>()??;
                let object = object.ok_or(GitError::Unavailable)?;
                let auth = super::project::author(self.owner.user_id);
                self.step = PackStep::Open;
                let read = open(self.node, &auth, &self.document, &object, &[]);
                Ok(Next::Call(IoCall::child(read)))
            }
            PackStep::Open => {
                let blob = result.child::<Blob, GitError>()??;
                self.step = PackStep::Collect;
                Ok(Next::Call(collect_call(blob, self.pack.size)))
            }
            PackStep::Collect => {
                let bytes = collected(result)?;
                // A longer stream was cut at the size, so it fails this check too.
                if objects::describe_pack(&bytes).ok().as_ref() != Some(&self.pack) {
                    return Err(GitError::Invalid);
                }
                let record = publish::pack_record(&self.owner, bytes.clone());
                self.bytes = Some(bytes);
                self.step = PackStep::Publish;
                let stored = publish::publish_pack(self.node, &self.document, record);
                Ok(Next::Call(IoCall::child(stored)))
            }
            PackStep::Publish => {
                result.child::<(), GitError>()??;
                Ok(Next::Done(self.bytes.take().ok_or(IoError::Unfinished)?))
            }
        }
    }
}

/// Opens exact bytes: this node's copy, the recording node's version, or any holder's copy.
pub(super) fn open(
    node: Option<NodeId>,
    auth: &AuthContext,
    document: &Document,
    object: &StoredObject,
    holders: &[NodeId],
) -> OpenBlobOperation {
    OpenBlobOperation {
        node,
        auth: auth.clone(),
        document: document.clone(),
        object: object.clone(),
        holders: holders.to_vec(),
        sources: VecDeque::new(),
        local: false,
        copied: false,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct OpenBlobOperation {
    node: Option<NodeId>,
    auth: AuthContext,
    document: Document,
    object: StoredObject,
    holders: Vec<NodeId>,
    sources: VecDeque<(NodeId, BaoReadTarget)>,
    local: bool,
    copied: bool,
}

type OpenTurn = Result<Next<Blob>, GitError>;

impl OpenBlobOperation {
    fn read_local(&mut self, own: &StoredObject, node: NodeId) -> OpenTurn {
        let input = GetObjectInput {
            bucket: own.bucket.clone(),
            key: own.key.clone(),
            version_id: Some(own.version_id),
            range: None,
            group_id: own.group_id.ok_or(GitError::Unavailable)?,
            user_identity: self.auth.user_id,
            realm_id: self.auth.realm_id,
            node_id: node,
        };
        self.local = true;
        let restrictions = self.auth.path_restrictions.clone();
        let read = GetRoutedOperation::new(input, restrictions, None);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next_source(&mut self) -> OpenTurn {
        let (source, target) = self.sources.pop_front().ok_or(GitError::Unavailable)?;
        let request = BaoReadRequest {
            auth_context: self.auth.clone(),
            realm_id: self.auth.realm_id,
            target,
            expected_blake3: Some(self.object.blake3),
            metadata_only: false,
            destination: None,
            known_refs: Vec::new(),
            range: None,
        };
        let read = ManagedReadOperation::new(source, request);
        Ok(Next::Call(IoCall::child(read)))
    }
}

impl Operation for OpenBlobOperation {
    type Output = Blob;
    type Error = GitError;

    fn first(&mut self) -> OpenTurn {
        self.node.ok_or(GitError::Unavailable)?;
        let copy = objects::copy(&self.document, &self.object.sha256);
        Ok(Next::Call(IoCall::child(copy)))
    }

    fn next(&mut self, result: IoResult) -> OpenTurn {
        let node = self.node.ok_or(GitError::Unavailable)?;
        if !self.copied {
            self.copied = true;
            let own = result.child::<Option<StoredObject>, GitError>()??;
            let here = (self.object.node_id == node).then(|| self.object.clone());
            if let Some(own) = own.or(here) {
                return self.read_local(&own, node);
            }
            let object = &self.object;
            let exact = BaoReadTarget::ExactVersion(VersionedObjectArn {
                realm_id: self.auth.realm_id,
                node_id: object.node_id,
                bucket: object.bucket.clone(),
                key: object.key.clone(),
                version: object.version_id,
            });
            let others = (self.holders.iter())
                .filter(|holder| **holder != node && **holder != object.node_id)
                .map(|holder| (*holder, BaoReadTarget::Blake3(object.blake3)));
            self.sources = std::iter::once((object.node_id, exact))
                .chain(others)
                .collect();
            return self.next_source();
        }
        if self.local {
            let read = result.child::<GetObjectResult, GetObjectError>()?;
            return Ok(Next::Done(read.map_err(|_| GitError::Unavailable)?.blob));
        }
        match result.child::<BaoReadOutput, BaoReadError>()? {
            Ok(BaoReadOutput::Stream { blob, size, .. }) if size == self.object.size => {
                Ok(Next::Done(blob))
            }
            _ => self.next_source(),
        }
    }
}

/// Resolves a distributed version within the resolver's budget.
pub(super) fn version_call(ids: NodeRealm, version_id: Ulid) -> IoCall {
    let resolve = child_member(resolve_bounded(ids, version_id));
    group_call(vec![resolve], 1, Some(RESOLVE_BUDGET))
}

/// What a [`version_call`] found; a run that did not finish finds nothing.
pub(super) fn version_answer(result: IoResult) -> Result<Option<ResolvedVersion>, GitError> {
    let answer = (group_results(result)?.pop())
        .and_then(member_outcome::<GlobalResolution, IoError>)
        .unwrap_or(Err(IoError::Unfinished));
    Ok(version_found(answer))
}

/// What the pending call of a download answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DownloadStep {
    Repository,
    Locate,
    Permit,
    Copy,
    Holders,
    SetHolders,
    Open,
}

/// Streams LFS content after authorizing the original object. Content held elsewhere is
/// relayed from its node and not stored here.
pub fn download(ids: NodeRealm, auth: &AuthContext, id: Ulid, oid: &str) -> DownloadOperation {
    DownloadOperation {
        ids,
        auth: auth.clone(),
        id,
        oid: oid.to_string(),
        step: DownloadStep::Repository,
        document: None,
        original: None,
        holders: Vec::new(),
        size: 0,
    }
}

#[derive(Debug, PartialEq)]
pub struct DownloadOperation {
    ids: NodeRealm,
    auth: AuthContext,
    id: Ulid,
    oid: String,
    step: DownloadStep,
    document: Option<Document>,
    original: Option<StoredObject>,
    holders: Vec<NodeId>,
    size: u64,
}

type DownloadTurn = Result<Next<(Blob, u64)>, GitError>;

impl DownloadOperation {
    fn node(&self) -> Option<NodeId> {
        self.ids.map(|(node_id, _)| node_id)
    }

    fn document(&self) -> Result<&Document, IoError> {
        self.document.as_ref().ok_or(IoError::Unfinished)
    }

    fn original(&self) -> Result<&StoredObject, IoError> {
        self.original.as_ref().ok_or(IoError::Unfinished)
    }

    fn open_object(&mut self, object: &StoredObject, holders: &[NodeId]) -> DownloadTurn {
        self.size = object.size;
        self.step = DownloadStep::Open;
        let read = open(self.node(), &self.auth, self.document()?, object, holders);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn open_original(&mut self) -> DownloadTurn {
        let original = self.original()?.clone();
        let holders = std::mem::take(&mut self.holders);
        self.open_object(&original, &holders)
    }
}

impl Operation for DownloadOperation {
    type Output = (Blob, u64);
    type Error = GitError;

    fn first(&mut self) -> DownloadTurn {
        let read = repository(self.node(), &self.auth, self.id, Permission::READ);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn next(&mut self, result: IoResult) -> DownloadTurn {
        match self.step {
            DownloadStep::Repository => {
                let (document, _) = result.child::<Repository, GitError>()??;
                let find = locate(&document, &self.oid);
                self.document = Some(document);
                self.step = DownloadStep::Locate;
                Ok(Next::Call(IoCall::child(find)))
            }
            DownloadStep::Locate => {
                let original = result.child::<Option<StoredObject>, GitError>()??;
                let original = original.ok_or(GitError::NotFound)?;
                let permit = PermitObjectOperation::new(&self.auth, &original, Permission::READ);
                self.original = Some(original);
                self.step = DownloadStep::Permit;
                Ok(Next::Call(IoCall::child(permit)))
            }
            DownloadStep::Permit => {
                result.child::<(), GitError>()??;
                self.node().ok_or(GitError::Unavailable)?;
                self.step = DownloadStep::Copy;
                let copy = objects::copy(self.document()?, &self.oid);
                Ok(Next::Call(IoCall::child(copy)))
            }
            DownloadStep::Copy => {
                let node = self.node().ok_or(GitError::Unavailable)?;
                let local = match result.child::<Option<StoredObject>, GitError>()?? {
                    Some(copy) => copy,
                    None if self.original()?.node_id == node => self.original()?.clone(),
                    None => {
                        self.step = DownloadStep::Holders;
                        let holders = publish::holders(Some(node), self.document()?);
                        return Ok(Next::Call(IoCall::child(holders)));
                    }
                };
                self.open_object(&local, &[])
            }
            DownloadStep::Holders => {
                self.holders = result.child::<Vec<NodeId>, GitError>()??;
                // The members that keep a pinned distributed version hold it too.
                match &self.original()?.global {
                    Some(GlobalId::Version { version_id, .. }) => {
                        let call = version_call(self.ids, *version_id);
                        self.step = DownloadStep::SetHolders;
                        Ok(Next::Call(call))
                    }
                    _ => self.open_original(),
                }
            }
            DownloadStep::SetHolders => {
                if let Some(resolved) = version_answer(result)? {
                    self.holders.extend(resolved.holders);
                }
                self.open_original()
            }
            DownloadStep::Open => {
                let blob = result.child::<Blob, GitError>()??;
                Ok(Next::Done((blob, self.size)))
            }
        }
    }
}

#[cfg(test)]
#[path = "download_tests.rs"]
mod tests;
