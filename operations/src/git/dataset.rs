//! Stores the data files of plain RO-Crate pushes in the dataset's storage location.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use aruna_core::NodeId;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{
    FileChangeKind, GitEffect, GitEvent, LfsObject, PendingMerge, StoredObject, ZERO_OID,
};
use aruna_core::operation::Operation;
use aruna_core::repo_layout::{Layout, StoredFile, name_stored, unstored_paths};
use aruna_core::stream::{BackendStream, StreamError};
use aruna_core::structs::checksum::Hasher;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::storage::blob::{BucketInfo, object_permission_path};
use aruna_core::structs::storage::data::identity::ObjectLocation;
use aruna_core::structs::storage::dataset_location::DatasetLocation;
use aruna_core::structs::storage::metadata_registry::MetadataRegistryRecord;
use aruna_core::types::GroupId;
use bytes::Bytes;
use serde_json::Value;
use tracing::warn;

use super::calls::{git_call, git_event};
use super::objects::{self, Blob};
use super::project::author;
use super::{GitError, GitNode, download, lfs, location, publish};
use crate::auth::error::AuthorizeError;
use crate::auth::request_authorization::AuthorizeOperation;
use crate::auth::request_policy::PolicyRequestExtras;
use crate::document_repository::{GetConfigError, GetConfigOperation};
use crate::s3::object::copy::{
    CopyObjectError, CopyObjectInput, CopyObjectOperation, CopyReferences, CopyResultData,
    CopySourceConditions,
};

type Document = MetadataRegistryRecord;

/// Pushed files are stored in the realm that holds the document.
fn pusher(document: &Document, merge: &PendingMerge) -> AuthContext {
    AuthContext {
        realm_id: document.realm_id,
        ..author(merge.user_id)
    }
}

/// What a push to main stores: the location, its bucket here, and the changed files.
#[derive(Debug, PartialEq)]
pub(super) struct PushTarget {
    location: DatasetLocation,
    bucket: Option<BucketInfo>,
    changed: BTreeSet<String>,
    /// Files stored so far, reused when the metadata update is retried.
    stored: BTreeMap<String, StoredFile>,
}

impl PushTarget {
    /// The location of this push, for matching pushed paths to stored entities.
    pub(super) fn location(&self) -> &DatasetLocation {
        &self.location
    }
}

/// What the pending call of finding a push target answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TargetStep {
    Layout,
    Location,
    Bucket,
    Diff,
}

/// The storage target of a plain RO-Crate push, or `None` for an ARC push. A location whose
/// bucket is missing here or not writable leaves the files unstored until a later push.
pub(super) fn target(
    node: &GitNode,
    document: &Document,
    merge: &PendingMerge,
) -> PushTargetOperation {
    PushTargetOperation {
        node: node.node(),
        document: document.clone(),
        merge: merge.clone(),
        step: TargetStep::Layout,
        location: None,
        bucket: None,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct PushTargetOperation {
    node: Option<NodeId>,
    document: Document,
    merge: PendingMerge,
    step: TargetStep,
    location: Option<DatasetLocation>,
    bucket: Option<BucketInfo>,
}

type TargetTurn = Result<Next<Option<PushTarget>>, GitError>;

impl Operation for PushTargetOperation {
    type Output = Option<PushTarget>;
    type Error = GitError;

    fn first(&mut self) -> TargetTurn {
        let layout = GitEffect::Layout {
            document_id: self.document.document_id,
            revision: self.merge.new.clone(),
        };
        Ok(Next::Call(git_call(layout, self.merge.user_id)))
    }

    fn next(&mut self, result: IoResult) -> TargetTurn {
        match self.step {
            TargetStep::Layout => {
                if !matches!(git_event(result)?, GitEvent::Layout(Some(Layout::RoCrate))) {
                    return Ok(Next::Done(None));
                }
                self.step = TargetStep::Location;
                let effective = location::effective(&self.document);
                Ok(Next::Call(IoCall::child(effective)))
            }
            TargetStep::Location => {
                let (chosen, _) = result.child::<(DatasetLocation, bool), GitError>()??;
                let auth = pusher(&self.document, &self.merge);
                let check = location::writable(self.node, &auth, &self.document, &chosen);
                self.location = Some(chosen);
                self.step = TargetStep::Bucket;
                Ok(Next::Call(IoCall::child(check)))
            }
            TargetStep::Bucket => {
                self.bucket = match result.child::<BucketInfo, GitError>()? {
                    Ok(bucket) => Some(bucket),
                    Err(error) => {
                        warn!(document_id = %self.document.document_id, %error,
                            "Pushed files stay unstored; the storage location is not writable here");
                        None
                    }
                };
                let merge = &self.merge;
                let diff = GitEffect::Diff {
                    document_id: self.document.document_id,
                    from: (merge.old != ZERO_OID).then(|| merge.old.clone()),
                    to: merge.new.clone(),
                };
                self.step = TargetStep::Diff;
                Ok(Next::Call(git_call(diff, merge.user_id)))
            }
            TargetStep::Diff => {
                let GitEvent::Diff(changes) = git_event(result)? else {
                    return Err(GitError::Unavailable);
                };
                let changed = (changes.into_iter())
                    .filter(|change| change.change != FileChangeKind::Deleted)
                    .map(|change| change.path)
                    .collect();
                Ok(Next::Done(Some(PushTarget {
                    location: self.location.take().ok_or(IoError::Unfinished)?,
                    bucket: self.bucket.take(),
                    changed,
                    stored: BTreeMap::new(),
                })))
            }
        }
    }
}

/// The target with the files stored so far, and the rewritten graph when anything changed.
pub(super) type StoredFiles = (PushTarget, Option<String>);

/// Stores every file the merged graph still needs and names the entities by content.
pub(super) fn store_files(
    node: &GitNode,
    document: &Document,
    merge: &PendingMerge,
    target: PushTarget,
    jsonld: &str,
) -> StoreFilesOperation {
    StoreFilesOperation {
        node: node.clone(),
        document: document.clone(),
        merge: merge.clone(),
        target: Some(target),
        jsonld: jsonld.to_string(),
        value: Value::Null,
        paths: VecDeque::new(),
        path: None,
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct StoreFilesOperation {
    node: GitNode,
    document: Document,
    merge: PendingMerge,
    target: Option<PushTarget>,
    jsonld: String,
    value: Value,
    paths: VecDeque<String>,
    path: Option<String>,
}

type FilesTurn = Result<Next<StoredFiles>, GitError>;

impl StoreFilesOperation {
    fn target(&self) -> Result<&PushTarget, IoError> {
        self.target.as_ref().ok_or(IoError::Unfinished)
    }

    fn next_path(&mut self) -> FilesTurn {
        while let Some(path) = self.paths.pop_front() {
            let target = self.target()?;
            if target.stored.contains_key(&path) {
                continue;
            }
            let bucket = target.bucket.clone().ok_or(IoError::Unfinished)?;
            let scope = (&self.document, &self.merge, &target.location);
            let store = store_file(&self.node, scope, bucket, &path);
            self.path = Some(path);
            return Ok(Next::Call(IoCall::child(store)));
        }
        let target = self.target.take().ok_or(IoError::Unfinished)?;
        let named = name_stored(&mut self.value, &target.stored);
        let graph = named.then(|| self.value.to_string());
        Ok(Next::Done((target, graph)))
    }
}

impl Operation for StoreFilesOperation {
    type Output = StoredFiles;
    type Error = GitError;

    fn first(&mut self) -> FilesTurn {
        let target = self.target()?;
        if target.bucket.is_none() {
            let target = self.target.take().ok_or(IoError::Unfinished)?;
            return Ok(Next::Done((target, None)));
        }
        let value: Value = serde_json::from_str(&self.jsonld).map_err(|_| GitError::Invalid)?;
        self.paths = unstored_paths(&value, &target.changed)
            .into_iter()
            .collect();
        self.value = value;
        self.next_path()
    }

    fn next(&mut self, result: IoResult) -> FilesTurn {
        let file = result.child::<Option<StoredFile>, GitError>()??;
        let path = self.path.take().ok_or(IoError::Unfinished)?;
        if let Some(file) = file {
            let target = self.target.as_mut().ok_or(IoError::Unfinished)?;
            target.stored.insert(path, file);
        }
        self.next_path()
    }
}

/// The bytes of a pushed file: a plain Git blob, or the LFS object its pointer names.
#[derive(Debug, PartialEq)]
enum Content {
    Blob(Bytes),
    Lfs(StoredObject),
}

impl Content {
    fn sha256(&self) -> String {
        match self {
            Self::Blob(bytes) => Hasher::new_with_bytes(bytes)
                .to_map()
                .get("sha256")
                .map(hex::encode)
                .unwrap_or_default(),
            Self::Lfs(object) => object.sha256.clone(),
        }
    }
}

/// What the pending call of storing one file answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileStep {
    Read,
    Authorize,
    Locate,
    Existing,
    Write,
}

/// Stores one repository file at its key; `None` when the commit has no such file or its LFS
/// content is unknown here.
fn store_file(
    node: &GitNode,
    (document, merge, location): (&Document, &PendingMerge, &DatasetLocation),
    bucket: BucketInfo,
    path: &str,
) -> StoreFileOperation {
    StoreFileOperation {
        node: node.clone(),
        document: document.clone(),
        revision: merge.new.clone(),
        auth: pusher(document, merge),
        name: location.bucket.to_string(),
        key: location.key(path),
        path: path.to_string(),
        bucket,
        step: FileStep::Read,
        file: None,
        content: None,
    }
}

#[derive(Debug, PartialEq)]
struct StoreFileOperation {
    node: GitNode,
    document: Document,
    revision: String,
    auth: AuthContext,
    name: String,
    key: String,
    path: String,
    bucket: BucketInfo,
    step: FileStep,
    file: Option<Bytes>,
    content: Option<Content>,
}

type FileTurn = Result<Next<Option<StoredFile>>, GitError>;

impl StoreFileOperation {
    fn find_existing(&mut self, content: Content) -> FileTurn {
        self.content = Some(content);
        self.step = FileStep::Existing;
        let scope = (self.name.as_str(), self.bucket.group_id);
        let read = objects::described(self.node.node(), scope, &self.key, None);
        Ok(Next::Call(IoCall::child(read)))
    }

    fn finish(stored: StoredObject) -> FileTurn {
        Ok(Next::Done(Some(StoredFile {
            hash: stored.blake3,
            location: ObjectLocation {
                bucket: stored.bucket,
                key: stored.key,
            },
            size: stored.size,
        })))
    }
}

impl Operation for StoreFileOperation {
    type Output = Option<StoredFile>;
    type Error = GitError;

    fn first(&mut self) -> FileTurn {
        let read = GitEffect::ReadFile {
            document_id: self.document.document_id,
            revision: self.revision.clone(),
            path: self.path.clone(),
        };
        Ok(Next::Call(git_call(read, self.auth.user_id)))
    }

    fn next(&mut self, result: IoResult) -> FileTurn {
        match self.step {
            FileStep::Read => {
                let GitEvent::File(bytes) = git_event(result)? else {
                    return Err(GitError::Unavailable);
                };
                let Some(bytes) = bytes else {
                    return Ok(Next::Done(None));
                };
                self.file = Some(bytes);
                let auth = &self.auth;
                let node = self.node.node().ok_or(GitError::Unavailable)?;
                let group_id = self.bucket.group_id;
                let target =
                    object_permission_path(auth.realm_id, group_id, node, &self.name, &self.key);
                let extras = PolicyRequestExtras::operation("s3.PutObject");
                let check = AuthorizeOperation::new(
                    auth.realm_id,
                    auth,
                    &target,
                    &Permission::WRITE,
                    extras,
                );
                self.step = FileStep::Authorize;
                Ok(Next::Call(IoCall::child(check)))
            }
            FileStep::Authorize => {
                result.child::<(), AuthorizeError>()??;
                let bytes = self.file.take().ok_or(IoError::Unfinished)?;
                match LfsObject::from_pointer(&bytes) {
                    Some(pointer) => {
                        self.step = FileStep::Locate;
                        let find = lfs::locate(&self.document, &pointer.oid);
                        Ok(Next::Call(IoCall::child(find)))
                    }
                    None => self.find_existing(Content::Blob(bytes)),
                }
            }
            FileStep::Locate => match result.child::<Option<StoredObject>, GitError>()?? {
                Some(source) => self.find_existing(Content::Lfs(source)),
                None => Ok(Next::Done(None)),
            },
            FileStep::Existing => {
                let existing = result.child::<Option<StoredObject>, GitError>()??;
                let content = self.content.take().ok_or(IoError::Unfinished)?;
                let sha256 = content.sha256();
                if let Some(existing) = existing.filter(|existing| existing.sha256 == sha256) {
                    return Self::finish(existing);
                }
                let write = write_content(self, content, sha256);
                self.step = FileStep::Write;
                Ok(Next::Call(IoCall::child(write)))
            }
            FileStep::Write => Self::finish(result.child::<StoredObject, GitError>()??),
        }
    }
}

/// What the pending call of writing a file's content answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteStep {
    Start,
    Copy,
    Copied,
    Holders,
    Open,
    Put,
}

/// Copies LFS content held here on the server; other content streams through a put.
fn write_content(
    file: &StoreFileOperation,
    content: Content,
    sha256: String,
) -> WriteContentOperation {
    WriteContentOperation {
        node: file.node.clone(),
        auth: file.auth.clone(),
        document: file.document.clone(),
        name: file.name.clone(),
        bucket: Some(file.bucket.clone()),
        key: file.key.clone(),
        sha256,
        content: Some(content),
        source: None,
        step: WriteStep::Start,
    }
}

#[derive(Debug, PartialEq)]
struct WriteContentOperation {
    node: GitNode,
    auth: AuthContext,
    document: Document,
    name: String,
    bucket: Option<BucketInfo>,
    key: String,
    sha256: String,
    content: Option<Content>,
    source: Option<StoredObject>,
    step: WriteStep,
}

type WriteTurn = Result<Next<StoredObject>, GitError>;

impl WriteContentOperation {
    fn source(&self) -> Result<&StoredObject, IoError> {
        self.source.as_ref().ok_or(IoError::Unfinished)
    }

    fn put(&mut self, size: u64, body: Blob) -> WriteTurn {
        let bucket = self.bucket.take().ok_or(IoError::Unfinished)?;
        let put = objects::put(
            (self.node.node(), &self.node.routing),
            &self.auth,
            (self.name.as_str(), bucket),
            (std::mem::take(&mut self.key), size, self.sha256.as_str()),
            body,
        );
        self.step = WriteStep::Put;
        Ok(Next::Call(IoCall::child(put)))
    }
}

impl Operation for WriteContentOperation {
    type Output = StoredObject;
    type Error = GitError;

    fn first(&mut self) -> WriteTurn {
        match self.content.take().ok_or(IoError::Unfinished)? {
            Content::Blob(bytes) => {
                let size = bytes.len() as u64;
                let chunks = futures_util::stream::iter([Ok::<_, StreamError>(bytes)]);
                self.put(size, BackendStream::new(chunks))
            }
            Content::Lfs(source) => {
                let copy = objects::copy(&self.document, &source.sha256);
                self.source = Some(source);
                self.step = WriteStep::Copy;
                Ok(Next::Call(IoCall::child(copy)))
            }
        }
    }

    fn next(&mut self, result: IoResult) -> WriteTurn {
        match self.step {
            WriteStep::Start => Err(IoError::Unfinished.into()),
            WriteStep::Copy => {
                let own = result.child::<Option<StoredObject>, GitError>()??;
                let source = self.source()?;
                let here = (Some(source.node_id) == self.node.node()).then(|| source.clone());
                if let Some(local) = own.or(here)
                    && let Some(group_id) = local.group_id
                {
                    let bucket = self.bucket.take().ok_or(IoError::Unfinished)?;
                    let copy = CopiedOperation {
                        routing: self.node.routing.clone(),
                        node: self.node.node(),
                        auth: self.auth.clone(),
                        source: local,
                        source_group: group_id,
                        name: self.name.clone(),
                        group_id: bucket.group_id,
                        key: std::mem::take(&mut self.key),
                        step: CopyStep::Authorize,
                    };
                    self.step = WriteStep::Copied;
                    return Ok(Next::Call(IoCall::child(copy)));
                }
                let holders = publish::holders(self.node.node(), &self.document);
                self.step = WriteStep::Holders;
                Ok(Next::Call(IoCall::child(holders)))
            }
            WriteStep::Holders => {
                let holders = result.child::<Vec<NodeId>, GitError>()??;
                let source = self.source()?;
                let open = download::open(
                    self.node.node(),
                    &self.auth,
                    &self.document,
                    source,
                    &holders,
                );
                self.step = WriteStep::Open;
                Ok(Next::Call(IoCall::child(open)))
            }
            WriteStep::Open => {
                let blob = result.child::<Blob, GitError>()??;
                let size = self.source()?.size;
                self.put(size, blob)
            }
            WriteStep::Copied | WriteStep::Put => {
                Ok(Next::Done(result.child::<StoredObject, GitError>()??))
            }
        }
    }
}

/// What the pending call of a server-side copy answers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CopyStep {
    Authorize,
    Config,
    Copy,
    Describe,
}

/// A server-side copy of an object of this node, authorized as a read of the source.
#[derive(Debug, PartialEq)]
struct CopiedOperation {
    routing: aruna_core::structs::storage::routing::NodeRouting,
    node: Option<NodeId>,
    auth: AuthContext,
    source: StoredObject,
    source_group: GroupId,
    name: String,
    group_id: GroupId,
    key: String,
    step: CopyStep,
}

impl Operation for CopiedOperation {
    type Output = StoredObject;
    type Error = GitError;

    fn first(&mut self) -> WriteTurn {
        let (auth, source) = (&self.auth, &self.source);
        let path = object_permission_path(
            auth.realm_id,
            self.source_group,
            source.node_id,
            &source.bucket,
            &source.key,
        );
        let extras = PolicyRequestExtras::operation("s3.GetObject");
        let check = AuthorizeOperation::new(auth.realm_id, auth, &path, &Permission::READ, extras);
        Ok(Next::Call(IoCall::child(check)))
    }

    fn next(&mut self, result: IoResult) -> WriteTurn {
        match self.step {
            CopyStep::Authorize => {
                result.child::<(), AuthorizeError>()??;
                self.step = CopyStep::Config;
                let config = GetConfigOperation::new(self.auth.realm_id);
                Ok(Next::Call(IoCall::child(config)))
            }
            CopyStep::Config => {
                let config = result.child::<RealmConfigDocument, GetConfigError>()?;
                let config = config.map_err(|_| GitError::Unavailable)?;
                let (auth, source) = (&self.auth, &self.source);
                let input = CopyObjectInput {
                    source_bucket: source.bucket.clone(),
                    source_key: source.key.clone(),
                    source_version_id: Some(source.version_id),
                    source_group_id: self.source_group,
                    source_auth_context: auth.clone(),
                    dest_bucket: self.name.clone(),
                    dest_key: self.key.clone(),
                    user_id: auth.user_id,
                    group_id: self.group_id,
                    realm_id: auth.realm_id,
                    node_id: source.node_id,
                    quota_ceiling: config.quota.effective_group_ceiling(&self.group_id),
                    conditions: CopySourceConditions::default(),
                    metadata: None,
                    restrictions: auth.path_restrictions.clone(),
                    references: CopyReferences::Materialize,
                    job_id: None,
                };
                self.step = CopyStep::Copy;
                let copy = CopyObjectOperation::new(input, self.routing.clone());
                Ok(Next::Call(IoCall::child(copy)))
            }
            CopyStep::Copy => {
                let copied = result.child::<CopyResultData, CopyObjectError>()?;
                let copied = copied.map_err(|_| GitError::Unavailable)?;
                self.step = CopyStep::Describe;
                let read = objects::described(
                    self.node,
                    (self.name.as_str(), self.group_id),
                    &self.key,
                    Some(copied.version_id),
                );
                Ok(Next::Call(IoCall::child(read)))
            }
            CopyStep::Describe => {
                let stored = result.child::<Option<StoredObject>, GitError>()??;
                Ok(Next::Done(stored.ok_or(GitError::Unavailable)?))
            }
        }
    }
}

#[cfg(test)]
#[path = "dataset_tests.rs"]
mod tests;
