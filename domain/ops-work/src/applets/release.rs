//! Releases static files as a new private applet version: copies them into the held bucket as
//! pinned objects, writes the version manifest and adds the version to the record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::manifest::{
    LOCKFILE, MAX_SITE_BYTES, ManifestFile, SOURCE_MAP, VersionManifest, content_type,
    lockfile_packages,
};
use aruna_core::applet::record::{Applet, PinnedObject, VersionRef, VersionState, held_bucket};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitNode, StoredObject};
use aruna_core::operation::Operation;
use aruna_core::stream::BackendStream;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_ops_data::git::GitError;
use aruna_ops_data::git::commit::{Tree, read_tree};
use aruna_ops_data::git::objects::{EnsureBucketOperation, put};
use bytes::Bytes;
use sha2::Digest;
use ulid::Ulid;

use super::change::{AppletChange, WriteChangeOperation};
use super::{AppletAccessOperation, AppletError, Need};

/// Where the files of a release come from.
#[derive(Clone, Debug, PartialEq)]
pub enum Source {
    /// The applet repository's `revision`, below the folder `root`.
    Commit { revision: String, root: String },
    /// Files the transport already holds, such as a builder job's captured outputs.
    Files {
        commit: Option<String>,
        files: Vec<(String, Bytes)>,
    },
}

#[derive(Debug, PartialEq)]
enum ReleaseStep {
    Access,
    Tree,
    Bucket,
    File,
    Manifest,
    Record,
}

/// Copies a release into the held bucket of the applet's host node as version `next_version`.
/// The caller needs WRITE on the group's data and on the held bucket.
#[derive(Debug, PartialEq)]
pub struct ReleaseOperation {
    node: GitNode,
    auth: AuthContext,
    applet_id: Ulid,
    source: Option<Source>,
    applet: Option<Applet>,
    bucket: Option<BucketInfo>,
    commit: Option<String>,
    pending: Vec<(String, Bytes)>,
    lockfile: Option<Bytes>,
    stored: Vec<(String, Option<StoredObject>)>,
    version: u32,
    now_ms: u64,
    step: ReleaseStep,
}

impl ReleaseOperation {
    pub fn new(node: &GitNode, auth: &AuthContext, applet_id: Ulid, source: Source) -> Self {
        Self {
            node: node.clone(),
            auth: auth.clone(),
            applet_id,
            source: Some(source),
            applet: None,
            bucket: None,
            commit: None,
            pending: Vec::new(),
            lockfile: None,
            stored: Vec::new(),
            version: 0,
            now_ms: 0,
            step: ReleaseStep::Access,
        }
    }

    fn applet(&self) -> Result<&Applet, AppletError> {
        self.applet
            .as_ref()
            .ok_or(AppletError::from(IoError::Unfinished))
    }

    /// Keeps the served files in path order and checks their size before anything is written.
    fn files(&mut self, files: Vec<(String, Bytes)>) -> Result<Next<VersionRef>, AppletError> {
        let total: u64 = files.iter().map(|(_, bytes)| bytes.len() as u64).sum();
        if total > MAX_SITE_BYTES {
            return Err(AppletError::Invalid(format!(
                "a version holds at most {MAX_SITE_BYTES} bytes"
            )));
        }
        let (lockfile, mut served): (Vec<_>, Vec<_>) =
            files.into_iter().partition(|(path, _)| path == LOCKFILE);
        self.lockfile = lockfile.into_iter().next().map(|(_, bytes)| bytes);
        if !served.iter().any(|(path, _)| path == "index.html") {
            return Err(AppletError::Invalid(
                "a version holds no index.html at its root".into(),
            ));
        }
        served.sort_by(|left, right| right.0.cmp(&left.0));
        self.pending = served;
        let applet = self.applet()?;
        let (group, node) = (applet.group_id, applet.host_node);
        let ensure = EnsureBucketOperation::named(
            held_bucket(group, node),
            group,
            self.auth.user_id,
            self.now_ms,
        );
        self.step = ReleaseStep::Bucket;
        Ok(Next::Call(IoCall::child(ensure)))
    }

    fn put_call(&self, key: String, bytes: Bytes) -> Result<IoCall, AppletError> {
        let applet = self.applet()?;
        let bucket = self
            .bucket
            .clone()
            .ok_or(AppletError::from(IoError::Unfinished))?;
        let held = (applet, bucket);
        Ok(put_held((&self.node, &self.auth), held, key, bytes))
    }

    fn key(&self, path: &str) -> String {
        format!("{}/{}/{path}", self.applet_id, self.version)
    }

    fn next_file(&mut self) -> Result<Next<VersionRef>, AppletError> {
        let Some((path, bytes)) = self.pending.pop() else {
            return self.manifest();
        };
        let call = self.put_call(self.key(&path), bytes)?;
        self.stored.push((path, None));
        self.step = ReleaseStep::File;
        Ok(Next::Call(call))
    }

    fn manifest(&mut self) -> Result<Next<VersionRef>, AppletError> {
        let packages = match &self.lockfile {
            Some(bytes) => {
                lockfile_packages(bytes).map_err(|error| AppletError::Invalid(error.to_string()))?
            }
            None => Vec::new(),
        };
        let manifest = VersionManifest {
            applet_id: self.applet_id,
            version: self.version,
            commit: self.commit.clone(),
            lockfile_blake3: self
                .lockfile
                .as_ref()
                .map(|bytes| blake3::hash(bytes).to_hex().to_string()),
            packages,
            files: (self.stored.iter())
                .filter(|(path, _)| path != SOURCE_MAP)
                .filter_map(|(path, object)| Some((path, object.as_ref()?)))
                .map(|(path, object)| ManifestFile {
                    path: path.clone(),
                    node: object.node_id,
                    bucket: object.bucket.clone(),
                    key: object.key.clone(),
                    version_id: object.version_id.to_string(),
                    blake3: hex::encode(object.blake3),
                    content_type: content_type(path).to_string(),
                    size_bytes: object.size,
                })
                .collect(),
            // Only preview sessions read the source map, so it is never a served file.
            source_map: (self.stored.iter())
                .find(|(path, _)| path == SOURCE_MAP)
                .and_then(|(_, object)| object.as_ref())
                .map(pinned),
        }
        .checked()
        .map_err(|error| AppletError::Invalid(error.to_string()))?;
        let bytes = Bytes::from(manifest.to_bytes()?);
        let call = self.put_call(self.key(".aruna/manifest.json"), bytes)?;
        self.step = ReleaseStep::Manifest;
        Ok(Next::Call(call))
    }
}

/// The exact object version `stored` names.
pub(crate) fn pinned(stored: &StoredObject) -> PinnedObject {
    PinnedObject {
        node: stored.node_id,
        bucket: stored.bucket.clone(),
        key: stored.key.clone(),
        version_id: stored.version_id.to_string(),
        blake3: hex::encode(stored.blake3),
    }
}

/// The child that writes `bytes` at `key` of the held bucket of `applet` on its host node.
pub(crate) fn put_held(
    (node, auth): (&GitNode, &AuthContext),
    (applet, bucket): (&Applet, BucketInfo),
    key: String,
    bytes: Bytes,
) -> IoCall {
    let name = held_bucket(applet.group_id, applet.host_node);
    put_named(node, auth, (&name, bucket), key, bytes)
}

/// The child that writes `bytes` at `key` of the bucket `name` on this node; callers authorize.
pub(crate) fn put_named(
    node: &GitNode,
    auth: &AuthContext,
    (name, bucket): (&str, BucketInfo),
    key: String,
    bytes: Bytes,
) -> IoCall {
    let sha256 = hex::encode(sha2::Sha256::digest(&bytes));
    let size = bytes.len() as u64;
    let body = BackendStream::once(bytes);
    let scope = (node.node(), &node.routing);
    let write = put(scope, auth, (name, bucket), (key, size, &sha256), body);
    IoCall::child(write)
}

pub(crate) fn git_error(error: GitError) -> AppletError {
    match error {
        GitError::NotFound | GitError::BranchMissing => {
            AppletError::Invalid("the revision or folder does not exist".into())
        }
        GitError::Authorization(_) => AppletError::Forbidden,
        GitError::Refused(reason) => AppletError::Invalid(reason),
        other => AppletError::Storage(other.to_string()),
    }
}

impl Operation for ReleaseOperation {
    type Output = VersionRef;
    type Error = AppletError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<VersionRef>, AppletError> {
        let access = AppletAccessOperation::new(&self.auth, self.applet_id, Need::Publish);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<VersionRef>, AppletError> {
        match self.step {
            ReleaseStep::Access => {
                let applet = result.child::<Applet, AppletError>()??;
                if Some(applet.host_node) != self.node.node() {
                    return Err(AppletError::Elsewhere(applet.host_node));
                }
                self.version = applet.next_version();
                let repository = applet.repository;
                self.applet = Some(applet);
                match self
                    .source
                    .take()
                    .ok_or(AppletError::from(IoError::Unfinished))?
                {
                    Source::Files { commit, files } => {
                        self.commit = commit;
                        self.files(files)
                    }
                    Source::Commit { revision, root } => {
                        let repository = repository.ok_or_else(|| {
                            AppletError::Invalid("the applet has no repository".into())
                        })?;
                        let read = read_tree(
                            &self.node,
                            &self.auth,
                            (repository, &revision),
                            &root,
                            Some(MAX_SITE_BYTES),
                        );
                        self.step = ReleaseStep::Tree;
                        Ok(Next::Call(IoCall::child(read)))
                    }
                }
            }
            ReleaseStep::Tree => {
                let tree = result.child::<Tree, GitError>()?.map_err(git_error)?;
                self.commit = Some(tree.commit);
                let files = (tree.files.into_iter())
                    .filter_map(|(path, bytes)| Some((path, bytes?)))
                    .collect();
                self.files(files)
            }
            ReleaseStep::Bucket => {
                let bucket = result.child::<BucketInfo, GitError>()?.map_err(git_error)?;
                self.bucket = Some(bucket);
                self.next_file()
            }
            ReleaseStep::File => {
                let stored = result
                    .child::<StoredObject, GitError>()?
                    .map_err(git_error)?;
                let entry = self
                    .stored
                    .last_mut()
                    .ok_or(AppletError::from(IoError::Unfinished))?;
                entry.1 = Some(stored);
                self.next_file()
            }
            ReleaseStep::Manifest => {
                let stored = result
                    .child::<StoredObject, GitError>()?
                    .map_err(git_error)?;
                let entry = VersionRef {
                    version: self.version,
                    commit: self.commit.clone(),
                    manifest: pinned(&stored),
                    state: VersionState::Private,
                    created_by: self.auth.user_id,
                    created_at_ms: self.now_ms,
                    approved_by: None,
                };
                let change = AppletChange::AddVersion(Box::new(entry));
                let write = WriteChangeOperation::node(self.applet_id, change, self.auth.user_id);
                self.step = ReleaseStep::Record;
                Ok(Next::Call(IoCall::retry(write, &self.applet_id.to_bytes())))
            }
            ReleaseStep::Record => {
                let applet = result.child::<Applet, AppletError>()??;
                let entry = applet.version(self.version).cloned();
                Ok(Next::Done(entry.ok_or(AppletError::NotFound)?))
            }
        }
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}
