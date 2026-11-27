//! Turns a finished build into a draft, and keeps a draft as a private version with the very
//! same pinned files, so what a previewer saw is what goes up for approval.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::manifest::{
    LOCKFILE, ManifestFile, SOURCE_MAP, VersionManifest, content_type, lockfile_packages,
};
use aruna_core::applet::record::{
    Applet, Draft, PinnedObject, VersionRef, VersionState, held_bucket,
};
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitNode, StoredObject};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::local::OutputObject;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_ops_data::git::GitError;
use aruna_ops_data::git::objects::EnsureBucketOperation;
use bytes::Bytes;
use ulid::Ulid;

use super::build::build_prefix;
use super::change::{AppletChange, WriteChangeOperation};
use super::release::{git_error, pinned, put_held};
use super::{AppletAccessOperation, AppletError, Need};

/// What happens to one draft.
#[derive(Clone, Debug, PartialEq)]
pub enum DraftAction {
    /// The build succeeded with these outputs; the lockfile's bytes when it wrote one.
    Capture {
        outputs: Vec<OutputObject>,
        lockfile: Option<Bytes>,
    },
    Fail(String),
    /// The bytes of the draft's manifest, checked against its pinned digest.
    Keep(Bytes),
}

/// The manifest of build `build_id` from its outputs below the build's output folder: the
/// source map apart from the served files and the lockfile only as its packages.
pub fn draft_manifest(
    applet: &Applet,
    draft: &Draft,
    outputs: &[OutputObject],
    lockfile: Option<&[u8]>,
) -> Result<VersionManifest, AppletError> {
    let bucket = held_bucket(applet.group_id, applet.host_node);
    let folder = format!("{}/out/", build_prefix(applet.applet_id, draft.build_id));
    let mut manifest = VersionManifest {
        applet_id: applet.applet_id,
        version: 0,
        commit: Some(draft.commit.clone()),
        lockfile_blake3: None,
        packages: Vec::new(),
        files: Vec::new(),
        source_map: None,
    };
    for output in outputs {
        let path = (output.key.strip_prefix(&folder))
            .filter(|_| output.bucket == bucket && output.node_id == applet.host_node)
            .ok_or_else(|| AppletError::Invalid(format!("{} is no build output", output.key)))?;
        let blake3 = (output.digest.clone())
            .ok_or_else(|| AppletError::Invalid(format!("the output {path} has no digest")))?;
        let object = PinnedObject {
            node: output.node_id,
            bucket: output.bucket.clone(),
            key: output.key.clone(),
            version_id: output.version_id.to_string(),
            blake3,
        };
        match path {
            SOURCE_MAP => manifest.source_map = Some(object),
            LOCKFILE => {
                let bytes = lockfile
                    .ok_or_else(|| AppletError::Invalid("the lockfile was not read".into()))?;
                if blake3::hash(bytes).to_hex().as_str() != object.blake3 {
                    return Err(AppletError::Invalid("the lockfile changed".into()));
                }
                manifest.packages = lockfile_packages(bytes)
                    .map_err(|error| AppletError::Invalid(error.to_string()))?;
                manifest.lockfile_blake3 = Some(object.blake3);
            }
            _ => manifest.files.push(ManifestFile {
                path: path.to_string(),
                content_type: content_type(path).to_string(),
                node: object.node,
                bucket: object.bucket,
                key: object.key,
                version_id: object.version_id,
                blake3: object.blake3,
                size_bytes: output.size,
            }),
        }
    }
    manifest
        .checked()
        .map_err(|error| AppletError::Invalid(error.to_string()))
}

#[derive(Debug, PartialEq)]
enum DraftStep {
    Access,
    Bucket,
    Manifest,
    Record,
}

/// Captures, fails or keeps draft `build_id`. The caller needs WRITE on the held bucket.
#[derive(Debug, PartialEq)]
pub struct DraftOperation {
    node: GitNode,
    auth: AuthContext,
    applet_id: Ulid,
    build_id: Ulid,
    action: Option<DraftAction>,
    applet: Option<Applet>,
    manifest: Option<(String, Bytes)>,
    version: u32,
    now_ms: u64,
    step: DraftStep,
}

impl DraftOperation {
    pub fn new(
        (node, auth): (&GitNode, &AuthContext),
        (applet_id, build_id): (Ulid, Ulid),
        action: DraftAction,
    ) -> Self {
        Self {
            node: node.clone(),
            auth: auth.clone(),
            applet_id,
            build_id,
            action: Some(action),
            applet: None,
            manifest: None,
            version: 0,
            now_ms: 0,
            step: DraftStep::Access,
        }
    }

    /// The manifest to write and its key, or the change when nothing is written first.
    fn plan(&mut self, applet: &Applet) -> Result<Option<AppletChange>, AppletError> {
        let draft = applet.draft(self.build_id).ok_or(AppletError::NotFound)?;
        let action = self.action.take().ok_or(IoError::Unfinished)?;
        let prefix = build_prefix(self.applet_id, self.build_id);
        let (key, manifest) = match action {
            DraftAction::Fail(reason) => {
                let build_id = self.build_id;
                return Ok(Some(AppletChange::FailDraft { build_id, reason }));
            }
            DraftAction::Capture { outputs, lockfile } => {
                // Outputs that make no site fail the draft rather than leave it building.
                match draft_manifest(applet, draft, &outputs, lockfile.as_deref()) {
                    Ok(manifest) => (format!("{prefix}/manifest.json"), manifest),
                    Err(AppletError::Invalid(reason)) => {
                        let build_id = self.build_id;
                        return Ok(Some(AppletChange::FailDraft { build_id, reason }));
                    }
                    Err(error) => return Err(error),
                }
            }
            DraftAction::Keep(bytes) => {
                let pinned = draft.manifest.as_ref().ok_or_else(|| {
                    AppletError::Conflict("the build has no captured outputs".into())
                })?;
                if blake3::hash(&bytes).to_hex().as_str() != pinned.blake3 {
                    return Err(AppletError::Storage("the draft manifest changed".into()));
                }
                let mut manifest = VersionManifest::from_bytes(&bytes)?;
                self.version = applet.next_version();
                manifest.version = self.version;
                let key = format!("{}/{}/.aruna/manifest.json", self.applet_id, self.version);
                (key, manifest)
            }
        };
        self.manifest = Some((key, Bytes::from(manifest.to_bytes()?)));
        Ok(None)
    }

    fn record(&mut self, change: AppletChange) -> Next<Applet> {
        let write = WriteChangeOperation::node(self.applet_id, change, self.auth.user_id);
        self.step = DraftStep::Record;
        Next::Call(IoCall::retry(write, &self.applet_id.to_bytes()))
    }

    fn change(&self, stored: &StoredObject) -> Result<AppletChange, AppletError> {
        let applet = self.applet.as_ref().ok_or(IoError::Unfinished)?;
        let build_id = self.build_id;
        if self.version == 0 {
            let manifest = pinned(stored);
            return Ok(AppletChange::CaptureDraft { build_id, manifest });
        }
        let commit = (applet.draft(build_id)).map(|draft| draft.commit.clone());
        let entry = VersionRef {
            version: self.version,
            commit,
            manifest: pinned(stored),
            state: VersionState::Private,
            created_by: self.auth.user_id,
            created_at_ms: self.now_ms,
            approved_by: None,
        };
        let entry = Box::new(entry);
        Ok(AppletChange::KeepDraft { build_id, entry })
    }
}

impl Operation for DraftOperation {
    type Output = Applet;
    type Error = AppletError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Applet>, AppletError> {
        let access = AppletAccessOperation::new(&self.auth, self.applet_id, Need::Publish);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Applet>, AppletError> {
        match self.step {
            DraftStep::Access => {
                let applet = result.child::<Applet, AppletError>()??;
                if Some(applet.host_node) != self.node.node() {
                    return Err(AppletError::Elsewhere(applet.host_node));
                }
                if let Some(change) = self.plan(&applet)? {
                    return Ok(self.record(change));
                }
                let ensure = EnsureBucketOperation::named(
                    held_bucket(applet.group_id, applet.host_node),
                    applet.group_id,
                    self.auth.user_id,
                    self.now_ms,
                );
                self.applet = Some(applet);
                self.step = DraftStep::Bucket;
                Ok(Next::Call(IoCall::child(ensure)))
            }
            DraftStep::Bucket => {
                let bucket = result.child::<BucketInfo, GitError>()?.map_err(git_error)?;
                let applet = self.applet.as_ref().ok_or(IoError::Unfinished)?;
                let (key, bytes) = self.manifest.take().ok_or(IoError::Unfinished)?;
                let call = put_held((&self.node, &self.auth), (applet, bucket), key, bytes);
                self.step = DraftStep::Manifest;
                Ok(Next::Call(call))
            }
            DraftStep::Manifest => {
                let stored = result
                    .child::<StoredObject, GitError>()?
                    .map_err(git_error)?;
                let change = self.change(&stored)?;
                Ok(self.record(change))
            }
            DraftStep::Record => result.child::<Applet, AppletError>()?.map(Next::Done),
        }
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}

#[cfg(test)]
#[path = "draft_tests.rs"]
mod tests;
