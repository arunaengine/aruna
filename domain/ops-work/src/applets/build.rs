//! Starts an applet build: stages one commit folder's sources in the held bucket and describes
//! the batch job that builds them with no credential and no network.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;

use aruna_core::applet::manifest::MAX_SITE_BYTES;
use aruna_core::applet::record::{Applet, Lifecycle, held_bucket};
use aruna_core::applet::settings::BuildingSettings;
use aruna_core::calls::{IoCall, IoError, IoResult, Next};
use aruna_core::git::{GitNode, StoredObject};
use aruna_core::operation::Operation;
use aruna_core::structs::execution::job::input::{
    ComputeResources, ExecutionSpec, InputMode, InputSelection, InputSource, OutputDestination,
    OutputSelection,
};
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::storage::blob::BucketInfo;
use aruna_ops_data::git::GitError;
use aruna_ops_data::git::commit::{Tree, read_tree};
use aruna_ops_data::git::objects::EnsureBucketOperation;
use bytes::Bytes;
use ulid::Ulid;

use super::release::{git_error, put_held};
use super::{AppletAccessOperation, AppletError, Need};
use crate::jobs::workflow::prepare::STAGING_TAG_KEY;

/// The builder reads its sources here.
pub const SOURCE_DIR: &str = "/src";
/// The builder writes the site, `package-lock.json` and `aruna-source-map.json` here.
pub const OUTPUT_DIR: &str = "/out";
const BUILD_CORES: u32 = 2;
const BUILD_RAM: u64 = 4 << 30;
const BUILD_WALLTIME_MS: u64 = 15 * 60 * 1000;

/// The held bucket folder of one build's sources and outputs.
pub fn build_prefix(applet_id: Ulid, build_id: Ulid) -> String {
    format!("{applet_id}/builds/{build_id}")
}

/// A build's sources, pinned in the held bucket of the applet's host node.
#[derive(Clone, Debug, PartialEq)]
pub struct Staged {
    pub applet: Applet,
    pub commit: String,
    pub files: Vec<(String, StoredObject)>,
}

#[derive(Debug, PartialEq)]
enum StageStep {
    Access,
    Tree,
    Bucket,
    File,
}

/// Copies the files of one repository folder into the build's folder of the held bucket. The
/// caller needs WRITE on the held bucket, and the applet must have room for one more draft.
#[derive(Debug, PartialEq)]
pub struct StageOperation {
    node: GitNode,
    auth: AuthContext,
    applet_id: Ulid,
    build_id: Ulid,
    max: usize,
    source: (String, String),
    applet: Option<Applet>,
    bucket: Option<BucketInfo>,
    commit: String,
    pending: Vec<(String, Bytes)>,
    current: Option<String>,
    files: Vec<(String, StoredObject)>,
    now_ms: u64,
    step: StageStep,
}

impl StageOperation {
    /// Stages `root` of `revision` as build `build_id` of an applet that keeps `max` drafts.
    pub fn new(
        (node, auth): (&GitNode, &AuthContext),
        (applet_id, build_id): (Ulid, Ulid),
        max: usize,
        (revision, root): (String, String),
    ) -> Self {
        Self {
            node: node.clone(),
            auth: auth.clone(),
            applet_id,
            build_id,
            max,
            source: (revision, root),
            applet: None,
            bucket: None,
            commit: String::new(),
            pending: Vec::new(),
            current: None,
            files: Vec::new(),
            now_ms: 0,
            step: StageStep::Access,
        }
    }

    fn applet(&self) -> Result<&Applet, AppletError> {
        self.applet
            .as_ref()
            .ok_or(AppletError::from(IoError::Unfinished))
    }

    fn next_file(&mut self) -> Result<Next<Staged>, AppletError> {
        let Some((path, bytes)) = self.pending.pop() else {
            let applet = self.applet.take().ok_or(IoError::Unfinished)?;
            return Ok(Next::Done(Staged {
                applet,
                commit: std::mem::take(&mut self.commit),
                files: std::mem::take(&mut self.files),
            }));
        };
        let bucket = self.bucket.clone().ok_or(IoError::Unfinished)?;
        let key = format!("{}/src/{path}", build_prefix(self.applet_id, self.build_id));
        let call = put_held(
            (&self.node, &self.auth),
            (self.applet()?, bucket),
            key,
            bytes,
        );
        self.current = Some(path);
        self.step = StageStep::File;
        Ok(Next::Call(call))
    }
}

impl Operation for StageOperation {
    type Output = Staged;
    type Error = AppletError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Result<Next<Staged>, AppletError> {
        let access = AppletAccessOperation::new(&self.auth, self.applet_id, Need::Publish);
        Ok(Next::Call(IoCall::child(access)))
    }

    fn next(&mut self, result: IoResult) -> Result<Next<Staged>, AppletError> {
        match self.step {
            StageStep::Access => {
                let applet = result.child::<Applet, AppletError>()??;
                if Some(applet.host_node) != self.node.node() {
                    return Err(AppletError::Elsewhere(applet.host_node));
                }
                if applet.lifecycle == Lifecycle::TakenDown {
                    return Err(AppletError::Conflict("the applet is taken down".into()));
                }
                if applet.draft_room(self.max) == 0 {
                    return Err(AppletError::Conflict(format!(
                        "the applet keeps at most {} drafts; keep or delete one first",
                        self.max
                    )));
                }
                let repository = (applet.repository)
                    .ok_or_else(|| AppletError::Invalid("the applet has no repository".into()))?;
                let (revision, root) = &self.source;
                let read = read_tree(
                    &self.node,
                    &self.auth,
                    (repository, revision),
                    root,
                    Some(MAX_SITE_BYTES),
                );
                self.applet = Some(applet);
                self.step = StageStep::Tree;
                Ok(Next::Call(IoCall::child(read)))
            }
            StageStep::Tree => {
                let tree = result.child::<Tree, GitError>()?.map_err(git_error)?;
                self.commit = tree.commit;
                self.pending = (tree.files.into_iter())
                    .filter_map(|(path, bytes)| Some((path, bytes?)))
                    .collect();
                if self.pending.is_empty() {
                    return Err(AppletError::Invalid("the folder holds no files".into()));
                }
                self.pending.sort_by(|left, right| right.0.cmp(&left.0));
                let applet = self.applet()?;
                let ensure = EnsureBucketOperation::named(
                    held_bucket(applet.group_id, applet.host_node),
                    applet.group_id,
                    self.auth.user_id,
                    self.now_ms,
                );
                self.step = StageStep::Bucket;
                Ok(Next::Call(IoCall::child(ensure)))
            }
            StageStep::Bucket => {
                let bucket = result.child::<BucketInfo, GitError>()?.map_err(git_error)?;
                self.bucket = Some(bucket);
                self.next_file()
            }
            StageStep::File => {
                let stored = result
                    .child::<StoredObject, GitError>()?
                    .map_err(git_error)?;
                let path = self.current.take().ok_or(IoError::Unfinished)?;
                self.files.push((path, stored));
                self.next_file()
            }
        }
    }

    fn expected_error(error: &AppletError) -> bool {
        !matches!(error, AppletError::Storage(_))
    }
}

/// The batch job of build `build_id`: the pinned builder image on the configured backend, the
/// staged sources as file inputs, and one output folder. It gets no credential and no network.
pub fn build_spec(staged: &Staged, build_id: Ulid, settings: &BuildingSettings) -> ExecutionSpec {
    let applet = &staged.applet;
    let bucket = held_bucket(applet.group_id, applet.host_node);
    let inputs: Vec<InputSelection> = (staged.files.iter())
        .map(|(path, object)| InputSelection {
            source: InputSource::S3 {
                bucket: bucket.clone(),
                key: object.key.clone(),
                version_id: Some(object.version_id.to_string()),
            },
            source_node_id: Some(object.node_id),
            dest_key: format!("src/{path}"),
            mode: InputMode::Snapshot,
            container_path: Some(format!("{SOURCE_DIR}/{path}")),
            name: None,
            description: None,
        })
        .collect();
    let output = OutputSelection {
        container_path: format!("{OUTPUT_DIR}/**"),
        path_prefix: Some(OUTPUT_DIR.to_string()),
        destination_node_id: Some(applet.host_node),
        destination: OutputDestination::S3 {
            bucket,
            key: format!("{}/out", build_prefix(applet.applet_id, build_id)),
        },
        name: None,
        description: None,
    };
    let env = BTreeMap::from([
        ("ARUNA_PREVIEW_SOURCE_MAP".to_string(), "1".to_string()),
        ("ARUNA_SOURCE_DIR".to_string(), SOURCE_DIR.to_string()),
        ("ARUNA_OUTPUT_DIR".to_string(), OUTPUT_DIR.to_string()),
    ]);
    ExecutionSpec {
        group_id: applet.group_id,
        name: Some(format!("{} build", applet.name)),
        description: None,
        tags: BTreeMap::from([(STAGING_TAG_KEY.to_string(), "files".to_string())]),
        image: settings.image.clone(),
        entrypoint: None,
        command: Vec::new(),
        workdir: Some(SOURCE_DIR.to_string()),
        env,
        resources: ComputeResources {
            cpu_cores: Some(BUILD_CORES),
            ram_bytes: Some(BUILD_RAM),
            disk_bytes: None,
            max_walltime_ms: Some(BUILD_WALLTIME_MS),
            preemptible: false,
            gpus: None,
        },
        executor_constraint: None,
        inputs,
        file_outputs: vec![output],
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
        backend: Some(settings.backend.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::applet::settings::default_catalogue;

    #[test]
    fn spec_lacks_access() {
        let mut applet = crate::applets::change::tests::applet();
        applet.name = "Bakta".into();
        let object = StoredObject {
            node_id: applet.host_node,
            group_id: Some(applet.group_id),
            bucket: held_bucket(applet.group_id, applet.host_node),
            key: "k/src/index.html".into(),
            version_id: Ulid::from_bytes([9; 16]),
            size: 1,
            sha256: String::new(),
            blake3: [0; 32],
            global: None,
        };
        let staged = Staged {
            applet,
            commit: "abc".into(),
            files: vec![("index.html".into(), object)],
        };
        let settings = BuildingSettings {
            backend: "builders".into(),
            image: format!("builder@sha256:{}", "a".repeat(64)),
            mirror: Some("https://npm.example.org".into()),
            max_drafts: 6,
            draft_ttl_hours: 24,
            catalogue: default_catalogue(),
        };
        let build = Ulid::from_bytes([8; 16]);
        let spec = build_spec(&staged, build, &settings);
        assert_eq!(spec.backend.as_deref(), Some("builders"));
        assert_eq!(
            spec.tags.get(STAGING_TAG_KEY).map(String::as_str),
            Some("files")
        );
        assert_eq!(
            spec.env.get("ARUNA_PREVIEW_SOURCE_MAP").map(String::as_str),
            Some("1")
        );
        // No mirror, network or credential reaches the builder.
        assert!(spec.env.keys().all(|name| !name.contains("MIRROR")));
        assert!(
            !spec
                .tags
                .contains_key(crate::jobs::workflow::prepare::NETWORK_TAG_KEY)
        );
        assert_eq!(
            spec.inputs[0].container_path.as_deref(),
            Some("/src/index.html")
        );
        let OutputDestination::S3 { key, .. } = &spec.file_outputs[0].destination else {
            panic!("the output goes to the held bucket");
        };
        assert_eq!(
            key,
            &format!("{}/out", build_prefix(staged.applet.applet_id, build))
        );
    }
}
