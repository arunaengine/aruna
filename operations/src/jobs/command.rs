//! Transport-independent execution commands shared by REST and MCP: the
//! submission shape both transports accept, the session directives a node
//! resolves into it, and the named outcome an accepted submission produces.

use std::collections::BTreeMap;

use aruna_core::compute::SessionMount;
use aruna_core::compute::normalize_container_path;
use aruna_core::compute::runtimes::{
    SESSION_MOUNT_DIR, SESSION_MOUNT_PREFIX, SESSION_EXPIRY_TAG, SESSION_IDLE_TAG,
    MOUNT_PATH_TAG, MOUNT_PREFIX_TAG, SESSION_RUNTIME_TAG, SESSION_RUNTIMES,
    SESSION_TAG, SESSION_TAG_NOTEBOOK, session_runtime,
};
use aruna_core::structs::execution::job::JobId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Working directory a session runs in when the caller names none.
const SESSION_WORKDIR: &str = "/work";

/// One object staged into the container before the run.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ExecutionInput {
    /// Source bucket holding the object, for example `project-data`.
    pub bucket: String,
    /// Source object key inside `bucket`, for example `inputs/reads.fastq.gz`.
    /// A relative key without a leading slash and without `..` segments.
    pub key: String,
    /// Exact object version to stage. Required by `exact_reference` mode and
    /// refused by `floating_reference` mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    /// Realm node holding this object. Only a `local` run accepts it: the named
    /// version is copied onto the device before the run and `version_id` is
    /// then required. A realm run refuses it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_node_id: Option<String>,
    /// Input name, for example `reads.fastq.gz`; it names no bucket key. Must
    /// not be empty and must be unique across the declared inputs.
    pub dest_key: String,
    /// Absolute container path; defaults to `/inputs/<dest_key>`. It must be
    /// absolute, must not be `/`, must carry no `.` or `..` component, and must
    /// be unique across the declared inputs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_path: Option<String>,
    /// Composition mode; defaults to `snapshot`.
    #[serde(default)]
    pub mode: InputMode,
}

/// Per-input composition mode. `exact_reference` requires `version_id`,
/// `floating_reference` rejects it.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InputMode {
    #[default]
    Snapshot,
    FloatingReference,
    ExactReference,
}

/// How a claimed destination key is resolved. `reject` refuses the submission,
/// `replace` lets the later declaration win, and `keep_existing` keeps the first.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CollisionPolicy {
    #[default]
    Reject,
    Replace,
    KeepExisting,
}

/// One container path captured after the run.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ExecutionOutput {
    /// Absolute container path captured after the task exits, for example
    /// `/work/result.json`. Must be unique across the declared outputs.
    pub container_path: String,
    /// Destination key this output is written to, for example
    /// `results/result.json`. Must not be empty and must be unique per bucket.
    pub dest_key: String,
    /// Destination bucket of this output, for example `project-data`. Required
    /// when `workspace.mode` is `none`, and defaults to the workspace bucket
    /// under `existing`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
}

/// Which bucket a run works inside. `none` creates and touches no bucket of its
/// own, and `existing` runs inside the named bucket the caller already owns.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WorkspaceMode {
    #[default]
    None,
    Existing,
}

/// The workspace a submission declares.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WorkspaceSpec {
    /// `none` runs without a bucket of its own, `existing` runs inside the named
    /// bucket. An omitted workspace block is `none`.
    pub mode: WorkspaceMode,
    /// Required by `existing` mode and refused by `none`. The bucket must exist,
    /// belong to the same group, and be writable by the caller.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
}

/// The slice of the workspace bucket a session sees as a folder. Everything
/// written below that folder lands in the bucket and every object under the
/// prefix shows up in it, when the executing backend has an S3 mount driver.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SessionMountSpec {
    /// Folder in the bucket, for example `raw/2024`; an empty string mounts the
    /// whole bucket. Omitted, the `data/` folder is mounted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Absolute folder below the working directory the prefix appears at, for
    /// example `/work/raw`. Omitted, it is `data` below the working directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// Where a submission runs. `local` is served by a user device only, and runs
/// the job on that machine for its owner.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionTarget {
    #[default]
    Realm,
    Local,
}

/// The complete native execution submission both transports accept. REST maps
/// its request DTO into this command; MCP's `submit_job` takes it directly.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SubmitExecutionCommand {
    /// Owning group's bare 26-character ULID, for example
    /// `01JZ8Y6T0K4W7M2N9Q5R3S8V1X`. The caller needs write permission on it.
    pub group_id: String,
    /// Short human name for the run, for example `GC content by sample`. Shown
    /// wherever the run is listed. Defaults to absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Longer note about what the run does and why. Defaults to absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// OCI image the task runs, for example `docker.io/library/python:3.13-slim`.
    /// Must not be blank unless `runtime` names a session runtime, which fills
    /// the image, entrypoint and command instead.
    #[serde(default)]
    pub image: String,
    /// Session runtime catalog id, for example `python-notebook`. Required by a
    /// submission carrying the tag `aruna-engine.org/session`, refused without
    /// it. It fills `image`, `entrypoint` and `command`, which must be empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    /// Idle wait of a session job in milliseconds, for example `600000` for ten
    /// minutes. The executing node clamps it to the realm's value, so a longer
    /// request never extends the session. Refused outside a session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "session_idle_after_ms")]
    pub session_idle_ms: Option<u64>,
    /// Which part of the workspace bucket a session mounts, and where. Refused
    /// outside a session.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_mount: Option<SessionMountSpec>,
    /// Replaces the image ENTRYPOINT. Omit to keep the image default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    /// Argument vector appended after the entrypoint, for example
    /// `["python", "/work/script.py"]`. Defaults to empty.
    #[serde(default)]
    pub command: Vec<String>,
    /// Environment variables for the task. Defaults to empty.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Scheduling tags. `aruna-engine.org/label/<key>` demands a matching target
    /// label, at most 16 of them. The workspace tags of that namespace are
    /// reserved and refused. Defaults to empty.
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// Absolute working directory inside the container, for example `/work`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    /// Whole CPU cores reserved. Defaults to 1; `0` is refused and the group's
    /// compute quota may cap it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_cores: Option<u32>,
    /// RAM reserved in bytes, for example `1073741824` for 1 GiB. Defaults to
    /// 1 GiB; `0` and anything above 9223372036854775807 are refused.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ram_bytes: Option<u64>,
    /// Wall-clock limit in milliseconds, for example `600000` for ten minutes.
    /// Defaults to 86400000, one day; the group's compute quota may cap it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_walltime_ms: Option<u64>,
    /// Optional executor selector. Leave unset unless the realm documents one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_constraint: Option<String>,
    /// Objects staged into the container before the run, at most 512.
    #[serde(default)]
    pub inputs: Vec<ExecutionInput>,
    /// Container paths captured after the run, at most 1024. Each names the
    /// bucket it lands in, or falls back to the workspace bucket.
    #[serde(default)]
    pub outputs: Vec<ExecutionOutput>,
    /// Workspace prefixes whose versions this execution wrote and inventories at
    /// completion. Requires `workspace.mode` `existing` and at least one
    /// bucket-qualified output.
    #[serde(default)]
    pub output_prefixes: Vec<String>,
    /// How a destination key already claimed by another declared input or by an
    /// object in the workspace bucket is resolved. Defaults to `reject`.
    #[serde(default)]
    pub collision_policy: CollisionPolicy,
    /// Caller-chosen key that makes a resubmission return the same job instead
    /// of starting a second one. A different request under a used key is a 409.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Which bucket the run works inside. Absent means `none`: the run touches
    /// no bucket of its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<WorkspaceSpec>,
    /// `realm` (the default) admits the job into the realm; `local` runs it on
    /// this machine and is served by a user device only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<ExecutionTarget>,
}

/// The application outcome of an accepted submission. REST maps it to its HTTP
/// body, MCP to its tool result. A local run belongs to no submission family,
/// so it names no submission id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptedExecution {
    /// The alias this responder bound the request to. Stable for the caller.
    pub job_id: JobId,
    pub created: bool,
    /// The replicated identity of the request itself, hex encoded. Two aliases
    /// of one request always share it. Absent for a local run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub submission_id: Option<String>,
    /// The alias the responder currently reduces as canonical.
    pub canonical_job_id: JobId,
    /// The family state at acceptance, or the current reduced state of a replay.
    pub state: String,
}

/// Why a session directive was refused. Every refusal is malformed caller
/// input, and each transport maps it to its own bad-request answer.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SessionCommandError(String);

impl SessionCommandError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl SubmitExecutionCommand {
    /// Resolves the session directives of this command. A session names a
    /// catalog runtime instead of an image; the resolved runtime and idle wait
    /// become engine tags the executing node reads back.
    pub fn resolve_session(
        &mut self,
        bearer_expires_at_ms: Option<u64>,
    ) -> Result<(), SessionCommandError> {
        let Some(value) = self.tags.get(SESSION_TAG) else {
            if self.runtime.is_some()
                || self.session_idle_ms.is_some()
                || self.session_mount.is_some()
            {
                return Err(SessionCommandError::new(format!(
                    "runtime, session_idle_after_ms and session_mount need the tag {SESSION_TAG}"
                )));
            }
            return Ok(());
        };
        if value != SESSION_TAG_NOTEBOOK {
            return Err(SessionCommandError::new(format!(
                "tag {SESSION_TAG} accepts only the value {SESSION_TAG_NOTEBOOK}"
            )));
        }
        if [
            SESSION_RUNTIME_TAG,
            SESSION_IDLE_TAG,
            SESSION_EXPIRY_TAG,
            MOUNT_PREFIX_TAG,
            MOUNT_PATH_TAG,
        ]
        .iter()
        .any(|tag| self.tags.contains_key(*tag))
        {
            return Err(SessionCommandError::new(
                "the session runtime, idle, expiry and mount tags are set by the node",
            ));
        }
        let existing = self
            .workspace
            .as_ref()
            .is_some_and(|workspace| matches!(workspace.mode, WorkspaceMode::Existing));
        if !existing {
            return Err(SessionCommandError::new(
                "a session runs inside an existing workspace bucket",
            ));
        }
        if !self.image.trim().is_empty() || self.entrypoint.is_some() || !self.command.is_empty() {
            return Err(SessionCommandError::new(
                "a session takes image, entrypoint and command from its runtime",
            ));
        }
        let id = self.runtime.as_deref().unwrap_or_default();
        let runtime = session_runtime(id).ok_or_else(|| {
            let known: Vec<&str> = SESSION_RUNTIMES.iter().map(|entry| entry.id).collect();
            SessionCommandError::new(format!(
                "unknown session runtime; known ids are {}",
                known.join(", ")
            ))
        })?;
        if let Some(idle) = self.session_idle_ms {
            if idle == 0 {
                return Err(SessionCommandError::new(
                    "session_idle_after_ms must be greater than zero",
                ));
            }
            self.tags
                .insert(SESSION_IDLE_TAG.to_string(), idle.to_string());
        }
        if self.workdir.is_none() {
            self.workdir = Some(SESSION_WORKDIR.to_string());
        }
        let mount = mount_request(
            self.session_mount.as_ref(),
            self.workdir.as_deref().unwrap_or(SESSION_WORKDIR),
        )?;
        self.tags
            .insert(MOUNT_PREFIX_TAG.to_string(), mount.prefix);
        self.tags
            .insert(MOUNT_PATH_TAG.to_string(), mount.path);
        self.cpu_cores.get_or_insert(2);
        self.ram_bytes.get_or_insert(4_000_000_000);
        self.image = runtime.image.to_string();
        self.command = runtime
            .command
            .iter()
            .map(|part| (*part).to_string())
            .collect();
        for (key, value) in runtime.env {
            self.env
                .entry((*key).to_string())
                .or_insert_with(|| (*value).to_string());
        }
        self.tags
            .insert(SESSION_RUNTIME_TAG.to_string(), runtime.id.to_string());
        if let Some(expires_at_ms) = bearer_expires_at_ms {
            self.tags
                .insert(SESSION_EXPIRY_TAG.to_string(), expires_at_ms.to_string());
        }
        Ok(())
    }
}

/// The bucket slice a session mounts and the folder it appears at. An omitted
/// block mounts `data/` at `<workdir>/data`. The folder must lie below the
/// working directory and clear of the helper's `.aruna` directory.
fn mount_request(
    mount: Option<&SessionMountSpec>,
    workdir: &str,
) -> Result<SessionMount, SessionCommandError> {
    let workdir = normalize_container_path(workdir).map_err(SessionCommandError::new)?;
    let prefix = match mount
        .and_then(|mount| mount.prefix.as_deref())
        .map(str::trim)
    {
        None => SESSION_MOUNT_PREFIX.to_string(),
        Some("") => String::new(),
        Some(prefix) => {
            let folder = prefix.trim_end_matches('/');
            if folder.starts_with('/')
                || folder
                    .split('/')
                    .any(|part| part.is_empty() || part == "." || part == "..")
            {
                return Err(SessionCommandError::new(
                    "session_mount.prefix must be a relative, traversal-free bucket folder",
                ));
            }
            format!("{folder}/")
        }
    };
    let path = match mount.and_then(|mount| mount.path.as_deref()).map(str::trim) {
        None | Some("") => workdir.join(SESSION_MOUNT_DIR),
        Some(path) => normalize_container_path(path).map_err(SessionCommandError::new)?,
    };
    let below = path
        .strip_prefix(&workdir)
        .ok()
        .filter(|rest| !rest.as_os_str().is_empty() && !rest.starts_with(".aruna"));
    if below.is_none() {
        return Err(SessionCommandError::new(
            "session_mount.path must be a folder below the working directory, outside .aruna",
        ));
    }
    Ok(SessionMount {
        prefix,
        path: path.display().to_string(),
    })
}

#[cfg(test)]
mod pure_tests {
    use super::*;
    use ulid::Ulid;

    /// The shape the portal posts for a session: no image, no command, an
    /// existing workspace bucket.
    fn session_command() -> SubmitExecutionCommand {
        let mut tags = BTreeMap::new();
        tags.insert(SESSION_TAG.to_string(), SESSION_TAG_NOTEBOOK.to_string());
        SubmitExecutionCommand {
            group_id: Ulid::from_bytes([5u8; 16]).to_string(),
            name: None,
            description: None,
            image: String::new(),
            runtime: Some("python-notebook".to_string()),
            session_idle_ms: Some(600_000),
            session_mount: None,
            entrypoint: None,
            command: Vec::new(),
            env: BTreeMap::new(),
            tags,
            workdir: None,
            cpu_cores: None,
            ram_bytes: None,
            max_walltime_ms: None,
            executor_constraint: None,
            inputs: Vec::new(),
            outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: CollisionPolicy::default(),
            idempotency_key: None,
            workspace: Some(WorkspaceSpec {
                mode: WorkspaceMode::Existing,
                bucket: Some("lab-data".to_string()),
            }),
            target: None,
        }
    }

    fn plain_command() -> SubmitExecutionCommand {
        SubmitExecutionCommand {
            group_id: Ulid::from_bytes([5u8; 16]).to_string(),
            name: Some("run".to_string()),
            description: None,
            image: "alpine:3".to_string(),
            runtime: None,
            session_idle_ms: None,
            session_mount: None,
            entrypoint: Some(vec!["sh".to_string()]),
            command: Vec::new(),
            env: BTreeMap::new(),
            tags: BTreeMap::new(),
            workdir: None,
            cpu_cores: Some(1),
            ram_bytes: Some(1_000_000_000),
            max_walltime_ms: None,
            executor_constraint: None,
            inputs: Vec::new(),
            outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: CollisionPolicy::default(),
            idempotency_key: None,
            workspace: None,
            target: None,
        }
    }

    #[test]
    fn session_takes_catalog() {
        let mut command = session_command();
        command
            .resolve_session(None)
            .expect("a session submit is accepted");
        let runtime = session_runtime("python-notebook").expect("the catalog holds it");
        assert_eq!(command.cpu_cores, Some(2));
        assert_eq!(command.ram_bytes, Some(4_000_000_000));
        assert_eq!(command.image, runtime.image);
        assert_eq!(command.command, vec![runtime.command[0].to_string()]);
        assert_eq!(command.workdir.as_deref(), Some(SESSION_WORKDIR));
        assert_eq!(
            command.tags.get(SESSION_RUNTIME_TAG).map(String::as_str),
            Some("python-notebook")
        );
        assert_eq!(
            command.tags.get(SESSION_IDLE_TAG).map(String::as_str),
            Some("600000")
        );
    }

    #[test]
    fn session_keeps_resources() {
        let mut command = session_command();
        command.cpu_cores = Some(8);
        command.ram_bytes = Some(16_000_000_000);
        command
            .resolve_session(None)
            .expect("explicit resources are accepted");
        assert_eq!(command.cpu_cores, Some(8));
        assert_eq!(command.ram_bytes, Some(16_000_000_000));
    }

    #[test]
    fn session_refuses_image() {
        for mutate in [
            |command: &mut SubmitExecutionCommand| command.image = "alpine:3".to_string(),
            |command: &mut SubmitExecutionCommand| {
                command.entrypoint = Some(vec!["sh".to_string()])
            },
            |command: &mut SubmitExecutionCommand| command.command = vec!["sh".to_string()],
        ] {
            let mut command = session_command();
            mutate(&mut command);
            assert!(command.resolve_session(None).is_err());
        }
    }

    #[test]
    fn session_needs_bucket() {
        let mut command = session_command();
        command.workspace = None;
        assert!(command.resolve_session(None).is_err());

        let mut command = session_command();
        command.workspace = Some(WorkspaceSpec {
            mode: WorkspaceMode::None,
            bucket: None,
        });
        assert!(command.resolve_session(None).is_err());
    }

    #[test]
    fn session_needs_runtime() {
        let mut command = session_command();
        command.runtime = None;
        assert!(command.resolve_session(None).is_err());

        let mut command = session_command();
        command.runtime = Some("nope".to_string());
        assert!(command.resolve_session(None).is_err());
    }

    #[test]
    fn runtime_needs_tag() {
        // A catalog runtime outside a session would leave the image unpinned.
        let mut command = session_command();
        command.tags.clear();
        assert!(command.resolve_session(None).is_err());
    }

    #[test]
    fn session_mount_defaults() {
        // An omitted block keeps the data/ folder below the working directory.
        let mut command = session_command();
        command
            .resolve_session(None)
            .expect("a session submit is accepted");
        assert_eq!(
            command
                .tags
                .get(MOUNT_PREFIX_TAG)
                .map(String::as_str),
            Some("data/")
        );
        assert_eq!(
            command.tags.get(MOUNT_PATH_TAG).map(String::as_str),
            Some("/work/data")
        );
    }

    #[test]
    fn session_mount_chosen() {
        // The caller picks the bucket folder and the kernel folder. The prefix
        // ends in one slash, the path is normalised, and an empty prefix is the
        // whole bucket.
        let mut command = session_command();
        command.workdir = Some("/home/user/".to_string());
        command.session_mount = Some(SessionMountSpec {
            prefix: Some(" raw/2024 ".to_string()),
            path: Some("/home/user/project//raw/".to_string()),
        });
        command
            .resolve_session(None)
            .expect("a chosen mount is accepted");
        assert_eq!(
            command
                .tags
                .get(MOUNT_PREFIX_TAG)
                .map(String::as_str),
            Some("raw/2024/")
        );
        assert_eq!(
            command.tags.get(MOUNT_PATH_TAG).map(String::as_str),
            Some("/home/user/project/raw")
        );

        let mut command = session_command();
        command.session_mount = Some(SessionMountSpec {
            prefix: Some(String::new()),
            path: None,
        });
        command
            .resolve_session(None)
            .expect("the whole bucket is accepted");
        assert_eq!(
            command
                .tags
                .get(MOUNT_PREFIX_TAG)
                .map(String::as_str),
            Some("")
        );
        assert_eq!(
            command.tags.get(MOUNT_PATH_TAG).map(String::as_str),
            Some("/work/data")
        );
    }

    #[test]
    fn session_mount_refused() {
        // A prefix must stay inside the bucket, and the folder must stay below
        // the working directory and clear of the helper's socket directory.
        for (prefix, path) in [
            (Some("/raw"), None),
            (Some("raw/../other"), None),
            (Some("raw//2024"), None),
            (Some("./raw"), None),
            (None, Some("/work")),
            (None, Some("/data")),
            (None, Some("work/data")),
            (None, Some("/work/.aruna/data")),
            (None, Some("/work/../data")),
        ] {
            let mut command = session_command();
            command.session_mount = Some(SessionMountSpec {
                prefix: prefix.map(str::to_string),
                path: path.map(str::to_string),
            });
            assert!(
                command.resolve_session(None).is_err(),
                "{prefix:?} {path:?}"
            );
        }

        // Outside a session the block has nothing to mount.
        let mut command = plain_command();
        command.session_mount = Some(SessionMountSpec {
            prefix: None,
            path: None,
        });
        assert!(command.resolve_session(None).is_err());
    }

    #[test]
    fn session_refuses_reserved() {
        // The runtime, idle, expiry and mount tags are the node's to set.
        for tag in [
            SESSION_RUNTIME_TAG,
            SESSION_IDLE_TAG,
            SESSION_EXPIRY_TAG,
            MOUNT_PREFIX_TAG,
            MOUNT_PATH_TAG,
        ] {
            let mut command = session_command();
            command.tags.insert(tag.to_string(), "x".to_string());
            assert!(command.resolve_session(None).is_err());
        }
    }

    #[test]
    fn plain_run_untouched() {
        // A run without the session tag keeps its own image and tags.
        let mut command = plain_command();
        command
            .resolve_session(None)
            .expect("a plain run is untouched");
        assert_eq!(command.image, "alpine:3");
        assert!(!command.tags.contains_key(SESSION_RUNTIME_TAG));
    }
}
