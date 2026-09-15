use std::collections::BTreeMap;
use std::path::Path as FilePath;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;

use crate::routes::routes_at;
use aruna_core::compute::{
    has_wildcard, literal_prefix, output_glob, output_suffix, paths_overlap,
};
use aruna_core::structs::identity::auth::{AuthContext, NodeCapabilities};
use aruna_core::structs::execution::job::{
    ComputeResources, ExecutionSpec, InputMode, InputSelection, InputSource, JobId, JobPayload,
    JobRecord, JobResultPayload, JobState, MAX_EXECUTION_OUTPUTS, OutputDestination,
    OutputSelection, PhysicalExecutionResult, ResultMessage, WorkspaceMode,
};
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_operations::device::compute::{LocalExecutionConfig, submit_local_execution};
use aruna_operations::driver::drive;
use aruna_operations::jobs::JobRouteError;
use aruna_operations::jobs::lifecycle::{FamilyReport, family_report, submit_external_job};
use aruna_operations::jobs::service::{
    RoutedCancelOutcome, cancel_job_routed, list_owned_jobs, read_record_routed,
};
use aruna_operations::s3::access::get::{GetAccessError, GetAccessOperation};
use axum::extract::{ConnectInfo, Path, Query, RawQuery, State};
use axum::http::{HeaderMap, StatusCode, header::AUTHORIZATION};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::require_owner;
use crate::auth::{ValidatedBearer, require_unrestricted_auth};
use crate::error::ServerError;
use crate::forwarded::external_base_url;
use crate::routes::execution::jobs::{ExecutionTarget, map_local_error};
use crate::server_state::ServerState;

/// GA4GH TES version this facade implements.
const TES_VERSION: &str = "1.1.0";
/// Optional tag overriding the caller credential's workspace parent group.
const GROUP_TAG_KEY: &str = "aruna-engine.org/group";
/// Optional tag pinning a backend executor kind.
const EXECUTOR_TAG_KEY: &str = "aruna-engine.org/executor";
/// Optional tag carrying the submission idempotency key.
const IDEMPOTENCY_TAG_KEY: &str = "aruna-engine.org/idempotency-key";
/// Optional tag choosing where the task runs: `realm` or `local`.
const TARGET_TAG_KEY: &str = "aruna-engine.org/target";

/// Read-only tags derived at read time from the job and its family. They are
/// never stored, so a task creation naming one of them is refused.
const JOB_ID_TAG_KEY: &str = "aruna-engine.org/job-id";
const LOGICAL_STATE_TAG_KEY: &str = "aruna-engine.org/logical-state";
const EXECUTOR_KIND_TAG_KEY: &str = "aruna-engine.org/executor-kind";
const TRANSFER_BYTES_TAG_KEY: &str = "aruna-engine.org/estimated-transfer-bytes";
const DERIVED_TAG_KEYS: [&str; 4] = [
    JOB_ID_TAG_KEY,
    LOGICAL_STATE_TAG_KEY,
    EXECUTOR_KIND_TAG_KEY,
    TRANSFER_BYTES_TAG_KEY,
];

const DEFAULT_PAGE_SIZE: usize = 256;
const MAX_PAGE_SIZE: usize = 512;
/// Bounds the quadratic input/output path-overlap validation.
const MAX_TASK_IO: usize = 512;

#[derive(OpenApi)]
#[openapi(
    tags((name = "tes", description = "GA4GH TES v1.1 task execution facade")),
    components(schemas(
        TesServiceInfo,
        TesServiceType,
        TesServiceOrganization,
        TesTask,
        TesExecutor,
        TesInput,
        TesOutput,
        TesResources,
        TesFileType,
        TesState,
        TesExecutorLog,
        TesFileLog,
        TesTaskLog,
        TesTaskResponse,
        TesTasksResponse,
        TesErrorPayload
    ))
)]
pub struct TesApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    routes_at(
        OpenApiRouter::with_openapi(TesApiDoc::openapi())
            .routes(routes!(service_info))
            .routes(routes!(list_tasks, create_task))
            .routes(routes!(get_task)),
        // The `:cancel` action suffix is parsed out of `{id}` by the handler.
        "/ga4gh/tes/v1/tasks/{id}",
        routes!(cancel_task),
    )
}

// GA4GH TES v1.1 wire types use the OpenAPI snake_case names.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TesState {
    Unknown,
    Queued,
    Initializing,
    Running,
    Paused,
    Complete,
    ExecutorError,
    SystemError,
    Canceled,
    Canceling,
    Preempted,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TesFileType {
    #[default]
    File,
    Directory,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesExecutor {
    #[serde(default)]
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default)]
    pub path: String,
    #[serde(rename = "type", default)]
    pub kind: TesFileType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesOutput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Destination URL; a directory prefix when `path` contains wildcards.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Absolute container path, optionally with POSIX (IEEE Std 1003.1-2017,
    /// 12.13) wildcards `*`, `?`, and `[...]` selecting several files.
    #[serde(default)]
    pub path: String,
    /// Literal ancestor stripped from every matched path before it is appended
    /// to `url`. Required when `path` has wildcards, ignored otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_prefix: Option<String>,
    #[serde(rename = "type", default)]
    pub kind: TesFileType,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesResources {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_cores: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preemptible: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ram_gb: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_gb: Option<f64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub zones: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesExecutorLog {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
#[schema(as = TesOutputFileLog)]
pub struct TesFileLog {
    pub url: String,
    pub path: String,
    pub size_bytes: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesTaskLog {
    /// Required by TES 1.1: always serialized, empty until the task is terminal.
    #[serde(default)]
    pub logs: Vec<TesExecutorLog>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    /// Required by TES 1.1: always serialized, empty until outputs exist.
    #[serde(default)]
    pub outputs: Vec<TesFileLog>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub system_logs: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct TesTask {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<TesState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<TesInput>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub outputs: Vec<TesOutput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<TesResources>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub executors: Vec<TesExecutor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub logs: Vec<TesTaskLog>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub creation_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = TesCreateTaskResponse)]
pub struct TesTaskResponse {
    pub id: String,
}

#[derive(Debug, Serialize)]
struct TesCancelResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[schema(as = TesListTasksResponse)]
pub struct TesTasksResponse {
    pub tasks: Vec<TesTask>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TesServiceType {
    group: &'static str,
    artifact: &'static str,
    version: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TesServiceOrganization {
    name: String,
    url: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TesServiceInfo {
    id: String,
    name: String,
    r#type: TesServiceType,
    description: String,
    organization: TesServiceOrganization,
    documentation_url: Option<String>,
    environment: String,
    version: String,
    storage: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[schema(example = json!({"status_code": 404, "msg": "TES task not found"}))]
pub struct TesErrorPayload {
    status_code: u16,
    msg: String,
    /// Machine-readable cause, present only where this facade defines one.
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ViewQuery {
    view: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListTasksQuery {
    view: Option<String>,
    page_size: Option<usize>,
    page_token: Option<String>,
    state: Option<String>,
    name_prefix: Option<String>,
}

struct TaskFilters {
    state: Option<TesState>,
    name_prefix: Option<String>,
    tags: Vec<(String, String)>,
}

impl TaskFilters {
    fn from_query(query: &ListTasksQuery, raw_query: Option<&str>) -> Result<Self, TesError> {
        Ok(Self {
            state: query.state.as_deref().map(TesState::parse).transpose()?,
            name_prefix: query
                .name_prefix
                .clone()
                .filter(|prefix| !prefix.is_empty()),
            tags: parse_tag_filters(raw_query),
        })
    }

    fn matches(&self, record: &JobRecord) -> bool {
        self.matches_base(record)
    }

    fn has_derived(&self) -> bool {
        self.tags
            .iter()
            .any(|(key, _)| DERIVED_TAG_KEYS.contains(&key.as_str()))
    }

    fn matches_base(&self, record: &JobRecord) -> bool {
        let JobPayload::Execution(spec) = &record.payload else {
            return false;
        };
        let tags = project_tags(spec);
        self.state.is_none_or(|state| tes_state(record) == state)
            && self.name_prefix.as_deref().is_none_or(|prefix| {
                spec.name
                    .as_deref()
                    .is_some_and(|name| name.starts_with(prefix))
            })
            && self
                .tags
                .iter()
                .filter(|(key, _)| !DERIVED_TAG_KEYS.contains(&key.as_str()))
                .all(|(key, value)| {
                    tags.get(key)
                        .is_some_and(|stored| value.is_empty() || stored == value)
                })
    }

    fn matches_details(&self, record: &JobRecord, details: &TaskDetails) -> bool {
        if !self.matches_base(record) {
            return false;
        }
        let JobPayload::Execution(spec) = &record.payload else {
            return false;
        };
        let mut tags = project_tags(spec);
        details.stamp(&record.job_id.to_string(), &mut tags);
        self.tags
            .iter()
            .filter(|(key, _)| DERIVED_TAG_KEYS.contains(&key.as_str()))
            .all(|(key, value)| {
                tags.get(key)
                    .is_some_and(|stored| value.is_empty() || stored == value)
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TesView {
    Minimal,
    Basic,
    Full,
}

impl TesView {
    fn parse(view: Option<&str>) -> Result<Self, TesError> {
        match view.unwrap_or("MINIMAL") {
            "MINIMAL" => Ok(Self::Minimal),
            "BASIC" => Ok(Self::Basic),
            "FULL" => Ok(Self::Full),
            other => Err(TesError::bad_request(format!("unknown view `{other}`"))),
        }
    }
}

impl TesState {
    fn parse(value: &str) -> Result<Self, TesError> {
        match value {
            "UNKNOWN" => Ok(Self::Unknown),
            "QUEUED" => Ok(Self::Queued),
            "INITIALIZING" => Ok(Self::Initializing),
            "RUNNING" => Ok(Self::Running),
            "PAUSED" => Ok(Self::Paused),
            "COMPLETE" => Ok(Self::Complete),
            "EXECUTOR_ERROR" => Ok(Self::ExecutorError),
            "SYSTEM_ERROR" => Ok(Self::SystemError),
            "CANCELED" => Ok(Self::Canceled),
            "CANCELING" => Ok(Self::Canceling),
            "PREEMPTED" => Ok(Self::Preempted),
            other => Err(TesError::bad_request(format!("unknown state `{other}`"))),
        }
    }
}

fn parse_tag_filters(raw_query: Option<&str>) -> Vec<(String, String)> {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for (field, value) in url::form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes()) {
        match field.as_ref() {
            "tag_key" => keys.push(value.into_owned()),
            "tag_value" => values.push(value.into_owned()),
            _ => {}
        }
    }
    keys.into_iter()
        .enumerate()
        .map(|(index, key)| (key, values.get(index).cloned().unwrap_or_default()))
        .collect()
}

#[utoipa::path(
    get,
    path = "/ga4gh/tes/v1/service-info",
    tag = "tes",
    summary = "Describe this GA4GH TES endpoint",
    description = r#"Describes this TES endpoint, the realm it serves and its conformance deviations.

**Authentication**: none; the route is deliberately public and every caller sees the same document.

**Behavior**
- Names the TES version spoken, the realm this endpoint serves, the running server version, and the
  deviations from full TES conformance a client must plan for: exactly one executor per task, and
  `PAUSED` is never entered.
- The organization url is the externally visible base url of the node that answered, taken from the
  forwarded headers only behind a trusted proxy and from the `Host` header otherwise."#,
    responses((
        status = 200,
        description = "Description of this TES endpoint and its conformance deviations",
        body = TesServiceInfo,
        example = json!({
            "id": "org.aruna.AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA",
            "name": "Aruna Realm AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA",
            "type": {
                "group": "org.ga4gh",
                "artifact": "tes",
                "version": "1.1.0"
            },
            "description": "Aruna TES facade over the internal execution job model.",
            "organization": {
                "name": "Aruna",
                "url": "https://node.example.test"
            },
            "documentation_url": "https://docs.aruna-engine.org",
            "environment": "dev",
            "version": "3.0.0-alpha.41",
            "storage": []
        })
    ))
)]
pub async fn service_info(
    State(state): State<Arc<ServerState>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let base_url = external_base_url(state.trusted_proxies(), peer.ip(), &headers);
    let info = TesServiceInfo {
        id: format!("org.aruna.{}", state.get_realm_id()),
        name: format!("Aruna Realm {}", state.get_realm_id()),
        r#type: TesServiceType {
            group: "org.ga4gh",
            artifact: "tes",
            version: TES_VERSION.to_string(),
        },
        // Deviations from full TES conformance surface here so a client can discover them.
        description: "Aruna TES facade over the internal execution job model. Deviations: \
             exactly one executor per task (multi-executor arrays are rejected); PAUSE is not \
             supported and never emitted."
            .to_string(),
        organization: TesServiceOrganization {
            name: "Aruna".to_string(),
            url: base_url,
        },
        documentation_url: Some("https://docs.aruna-engine.org".to_string()),
        environment: "dev".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        storage: Vec::new(),
    };
    tes_json_response(StatusCode::OK, info)
}

#[utoipa::path(
    post,
    path = "/ga4gh/tes/v1/tasks",
    tag = "tes",
    summary = "Create a TES task",
    description = r#"Accepts a task for asynchronous execution and returns the id to poll.

**Authentication**: realm bearer token, or HTTP Basic with an access key and secret issued by this
node; a path-restricted credential is refused. The caller needs WRITE on the target group. Basic
authentication takes the group from the credential and refuses an `aruna-engine.org/group` tag
naming a different one; a bearer token requires that tag.

**Behavior**
- 200 means the task was durably accepted and queued, never that it started or finished.
- State runs `QUEUED`, `INITIALIZING`, `RUNNING`, then `COMPLETE`, `EXECUTOR_ERROR`,
  `SYSTEM_ERROR` or `CANCELED`; `CANCELING` shows while a cancellation is in flight and `UNKNOWN`
  when the outcome cannot be determined. `PAUSED` and `PREEMPTED` are never emitted.
- A permanent, job-specific failure surfaces as `EXECUTOR_ERROR` and suppresses retry, while an
  infrastructure or retryable error surfaces as `SYSTEM_ERROR` and is re-planned, so the two
  classes stay distinguishable through the facade.
- An `aruna-engine.org/idempotency-key` tag deduplicates submissions per caller; reusing a key
  bound to a different task is a 409 carrying that task id. A replay is settled before any quota
  is read and is never quota-refused.
- On a user device this facade only proxies: the task is forwarded to a realm holder that pins the
  outputs to itself and resolves the referenced inputs, and the device never executes a task. The
  device forwards the caller's own bearer token, so basic authentication is refused there even
  though it is accepted on a realm node.
- An `aruna-engine.org/target` tag of `local` runs the task on a user device instead, for the owner
  of that device only. `realm`, the default, is the behavior above; either way the tag stays on the
  task and is reported back with it.
- A local task reads objects this device holds and writes each declared output to the device-local
  bucket its `s3://` url names, which must belong to the execution group and grant the owner WRITE;
  the run's own workspace bucket holds the staged inputs. Nothing about it is forwarded or
  replicated.
- A realm task reads every input from the bucket its `s3://` url names, at the version resolved
  when the task was accepted, so a later write to the source key never changes what a running task
  reads. Each declared output is written to the bucket its own url names. No bucket is created for
  a run and none is dropped afterwards.

**Limits** (all refused with 400)
- Exactly one executor whose `command` is the full argv.
- `id`, `state`, `logs` and `creation_time` are read only, as are the derived tags
  `aruna-engine.org/job-id`, `aruna-engine.org/logical-state`, `aruna-engine.org/executor-kind`
  and `aruna-engine.org/estimated-transfer-bytes`; naming one is refused with error code
  `reserved_tag`.
- Input and output urls must be `s3://bucket/key`; container paths must be absolute, canonical and
  may not overlap between inputs and outputs; at most 512 inputs and 1024 outputs, the same bound
  the immutable output record carries.
- An output path with POSIX wildcards additionally requires `path_prefix`, the literal ancestor
  stripped from each match before it is appended to the destination url.
- Unsupported: directory entries, inline input content, wildcards in an input path, volumes,
  executor stdin/stdout/stderr redirection and resource zones."#,
    request_body(
        content = TesTask,
        description = "Task definition: one executor, s3:// inputs and outputs, and optional resources and tags",
        example = json!({
            "name": "align-reads",
            "description": "align one fastq against the reference",
            "executors": [
                {
                    "image": "ghcr.io/example/aligner:1.4.0",
                    "command": [
                        "/usr/bin/align",
                        "--in",
                        "/data/input.fastq",
                        "--out",
                        "/data/out/aligned.bam"
                    ],
                    "workdir": "/data",
                    "env": {
                        "THREADS": "4"
                    }
                }
            ],
            "inputs": [
                {
                    "name": "reads",
                    "url": "s3://example-bucket/reads/input.fastq",
                    "path": "/data/input.fastq",
                    "type": "FILE"
                }
            ],
            "outputs": [
                {
                    "name": "aligned",
                    "url": "s3://example-bucket/results/aligned.bam",
                    "path": "/data/out/aligned.bam",
                    "type": "FILE"
                }
            ],
            "resources": {
                "cpu_cores": 4,
                "ram_gb": 8.0,
                "disk_gb": 20.0,
                "preemptible": false
            },
            "tags": {
                "aruna-engine.org/group": "01JABCDEF0123456789ABCDEFG",
                "aruna-engine.org/idempotency-key": "align-reads-2026-04-09-001"
            }
        })
    ),
    responses(
        (
            status = 200,
            description = "Task durably accepted and queued; the body carries the id to poll and cancel with",
            body = TesTaskResponse,
            example = json!({
                "id": "01JABCDEF0123456789ABCDEFG"
            })
        ),
        (status = 400, description = "Malformed task, an unsupported TES feature, an input that is not a readable object, more outputs than a task may declare, a reserved tag, an unknown target tag value, or the local target on a node that serves no device plane", body = TesErrorPayload),
        (status = 401, description = "Missing or invalid bearer token or basic credential", body = TesErrorPayload),
        (status = 403, description = "No WRITE permission on the target group, a group tag contradicting the credential, a path restricted credential, a routed authority refusing the submission, or a local task from someone other than this device's owner", body = TesErrorPayload),
        (status = 409, description = "The idempotency key tag is already bound to a different task, the composition conflicts on a staged key, or this device's compute plane is paused, absent or at its run ceiling. The group's standing compute quota also refuses this admission when its demand view understates the group, exactly as an exceeded cap does", body = TesErrorPayload),
        (status = 503, description = "Retryable admission failure, and the caller may create the task again with the same idempotency key: an unreachable family holder, a demand view that could not be read or did not settle, admission losing three transactions in a row to concurrent submissions of the same group, or an unhealthy id clock. A task forwarded from a device whose referenced inputs sit on no holder of its family answers the same 503 and retrying does not clear it; submit from a node that holds the inputs, or replicate them first", body = TesErrorPayload)
    ),
    security(("bearer_auth" = []), ("basic_auth" = []))
)]
pub async fn create_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Json(task): Json<TesTask>,
) -> Response {
    let caller = match authenticate_tes(&state, auth, &headers).await {
        Ok(caller) => caller,
        Err(error) => return error.into_response(),
    };

    let target = match task_target(&task.tags) {
        Ok(target) => target,
        Err(error) => return error.into_response(),
    };
    let caller = match local_owner(&state, caller, target).await {
        Ok(caller) => caller,
        Err(error) => return error.into_response(),
    };

    // Authorize before parsing: the group comes from the tag or credential
    // alone, so the unbounded payload is only validated for permitted callers.
    let group_id = match resolve_task_group(&task, caller.credential_group) {
        Ok(group_id) => group_id,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = ensure_group_write(&state, &caller.auth, group_id).await {
        return error.into_response();
    }

    let (spec, idempotency_key) = match map_execution_spec(&task, caller.credential_group, target) {
        Ok(mapped) => mapped,
        Err(error) => return error.into_response(),
    };
    let workspace_mode = target_modes(target).1;

    if target == ExecutionTarget::Local {
        return match submit_local_execution(
            &state.get_ctx(),
            LocalExecutionConfig {
                spec,
                owner: caller.auth.user_id,
                node_id: state.get_node_id(),
                idempotency_key,
                workspace_mode,
                retention_ms: state.rocrate_limits().artifact_retention_ms,
            },
        )
        .await
        {
            Ok(result) => tes_json_response(
                StatusCode::OK,
                TesTaskResponse {
                    id: result.job_id.to_string(),
                },
            ),
            Err(error) => TesError::from_server(map_local_error(error)).into_response(),
        };
    }

    let forwarded = match super::jobs::forwarded_job_auth(bearer) {
        Ok(token) => token.or_else(|| {
            Some(aruna_operations::metadata::AuthToken::internal(
                caller.auth.clone(),
            ))
        }),
        Err(error) => return error.into_response(),
    };
    match submit_external_job(
        &state.get_ctx(),
        spec,
        caller.auth.user_id,
        idempotency_key,
        workspace_mode,
        None,
        state.rocrate_limits().artifact_retention_ms,
        forwarded,
    )
    .await
    {
        Ok(result) => tes_json_response(
            StatusCode::OK,
            TesTaskResponse {
                id: result.job_id.to_string(),
            },
        ),
        Err(error) => TesError::from_submit(error).into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/ga4gh/tes/v1/tasks/{id}",
    tag = "tes",
    summary = "Get a single TES task",
    description = r#"Returns one task of the calling user, projected to the requested view.

**Authentication**: realm bearer token, or HTTP Basic with an access key and secret issued by this
node; a path-restricted credential is refused. Tasks are self scoped: another user's task, a task
outside the group of the basic credential, and an id that is not a parsable task of this facade all
answer 404, so the existence of a task is never disclosed.

**Behavior**
- A distributed execution task is reduced from its replicated records by whichever node answers, so
  it carries the same logical view as the native REST status; any other task is read from the node
  that owns it and only that node answers absence.
- The state reported is the polling contract of task creation: `QUEUED`, `INITIALIZING`, `RUNNING`,
  then `COMPLETE`, `EXECUTOR_ERROR`, `SYSTEM_ERROR` or `CANCELED`, with `CANCELING` while a
  cancellation is in flight and `UNKNOWN` when the outcome cannot be determined.
- `UNKNOWN` is also what a distributed task reports while no execution has succeeded, because
  realm-wide failure can never be inferred from silence.
- `view` selects the projection: `MINIMAL` (the default) returns only `id` and `state`, `BASIC` adds
  the task definition, tags, timing and captured output files, and `FULL` adds executor stdout and
  stderr and the system logs.
- `BASIC` and `FULL` also carry the derived read-only tags `aruna-engine.org/job-id` always,
  `aruna-engine.org/logical-state` once a family is known, and `aruna-engine.org/executor-kind` plus
  `aruna-engine.org/estimated-transfer-bytes` once this responder stored a placement.
- Output urls in the task log name the exact `versionId` the canonical execution wrote, which is not
  necessarily the object's current version: a duplicate execution admitted during a partition, or
  any later write, may have made another version current.
- Executor logs, including the exit code, appear only once the task is terminal.

**Limits**
- `view` must be `MINIMAL`, `BASIC` or `FULL`."#,
    params(
        ("id" = String, Path, description = "TES task id (the JobId): the 26 character ULID returned by task creation"),
        ("view" = Option<String>, Query, description = "Projection to apply: `MINIMAL` (the default), `BASIC` or `FULL`")
    ),
    responses(
        (
            status = 200,
            description = "The task, projected to the requested view",
            body = TesTask,
            example = json!({
                "id": "01JABCDEF0123456789ABCDEFG",
                "state": "COMPLETE",
                "name": "align-reads",
                "inputs": [
                    {
                        "name": "reads",
                        "url": "s3://example-bucket/reads/input.fastq",
                        "path": "/data/input.fastq",
                        "type": "FILE"
                    }
                ],
                "outputs": [
                    {
                        "name": "aligned",
                        "url": "s3://example-bucket/results/aligned.bam",
                        "path": "/data/out/aligned.bam",
                        "type": "FILE"
                    }
                ],
                "resources": {
                    "cpu_cores": 4,
                    "ram_gb": 8.0,
                    "disk_gb": 20.0,
                    "preemptible": false
                },
                "executors": [
                    {
                        "image": "ghcr.io/example/aligner:1.4.0",
                        "command": [
                            "/usr/bin/align",
                            "--in",
                            "/data/input.fastq",
                            "--out",
                            "/data/out/aligned.bam"
                        ],
                        "workdir": "/data",
                        "env": {
                            "THREADS": "4"
                        }
                    }
                ],
                "tags": {
                    "aruna-engine.org/group": "01JABCDEF0123456789ABCDEFG",
                    "aruna-engine.org/job-id": "01JABCDEF0123456789ABCDEFG",
                    "aruna-engine.org/logical-state": "running",
                    "aruna-engine.org/executor-kind": "docker",
                    "aruna-engine.org/estimated-transfer-bytes": "4096"
                },
                "logs": [
                    {
                        "logs": [
                            {
                                "start_time": "2026-04-09T14:23:11.123+00:00",
                                "end_time": "2026-04-09T14:25:02.900+00:00",
                                "stdout": "aligned 1200 reads",
                                "exit_code": 0
                            }
                        ],
                        "start_time": "2026-04-09T14:23:11.123+00:00",
                        "end_time": "2026-04-09T14:25:02.900+00:00",
                        "outputs": [
                            {
                                "url": "s3://example-bucket/results/aligned.bam",
                                "path": "/data/out/aligned.bam",
                                "size_bytes": "20480"
                            }
                        ]
                    }
                ],
                "creation_time": "2026-04-09T14:23:10.010+00:00"
            })
        ),
        (status = 400, description = "`view` is not one of MINIMAL, BASIC or FULL", body = TesErrorPayload),
        (status = 401, description = "Missing or invalid bearer token or basic credential", body = TesErrorPayload),
        (status = 404, description = "No such task for this caller; also returned for another user's task, a task outside the credential's group and an unparsable id", body = TesErrorPayload),
        (status = 503, description = "The node owning the task is unreachable, so its state is unknown rather than missing; the caller may retry", body = TesErrorPayload)
    ),
    security(("bearer_auth" = []), ("basic_auth" = []))
)]
pub async fn get_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(query): Query<ViewQuery>,
) -> Response {
    let caller = match authenticate_tes(&state, auth, &headers).await {
        Ok(caller) => caller,
        Err(error) => return error.into_response(),
    };
    let view = match TesView::parse(query.view.as_deref()) {
        Ok(view) => view,
        Err(error) => return error.into_response(),
    };
    let job_id = match JobId::from_str(&id) {
        Ok(job_id) => job_id,
        Err(_) => return TesError::not_found("TES task not found").into_response(),
    };

    let forwarded = match super::jobs::forwarded_job_auth(bearer) {
        Ok(token) => token.or_else(|| {
            Some(aruna_operations::metadata::AuthToken::internal(
                caller.auth.clone(),
            ))
        }),
        Err(error) => return TesError::from_server(error).into_response(),
    };
    // TES and native REST project the same family and exact output version identifiers.
    let (record, details) = match family_report(&state.get_ctx(), &caller.auth, job_id).await {
        Some(Ok(report)) => (family_record(&report), TaskDetails::from_report(&report)),
        Some(Err(error)) => return TesError::from_job_route(error).into_response(),
        // The owner is the sole 404 authority; a non-owner routes or reports 503.
        None => {
            match read_record_routed(&state.get_ctx(), caller.auth.user_id, job_id, forwarded).await
            {
                Ok(Some(record)) => (record, TaskDetails::default()),
                Ok(None) => return TesError::not_found("TES task not found").into_response(),
                Err(error) => return TesError::from_job_route(error).into_response(),
            }
        }
    };
    // Only execution jobs are TES tasks; other job kinds are not addressable here.
    if !task_in_group(&record, caller.credential_group) {
        return TesError::not_found("TES task not found").into_response();
    }

    let base_url = external_base_url(state.trusted_proxies(), peer.ip(), &headers);
    tes_json_response(
        StatusCode::OK,
        project_task(&record, &details, view, &base_url),
    )
}

async fn task_record(
    state: &ServerState,
    auth: &AuthContext,
    record: JobRecord,
) -> Result<(JobRecord, TaskDetails), TesError> {
    match family_report(&state.get_ctx(), auth, record.job_id).await {
        Some(Ok(report)) => Ok((family_record(&report), TaskDetails::from_report(&report))),
        Some(Err(error)) => Err(TesError::from_job_route(error)),
        None => Ok((record, TaskDetails::default())),
    }
}

async fn list_derived(
    state: &ServerState,
    caller: &TesCaller,
    filters: &TaskFilters,
    mut cursor: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<(JobRecord, TaskDetails)>, Option<Vec<u8>>), TesError> {
    let mut selected = Vec::with_capacity(limit);
    let mut page_cursor = None;
    loop {
        let (mut records, next_cursor) =
            list_owned_jobs(&state.get_ctx(), caller.auth.user_id, cursor, 1, |record| {
                filters.matches_base(record) && task_in_group(record, caller.credential_group)
            })
            .await
            .map_err(TesError::internal)?;
        let Some(record) = records.pop() else {
            return Ok((selected, None));
        };
        let task = task_record(state, &caller.auth, record).await?;
        if filters.matches_details(&task.0, &task.1) {
            if selected.len() == limit {
                return Ok((selected, page_cursor));
            }
            page_cursor = next_cursor.clone();
            selected.push(task);
        }
        let Some(next_cursor) = next_cursor else {
            return Ok((selected, None));
        };
        cursor = Some(next_cursor);
    }
}

#[utoipa::path(
    get,
    path = "/ga4gh/tes/v1/tasks",
    tag = "tes",
    summary = "List the caller's TES tasks",
    description = r#"Lists the tasks the calling user created, newest first.

**Authentication**: realm bearer token, or HTTP Basic with an access key and secret issued by this
node; a path-restricted credential is refused. The listing is keyed by the caller, and a basic
credential additionally sees only tasks of that credential's group.

**Behavior**
- Only tasks owned by the node that answers are listed, so tasks submitted through another node of
  the realm are omitted rather than fetched from it.
- Paging is cursor based and forward only: read `next_page_token` from a page and send it back as
  `page_token`, and treat its absence as the end of the listing.
- State, name and tag filters are applied before a page is filled, so a short page means the
  listing is exhausted, not that everything was filtered away.
- `view` projects every task in the page: `MINIMAL` (the default) returns only `id` and `state`,
  `BASIC` adds the task definition, tags, timing and captured output files, and `FULL` adds executor
  stdout and stderr and the system logs.
- `BASIC` and `FULL` also carry the derived read-only tags `aruna-engine.org/job-id`,
  `aruna-engine.org/logical-state`, `aruna-engine.org/executor-kind` and
  `aruna-engine.org/estimated-transfer-bytes` wherever this responder knows them.
- `tag_value` pairs by position with `tag_key`, and an empty or missing value matches any value of
  that key.

**Limits**
- `page_size` defaults to 256, is capped at 512, and 0 is treated as unset.
- An empty `name_prefix` is ignored."#,
    params(
        ("view" = Option<String>, Query, description = "Projection applied to every task in the page: `MINIMAL` (the default), `BASIC` or `FULL`"),
        ("page_size" = Option<usize>, Query, description = "Max tasks per page: default 256, capped at 512, and 0 is treated as unset"),
        ("page_token" = Option<String>, Query, description = "Opaque page token: the `next_page_token` of the previous page; omit it to start at the newest task"),
        ("state" = Option<String>, Query, description = "TES task state to filter by, for example `QUEUED`, `RUNNING` or `COMPLETE`"),
        ("name_prefix" = Option<String>, Query, description = "Task name prefix; only tasks whose name starts with it are returned, and an empty value is ignored"),
        ("tag_key" = Vec<String>, Query, description = "Repeated tag keys; a task matches only when it carries every key given"),
        ("tag_value" = Vec<String>, Query, description = "Repeated tag values, paired by position with `tag_key`; an empty or missing value matches any value of that key")
    ),
    responses(
        (
            status = 200,
            description = "Node-local tasks page; tasks owned by other nodes are omitted, and a missing `next_page_token` means this was the last page",
            body = TesTasksResponse,
            example = json!({
                "tasks": [
                    {
                        "id": "01JABCDEF0123456789ABCDEFG",
                        "state": "RUNNING"
                    },
                    {
                        "id": "01JMETADATA0123456789ABCDE",
                        "state": "COMPLETE"
                    }
                ],
                "next_page_token": "ZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXp7"
            })
        ),
        (status = 400, description = "An invalid `view`, an unknown `state` name, or a `page_token` that is not a token of this listing", body = TesErrorPayload),
        (status = 401, description = "Missing or invalid bearer token or basic credential", body = TesErrorPayload)
    ),
    security(("bearer_auth" = []), ("basic_auth" = []))
)]
pub async fn list_tasks(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
    Query(query): Query<ListTasksQuery>,
) -> Response {
    let caller = match authenticate_tes(&state, auth, &headers).await {
        Ok(caller) => caller,
        Err(error) => return error.into_response(),
    };
    let view = match TesView::parse(query.view.as_deref()) {
        Ok(view) => view,
        Err(error) => return error.into_response(),
    };
    let filters = match TaskFilters::from_query(&query, raw_query.as_deref()) {
        Ok(filters) => filters,
        Err(error) => return error.into_response(),
    };
    let cursor = match decode_page_token(query.page_token.as_deref()) {
        Ok(cursor) => cursor,
        Err(error) => return error.into_response(),
    };
    let limit = query
        .page_size
        .filter(|size| *size > 0)
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .min(MAX_PAGE_SIZE);

    let base_url = external_base_url(state.trusted_proxies(), peer.ip(), &headers);
    let page = if filters.has_derived() {
        list_derived(&state, &caller, &filters, cursor, limit).await
    } else {
        let (records, next_cursor) = match list_owned_jobs(
            &state.get_ctx(),
            caller.auth.user_id,
            cursor,
            limit,
            |record| filters.matches(record) && task_in_group(record, caller.credential_group),
        )
        .await
        {
            Ok(page) => page,
            Err(error) => return TesError::internal(error).into_response(),
        };
        let mut tasks = Vec::with_capacity(records.len());
        for record in records {
            match task_record(&state, &caller.auth, record).await {
                Ok(task) => tasks.push(task),
                Err(error) => return error.into_response(),
            }
        }
        Ok((tasks, next_cursor))
    };
    let (records, next_cursor) = match page {
        Ok(page) => page,
        Err(error) => return error.into_response(),
    };
    let tasks = records
        .iter()
        .map(|(record, details)| project_task(record, details, view, &base_url))
        .collect();

    tes_json_response(
        StatusCode::OK,
        TesTasksResponse {
            tasks,
            next_page_token: next_cursor.map(|cursor| URL_SAFE_NO_PAD.encode(cursor)),
        },
    )
}

#[utoipa::path(
    post,
    path = "/ga4gh/tes/v1/tasks/{id}:cancel",
    tag = "tes",
    summary = "Cancel a TES task",
    description = r#"Requests cancellation of a task the calling user created.

**Authentication**: realm bearer token, or HTTP Basic with an access key and secret issued by this
node; a path-restricted credential is refused. Self scoped exactly like the read: another user's
task, a task outside the group of the basic credential and an id that does not parse all answer
404.

**Behavior**
- TES addresses this as a POST whose final path segment is the task id followed by the literal
  `:cancel` action suffix; a POST that omits the suffix is refused.
- The request is carried out on the node that owns the task.
- A 200 records only that cancellation was requested, or that the task had already reached a
  terminal state; the executor may still be winding down, so poll the task until it reports
  `CANCELED`."#,
    params(("id" = String, Path, description = "TES task id (the JobId) followed by the `:cancel` suffix, for example `01JABCDEF0123456789ABCDEFG:cancel`")),
    responses(
        (
            status = 200,
            description = "Cancellation requested, or the task was already terminal; the body is an empty JSON object and the task may still be stopping",
            body = Object,
            content_type = "application/json",
            example = json!({})
        ),
        (status = 400, description = "The final path segment lacks the `:cancel` action suffix", body = TesErrorPayload),
        (status = 401, description = "Missing or invalid bearer token or basic credential", body = TesErrorPayload),
        (status = 404, description = "No such task for this caller; also returned for another user's task, a task outside the credential's group and an unparsable id", body = TesErrorPayload),
        (status = 503, description = "The node owning the task is unreachable, so the cancellation was not delivered anywhere; the caller may retry", body = TesErrorPayload)
    ),
    security(("bearer_auth" = []), ("basic_auth" = []))
)]
pub async fn cancel_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedBearer>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let caller = match authenticate_tes(&state, auth, &headers).await {
        Ok(caller) => caller,
        Err(error) => return error.into_response(),
    };
    // TES addresses cancellation as `POST /tasks/{id}:cancel`; the `:cancel` action
    // suffix rides on the final path segment, so strip it here.
    let Some(raw_id) = id.strip_suffix(":cancel") else {
        return TesError::bad_request("cancel requires the :cancel action suffix").into_response();
    };
    let job_id = match JobId::from_str(raw_id) {
        Ok(job_id) => job_id,
        Err(_) => return TesError::not_found("TES task not found").into_response(),
    };
    let forwarded = match super::jobs::forwarded_job_auth(bearer) {
        Ok(token) => token.or_else(|| {
            Some(aruna_operations::metadata::AuthToken::internal(
                caller.auth.clone(),
            ))
        }),
        Err(error) => return TesError::from_server(error).into_response(),
    };
    let record = match family_report(&state.get_ctx(), &caller.auth, job_id).await {
        Some(Ok(report)) => family_record(&report),
        Some(Err(error)) => return TesError::from_job_route(error).into_response(),
        None => match read_record_routed(
            &state.get_ctx(),
            caller.auth.user_id,
            job_id,
            forwarded.clone(),
        )
        .await
        {
            Ok(Some(record)) => record,
            Ok(None) => return TesError::not_found("TES task not found").into_response(),
            Err(error) => return TesError::from_job_route(error).into_response(),
        },
    };
    if !task_in_group(&record, caller.credential_group) {
        return TesError::not_found("TES task not found").into_response();
    }

    match cancel_job_routed(
        &state.get_ctx(),
        &state.jobs_runtime(),
        caller.auth.user_id,
        job_id,
        forwarded,
    )
    .await
    {
        Ok(RoutedCancelOutcome::NotFound) => {
            TesError::not_found("TES task not found").into_response()
        }
        Ok(RoutedCancelOutcome::AlreadyTerminal(_) | RoutedCancelOutcome::Requested(_)) => {
            tes_json_response(StatusCode::OK, TesCancelResponse {})
        }
        Err(error) => TesError::from_job_route(error).into_response(),
    }
}

/// Resolve the effective group from the tag or credential alone, without
/// touching the rest of the untrusted task payload.
fn resolve_task_group(task: &TesTask, credential_group: Option<Ulid>) -> Result<Ulid, TesError> {
    let group_id = match task.tags.get(GROUP_TAG_KEY) {
        Some(group) => Ulid::from_string(group).map_err(|_| {
            TesError::bad_request(format!("`{GROUP_TAG_KEY}` is not a valid group id"))
        })?,
        None => credential_group.ok_or_else(|| {
            TesError::bad_request(format!(
                "a `{GROUP_TAG_KEY}` tag is required for bearer authentication"
            ))
        })?,
    };
    if credential_group.is_some_and(|credential_group| credential_group != group_id) {
        return Err(TesError::forbidden(
            "group tag does not match the caller credential",
        ));
    }
    Ok(group_id)
}

/// How a task reads its inputs. No run owns a bucket: a realm run reads the
/// buckets its inputs live in, and a device refuses mounts, so a local run
/// stages the same objects as files.
fn target_modes(target: ExecutionTarget) -> (InputMode, WorkspaceMode) {
    match target {
        ExecutionTarget::Realm => (InputMode::Mount, WorkspaceMode::None),
        ExecutionTarget::Local => (InputMode::Snapshot, WorkspaceMode::None),
    }
}

/// Map a TES task onto the internal execution plan and optional dedup key.
/// Pure and self-contained: the group-write permission check happens separately.
fn map_execution_spec(
    task: &TesTask,
    credential_group: Option<Ulid>,
    target: ExecutionTarget,
) -> Result<(ExecutionSpec, Option<String>), TesError> {
    let executor = validate_task_shape(task)?;
    let group_id = resolve_task_group(task, credential_group)?;
    let (input_mode, _) = target_modes(target);
    let inputs = map_task_inputs(task, input_mode)?;
    let file_outputs = map_task_outputs(task, &inputs)?;
    let resources = map_task_resources(task)?;

    let spec = ExecutionSpec {
        group_id,
        name: task.name.clone(),
        description: task.description.clone(),
        tags: task.tags.clone(),
        image: executor.image.clone(),
        // TES `command` is the full argv, so it replaces the image entrypoint.
        entrypoint: Some(executor.command.clone()),
        command: Vec::new(),
        workdir: executor.workdir.clone(),
        env: executor.env.clone(),
        resources,
        executor_constraint: task.tags.get(EXECUTOR_TAG_KEY).cloned(),
        inputs,
        file_outputs,
        workspace_outputs: Vec::new(),
        output_prefixes: Vec::new(),
        collision_policy: Default::default(),
    };

    // Ingress adds the per-user namespace to this raw idempotency key.
    Ok((spec, task.tags.get(IDEMPOTENCY_TAG_KEY).cloned()))
}

fn validate_task_shape(task: &TesTask) -> Result<&TesExecutor, TesError> {
    if task.id.is_some()
        || task.state.is_some()
        || !task.logs.is_empty()
        || task.creation_time.is_some()
    {
        return Err(TesError::bad_request(
            "task id, state, logs, and creation_time are read-only",
        ));
    }
    if let Some(key) = reserved_tag(&task.tags) {
        return Err(TesError::coded(
            StatusCode::BAD_REQUEST,
            format!("tag `{key}` is derived at read time and read-only"),
            "reserved_tag",
        ));
    }
    let executor = match task.executors.as_slice() {
        [executor] => executor,
        [] => {
            return Err(TesError::bad_request(
                "a task requires exactly one executor",
            ));
        }
        _ => {
            return Err(TesError::bad_request(
                "multiple executors are not supported; this facade runs a single executor per task",
            ));
        }
    };

    if executor.image.trim().is_empty() {
        return Err(TesError::bad_request("executor image is required"));
    }
    if executor.command.is_empty() {
        return Err(TesError::bad_request("executor command is required"));
    }
    if let Some(workdir) = executor.workdir.as_deref() {
        validate_path(workdir, "executor workdir", true)?;
    }
    if executor.stdin.is_some() || executor.stdout.is_some() || executor.stderr.is_some() {
        return Err(TesError::bad_request(
            "executor stdin, stdout, and stderr paths are not supported",
        ));
    }
    if !task.volumes.is_empty() {
        return Err(TesError::bad_request("task volumes are not supported"));
    }

    Ok(executor)
}

fn map_task_inputs(task: &TesTask, input_mode: InputMode) -> Result<Vec<InputSelection>, TesError> {
    if task.inputs.len() > MAX_TASK_IO {
        return Err(TesError::bad_request("too many task inputs"));
    }
    let mut inputs: Vec<InputSelection> = Vec::with_capacity(task.inputs.len());
    for input in &task.inputs {
        let input = map_input(input, input_mode)?;
        if inputs
            .iter()
            .any(|existing| existing.container_path == input.container_path)
        {
            return Err(TesError::bad_request("duplicate input path"));
        }
        if let Some(path) = input.container_path.as_deref()
            && inputs.iter().any(|existing| {
                existing.container_path.as_deref().is_some_and(|other| {
                    FilePath::new(path).starts_with(other) || FilePath::new(other).starts_with(path)
                })
            })
        {
            return Err(TesError::bad_request("input paths overlap"));
        }
        inputs.push(input);
    }
    Ok(inputs)
}

fn map_task_outputs(
    task: &TesTask,
    inputs: &[InputSelection],
) -> Result<Vec<OutputSelection>, TesError> {
    if task.outputs.len() > MAX_EXECUTION_OUTPUTS {
        return Err(TesError::bad_request("too many task outputs"));
    }
    let mut file_outputs: Vec<OutputSelection> = Vec::with_capacity(task.outputs.len());
    for output in &task.outputs {
        let output = map_output(output)?;
        if file_outputs
            .iter()
            .any(|existing| existing.container_path == output.container_path)
        {
            return Err(TesError::bad_request("duplicate output path"));
        }
        if file_outputs
            .iter()
            .any(|existing| existing.destination == output.destination)
        {
            return Err(TesError::bad_request("duplicate output destination"));
        }
        file_outputs.push(output);
    }
    for output in &file_outputs {
        // A pattern is captured under its literal prefix, which is the directory
        // the container must be able to write.
        let pattern = has_wildcard(&output.container_path)
            .then(|| output_glob(&output.container_path))
            .transpose()
            .map_err(|_| TesError::bad_request("invalid output path"))?;
        let parent = if pattern.is_some() {
            literal_prefix(&output.container_path)
                .map_err(|_| TesError::bad_request("invalid output parent path"))?
        } else {
            FilePath::new(&output.container_path)
                .parent()
                .ok_or_else(|| TesError::bad_request("invalid output parent path"))?
                .to_path_buf()
        };
        let parent = parent
            .to_str()
            .ok_or_else(|| TesError::bad_request("invalid output parent path"))?;
        if parent == "/" {
            return Err(TesError::bad_request("root output parent is forbidden"));
        }
        for input in inputs {
            if let Some(path) = input.container_path.as_deref()
                && (path == output.container_path
                    || pattern.as_ref().is_some_and(|glob| glob.is_match(path))
                    || paths_overlap(path, parent)
                        .map_err(|_| TesError::bad_request("invalid input or output path"))?)
            {
                return Err(TesError::bad_request("input and output paths overlap"));
            }
        }
    }

    Ok(file_outputs)
}

fn map_task_resources(task: &TesTask) -> Result<ComputeResources, TesError> {
    let cpu_cores = task.resources.as_ref().and_then(|r| r.cpu_cores);
    if cpu_cores == Some(0) {
        return Err(TesError::bad_request("invalid cpu_cores"));
    }
    if task
        .resources
        .as_ref()
        .is_some_and(|resources| !resources.zones.is_empty())
    {
        return Err(TesError::bad_request("resource zones are not supported"));
    }

    let ram_bytes = task
        .resources
        .as_ref()
        .and_then(|r| r.ram_gb)
        .map(|gb| gb_to_bytes(gb, "ram_gb"))
        .transpose()?;
    let disk_bytes = task
        .resources
        .as_ref()
        .and_then(|r| r.disk_gb)
        .map(|gb| gb_to_bytes(gb, "disk_gb"))
        .transpose()?;
    Ok(ComputeResources {
        cpu_cores,
        ram_bytes,
        disk_bytes,
        max_walltime_ms: None,
        preemptible: task
            .resources
            .as_ref()
            .and_then(|resources| resources.preemptible)
            .unwrap_or(false),
    })
}

fn map_input(input: &TesInput, mode: InputMode) -> Result<InputSelection, TesError> {
    if input.kind != TesFileType::File {
        return Err(TesError::bad_request("directory inputs are not supported"));
    }
    if input.content.is_some() {
        return Err(TesError::bad_request(
            "inline input content is not supported",
        ));
    }
    let url = input
        .url
        .as_deref()
        .ok_or_else(|| TesError::bad_request("input url is required"))?;
    let (bucket, key) = parse_s3_url(url, "input")?;
    validate_path(&input.path, "input path", false)?;
    // TES 1.1 defines wildcards for outputs only; an input path is one file.
    if has_wildcard(&input.path) {
        return Err(TesError::bad_request(
            "input path must not contain wildcards",
        ));
    }
    Ok(InputSelection {
        source: InputSource::S3 {
            bucket,
            key,
            version_id: None,
        },
        source_node_id: None,
        dest_key: input.path[1..].to_string(),
        mode,
        container_path: Some(input.path.clone()),
        name: input.name.clone(),
        description: input.description.clone(),
    })
}

fn map_output(output: &TesOutput) -> Result<OutputSelection, TesError> {
    if output.kind != TesFileType::File {
        return Err(TesError::bad_request("directory outputs are not supported"));
    }
    validate_path(&output.path, "output path", false)?;
    let path_prefix = output_prefix(output)?;
    let url = output
        .url
        .as_deref()
        .ok_or_else(|| TesError::bad_request("output url is required"))?;
    let (bucket, key) = parse_s3_url(url, "output")?;
    Ok(OutputSelection {
        container_path: output.path.clone(),
        path_prefix,
        destination_node_id: None,
        destination: OutputDestination::S3 { bucket, key },
        name: output.name.clone(),
        description: output.description.clone(),
    })
}

/// TES 1.1 makes `path_prefix` required when `path` carries POSIX wildcards and
/// ignored otherwise, so a wildcard-free output drops any prefix it was sent.
fn output_prefix(output: &TesOutput) -> Result<Option<String>, TesError> {
    if !has_wildcard(&output.path) {
        return Ok(None);
    }
    output_glob(&output.path).map_err(|error| {
        TesError::bad_request(format!(
            "output path `{}` is not a valid pattern: {error}",
            output.path
        ))
    })?;
    let prefix = output.path_prefix.as_deref().ok_or_else(|| {
        TesError::bad_request(format!(
            "output path `{}` contains wildcards and requires path_prefix",
            output.path
        ))
    })?;
    validate_path(prefix, "output path_prefix", false)?;
    if has_wildcard(prefix) || output_suffix(&output.path, prefix).is_none() {
        return Err(TesError::bad_request(format!(
            "output path_prefix `{prefix}` must be a literal ancestor of `{}`",
            output.path
        )));
    }
    Ok(Some(prefix.to_string()))
}

fn parse_s3_url(url: &str, role: &str) -> Result<(String, String), TesError> {
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| TesError::bad_request(format!("only s3:// {role} urls are supported")))?;
    let (bucket, key) = rest
        .split_once('/')
        .ok_or_else(|| TesError::bad_request(format!("s3 {role} url must be s3://bucket/key")))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(TesError::bad_request(format!(
            "s3 {role} url must be s3://bucket/key"
        )));
    }
    Ok((bucket.to_string(), key.to_string()))
}

fn validate_path(value: &str, role: &str, allow_root: bool) -> Result<(), TesError> {
    let invalid = !value.starts_with('/')
        || value.contains('\0')
        || (!allow_root && value == "/")
        || (value != "/"
            && value
                .split('/')
                .skip(1)
                .any(|component| component.is_empty() || component == "." || component == ".."));
    if invalid {
        return Err(TesError::bad_request(format!(
            "{role} must be an absolute canonical path"
        )));
    }
    Ok(())
}

fn gb_to_bytes(gb: f64, field: &str) -> Result<u64, TesError> {
    let bytes = (gb * 1_000_000_000.0) as u64;
    if !gb.is_finite() || gb <= 0.0 || bytes == 0 || bytes > i64::MAX as u64 {
        return Err(TesError::bad_request(format!("invalid {field}")));
    }
    Ok(bytes)
}

/// Map an internal job state onto its TES external state. `Failed` splits on
/// evidence: a non-zero container exit is an executor error; post-processing and
/// evidence-free failures are system errors. `Indeterminate` maps to TES `UNKNOWN`.
fn tes_state(record: &JobRecord) -> TesState {
    if record.cancel_requested && !record.state.is_terminal() {
        return TesState::Canceling;
    }
    match record.state {
        JobState::Queued | JobState::Claimed => TesState::Queued,
        JobState::Preparing | JobState::Ready => TesState::Initializing,
        JobState::Running => TesState::Running,
        JobState::Cancelling => TesState::Canceling,
        JobState::Indeterminate => TesState::Unknown,
        JobState::Succeeded => TesState::Complete,
        JobState::Failed => match &record.result {
            Some(JobResultPayload::Execution {
                exit_code: Some(code),
                ..
            }) if *code != 0 => TesState::ExecutorError,
            _ => TesState::SystemError,
        },
        JobState::Cancelled => TesState::Canceled,
    }
}

/// Reduces a family to the local row used by every TES projection.
/// Only canonical success supplies exact output versions, excluding later unrelated writes.
fn family_record(report: &FamilyReport) -> JobRecord {
    let mut record = JobRecord::new(
        report.job.job_id,
        JobPayload::Execution(report.spec.payload.clone()),
        report.spec.created_by,
        report.spec.origin_node_id,
        report.spec.created_at_ms,
        report.job.updated_at_ms,
        None,
    );
    record.state = report.job.state;
    record.attempts = report.job.attempts;
    record.cancel_requested = report.cancel_requested;
    record.workspace_mode = report.job.workspace_mode;
    record.workspace_bucket = report.job.workspace_bucket.clone();
    record.retention_ms = report.spec.retention_ms;
    record.started_at_ms = report.started_at_ms;
    record.finished_at_ms = report.job.finished_at_ms;
    record.last_error = report.job.last_error.clone();
    record.result = matches!(report.job.state, JobState::Succeeded | JobState::Failed).then(|| {
        JobResultPayload::Execution {
            exit_code: report
                .canonical_result
                .as_ref()
                .and_then(|result| result.exit_code),
            workspace_bucket: report.job.workspace_bucket.clone(),
            outputs: if report.job.state == JobState::Succeeded {
                report.outputs.clone()
            } else {
                Vec::new()
            },
            stdout: result_tail(report, |result| result.stdout.as_ref()),
            stderr: result_tail(report, |result| result.stderr.as_ref()),
            output_digest: report
                .canonical_result
                .as_ref()
                .and_then(|result| result.output_digest),
        }
    });
    record
}

/// One bounded log tail of the canonical result. A missing tail reads as an
/// empty stream, which is what a run with no output produced anyway.
fn result_tail(
    report: &FamilyReport,
    pick: impl Fn(&PhysicalExecutionResult) -> Option<&ResultMessage>,
) -> String {
    report
        .canonical_result
        .as_ref()
        .and_then(pick)
        .map(|tail| tail.as_str().to_string())
        .unwrap_or_default()
}

/// Details a TES tag exposes at read time. They come from the same family and
/// plan `GET /jobs/{id}` reports and are never kept on the task.
#[derive(Debug, Default)]
struct TaskDetails {
    logical_state: Option<String>,
    executor_kind: Option<String>,
    transfer_bytes: Option<u64>,
}

impl TaskDetails {
    fn from_report(report: &FamilyReport) -> Self {
        // Only a plan that selected a target is a placement; without one the
        // transfer estimate names nothing.
        let placed = report.plan.as_ref().filter(|plan| plan.target.is_some());
        Self {
            logical_state: Some(report.state.name().to_string()),
            executor_kind: placed
                .and_then(|plan| plan.target.as_ref())
                .map(|target| target.executor_kind.clone()),
            transfer_bytes: placed.map(|plan| plan.estimated_transfer_bytes),
        }
    }

    fn stamp(&self, id: &str, tags: &mut BTreeMap<String, String>) {
        tags.insert(JOB_ID_TAG_KEY.to_string(), id.to_string());
        if let Some(state) = &self.logical_state {
            tags.insert(LOGICAL_STATE_TAG_KEY.to_string(), state.clone());
        }
        if let Some(kind) = &self.executor_kind {
            tags.insert(EXECUTOR_KIND_TAG_KEY.to_string(), kind.clone());
        }
        if let Some(bytes) = self.transfer_bytes {
            tags.insert(TRANSFER_BYTES_TAG_KEY.to_string(), bytes.to_string());
        }
    }
}

fn project_task(
    record: &JobRecord,
    details: &TaskDetails,
    view: TesView,
    base_url: &str,
) -> TesTask {
    let id = record.job_id.to_string();
    let state = tes_state(record);
    if view == TesView::Minimal {
        return TesTask {
            id: Some(id),
            state: Some(state),
            ..Default::default()
        };
    }

    let JobPayload::Execution(spec) = &record.payload else {
        // Never reached for a TES task id, but keep the projection total.
        return TesTask {
            id: Some(id),
            state: Some(state),
            ..Default::default()
        };
    };

    // Docker runs entrypoint + command together; project the full argv.
    let command = match &spec.entrypoint {
        Some(entrypoint) => entrypoint.iter().chain(&spec.command).cloned().collect(),
        None => spec.command.clone(),
    };
    let executors = vec![TesExecutor {
        image: spec.image.clone(),
        command,
        workdir: spec.workdir.clone(),
        env: spec.env.clone(),
        ..Default::default()
    }];

    let inputs = spec
        .inputs
        .iter()
        .filter_map(|input| {
            let container_path = input.container_path.as_ref()?;
            let InputSource::S3 { bucket, key, .. } = &input.source;
            Some(TesInput {
                name: input.name.clone(),
                description: input.description.clone(),
                url: Some(format!("s3://{bucket}/{key}")),
                path: container_path.clone(),
                kind: TesFileType::File,
                ..Default::default()
            })
        })
        .collect();

    let outputs = spec
        .file_outputs
        .iter()
        .map(|output| {
            let OutputDestination::S3 { bucket, key } = &output.destination;
            TesOutput {
                name: output.name.clone(),
                description: output.description.clone(),
                url: Some(format!("s3://{bucket}/{key}")),
                path: output.container_path.clone(),
                path_prefix: output.path_prefix.clone(),
                kind: TesFileType::File,
            }
        })
        .collect();

    let resources = Some(TesResources {
        cpu_cores: spec.resources.cpu_cores,
        ram_gb: spec
            .resources
            .ram_bytes
            .map(|bytes| bytes as f64 / 1_000_000_000.0),
        disk_gb: spec
            .resources
            .disk_bytes
            .map(|bytes| bytes as f64 / 1_000_000_000.0),
        preemptible: Some(spec.resources.preemptible),
        ..Default::default()
    });

    let mut tags = project_tags(spec);
    details.stamp(&id, &mut tags);

    let mut log = build_task_log(record, base_url);
    if view == TesView::Basic {
        log.system_logs.clear();
        for executor in &mut log.logs {
            executor.stdout = None;
            executor.stderr = None;
        }
    }

    TesTask {
        id: Some(id),
        state: Some(state),
        name: spec.name.clone(),
        description: spec.description.clone(),
        executors,
        inputs,
        outputs,
        resources,
        tags,
        logs: vec![log],
        creation_time: Some(rfc3339(record.created_at_ms)),
        ..Default::default()
    }
}

/// Where the task runs, from its tag. An unknown value is refused rather than
/// silently sent to the realm.
fn task_target(tags: &BTreeMap<String, String>) -> Result<ExecutionTarget, TesError> {
    match tags.get(TARGET_TAG_KEY).map(String::as_str) {
        None | Some("realm") => Ok(ExecutionTarget::Realm),
        Some("local") => Ok(ExecutionTarget::Local),
        Some(other) => Err(TesError::bad_request(format!(
            "unknown `{TARGET_TAG_KEY}` value `{other}`"
        ))),
    }
}

/// Binds a local task to the owner this device is enrolled for. A node that
/// serves no device plane refuses the target itself, not the caller.
async fn local_owner(
    state: &ServerState,
    caller: TesCaller,
    target: ExecutionTarget,
) -> Result<TesCaller, TesError> {
    if target == ExecutionTarget::Realm {
        return Ok(caller);
    }
    if !matches!(state.node_capabilities(), NodeCapabilities::User { .. }) {
        return Err(TesError::bad_request(format!(
            "`{TARGET_TAG_KEY}` `local` is served by a user device only"
        )));
    }
    let auth = require_owner(state, Some(caller.auth))
        .await
        .map_err(TesError::from_server)?;
    Ok(TesCaller { auth, ..caller })
}

/// Names the first derived tag a creation tried to set. Every one of them is
/// stamped from the job and its family on read, so no client may claim one.
fn reserved_tag(tags: &BTreeMap<String, String>) -> Option<&str> {
    tags.keys()
        .find(|key| DERIVED_TAG_KEYS.contains(&key.as_str()))
        .map(String::as_str)
}

fn project_tags(spec: &ExecutionSpec) -> BTreeMap<String, String> {
    let mut tags = spec.tags.clone();
    tags.entry(GROUP_TAG_KEY.to_string())
        .or_insert_with(|| spec.group_id.to_string());
    if let Some(constraint) = &spec.executor_constraint {
        tags.entry(EXECUTOR_TAG_KEY.to_string())
            .or_insert_with(|| constraint.clone());
    }
    tags
}

fn build_task_log(record: &JobRecord, _base_url: &str) -> TesTaskLog {
    let start_time = record.started_at_ms.map(rfc3339);
    let mut executor_log = TesExecutorLog {
        start_time: start_time.clone(),
        end_time: record.finished_at_ms.map(rfc3339),
        ..Default::default()
    };
    let mut outputs = Vec::new();
    if let Some(JobResultPayload::Execution {
        exit_code,
        outputs: captured,
        stdout,
        stderr,
        ..
    }) = &record.result
    {
        executor_log.exit_code = *exit_code;
        executor_log.stdout = (!stdout.is_empty()).then(|| stdout.clone());
        executor_log.stderr = (!stderr.is_empty()).then(|| stderr.clone());
        outputs = captured
            .iter()
            .map(|output| TesFileLog {
                // Names the exact version, so the caller still retrieves this
                // output after a later write becomes the object's latest.
                url: format!(
                    "s3://{}/{}?versionId={}",
                    output.bucket, output.key, output.version_id
                ),
                path: if output.container_path.is_empty() {
                    output.key.clone()
                } else {
                    output.container_path.clone()
                },
                size_bytes: output.size.to_string(),
            })
            .collect();
    }
    let system_logs = record
        .last_error
        .as_ref()
        .map(|error| vec![error.message.clone()])
        .unwrap_or_default();

    // Executor logs are emitted only once the task is terminal; a running task
    // has no meaningful (and TES-required) exit_code yet.
    let logs = if record.state.is_terminal() {
        vec![executor_log]
    } else {
        Vec::new()
    };
    TesTaskLog {
        logs,
        start_time,
        end_time: record.finished_at_ms.map(rfc3339),
        outputs,
        system_logs,
    }
}

#[derive(Debug)]
struct TesCaller {
    auth: AuthContext,
    credential_group: Option<Ulid>,
}

async fn authenticate_tes(
    state: &ServerState,
    auth: Option<AuthContext>,
    headers: &HeaderMap,
) -> Result<TesCaller, TesError> {
    if let Some(auth) = auth {
        let auth = require_unrestricted_auth(state, Some(auth)).map_err(TesError::from_server)?;
        return Ok(TesCaller {
            auth,
            credential_group: None,
        });
    }

    let (access_key, provided_secret) = parse_basic(headers)?;
    let access = match drive(
        GetAccessOperation::new(access_key.clone()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(access) => access,
        Err(GetAccessError::NotFound) => return Err(TesError::unauthorized()),
        Err(error) => return Err(TesError::internal(error.to_string())),
    };
    // The secret is encrypted with this node's issuer-local key, so it opens only for a
    // credential this node issued; a foreign or tampered record never matches.
    let secret_matches = access
        .open_secret(state.credential_encryption_key())
        .is_ok_and(|plaintext| {
            blake3::hash(provided_secret.as_slice()) == blake3::hash(plaintext.as_bytes())
        });
    if access.access_key != access_key
        || access.user_identity.realm_id != state.get_realm_id()
        || access.issued_by != *state.get_node_id().as_bytes()
        || access.is_revoked()
        || access.is_expired(SystemTime::now())
        || !secret_matches
    {
        return Err(TesError::unauthorized());
    }

    let credential_group = access.group_id;
    let auth = require_unrestricted_auth(
        state,
        Some(AuthContext {
            user_id: access.user_identity,
            realm_id: access.user_identity.realm_id,
            path_restrictions: access.path_restrictions,
            session: None,
        }),
    )
    .map_err(TesError::from_server)?;
    Ok(TesCaller {
        auth,
        credential_group: Some(credential_group),
    })
}

fn parse_basic(headers: &HeaderMap) -> Result<(String, Vec<u8>), TesError> {
    let value = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(TesError::unauthorized)?;
    let mut parts = value.split_ascii_whitespace();
    let (Some(scheme), Some(encoded), None) = (parts.next(), parts.next(), parts.next()) else {
        return Err(TesError::unauthorized());
    };
    if !scheme.eq_ignore_ascii_case("Basic") {
        return Err(TesError::unauthorized());
    }
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|_| TesError::unauthorized())?;
    // Aruna access keys contain a colon, while generated access secrets do not.
    let Some(separator) = decoded.iter().rposition(|byte| *byte == b':') else {
        return Err(TesError::unauthorized());
    };
    let access_key = std::str::from_utf8(&decoded[..separator])
        .map_err(|_| TesError::unauthorized())?
        .to_string();
    let secret = decoded[separator + 1..].to_vec();
    if access_key.is_empty() || secret.is_empty() {
        return Err(TesError::unauthorized());
    }
    Ok((access_key, secret))
}

fn task_in_group(record: &JobRecord, credential_group: Option<Ulid>) -> bool {
    let JobPayload::Execution(spec) = &record.payload else {
        return false;
    };
    credential_group.is_none_or(|credential_group| credential_group == spec.group_id)
}

async fn ensure_group_write(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
) -> Result<(), TesError> {
    crate::auth::ensure_permission(
        state,
        auth,
        group_permission_path(state.get_realm_id(), group_id, state.get_node_id()),
        aruna_core::structs::identity::auth::Permission::WRITE,
    )
    .await
    .map_err(|error| match error {
        crate::error::ServerError::InternalError(message) => TesError::internal(message),
        _ => TesError::forbidden("no write access to group"),
    })
}

fn decode_page_token(token: Option<&str>) -> Result<Option<Vec<u8>>, TesError> {
    match token {
        Some(token) => {
            let bytes = URL_SAFE_NO_PAD
                .decode(token)
                .map_err(|_| TesError::bad_request("invalid page_token"))?;
            if bytes.len() != 24 {
                return Err(TesError::bad_request("invalid page_token"));
            }
            Ok(Some(bytes))
        }
        None => Ok(None),
    }
}

fn rfc3339(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_default()
}

fn tes_json_response<T: Serialize>(status: StatusCode, value: T) -> Response {
    let body = serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec());
    let mut response = Response::new(axum::body::Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json; charset=utf-8"),
    );
    response
}

#[cfg(test)]
pub(crate) fn cancel_response() -> Response {
    tes_json_response(StatusCode::OK, TesCancelResponse {})
}

#[derive(Debug)]
struct TesError {
    status: StatusCode,
    message: String,
    code: Option<String>,
}

impl TesError {
    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: "unauthorized".to_string(),
            code: None,
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
            code: None,
        }
    }

    fn coded(status: StatusCode, message: impl Into<String>, code: &str) -> Self {
        Self {
            status,
            message: message.into(),
            code: Some(code.to_string()),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
            code: None,
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: message.into(),
            code: None,
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
            code: None,
        }
    }

    fn from_server(error: ServerError) -> Self {
        match error {
            ServerError::Unauthorized => Self::unauthorized(),
            ServerError::Forbidden => Self::forbidden("forbidden"),
            ServerError::NotFound => Self::not_found("TES task not found"),
            other => Self {
                status: other.status_code(),
                message: other.public_message(),
                code: None,
            },
        }
    }

    /// Task creation shares the REST submit mapping, so a refusal a TES client
    /// must not retry never reaches it as a retryable 500.
    fn from_submit(error: aruna_operations::jobs::submit::SubmitJobError) -> Self {
        Self::from_server(super::jobs::map_submit_error(error))
    }

    fn from_job_route(error: JobRouteError) -> Self {
        match error {
            JobRouteError::Unauthorized => Self::unauthorized(),
            JobRouteError::Forbidden => Self::forbidden("forbidden"),
            JobRouteError::NotFound => Self::not_found("TES task not found"),
            JobRouteError::Unavailable(message) => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                message,
                code: None,
            },
            JobRouteError::Internal(message) => Self::internal(message),
        }
    }
}

impl IntoResponse for TesError {
    fn into_response(self) -> Response {
        // Internal detail is logged, never returned to the client.
        let message = if self.status == StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(detail = %self.message, "TES internal error");
            "Internal server error".to_string()
        } else {
            self.message
        };
        tes_json_response(
            self.status,
            TesErrorPayload {
                status_code: self.status.as_u16(),
                msg: message,
                code: self.code,
            },
        )
    }
}

#[cfg(test)]
#[path = "tes_tests.rs"]
mod tests;
