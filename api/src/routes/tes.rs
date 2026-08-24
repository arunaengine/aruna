use std::collections::BTreeMap;
use std::path::Path as FilePath;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;

use super::routes_at;
use aruna_core::compute::{
    has_wildcard, literal_prefix, output_glob, output_suffix, paths_overlap,
};
use aruna_core::structs::{
    AuthContext, ComputeResources, ExecutionSpec, InputMode, InputSelection, InputSource, JobId,
    JobPayload, JobRecord, JobResultPayload, JobState, MAX_EXECUTION_OUTPUTS, OutputDestination,
    OutputSelection, blob_group_permission_path,
};
use aruna_operations::driver::drive;
use aruna_operations::jobs::JobRouteError;
use aruna_operations::jobs::lifecycle::{FamilyReport, family_report, submit_external_job};
use aruna_operations::jobs::service::{
    RoutedCancelOutcome, cancel_job_routed, list_owned_jobs, read_record_routed,
};
use aruna_operations::s3::get_user_access::{GetUserAccessError, GetUserAccessOperation};
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

use crate::auth::{ValidatedArunaBearerTokenCarrier, require_unrestricted_realm_auth};
use crate::error::ServerError;
use crate::forwarded::external_base_url;
use crate::server_state::ServerState;

/// GA4GH TES version this facade implements.
const TES_VERSION: &str = "1.1.0";
/// Optional tag overriding the caller credential's workspace parent group.
const GROUP_TAG_KEY: &str = "aruna-engine.org/group";
/// Optional tag pinning a backend executor kind.
const EXECUTOR_TAG_KEY: &str = "aruna-engine.org/executor";
/// Optional tag carrying the submission idempotency key.
const IDEMPOTENCY_TAG_KEY: &str = "aruna-engine.org/idempotency-key";

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
        TesOutputFileLog,
        TesTaskLog,
        TesCreateTaskResponse,
        TesListTasksResponse,
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

// ---------------------------------------------------------------------------
// TES wire types (GA4GH TES v1.1, snake_case per the TES OpenAPI).
// ---------------------------------------------------------------------------

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
pub struct TesOutputFileLog {
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
    pub outputs: Vec<TesOutputFileLog>,
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
pub struct TesCreateTaskResponse {
    pub id: String,
}

#[derive(Debug, Serialize)]
struct TesCancelResponse {}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TesListTasksResponse {
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

    fn matches_facts(&self, record: &JobRecord, facts: &TaskFacts) -> bool {
        if !self.matches_base(record) {
            return false;
        }
        let JobPayload::Execution(spec) = &record.payload else {
            return false;
        };
        let mut tags = project_tags(spec);
        facts.stamp(&record.job_id.to_string(), &mut tags);
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

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

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
  forwarded headers only when the request arrived through a trusted proxy and from the `Host` header
  otherwise."#,
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

**Authentication**: realm bearer token, or HTTP Basic with an access key and secret issued by
this node; a path-restricted credential is rejected. Basic authentication uses the credential's
group and refuses an `aruna-engine.org/group` tag naming a different group; a bearer token requires
that tag. The caller needs WRITE on the target group.

**Behavior**
- 200 means the task was durably accepted and queued, never that it started or finished.
- State runs `QUEUED`, `INITIALIZING`, `RUNNING`, then `COMPLETE`, `EXECUTOR_ERROR`,
  `SYSTEM_ERROR` or `CANCELED`; `CANCELING` shows while a cancellation is in flight and `UNKNOWN`
  when the outcome cannot be determined. `PAUSED` and `PREEMPTED` are never emitted.
- An `aruna-engine.org/idempotency-key` tag deduplicates submissions per caller; reusing a key
  bound to a different task is a 409 carrying that task id. A replay is settled before any quota
  is read and is never quota-refused.
- On a user device this facade only proxies: the task is forwarded to a realm holder that pins the
  outputs to itself and resolves the referenced inputs, and the device never executes a task. The
  device forwards the caller's own bearer token, so basic authentication is refused with a 403
  here even though it is accepted on a realm node.

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
  executor stdin/stdout/stderr redirection and resource zones.

**Errors**
- Admission refusals carry the same status semantics as the native submit surface: a quota or
  composition refusal is a 409, an unusable input or workspace a 400, a refused routed authority a
  403.
- 503 is reserved for an unreachable family holder, a demand view that could not be read or did not
  settle, admission losing three transactions in a row to concurrent submissions of the same group,
  and an unhealthy id clock.
- A task forwarded from a device whose inputs sit on no holder of its family answers that same 503
  and retrying does not clear it. The family is picked by hashing the request, the holder resolves
  the referenced inputs against its own objects only, and a holder that cannot read one refuses
  without naming it, so an unstaged input is indistinguishable here from an unreachable realm.
  Submit from a node that holds the inputs, or replicate them first.
- A standing quota decided on an understated demand view is a 409 like an exceeded cap."#,
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
            body = TesCreateTaskResponse,
            example = json!({
                "id": "01JABCDEF0123456789ABCDEFG"
            })
        ),
        (status = 400, description = "Malformed task, an unsupported TES feature, an input that is not a readable object, more outputs than a task may declare, or a reserved tag", body = TesErrorPayload),
        (status = 401, description = "Missing or invalid bearer token or basic credential", body = TesErrorPayload),
        (status = 403, description = "No WRITE permission on the target group, a group tag contradicting the credential, a path restricted credential, or a routed authority refusing the submission", body = TesErrorPayload),
        (status = 409, description = "The idempotency key tag is already bound to a different task, the group's standing compute quota refuses this admission, or the composition conflicts on a staged key", body = TesErrorPayload),
        (status = 503, description = "Retryable admission failure; the caller may create the task again with the same idempotency key", body = TesErrorPayload)
    ),
    security(("bearer_auth" = []), ("basic_auth" = []))
)]
pub async fn create_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    headers: HeaderMap,
    Json(task): Json<TesTask>,
) -> Response {
    let caller = match authenticate_tes(&state, auth, &headers).await {
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

    // S3 mounts are a local deployment property; without them TES stages inputs
    // via a kept workspace snapshot, matching submit validation on both sides.
    let s3_mounts = state.s3_mounts_available();
    let (spec, idempotency_key) = match map_task_to_spec(&task, caller.credential_group, s3_mounts)
    {
        Ok(mapped) => mapped,
        Err(error) => return error.into_response(),
    };
    let workspace_mode = if s3_mounts {
        aruna_core::structs::WorkspaceMode::None
    } else {
        aruna_core::structs::WorkspaceMode::Kept
    };

    let forwarded = match super::jobs::forwarded_job_auth(bearer) {
        Ok(token) => token.or_else(|| {
            Some(aruna_operations::metadata::MetadataAuthToken::internal(
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
            TesCreateTaskResponse {
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
node; a path-restricted credential is rejected.

**Behavior**
- Tasks are self scoped: a task created by another user, a task outside the group of the basic
  credential, an id that is not a task of this facade and an id that does not parse are all answered
  with 404 rather than 403, so the existence of a task is never disclosed.
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
  `aruna-engine.org/estimated-transfer-bytes` once this responder sealed a placement.
- Output urls in the task log name the exact `versionId` the canonical execution wrote, which is not
  necessarily the object's current version: a duplicate execution admitted during a partition, or
  any later write, may have made another version current.
- Executor logs, including the exit code, appear only once the task is terminal.

**Limits**
- `view` must be `MINIMAL`, `BASIC` or `FULL`; any other value is a 400.

**Errors**: when the node owning the task cannot be reached the call fails with a retryable 503
instead of reporting the task as missing."#,
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
        (status = 503, description = "The node owning the task is unreachable, so its state is unknown; the caller may retry", body = TesErrorPayload)
    ),
    security(("bearer_auth" = []), ("basic_auth" = []))
)]
pub async fn get_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
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
            Some(aruna_operations::metadata::MetadataAuthToken::internal(
                caller.auth.clone(),
            ))
        }),
        Err(error) => return TesError::from_server(error).into_response(),
    };
    // A distributed external job is projected from the replicated family, so
    // this surface reports the same logical view and the same exact output
    // VersionIds as the native REST status.
    let (record, facts) = match family_report(&state.get_ctx(), &caller.auth, job_id).await {
        Some(Ok(report)) => (family_record(&report), TaskFacts::from_report(&report)),
        Some(Err(error)) => return TesError::from_job_route(error).into_response(),
        // The owner is the sole 404 authority; a non-owner routes or reports 503.
        None => {
            match read_record_routed(&state.get_ctx(), caller.auth.user_id, job_id, forwarded).await
            {
                Ok(Some(record)) => (record, TaskFacts::default()),
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
        project_task(&record, &facts, view, &base_url),
    )
}

async fn task_record(
    state: &ServerState,
    auth: &AuthContext,
    record: JobRecord,
) -> Result<(JobRecord, TaskFacts), TesError> {
    match family_report(&state.get_ctx(), auth, record.job_id).await {
        Some(Ok(report)) => Ok((family_record(&report), TaskFacts::from_report(&report))),
        Some(Err(error)) => Err(TesError::from_job_route(error)),
        None => Ok((record, TaskFacts::default())),
    }
}

async fn list_derived(
    state: &ServerState,
    caller: &TesCaller,
    filters: &TaskFilters,
    mut cursor: Option<Vec<u8>>,
    limit: usize,
) -> Result<(Vec<(JobRecord, TaskFacts)>, Option<Vec<u8>>), TesError> {
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
        if filters.matches_facts(&task.0, &task.1) {
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
node; a path-restricted credential is rejected.

**Behavior**
- The listing is keyed by the caller: another user's tasks are never returned, and a basic
  credential additionally sees only tasks of that credential's group.
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
- An empty `name_prefix` is ignored.
- An invalid `view`, an unknown `state` name, or a `page_token` that is not a token of this listing
  is refused with 400."#,
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
            body = TesListTasksResponse,
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
        (status = 400, description = "Invalid `view`, `state` or `page_token`", body = TesErrorPayload),
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
        .map(|(record, facts)| project_task(record, facts, view, &base_url))
        .collect();

    tes_json_response(
        StatusCode::OK,
        TesListTasksResponse {
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
node; a path-restricted credential is rejected.

**Behavior**
- TES addresses this as a POST whose final path segment is the task id followed by the literal
  `:cancel` action suffix.
- Cancellation is self scoped exactly like reads: another user's task, a task outside the group of
  the basic credential and an id that does not parse are all answered with 404 rather than 403.
- The request is carried out on the node that owns the task.
- A 200 records only that cancellation was requested, or that the task had already reached a
  terminal state; the executor may still be winding down, so poll the task until it reports
  `CANCELED`.

**Limits**
- A POST that omits the `:cancel` suffix is refused with 400.

**Errors**: when the node owning the task is unreachable the cancellation was not delivered and the
call fails with a retryable 503."#,
    params(("id" = String, Path, description = "TES task id (the JobId) followed by the `:cancel` suffix, for example `01JABCDEF0123456789ABCDEFG:cancel`")),
    responses(
        (
            status = 200,
            description = "Cancellation requested, or the task was already terminal; the body is an empty JSON object and the task may still be stopping",
            body = Object,
            content_type = "application/json",
            example = json!({})
        ),
        (status = 401, description = "Missing or invalid bearer token or basic credential", body = TesErrorPayload),
        (status = 404, description = "No such task for this caller; also returned for another user's task, a task outside the credential's group and an unparsable id", body = TesErrorPayload),
        (status = 503, description = "The node owning the task is unreachable, so the cancellation was not delivered; the caller may retry", body = TesErrorPayload)
    ),
    security(("bearer_auth" = []), ("basic_auth" = []))
)]
pub async fn cancel_task(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
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
            Some(aruna_operations::metadata::MetadataAuthToken::internal(
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

// ---------------------------------------------------------------------------
// Mapping: TesTask -> ExecutionSpec
// ---------------------------------------------------------------------------

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

/// Map a TES task onto the internal execution plan and optional dedup key.
/// Pure and self-contained: the group-write permission check happens separately.
fn map_task_to_spec(
    task: &TesTask,
    credential_group: Option<Ulid>,
    s3_mounts: bool,
) -> Result<(ExecutionSpec, Option<String>), TesError> {
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

    let group_id = resolve_task_group(task, credential_group)?;

    if task.inputs.len() > MAX_TASK_IO {
        return Err(TesError::bad_request("too many task inputs"));
    }
    let mut inputs: Vec<InputSelection> = Vec::with_capacity(task.inputs.len());
    for input in &task.inputs {
        let input = map_input(input, s3_mounts)?;
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
        for input in &inputs {
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
    let resources = ComputeResources {
        cpu_cores,
        ram_bytes,
        disk_bytes,
        max_walltime_ms: None,
        preemptible: task
            .resources
            .as_ref()
            .and_then(|resources| resources.preemptible)
            .unwrap_or(false),
    };

    let spec = ExecutionSpec {
        group_id,
        name: task.name.clone(),
        description: task.description.clone(),
        tags: task.tags.clone(),
        image: executor.image.clone(),
        // TES `command` is the full argv; override the image ENTRYPOINT with it and
        // leave the image CMD unset so exactly the requested argv runs.
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

    // Handed over as the raw idempotency key: the ingress applies the per-user
    // `user/` namespacing itself, so TES inherits dedup scoping for free.
    let idempotency_key = task.tags.get(IDEMPOTENCY_TAG_KEY).cloned();

    Ok((spec, idempotency_key))
}

fn map_input(input: &TesInput, s3_mounts: bool) -> Result<InputSelection, TesError> {
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
        mode: if s3_mounts {
            InputMode::Mount
        } else {
            InputMode::Snapshot
        },
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

// ---------------------------------------------------------------------------
// Mapping: JobRecord -> TesTask / TesState
// ---------------------------------------------------------------------------

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

/// The reduced family as the local row shape every TES projection reads. Only
/// the canonical successful execution supplies outputs, and they keep their
/// exact VersionIds, so a later unrelated write never becomes this task's
/// result.
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
            stdout: String::new(),
            stderr: String::new(),
            output_digest: report
                .canonical_result
                .as_ref()
                .and_then(|result| result.output_digest),
        }
    });
    record
}

/// Facts a TES tag exposes at read time. They come from the same family and
/// sealed plan `GET /jobs/{id}` reports and are never stored on the task.
#[derive(Debug, Default)]
struct TaskFacts {
    logical_state: Option<String>,
    executor_kind: Option<String>,
    transfer_bytes: Option<u64>,
}

impl TaskFacts {
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

fn project_task(record: &JobRecord, facts: &TaskFacts, view: TesView, base_url: &str) -> TesTask {
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
    facts.stamp(&id, &mut tags);

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
            .map(|output| TesOutputFileLog {
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
        let auth =
            require_unrestricted_realm_auth(state, Some(auth)).map_err(TesError::from_server)?;
        return Ok(TesCaller {
            auth,
            credential_group: None,
        });
    }

    let (access_key, provided_secret) = parse_basic(headers)?;
    let access = match drive(
        GetUserAccessOperation::new(access_key.clone()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(access))) => access,
        Ok(None)
        | Ok(Some(Err(GetUserAccessError::NotFound)))
        | Err(GetUserAccessError::NotFound) => return Err(TesError::unauthorized()),
        Ok(Some(Err(error))) | Err(error) => return Err(TesError::internal(error.to_string())),
    };
    // The secret seals with this node's issuer-local key, so it opens only for a
    // credential this node issued; a foreign or tampered record never matches.
    let secret_matches = access
        .open_secret(state.credential_seal_key())
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
    let auth = require_unrestricted_realm_auth(
        state,
        Some(AuthContext {
            user_id: access.user_identity,
            realm_id: access.user_identity.realm_id,
            path_restrictions: access.path_restrictions,
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
        blob_group_permission_path(state.get_realm_id(), group_id, state.get_node_id()),
        aruna_core::structs::Permission::WRITE,
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
mod tests {
    use super::*;
    use std::time::Duration;

    use axum::body::to_bytes;

    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, GROUP_KEYSPACE, REALM_CONFIG_KEYSPACE, USER_ACCESS_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, JobError, NodeCapabilities, OutputObject,
        RealmAuthorizationDocument, RealmConfigDocument, RealmId, UserAccess,
    };
    use aruna_core::types::{NodeId, UserId};
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_operations::jobs::store::insert_job;
    use aruna_storage::FjallStorage;
    use tempfile::TempDir;

    fn realm() -> RealmId {
        RealmId([1u8; 32])
    }

    fn node_id() -> NodeId {
        iroh::SecretKey::from_bytes(&[7u8; 32]).public()
    }

    fn user(byte: u8) -> UserId {
        UserId::new(Ulid::from_bytes([byte; 16]), realm())
    }

    fn auth_for(user_id: UserId) -> Option<AuthContext> {
        Some(AuthContext {
            user_id,
            realm_id: realm(),
            path_restrictions: None,
        })
    }

    const TES_SECRET: &str = "tes-secret";

    fn credential(group_id: Ulid) -> UserAccess {
        let user_identity = user(2);
        UserAccess {
            access_key: UserAccess::build_access_key("tes").unwrap(),
            user_identity,
            group_id,
            secret: aruna_core::credential_seal::SealedS3Secret::empty(),
            expiry: SystemTime::now() + Duration::from_secs(60),
            path_restrictions: None,
            issued_by: *node_id().as_bytes(),
            revoked_at: None,
        }
    }

    /// A credential whose secret is sealed with the node's issuer-local key, as
    /// the create-credential path would have produced.
    fn sealed(state: &ServerState, group_id: Ulid) -> UserAccess {
        let mut access = credential(group_id);
        access
            .seal_secret(state.credential_seal_key(), TES_SECRET)
            .unwrap();
        access
    }

    fn basic_headers(access: &UserAccess, secret: &str) -> HeaderMap {
        let encoded = STANDARD.encode(format!("{}:{secret}", access.access_key));
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, format!("Basic {encoded}").parse().unwrap());
        headers
    }

    async fn write_credential(state: &ServerState, access: &UserAccess) {
        let _ = state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access.access_key.as_bytes().into(),
                value: access.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    async fn write_auth(state: &ServerState, group_id: Ulid, owner: UserId) {
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id: owner,
            realm_id: realm(),
        };
        let realm_auth = RealmAuthorizationDocument::new_default_realm_doc(realm());
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(owner, realm(), group_id);
        let group = Group {
            display_name: "tes-group".to_string(),
            group_id,
            realm_id: realm(),
            roles: group_auth.roles.keys().copied().collect(),
            owner,
        };
        // Request-policy loading fails closed without the realm config, the group
        // record, and the group auth document.
        for (key_space, key, value) in [
            (
                REALM_CONFIG_KEYSPACE,
                realm().as_bytes().to_vec(),
                RealmConfigDocument::default_for_realm(realm(), Vec::new())
                    .to_bytes(&actor)
                    .unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                realm().as_bytes().to_vec(),
                realm_auth.to_bytes(&actor).unwrap(),
            ),
            (
                AUTH_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group_auth.to_bytes(&actor).unwrap(),
            ),
            (
                GROUP_KEYSPACE,
                group_id.to_bytes().to_vec(),
                group.to_bytes(&actor).unwrap(),
            ),
        ] {
            let _ = state
                .get_ctx()
                .storage_handle
                .send_storage_effect(StorageEffect::Write {
                    key_space: key_space.to_string(),
                    key: key.into(),
                    value: value.into(),
                    txn_id: None,
                })
                .await;
        }
    }

    async fn build_state(s3_mounts: bool) -> (TempDir, Arc<ServerState>) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        let state = ServerState::new(
            ctx,
            realm(),
            node_id(),
            NodeCapabilities::user_node(realm()).unwrap(),
            false,
            None,
            JobsRuntime::new(),
        )
        .await
        .with_s3_mounts(s3_mounts);
        (dir, Arc::new(state))
    }

    fn sample_task(group: Ulid) -> TesTask {
        TesTask {
            name: Some("align reads".to_string()),
            description: Some("sample task".to_string()),
            executors: vec![TesExecutor {
                image: "alpine:3".to_string(),
                command: vec!["echo".to_string(), "hi".to_string()],
                workdir: Some("/work".to_string()),
                env: BTreeMap::from([("K".to_string(), "V".to_string())]),
                ..Default::default()
            }],
            inputs: vec![TesInput {
                name: Some("reads".to_string()),
                description: Some("input reads".to_string()),
                url: Some("s3://src/data.csv".to_string()),
                path: "/in/data.csv".to_string(),
                kind: TesFileType::File,
                ..Default::default()
            }],
            outputs: vec![TesOutput {
                name: Some("report".to_string()),
                description: Some("output report".to_string()),
                url: Some("s3://dest/out/report.txt".to_string()),
                path: "/out/report.txt".to_string(),
                ..Default::default()
            }],
            resources: Some(TesResources {
                cpu_cores: Some(2),
                ram_gb: Some(4.0),
                disk_gb: Some(8.0),
                preemptible: Some(true),
                ..Default::default()
            }),
            tags: BTreeMap::from([
                (GROUP_TAG_KEY.to_string(), group.to_string()),
                ("project".to_string(), "alpha".to_string()),
            ]),
            ..Default::default()
        }
    }

    fn execution_record(job_id: JobId, owner: UserId, spec: ExecutionSpec) -> JobRecord {
        JobRecord::new(
            job_id,
            JobPayload::Execution(spec),
            owner,
            node_id(),
            1_000,
            1_000,
            None,
        )
    }

    #[test]
    fn emits_required_logs() {
        // TES 1.1 requires taskLog.logs and taskLog.outputs to be present;
        // executor logs appear only once the task is terminal.
        let group = Ulid::from_bytes([5u8; 16]);
        let (spec, _) = map_task_to_spec(&sample_task(group), None, true).unwrap();
        let mut record = execution_record(JobId::from_bytes([9u8; 16]), user(2), spec);

        let running = build_task_log(&record, "");
        assert!(running.logs.is_empty());
        let json = serde_json::to_value(&running).unwrap();
        assert_eq!(json["outputs"], serde_json::json!([]));
        assert_eq!(json["logs"], serde_json::json!([]));

        record.state = JobState::Succeeded;
        record.result = Some(JobResultPayload::Execution {
            exit_code: Some(0),
            workspace_bucket: Some("ws".to_string()),
            outputs: Vec::new(),
            stdout: String::new(),
            stderr: String::new(),
            output_digest: None,
        });
        let terminal = build_task_log(&record, "");
        assert_eq!(terminal.logs.len(), 1);
        assert_eq!(terminal.logs[0].exit_code, Some(0));
    }

    #[tokio::test]
    async fn redacts_internal_detail() {
        // Raw server error text must never reach a TES client on 500.
        let response = TesError::internal("secret backend detail").into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["msg"], "Internal server error");

        let response = TesError::bad_request("visible reason").into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["msg"], "visible reason");
    }

    #[test]
    fn maps_submit_errors() {
        // A TES client retries 500, so every non-retryable admission refusal
        // must keep the status the native submit surface answers with.
        use aruna_core::ClockHealthError;
        use aruna_core::compute_quota::{QuotaDenied, QuotaDimension, QuotaScope};
        use aruna_core::structs::CompositionError;
        use aruna_operations::jobs::submit::SubmitJobError;

        let cases = [
            (
                SubmitJobError::JobPlanConflict {
                    existing_job_id: JobId::from_bytes([7u8; 16]),
                },
                StatusCode::CONFLICT,
            ),
            (
                SubmitJobError::QuotaDenied(QuotaDenied {
                    scope: QuotaScope::Group,
                    dimension: QuotaDimension::CpuCores,
                    observed: 30,
                    requested: 8,
                    limit: 32,
                }),
                StatusCode::CONFLICT,
            ),
            (
                SubmitJobError::ActiveJobLimit { limit: 4 },
                StatusCode::CONFLICT,
            ),
            (
                SubmitJobError::Composition(CompositionError::KeyConflict("reads".to_string())),
                StatusCode::CONFLICT,
            ),
            (
                SubmitJobError::Composition(CompositionError::MissingVersion("reads".to_string())),
                StatusCode::BAD_REQUEST,
            ),
            (
                SubmitJobError::TooManyOutputs { limit: 1024 },
                StatusCode::BAD_REQUEST,
            ),
            (
                SubmitJobError::InvalidWorkspace("no bucket".to_string()),
                StatusCode::BAD_REQUEST,
            ),
            (SubmitJobError::AuthorityDenied, StatusCode::FORBIDDEN),
            (
                SubmitJobError::ClockHealth(ClockHealthError::TimestampOverflow {
                    timestamp_ms: u64::MAX,
                }),
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(TesError::from_submit(error).status, expected);
        }

        // The 503 body carries the fixed reason, never a holder identity.
        let unavailable = TesError::from_submit(SubmitJobError::PlacementUnavailable(
            "node 7 idle".to_string(),
        ));
        assert_eq!(unavailable.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(unavailable.message, "job_placement_unavailable");
    }

    #[test]
    fn resolves_task_group() {
        // Pre-authorization group resolution must mirror the mapping rules.
        let group = Ulid::from_bytes([5u8; 16]);
        let other = Ulid::from_bytes([6u8; 16]);
        let task = sample_task(group);
        assert_eq!(resolve_task_group(&task, None).unwrap(), group);
        assert_eq!(resolve_task_group(&task, Some(group)).unwrap(), group);
        assert_eq!(
            resolve_task_group(&task, Some(other)).unwrap_err().status,
            StatusCode::FORBIDDEN
        );

        let mut untagged = task.clone();
        untagged.tags.remove(GROUP_TAG_KEY);
        assert_eq!(resolve_task_group(&untagged, Some(other)).unwrap(), other);
        assert_eq!(
            resolve_task_group(&untagged, None).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn caps_task_io() {
        // Input and output counts are bounded before quadratic validation.
        let group = Ulid::from_bytes([5u8; 16]);
        let mut task = sample_task(group);
        task.inputs = vec![task.inputs[0].clone(); MAX_TASK_IO + 1];
        let error = map_task_to_spec(&task, None, true).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);

        let mut task = sample_task(group);
        task.outputs = vec![task.outputs[0].clone(); MAX_EXECUTION_OUTPUTS + 1];
        let error = map_task_to_spec(&task, None, true).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn maps_task() {
        let group = Ulid::from_bytes([5u8; 16]);
        let (spec, dedup) = map_task_to_spec(&sample_task(group), None, true).unwrap();
        assert_eq!(spec.group_id, group);
        assert_eq!(spec.name.as_deref(), Some("align reads"));
        assert_eq!(spec.description.as_deref(), Some("sample task"));
        assert_eq!(spec.tags.get("project").map(String::as_str), Some("alpha"));
        assert_eq!(spec.image, "alpine:3");
        // TES command becomes the entrypoint override; image CMD stays empty.
        assert_eq!(spec.entrypoint, Some(vec!["echo".into(), "hi".into()]));
        assert!(spec.command.is_empty());
        assert_eq!(spec.workdir.as_deref(), Some("/work"));
        assert_eq!(spec.env.get("K").map(String::as_str), Some("V"));
        assert_eq!(spec.resources.cpu_cores, Some(2));
        assert_eq!(spec.resources.ram_bytes, Some(4_000_000_000));
        assert_eq!(spec.resources.disk_bytes, Some(8_000_000_000));
        assert!(spec.resources.preemptible);
        assert_eq!(spec.inputs.len(), 1);
        assert_eq!(spec.inputs[0].mode, InputMode::Mount);
        assert_eq!(spec.inputs[0].dest_key, "in/data.csv");
        assert_eq!(
            spec.inputs[0].container_path.as_deref(),
            Some("/in/data.csv")
        );
        assert_eq!(spec.inputs[0].name.as_deref(), Some("reads"));
        assert_eq!(spec.inputs[0].description.as_deref(), Some("input reads"));
        assert_eq!(spec.file_outputs.len(), 1);
        assert_eq!(spec.file_outputs[0].container_path, "/out/report.txt");
        assert_eq!(spec.file_outputs[0].name.as_deref(), Some("report"));
        assert_eq!(
            spec.file_outputs[0].description.as_deref(),
            Some("output report")
        );
        assert_eq!(
            spec.file_outputs[0].destination,
            OutputDestination::S3 {
                bucket: "dest".to_string(),
                key: "out/report.txt".to_string(),
            }
        );
        assert!(spec.output_prefixes.is_empty());
        assert!(dedup.is_none());
    }

    #[test]
    fn filters_tasks() {
        let group = Ulid::from_bytes([5u8; 16]);
        let (spec, _) = map_task_to_spec(&sample_task(group), None, true).unwrap();
        let mut record = execution_record(JobId::from_bytes([6u8; 16]), user(2), spec);
        record.state = JobState::Running;
        let uri: axum::http::Uri = "/ga4gh/tes/v1/tasks?state=RUNNING&name_prefix=align&tag_key=project&tag_key=aruna-engine.org%2Fgroup&tag_value=alpha"
            .parse()
            .unwrap();
        let Query(query) = Query::<ListTasksQuery>::try_from_uri(&uri).unwrap();
        let filters = TaskFilters::from_query(&query, uri.query()).unwrap();
        assert!(filters.matches(&record));

        let derived = TaskFilters::from_query(
            &ListTasksQuery::default(),
            Some(&format!(
                "tag_key=aruna-engine.org%2Fjob-id&tag_value={}&tag_key=aruna-engine.org%2Flogical-state&tag_value=running&tag_key=aruna-engine.org%2Fexecutor-kind&tag_value=docker&tag_key=aruna-engine.org%2Festimated-transfer-bytes&tag_value=4096",
                record.job_id
            )),
        )
        .unwrap();
        let facts = TaskFacts {
            logical_state: Some("running".to_string()),
            executor_kind: Some("docker".to_string()),
            transfer_bytes: Some(4_096),
        };
        assert!(derived.has_derived());
        assert!(derived.matches(&record));
        assert!(derived.matches_facts(&record, &facts));

        let wrong_name = ListTasksQuery {
            name_prefix: Some("other".to_string()),
            ..Default::default()
        };
        assert!(
            !TaskFilters::from_query(&wrong_name, None)
                .unwrap()
                .matches(&record)
        );
        assert!(
            !TaskFilters::from_query(
                &ListTasksQuery::default(),
                Some("tag_key=project&tag_value=beta"),
            )
            .unwrap()
            .matches(&record)
        );
        assert!(
            !TaskFilters::from_query(&ListTasksQuery::default(), Some("tag_key=missing"),)
                .unwrap()
                .matches(&record)
        );
        assert!(
            TaskFilters::from_query(
                &ListTasksQuery {
                    state: Some("INVALID".to_string()),
                    ..Default::default()
                },
                None,
            )
            .is_err()
        );

        let probe = JobRecord::new(
            JobId::from_bytes([7u8; 16]),
            JobPayload::Probe {
                steps: 1,
                step_sleep_ms: 0,
                fail_at: None,
                panic_at: None,
                cleanup_marker: None,
            },
            user(2),
            node_id(),
            1_000,
            1_000,
            None,
        );
        assert!(
            !TaskFilters::from_query(&ListTasksQuery::default(), None)
                .unwrap()
                .matches(&probe)
        );
    }

    #[test]
    fn rejects_duplicate_inputs() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        let mut input = task.inputs[0].clone();
        input.url = Some("s3://src/other.csv".to_string());
        task.inputs.push(input);
        assert_eq!(
            map_task_to_spec(&task, None, true).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn rejects_invalid_size() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        for size_gb in [-1.0, 0.0, f64::NAN, 1e-10, f64::MAX] {
            task.resources.as_mut().unwrap().ram_gb = Some(size_gb);
            assert_eq!(
                map_task_to_spec(&task, None, true).unwrap_err().status,
                StatusCode::BAD_REQUEST
            );
            task.resources.as_mut().unwrap().ram_gb = Some(4.0);
            task.resources.as_mut().unwrap().disk_gb = Some(size_gb);
            assert_eq!(
                map_task_to_spec(&task, None, true).unwrap_err().status,
                StatusCode::BAD_REQUEST
            );
            task.resources.as_mut().unwrap().disk_gb = Some(8.0);
        }
    }

    #[test]
    fn rejects_multi_executor() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.executors.push(task.executors[0].clone());
        let error = map_task_to_spec(&task, None, true).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("single executor"));
    }

    #[test]
    fn rejects_missing_group() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.tags.clear();
        let error = map_task_to_spec(&task, None, true).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains(GROUP_TAG_KEY));
    }

    #[test]
    fn defaults_group() {
        let group = Ulid::from_bytes([5u8; 16]);
        let mut task = sample_task(group);
        task.tags.remove(GROUP_TAG_KEY);
        let (spec, _) = map_task_to_spec(&task, Some(group), true).unwrap();
        assert_eq!(spec.group_id, group);
    }

    #[test]
    fn rejects_group_override() {
        let group = Ulid::from_bytes([5u8; 16]);
        let credential_group = Ulid::from_bytes([6u8; 16]);
        let error =
            map_task_to_spec(&sample_task(group), Some(credential_group), true).unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
    }

    #[test]
    fn rejects_invalid_paths() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.executors[0].workdir = Some("work".to_string());
        assert_eq!(
            map_task_to_spec(&task, None, true).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
        task.executors[0].workdir = Some("/work".to_string());
        task.inputs[0].path = "/in/../data.csv".to_string();
        assert_eq!(
            map_task_to_spec(&task, None, true).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
        task.inputs[0].path = "/in/data.csv".to_string();
        task.outputs[0].path = "/out//report.txt".to_string();
        assert_eq!(
            map_task_to_spec(&task, None, true).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn rejects_unsupported_fields() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.id = Some("server-owned".to_string());
        assert!(map_task_to_spec(&task, None, true).is_err());
        task.id = None;
        task.inputs[0].kind = TesFileType::Directory;
        assert!(map_task_to_spec(&task, None, true).is_err());
        task.inputs[0].kind = TesFileType::File;
        task.outputs[0].kind = TesFileType::Directory;
        assert!(map_task_to_spec(&task, None, true).is_err());
        task.outputs[0].kind = TesFileType::File;
        task.executors[0].stdout = Some("/logs/out".to_string());
        assert!(map_task_to_spec(&task, None, true).is_err());
        task.executors[0].stdout = None;
        task.volumes.push("/data".to_string());
        assert!(map_task_to_spec(&task, None, true).is_err());
        task.volumes.clear();
        task.resources
            .as_mut()
            .unwrap()
            .zones
            .push("zone-a".to_string());
        assert!(map_task_to_spec(&task, None, true).is_err());
    }

    #[test]
    fn maps_wildcard_output() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.outputs[0].path = "/out/*.txt".to_string();
        task.outputs[0].path_prefix = Some("/out".to_string());
        task.outputs[0].url = Some("s3://dest/results".to_string());

        let (spec, _) = map_task_to_spec(&task, None, true).unwrap();

        assert_eq!(spec.file_outputs[0].container_path, "/out/*.txt");
        assert_eq!(spec.file_outputs[0].path_prefix.as_deref(), Some("/out"));
    }

    #[test]
    fn rejects_input_match() {
        // An input the pattern would select must not be captured as an output.
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.inputs[0].path = "/in/data.csv".to_string();
        task.outputs[0].path = "/in/*.csv".to_string();
        task.outputs[0].path_prefix = Some("/in".to_string());

        let error = map_task_to_spec(&task, None, true).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rejects_missing_prefix() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.outputs[0].path = "/out/*.txt".to_string();

        let error = map_task_to_spec(&task, None, true).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert!(error.message.contains("/out/*.txt"), "{}", error.message);
    }

    #[test]
    fn rejects_foreign_prefix() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.outputs[0].path = "/out/sub/*.txt".to_string();
        for prefix in ["/other", "/out/s", "/out/*", "out"] {
            task.outputs[0].path_prefix = Some(prefix.to_string());
            let error = map_task_to_spec(&task, None, true).unwrap_err();
            assert_eq!(error.status, StatusCode::BAD_REQUEST, "{prefix}");
        }
        // The pattern itself must still compile.
        task.outputs[0].path = "/out/[a.txt".to_string();
        task.outputs[0].path_prefix = Some("/out".to_string());
        assert_eq!(
            map_task_to_spec(&task, None, true).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn ignores_unused_prefix() {
        // TES 1.1 ignores path_prefix unless the path carries wildcards.
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.outputs[0].path_prefix = Some("/out".to_string());

        let (spec, _) = map_task_to_spec(&task, None, true).unwrap();

        assert!(spec.file_outputs[0].path_prefix.is_none());
    }

    #[test]
    fn rejects_wildcard_input() {
        // TES 1.1 defines wildcards for outputs only.
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.inputs[0].path = "/in/*.csv".to_string();

        let error = map_task_to_spec(&task, None, true).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rejects_duplicate_outputs() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        let mut output = task.outputs[0].clone();
        output.url = Some("s3://dest/out/other.txt".to_string());
        task.outputs.push(output);
        assert!(map_task_to_spec(&task, None, true).is_err());

        task.outputs[1].path = "/out/other.txt".to_string();
        task.outputs[1].url = task.outputs[0].url.clone();
        assert!(map_task_to_spec(&task, None, true).is_err());

        task.outputs.truncate(1);
        task.outputs[0].path = task.inputs[0].path.clone();
        assert!(map_task_to_spec(&task, None, true).is_err());
    }

    #[test]
    fn allows_shared_workdir() {
        let mut task = sample_task(Ulid::from_bytes([5u8; 16]));
        task.inputs[0].path = "/work/.command.sh".to_string();
        task.outputs[0].path = "/work/out.txt".to_string();
        assert!(map_task_to_spec(&task, None, true).is_ok());
    }

    #[test]
    fn maps_states() {
        let spec = || ExecutionSpec {
            group_id: Ulid::from_bytes([5u8; 16]),
            name: None,
            description: None,
            tags: BTreeMap::new(),
            image: "img".to_string(),
            entrypoint: None,
            command: vec!["run".to_string()],
            workdir: None,
            env: BTreeMap::new(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        };
        let mut record = execution_record(JobId::from_bytes([1u8; 16]), user(2), spec());
        let cases = [
            (JobState::Queued, TesState::Queued),
            (JobState::Claimed, TesState::Queued),
            (JobState::Preparing, TesState::Initializing),
            (JobState::Ready, TesState::Initializing),
            (JobState::Running, TesState::Running),
            (JobState::Cancelling, TesState::Canceling),
            (JobState::Indeterminate, TesState::Unknown),
            (JobState::Succeeded, TesState::Complete),
            (JobState::Cancelled, TesState::Canceled),
        ];
        for (job_state, expected) in cases {
            record.state = job_state;
            assert_eq!(tes_state(&record), expected, "{job_state:?}");
        }
        record.state = JobState::Queued;
        record.cancel_requested = true;
        assert_eq!(tes_state(&record), TesState::Canceling);
        record.cancel_requested = false;
        // Failed splits on evidence.
        record.state = JobState::Failed;
        record.result = None;
        assert_eq!(tes_state(&record), TesState::SystemError);
        record.result = Some(JobResultPayload::Execution {
            exit_code: Some(1),
            workspace_bucket: Some("ws".to_string()),
            outputs: Vec::new(),
            stdout: String::new(),
            stderr: String::new(),
            output_digest: None,
        });
        assert_eq!(tes_state(&record), TesState::ExecutorError);
        if let Some(JobResultPayload::Execution { exit_code, .. }) = &mut record.result {
            *exit_code = Some(0);
        }
        assert_eq!(tes_state(&record), TesState::SystemError);
    }

    #[test]
    fn view_projections() {
        let (spec, _) =
            map_task_to_spec(&sample_task(Ulid::from_bytes([5u8; 16])), None, true).unwrap();
        let mut record = execution_record(JobId::from_bytes([2u8; 16]), user(2), spec);
        let queued = project_task(&record, &TaskFacts::default(), TesView::Full, "http://x");
        assert!(queued.logs[0].start_time.is_none());
        assert!(queued.logs[0].logs.is_empty());
        record.state = JobState::Succeeded;
        record.finished_at_ms = Some(2_000);
        record.workspace_bucket = Some("ws-x".to_string());
        let JobPayload::Execution(spec) = &mut record.payload else {
            unreachable!();
        };
        spec.inputs.push(InputSelection {
            source: InputSource::S3 {
                bucket: "native".to_string(),
                key: "workspace-only".to_string(),
                version_id: None,
            },
            source_node_id: None,
            dest_key: "native/input".to_string(),
            mode: InputMode::Snapshot,
            container_path: None,
            name: None,
            description: None,
        });
        spec.output_prefixes.push("native/".to_string());
        record.last_error = Some(JobError::permanent("prior failure"));
        record.result = Some(JobResultPayload::Execution {
            exit_code: Some(0),
            workspace_bucket: Some("ws-x".to_string()),
            outputs: vec![OutputObject {
                node_id: record.owner_node_id,
                bucket: "dest".to_string(),
                key: "out/r.txt".to_string(),
                version_id: Ulid::from_bytes([21u8; 16]),
                execution_id: Ulid::from_bytes([22u8; 16]),
                container_path: "/out/report.txt".to_string(),
                size: 12,
                digest: None,
            }],
            stdout: "hello".to_string(),
            stderr: "error".to_string(),
            output_digest: None,
        });

        let minimal = project_task(&record, &TaskFacts::default(), TesView::Minimal, "http://x");
        assert!(minimal.executors.is_empty());
        assert!(minimal.logs.is_empty());
        assert_eq!(minimal.state, Some(TesState::Complete));

        let basic = project_task(&record, &TaskFacts::default(), TesView::Basic, "http://x");
        assert_eq!(basic.name.as_deref(), Some("align reads"));
        assert_eq!(basic.description.as_deref(), Some("sample task"));
        assert_eq!(basic.tags.get("project").map(String::as_str), Some("alpha"));
        assert_eq!(basic.executors.len(), 1);
        assert_eq!(basic.executors[0].command, vec!["echo", "hi"]);
        assert_eq!(basic.executors[0].workdir.as_deref(), Some("/work"));
        assert_eq!(basic.logs.len(), 1);
        assert!(basic.logs[0].system_logs.is_empty());
        assert!(basic.logs[0].logs[0].stdout.is_none());
        assert!(basic.logs[0].logs[0].stderr.is_none());
        assert_eq!(basic.inputs.len(), 1);
        assert_eq!(basic.inputs[0].path, "/in/data.csv");
        assert_eq!(basic.inputs[0].name.as_deref(), Some("reads"));
        assert_eq!(basic.outputs.len(), 1);
        assert_eq!(basic.outputs[0].path, "/out/report.txt");
        assert_eq!(
            basic.outputs[0].url.as_deref(),
            Some("s3://dest/out/report.txt")
        );
        assert_eq!(basic.resources.as_ref().unwrap().disk_gb, Some(8.0));
        assert_eq!(basic.resources.as_ref().unwrap().preemptible, Some(true));

        let full = project_task(&record, &TaskFacts::default(), TesView::Full, "http://x");
        assert_eq!(full.logs.len(), 1);
        assert_eq!(full.logs[0].logs[0].exit_code, Some(0));
        assert_eq!(full.logs[0].logs[0].stdout.as_deref(), Some("hello"));
        assert_eq!(full.logs[0].logs[0].stderr.as_deref(), Some("error"));
        assert_eq!(full.logs[0].system_logs, vec!["prior failure"]);
        assert_eq!(full.logs[0].outputs.len(), 1);
        assert_eq!(
            full.logs[0].outputs[0].url,
            format!(
                "s3://dest/out/r.txt?versionId={}",
                Ulid::from_bytes([21u8; 16])
            )
        );
        assert_eq!(full.logs[0].outputs[0].path, "/out/report.txt");
    }

    /// The replicated family behind one succeeded distributed task.
    fn family_fixture() -> aruna_operations::jobs::lifecycle::FamilyReport {
        use aruna_core::jobs::{JobKind, JobStatusView};
        use aruna_core::structs::{
            EffectiveResources, JobAdmissionRecord, JobProgress, JobRetryPolicy, LogicalJobSpec,
            LogicalJobState, OutputObject, PlacementRef, RealmId, SubmissionId, WorkspaceMode,
        };
        use aruna_operations::jobs::lifecycle::FamilyReport;

        let realm_id = RealmId([1u8; 32]);
        let created_by = UserId::new(Ulid::from_bytes([2u8; 16]), realm_id);
        let job_id = JobId::from_bytes([3u8; 16]);
        let node_id = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
        let submission_id = SubmissionId([5u8; 32]);
        let resources = EffectiveResources {
            cpu_cores: 1,
            ram_bytes: 1,
            disk_bytes: 0,
            max_walltime_ms: 1_000,
            preemptible: false,
        };
        let mut payload = ExecutionSpec {
            group_id: Ulid::from_bytes([6u8; 16]),
            name: None,
            description: None,
            tags: BTreeMap::new(),
            image: "img".to_string(),
            entrypoint: None,
            command: vec!["true".to_string()],
            workdir: None,
            env: BTreeMap::new(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        };
        payload.name = Some("family task".to_string());
        let spec = LogicalJobSpec {
            submission_id,
            job_id,
            origin_node_id: node_id,
            ingress_node_id: node_id,
            realm_id,
            group_id: payload.group_id,
            created_by,
            created_at_ms: 10,
            retention_ms: aruna_core::structs::DEFAULT_JOB_RETENTION_MS,
            payload,
            request_digest: [7u8; 32],
            spec_digest: [8u8; 32],
            resources,
            retry: JobRetryPolicy {
                max_launches_per_witness: 3,
            },
            admission: JobAdmissionRecord {
                submission_id,
                request_digest: [7u8; 32],
                job_id,
                group_id: Ulid::from_bytes([6u8; 16]),
                admitting_node_id: node_id,
                membership_generation: 0,
                resources,
                admitted_at_ms: 10,
            },
            input_facts: Vec::new(),
            output_policies: Vec::new(),
            placement: PlacementRef::NIL,
        };
        let version_id = Ulid::from_bytes([9u8; 16]);
        let execution_id = Ulid::from_bytes([10u8; 16]);
        FamilyReport {
            job: JobStatusView {
                job_id,
                created_by,
                kind: JobKind::Execution,
                state: JobState::Succeeded,
                attempts: 2,
                cancel_requested: false,
                created_at_ms: 10,
                updated_at_ms: 20,
                finished_at_ms: Some(20),
                progress: JobProgress::new("phases"),
                last_error: None,
                result: None,
                workspace_bucket: Some("ws".to_string()),
                workspace_mode: WorkspaceMode::Kept,
                locally_exhausted: false,
            },
            spec,
            submission_id,
            request_digest: [7u8; 32],
            canonical_job_id: job_id,
            aliases: vec![job_id],
            conflicts: 0,
            state: LogicalJobState::Succeeded,
            canonical_execution_id: Some(execution_id),
            canonical_result: Some(aruna_core::structs::PhysicalExecutionResult {
                exit_code: Some(0),
                output_digest: Some([12u8; 32]),
                message: None,
            }),
            executions: 2,
            duplicate_successes: 1,
            outputs: vec![OutputObject {
                node_id,
                bucket: "dest".to_string(),
                key: "out/r.txt".to_string(),
                version_id,
                execution_id,
                container_path: "/out/report.txt".to_string(),
                size: 12,
                digest: None,
            }],
            output_endpoints: std::collections::BTreeMap::new(),
            revision: 3,
            digest: [11u8; 32],
            cancel_requested: false,
            responder: Some(node_id),
            partial: false,
            locally_exhausted: false,
            plan: None,
        }
    }

    #[test]
    fn family_keeps_exact_versions() {
        // The TES view of a distributed job is the same logical projection as
        // the native REST one: the canonical execution's exact VersionIds, and
        // no result at all while the family has no canonical success.
        use aruna_core::structs::LogicalJobState;

        let report = family_fixture();
        let version_id = Ulid::from_bytes([9u8; 16]);

        let task = project_task(
            &family_record(&report),
            &TaskFacts::from_report(&report),
            TesView::Full,
            "http://x",
        );

        assert_eq!(task.state, Some(TesState::Complete));
        assert_eq!(
            task.logs[0].outputs[0].url,
            format!("s3://dest/out/r.txt?versionId={version_id}")
        );

        let mut running = report.clone();
        running.job.state = JobState::Running;
        running.state = LogicalJobState::Running;
        let pending = project_task(
            &family_record(&running),
            &TaskFacts::from_report(&running),
            TesView::Full,
            "http://x",
        );
        assert_eq!(pending.state, Some(TesState::Running));
        assert!(pending.logs[0].outputs.is_empty());
    }

    #[test]
    fn derives_family_tags() {
        // Placement facts are stamped at read time, so a task that was never
        // placed carries the job id and logical state and nothing more.
        use aruna_core::compute::ExecutionTargetId;
        use aruna_operations::jobs::lifecycle::PlanEstimate;

        let mut report = family_fixture();
        let unplaced = project_task(
            &family_record(&report),
            &TaskFacts::from_report(&report),
            TesView::Basic,
            "http://x",
        );
        assert_eq!(
            unplaced.tags.get(JOB_ID_TAG_KEY).map(String::as_str),
            Some(report.job.job_id.to_string().as_str())
        );
        assert_eq!(
            unplaced.tags.get(LOGICAL_STATE_TAG_KEY).map(String::as_str),
            Some("succeeded")
        );
        assert!(!unplaced.tags.contains_key(EXECUTOR_KIND_TAG_KEY));
        assert!(!unplaced.tags.contains_key(TRANSFER_BYTES_TAG_KEY));

        report.plan = Some(PlanEstimate {
            target: Some(ExecutionTargetId {
                node_id: iroh::SecretKey::from_bytes(&[4u8; 32]).public(),
                executor_kind: "docker".to_string(),
            }),
            estimated_transfer_bytes: 4_096,
            estimated_transfer_ms: 12,
            alternatives: 2,
            rejected: 1,
            omitted: 0,
            sealed_at_ms: 15,
        });
        let facts = TaskFacts::from_report(&report);
        let record = family_record(&report);
        for view in [TesView::Basic, TesView::Full] {
            let task = project_task(&record, &facts, view, "http://x");
            assert_eq!(
                task.tags.get(EXECUTOR_KIND_TAG_KEY).map(String::as_str),
                Some("docker")
            );
            assert_eq!(
                task.tags.get(TRANSFER_BYTES_TAG_KEY).map(String::as_str),
                Some("4096")
            );
        }
        let minimal = project_task(&record, &facts, TesView::Minimal, "http://x");
        assert!(minimal.tags.is_empty());
    }

    #[test]
    fn rejects_derived_tag() {
        let group = Ulid::from_bytes([5u8; 16]);
        let mut task = sample_task(group);
        task.tags
            .insert(EXECUTOR_KIND_TAG_KEY.to_string(), "docker".to_string());

        let error = map_task_to_spec(&task, None, true).unwrap_err();
        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.code.as_deref(), Some("reserved_tag"));
    }

    #[test]
    fn projects_full_command() {
        // Docker runs entrypoint + command together; the projection shows both.
        let mut spec = ExecutionSpec {
            group_id: Ulid::from_bytes([5u8; 16]),
            name: None,
            description: None,
            tags: BTreeMap::new(),
            image: "img".to_string(),
            entrypoint: Some(vec!["/bin/tool".to_string()]),
            command: vec!["--flag".to_string(), "x".to_string()],
            workdir: None,
            env: BTreeMap::new(),
            resources: ComputeResources::default(),
            executor_constraint: None,
            inputs: Vec::new(),
            file_outputs: Vec::new(),
            workspace_outputs: Vec::new(),
            output_prefixes: Vec::new(),
            collision_policy: Default::default(),
        };
        let record = execution_record(JobId::from_bytes([3u8; 16]), user(2), spec.clone());
        let task = project_task(&record, &TaskFacts::default(), TesView::Basic, "http://x");
        assert_eq!(task.executors[0].command, vec!["/bin/tool", "--flag", "x"]);

        spec.entrypoint = None;
        let record = execution_record(JobId::from_bytes([4u8; 16]), user(2), spec);
        let task = project_task(&record, &TaskFacts::default(), TesView::Basic, "http://x");
        assert_eq!(task.executors[0].command, vec!["--flag", "x"]);
    }

    #[tokio::test]
    async fn rejects_basic() {
        let (_dir, state) = build_state(false).await;
        let group = Ulid::from_bytes([5u8; 16]);
        let access = sealed(&state, group);
        let mut revoked = access.clone();
        revoked.revoked_at = Some(SystemTime::now());
        let mut expired = access.clone();
        expired.expiry = SystemTime::UNIX_EPOCH;
        let mut foreign_issuer = access.clone();
        foreign_issuer.issued_by = [0u8; 32];

        for (access, secret) in [
            (access, "wrong-secret"),
            (revoked, "tes-secret"),
            (expired, "tes-secret"),
            (foreign_issuer, "tes-secret"),
        ] {
            write_credential(&state, &access).await;
            let error = authenticate_tes(&state, None, &basic_headers(&access, secret))
                .await
                .unwrap_err();
            assert_eq!(error.status, StatusCode::UNAUTHORIZED);
        }
    }

    #[tokio::test]
    async fn rejects_restricted_basic() {
        let (_dir, state) = build_state(false).await;
        let mut access = sealed(&state, Ulid::from_bytes([5u8; 16]));
        access.path_restrictions = Some(Vec::new());
        write_credential(&state, &access).await;

        let error = authenticate_tes(&state, None, &basic_headers(&access, TES_SECRET))
            .await
            .unwrap_err();
        assert_eq!(error.status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn creates_tagless_basic() {
        // Tagless basic auth infers the group and reaches admission; the
        // fixture has no network handle, so no family holder exists and the
        // honest single-node answer is the fixed-text 503, not an auth failure.
        let (_dir, state) = build_state(true).await;
        let group = Ulid::from_bytes([5u8; 16]);
        let access = sealed(&state, group);
        write_credential(&state, &access).await;
        write_auth(&state, group, access.user_identity).await;
        let mut task = sample_task(group);
        task.tags.remove(GROUP_TAG_KEY);

        let (spec, workspace) = map_task_to_spec(&task, Some(group), true).unwrap();
        assert_eq!(spec.group_id, group);
        assert!(workspace.is_none());

        let response = create_task(
            State(state.clone()),
            Extension(None),
            Extension(None),
            basic_headers(&access, TES_SECRET),
            Json(task),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["msg"], "job_placement_unavailable");
    }

    #[test]
    fn switches_input_mode() {
        // Inputs mount when S3 mounts are available and snapshot otherwise.
        let group = Ulid::from_bytes([5u8; 16]);
        let (mounted, _) = map_task_to_spec(&sample_task(group), None, true).unwrap();
        assert_eq!(mounted.inputs[0].mode, InputMode::Mount);
        let (snapshot, _) = map_task_to_spec(&sample_task(group), None, false).unwrap();
        assert_eq!(snapshot.inputs[0].mode, InputMode::Snapshot);
    }

    #[tokio::test]
    async fn snapshot_when_disabled() {
        // Without S3 mounts the mapping falls back to snapshot inputs, and the
        // create call reaches admission; the handle-less fixture has no family
        // holder, so 503 is the honest outcome.
        let (_dir, state) = build_state(false).await;
        let group = Ulid::from_bytes([5u8; 16]);
        let access = sealed(&state, group);
        write_credential(&state, &access).await;
        write_auth(&state, group, access.user_identity).await;

        let (spec, _) = map_task_to_spec(&sample_task(group), None, false).unwrap();
        assert_eq!(spec.inputs[0].mode, InputMode::Snapshot);

        let response = create_task(
            State(state.clone()),
            Extension(None),
            Extension(None),
            basic_headers(&access, TES_SECRET),
            Json(sample_task(group)),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn basic_scopes_tasks() {
        let (_dir, state) = build_state(false).await;
        let owner = user(2);
        let group = Ulid::from_bytes([5u8; 16]);
        let sibling = Ulid::from_bytes([6u8; 16]);
        let access = sealed(&state, group);
        write_credential(&state, &access).await;
        let headers = basic_headers(&access, TES_SECRET);
        let caller = authenticate_tes(&state, None, &headers).await.unwrap();
        assert_eq!(caller.auth.user_id, owner);
        assert_eq!(caller.credential_group, Some(group));

        let visible_id = JobId::from_bytes([9u8; 16]);
        let hidden_id = JobId::from_bytes([10u8; 16]);
        for (job_id, group_id) in [(visible_id, group), (hidden_id, sibling)] {
            let (spec, _) = map_task_to_spec(&sample_task(group_id), None, true).unwrap();
            insert_job(
                &state.get_ctx().storage_handle,
                &execution_record(job_id, owner, spec),
            )
            .await
            .unwrap();
        }

        let visible = get_task(
            State(state.clone()),
            Extension(None),
            Extension(None),
            ConnectInfo("127.0.0.1:1".parse().unwrap()),
            headers.clone(),
            Path(visible_id.to_string()),
            Query(ViewQuery::default()),
        )
        .await;
        assert_eq!(visible.status(), StatusCode::OK);
        let hidden = get_task(
            State(state.clone()),
            Extension(None),
            Extension(None),
            ConnectInfo("127.0.0.1:1".parse().unwrap()),
            headers.clone(),
            Path(hidden_id.to_string()),
            Query(ViewQuery::default()),
        )
        .await;
        assert_eq!(hidden.status(), StatusCode::NOT_FOUND);

        let hidden_cancel = cancel_task(
            State(state.clone()),
            Extension(None),
            Extension(None),
            headers.clone(),
            Path(format!("{hidden_id}:cancel")),
        )
        .await;
        assert_eq!(hidden_cancel.status(), StatusCode::NOT_FOUND);
        let visible_cancel = cancel_task(
            State(state.clone()),
            Extension(None),
            Extension(None),
            headers.clone(),
            Path(format!("{visible_id}:cancel")),
        )
        .await;
        assert_eq!(visible_cancel.status(), StatusCode::OK);

        let listed = list_tasks(
            State(state),
            Extension(None),
            ConnectInfo("127.0.0.1:1".parse().unwrap()),
            headers,
            RawQuery(None),
            Query(ListTasksQuery::default()),
        )
        .await;
        assert_eq!(listed.status(), StatusCode::OK);
        let body = axum::body::to_bytes(listed.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: TesListTasksResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.tasks.len(), 1);
        assert_eq!(page.tasks[0].id, Some(visible_id.to_string()));
    }

    #[tokio::test]
    async fn lists_zero_pagesize() {
        // page_size=0 must fall back to the default, not report an empty page.
        let (_dir, state) = build_state(false).await;
        let owner = user(2);
        let group = Ulid::from_bytes([5u8; 16]);
        let access = sealed(&state, group);
        write_credential(&state, &access).await;
        let headers = basic_headers(&access, TES_SECRET);
        let (spec, _) = map_task_to_spec(&sample_task(group), None, true).unwrap();
        insert_job(
            &state.get_ctx().storage_handle,
            &execution_record(JobId::from_bytes([9u8; 16]), owner, spec),
        )
        .await
        .unwrap();

        let listed = list_tasks(
            State(state),
            Extension(None),
            ConnectInfo("127.0.0.1:1".parse().unwrap()),
            headers,
            RawQuery(None),
            Query(ListTasksQuery {
                page_size: Some(0),
                ..Default::default()
            }),
        )
        .await;
        assert_eq!(listed.status(), StatusCode::OK);
        let body = axum::body::to_bytes(listed.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: TesListTasksResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.tasks.len(), 1);
    }

    #[tokio::test]
    async fn lists_derived_tags() {
        let (_dir, state) = build_state(false).await;
        let owner = user(2);
        let group = Ulid::from_bytes([5u8; 16]);
        let access = sealed(&state, group);
        write_credential(&state, &access).await;
        let headers = basic_headers(&access, TES_SECRET);
        let target = JobId::from_bytes([9u8; 16]);
        for job_id in [
            JobId::from_bytes([8u8; 16]),
            target,
            JobId::from_bytes([10u8; 16]),
        ] {
            let (spec, _) = map_task_to_spec(&sample_task(group), None, true).unwrap();
            insert_job(
                &state.get_ctx().storage_handle,
                &execution_record(job_id, owner, spec),
            )
            .await
            .unwrap();
        }

        let raw_query = format!("tag_key=aruna-engine.org%2Fjob-id&tag_value={target}");
        let listed = list_tasks(
            State(state),
            Extension(None),
            ConnectInfo("127.0.0.1:1".parse().unwrap()),
            headers,
            RawQuery(Some(raw_query)),
            Query(ListTasksQuery {
                view: Some("BASIC".to_string()),
                page_size: Some(1),
                ..Default::default()
            }),
        )
        .await;
        assert_eq!(listed.status(), StatusCode::OK);
        let body = axum::body::to_bytes(listed.into_body(), usize::MAX)
            .await
            .unwrap();
        let page: TesListTasksResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(page.tasks.len(), 1);
        assert_eq!(page.tasks[0].id, Some(target.to_string()));
        assert_eq!(
            page.tasks[0].tags.get(JOB_ID_TAG_KEY),
            Some(&target.to_string())
        );
        assert!(page.next_page_token.is_none());
    }

    #[tokio::test]
    async fn get_resolves() {
        let (_dir, state) = build_state(false).await;
        let owner = user(2);
        let (spec, _) =
            map_task_to_spec(&sample_task(Ulid::from_bytes([5u8; 16])), None, true).unwrap();
        let job_id = JobId::from_bytes([9u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &execution_record(job_id, owner, spec),
        )
        .await
        .unwrap();

        let response = get_task(
            State(state.clone()),
            Extension(auth_for(owner)),
            Extension(None),
            ConnectInfo("127.0.0.1:1".parse().unwrap()),
            HeaderMap::new(),
            Path(job_id.to_string()),
            Query(ViewQuery {
                view: Some("BASIC".to_string()),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        // A foreign caller cannot see it.
        let foreign = get_task(
            State(state.clone()),
            Extension(auth_for(user(3))),
            Extension(None),
            ConnectInfo("127.0.0.1:1".parse().unwrap()),
            HeaderMap::new(),
            Path(job_id.to_string()),
            Query(ViewQuery::default()),
        )
        .await;
        assert_eq!(foreign.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cancel_maps_through() {
        let (_dir, state) = build_state(false).await;
        let owner = user(2);
        let (spec, _) =
            map_task_to_spec(&sample_task(Ulid::from_bytes([5u8; 16])), None, true).unwrap();
        let job_id = JobId::from_bytes([9u8; 16]);
        insert_job(
            &state.get_ctx().storage_handle,
            &execution_record(job_id, owner, spec),
        )
        .await
        .unwrap();

        let ok = cancel_task(
            State(state.clone()),
            Extension(auth_for(owner)),
            Extension(None),
            HeaderMap::new(),
            Path(format!("{job_id}:cancel")),
        )
        .await;
        assert_eq!(ok.status(), StatusCode::OK);
        let body = to_bytes(ok.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"{}");

        // Missing the action suffix is a bad request.
        let bad = cancel_task(
            State(state.clone()),
            Extension(auth_for(owner)),
            Extension(None),
            HeaderMap::new(),
            Path(job_id.to_string()),
        )
        .await;
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn service_info_shape() {
        let info = TesServiceType {
            group: "org.ga4gh",
            artifact: "tes",
            version: TES_VERSION.to_string(),
        };
        assert_eq!(info.artifact, "tes");
        assert_eq!(info.version, "1.1.0");
    }

    #[test]
    fn openapi_has_tes() {
        let openapi = crate::openapi::ApiDoc::openapi();
        assert!(
            openapi
                .paths
                .paths
                .contains_key("/ga4gh/tes/v1/service-info")
        );
        assert!(openapi.paths.paths.contains_key("/ga4gh/tes/v1/tasks"));
        assert!(openapi.paths.paths.contains_key("/ga4gh/tes/v1/tasks/{id}"));
        assert!(
            openapi
                .components
                .is_some_and(|components| components.security_schemes.contains_key("basic_auth"))
        );
    }
}
