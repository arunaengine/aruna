//! Realm-admin placement-policy administration.
//!
//! Every handler here only validates transport input, builds an operation
//! configuration and maps domain errors: authorization, ref authentication and
//! all transactional rules live in the operations. None of these surfaces is
//! reachable without a realm bearer token, so policy ids never leak to a public
//! S3 caller.

use crate::auth::{ValidatedArunaBearerTokenCarrier, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::metadata::forwarded_auth_token;
use crate::server_state::ServerState;
use aruna_core::structs::{
    Actor, AuthContext, CurrentVersionPointer, LabelMatch, PlacementPolicy,
    PlacementPolicyDocument, PlacementPolicyError, PlacementPolicyRef, PlacementSelector,
    PolicyBlockedReason, PolicyBulkStatus,
};
use aruna_operations::driver::{drive, gate_context};
use aruna_operations::metadata::forward::MetadataWriteError;
use aruna_operations::placement_policy::create::{CreatePolicyConfig, CreatePolicyError};
use aruna_operations::placement_policy::diagnostics::{
    DiagnosticsError, DiagnosticsInput, PolicyDiagnosticsOperation,
};
use aruna_operations::placement_policy::read::{
    ReadPolicyConfig, ReadPolicyError, ReadPolicyOperation,
};
use aruna_operations::placement_policy::{PolicyForwardError, create_policy_routed};
use aruna_operations::s3::bucket_placement::{
    PutBucketPlacementError, PutBucketPlacementInput, PutBucketPlacementOperation,
};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::policy_bulk::{BulkConfig, BulkError, PolicyBulkOperation};
use aruna_operations::s3::policy_coverage::{
    CoverageError, CoverageInput, CoverageScope, PolicyCoverageOperation,
};
use aruna_operations::s3::policy_mutation::{
    PolicyMutationConfig, PolicyMutationError, PolicyMutationOperation,
};
use aruna_operations::s3::policy_successor::{SuccessorError, SuccessorOutcome};
use axum::extract::{Path, Query, State};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use ulid::Ulid;
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(tags((
    name = "placement",
    description = "Realm-admin administration of placement policies, bucket defaults and their application"
)))]
pub struct PlacementApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(PlacementApiDoc::openapi())
        .routes(routes!(create_placement_policy))
        .routes(routes!(get_placement_policy))
        .routes(routes!(get_placement_diagnostics))
        .routes(routes!(get_bucket_placement, put_bucket_placement))
        .routes(routes!(mint_object_placement))
        .routes(routes!(run_bucket_placement))
        .routes(routes!(get_placement_coverage))
}

/// A rule reference: the immutable policy id plus the digest of its definition.
/// Both are required, because an id alone could be answered with other bytes.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct PolicyRefBody {
    pub policy_id: String,
    /// Lowercase hex of the 32-byte definition digest.
    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct LabelMatchBody {
    pub key: String,
    pub value: String,
}

/// Fields inside one selector are ANDed; selectors are ORed.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct SelectorBody {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default)]
    pub labels: Vec<LabelMatchBody>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_kind: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CreatePolicyRequest {
    /// Optional caller-chosen id, so a retried publication is idempotent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_id: Option<String>,
    pub name: String,
    pub allowed: Vec<SelectorBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct PolicyResponse {
    pub policy_id: String,
    pub digest: String,
    pub name: String,
    pub allowed: Vec<SelectorBody>,
    /// The node that published the definition under realm-admin authority.
    pub publisher: String,
    pub created_by: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketPlacementRequest {
    /// The complete default set; an empty list clears it.
    pub policies: Vec<PolicyRefBody>,
    /// The generation the caller read, making the change a compare-and-set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_generation: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BucketPlacementResponse {
    pub bucket: String,
    pub policies: Vec<PolicyRefBody>,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ObjectPlacementRequest {
    pub key: String,
    /// Replay key of this mutation.
    pub mutation_id: String,
    pub expected_version_id: String,
    pub expected_generation: u64,
    /// The exact refs the successor carries; this is a replacement, not a union.
    pub policies: Vec<PolicyRefBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ObjectPlacementResponse {
    /// `minted`, `replayed` or `blocked`.
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialized: Option<bool>,
    /// Why nothing was written, for a blocked outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    pub policies: Vec<PolicyRefBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BulkRunRequest {
    /// Run identity; repeating it resumes the sealed run instead of starting a
    /// new one.
    pub operation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Hex cursor from a previous pass.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BlockedGapBody {
    pub key: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct BulkRunResponse {
    pub operation_id: String,
    /// `active`, `completed` or `superseded`.
    pub status: String,
    pub generation: u64,
    pub target_policies: Vec<PolicyRefBody>,
    pub observed: usize,
    pub covered: usize,
    pub minted: usize,
    pub replanned: usize,
    pub blocked: Vec<BlockedGapBody>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// True only when this pass exhausted the responder's own iterator.
    pub complete: bool,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct CoverageQuery {
    /// `current` (default) or `historical`.
    #[serde(default)]
    pub scope: Option<String>,
    /// Opaque continuation returned by the previous page.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Page bound; the server default applies when omitted.
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CoverageGapBody {
    pub key: String,
    pub version_id: String,
    /// `missing` or `partial`.
    pub attachment: String,
    /// `registered`, `quarantined`, `absent` or `reference_only`; absent in the
    /// historical scope, where local copy state is not the subject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub copy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CoverageResponse {
    pub bucket: String,
    pub scope: String,
    pub generation: u64,
    pub target_policies: Vec<PolicyRefBody>,
    pub observed: usize,
    pub deleted: usize,
    pub gaps: Vec<CoverageGapBody>,
    pub registered: usize,
    pub quarantined: usize,
    pub absent: usize,
    pub reference_only: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub complete: bool,
    /// What this report deliberately does not claim.
    pub limits: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct DiagnosticsQuery {
    /// Opaque continuation returned by the previous page.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Page bound; the server default applies when omitted.
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct CopyViolationBody {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub state: String,
    pub policies: Vec<PolicyRefBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DiagnosticsResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_location: Option<String>,
    pub policy_draining: bool,
    pub serving_blocked: bool,
    pub observed: usize,
    pub registered: usize,
    pub quarantined: usize,
    pub unresolved_departed: usize,
    pub violations: Vec<CopyViolationBody>,
    pub cache_entries: usize,
    pub cache_verified: usize,
    pub cache_unavailable: usize,
    pub cache_bytes: usize,
    pub cache_truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub complete: bool,
}

impl TryFrom<PolicyRefBody> for PlacementPolicyRef {
    type Error = ServerError;

    fn try_from(value: PolicyRefBody) -> Result<Self, Self::Error> {
        let policy_id = Ulid::from_string(&value.policy_id).map_err(|_| ServerError::BadRequest)?;
        let mut digest = [0u8; 32];
        hex::decode_to_slice(value.digest.as_bytes(), &mut digest)
            .map_err(|_| ServerError::BadRequest)?;
        Ok(Self { policy_id, digest })
    }
}

impl From<PlacementPolicyRef> for PolicyRefBody {
    fn from(value: PlacementPolicyRef) -> Self {
        Self {
            policy_id: value.policy_id.to_string(),
            digest: hex::encode(value.digest),
        }
    }
}

impl TryFrom<SelectorBody> for PlacementSelector {
    type Error = ServerError;

    fn try_from(value: SelectorBody) -> Result<Self, Self::Error> {
        let node_id = value
            .node_id
            .map(|node_id| {
                aruna_core::NodeId::from_str(&node_id).map_err(|_| ServerError::BadRequest)
            })
            .transpose()?;
        Ok(Self {
            node_id,
            location: value.location,
            labels: value
                .labels
                .into_iter()
                .map(|label| LabelMatch {
                    key: label.key,
                    value: label.value,
                })
                .collect(),
            executor_kind: value.executor_kind,
        })
    }
}

impl From<PlacementSelector> for SelectorBody {
    fn from(value: PlacementSelector) -> Self {
        Self {
            node_id: value.node_id.map(|node_id| node_id.to_string()),
            location: value.location,
            labels: value
                .labels
                .into_iter()
                .map(|label| LabelMatchBody {
                    key: label.key,
                    value: label.value,
                })
                .collect(),
            executor_kind: value.executor_kind,
        }
    }
}

fn refs_from(policies: Vec<PolicyRefBody>) -> ServerResult<Vec<PlacementPolicyRef>> {
    policies.into_iter().map(TryInto::try_into).collect()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

fn decode_cursor(cursor: Option<String>) -> ServerResult<Option<aruna_core::types::Key>> {
    cursor
        .map(|cursor| {
            hex::decode(cursor)
                .map(Into::into)
                .map_err(|_| ServerError::BadRequest)
        })
        .transpose()
}

fn policy_response(document: &PlacementPolicyDocument) -> ServerResult<PolicyResponse> {
    let policy_ref = document
        .policy_ref()
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(PolicyResponse {
        policy_id: policy_ref.policy_id.to_string(),
        digest: hex::encode(policy_ref.digest),
        name: document.policy.name.clone(),
        allowed: document
            .policy
            .allowed
            .iter()
            .cloned()
            .map(Into::into)
            .collect(),
        publisher: document.publication.publisher.to_string(),
        created_by: document.publication.created_by.to_string(),
        created_at_ms: document.publication.created_at_ms,
    })
}

fn map_create_error(error: PolicyForwardError) -> ServerError {
    match error {
        PolicyForwardError::Create(CreatePolicyError::Unauthorized) => ServerError::Forbidden,
        PolicyForwardError::Create(CreatePolicyError::Policy(
            PlacementPolicyError::PolicyIdReuse { .. },
        )) => ServerError::Conflict("policy id already carries another definition".to_string()),
        PolicyForwardError::Create(CreatePolicyError::Policy(reason)) => {
            ServerError::BadRequestReason(reason.to_string())
        }
        PolicyForwardError::Forward(MetadataWriteError::Unauthorized) => ServerError::Unauthorized,
        PolicyForwardError::Forward(MetadataWriteError::Forbidden) => ServerError::Forbidden,
        other => ServerError::ServiceUnavailableReason(other.to_string()),
    }
}

fn map_read_error(error: ReadPolicyError) -> ServerError {
    match error {
        ReadPolicyError::NotFound { .. }
        | ReadPolicyError::DigestMismatch
        | ReadPolicyError::RealmMismatch => ServerError::NotFound,
        ReadPolicyError::Unavailable(_) | ReadPolicyError::PlacementUnavailable => {
            ServerError::ServiceUnavailableReason("placement_policy_unavailable".to_string())
        }
        ReadPolicyError::Authority(_) => {
            ServerError::ServiceUnavailableReason("placement_policy_unverified".to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_default_error(error: PutBucketPlacementError) -> ServerError {
    match error {
        PutBucketPlacementError::Unauthorized => ServerError::Forbidden,
        PutBucketPlacementError::NoSuchBucket | PutBucketPlacementError::GroupMismatch => {
            ServerError::NotFound
        }
        PutBucketPlacementError::GenerationConflict { .. }
        | PutBucketPlacementError::GenerationExhausted => ServerError::Conflict(error.to_string()),
        PutBucketPlacementError::PolicyUnavailable { .. } => {
            ServerError::ServiceUnavailableReason("placement_policy_unavailable".to_string())
        }
        PutBucketPlacementError::Policy(reason) => {
            ServerError::BadRequestReason(reason.to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_successor_error(error: SuccessorError) -> ServerError {
    match error {
        SuccessorError::HeadConflict { .. }
        | SuccessorError::BucketChanged
        | SuccessorError::DefaultChanged { .. }
        | SuccessorError::MutationConflict(_)
        | SuccessorError::VersionCollision(_)
        | SuccessorError::IntentConflict
        | SuccessorError::HeadDeleted => ServerError::Conflict(error.to_string()),
        SuccessorError::VersionMissing => ServerError::NotFound,
        SuccessorError::Policy(reason) => ServerError::BadRequestReason(reason.to_string()),
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_mutation_error(error: PolicyMutationError) -> ServerError {
    match error {
        PolicyMutationError::Unauthorized => ServerError::Forbidden,
        PolicyMutationError::PolicyUnavailable { .. } => {
            ServerError::ServiceUnavailableReason("placement_policy_unavailable".to_string())
        }
        PolicyMutationError::Successor(error) => map_successor_error(error),
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_bulk_error(error: BulkError) -> ServerError {
    match error {
        BulkError::Unauthorized => ServerError::Forbidden,
        BulkError::NoSuchBucket => ServerError::NotFound,
        BulkError::BucketChanged => ServerError::Conflict(error.to_string()),
        other => ServerError::InternalError(other.to_string()),
    }
}

fn blocked_reason(reason: PolicyBlockedReason) -> String {
    match reason {
        PolicyBlockedReason::SourceUnavailable => "source_unavailable",
        PolicyBlockedReason::DestinationDenied => "destination_denied",
        PolicyBlockedReason::PolicyUnresolved => "policy_unresolved",
    }
    .to_string()
}

fn bulk_status(status: PolicyBulkStatus) -> String {
    match status {
        PolicyBulkStatus::Active => "active",
        PolicyBulkStatus::Completed => "completed",
        PolicyBulkStatus::Superseded => "superseded",
    }
    .to_string()
}

/// This node's advertised placement subject, without which nothing governed may
/// be minted here.
async fn local_subject(
    state: &ServerState,
    realm_id: aruna_core::structs::RealmId,
) -> ServerResult<aruna_core::structs::PlacementSubject> {
    gate_context(&state.get_ctx(), realm_id, now_ms())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .map(|gate| gate.subject)
        .ok_or_else(|| ServerError::ServiceUnavailableReason("no_placement_subject".to_string()))
}

async fn bucket_info(
    state: &ServerState,
    bucket: &str,
) -> ServerResult<aruna_core::structs::BucketInfo> {
    match drive(
        GetBucketInfoOperation::new(bucket.to_string()),
        &state.get_ctx(),
    )
    .await
    {
        Ok(Some(Ok(info))) => Ok(info),
        Ok(Some(Err(GetBucketInfoError::NotFound))) | Err(GetBucketInfoError::NotFound) => {
            Err(ServerError::NotFound)
        }
        Ok(Some(Err(error))) | Err(error) => Err(ServerError::InternalError(error.to_string())),
        Ok(None) => Err(ServerError::NotFound),
    }
}

#[utoipa::path(
    post,
    path = "/admin/placement-policies",
    tag = "placement",
    summary = "Publish an immutable placement policy",
    description = "Requires a bearer token issued for this realm and WRITE on the realm configuration path; the check runs inside the operation, so a caller without it is refused before anything is written. The definition is immutable: publishing the same id with the same selectors returns the stored document unchanged, while the same id with different selectors is refused with 409 and never replaces the stored rule. Omitting `policy_id` mints one. The document is committed on a holder of the bucket its id resolves to; when this node holds none, the publication is forwarded to a current holder under the caller's own token, and that holder re-runs the same admin check, so a relay never becomes the author. A 503 means no holder could be reached or this node cannot sign, and nothing was published. The response carries the publisher, the authorizing user and the definition digest that every later reference must name.",
    request_body(content = CreatePolicyRequest, example = json!({
        "name": "eu-residency",
        "allowed": [{ "location": "eu-west" }]
    })),
    responses(
        (status = 200, description = "The published document, or the identical one that already existed", body = PolicyResponse, example = json!({
            "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
            "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
            "name": "eu-residency",
            "allowed": [{ "location": "eu-west" }],
            "publisher": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
            "created_by": "01K2ZK4Q0X3D5M6P7R8S9T0V2A@0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0",
            "created_at_ms": 1755500000000u64
        })),
        (status = 400, description = "The definition is invalid, or an id, node id or digest could not be parsed", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller may not administer the realm configuration", body = ErrorResponse),
        (status = 409, description = "The policy id already carries a different definition", body = ErrorResponse),
        (status = 503, description = "No holder could commit the publication right now; nothing was written", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn create_placement_policy(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Json(request): Json<CreatePolicyRequest>,
) -> ServerResult<Json<PolicyResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let policy_id = request
        .policy_id
        .map(|policy_id| Ulid::from_string(&policy_id).map_err(|_| ServerError::BadRequest))
        .transpose()?
        .unwrap_or_else(Ulid::generate);
    let allowed = request
        .allowed
        .into_iter()
        .map(TryInto::try_into)
        .collect::<ServerResult<Vec<PlacementSelector>>>()?;
    let policy = PlacementPolicy::new(policy_id, request.name, allowed)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    let config = CreatePolicyConfig {
        actor: Actor {
            node_id: state.get_node_id(),
            user_id: auth.user_id,
            realm_id: auth.realm_id,
        },
        auth_context: auth,
        policy,
        created_at_ms: now_ms(),
    };
    let document = create_policy_routed(
        &state.get_ctx(),
        config,
        forwarded_auth_token(bearer_token)?,
    )
    .await
    .map_err(map_create_error)?;
    Ok(Json(policy_response(&document)?))
}

#[utoipa::path(
    get,
    path = "/admin/placement-policies/{policy_id}",
    tag = "placement",
    summary = "Read one placement policy by reference",
    description = "Requires a bearer token issued for this realm. Both the id and the `digest` of the definition are needed, because an id alone could be answered with other bytes; a digest that does not match what the holders serve is reported as 404 rather than returning a substituted rule. The document is read from this node when it holds a replica and otherwise fetched from the holders the policy id resolves to, and it is only returned after its original publication verified against this node's replicated realm view, so a relay cannot present itself as the author. A 503 means no holder answered or the publication could not be verified here; it never means the rule denies anything.",
    params(
        ("policy_id" = String, Path, description = "ULID of the policy"),
        ("digest" = String, Query, description = "Lowercase hex of the 32-byte definition digest")
    ),
    responses(
        (status = 200, description = "The authenticated policy document", body = PolicyResponse, example = json!({
            "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
            "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
            "name": "eu-residency",
            "allowed": [{ "location": "eu-west" }],
            "publisher": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
            "created_by": "01K2ZK4Q0X3D5M6P7R8S9T0V2A@0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0",
            "created_at_ms": 1755500000000u64
        })),
        (status = 400, description = "The id or digest could not be parsed", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm", body = ErrorResponse),
        (status = 404, description = "No holder has a policy with that id and digest", body = ErrorResponse),
        (status = 503, description = "No holder answered, or the publication could not be verified", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_placement_policy(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(policy_id): Path<String>,
    Query(query): Query<PolicyRefQuery>,
) -> ServerResult<Json<PolicyResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let policy_ref = PolicyRefBody {
        policy_id,
        digest: query.digest,
    }
    .try_into()?;
    let (authentic, _) = drive(
        ReadPolicyOperation::new(ReadPolicyConfig {
            realm_id: auth.realm_id,
            policy_ref,
            local_node_id: state.get_node_id(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_read_error)?;
    Ok(Json(policy_response(&authentic.document)?))
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct PolicyRefQuery {
    /// Lowercase hex of the 32-byte definition digest.
    pub digest: String,
}

#[utoipa::path(
    get,
    path = "/buckets/{bucket}/placement",
    tag = "placement",
    summary = "Read a bucket's default placement policies",
    description = "Requires a bearer token issued for this realm and READ on the realm configuration path, because the default names policy ids. This is a node-local read of the replicated bucket record: a default written on another node can be missing until it arrives here. The `generation` is the counter every default change advances, and it is what a bulk run seals and what a compare-and-set update must present. A bucket that has never been given a default returns an empty list at its current generation.",
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface")),
    responses(
        (status = 200, description = "The default ref set and the generation it was written at", body = BucketPlacementResponse, example = json!({
            "bucket": "datasets",
            "policies": [{
                "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
            }],
            "generation": 3
        })),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller may not read the realm configuration", body = ErrorResponse),
        (status = 404, description = "No bucket of that name is known to this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_bucket_placement(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
) -> ServerResult<Json<BucketPlacementResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    // Bucket defaults name policy ids; only realm-config readers may see them.
    crate::auth::ensure_permission(
        &state,
        &auth,
        aruna_core::structs::policy_admin_path(auth.realm_id),
        aruna_core::structs::Permission::READ,
    )
    .await?;
    let info = bucket_info(&state, &bucket).await?;
    Ok(Json(BucketPlacementResponse {
        bucket,
        policies: info
            .placement_policies
            .into_iter()
            .map(Into::into)
            .collect(),
        generation: info.placement_policy_generation,
    }))
}

#[utoipa::path(
    put,
    path = "/buckets/{bucket}/placement",
    tag = "placement",
    summary = "Replace a bucket's default placement policies",
    description = "Requires a bearer token issued for this realm and WRITE on the realm configuration path, checked inside the operation. The submitted list replaces the whole default set; an empty list clears it. Every ref is resolved and authenticated through the ordinary policy read before it can become a default, so a ref no holder can supply is refused with 503 and the stored default is untouched. A real change advances `placement_policy_generation` exactly once inside the same transaction; submitting the set that is already stored commits nothing and returns the current generation, so a replay cannot supersede a bulk run that sealed the same refs. Sending `expected_generation` makes the change a compare-and-set that is refused with 409 when another writer moved the default first. The default governs versions minted after it: stored versions keep their own refs until a successor is minted for them.",
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface")),
    request_body(content = BucketPlacementRequest, example = json!({
        "policies": [{
            "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
            "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
        }],
        "expected_generation": 3
    })),
    responses(
        (status = 200, description = "The default set as stored, with the generation it now carries", body = BucketPlacementResponse, example = json!({
            "bucket": "datasets",
            "policies": [{
                "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
            }],
            "generation": 4
        })),
        (status = 400, description = "A ref could not be parsed, or the set is not a valid ref set", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller may not administer the realm configuration", body = ErrorResponse),
        (status = 404, description = "No bucket of that name is known to this node", body = ErrorResponse),
        (status = 409, description = "The stored generation is not the expected one, or the counter is exhausted", body = ErrorResponse),
        (status = 503, description = "A referenced policy could not be authenticated; nothing was changed", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn put_bucket_placement(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<BucketPlacementRequest>,
) -> ServerResult<Json<BucketPlacementResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let policies = refs_from(request.policies)?;
    let stored = drive(
        PutBucketPlacementOperation::new(PutBucketPlacementInput {
            bucket: bucket.clone(),
            group_id: info.group_id,
            policies,
            expected_generation: request.expected_generation,
            auth_context: auth,
            local_node_id: state.get_node_id(),
            now_ms: now_ms(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_default_error)?;
    Ok(Json(BucketPlacementResponse {
        bucket,
        policies: stored.policies.into_iter().map(Into::into).collect(),
        generation: stored.generation,
    }))
}

#[utoipa::path(
    post,
    path = "/buckets/{bucket}/placement/objects",
    tag = "placement",
    summary = "Attach an exact policy set to one object",
    description = "Requires a bearer token issued for this realm and WRITE on the realm configuration path, checked inside the operation. This is an exact replacement, not a union: the successor carries exactly the submitted refs, so an explicit mutation may tighten or relax. Nothing stored is rewritten; a new version is minted that carries the new refs and the predecessor's bytes, and the predecessor keeps its own refs. The mutation advances the head only while it still is exactly `expected_version_id` at `expected_generation` and the bucket is still the same record, so a concurrent write is reported as 409 and the caller replans from the new head. Repeating the same `mutation_id` with the same parameters returns the version the first attempt assigned, which is what makes a lost response safe; the same id with different parameters is a 409. A materialized object needs a verified local copy of its bytes on a destination the new refs admit: without one the response is `blocked` with a reason and nothing was written, and the ordinary movement path has to stage and register compliant bytes first. A reference-only head mints a successor and registers no copy.",
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface")),
    request_body(content = ObjectPlacementRequest, example = json!({
        "key": "raw/sample.fastq",
        "mutation_id": "01K2ZK4Q0X3D5M6P7R8S9T0V4C",
        "expected_version_id": "01K2ZK4Q0X3D5M6P7R8S9T0V3B",
        "expected_generation": 7,
        "policies": [{
            "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
            "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
        }]
    })),
    responses(
        (status = 200, description = "The minted, replayed or blocked outcome", body = ObjectPlacementResponse, example = json!({
            "outcome": "minted",
            "version_id": "01K2ZK4Q0X3D5M6P7R8S9T0V5D",
            "materialized": true,
            "policies": [{
                "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
            }]
        })),
        (status = 400, description = "An id, version id or ref could not be parsed", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller may not administer the realm configuration", body = ErrorResponse),
        (status = 404, description = "No such bucket, or the expected head version no longer exists", body = ErrorResponse),
        (status = 409, description = "The head moved, the bucket changed, the mutation id was reused with other parameters, or the assigned version id is taken", body = ErrorResponse),
        (status = 503, description = "A referenced policy could not be authenticated, or this node advertises no placement subject; nothing was written", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn mint_object_placement(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<ObjectPlacementRequest>,
) -> ServerResult<Json<ObjectPlacementResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    let subject = local_subject(&state, auth.realm_id).await?;
    let mutation_id =
        Ulid::from_string(&request.mutation_id).map_err(|_| ServerError::BadRequest)?;
    let expected_version_id =
        Ulid::from_string(&request.expected_version_id).map_err(|_| ServerError::BadRequest)?;
    let outcome = drive(
        PolicyMutationOperation::new(PolicyMutationConfig {
            context: aruna_operations::blob::blob_keyspace_helper::HeadAliasContext::new(
                auth.realm_id,
                info.group_id,
                state.get_node_id(),
                bucket,
                request.key,
            ),
            auth_context: auth,
            mutation_id,
            expected_head: CurrentVersionPointer::new_with_generation(
                expected_version_id,
                request.expected_generation,
            ),
            bucket_identity: info.identity(),
            target_refs: refs_from(request.policies)?,
            subject,
            created_at: SystemTime::now(),
            now_ms: now_ms(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_mutation_error)?;
    Ok(Json(mutation_response(outcome)))
}

fn mutation_response(outcome: SuccessorOutcome) -> ObjectPlacementResponse {
    match outcome {
        SuccessorOutcome::Minted {
            version_id,
            refs,
            materialized,
        } => ObjectPlacementResponse {
            outcome: "minted".to_string(),
            version_id: Some(version_id.to_string()),
            materialized: Some(materialized),
            blocked_reason: None,
            policies: refs.into_iter().map(Into::into).collect(),
        },
        SuccessorOutcome::Replayed {
            version_id,
            refs,
            materialized,
        } => ObjectPlacementResponse {
            outcome: "replayed".to_string(),
            version_id: Some(version_id.to_string()),
            materialized: Some(materialized),
            blocked_reason: None,
            policies: refs.into_iter().map(Into::into).collect(),
        },
        SuccessorOutcome::Blocked(reason) => ObjectPlacementResponse {
            outcome: "blocked".to_string(),
            version_id: None,
            materialized: None,
            blocked_reason: Some(blocked_reason(reason)),
            policies: Vec::new(),
        },
    }
}

#[utoipa::path(
    post,
    path = "/buckets/{bucket}/placement/runs",
    tag = "placement",
    summary = "Apply the bucket default to this responder's current heads",
    description = "Requires a bearer token issued for this realm and WRITE on the realm configuration path, checked inside the operation. The first call under an `operation_id` seals the run against the bucket's exact identity, generation and default ref set; repeating that id resumes the sealed run, and every later pass is bound to what was sealed. The application is additive: each object's successor carries the union of the refs its head already had and the sealed target, so applying a default never removes a constraint. Exact replacement is a separate surface. One pass walks a bounded page of this responder's own heads and returns a `cursor` to continue with; heads that already carry the target and delete markers are counted as covered. An object whose bytes cannot be reused, whose destination the refs deny, or whose policy cannot be authenticated is retained as a durable blocked gap and retried by a later pass rather than being reported as done. A head that moved is replanned instead of advanced. When the bucket default changes the run is marked superseded and stops, so one run never mixes two policies. `complete` means this node's bounded iterator was exhausted, never that another partition converged.",
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface")),
    request_body(content = BulkRunRequest, example = json!({
        "operation_id": "01K2ZK4Q0X3D5M6P7R8S9T0V6E",
        "limit": 64
    })),
    responses(
        (status = 200, description = "What this pass observed, plus the resumable cursor and the run status", body = BulkRunResponse, example = json!({
            "operation_id": "01K2ZK4Q0X3D5M6P7R8S9T0V6E",
            "status": "active",
            "generation": 3,
            "target_policies": [{
                "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
            }],
            "observed": 64,
            "covered": 58,
            "minted": 4,
            "replanned": 1,
            "blocked": [{ "key": "raw/sample.fastq", "reason": "source_unavailable" }],
            "cursor": "6b0d",
            "complete": false
        })),
        (status = 400, description = "The operation id or cursor could not be parsed", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller may not administer the realm configuration", body = ErrorResponse),
        (status = 404, description = "No bucket of that name is known to this node", body = ErrorResponse),
        (status = 409, description = "The run was sealed against a different bucket record", body = ErrorResponse),
        (status = 503, description = "This node advertises no placement subject, so nothing governed can be minted here", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn run_bucket_placement(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(request): Json<BulkRunRequest>,
) -> ServerResult<Json<BulkRunResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let subject = local_subject(&state, auth.realm_id).await?;
    let operation_id =
        Ulid::from_string(&request.operation_id).map_err(|_| ServerError::BadRequest)?;
    let report = drive(
        PolicyBulkOperation::new(BulkConfig {
            operation_id,
            bucket,
            auth_context: auth,
            subject,
            start_after: decode_cursor(request.cursor)?,
            limit: request.limit.unwrap_or(BULK_DEFAULT_LIMIT),
            now_ms: now_ms(),
            created_at: SystemTime::now(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(map_bulk_error)?;
    Ok(Json(BulkRunResponse {
        operation_id: report.operation_id.to_string(),
        status: bulk_status(report.status),
        generation: report.generation,
        target_policies: report.target_refs.into_iter().map(Into::into).collect(),
        observed: report.observed,
        covered: report.covered,
        minted: report.minted,
        replanned: report.replanned,
        blocked: report
            .blocked
            .into_iter()
            .map(|gap| BlockedGapBody {
                key: gap.key,
                reason: blocked_reason(gap.reason),
            })
            .collect(),
        cursor: report.cursor.map(hex::encode),
        complete: report.complete,
    }))
}

/// Page size a pass uses when the caller names none.
const BULK_DEFAULT_LIMIT: usize = 64;
/// Page size a scan uses when the caller names none.
const SCAN_DEFAULT_LIMIT: usize = 128;

#[utoipa::path(
    get,
    path = "/buckets/{bucket}/placement/coverage",
    tag = "placement",
    summary = "Report responder-local coverage of the bucket default",
    description = "Requires a bearer token issued for this realm and READ on the realm configuration path, checked inside the operation. The report names the exact default ref set and generation it compared against and reports only what this responder stores: `complete` means its own bounded iterator was exhausted, not that another partition or a concurrent write was observed, and the `limits` list states that explicitly. Attachment gaps and local copy state are separate answers: an object can carry every ref and still have no serveable copy here, so zero gaps never implies that every registered copy is compliant. Scope `current` walks current heads; scope `historical` reports versions that are no longer the head and lack the default, which is diagnostic only, because minting successors never rewrites their immutable refs. Reference-only heads are included and labelled rather than omitted.",
    params(
        ("bucket" = String, Path, description = "Bucket name as used by the S3 surface"),
        CoverageQuery
    ),
    responses(
        (status = 200, description = "The bounded, responder-local coverage page", body = CoverageResponse, example = json!({
            "bucket": "datasets",
            "scope": "current",
            "generation": 3,
            "target_policies": [{
                "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
            }],
            "observed": 64,
            "deleted": 2,
            "gaps": [{
                "key": "raw/sample.fastq",
                "version_id": "01K2ZK4Q0X3D5M6P7R8S9T0V3B",
                "attachment": "missing",
                "copy": "registered"
            }],
            "registered": 60,
            "quarantined": 1,
            "absent": 1,
            "reference_only": 2,
            "complete": true,
            "limits": ["responder-local", "current-heads-only"]
        })),
        (status = 400, description = "The scope, cursor or limit could not be parsed", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller may not administer the realm configuration", body = ErrorResponse),
        (status = 404, description = "No bucket of that name is known to this node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_placement_coverage(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(query): Query<CoverageQuery>,
) -> ServerResult<Json<CoverageResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let scope = match query.scope.as_deref() {
        None | Some("current") => CoverageScope::CurrentHeads,
        Some("historical") => CoverageScope::Historical,
        Some(_) => return Err(ServerError::BadRequest),
    };
    let report = drive(
        PolicyCoverageOperation::new(CoverageInput {
            bucket: bucket.clone(),
            scope,
            start_after: decode_cursor(query.cursor)?,
            limit: query.limit.unwrap_or(SCAN_DEFAULT_LIMIT),
            auth_context: auth,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        CoverageError::Unauthorized => ServerError::Forbidden,
        CoverageError::NoSuchBucket => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok(Json(coverage_response(bucket, report)))
}

fn coverage_response(
    bucket: String,
    report: aruna_operations::s3::policy_coverage::CoverageReport,
) -> CoverageResponse {
    use aruna_operations::s3::policy_coverage::{AttachmentGap, CopyState, CoverageLimit};
    CoverageResponse {
        bucket,
        scope: match report.scope {
            CoverageScope::CurrentHeads => "current".to_string(),
            CoverageScope::Historical => "historical".to_string(),
        },
        generation: report.generation,
        target_policies: report.target_refs.into_iter().map(Into::into).collect(),
        observed: report.observed,
        deleted: report.deleted,
        gaps: report
            .gaps
            .into_iter()
            .map(|gap| CoverageGapBody {
                key: gap.key,
                version_id: gap.version_id.to_string(),
                attachment: match gap.attachment {
                    AttachmentGap::Missing => "missing".to_string(),
                    AttachmentGap::Partial => "partial".to_string(),
                },
                copy: gap.copy.map(|copy| {
                    match copy {
                        CopyState::Registered => "registered",
                        CopyState::Quarantined => "quarantined",
                        CopyState::Absent => "absent",
                        CopyState::ReferenceOnly => "reference_only",
                    }
                    .to_string()
                }),
            })
            .collect(),
        registered: report.copies.registered,
        quarantined: report.copies.quarantined,
        absent: report.copies.absent,
        reference_only: report.copies.reference_only,
        cursor: report.cursor.map(hex::encode),
        complete: report.complete,
        limits: report
            .limits
            .into_iter()
            .map(|limit| {
                match limit {
                    CoverageLimit::ResponderLocal => "responder_local",
                    CoverageLimit::BoundedPage => "bounded_page",
                    CoverageLimit::HistoricalExcluded => "historical_excluded",
                    CoverageLimit::ConcurrentWrites => "concurrent_writes",
                }
                .to_string()
            })
            .collect(),
    }
}

#[utoipa::path(
    get,
    path = "/admin/placement-diagnostics",
    tag = "placement",
    summary = "Inspect local policy enforcement, violations and cache coverage",
    description = "Requires a bearer token issued for this realm and READ on the realm configuration path, checked inside the operation. Everything reported is an observation of this node's own rows: the placement subject it advertises, whether serving is currently blocked or draining, and a bounded page of its registered copies. A copy that is quarantined or was last seen on a departed node is listed as a violation with the refs it was registered under; a serveable registration is counted but never listed, and it is not by itself a compliance claim. Cache figures are diagnostics only and never policy truth: an evicted entry only costs a refetch, and a negative entry is an availability hint that expires. `complete` refers to this node's bounded copy iterator, and `cache_truncated` says the cache scan hit its own bound.",
    params(DiagnosticsQuery),
    responses(
        (status = 200, description = "The responder-local enforcement, violation and cache page", body = DiagnosticsResponse, example = json!({
            "subject_generation": 5,
            "subject_location": "eu-west",
            "policy_draining": false,
            "serving_blocked": false,
            "observed": 128,
            "registered": 126,
            "quarantined": 2,
            "unresolved_departed": 0,
            "violations": [{
                "bucket": "datasets",
                "key": "raw/sample.fastq",
                "version_id": "01K2ZK4Q0X3D5M6P7R8S9T0V3B",
                "state": "quarantined",
                "policies": [{
                    "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                    "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
                }]
            }],
            "cache_entries": 12,
            "cache_verified": 11,
            "cache_unavailable": 1,
            "cache_bytes": 18432,
            "cache_truncated": false,
            "complete": true
        })),
        (status = 400, description = "The cursor or limit could not be parsed", body = ErrorResponse),
        (status = 401, description = "No bearer token was presented", body = ErrorResponse),
        (status = 403, description = "The token belongs to another realm, or the caller may not administer the realm configuration", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_placement_diagnostics(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<DiagnosticsQuery>,
) -> ServerResult<Json<DiagnosticsResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let report = drive(
        PolicyDiagnosticsOperation::new(DiagnosticsInput {
            auth_context: auth,
            start_after: decode_cursor(query.cursor)?,
            limit: query.limit.unwrap_or(SCAN_DEFAULT_LIMIT),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        DiagnosticsError::Unauthorized => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok(Json(DiagnosticsResponse {
        subject_generation: report.subject.as_ref().map(|subject| subject.generation),
        subject_location: report
            .subject
            .as_ref()
            .map(|subject| subject.location.clone()),
        policy_draining: report.policy_draining,
        serving_blocked: report.serving_blocked,
        observed: report.observed,
        registered: report.registered,
        quarantined: report.quarantined,
        unresolved_departed: report.unresolved_departed,
        violations: report
            .violations
            .into_iter()
            .map(|violation| CopyViolationBody {
                bucket: violation.version.bucket.clone(),
                key: violation.version.key.clone(),
                version_id: violation.version.version_id.to_string(),
                state: match violation.state {
                    aruna_core::structs::ManagedCopyState::Registered => "registered".to_string(),
                    aruna_core::structs::ManagedCopyState::Quarantined(_) => {
                        "quarantined".to_string()
                    }
                    aruna_core::structs::ManagedCopyState::UnresolvedDeparted => {
                        "unresolved_departed".to_string()
                    }
                },
                policies: violation.policies.into_iter().map(Into::into).collect(),
            })
            .collect(),
        cache_entries: report.cache.entries,
        cache_verified: report.cache.verified,
        cache_unavailable: report.cache.unavailable,
        cache_bytes: report.cache.bytes,
        cache_truncated: report.cache.truncated,
        cursor: report.cursor.map(hex::encode),
        complete: report.complete,
    }))
}

#[cfg(test)]
mod tests {
    use super::{PolicyRefBody, SelectorBody};
    use aruna_core::structs::{PlacementPolicyRef, PlacementSelector};
    use ulid::Ulid;

    #[test]
    fn ref_round_trips() {
        // A ref must survive the transport form exactly: a truncated digest
        // would silently name another definition.
        let policy_ref = PlacementPolicyRef {
            policy_id: Ulid::from_bytes([5u8; 16]),
            digest: [7u8; 32],
        };
        let body: PolicyRefBody = policy_ref.into();
        assert_eq!(body.digest.len(), 64);
        assert_eq!(
            PlacementPolicyRef::try_from(body).expect("ref parses"),
            policy_ref
        );
    }

    #[test]
    fn rejects_short_digest() {
        let body = PolicyRefBody {
            policy_id: Ulid::from_bytes([5u8; 16]).to_string(),
            digest: "00".to_string(),
        };
        assert!(PlacementPolicyRef::try_from(body).is_err());
    }

    #[test]
    fn selector_round_trips() {
        let selector = PlacementSelector {
            node_id: None,
            location: Some("eu-west".to_string()),
            labels: Vec::new(),
            executor_kind: None,
        };
        let body: SelectorBody = selector.clone().into();
        assert_eq!(
            PlacementSelector::try_from(body).expect("selector parses"),
            selector
        );
    }
}
