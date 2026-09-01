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
    PolicyBlockedReason, PolicyBulkStatus, VersionKey,
};
use aruna_operations::driver::{drive, gate_context, now_ms};
use aruna_operations::metadata::forward::MetadataWriteError;
use aruna_operations::placement_policy::create::{CreatePolicyConfig, CreatePolicyError};
use aruna_operations::placement_policy::diagnostics::{
    DiagnosticsError, DiagnosticsInput, PolicyDiagnosticsOperation,
};
use aruna_operations::placement_policy::list::{
    ListPoliciesError, ListPoliciesInput, ListPoliciesOperation, POLICY_LIST_DEFAULT,
};
use aruna_operations::placement_policy::names::PolicyNamesOperation;
use aruna_operations::placement_policy::read::{
    ReadPolicyConfig, ReadPolicyError, ReadPolicyOperation,
};
use aruna_operations::placement_policy::{
    PolicyForwardError, PolicyGateError, QuarantineError, ResolveQuarantineConfig,
    ResolveQuarantineOperation, create_policy_routed,
};
use aruna_operations::s3::bucket_placement::{
    PutBucketPlacementError, PutBucketPlacementInput, PutBucketPlacementOperation,
};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::object_placement::{
    ObjectPlacementError, ObjectPlacementInput, ObjectPlacementOperation,
};
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
    name = "data/placement",
    description = "Realm-admin administration of placement policies, bucket defaults and their application"
)))]
pub struct PlacementApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(PlacementApiDoc::openapi())
        .routes(routes!(create_placement_policy, list_placement_policies))
        .routes(routes!(get_placement_policy))
        .routes(routes!(get_placement_diagnostics))
        .routes(routes!(get_bucket_placement, put_bucket_placement))
        .routes(routes!(get_object_placement, mint_object_placement))
        .routes(routes!(run_bucket_placement))
        .routes(routes!(get_placement_coverage))
        .routes(routes!(resolve_placement_quarantine))
}

/// A rule reference: the immutable policy id plus the digest of its definition.
/// Both are required, because an id alone could be answered with other bytes.
/// `name` and `owner_group_id` are what this node resolved for the id and are
/// null when it holds no such rule; a request body may omit both.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct PolicyRefBody {
    pub policy_id: String,
    /// Lowercase hex of the 32-byte definition digest.
    pub digest: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub owner_group_id: Option<String>,
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
    /// Group that owns the rule; omitted or null publishes a realm-wide rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_group_id: Option<String>,
    pub allowed: Vec<SelectorBody>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct PolicyResponse {
    pub policy_id: String,
    pub digest: String,
    pub name: String,
    /// Group that owns the rule; null is realm-wide.
    pub owner_group_id: Option<String>,
    pub allowed: Vec<SelectorBody>,
    /// The node that published the definition under realm-admin authority.
    pub publisher: String,
    pub created_by: String,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct PolicyListQuery {
    /// Lists the realm-wide rules plus this group's own, readable by that
    /// group's administrators. Omitted, the page needs realm-configuration
    /// read and carries every rule this node holds.
    #[serde(default)]
    pub group_id: Option<String>,
    /// Opaque continuation returned by the previous page.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Page bound; the server default applies when omitted.
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct PolicyListResponse {
    /// Ascending by policy id.
    pub policies: Vec<PolicyResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// True only when this responder's bounded iterator was exhausted.
    pub complete: bool,
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

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct ObjectPlacementQuery {
    /// Object key within the bucket, without a leading slash.
    pub key: String,
}

/// What the object's current head carries, and the generation an exact-set
/// mutation has to present.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct ObjectPlacementView {
    pub bucket: String,
    pub key: String,
    pub version_id: String,
    pub generation: u64,
    pub policies: Vec<PolicyRefBody>,
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
            name: None,
            owner_group_id: None,
        }
    }
}

/// Joins refs with what this node holds for them. An id this node does not
/// hold stays a plain reference rather than failing the response.
async fn named_refs(
    state: &ServerState,
    realm_id: aruna_core::structs::RealmId,
    refs: Vec<PlacementPolicyRef>,
) -> ServerResult<Vec<PolicyRefBody>> {
    let names = drive(PolicyNamesOperation::new(realm_id, &refs), &state.get_ctx())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?;
    Ok(refs
        .into_iter()
        .map(|policy_ref| {
            let named = names.get(&policy_ref.policy_id);
            PolicyRefBody {
                name: named.map(|named| named.name.clone()),
                owner_group_id: named
                    .and_then(|named| named.owner_group_id)
                    .map(|group_id| group_id.to_string()),
                ..PolicyRefBody::from(policy_ref)
            }
        })
        .collect())
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
        owner_group_id: document
            .policy
            .owner_group_id
            .map(|group_id| group_id.to_string()),
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
        PutBucketPlacementError::Policy(_) => policy_denied(),
        PutBucketPlacementError::ForeignPolicy { .. } => foreign_policy(),
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
        SuccessorError::Policy(_) => policy_denied(),
        // The subject moved under the plan, so the evaluation it carries is
        // stale rather than wrong; nothing was written.
        SuccessorError::SubjectDrift => {
            ServerError::ServiceUnavailableReason("placement_subject_drift".to_string())
        }
        SuccessorError::Gate(error) => map_gate_error(error),
        other => ServerError::InternalError(other.to_string()),
    }
}

/// Bucket defaults name policy ids: a realm-configuration reader may see them,
/// and so may an administrator of the group that owns the bucket, who is also
/// the one allowed to change them.
async fn ensure_placement_read(
    state: &ServerState,
    auth: &AuthContext,
    group_id: aruna_core::types::GroupId,
) -> ServerResult<()> {
    let realm_reader = crate::auth::permission_granted(
        state,
        auth,
        aruna_core::structs::policy_admin_path(auth.realm_id),
        aruna_core::structs::Permission::READ,
    )
    .await?;
    if realm_reader {
        return Ok(());
    }
    crate::auth::ensure_permission(
        state,
        auth,
        aruna_core::structs::group_admin_path(auth.realm_id, group_id),
        aruna_core::structs::Permission::READ,
    )
    .await
}

/// A group-owned rule governs only its owner's buckets. The reason is readable
/// because the caller supplied the reference or already reads the default.
fn foreign_policy() -> ServerError {
    ServerError::BadRequestReason(
        "a placement policy owned by another group cannot govern this bucket".to_string(),
    )
}

/// A refusal never names a policy, a ref or a node; only its retryability is
/// disclosed.
fn policy_denied() -> ServerError {
    ServerError::BadRequestReason("placement_policy_denied".to_string())
}

fn map_gate_error(error: PolicyGateError) -> ServerError {
    match error {
        PolicyGateError::Denied { .. }
        | PolicyGateError::NoSubject
        | PolicyGateError::Invalid
        | PolicyGateError::Policy(_) => policy_denied(),
        PolicyGateError::Unavailable { .. }
        | PolicyGateError::Required { .. }
        | PolicyGateError::Drift
        | PolicyGateError::AdmissionStopped
        | PolicyGateError::Read(_) => {
            ServerError::ServiceUnavailableReason("placement_policy_unavailable".to_string())
        }
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
        PolicyMutationError::ForeignPolicy { .. } => foreign_policy(),
        other => ServerError::InternalError(other.to_string()),
    }
}

fn map_bulk_error(error: BulkError) -> ServerError {
    match error {
        BulkError::Unauthorized => ServerError::Forbidden,
        BulkError::NoSuchBucket => ServerError::NotFound,
        BulkError::BucketChanged => ServerError::Conflict(error.to_string()),
        BulkError::ForeignPolicy { .. } => foreign_policy(),
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
/// be minted here. A node that is blocked or draining is refused up front, so a
/// run is never started where the first mint would immediately stop it.
async fn local_subject(
    state: &ServerState,
    realm_id: aruna_core::structs::RealmId,
) -> ServerResult<aruna_core::structs::PlacementSubject> {
    let gate = gate_context(&state.get_ctx(), realm_id, now_ms())
        .await
        .map_err(|error| ServerError::InternalError(error.to_string()))?
        .ok_or_else(|| ServerError::ServiceUnavailableReason("no_placement_subject".to_string()))?;
    if !gate.admitting {
        return Err(ServerError::ServiceUnavailableReason(
            "placement_admission_stopped".to_string(),
        ));
    }
    Ok(gate.subject)
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
    path = "/data/placement/policies",
    tag = "data/placement",
    summary = "Publish an immutable placement policy",
    description = r#"Publishes an immutable placement policy definition and returns the document holders serve.

**Authentication**: realm bearer token. A realm-wide rule (no `owner_group_id`) needs WRITE on the
realm-configuration path `/{realm_id}/admin/config`; a group-owned rule needs WRITE on
`/{realm_id}/g/{owner_group_id}/admin`. The check runs inside the operation before anything else is
read, and every verifier re-runs the same check against its own replicated view.

**Behavior**
- A definition is immutable: omitting `policy_id` mints one, and republishing an id with the same
  selectors returns the stored document unchanged. The owner is part of the definition, so binding
  a rule to a group mints a different digest and therefore a different reference.
- A group-owned rule may only be referenced by that group's own buckets and objects; a reference
  from another group's bucket is refused.
- The document is committed on a holder of the bucket its id resolves to; when this node holds none,
  the publication is forwarded to a current holder under the caller's own token, and that holder
  re-runs the same admin check, so a relay never becomes the author.
- The response carries the publisher, the authorizing user and the definition digest that every
  later reference must name."#,
    request_body(content = CreatePolicyRequest, example = json!({
        "name": "eu-residency",
        "owner_group_id": "01JMETADATA0123456789ABCDE",
        "allowed": [
            { "location": "eu-west" }
        ]
    })),
    responses(
        (status = 200, description = "The published document, or the identical one that already existed", body = PolicyResponse, example = json!({
            "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
            "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
            "name": "eu-residency",
            "owner_group_id": "01JMETADATA0123456789ABCDE",
            "allowed": [
                { "location": "eu-west" }
            ],
            "publisher": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
            "created_by": "01K2ZK4Q0X3D5M6P7R8S9T0V2A@0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0",
            "created_at_ms": 1755500000000u64
        })),
        (status = 400, description = "The definition is invalid, or an id, node id or digest could not be parsed", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, no WRITE on the realm-configuration path for a realm-wide rule, or no WRITE on the owning group's admin path for a group-owned one", body = ErrorResponse),
        (status = 409, description = "The policy id already carries a different definition, which is never replaced", body = ErrorResponse),
        (status = 503, description = "No holder could commit the publication; nothing was written", body = ErrorResponse)
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
    let mut policy = PlacementPolicy::new(policy_id, request.name, allowed)
        .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    if let Some(owner_group_id) = request.owner_group_id.as_deref() {
        let owner = Ulid::from_string(owner_group_id).map_err(|_| ServerError::BadRequest)?;
        policy = policy
            .owned_by(owner)
            .map_err(|error| ServerError::BadRequestReason(error.to_string()))?;
    }
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
    path = "/data/placement/policies/{policy_id}",
    tag = "data/placement",
    summary = "Read one placement policy by reference",
    description = r#"Returns one authenticated placement policy document named by its id and definition digest.

**Authentication**: realm bearer token; this is the one placement route that needs no
realm-configuration permission.

**Behavior**
- Both the id and the `digest` of the definition are required, because an id alone could be answered
  with other bytes.
- The document is read from this node when it holds a replica and otherwise fetched from the holders
  the policy id resolves to, and it is only returned after its original publication verified against
  this node's replicated realm view, so a relay cannot present itself as the author."#,
    params(
        ("policy_id" = String, Path, description = "ULID of the policy"),
        ("digest" = String, Query, description = "Lowercase hex of the 32-byte definition digest")
    ),
    responses(
        (status = 200, description = "The authenticated policy document", body = PolicyResponse, example = json!({
            "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
            "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
            "name": "eu-residency",
            "owner_group_id": "01JMETADATA0123456789ABCDE",
            "allowed": [
                { "location": "eu-west" }
            ],
            "publisher": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
            "created_by": "01K2ZK4Q0X3D5M6P7R8S9T0V2A@0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0",
            "created_at_ms": 1755500000000u64
        })),
        (status = 400, description = "The id or digest could not be parsed", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm", body = ErrorResponse),
        (status = 404, description = "No holder has a policy with that id and digest; a mismatched digest is a 404 rather than a substituted rule", body = ErrorResponse),
        (status = 503, description = "No holder answered, or the publication could not be verified here; never a denial", body = ErrorResponse)
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
        name: None,
        owner_group_id: None,
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

#[utoipa::path(
    get,
    path = "/data/placement/policies",
    tag = "data/placement",
    summary = "List the placement policies this responder holds",
    description = r#"Returns one bounded page of the placement policy documents this node holds.

**Authentication**: realm bearer token. Without `group_id` the caller needs READ on the
realm-configuration path and sees every rule this node holds; with it the caller needs READ on
`/{realm_id}/g/{group_id}/admin` and the page holds the realm-wide rules plus that group's own.
The check runs inside the operation, so the listing is no existence oracle.

**Behavior**
- Policy documents replicate only to the holders their id resolves to, so a page names what this
  node stores rather than a realm-wide catalog.
- `complete` means this node's own bounded iterator was exhausted in the pass.
- Policies are ordered by ascending policy id; `next_cursor` continues after the last one returned.

**Limits**
- `limit` defaults to 50 and is capped at 200."#,
    params(PolicyListQuery),
    responses(
        (status = 200, description = "One bounded page of policies this node holds", body = PolicyListResponse, example = json!({
            "policies": [
                {
                    "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                    "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
                    "name": "eu-residency",
                    "owner_group_id": null,
                    "allowed": [
                        { "location": "eu-west" }
                    ],
                    "publisher": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
                    "created_by": "01K2ZK4Q0X3D5M6P7R8S9T0V2A@0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0",
                    "created_at_ms": 1755500000000u64
                }
            ],
            "next_cursor": "0197d0b41c1e3d5a6b7c8d9e0f1a2b3c",
            "complete": false
        })),
        (status = 400, description = "The cursor or limit could not be parsed", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the path the query names", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_placement_policies(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<PolicyListQuery>,
) -> ServerResult<Json<PolicyListResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let page = drive(
        ListPoliciesOperation::new(ListPoliciesInput {
            auth_context: auth,
            group_id: query
                .group_id
                .as_deref()
                .map(Ulid::from_string)
                .transpose()
                .map_err(|_| ServerError::BadRequest)?,
            start_after: decode_cursor(query.cursor)?,
            limit: query.limit.unwrap_or(POLICY_LIST_DEFAULT),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        ListPoliciesError::Unauthorized => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok(Json(PolicyListResponse {
        policies: page
            .policies
            .iter()
            .map(policy_response)
            .collect::<ServerResult<Vec<_>>>()?,
        next_cursor: page.cursor.map(hex::encode),
        complete: page.complete,
    }))
}

#[derive(Debug, Clone, Deserialize, IntoParams)]
pub struct PolicyRefQuery {
    /// Lowercase hex of the 32-byte definition digest.
    pub digest: String,
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/placement",
    tag = "data/placement",
    summary = "Read a bucket's default placement policies",
    description = r#"Returns the placement policy references a bucket applies by default, with their generation.

**Authentication**: realm bearer token with READ on the realm-configuration path, or READ on
`/{realm_id}/g/{group}/admin` for the group that owns the bucket, because the default names policy
ids.

**Behavior**
- This is a node-local read of the replicated bucket record: a default written on another node can
  be missing until it arrives here.
- The `generation` is the counter every default change advances, and it is what a bulk run seals and
  what a compare-and-set update must present.
- A bucket that has never been given a default returns an empty list at its current generation."#,
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface")),
    responses(
        (status = 200, description = "The default reference set and the generation it was written at", body = BucketPlacementResponse, example = json!({
            "bucket": "datasets",
            "policies": [
                {
                    "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                    "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
                }
            ],
            "generation": 3
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or neither realm-configuration read nor admin read on the bucket's group", body = ErrorResponse),
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
    let info = bucket_info(&state, &bucket).await?;
    ensure_placement_read(&state, &auth, info.group_id).await?;
    let policies = named_refs(&state, auth.realm_id, info.placement_policies).await?;
    Ok(Json(BucketPlacementResponse {
        bucket,
        policies,
        generation: info.placement_policy_generation,
    }))
}

#[utoipa::path(
    put,
    path = "/data/buckets/{bucket}/placement",
    tag = "data/placement",
    summary = "Replace a bucket's default placement policies",
    description = r#"Replaces the placement policy references a bucket applies by default to newly minted versions.

**Authentication**: realm bearer token with WRITE on the realm-configuration path, or WRITE on
`/{realm_id}/g/{group}/admin` for the group that owns the bucket. Both are checked inside the
operation.

**Behavior**
- The submitted list replaces the whole default set; an empty list clears it.
- Every reference must be realm-wide or owned by the bucket's own group; a rule another group owns
  is refused whoever attaches it.
- Every reference is resolved and authenticated through the ordinary policy read before it can
  become a default, so one no holder can supply leaves the stored default untouched.
- A real change advances `placement_policy_generation` exactly once inside the same transaction;
  submitting the set that is already stored commits nothing and returns the current generation, so a
  replay cannot supersede a bulk run that sealed the same references.
- Sending `expected_generation` makes the change a compare-and-set.
- The default governs versions minted after it: stored versions keep their own references until a
  successor is minted for them."#,
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface")),
    request_body(content = BucketPlacementRequest, example = json!({
        "policies": [
            {
                "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
            }
        ],
        "expected_generation": 3
    })),
    responses(
        (status = 200, description = "The default reference set as stored, with the generation it now carries", body = BucketPlacementResponse, example = json!({
            "bucket": "datasets",
            "policies": [
                {
                    "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                    "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
                }
            ],
            "generation": 4
        })),
        (status = 400, description = "A reference could not be parsed, the set is not a valid reference set, a referenced rule is owned by another group, or a placement rule refuses it under a fixed reason that names no policy, reference or node", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or neither realm-configuration write nor admin write on the bucket's group", body = ErrorResponse),
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
    let realm_id = auth.realm_id;
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
    let policies = named_refs(&state, realm_id, stored.policies).await?;
    Ok(Json(BucketPlacementResponse {
        bucket,
        policies,
        generation: stored.generation,
    }))
}

#[utoipa::path(
    get,
    path = "/data/buckets/{bucket}/placement/objects",
    tag = "data/placement",
    summary = "Read the placement rules one object's head carries",
    description = r#"Returns the placement references the object's current version carries, with the generation an exact-set change has to present.

**Authentication**: realm bearer token with READ on the object.

**Behavior**
- This is a node-local read of the head this node stores: a version written on another node is
  reported only once it has arrived here.
- `generation` is the head pointer's counter, which `POST /data/buckets/{bucket}/placement/objects`
  takes as `expected_generation`; `version_id` is what it takes as `expected_version_id`.
- Every reference carries the `name` and `owner_group_id` this node resolved for its id, and null
  for both when it holds no such rule.
- An object with no references is governed by nothing and returns an empty list."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used by the S3 surface"),
        ObjectPlacementQuery
    ),
    responses(
        (status = 200, description = "The references the current head carries", body = ObjectPlacementView, example = json!({
            "bucket": "datasets",
            "key": "raw/sample.fastq",
            "version_id": "01K2ZK4Q0X3D5M6P7R8S9T0V3B",
            "generation": 7,
            "policies": [
                {
                    "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                    "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d",
                    "name": "eu-residency",
                    "owner_group_id": null
                }
            ]
        })),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the object", body = ErrorResponse),
        (status = 404, description = "No bucket of that name is known to this node, or the key has no current version here", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_object_placement(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(query): Query<ObjectPlacementQuery>,
) -> ServerResult<Json<ObjectPlacementView>> {
    let auth = require_realm_auth(&state, auth)?;
    let info = bucket_info(&state, &bucket).await?;
    crate::auth::ensure_permission(
        &state,
        &auth,
        aruna_core::structs::blob_object_permission_path(
            auth.realm_id,
            info.group_id,
            state.get_node_id(),
            &bucket,
            &query.key,
        ),
        aruna_core::structs::Permission::READ,
    )
    .await?;
    let placement = drive(
        ObjectPlacementOperation::new(ObjectPlacementInput {
            bucket: bucket.clone(),
            key: query.key.clone(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        ObjectPlacementError::NoSuchKey => ServerError::NotFound,
        other => ServerError::InternalError(other.to_string()),
    })?;
    let policies = named_refs(&state, auth.realm_id, placement.policies).await?;
    Ok(Json(ObjectPlacementView {
        bucket,
        key: query.key,
        version_id: placement.version_id.to_string(),
        generation: placement.generation,
        policies,
    }))
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/placement/objects",
    tag = "data/placement",
    summary = "Attach an exact policy set to one object",
    description = r#"Mints a successor version of one object that carries exactly the submitted placement references.

**Authentication**: realm bearer token with WRITE on the realm-configuration path, or WRITE on
`/{realm_id}/g/{group}/admin` for the group that owns the bucket. Both are checked inside the
operation.

**Behavior**
- This is an exact replacement, not a union: the successor carries exactly the submitted references,
  so an explicit mutation may tighten or relax.
- Every reference must be realm-wide or owned by the bucket's own group; a rule another group owns
  is refused whoever attaches it.
- `expected_version_id` and `expected_generation` come from
  `GET /data/buckets/{bucket}/placement/objects`.
- Nothing stored is rewritten; a new version is minted that carries the new references and the
  predecessor's bytes, and the predecessor keeps its own.
- The mutation advances the head only while it still is exactly `expected_version_id` at
  `expected_generation` and the bucket is still the same record; a concurrent write is a 409 and the
  caller replans from the new head.
- Repeating the same `mutation_id` with the same parameters returns the version the first attempt
  assigned, which is what makes a lost response safe; the same id with other parameters is a 409.
- A materialized object needs a verified local copy of its bytes on a destination the new references
  admit: without one the outcome is `blocked` with a reason and nothing was written, and compliant
  bytes have to be staged and registered first.
- A reference-only head mints a successor and registers no copy."#,
    params(("bucket" = String, Path, description = "Bucket name as used by the S3 surface")),
    request_body(content = ObjectPlacementRequest, example = json!({
        "key": "raw/sample.fastq",
        "mutation_id": "01K2ZK4Q0X3D5M6P7R8S9T0V4C",
        "expected_version_id": "01K2ZK4Q0X3D5M6P7R8S9T0V3B",
        "expected_generation": 7,
        "policies": [
            {
                "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
            }
        ]
    })),
    responses(
        (status = 200, description = "The minted, replayed or blocked outcome", body = ObjectPlacementResponse, example = json!({
            "outcome": "minted",
            "version_id": "01K2ZK4Q0X3D5M6P7R8S9T0V5D",
            "materialized": true,
            "policies": [
                {
                    "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                    "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
                }
            ]
        })),
        (status = 400, description = "An id, version id or reference could not be parsed, a referenced rule is owned by another group, or a placement rule refuses the destination under a fixed reason that names no policy, reference or node", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or neither realm-configuration write nor admin write on the bucket's group", body = ErrorResponse),
        (status = 404, description = "No such bucket, or the expected head version no longer exists", body = ErrorResponse),
        (status = 409, description = "The head moved, the bucket changed, the mutation id was reused with other parameters, or the assigned version id is taken", body = ErrorResponse),
        (status = 503, description = "A referenced policy could not be authenticated, this node advertises no placement subject or is not admitting governed data, or its subject moved during the mutation; nothing was written and the call may be retried", body = ErrorResponse)
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
    let realm_id = auth.realm_id;
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
    let mut response = mutation_response(&outcome);
    response.policies = named_refs(&state, realm_id, outcome.refs()).await?;
    Ok(Json(response))
}

/// Everything but the refs, which the caller resolves names for.
fn mutation_response(outcome: &SuccessorOutcome) -> ObjectPlacementResponse {
    let (outcome, version_id, materialized, blocked_reason) = match outcome {
        SuccessorOutcome::Minted {
            version_id,
            materialized,
            ..
        } => ("minted", Some(*version_id), Some(*materialized), None),
        SuccessorOutcome::Replayed {
            version_id,
            materialized,
            ..
        } => ("replayed", Some(*version_id), Some(*materialized), None),
        SuccessorOutcome::Blocked(reason) => ("blocked", None, None, Some(blocked_reason(*reason))),
    };
    ObjectPlacementResponse {
        outcome: outcome.to_string(),
        version_id: version_id.map(|version_id| version_id.to_string()),
        materialized,
        blocked_reason,
        policies: Vec::new(),
    }
}

#[utoipa::path(
    post,
    path = "/data/buckets/{bucket}/placement/runs",
    tag = "data/placement",
    summary = "Apply the bucket default to local heads",
    description = r#"Runs one resumable pass applying the bucket's default references to this responder's current heads.

**Authentication**: realm bearer token with WRITE on the realm-configuration path, or WRITE on
`/{realm_id}/g/{group}/admin` for the group that owns the bucket. Both are checked inside the
operation.

**Behavior**
- Every sealed reference must be realm-wide or owned by the bucket's own group.
- The first call under an `operation_id` seals the run against the bucket's exact identity,
  generation and default reference set; repeating that id resumes the sealed run, and every later
  pass is bound to what was sealed.
- The application is additive: each object's successor carries the union of the references its head
  already had and the sealed target, so applying a default never removes a constraint. Exact
  replacement is the per-object route.
- One pass walks a bounded page of this responder's own heads and returns a `cursor` to continue
  with; heads that already carry the target and delete markers count as covered, and a head that
  moved is replanned.
- An object whose bytes cannot be reused, whose destination the references deny, or whose policy
  cannot be authenticated becomes a durable blocked gap that a later pass retries.
- Status `active` is resumable, including after a pass stopped because this node's own placement
  subject moved; `superseded` means the bucket default itself moved, so one run never mixes two
  policies.
- `complete` means this node's bounded iterator was exhausted, never that another partition
  converged.

**Limits**
- The default page is 64 heads."#,
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
            "target_policies": [
                {
                    "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                    "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
                }
            ],
            "observed": 64,
            "covered": 58,
            "minted": 4,
            "replanned": 1,
            "blocked": [
                {
                    "key": "raw/sample.fastq",
                    "reason": "source_unavailable"
                }
            ],
            "cursor": "6b0d",
            "complete": false
        })),
        (status = 400, description = "The operation id or cursor could not be parsed, or the sealed default references a rule owned by another group", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or neither realm-configuration write nor admin write on the bucket's group", body = ErrorResponse),
        (status = 404, description = "No bucket of that name is known to this node", body = ErrorResponse),
        (status = 409, description = "The run was sealed against a different bucket record", body = ErrorResponse),
        (status = 503, description = "This node advertises no placement subject or is not admitting governed data, so nothing governed can be minted here; the run was not started", body = ErrorResponse)
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
    let realm_id = auth.realm_id;
    let info = bucket_info(&state, &bucket).await?;
    let subject = local_subject(&state, auth.realm_id).await?;
    let operation_id =
        Ulid::from_string(&request.operation_id).map_err(|_| ServerError::BadRequest)?;
    let report = drive(
        PolicyBulkOperation::new(BulkConfig {
            operation_id,
            bucket,
            group_id: info.group_id,
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
    let target_policies = named_refs(&state, realm_id, report.target_refs).await?;
    Ok(Json(BulkRunResponse {
        operation_id: report.operation_id.to_string(),
        status: bulk_status(report.status),
        generation: report.generation,
        target_policies,
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
    path = "/data/buckets/{bucket}/placement/coverage",
    tag = "data/placement",
    summary = "Report responder-local coverage of the bucket default",
    description = r#"Reports how far this responder's own stored objects carry the bucket's default placement references.

**Authentication**: realm bearer token with READ on the realm-configuration path, checked inside the
operation.

**Behavior**
- The report names the exact default reference set and generation it compared against, and covers
  only what this responder stores: `complete` means its own bounded iterator was exhausted, and the
  `limits` list states what the report deliberately does not claim.
- Attachment gaps and local copy state are separate answers: an object can carry every reference and
  still have no serveable copy here, so zero gaps never implies that every registered copy complies.
- Scope `current` walks current heads; scope `historical` reports versions that are no longer the
  head and lack the default, which is diagnostic only, because minting successors never rewrites
  their immutable references.
- Reference-only heads are labelled rather than omitted."#,
    params(
        ("bucket" = String, Path, description = "Bucket name as used by the S3 surface"),
        CoverageQuery
    ),
    responses(
        (status = 200, description = "The bounded, responder-local coverage page", body = CoverageResponse, example = json!({
            "bucket": "datasets",
            "scope": "current",
            "generation": 3,
            "target_policies": [
                {
                    "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                    "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
                }
            ],
            "observed": 64,
            "deleted": 2,
            "gaps": [
                {
                    "key": "raw/sample.fastq",
                    "version_id": "01K2ZK4Q0X3D5M6P7R8S9T0V3B",
                    "attachment": "missing",
                    "copy": "registered"
                }
            ],
            "registered": 60,
            "quarantined": 1,
            "absent": 1,
            "reference_only": 2,
            "complete": true,
            "limits": [
                "responder_local",
                "historical_excluded"
            ]
        })),
        (status = 400, description = "The scope, cursor or limit could not be parsed", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the realm-configuration path", body = ErrorResponse),
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
    path = "/data/placement/diagnostics",
    tag = "data/placement",
    summary = "Inspect local policy enforcement, violations and cache coverage",
    description = r#"Reports this node's own placement subject, its policy violations and its policy cache figures.

**Authentication**: realm bearer token with READ on the realm-configuration path, checked inside the
operation.

**Behavior**
- Everything reported is an observation of this node's own rows: the placement subject it
  advertises, whether serving is currently blocked or the node is policy-draining, and a bounded
  page of its registered copies.
- A copy that is quarantined or was last seen on a departed node is listed as a violation with the
  references it was registered under; a serveable registration is counted but never listed, and
  being counted is no compliance claim.
- Cache figures are diagnostics only and never policy truth: an evicted entry only costs a refetch,
  and a negative entry is an availability hint that expires.
- `complete` refers to this node's bounded copy iterator, and `cache_truncated` says the cache scan
  hit its own bound."#,
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
            "violations": [
                {
                    "bucket": "datasets",
                    "key": "raw/sample.fastq",
                    "version_id": "01K2ZK4Q0X3D5M6P7R8S9T0V3B",
                    "state": "quarantined",
                    "policies": [
                        {
                            "policy_id": "01K2ZK4Q0X3D5M6P7R8S9T0V1W",
                            "digest": "9d3b0c1a2e4f5a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d"
                        }
                    ]
                }
            ],
            "cache_entries": 12,
            "cache_verified": 11,
            "cache_unavailable": 1,
            "cache_bytes": 18432,
            "cache_truncated": false,
            "complete": true
        })),
        (status = 400, description = "The cursor or limit could not be parsed", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no READ on the realm-configuration path", body = ErrorResponse)
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

/// One quarantined copy an operator decides about, named exactly.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct QuarantineResolveRequest {
    /// `revalidate` re-evaluates every local copy against the current subject;
    /// `release` first drops the local registrations of the named version.
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct QuarantineResolveResponse {
    pub released: bool,
    pub scanned: usize,
    pub restored: usize,
    pub quarantined: usize,
    /// True once nothing quarantined remains: this node admits and serves
    /// governed data again.
    pub cleared: bool,
}

#[utoipa::path(
    post,
    path = "/data/placement/quarantine",
    tag = "data/placement",
    summary = "Resolve the quarantined copies that block governed admission",
    description = r#"Revalidates or releases the quarantined copies that keep this node from admitting governed data.

**Authentication**: realm bearer token with WRITE on the realm-configuration path, checked inside
the operation.

**Behavior**
- While any copy stays quarantined this node serves no governed data and admits no new governed
  work, including new execution targets; this route is how an operator ends that state.
- List the quarantined copies with `GET /data/placement/diagnostics` first: each violation names the
  exact bucket, key and version to act on.
- `revalidate` re-evaluates every local registration against the subject this node advertises now,
  restoring the ones that comply again.
- `release` additionally drops every local registration of the one named version first, which makes
  that version locally unavailable rather than serveable and never deletes data on another node.
- The block ends only when the walk finds nothing quarantined, which the response reports as
  `cleared`; a still-quarantined copy leaves the node draining, which is the safe state, not an
  error.

**Limits** (all refused with 400)
- A release needs `bucket`, `key` and `version_id`.
- Sending them with `revalidate` is refused, so an accidental release is impossible."#,
    request_body(
        content = QuarantineResolveRequest,
        description = "The decision plus, for a release, the exact version it applies to",
        example = json!({
            "action": "release",
            "bucket": "datasets",
            "key": "raw/sample.fastq",
            "version_id": "01K2ZK4Q0X3D5M6P7R8S9T0V3B"
        })
    ),
    responses(
        (status = 200, description = "What the walk decided, and whether governed admission is open again", body = QuarantineResolveResponse, example = json!({
            "released": true,
            "scanned": 128,
            "restored": 127,
            "quarantined": 0,
            "cleared": true
        })),
        (status = 400, description = "Unknown `action`, a release without an exact version, or a version that could not be parsed", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm, or no WRITE on the realm-configuration path", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn resolve_placement_quarantine(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<QuarantineResolveRequest>,
) -> ServerResult<Json<QuarantineResolveResponse>> {
    let auth = require_realm_auth(&state, auth)?;
    let release = quarantine_release(&request)?;
    let resolution = drive(
        ResolveQuarantineOperation::new(ResolveQuarantineConfig {
            auth_context: auth.clone(),
            realm_id: auth.realm_id,
            release,
            now_ms: now_ms(),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        QuarantineError::Unauthorized => ServerError::Forbidden,
        other => ServerError::InternalError(other.to_string()),
    })?;
    Ok(Json(QuarantineResolveResponse {
        released: resolution.released,
        scanned: resolution.scanned,
        restored: resolution.restored,
        quarantined: resolution.quarantined,
        cleared: resolution.cleared,
    }))
}

/// The version a release names, or `None` for a plain revalidation. A version
/// sent with `revalidate` is refused rather than silently ignored.
fn quarantine_release(request: &QuarantineResolveRequest) -> ServerResult<Option<VersionKey>> {
    let named = request.bucket.is_some() || request.key.is_some() || request.version_id.is_some();
    match request.action.as_str() {
        "revalidate" if !named => Ok(None),
        "release" => {
            let (Some(bucket), Some(key), Some(version_id)) = (
                request.bucket.as_deref(),
                request.key.as_deref(),
                request.version_id.as_deref(),
            ) else {
                return Err(ServerError::BadRequestReason(
                    "a release names an exact bucket, key and version_id".to_string(),
                ));
            };
            let version_id = Ulid::from_string(version_id).map_err(|_| ServerError::BadRequest)?;
            Ok(Some(VersionKey::new(bucket, key, version_id)))
        }
        _ => Err(ServerError::BadRequestReason(
            "action must be revalidate or release".to_string(),
        )),
    }
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
            name: None,
            owner_group_id: None,
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

#[cfg(test)]
mod route_tests {
    use super::{ObjectPlacementQuery, get_object_placement};
    use crate::error::ServerError;
    use crate::openapi::ApiDoc;
    use crate::server_state::ServerState;
    use aruna_core::effects::StorageEffect;
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::keyspaces::{
        AUTH_KEYSPACE, BLOB_HEAD_KEYSPACE, BLOB_VERSIONS_KEYSPACE, GROUP_KEYSPACE,
        REALM_CONFIG_KEYSPACE, S3_BUCKET_KEYSPACE,
    };
    use aruna_core::structs::{
        Actor, AuthContext, BackendRef, BlobHeadKey, BlobVersion, BucketInfo,
        CurrentVersionPointer, Group, GroupAuthorizationDocument, NodeCapabilities,
        PlacementPolicy, PlacementPolicyDocument, PlacementPolicyRef, PlacementSelector,
        PolicyPublicationClaim, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
        VerifiedPolicy, VersionKey, placement_policy_key,
    };
    use aruna_core::types::UserId;
    use aruna_operations::driver::DriverContext;
    use aruna_operations::jobs::runtime::JobsRuntime;
    use aruna_storage::FjallStorage;
    use axum::Extension;
    use axum::extract::{Path, Query, State};
    use byteview::ByteView;
    use std::sync::Arc;
    use std::time::SystemTime;
    use tempfile::TempDir;
    use ulid::Ulid;

    const BUCKET: &str = "datasets";
    const KEY: &str = "raw/sample.fastq";

    fn node_id() -> aruna_core::NodeId {
        iroh::SecretKey::from_bytes(&[3u8; 32]).public()
    }

    fn realm_id() -> RealmId {
        RealmId::from_bytes(
            *ed25519_dalek::SigningKey::from_bytes(&[7u8; 32])
                .verifying_key()
                .as_bytes(),
        )
    }

    fn policy() -> VerifiedPolicy {
        let policy = PlacementPolicy::new(
            Ulid::from_bytes([4u8; 16]),
            "eu-residency".to_string(),
            vec![PlacementSelector {
                node_id: None,
                location: Some("eu-west".to_string()),
                labels: Vec::new(),
                executor_kind: None,
            }],
        )
        .expect("policy is valid");
        VerifiedPolicy::verify(policy).expect("policy verifies")
    }

    async fn write_fixture(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        match state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(value),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::WriteResult { .. }) => {}
            other => panic!("unexpected fixture write event: {other:?}"),
        }
    }

    /// One governed object whose head this node holds, plus the rule its ref
    /// names, so a name can be resolved locally.
    async fn setup(owner: UserId) -> (TempDir, Arc<ServerState>, Ulid) {
        let dir = tempfile::tempdir().expect("temp dir");
        let storage =
            FjallStorage::open(dir.path().to_str().expect("temp path")).expect("storage opens");
        let realm_id = realm_id();
        let state = Arc::new(
            ServerState::new(
                Arc::new(DriverContext {
                    storage_handle: storage,
                    net_handle: None,
                    blob_handle: None,
                    metadata_handle: None,
                    task_handle: None,
                    compute_handle: None,
                }),
                realm_id,
                node_id(),
                NodeCapabilities::user_node(realm_id).expect("capabilities"),
                false,
                None,
                JobsRuntime::new(),
            )
            .await,
        );
        let group_id = Ulid::from_bytes([11u8; 16]);
        let actor = Actor {
            node_id: node_id(),
            user_id: owner,
            realm_id,
        };
        write_fixture(
            &state,
            AUTH_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmAuthorizationDocument::new_default_realm_doc(realm_id)
                .to_bytes(&actor)
                .expect("realm auth serializes"),
        )
        .await;
        let group_auth =
            GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
        write_fixture(
            &state,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group_auth.to_bytes(&actor).expect("group auth serializes"),
        )
        .await;
        write_fixture(
            &state,
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            Group {
                display_name: "placement-group".to_string(),
                group_id,
                realm_id,
                roles: group_auth.roles.keys().copied().collect(),
                owner,
            }
            .to_bytes(&actor)
            .expect("group serializes"),
        )
        .await;
        write_fixture(
            &state,
            REALM_CONFIG_KEYSPACE,
            realm_id.as_bytes().to_vec(),
            RealmConfigDocument::default_for_realm(realm_id, Vec::new())
                .to_bytes(&actor)
                .expect("realm config serializes"),
        )
        .await;
        write_fixture(
            &state,
            S3_BUCKET_KEYSPACE,
            BUCKET.as_bytes().to_vec(),
            BucketInfo {
                group_id,
                created_at: SystemTime::UNIX_EPOCH,
                created_by: owner,
                cors_configuration: None,
                storage_routing: Vec::new(),
                placement_policies: Vec::new(),
                placement_policy_generation: 0,
            }
            .to_bytes()
            .expect("bucket serializes"),
        )
        .await;

        let version_id = Ulid::from_bytes([9u8; 16]);
        let policy = policy();
        let version = BlobVersion::materialized(
            [7u8; 32],
            BackendRef::node_default(),
            SystemTime::UNIX_EPOCH,
            owner,
            None,
        )
        .with_policies(vec![policy.policy_ref()])
        .expect("refs seal");
        write_fixture(
            &state,
            BLOB_HEAD_KEYSPACE,
            BlobHeadKey::new(BUCKET, KEY).to_bytes().expect("head key"),
            CurrentVersionPointer::new_with_generation(version_id, 7)
                .to_bytes()
                .expect("pointer serializes"),
        )
        .await;
        write_fixture(
            &state,
            BLOB_VERSIONS_KEYSPACE,
            VersionKey::new(BUCKET, KEY, version_id)
                .to_bytes()
                .expect("version key"),
            version.to_bytes().expect("version serializes"),
        )
        .await;
        let secret = iroh::SecretKey::from_bytes(&[3u8; 32]);
        let publication = PolicyPublicationClaim::new(
            realm_id,
            &policy,
            secret.public(),
            owner,
            Ulid::from_bytes([5u8; 16]),
            7,
            [0u8; 32],
        )
        .sign(&secret);
        let document = PlacementPolicyDocument::new(realm_id, &policy, publication);
        write_fixture(
            &state,
            aruna_core::keyspaces::PLACEMENT_POLICY_KEYSPACE,
            placement_policy_key(policy.policy().policy_id),
            document.to_bytes().expect("document serializes"),
        )
        .await;
        (dir, state, version_id)
    }

    fn auth(owner: UserId) -> AuthContext {
        AuthContext {
            user_id: owner,
            realm_id: realm_id(),
            path_restrictions: None,
            session: None,
        }
    }

    #[tokio::test]
    async fn reads_object_refs() {
        // The head generation and version id are exactly what an exact-set
        // change presents, and a ref this node holds carries its name.
        let owner = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id());
        let (_dir, state, version_id) = setup(owner).await;

        let Ok(axum::Json(view)) = get_object_placement(
            State(state),
            Extension(Some(auth(owner))),
            Path(BUCKET.to_string()),
            Query(ObjectPlacementQuery {
                key: KEY.to_string(),
            }),
        )
        .await
        else {
            panic!("the object placement read must succeed");
        };

        assert_eq!(view.bucket, BUCKET);
        assert_eq!(view.key, KEY);
        assert_eq!(view.version_id, version_id.to_string());
        assert_eq!(view.generation, 7);
        assert_eq!(view.policies.len(), 1);
        assert_eq!(view.policies[0].name.as_deref(), Some("eu-residency"));
        assert!(view.policies[0].owner_group_id.is_none());
        assert_eq!(
            PlacementPolicyRef::try_from(view.policies[0].clone()).expect("ref parses"),
            policy().policy_ref()
        );
    }

    #[tokio::test]
    async fn unknown_key_missing() {
        let owner = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id());
        let (_dir, state, _) = setup(owner).await;

        let error = get_object_placement(
            State(state),
            Extension(Some(auth(owner))),
            Path(BUCKET.to_string()),
            Query(ObjectPlacementQuery {
                key: "raw/missing.fastq".to_string(),
            }),
        )
        .await
        .expect_err("an unknown key has no head");

        assert!(matches!(error, ServerError::NotFound));
    }

    #[tokio::test]
    async fn refuses_foreign_reader() {
        // A realm member outside the bucket's group holds no READ on the object.
        let owner = UserId::local(Ulid::from_bytes([2u8; 16]), realm_id());
        let (_dir, state, _) = setup(owner).await;
        let outsider = UserId::local(Ulid::from_bytes([6u8; 16]), realm_id());

        let error = get_object_placement(
            State(state),
            Extension(Some(auth(outsider))),
            Path(BUCKET.to_string()),
            Query(ObjectPlacementQuery {
                key: KEY.to_string(),
            }),
        )
        .await
        .expect_err("a caller without READ is refused");

        assert!(matches!(error, ServerError::Forbidden));
    }

    #[test]
    fn openapi_lists_objects() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).expect("openapi serializes");
        let path = &openapi["paths"]["/data/buckets/{bucket}/placement/objects"];
        assert!(path.get("get").is_some());
        assert!(path.get("post").is_some());
        assert!(
            openapi["components"]["schemas"]
                .get("ObjectPlacementView")
                .is_some()
        );
    }
}
