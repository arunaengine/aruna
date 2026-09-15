//! Metadata profile validation routes: thin request-to-operation conversion
//! over the shared `crate::metadata` adapter.

use crate::auth::{ValidatedBearer, parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerResult};
use crate::metadata::{
    ProfileCapabilitiesResponse, ProfilePreviewRequest, ProfilePreviewResponse,
    ProfileValidationResponse, ensure_metadata_scope, forwarded_auth_token, map_api_error,
    map_metadata_error, parse_document_id, serialize_jsonld_object,
};
use crate::server_state::ServerState;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_operations::metadata::api::GetVisibleRequest;
use aruna_operations::metadata::forward::route_profile_status as run_profile_validation_status;
use aruna_operations::metadata::profile_validation::{
    SUPPORTED_PROFILE_CONSTRAINTS, evaluator_name, preview_submission as run_preview_submission,
};
use aruna_operations::metadata::public_preview::restricted_files as run_restricted_files;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use std::sync::Arc;

#[utoipa::path(
    get,
    path = "/metadata/profile/validation/capabilities",
    tag = "metadata/validation",
    summary = "Get backend Profile validation capabilities",
    description = r#"Reports how this node compiles and evaluates registered Profile shapes.

**Authentication**: none; public route, every caller gets the same capability report.

**Behavior**
- Registered Profile shapes are compiled and executed server side by craqle's native SHACL Core
  Subset v1 engine.
- Supported targets are `sh:targetClass`, `sh:targetNode`, `sh:targetSubjectsOf`,
  `sh:targetObjectsOf` and implicit class targets; a node shape that names no target at all is
  bound to the crate root, so a Profile can constrain the root entity without knowing its minted
  IRI.
- Supported paths are predicate, `sh:inversePath`, sequence, `sh:alternativePath`,
  `sh:zeroOrOnePath`, `sh:zeroOrMorePath` and `sh:oneOrMorePath`.
- `sh:class` is exact `rdf:type` membership: no RDFS or OWL inference is applied, so a subclass
  instance does not satisfy a superclass constraint.
- Shapes may address the crate root relatively; any other crate-local id fails closed with the
  `crate_local_reference` rule.

**Limits**
- SHACL-SPARQL, SHACL-JS, SHACL-AF, custom components and targets, recursive shapes, RDF-star
  terms and remote `owl:imports` fail closed with an `unsupported_constraint` finding that names
  the construct; the same finding is returned when the registered Turtle cannot be parsed.
- Evaluation is bounded: exceeding the result, path-edge or path-depth budget returns a permanent
  `validation_limit` finding with incomplete completeness instead of a partial verdict."#,
    responses((
        status = 200,
        description = "Evaluator identity, exact supported constraints, fail-closed policy, and accepted Profile IRI forms",
        body = ProfileCapabilitiesResponse,
        example = json!({
            "evaluator": "craqle-shacl-core/0.2",
            "supported_constraints": [
                "sh:targetClass",
                "sh:property",
                "sh:path",
                "sh:minCount",
                "sh:maxCount",
                "sh:datatype",
                "sh:class",
                "sh:nodeKind",
                "sh:pattern",
                "sh:in",
                "sh:hasValue",
                "sh:closed"
            ],
            "unsupported_constraint_policy": "fail_closed",
            "public_profile_iri_template": "https://w3id.org/aruna/profile/{id}"
        })
    ))
)]
pub async fn profile_validation_capabilities() -> (StatusCode, Json<ProfileCapabilitiesResponse>) {
    (
        StatusCode::OK,
        Json(ProfileCapabilitiesResponse {
            evaluator: evaluator_name().to_string(),
            supported_constraints: SUPPORTED_PROFILE_CONSTRAINTS
                .iter()
                .map(|constraint| (*constraint).to_string())
                .collect(),
            unsupported_constraint_policy: "fail_closed".to_string(),
            profile_iri_template: "https://w3id.org/aruna/profile/{id}".to_string(),
        }),
    )
}

#[utoipa::path(
    post,
    path = "/metadata/profile/validation/preview",
    tag = "metadata/validation",
    summary = "Preview the Profile verdict for a draft",
    description = r#"Runs the Profile and structural verdict for a draft crate without storing it.

**Authentication**: realm bearer token; a `group_id` additionally needs READ on that group's
metadata path, because the group's own Profiles resolve for it.

**Behavior**
- Applies the exact verdict `POST /metadata` and `PUT /metadata/{document_id}/rocrate` would
  enforce, and stores nothing.
- `accepted` is true only when the structural RO-Crate rules and the Profile constraints would
  both let the write through.
- The draft is evaluated under its own crate root, so focus nodes and paths are reported in
  crate-local form with the root as `./`.
- `group_id` names the group the draft would be saved in. A Profile of that group resolves even
  while it is not public; without it only public Profiles resolve, so a group Profile reports
  `profile_not_registered`.
- A built-in Profile such as `https://w3id.org/ro/wfrun/process/0.5` resolves from shapes the node
  ships, in any group and with no registry row: `profile_id` is absent and `profile_revision` is
  `builtin`.
- `public` marks a draft that would be saved as a public dataset. It then lists every File or
  MediaObject entity that resolves to an Aruna object the realm's anonymous principal may not
  read, as `restricted_files`. Each entry carries `permission_path`, the object's full
  permission path, so READ can be granted on exactly that object. Resolution only follows paths
  the caller may read, so an object the caller cannot read is reported by entity id alone,
  without `permission_path`, `bucket` or `key`. The list is advisory: it never changes
  `accepted`, and it stays empty when `public` is false. `restricted_files_complete` is false
  when limits, remote-only objects or unavailable authorization prevent a complete check;
  an empty list then does not establish public readability. `group_id` identifies the owning
  group for each caller-readable permission path."#,
    request_body(content = ProfilePreviewRequest,
        example = json!({
            "group_id": "01JGROUP00000000000000000",
            "public": true,
            "rocrate": {
                "@context": "https://w3id.org/ro/crate/1.3/context",
                "@graph": [
                    {
                        "@id": "ro-crate-metadata.json",
                        "@type": "CreativeWork",
                        "conformsTo": { "@id": "https://w3id.org/ro/crate/1.3" },
                        "about": { "@id": "./" }
                    },
                    {
                        "@id": "./",
                        "@type": "Dataset",
                        "name": "Draft dataset",
                        "description": "Validated before it is saved",
                        "datePublished": "2026-08-22",
                        "conformsTo": {
                            "@id": "https://w3id.org/aruna/profile/01JPROFILE0000000000000000"
                        }
                    }
                ]
            }
        })),
    responses(
        (status = 200, description = "Verdict for the draft, including structural violations and Profile findings", body = ProfilePreviewResponse,
            example = json!({
                "accepted": false,
                "state": "invalid",
                "profile_id": "01JPROFILE0000000000000000",
                "profile_iri": "https://w3id.org/aruna/profile/01JPROFILE0000000000000000",
                "profile_revision": "01JPROFILEREVISION00000000",
                "evaluator": "craqle-shacl-core/0.2",
                "findings": [
                    {
                        "code": "constraint_violation",
                        "severity": "violation",
                        "focus_node": "./",
                        "path": "http://schema.org/identifier",
                        "rule": "http://www.w3.org/ns/shacl#minCount",
                        "message": "fewer values are present than the Profile requires",
                        "profile_revision": "01JPROFILEREVISION00000000",
                        "completeness": "complete"
                    }
                ],
                "completeness": "complete",
                "structural_violations": [],
                "restricted_files": [
                    {
                        "entity_id": "https://w3id.org/aruna/data/0000000000000000000000000000000000000000000000000000000000000000",
                        "group_id": "01JGROUP00000000000000000",
                        "permission_path": "/01JREALM00000000000000000/g/01JGROUP00000000000000000/data/01JNODE000000000000000000/reads/raw/one.csv",
                        "bucket": "reads",
                        "key": "raw/one.csv"
                    }
                ],
                "restricted_files_complete": true
            })),
        (status = 400, description = "The body is not a parseable RO-Crate JSON-LD document", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token belongs to another realm, or READ is denied on the named group's metadata", body = ErrorResponse),
        (status = 503, description = "The Profile or the evaluator is temporarily unavailable; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn preview_profile_validation(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Json(request): Json<ProfilePreviewRequest>,
) -> ServerResult<(StatusCode, Json<ProfilePreviewResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let group_id = request
        .group_id
        .as_deref()
        .map(parse_group_id)
        .transpose()?;
    // A group scope resolves that group's non-public Profiles, so it needs READ
    // on the group's metadata.
    if let Some(group_id) = group_id {
        ensure_metadata_scope(&state, &auth, group_id, Permission::READ).await?;
    }
    let jsonld = serialize_jsonld_object(&request.rocrate)?;
    let context = state.get_ctx();
    let preview = run_preview_submission(&context, group_id, &jsonld)
        .await
        .map_err(map_metadata_error)?;
    let mut response = ProfilePreviewResponse::from(preview);
    if request.public {
        response.set_restricted(
            run_restricted_files(
                &context,
                state.get_realm_id(),
                state.get_node_id(),
                &auth,
                &request.rocrate,
            )
            .await
            .map_err(map_metadata_error)?,
        );
    }
    Ok((StatusCode::OK, Json(response)))
}

#[utoipa::path(
    get,
    path = "/metadata/{document_id}/profile/validation",
    tag = "metadata/validation",
    summary = "Get revision-bound Profile validation status",
    description = r#"Returns the durable validation status bound to the accepted revision.

**Authentication**: optional bearer token; the same document READ rules as metadata retrieval
apply.

**Behavior**
- The status is written atomically with the accepted metadata revision.
- It becomes stale when either the Dataset revision or the exact registered Profile revision
  changes."#,
    params(("document_id" = String, Path, description = "Metadata document id")),
    responses(
        (status = 200, description = "Current, invalid, unprofiled, or stale revision-bound validation status", body = ProfileValidationResponse,
            example = json!({
                "document_id": "01JMETADATA0123456789ABCDE",
                "dataset_revision": "01JREVISION000000000000000",
                "state": "invalid",
                "profile_id": "01JPROFILE0000000000000000",
                "profile_iri": "https://w3id.org/aruna/profile/01JPROFILE0000000000000000",
                "profile_revision": "01JPROFILEREVISION00000000",
                "evaluator": "craqle-shacl-core/0.2",
                "validated_at_ms": 1787000000000_u64,
                "findings": [
                    {
                        "code": "constraint_violation",
                        "severity": "violation",
                        "focus_node": "./",
                        "path": "http://schema.org/identifier",
                        "rule": "http://www.w3.org/ns/shacl#minCount",
                        "message": "fewer values are present than the Profile requires",
                        "profile_revision": "01JPROFILEREVISION00000000",
                        "completeness": "complete"
                    }
                ],
                "completeness": "complete",
                "stale_reason": null
            })),
        (status = 400, description = "Document id is not a structured metadata id", body = ErrorResponse),
        (status = 401, description = "A holder rejected the forwarded credential", body = ErrorResponse),
        (status = 403, description = "Caller lacks READ on the document", body = ErrorResponse),
        (status = 404, description = "The document does not exist or is not readable", body = ErrorResponse),
        (status = 503, description = "No holder can supply the revision-bound status; retryable", body = ErrorResponse)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn get_validation_status(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(document_id): Path<String>,
) -> ServerResult<(StatusCode, Json<ProfileValidationResponse>)> {
    let document_id = parse_document_id(&document_id)?;
    let status = run_profile_validation_status(
        &state.get_ctx(),
        state.get_realm_id(),
        GetVisibleRequest { document_id, auth },
        forwarded_auth_token(bearer_token)?,
        false,
    )
    .await
    .map_err(map_api_error)?;
    Ok((StatusCode::OK, Json(status.into())))
}

#[utoipa::path(
    post,
    path = "/metadata/{document_id}/profile/validation/revalidate",
    tag = "metadata/validation",
    summary = "Revalidate a document against its current Profile",
    description = r#"Rebuilds the durable validation status from the current Profile revision.

**Authentication**: realm bearer token with READ on the document.

**Behavior**
- Revalidates the last accepted raw Dataset revision against the registered Profile's current
  exact revision.
- Fences the Dataset revision and durably replaces the stored status."#,
    params(("document_id" = String, Path, description = "Metadata document id")),
    responses(
        (status = 200, description = "Fresh valid, invalid, or unprofiled status", body = ProfileValidationResponse,
            example = json!({
                "document_id": "01JMETADATA0123456789ABCDE",
                "dataset_revision": "01JREVISION000000000000000",
                "state": "valid",
                "profile_id": "01JPROFILE0000000000000000",
                "profile_iri": "https://w3id.org/aruna/profile/01JPROFILE0000000000000000",
                "profile_revision": "01JPROFILEREVISION00000000",
                "evaluator": "craqle-shacl-core/0.2",
                "validated_at_ms": 1787000000000_u64,
                "findings": [],
                "completeness": "complete",
                "stale_reason": null
            })),
        (status = 400, description = "Document id or Profile tag is invalid", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller lacks READ on the document", body = ErrorResponse),
        (status = 404, description = "The document does not exist or is not readable", body = ErrorResponse),
        (status = 503, description = "The Profile, evaluator, Dataset revision or a holder is temporarily unavailable; retryable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn revalidate_profile(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedBearer>>,
    Path(document_id): Path<String>,
) -> ServerResult<(StatusCode, Json<ProfileValidationResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let document_id = parse_document_id(&document_id)?;
    let status = run_profile_validation_status(
        &state.get_ctx(),
        state.get_realm_id(),
        GetVisibleRequest {
            document_id,
            auth: Some(auth),
        },
        forwarded_auth_token(bearer_token)?,
        true,
    )
    .await
    .map_err(map_api_error)?;
    Ok((StatusCode::OK, Json(status.into())))
}
