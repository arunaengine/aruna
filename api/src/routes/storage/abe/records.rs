//! Exposes recipient-only grants and current-holder issuance proposals.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::storage::abe_access::{
    GrantContext, KeyGrant, KeyIssuer, KeyRequest, KeyScope,
};
use aruna_operations::abe::{KeyAction, KeyOperation, KeyResult, MemberKeysOperation};
use axum::extract::Path;

#[derive(Deserialize, ToSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ScopeView {
    Exact(String),
    Subtree(String),
}
#[derive(Deserialize, ToSchema)]
pub struct RequestBody {
    pub scope: ScopeView,
}
#[derive(Deserialize, ToSchema)]
pub struct GrantBody {
    pub context: String,
    pub enc: String,
    pub ciphertext: String,
}
#[derive(Serialize, ToSchema)]
pub struct RecordView {
    pub fields: Value,
    pub record: String,
    pub aad: Option<String>,
}
#[derive(Serialize, ToSchema)]
pub struct RecordList {
    pub next_cursor: Option<String>,
    pub records: Vec<RecordView>,
}
#[derive(Deserialize)]
pub struct PageQuery {
    pub cursor: Option<String>,
}
impl PageQuery {
    fn decode(self) -> ServerResult<Option<Vec<u8>>> {
        self.cursor
            .map(|v| match STANDARD.decode(v) {
                Ok(bytes) if bytes.len() == 80 => Ok(bytes),
                _ => Err(ServerError::BadRequest),
            })
            .transpose()
    }
}
pub(super) fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(request_key, open_requests))
        .routes(routes!(publish_grant))
        .routes(routes!(own_grants))
}
fn request_fields(r: &KeyRequest) -> ServerResult<Value> {
    let scope = match &r.scope {
        KeyScope::Exact(value) => json!({"kind":"exact","value":value}),
        KeyScope::Subtree(value) => json!({"kind":"subtree","value":value}),
    };
    let epoch = r.epochs.first().copied().unwrap_or_default();
    Ok(
        json!({"request_id":r.request_id.to_string(),"requesting_user":r.requesting_user.to_string(),
        "recipient_user":r.recipient_user.to_string(),"recipient_record":r.recipient_record.map(|v|v.to_string()),
        "recipient_public":r.recipient_public.map(|v|STANDARD.encode(v)),
        "recipient_fingerprint":r.recipient_fingerprint.map(|v|STANDARD.encode(v)),
        "bucket":r.bucket,"parameters":parameter_view(&r.parameters, epoch)?,"scope":scope,
        "epochs":r.epochs,"credential_id":r.credential_id,"restrictions":r.restrictions,
        "revisions":r.revisions.iter().map(|v|STANDARD.encode(v)).collect::<Vec<_>>(),
        "created_at_ms":r.created_at_ms}),
    )
}
fn issuer_view(issuer: &KeyIssuer) -> Value {
    match issuer {
        KeyIssuer::User(user) => json!({"kind":"user","id":user.to_string()}),
        KeyIssuer::Node(node) => json!({"kind":"node","id":node.to_string()}),
    }
}
fn proposal_view(request: KeyRequest, issuer: KeyIssuer) -> ServerResult<RecordView> {
    let mut fields = request_fields(&request)?;
    fields["issuer"] = issuer_view(&issuer);
    let context = GrantContext { request, issuer };
    let record = postcard::to_allocvec(&context).map_err(|_| ServerError::BadRequest)?;
    Ok(RecordView {
        fields,
        record: STANDARD.encode(record),
        aad: Some(STANDARD.encode(context.bytes().map_err(abe_error)?)),
    })
}
fn grant_view(grant: KeyGrant) -> ServerResult<RecordView> {
    let fields = json!({"request":request_fields(&grant.context.request)?,
        "issuer":issuer_view(&grant.context.issuer),
        "enc":STANDARD.encode(grant.enc),"ciphertext":STANDARD.encode(&grant.ciphertext)});
    Ok(RecordView {
        fields,
        record: STANDARD.encode(grant.to_bytes().map_err(abe_error)?),
        aad: Some(STANDARD.encode(grant.context.bytes().map_err(abe_error)?)),
    })
}
async fn execute(
    state: &ServerState,
    auth: AuthContext,
    bucket: String,
    action: KeyAction,
) -> ServerResult<KeyResult> {
    let now = aruna_core::time::unix_timestamp_millis();
    let operation = KeyOperation::new(bucket, auth, state.get_node_id(), action, now);
    drive(operation, &state.get_ctx()).await.map_err(key_error)
}
/// Opens key requests for members in the group's encrypted buckets and returns open ids.
pub(crate) async fn member_requests(
    state: &ServerState,
    auth: &AuthContext,
    group_id: Ulid,
    members: Vec<aruna_core::UserId>,
) -> Vec<String> {
    let now = aruna_core::time::unix_timestamp_millis();
    let node = state.get_node_id();
    let operation = MemberKeysOperation::new(auth.clone(), node, group_id, members, now);
    match drive(operation, &state.get_ctx()).await {
        Ok(ids) => ids.iter().map(Ulid::to_string).collect(),
        Err(error) => {
            tracing::warn!(event = "abe.member_requests.failed", error = %error);
            Vec::new()
        }
    }
}
fn unexpected() -> ServerError {
    ServerError::InternalError("unexpected encryption result".into())
}

#[utoipa::path(post, path = "/data/buckets/{bucket}/abe/requests", tag = "data/blobs",
    summary = "Request a scoped key",
    description = r#"Creates or repeats the caller's scoped key request.

**Authentication**: A realm bearer token; the scope must be covered by current READ authority.

**Behavior**
- An unlocked or node-managed bucket issues the grant at once (`200`).
- Otherwise the deduplicated open request is returned (`202`); repeating it returns its state.
- Repeating an issued request returns its current grant (`200`).
- A recipient holds at most 64 current grants per bucket; a new grant past that returns `413`.
- At most 64 open requests per recipient and bucket; a new one past that returns `413`.
- Binary fields use padded base64; request ids use ULIDs."#, params(("bucket" = String, Path, description = "Node-local S3 bucket name")),
    request_body(content = RequestBody, example = json!({"scope":{"kind":"subtree","value":"foo/"}})),
    responses((status = 200, body = RecordView, description = "Node-issued grant", example = json!({"fields":{},"record":"AA==","aad":"AA=="})),
        (status = 202, body = RecordView, description = "Open request, waiting for an issuer or vault key", example = json!({"fields":{},"record":"AA==","aad":null})),
        (status = 400, body = ErrorResponse, description = "Invalid scope"), (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 403, body = ErrorResponse, description = "READ refused"), (status = 404, body = ErrorResponse, description = "Bucket missing"),
        (status = 409, body = ErrorResponse, description = "Stale request"), (status = 413, body = ErrorResponse, description = "Request limit"),
        (status = 422, body = ErrorResponse, description = "Scope or CEL policy cannot be compiled")), security(("bearer_auth" = [])))]
pub async fn request_key(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Json(body): Json<RequestBody>,
) -> ServerResult<(StatusCode, Json<RecordView>)> {
    let auth = crate::auth::require_realm_auth(&state, auth)?;
    let scope = match body.scope {
        ScopeView::Exact(v) => KeyScope::Exact(v),
        ScopeView::Subtree(v) => KeyScope::Subtree(v),
    };
    scope.validate().map_err(|_| ServerError::BadRequest)?;
    match execute(&state, auth, bucket, KeyAction::Request(scope)).await? {
        KeyResult::Grant(g) => Ok((StatusCode::OK, Json(grant_view(g)?))),
        KeyResult::Request(r) => {
            let view = RecordView {
                fields: request_fields(&r)?,
                record: STANDARD.encode(r.to_bytes().map_err(abe_error)?),
                aad: None,
            };
            Ok((StatusCode::ACCEPTED, Json(view)))
        }
        _ => Err(unexpected()),
    }
}

#[utoipa::path(get, path = "/data/buckets/{bucket}/abe/requests", tag = "data/blobs",
    summary = "List open key requests",
    description = r#"Lists open requests a current bucket key holder can issue.

**Authentication**: An unrestricted realm bearer token of a current key holder.

**Behavior**
- Each `record` is the grant context to echo in the grant submission; `aad` is its associated data.
- At most 64 records per page; pass `next_cursor` as `cursor` for the next page."#,
    params(("bucket" = String, Path, description = "Node-local S3 bucket name"),("cursor" = Option<String>, Query, description = "Opaque next_cursor from the previous page")),
    responses((status = 200, body = RecordList, description = "Bounded open requests", example = json!({"records":[],"next_cursor":null})),
        (status = 401, body = ErrorResponse, description = "Bearer token required"), (status = 403, body = ErrorResponse, description = "Caller is no current key holder"),
        (status = 404, body = ErrorResponse, description = "Bucket missing")), security(("bearer_auth" = [])))]
pub async fn open_requests(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(page): Query<PageQuery>,
) -> ServerResult<Json<RecordList>> {
    let auth = crate::auth::require_unrestricted_auth(&state, auth)?;
    let issuer = KeyIssuer::User(auth.user_id);
    let KeyResult::Requests(requests, next) =
        execute(&state, auth, bucket, KeyAction::Open(page.decode()?)).await?
    else {
        return Err(unexpected());
    };
    let records = requests
        .into_iter()
        .map(|r| proposal_view(r, issuer.clone()))
        .collect::<ServerResult<_>>()?;
    Ok(Json(RecordList {
        records,
        next_cursor: next.map(|v| STANDARD.encode(v)),
    }))
}

#[utoipa::path(post, path = "/data/buckets/{bucket}/abe/requests/{request_id}/grant", tag = "data/blobs",
    summary = "Submit a scoped key grant",
    description = r#"Admits a recipient-sealed key from a current bucket key holder.

**Authentication**: An unrestricted realm bearer token of a current key holder.

**Behavior**
- `context` echoes the listed `record`; issuer, recipient, scope, parameters and epoch are rechecked.
- Repeating an admitted submission returns the stored grant.
- A recipient holds at most 64 current grants per bucket; a new grant past that returns `413`."#,
    params(("bucket" = String, Path, description = "Node-local S3 bucket name"),("request_id" = String, Path, description = "Key request ULID")),
    request_body(content = GrantBody, example = json!({"context":"AA==","enc":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","ciphertext":"AAAAAAAAAAAAAAAAAAAAAA=="})),
    responses((status = 200, body = RecordView, description = "Admitted grant", example = json!({"fields":{},"record":"AA==","aad":"AA=="})),
        (status = 400, body = ErrorResponse, description = "Invalid encoding"), (status = 401, body = ErrorResponse, description = "Bearer token required"),
        (status = 403, body = ErrorResponse, description = "Caller is no current issuer"), (status = 404, body = ErrorResponse, description = "Request missing"),
        (status = 409, body = ErrorResponse, description = "Stale binding"), (status = 413, body = ErrorResponse, description = "Grant too large or grant limit"),
        (status = 422, body = ErrorResponse, description = "Scope or CEL policy cannot be compiled")), security(("bearer_auth" = [])))]
pub async fn publish_grant(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path((bucket, request)): Path<(String, String)>,
    Json(body): Json<GrantBody>,
) -> ServerResult<Json<RecordView>> {
    let auth = crate::auth::require_unrestricted_auth(&state, auth)?;
    if body.context.len() > 128 * 1024 || body.ciphertext.len() > 128 * 1024 {
        return Err(ServerError::PayloadTooLarge("grant limit".into()));
    }
    let decode = |v: &str| STANDARD.decode(v).map_err(|_| ServerError::BadRequest);
    let context: GrantContext =
        postcard::from_bytes(&decode(&body.context)?).map_err(|_| ServerError::BadRequest)?;
    if context.request.request_id.to_string() != request || context.request.bucket != bucket {
        return Err(abe_error(AbeError::Stale));
    }
    let grant = KeyGrant {
        context,
        enc: decode(&body.enc)?
            .try_into()
            .map_err(|_| ServerError::BadRequest)?,
        ciphertext: decode(&body.ciphertext)?,
    };
    grant.to_bytes().map_err(abe_error)?;
    match execute(&state, auth, bucket, KeyAction::Publish(grant)).await? {
        KeyResult::Grant(g) => Ok(Json(grant_view(g)?)),
        _ => Err(unexpected()),
    }
}

#[utoipa::path(get, path = "/data/buckets/{bucket}/abe/grants", tag = "data/blobs",
    summary = "List own scoped grants",
    description = r#"Returns a bounded page of the caller's admitted sealed keys.

**Authentication**: An unrestricted realm bearer token; only the recipient's grants are returned.

**Behavior**
- Grants for a replaced recipient key or lost READ scope are deleted, not returned.
- At most 64 records per page; pass `next_cursor` as `cursor` for the next page."#,
    params(("bucket" = String, Path, description = "Node-local S3 bucket name"),("cursor" = Option<String>, Query, description = "Opaque next_cursor from the previous page")),
    responses((status = 200, body = RecordList, description = "Own admitted grants", example = json!({"records":[],"next_cursor":null})),
        (status = 401, body = ErrorResponse, description = "Bearer token required"), (status = 403, body = ErrorResponse, description = "Restricted token"),
        (status = 404, body = ErrorResponse, description = "Bucket missing")), security(("bearer_auth" = [])))]
pub async fn own_grants(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(bucket): Path<String>,
    Query(page): Query<PageQuery>,
) -> ServerResult<Json<RecordList>> {
    let auth = crate::auth::require_unrestricted_auth(&state, auth)?;
    let KeyResult::Grants(grants, next) =
        execute(&state, auth, bucket, KeyAction::Grants(page.decode()?)).await?
    else {
        return Err(unexpected());
    };
    let records = grants
        .into_iter()
        .map(grant_view)
        .collect::<ServerResult<_>>()?;
    Ok(Json(RecordList {
        records,
        next_cursor: next.map(|v| STANDARD.encode(v)),
    }))
}
