//! Realm-admin surface over this node's sync-quarantine store (#338).
//!
//! Quarantine evidence is node-local: every node keeps the events its own
//! replication path rejected, so these routes are served by the node that holds
//! them rather than being restricted to the management node.

use std::sync::Arc;

use aruna_core::document::DocumentSyncEvent;
use aruna_core::structs::{
    AuthContext, Permission, SyncQuarantineCapacity, SyncQuarantineRecord, SyncQuarantineUsage,
};
use aruna_operations::sync_quarantine::{
    QuarantineAdminError, QuarantinePageRequest, acknowledge_quarantine_row,
    list_quarantine_records, prune_quarantine_records, read_quarantine_record,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_unrestricted_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;

#[derive(OpenApi)]
#[openapi()]
pub struct SyncQuarantineApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(SyncQuarantineApiDoc::openapi())
        .routes(routes!(list_quarantine, prune_quarantine))
        .routes(routes!(inspect_quarantine))
        .routes(routes!(acknowledge_quarantine))
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
pub struct QuarantineQuery {
    /// Opaque continuation token from a previous page.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Hex sync topic; restricts the page to that topic's evidence.
    #[serde(default)]
    pub topic: Option<String>,
    /// Page size (default 50, clamped to 1..=200).
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QuarantineUsageResponse {
    pub records: u64,
    pub bytes: u64,
    pub max_records: u64,
    pub max_bytes: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QuarantineRecordResponse {
    /// Opaque row id: hex of `topic || actor || actor_seq`.
    pub id: String,
    pub topic: String,
    pub actor: String,
    pub actor_seq: u64,
    /// Absent when the payload never decoded into an event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_node_id: Option<String>,
    pub reason: String,
    pub quarantined_at_ms: u64,
    pub acknowledged: bool,
    pub event_bytes: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QuarantinePageResponse {
    pub records: Vec<QuarantineRecordResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub usage: QuarantineUsageResponse,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QuarantineInspectResponse {
    pub record: QuarantineRecordResponse,
    /// Decoded envelope summary; absent when the retained bytes cannot be decoded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QuarantinePruneResponse {
    pub pruned: usize,
    pub scanned: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub usage: QuarantineUsageResponse,
}

/// Realm admins only: retained evidence carries whole replicated documents.
async fn authorize_quarantine_admin(
    state: &Arc<ServerState>,
    auth: Option<AuthContext>,
) -> ServerResult<AuthContext> {
    let auth = require_unrestricted_realm_auth(state, auth)?;
    ensure_permission(
        state,
        &auth,
        format!("/{}/admin/sync-quarantine", state.get_realm_id()),
        Permission::WRITE,
    )
    .await?;
    Ok(auth)
}

fn map_admin_error(error: QuarantineAdminError) -> ServerError {
    match error {
        QuarantineAdminError::Storage(error) => ServerError::InternalError(error.to_string()),
        QuarantineAdminError::Conversion(error) => ServerError::InternalError(error.to_string()),
        QuarantineAdminError::Unexpected { .. } => ServerError::ServiceUnavailable,
    }
}

fn page_request(query: &QuarantineQuery) -> ServerResult<QuarantinePageRequest> {
    let decode = |value: &str| hex::decode(value).map_err(|_| ServerError::BadRequest);
    Ok(QuarantinePageRequest {
        start_after: query.cursor.as_deref().map(decode).transpose()?,
        topic: query.topic.as_deref().map(decode).transpose()?,
        limit: query.limit,
    })
}

fn map_usage(usage: SyncQuarantineUsage) -> QuarantineUsageResponse {
    let capacity = SyncQuarantineCapacity::default();
    QuarantineUsageResponse {
        records: usage.records,
        bytes: usage.bytes,
        max_records: capacity.max_records,
        max_bytes: capacity.max_bytes,
    }
}

fn map_record(record: &SyncQuarantineRecord) -> QuarantineRecordResponse {
    QuarantineRecordResponse {
        id: hex::encode(record.storage_key()),
        topic: record.identity.topic.to_string(),
        actor: record.identity.actor.to_string(),
        actor_seq: record.identity.actor_seq,
        event_id: record.event_id().map(|event_id| event_id.to_string()),
        family: record.family().map(|family| family.as_str().to_string()),
        target: record.target().map(|target| format!("{target:?}")),
        origin_node_id: record.origin().map(|origin| origin.to_string()),
        reason: record.reason.clone(),
        quarantined_at_ms: record.quarantined_at_ms,
        acknowledged: record.acknowledged,
        event_bytes: record.evidence.bytes().len(),
    }
}

fn event_summary(event: &DocumentSyncEvent) -> String {
    format!(
        "{:?} target={:?} placement={:?}",
        event.event_id(),
        event.target(),
        event.placement()
    )
}

#[utoipa::path(
    get,
    path = "/data/sync/quarantine",
    tag = "data/sync",
    summary = "List rejected sync events held on this node",
    description = r#"Lists the replicated sync events this node refused, as evidence for a realm administrator.

**Authentication**: realm bearer token with WRITE on the realm's sync-quarantine admin path,
because a retained row carries the whole rejected document; a path-restricted token is refused.

**Behavior**
- The listing is node-local and never fanned out: each node keeps the events its own replication
  path rejected, so an operator asks the node that saw them.
- Rows are ordered by their transport identity, publisher then delivery order, not by rejection
  time.
- Retention is manual: a row stays until an operator acknowledges and prunes it.
- `usage` reports how much of the node's fixed quarantine budget is consumed, so an operator can
  see a node approaching the point where new rejections can no longer be retained.
- A page without `next_cursor` is the last one.

**Limits**
- `limit` defaults to 50 and is clamped to 1..=200."#,
    params(
        ("cursor" = Option<String>, Query, description = "Opaque continuation token from a previous page's `next_cursor`, hex encoded. Absent starts at the first row"),
        ("topic" = Option<String>, Query, description = "Hex-encoded sync topic, 64 characters; a shorter even-length hex string matches as a leading prefix. Absent lists every topic"),
        ("limit" = Option<usize>, Query, description = "Maximum rows in one page. Default 50, clamped to 1..=200")
    ),
    responses(
        (
            status = 200,
            description = "One page of retained evidence, with the node's quarantine usage against its fixed capacity",
            body = QuarantinePageResponse,
            example = json!({
                "records": [
                    {
                        "id": "a5d03b267c5480c60614edcfc08d83615fd0d6ce38048282ba9a85e8d9d61b64d8d15044cac756439413aaa60e3fa5cf7e7c500a6433f9512c1700b7f7fc0a950000000000000007",
                        "topic": "a5d03b267c5480c60614edcfc08d83615fd0d6ce38048282ba9a85e8d9d61b64",
                        "actor": "d8d15044cac756439413aaa60e3fa5cf7e7c500a6433f9512c1700b7f7fc0a95",
                        "actor_seq": 7,
                        "event_id": "01JMETADATA0123456789ABCDE",
                        "family": "delete",
                        "target": "RealmConfig { realm_id: RealmId(AAECAwQF...) }",
                        "origin_node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                        "reason": "unauthorized",
                        "quarantined_at_ms": 1775744591123_i64,
                        "acknowledged": false,
                        "event_bytes": 412
                    }
                ],
                "next_cursor": "a5d03b267c5480c60614edcfc08d83615fd0d6ce38048282ba9a85e8d9d61b64d8d15044cac756439413aaa60e3fa5cf7e7c500a6433f9512c1700b7f7fc0a950000000000000007",
                "usage": {
                    "records": 3,
                    "bytes": 2048,
                    "max_records": 4096,
                    "max_bytes": 67108864
                }
            })
        ),
        (status = 400, description = "The cursor or topic filter is not valid hex", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm or path-restricted, or no WRITE on the realm's sync-quarantine admin path", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_quarantine(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<QuarantineQuery>,
) -> ServerResult<(StatusCode, Json<QuarantinePageResponse>)> {
    authorize_quarantine_admin(&state, auth).await?;
    let page = list_quarantine_records(&state.get_ctx(), page_request(&query)?)
        .await
        .map_err(map_admin_error)?;

    Ok((
        StatusCode::OK,
        Json(QuarantinePageResponse {
            records: page.records.iter().map(map_record).collect(),
            next_cursor: page.next_start_after.map(hex::encode),
            usage: map_usage(page.usage),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/data/sync/quarantine/{record_id}",
    tag = "data/sync",
    summary = "Inspect one rejected sync event",
    description = r#"Reads one retained row and a summary of the envelope it carried.

**Authentication**: realm bearer token with WRITE on the realm's sync-quarantine admin path,
because the row carries the whole rejected document; a path-restricted token is refused.

**Behavior**
- Evidence is node-local, so only the node that rejected the event can answer for it.
- A payload that never decoded into a sync event is still kept, byte for byte, as evidence: such a
  row has no decoded `event` and no `event_id`, `family`, `target` or `origin_node_id`.
- Inspecting changes nothing: the event stays rejected and the row stays unacknowledged."#,
    params(("record_id" = String, Path, description = "Hex row id exactly as the listing reported it in `id`: the row's transport identity of topic, publisher and sequence")),
    responses(
        (
            status = 200,
            description = "The retained row and, when the payload decoded, a summary of the sync event it carried",
            body = QuarantineInspectResponse,
            example = json!({
                "record": {
                    "id": "a5d03b267c5480c60614edcfc08d83615fd0d6ce38048282ba9a85e8d9d61b64d8d15044cac756439413aaa60e3fa5cf7e7c500a6433f9512c1700b7f7fc0a950000000000000007",
                    "topic": "a5d03b267c5480c60614edcfc08d83615fd0d6ce38048282ba9a85e8d9d61b64",
                    "actor": "d8d15044cac756439413aaa60e3fa5cf7e7c500a6433f9512c1700b7f7fc0a95",
                    "actor_seq": 7,
                    "event_id": "01JMETADATA0123456789ABCDE",
                    "family": "delete",
                    "target": "RealmConfig { realm_id: RealmId(AAECAwQF...) }",
                    "origin_node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                    "reason": "unauthorized",
                    "quarantined_at_ms": 1775744591123_i64,
                    "acknowledged": false,
                    "event_bytes": 412
                },
                "event": "Ulid(2043894723004516761640472817206688257) target=RealmConfig { realm_id: RealmId(AAECAwQF...) } placement=PlacementRef { strategy_id: Ulid(0), epoch: 0, shard: 0 }"
            })
        ),
        (status = 400, description = "The row id is not valid hex", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm or path-restricted, or no WRITE on the realm's sync-quarantine admin path", body = ErrorResponse),
        (status = 404, description = "This node holds no evidence under that row id; it may have been pruned, or retained by a different node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn inspect_quarantine(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(record_id): Path<String>,
) -> ServerResult<(StatusCode, Json<QuarantineInspectResponse>)> {
    authorize_quarantine_admin(&state, auth).await?;
    let key = hex::decode(&record_id).map_err(|_| ServerError::BadRequest)?;
    let record = read_quarantine_record(&state.get_ctx(), &key)
        .await
        .map_err(map_admin_error)?
        .ok_or(ServerError::NotFound)?;

    Ok((
        StatusCode::OK,
        Json(QuarantineInspectResponse {
            record: map_record(&record),
            event: record.decoded_event().as_ref().map(event_summary),
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/data/sync/quarantine/{record_id}/acknowledge",
    tag = "data/sync",
    summary = "Acknowledge one piece of quarantine evidence",
    description = r#"Marks a retained row as seen by an operator, the only thing that makes it prunable.

**Authentication**: realm bearer token with WRITE on the realm's sync-quarantine admin path; a
path-restricted token is refused.

**Behavior**
- This is bookkeeping, not recovery: the rejected event is not replayed, re-applied or accepted,
  nothing is sent back to the publisher, and the document it carried stays exactly as it was.
- Evidence is node-local, so the acknowledgement applies to this node's copy only.
- Acknowledging an already acknowledged row rewrites nothing and answers with the same row."#,
    params(("record_id" = String, Path, description = "Hex row id exactly as the listing reported it in `id`: the row's transport identity of topic, publisher and sequence")),
    responses(
        (
            status = 200,
            description = "The row after acknowledgement; a prune pass may now delete it",
            body = QuarantineRecordResponse,
            example = json!({
                "id": "a5d03b267c5480c60614edcfc08d83615fd0d6ce38048282ba9a85e8d9d61b64d8d15044cac756439413aaa60e3fa5cf7e7c500a6433f9512c1700b7f7fc0a950000000000000007",
                "topic": "a5d03b267c5480c60614edcfc08d83615fd0d6ce38048282ba9a85e8d9d61b64",
                "actor": "d8d15044cac756439413aaa60e3fa5cf7e7c500a6433f9512c1700b7f7fc0a95",
                "actor_seq": 7,
                "event_id": "01JMETADATA0123456789ABCDE",
                "family": "delete",
                "target": "RealmConfig { realm_id: RealmId(AAECAwQF...) }",
                "origin_node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                "reason": "unauthorized",
                "quarantined_at_ms": 1775744591123_i64,
                "acknowledged": true,
                "event_bytes": 412
            })
        ),
        (status = 400, description = "The row id is not valid hex", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm or path-restricted, or no WRITE on the realm's sync-quarantine admin path", body = ErrorResponse),
        (status = 404, description = "This node holds no evidence under that row id; it may have been pruned, or retained by a different node", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn acknowledge_quarantine(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Path(record_id): Path<String>,
) -> ServerResult<(StatusCode, Json<QuarantineRecordResponse>)> {
    authorize_quarantine_admin(&state, auth).await?;
    let key = hex::decode(&record_id).map_err(|_| ServerError::BadRequest)?;
    let record = acknowledge_quarantine_row(&state.get_ctx(), &key)
        .await
        .map_err(map_admin_error)?
        .ok_or(ServerError::NotFound)?;

    Ok((StatusCode::OK, Json(map_record(&record))))
}

#[utoipa::path(
    delete,
    path = "/data/sync/quarantine",
    tag = "data/sync",
    summary = "Prune acknowledged quarantine evidence",
    description = r#"Runs one bounded sweep over this node's evidence and deletes the acknowledged rows it scanned.

**Authentication**: realm bearer token with WRITE on the realm's sync-quarantine admin path; a
path-restricted token is refused.

**Behavior**
- Only acknowledged rows are removed, so unreviewed evidence is never lost to a sweep, and deletion
  is permanent: the rejected events are gone from this node and removing them replays nothing.
- One call is a page, not the whole store: repeat it with the returned `next_cursor` until none
  comes back.
- Pruning is how a node reclaims its fixed quarantine budget, and a node whose budget is full stops
  accepting further replicated events on the affected topics until it has room again, so `usage` is
  the signal to keep sweeping.

**Limits**
- `limit` bounds the rows examined in one pass, not the rows deleted. Default 50, clamped to
  1..=200."#,
    params(
        ("cursor" = Option<String>, Query, description = "Opaque continuation token from a previous pass's `next_cursor`, hex encoded. Absent starts at the first row"),
        ("topic" = Option<String>, Query, description = "Hex-encoded sync topic, 64 characters; a shorter even-length hex string matches as a leading prefix. Absent sweeps every topic"),
        ("limit" = Option<usize>, Query, description = "Maximum rows examined in one pass, of which only the acknowledged ones are deleted. Default 50, clamped to 1..=200")
    ),
    responses(
        (
            status = 200,
            description = "Rows examined and deleted in this pass, the cursor to continue with, and the node's quarantine usage after the deletions",
            body = QuarantinePruneResponse,
            example = json!({
                "pruned": 1,
                "scanned": 50,
                "next_cursor": "a5d03b267c5480c60614edcfc08d83615fd0d6ce38048282ba9a85e8d9d61b64d8d15044cac756439413aaa60e3fa5cf7e7c500a6433f9512c1700b7f7fc0a950000000000000007",
                "usage": {
                    "records": 2,
                    "bytes": 1536,
                    "max_records": 4096,
                    "max_bytes": 67108864
                }
            })
        ),
        (status = 400, description = "The cursor or topic filter is not valid hex", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Token from another realm or path-restricted, or no WRITE on the realm's sync-quarantine admin path", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn prune_quarantine(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<QuarantineQuery>,
) -> ServerResult<(StatusCode, Json<QuarantinePruneResponse>)> {
    authorize_quarantine_admin(&state, auth).await?;
    let result = prune_quarantine_records(&state.get_ctx(), page_request(&query)?)
        .await
        .map_err(map_admin_error)?;

    Ok((
        StatusCode::OK,
        Json(QuarantinePruneResponse {
            pruned: result.pruned,
            scanned: result.scanned,
            next_cursor: result.next_start_after.map(hex::encode),
            usage: map_usage(result.usage),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_core::NodeId;
    use aruna_core::document::{
        DocumentSyncChange, DocumentSyncChangeKind, DocumentSyncRevision, DocumentSyncTarget,
    };
    use aruna_core::effects::StorageEffect;
    use aruna_core::structs::{
        Actor, NodeCapabilities, PlacementRef, RealmId, SyncQuarantineEvidence,
        SyncQuarantineIdentity, SyncQuarantineInput, SyncQuarantineUsage, build_quarantine_entries,
        quarantine_usage_entry,
    };
    use aruna_core::types::UserId;
    use aruna_operations::claim_initial_realm_admin::{
        ClaimInitialRealmAdminInput, ClaimInitialRealmAdminOperation,
    };
    use aruna_operations::create_realm::{CreateRealmConfig, CreateRealmOperation};
    use aruna_operations::driver::{DriverContext, drive};
    use aruna_storage::storage::FjallStorage;
    use aruna_tasks::TaskHandle;
    use ulid::Ulid;

    struct Fixture {
        _dir: tempfile::TempDir,
        state: Arc<ServerState>,
        admin: AuthContext,
        realm_id: RealmId,
    }

    async fn setup() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let storage = FjallStorage::open(dir.path().to_str().unwrap()).unwrap();
        let context = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        });
        let realm_id = RealmId::from_bytes(
            ed25519_dalek::SigningKey::from_bytes(&[31u8; 32])
                .verifying_key()
                .to_bytes(),
        );
        let node_id = iroh::SecretKey::from_bytes(&[6u8; 32]).public();
        let admin_id = UserId::local(Ulid::from_bytes([8u8; 16]), realm_id);
        let actor = Actor {
            node_id,
            user_id: admin_id,
            realm_id,
        };
        drive(
            CreateRealmOperation::new(CreateRealmConfig {
                actor: actor.clone(),
                realm_description: "quarantine".to_string(),
                oidc_providers: Vec::new(),
                node_location: None,
                node_weight: None,
                node_labels: Default::default(),
            }),
            context.as_ref(),
        )
        .await
        .unwrap();
        drive(
            ClaimInitialRealmAdminOperation::new(ClaimInitialRealmAdminInput { actor }),
            context.as_ref(),
        )
        .await
        .unwrap();
        let state = Arc::new(
            ServerState::new(
                context,
                realm_id,
                node_id,
                NodeCapabilities::user_node(realm_id).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );
        Fixture {
            _dir: dir,
            state,
            admin: AuthContext {
                user_id: admin_id,
                realm_id,
                path_restrictions: None,
                session: None,
            },
            realm_id,
        }
    }

    fn event(index: u8) -> DocumentSyncEvent {
        DocumentSyncEvent::Delete {
            event_id: Ulid::from_bytes([index; 16]),
            target: DocumentSyncTarget::RealmConfig {
                realm_id: RealmId([9; 32]),
            },
            change: DocumentSyncChange {
                base: None,
                current: DocumentSyncRevision {
                    generation: 1,
                    event_id: Ulid::from_bytes([index; 16]),
                    actor: NodeId::from_bytes(&[1u8; 32]).unwrap(),
                    updated_at_ms: 1,
                },
                kind: DocumentSyncChangeKind::Delete,
                placement: PlacementRef::NIL,
            },
        }
    }

    fn identity(index: u8) -> SyncQuarantineIdentity {
        SyncQuarantineIdentity::from_parts([7; 32], [8; 32], u64::from(index) + 1)
    }

    async fn seed_rows(fx: &Fixture, count: u8) {
        let ctx = fx.state.get_ctx();
        let mut usage = SyncQuarantineUsage::default();
        for index in 0..count {
            let write = build_quarantine_entries(
                SyncQuarantineInput {
                    identity: identity(index),
                    evidence: SyncQuarantineEvidence::from_event(&event(index)),
                    reason: "unauthorized",
                    quarantined_at_ms: 42,
                    replaced_bytes: None,
                },
                usage,
                SyncQuarantineCapacity::default(),
            )
            .unwrap();
            usage = write.usage;
            ctx.storage_handle
                .send_storage_effect(StorageEffect::BatchWrite {
                    writes: vec![write.row, quarantine_usage_entry(usage).unwrap()],
                    txn_id: None,
                })
                .await;
        }
    }

    #[tokio::test]
    async fn admin_lists_records() {
        let fx = setup().await;
        seed_rows(&fx, 3).await;

        let (_, Json(page)) = list_quarantine(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Query(QuarantineQuery {
                cursor: None,
                topic: None,
                limit: Some(2),
            }),
        )
        .await
        .unwrap();
        assert_eq!(page.records.len(), 2);
        assert_eq!(page.usage.records, 3);
        assert!(page.next_cursor.is_some());
        assert_eq!(page.records[0].family.as_deref(), Some("delete"));

        let (_, Json(inspected)) = inspect_quarantine(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Path(page.records[0].id.clone()),
        )
        .await
        .unwrap();
        assert_eq!(inspected.record.id, page.records[0].id);
        assert!(inspected.event.is_some());
    }

    #[tokio::test]
    async fn admin_acknowledges_row() {
        let fx = setup().await;
        seed_rows(&fx, 2).await;
        let (_, Json(page)) = list_quarantine(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Query(QuarantineQuery::default()),
        )
        .await
        .unwrap();
        let id = page.records[0].id.clone();

        for _ in 0..2 {
            let (_, Json(record)) = acknowledge_quarantine(
                State(fx.state.clone()),
                Extension(Some(fx.admin.clone())),
                Path(id.clone()),
            )
            .await
            .unwrap();
            assert!(record.acknowledged);
        }

        let (_, Json(pruned)) = prune_quarantine(
            State(fx.state.clone()),
            Extension(Some(fx.admin.clone())),
            Query(QuarantineQuery::default()),
        )
        .await
        .unwrap();
        assert_eq!(pruned.pruned, 1);
        assert_eq!(pruned.usage.records, 1);
        assert_eq!(
            pruned.usage.max_records,
            SyncQuarantineCapacity::default().max_records
        );
    }

    #[tokio::test]
    async fn anonymous_is_rejected() {
        let fx = setup().await;
        for result in [
            list_quarantine(
                State(fx.state.clone()),
                Extension(None),
                Query(QuarantineQuery::default()),
            )
            .await
            .err(),
            prune_quarantine(
                State(fx.state.clone()),
                Extension(None),
                Query(QuarantineQuery::default()),
            )
            .await
            .err(),
        ] {
            assert!(matches!(result, Some(ServerError::Unauthorized)));
        }
        let inspected = inspect_quarantine(
            State(fx.state.clone()),
            Extension(None),
            Path("00".to_string()),
        )
        .await;
        assert!(matches!(inspected, Err(ServerError::Unauthorized)));
    }

    #[tokio::test]
    async fn stranger_is_rejected() {
        let fx = setup().await;
        seed_rows(&fx, 1).await;
        let stranger = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([77; 16]), fx.realm_id),
            realm_id: fx.realm_id,
            path_restrictions: None,
            session: None,
        };
        let listed = list_quarantine(
            State(fx.state.clone()),
            Extension(Some(stranger.clone())),
            Query(QuarantineQuery::default()),
        )
        .await;
        assert!(matches!(listed, Err(ServerError::Forbidden)));

        let acknowledged = acknowledge_quarantine(
            State(fx.state.clone()),
            Extension(Some(stranger)),
            Path("00".to_string()),
        )
        .await;
        assert!(matches!(acknowledged, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn restricted_token_rejected() {
        let fx = setup().await;
        let delegated = AuthContext {
            user_id: fx.admin.user_id,
            realm_id: fx.realm_id,
            path_restrictions: Some(Vec::new()),
            session: None,
        };
        let listed = list_quarantine(
            State(fx.state.clone()),
            Extension(Some(delegated)),
            Query(QuarantineQuery::default()),
        )
        .await;
        assert!(matches!(listed, Err(ServerError::Forbidden)));
    }
}
