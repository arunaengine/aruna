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
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::{OpenApi, ToSchema};

use crate::auth::{ensure_permission, require_unrestricted_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server_state::ServerState;

#[derive(OpenApi)]
#[openapi(
    tags((name = "sync-quarantine", description = "Rejected replicated sync events")),
    paths(
        list_quarantine,
        prune_quarantine,
        inspect_quarantine,
        acknowledge_quarantine
    )
)]
pub struct SyncQuarantineApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new()
        .route(
            "/admin/sync-quarantine",
            get(list_quarantine).delete(prune_quarantine),
        )
        .route(
            "/admin/sync-quarantine/{record_id}",
            get(inspect_quarantine),
        )
        .route(
            "/admin/sync-quarantine/{record_id}/acknowledge",
            post(acknowledge_quarantine),
        )
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
pub struct QuarantineQuery {
    /// Opaque continuation token from a previous page.
    #[serde(default)]
    pub cursor: Option<String>,
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
    /// Opaque row id: hex of `topic || event_id`.
    pub id: String,
    pub topic: String,
    pub event_id: String,
    pub family: String,
    pub target: String,
    pub origin_node_id: String,
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
    let start_after = query
        .cursor
        .as_deref()
        .map(|cursor| hex::decode(cursor).map_err(|_| ServerError::BadRequest))
        .transpose()?;
    Ok(QuarantinePageRequest {
        start_after,
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
        topic: hex::encode(&record.topic),
        event_id: record.event_id.to_string(),
        family: record.family.as_str().to_string(),
        target: format!("{:?}", record.target),
        origin_node_id: record.origin_node_id.to_string(),
        reason: record.reason.clone(),
        quarantined_at_ms: record.quarantined_at_ms,
        acknowledged: record.acknowledged,
        event_bytes: record.event_bytes.len(),
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
    path = "/admin/sync-quarantine",
    tag = "sync-quarantine",
    params(
        ("cursor" = Option<String>, Query, description = "Continuation token"),
        ("limit" = Option<usize>, Query, description = "Page size (max 200)")
    ),
    responses(
        (status = 200, description = "Quarantined events", body = QuarantinePageResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
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
    path = "/admin/sync-quarantine/{record_id}",
    tag = "sync-quarantine",
    params(("record_id" = String, Path, description = "Row id from the listing")),
    responses(
        (status = 200, description = "Quarantined event", body = QuarantineInspectResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Unknown row", body = ErrorResponse)
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
            event: record.decode_event().ok().as_ref().map(event_summary),
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/admin/sync-quarantine/{record_id}/acknowledge",
    tag = "sync-quarantine",
    params(("record_id" = String, Path, description = "Row id from the listing")),
    responses(
        (status = 200, description = "Acknowledged row", body = QuarantineRecordResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse),
        (status = 404, description = "Unknown row", body = ErrorResponse)
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
    path = "/admin/sync-quarantine",
    tag = "sync-quarantine",
    params(
        ("cursor" = Option<String>, Query, description = "Continuation token"),
        ("limit" = Option<usize>, Query, description = "Rows scanned this pass (max 200)")
    ),
    responses(
        (status = 200, description = "Prune pass result", body = QuarantinePruneResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
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
        Actor, NodeCapabilities, PlacementRef, RealmId, SyncQuarantineInput, SyncQuarantineUsage,
        build_quarantine_entries,
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
                NodeCapabilities::local_node(realm_id).unwrap(),
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

    async fn seed_rows(fx: &Fixture, count: u8) {
        let ctx = fx.state.get_ctx();
        let mut usage = SyncQuarantineUsage::default();
        for index in 0..count {
            let event = event(index);
            let write = build_quarantine_entries(
                SyncQuarantineInput {
                    topic: &[7u8; 32],
                    event: &event,
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
                    writes: write.entries,
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
                limit: Some(2),
            }),
        )
        .await
        .unwrap();
        assert_eq!(page.records.len(), 2);
        assert_eq!(page.usage.records, 3);
        assert!(page.next_cursor.is_some());
        assert_eq!(page.records[0].family, "delete");

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
