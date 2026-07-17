use crate::auth::{ValidatedArunaBearerTokenCarrier, parse_group_id, require_realm_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::routes::metadata::{
    MetadataQueryMode, MetadataSearchHitResponse, map_metadata_api_error, map_query_mode,
    map_search_hit,
};
use crate::routes::users::MIN_SEARCH_QUERY_CHARS;
use crate::server_state::ServerState;
use aruna_core::UserId;
use aruna_core::structs::AuthContext;
use aruna_operations::driver::drive;
use aruna_operations::metadata::api::{
    MetadataSearchRequest, search_metadata as run_search_metadata,
};
use aruna_operations::search_groups::{SearchGroupsInput, SearchGroupsOperation};
use aruna_operations::search_users::{SearchUsersInput, SearchUsersOperation};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Json, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;
use utoipa::{OpenApi, ToSchema};

const DEFAULT_SEARCH_LIMIT: usize = 10;
const MAX_SEARCH_LIMIT: usize = 100;
const SEARCH_TYPE_DOCUMENTS: &str = "documents";
const SEARCH_TYPE_GROUPS: &str = "groups";
const SEARCH_TYPE_USERS: &str = "users";

#[derive(OpenApi)]
#[openapi(
    tags((name = "search", description = "Unified realm search")),
    paths(unified_search)
)]
pub struct SearchApiDoc;

pub fn router() -> Router<Arc<ServerState>> {
    Router::new().route("/search", get(unified_search))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct SearchParams {
    #[serde(default)]
    pub q: String,
    /// Comma-separated subset of documents,groups,users. Defaults to all three.
    #[serde(default)]
    pub types: Option<String>,
    /// Per-section page size (default 10, clamped to 1..=100).
    #[serde(default)]
    pub limit: Option<usize>,
    /// Opaque continuation token. Only accepted when exactly one type is
    /// requested; a multi-type request with a cursor is rejected with 400.
    #[serde(default)]
    pub cursor: Option<String>,
    /// Documents-only: restrict metadata hits to a single group id.
    #[serde(default)]
    pub group_id: Option<String>,
    /// Documents-only: exact schema.org conformsTo profile IRI.
    #[serde(default)]
    pub conforms_to: Option<String>,
    /// Documents-only: search mode (local or distributed).
    #[serde(default)]
    pub mode: Option<MetadataQueryMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SearchResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub documents: Option<DocumentsSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<GroupsSection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub users: Option<UsersSection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DocumentsSection {
    pub hits: Vec<MetadataSearchHitResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub nodes_queried: usize,
    pub nodes_failed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupsSection {
    pub hits: Vec<GroupHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GroupHit {
    pub group_id: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UsersSection {
    pub hits: Vec<UserHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UserHit {
    pub user_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SearchTypes {
    documents: bool,
    groups: bool,
    users: bool,
}

impl SearchTypes {
    fn all() -> Self {
        Self {
            documents: true,
            groups: true,
            users: true,
        }
    }

    fn count(&self) -> usize {
        self.documents as usize + self.groups as usize + self.users as usize
    }
}

fn parse_search_types(types: Option<&str>) -> ServerResult<SearchTypes> {
    let Some(types) = types else {
        return Ok(SearchTypes::all());
    };
    let mut selected = SearchTypes {
        documents: false,
        groups: false,
        users: false,
    };
    let mut any = false;
    for value in types.split(',').map(str::trim) {
        if value.is_empty() {
            continue;
        }
        match value {
            SEARCH_TYPE_DOCUMENTS => selected.documents = true,
            SEARCH_TYPE_GROUPS => selected.groups = true,
            SEARCH_TYPE_USERS => selected.users = true,
            _ => return Err(ServerError::BadRequest),
        }
        any = true;
    }
    if any {
        Ok(selected)
    } else {
        Ok(SearchTypes::all())
    }
}

#[utoipa::path(
    get,
    path = "/search",
    tag = "search",
    params(
        ("q" = String, Query, description = "Search query; trimmed, minimum 2 characters"),
        ("types" = Option<String>, Query, description = "Comma-separated subset of documents,groups,users. Defaults to all three; an unknown type returns 400"),
        ("limit" = Option<usize>, Query, description = "Per-section page size (default 10, clamped to 1..=100)"),
        ("cursor" = Option<String>, Query, description = "Opaque continuation token. Only accepted when exactly one type is requested"),
        ("group_id" = Option<String>, Query, description = "Documents-only: restrict metadata hits to a single group id"),
        ("conforms_to" = Option<String>, Query, description = "Documents-only: exact schema.org conformsTo profile IRI"),
        ("mode" = Option<MetadataQueryMode>, Query, description = "Documents-only: search mode local or distributed")
    ),
    responses(
        (status = 200, description = "Sectioned search results", body = SearchResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 403, description = "Forbidden", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn unified_search(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Extension(bearer_token): Extension<Option<ValidatedArunaBearerTokenCarrier>>,
    Query(params): Query<SearchParams>,
) -> ServerResult<(StatusCode, Json<SearchResponse>)> {
    let auth = require_realm_auth(&state, auth)?;
    let types = parse_search_types(params.types.as_deref())?;
    if let Some(cursor) = params.cursor.as_deref() {
        if types.count() != 1 {
            return Err(ServerError::BadRequest);
        }
        // Validate the selected section's cursor shape up front so a malformed
        // group or user id returns 400 rather than a downstream 500.
        if types.groups {
            parse_group_id(cursor)?;
        } else if types.users {
            UserId::from_string(cursor).map_err(|_| ServerError::BadRequest)?;
        }
    }
    let q = params.q.trim().to_string();
    if q.chars().count() < MIN_SEARCH_QUERY_CHARS {
        return Err(ServerError::BadRequest);
    }
    let limit = params
        .limit
        .unwrap_or(DEFAULT_SEARCH_LIMIT)
        .clamp(1, MAX_SEARCH_LIMIT);
    let group_id = params.group_id.as_deref().map(parse_group_id).transpose()?;
    let bearer = bearer_token.map(|carrier| carrier.as_str().to_string());

    let (documents, groups, users) = tokio::join!(
        run_documents(
            &state,
            &auth,
            types.documents,
            &q,
            bearer,
            params.conforms_to.clone(),
            group_id,
            limit,
            params.cursor.clone(),
            params.mode.clone(),
        ),
        run_groups(&state, types.groups, &q, limit, params.cursor.clone()),
        run_users(&state, types.users, &q, limit, params.cursor.clone()),
    );

    Ok((
        StatusCode::OK,
        Json(SearchResponse {
            documents: documents?,
            groups: groups?,
            users: users?,
        }),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_documents(
    state: &ServerState,
    auth: &AuthContext,
    requested: bool,
    query: &str,
    bearer_token: Option<String>,
    conforms_to: Option<String>,
    group_id: Option<Ulid>,
    limit: usize,
    cursor: Option<String>,
    mode: Option<MetadataQueryMode>,
) -> ServerResult<Option<DocumentsSection>> {
    if !requested {
        return Ok(None);
    }
    let ctx = state.get_ctx();
    let result = run_search_metadata(
        ctx.as_ref(),
        state.get_realm_id(),
        state.get_node_id(),
        MetadataSearchRequest {
            auth: Some(auth.clone()),
            bearer_token,
            graph_iris: None,
            query: query.to_string(),
            conforms_to,
            group_id,
            limit: Some(limit),
            cursor,
            mode: map_query_mode(mode),
            target_nodes: None,
        },
    )
    .await
    .map_err(map_metadata_api_error)?;
    Ok(Some(DocumentsSection {
        hits: result.hits.into_iter().map(map_search_hit).collect(),
        next_cursor: result.next_cursor,
        nodes_queried: result.fanout_stats.nodes_queried,
        nodes_failed: result.fanout_stats.nodes_failed,
    }))
}

async fn run_groups(
    state: &ServerState,
    requested: bool,
    query: &str,
    limit: usize,
    cursor: Option<String>,
) -> ServerResult<Option<GroupsSection>> {
    if !requested {
        return Ok(None);
    }
    let output = drive(
        SearchGroupsOperation::new(SearchGroupsInput {
            query: query.to_string(),
            limit,
            start_after: cursor,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok(Some(GroupsSection {
        hits: output
            .groups
            .into_iter()
            .map(|group| GroupHit {
                group_id: group.group_id.to_string(),
                display_name: group.display_name,
            })
            .collect(),
        next_cursor: output.next_start_after,
    }))
}

async fn run_users(
    state: &ServerState,
    requested: bool,
    query: &str,
    limit: usize,
    cursor: Option<String>,
) -> ServerResult<Option<UsersSection>> {
    if !requested {
        return Ok(None);
    }
    let output = drive(
        SearchUsersOperation::new(SearchUsersInput {
            realm_id: state.get_realm_id(),
            query: query.to_string(),
            limit,
            start_after: cursor,
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(|err| ServerError::InternalError(err.to_string()))?;
    Ok(Some(UsersSection {
        hits: output
            .users
            .into_iter()
            .map(|user| UserHit {
                user_id: user.user_id.to_string(),
                name: user.name,
            })
            .collect(),
        next_cursor: output.next_start_after,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ServerError;
    use crate::routes::metadata::{
        CreateMetadataRequest, CreateMetadataScaffoldRequest, MetadataQueryMode,
        create_metadata_document,
    };
    use aruna_core::UserId;
    use aruna_core::effects::{Effect, StorageEffect};
    use aruna_core::events::{Event, StorageEvent};
    use aruna_core::handle::Handle;
    use aruna_core::keyspaces::{AUTH_KEYSPACE, GROUP_KEYSPACE, USER_KEYSPACE};
    use aruna_core::structs::{
        Actor, Group, GroupAuthorizationDocument, NodeCapabilities, RealmAuthorizationDocument,
        RealmId, User,
    };
    use aruna_operations::driver::DriverContext;
    use aruna_operations::metadata::MetadataHandle;
    use aruna_operations::metadata::materialization_queue::process_metadata_materialization_batch;
    use aruna_operations::metadata::projector::{
        drain_pending_metadata_projection_queue, replay_metadata_event_log,
    };
    use aruna_storage::storage;
    use aruna_tasks::TaskHandle;
    use byteview::ByteView;
    use ed25519_dalek::SigningKey;
    use tempfile::TempDir;
    use ulid::Ulid;

    struct Fixture {
        _storage_dir: TempDir,
        _metadata_dir: TempDir,
        state: Arc<ServerState>,
        auth: AuthContext,
        realm_id: RealmId,
        groups: [Ulid; 2],
        users: [UserId; 2],
    }

    fn realm_id(seed: u8) -> RealmId {
        RealmId::from_bytes(
            SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .to_bytes(),
        )
    }

    async fn write_bytes(state: &ServerState, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
        let event = state
            .get_ctx()
            .storage_handle
            .send_effect(Effect::Storage(StorageEffect::Write {
                key_space: key_space.to_string(),
                key: ByteView::from(key),
                value: ByteView::from(value),
                txn_id: None,
            }))
            .await;
        assert!(matches!(
            event,
            Event::Storage(StorageEvent::WriteResult { .. })
        ));
    }

    async fn seed_group(state: &ServerState, actor: &Actor, group_id: Ulid, name: &str) {
        let realm = actor.realm_id;
        let auth_doc =
            GroupAuthorizationDocument::new_default_group_doc(actor.user_id, realm, group_id);
        let group = Group {
            display_name: name.to_string(),
            group_id,
            realm_id: realm,
            roles: auth_doc.roles.keys().copied().collect(),
            owner: actor.user_id,
        };
        write_bytes(
            state,
            GROUP_KEYSPACE,
            group_id.to_bytes().to_vec(),
            group.to_bytes(actor).unwrap(),
        )
        .await;
        write_bytes(
            state,
            AUTH_KEYSPACE,
            group_id.to_bytes().to_vec(),
            auth_doc.to_bytes(actor).unwrap(),
        )
        .await;
    }

    async fn seed_user(state: &ServerState, actor: &Actor, user_id: UserId, name: &str) {
        let user = User {
            user_id,
            name: name.to_string(),
            subject_ids: Vec::new(),
            alias_user_ids: Default::default(),
            attributes: Default::default(),
        };
        write_bytes(
            state,
            USER_KEYSPACE,
            user_id.to_storage_key(),
            user.to_bytes(actor).unwrap(),
        )
        .await;
    }

    async fn setup() -> Fixture {
        let storage_dir = tempfile::tempdir().unwrap();
        let metadata_dir = tempfile::tempdir().unwrap();
        let storage_handle =
            storage::FjallStorage::open(storage_dir.path().to_str().unwrap()).unwrap();
        let node_id = iroh::SecretKey::from_bytes(&[11u8; 32]).public();
        let realm = realm_id(5);
        let user_id = UserId::local(Ulid::from_bytes([200u8; 16]), realm);
        let actor = Actor {
            node_id,
            user_id,
            realm_id: realm,
        };
        let metadata_handle = MetadataHandle::new(
            metadata_dir.path(),
            node_id,
            storage_handle.clone(),
            None,
            None,
            None,
        )
        .unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle,
            net_handle: None,
            blob_handle: None,
            metadata_handle: Some(metadata_handle),
            task_handle: Some(TaskHandle::new()),
            compute_handle: None,
        });
        let state = Arc::new(
            ServerState::new(
                driver_ctx,
                realm,
                node_id,
                NodeCapabilities::local_node(realm).unwrap(),
                false,
                None,
                aruna_operations::jobs::runtime::JobsRuntime::new(),
            )
            .await,
        );

        write_bytes(
            &state,
            AUTH_KEYSPACE,
            realm.as_bytes().to_vec(),
            RealmAuthorizationDocument::new_default_realm_doc(realm)
                .to_bytes(&actor)
                .unwrap(),
        )
        .await;

        let groups = [Ulid::from_bytes([1u8; 16]), Ulid::from_bytes([2u8; 16])];
        seed_group(&state, &actor, groups[0], "alpha-team").await;
        seed_group(&state, &actor, groups[1], "alpha-squad").await;

        let users = [
            UserId::local(Ulid::from_bytes([1u8; 16]), realm),
            UserId::local(Ulid::from_bytes([2u8; 16]), realm),
        ];
        seed_user(&state, &actor, users[0], "beta-anna").await;
        seed_user(&state, &actor, users[1], "beta-bob").await;

        Fixture {
            _storage_dir: storage_dir,
            _metadata_dir: metadata_dir,
            state,
            auth: AuthContext {
                user_id,
                realm_id: realm,
                path_restrictions: None,
            },
            realm_id: realm,
            groups,
            users,
        }
    }

    async fn create_doc(fx: &Fixture, group_id: Ulid, path: &str, name: &str) {
        let _ = create_metadata_document(
            State(fx.state.clone()),
            Extension(Some(fx.auth.clone())),
            Extension(None),
            Json(CreateMetadataRequest::Scaffold(
                CreateMetadataScaffoldRequest {
                    group_id: group_id.to_string(),
                    path: path.to_string(),
                    name: name.to_string(),
                    description: "desc".to_string(),
                    date_published: "2026-01-01".to_string(),
                    license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
                    public: true,
                },
            )),
        )
        .await
        .unwrap();
    }

    async fn drain_projection(state: &ServerState) {
        let ctx = state.get_ctx();
        let drained = drain_pending_metadata_projection_queue(ctx.as_ref())
            .await
            .unwrap();
        if drained.markers_examined == 0 {
            replay_metadata_event_log(ctx.as_ref()).await.unwrap();
        }
        process_metadata_materialization_batch(ctx.as_ref())
            .await
            .unwrap();
    }

    async fn flush_search(state: &ServerState) {
        let ctx = state.get_ctx();
        ctx.metadata_handle
            .as_ref()
            .unwrap()
            .flush_search_updates()
            .await
            .unwrap();
    }

    fn params(q: &str) -> SearchParams {
        SearchParams {
            q: q.to_string(),
            ..Default::default()
        }
    }

    async fn search(fx: &Fixture, params: SearchParams) -> ServerResult<SearchResponse> {
        unified_search(
            State(fx.state.clone()),
            Extension(Some(fx.auth.clone())),
            Extension(None),
            Query(params),
        )
        .await
        .map(|(_, Json(body))| body)
    }

    #[tokio::test]
    async fn selects_types() {
        let fx = setup().await;
        let resp = search(
            &fx,
            SearchParams {
                types: Some("groups,users".to_string()),
                ..params("alpha")
            },
        )
        .await
        .unwrap();
        assert!(resp.documents.is_none());
        assert!(resp.groups.is_some());
        assert!(resp.users.is_some());
    }

    #[tokio::test]
    async fn rejects_unknown_type() {
        let fx = setup().await;
        let result = search(
            &fx,
            SearchParams {
                types: Some("groups,bogus".to_string()),
                ..params("alpha")
            },
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn rejects_short_query() {
        let fx = setup().await;
        let result = search(&fx, params("a")).await;
        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn rejects_cursor_multi() {
        // A cursor is only valid when exactly one type is requested.
        let fx = setup().await;
        let result = search(
            &fx,
            SearchParams {
                cursor: Some("token".to_string()),
                ..params("alpha")
            },
        )
        .await;
        assert!(matches!(result, Err(ServerError::BadRequest)));
    }

    #[tokio::test]
    async fn rejects_malformed_cursor() {
        // A garbage single-type cursor is caller input: 400, never a 500.
        let fx = setup().await;
        for section in ["groups", "users"] {
            let result = search(
                &fx,
                SearchParams {
                    types: Some(section.to_string()),
                    cursor: Some("garbage".to_string()),
                    ..params("alpha")
                },
            )
            .await;
            assert!(
                matches!(result, Err(ServerError::BadRequest)),
                "{section} cursor should be rejected"
            );
        }
    }

    #[tokio::test]
    async fn pages_groups() {
        let fx = setup().await;
        let first = search(
            &fx,
            SearchParams {
                types: Some("groups".to_string()),
                limit: Some(1),
                ..params("alpha")
            },
        )
        .await
        .unwrap()
        .groups
        .unwrap();
        assert_eq!(first.hits.len(), 1);
        assert_eq!(first.hits[0].group_id, fx.groups[0].to_string());
        let cursor = first.next_cursor.clone().unwrap();

        let second = search(
            &fx,
            SearchParams {
                types: Some("groups".to_string()),
                limit: Some(1),
                cursor: Some(cursor),
                ..params("alpha")
            },
        )
        .await
        .unwrap()
        .groups
        .unwrap();
        assert_eq!(second.hits.len(), 1);
        assert_eq!(second.hits[0].group_id, fx.groups[1].to_string());
        assert!(second.next_cursor.is_none());
    }

    #[tokio::test]
    async fn pages_users() {
        let fx = setup().await;
        let first = search(
            &fx,
            SearchParams {
                types: Some("users".to_string()),
                limit: Some(1),
                ..params("beta")
            },
        )
        .await
        .unwrap()
        .users
        .unwrap();
        assert_eq!(first.hits.len(), 1);
        assert_eq!(first.hits[0].user_id, fx.users[0].to_string());
        let cursor = first.next_cursor.clone().unwrap();

        let second = search(
            &fx,
            SearchParams {
                types: Some("users".to_string()),
                limit: Some(1),
                cursor: Some(cursor),
                ..params("beta")
            },
        )
        .await
        .unwrap()
        .users
        .unwrap();
        assert_eq!(second.hits.len(), 1);
        assert_eq!(second.hits[0].user_id, fx.users[1].to_string());
        assert!(second.next_cursor.is_none());
    }

    #[tokio::test]
    async fn filters_documents_group() {
        // group_id passes through to metadata search and constrains the hits.
        let fx = setup().await;
        create_doc(&fx, fx.groups[0], "datasets/one", "gamma-one").await;
        create_doc(&fx, fx.groups[1], "datasets/two", "gamma-two").await;
        drain_projection(&fx.state).await;
        flush_search(&fx.state).await;

        let documents = search(
            &fx,
            SearchParams {
                types: Some("documents".to_string()),
                group_id: Some(fx.groups[0].to_string()),
                mode: Some(MetadataQueryMode::Local),
                ..params("gamma")
            },
        )
        .await
        .unwrap()
        .documents
        .unwrap();
        assert!(!documents.hits.is_empty());
        assert!(
            documents
                .hits
                .iter()
                .all(|hit| hit.group_id == fx.groups[0].to_string())
        );
    }

    #[tokio::test]
    async fn requires_auth() {
        let fx = setup().await;
        let unauthenticated = unified_search(
            State(fx.state.clone()),
            Extension(None),
            Extension(None),
            Query(params("alpha")),
        )
        .await;
        assert!(matches!(unauthenticated, Err(ServerError::Unauthorized)));

        let foreign = AuthContext {
            user_id: UserId::local(Ulid::from_bytes([9u8; 16]), realm_id(9)),
            realm_id: realm_id(9),
            path_restrictions: None,
        };
        assert_ne!(foreign.realm_id, fx.realm_id);
        let wrong_realm = unified_search(
            State(fx.state.clone()),
            Extension(Some(foreign)),
            Extension(None),
            Query(params("alpha")),
        )
        .await;
        assert!(matches!(wrong_realm, Err(ServerError::Forbidden)));
    }

    #[tokio::test]
    async fn empty_shape() {
        let fx = setup().await;
        let resp = search(
            &fx,
            SearchParams {
                mode: Some(MetadataQueryMode::Local),
                ..params("nomatchquery")
            },
        )
        .await
        .unwrap();
        let documents = resp.documents.unwrap();
        assert!(documents.hits.is_empty());
        assert!(documents.next_cursor.is_none());
        let groups = resp.groups.unwrap();
        assert!(groups.hits.is_empty());
        assert!(groups.next_cursor.is_none());
        let users = resp.users.unwrap();
        assert!(users.hits.is_empty());
        assert!(users.next_cursor.is_none());
    }
}
