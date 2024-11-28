use crate::models::models::Permission;
use crate::models::requests::*;
use crate::{error::ArunaError, transactions::controller::Controller};
use axum::extract::Path;
use axum::{
    extract::{Query, State},
    http::HeaderMap,
    response::IntoResponse,
    Json,
};
use std::sync::Arc;
use tags::{GLOBAL, GROUPS, INFO, REALMS, RESOURCES, USERS};
use ulid::Ulid;

use super::utils::{extract_token, into_axum_response};

mod tags {
    pub const RESOURCES: &str = "resources";
    pub const REALMS: &str = "realms";
    pub const GROUPS: &str = "groups";
    pub const USERS: &str = "users";
    pub const GLOBAL: &str = "global";
    pub const INFO: &str = "info";
}

/// Create a new resource
#[utoipa::path(
    post,
    path = "/resources",
    request_body = CreateResourceRequest,
    responses(
        (status = 200, body = CreateResourceResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
pub async fn create_resource(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<CreateResourceRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Create a new resource
#[utoipa::path(
    post,
    path = "/resources/batch",
    request_body = CreateResourceBatchRequest,
    responses(
        (status = 200, body = CreateResourceResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
pub async fn create_resource_batch(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<CreateResourceBatchRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Create a new resource
#[utoipa::path(
    post,
    path = "/resources/projects",
    request_body = CreateProjectRequest,
    responses(
        (status = 200, body = CreateProjectResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
pub async fn create_project(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<CreateProjectRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Create a new relation
#[utoipa::path(
    post,
    path = "/resources/relations",
    request_body = CreateRelationRequest,
    responses(
        (status = 200, body = CreateRelationResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
pub async fn create_relation(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<CreateResourceRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Create a new relation variant
#[utoipa::path(
    post,
    path = "/global/relation_variant",
    request_body = CreateRelationVariantRequest,
    responses(
        (status = 200, body = CreateRelationVariantResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = GLOBAL,
)]
pub async fn create_relation_variant(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<CreateResourceRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Get resources
#[utoipa::path(
    get,
    path = "/resources",
    params(
        GetResourcesRequest,
    ),
    responses(
        (status = 200, body = GetResourcesResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
pub async fn get_resource(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetResourcesRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Create a new realm
#[utoipa::path(
    post,
    path = "/realms",
    request_body = CreateRealmRequest,
    responses(
        (status = 200, body = CreateRealmResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = REALMS,
)]
pub async fn create_realm(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<CreateRealmRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get realm
#[utoipa::path(
    get,
    path = "/realms/{id}",
    params(
        ("id" = Ulid, Path, description = "Realm ID"),
    ),
    responses(
        (status = 200, body = GetRealmResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = REALMS,
)]
pub async fn get_realm(
    Path(id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetRealmRequest { id }, extract_token(&header))
            .await,
    )
}

/// Add group to realm
#[utoipa::path(
    patch,
    path = "/realms/{id}/groups/{group_id}",
    params(
        ("id" = Ulid, Path, description = "Realm ID"),
        ("group_id" = Ulid, Path, description = "Group ID"),
    ),
    request_body = AddGroupRequest,
    responses(
        (status = 200, body = AddGroupResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = REALMS,
)]
pub async fn add_group(
    Path((realm_id, group_id)): Path<(Ulid, Ulid)>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(
                AddGroupRequest { realm_id, group_id },
                extract_token(&header),
            )
            .await,
    )
}

/// Create a new  
#[utoipa::path(
    post,
    path = "/groups",
    request_body = CreateGroupRequest,
    responses(
        (status = 200, body = CreateGroupResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = GROUPS,
)]
pub async fn create_group(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<CreateGroupRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get group by id
#[utoipa::path(
    get,
    path = "/groups/{id}",
    params(
        ("id" = Ulid, Path, description = "Realm ID"),
    ),
    responses(
        (status = 200, body = GetGroupResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = GROUPS,
)]
pub async fn get_group(
    Path(id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetGroupRequest { id }, extract_token(&header))
            .await,
    )
}

/// Register a new user
#[utoipa::path(
    post,
    path = "/users",
    request_body = RegisterUserRequest,
    responses(
        (status = 200, body = RegisterUserResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = USERS,
)]
pub async fn register_user(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<RegisterUserRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Add user to group
#[utoipa::path(
    patch,
    path = "/groups/{group_id}/user/{user_id}",
    params(
        ("group_id" = Ulid, Path, description = "Group ID"),
        ("user_id" = Ulid, Path, description = "User ID"),
        ("permission" = Permission, Query, description = "Permission"),
    ),
    responses(
        (status = 200, body = AddUserResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = GROUPS,
)]
pub async fn add_user(
    Path((group_id, user_id)): Path<(Ulid, Ulid)>,
    State(state): State<Arc<Controller>>,
    Query(permission): Query<Permission>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(
                AddUserRequest {
                    group_id,
                    user_id,
                    permission,
                },
                extract_token(&header),
            )
            .await,
    )
}

/// Create a token
#[utoipa::path(
    post,
    path = "/users/tokens",
    request_body = CreateTokenRequest,
    responses(
        (status = 200, body = CreateTokenResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = USERS,
)]
pub async fn create_token(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<CreateTokenRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Search for resources
#[utoipa::path(
    get,
    path = "/info/search",
    params(
        SearchRequest,
    ),
    responses(
        (status = 200, body = SearchResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = INFO,
)]
pub async fn search(
    State(state): State<Arc<Controller>>,
    Query(request): Query<SearchRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get all realms from the current user
#[utoipa::path(
    get,
    path = "/users/realms",
    responses(
        (status = 200, body = GetRealmsFromUserResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = USERS,
)]
pub async fn get_user_realms(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetRealmsFromUserRequest {}, extract_token(&header))
            .await,
    )
}

/// Get all groups from the current user
#[utoipa::path(
    get,
    path = "/users/groups",
    responses(
        (status = 200, body = GetGroupsFromUserResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = USERS,
)]
pub async fn get_user_groups(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetGroupsFromUserRequest {}, extract_token(&header))
            .await,
    )
}

/// Get global server stats
#[utoipa::path(
    get,
    path = "/info/stats",
    responses(
        (status = 200, body = GetStatsResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = INFO,
)]
pub async fn get_stats(
    State(_state): State<Arc<Controller>>,
    _header: HeaderMap,
) -> impl IntoResponse {
    // TODO: Remove dummy data and impl stats collection
    // todo!();
    //into_axum_response(state.request(GetRealmsFromUserRequest{}, extract_token(&header)).await)
    Json(GetStatsResponse {
        resources: 1023,
        projects: 5,
        users: 12,
        storage: 12312930192,
        realms: 3,
    })
}

/// Get components of a realm (server, dataproxies, etc)
#[utoipa::path(
    get,
    path = "/realms/{id}/components",
    params(
        ("id" = Ulid, Path, description = "Realm ID"),
    ),
    responses(
        (status = 200, body = GetRealmComponentsRequest),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = REALMS,
)]
pub async fn get_realm_components(
    Path(id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(
                GetRealmComponentsRequest { realm_id: id },
                extract_token(&header),
            )
            .await,
    )
}


/// Add a component to a realm (dataproxies, etc)
#[utoipa::path(
    patch,
    path = "/realms/{id}/components/{component_id}",
    params(
        ("id" = Ulid, Path, description = "Realm ID"),
        ("component_id" = Ulid, Path, description = "Component ID"),
    ),
    responses(
        (status = 200, body = AddComponentToRealmResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = REALMS,
)]
pub async fn add_component_to_realm(
    Path((id, component_id)): Path<(Ulid, Ulid)>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(
                AddComponentToRealmRequest { realm_id: id, component_id },
                extract_token(&header),
            )
            .await,
    )
}

/// Get relations of a resource
#[utoipa::path(
    get,
    path = "/resources/{id}/relations",
    params(
        ("id" = Ulid, Path, description = "Resource ID"),
    ),
    responses(
        (status = 200, body = GetRelationsResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
pub async fn get_relations(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetRelationsRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get users from group
#[utoipa::path(
    get,
    path = "/groups/{id}/users",
    params(
        ("id" = Ulid, Path, description = "Group ID"),
    ),
    responses(
        (status = 200, body = GetUsersFromGroupResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = GROUPS,
)]
pub async fn get_group_users(
    Path(group_id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(
                GetUsersFromGroupRequest { group_id },
                extract_token(&header),
            )
            .await,
    )
}

/// Get groups from realm
#[utoipa::path(
    get,
    path = "/realms/{id}/groups",
    params(
        ("id" = Ulid, Path, description = "Realm ID"),
    ),
    responses(
        (status = 200, body = GetGroupsFromRealmResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = REALMS,
)]
pub async fn get_realm_groups(
    Path(realm_id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(
                GetGroupsFromRealmRequest { realm_id },
                extract_token(&header),
            )
            .await,
    )
}

/// Get relation info
#[utoipa::path(
    get,
    path = "/global/relations",
    responses(
        (status = 200, body = GetRelationInfosResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = GLOBAL,
)]
pub async fn get_relation_infos(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetRelationInfosRequest {}, extract_token(&header))
            .await,
    )
}

/// Get current user
#[utoipa::path(
    get,
    path = "/users",
    responses(
        (status = 200, body = GetUserResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = USERS,
)]
pub async fn get_user(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetUserRequest {}, extract_token(&header))
            .await,
    )
}

/// Get events information
#[utoipa::path(
    get,
    path = "/info/events",
    params(
        GetEventsRequest,
    ),
    responses(
        (status = 200, body = GetEventsResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = INFO,
)]
pub async fn get_events(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetEventsRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Request group join realm
#[utoipa::path(
    post,
    path = "/realms/{id}/access",
    params(
        ("id" = Ulid, Path, description = "Realm ID"),
        ("group_id" = Ulid, Query, description = "Group ID"),
    ),
    responses(
        (status = 200, body = GroupAccessRealmResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = REALMS,
)]
pub async fn request_group_access_realm(
    Path(realm_id): Path<Ulid>,
    Query(group_id): Query<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    todo!();
    // into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Request user join group
#[utoipa::path(
    post,
    path = "/groups/{id}/join",
    params(
        ("id" = Ulid, Path, description = "Group ID"),
    ),
    responses(
        (status = 200, body = UserAccessGroupResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = GROUPS,
)]
pub async fn request_user_access_group(
    Path(group_id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    todo!()
    // into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Create a new component
#[utoipa::path(
    post,
    path = "/global/components",
    request_body = CreateComponentRequest,
    responses(
        (status = 200, body = CreateComponentResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = GLOBAL,
)]
pub async fn create_component(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<CreateComponentRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}
