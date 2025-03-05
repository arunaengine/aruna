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
use tags::{GLOBAL, GROUPS, INFO, LICENSE, REALMS, RESOURCES, USERS};
use ulid::Ulid;

use super::utils::{extract_token, into_axum_response};

mod tags {
    pub const RESOURCES: &str = "resources";
    pub const REALMS: &str = "realms";
    pub const GROUPS: &str = "groups";
    pub const USERS: &str = "users";
    pub const GLOBAL: &str = "global";
    pub const INFO: &str = "info";
    pub const LICENSE: &str = "license";
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
#[tracing::instrument(level = "trace", skip(state))]
pub async fn create_resource(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<CreateResourceRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Update resource name
#[utoipa::path(
    post,
    path = "/resources/name",
    request_body = UpdateResourceNameRequest,
    responses(
        (status = 200, body = UpdateResourceNameResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_name(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceNameRequest>,
) -> impl IntoResponse {
    match state
        .request(
            ResourceUpdateRequests::Name(request),
            extract_token(&headers),
        )
        .await
    {
        Ok(ResourceUpdateResponses::Name(res)) => {
            (axum::http::StatusCode::OK, Json(res)).into_response()
        }
        Ok(_) => ArunaError::DeserializeError("Internal response serialization error".to_string())
            .into_axum_tuple()
            .into_response(),
        Err(e) => e.into_axum_tuple().into_response(),
    }
}

/// Update resource title
#[utoipa::path(
    post,
    path = "/resources/title",
    request_body = UpdateResourceTitleRequest,
    responses(
        (status = 200, body = UpdateResourceTitleResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_title(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceTitleRequest>,
) -> impl IntoResponse {
    match state
        .request(
            ResourceUpdateRequests::Title(request),
            extract_token(&headers),
        )
        .await
    {
        Ok(ResourceUpdateResponses::Title(res)) => {
            (axum::http::StatusCode::OK, Json(res)).into_response()
        }
        Ok(_) => ArunaError::DeserializeError("Internal response serialization error".to_string())
            .into_axum_tuple()
            .into_response(),
        Err(e) => e.into_axum_tuple().into_response(),
    }
}

/// Update resource description
#[utoipa::path(
    post,
    path = "/resources/description",
    request_body = UpdateResourceDescriptionRequest,
    responses(
        (status = 200, body = UpdateResourceDescriptionResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_description(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceDescriptionRequest>,
) -> impl IntoResponse {
    match state
        .request(
            ResourceUpdateRequests::Description(request),
            extract_token(&headers),
        )
        .await
    {
        Ok(ResourceUpdateResponses::Description(res)) => {
            (axum::http::StatusCode::OK, Json(res)).into_response()
        }
        Ok(_) => ArunaError::DeserializeError("Internal response serialization error".to_string())
            .into_axum_tuple()
            .into_response(),
        Err(e) => e.into_axum_tuple().into_response(),
    }
}

/// Update resource visiblity
#[utoipa::path(
    post,
    path = "/resources/visibility",
    request_body = UpdateResourceVisibilityRequest,
    responses(
        (status = 200, body = UpdateResourceVisibilityResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_visibility(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceVisibilityRequest>,
) -> impl IntoResponse {
    match state
        .request(
            ResourceUpdateRequests::Visibility(request),
            extract_token(&headers),
        )
        .await
    {
        Ok(ResourceUpdateResponses::Visibility(res)) => {
            (axum::http::StatusCode::OK, Json(res)).into_response()
        }
        Ok(_) => ArunaError::DeserializeError("Internal response serialization error".to_string())
            .into_axum_tuple()
            .into_response(),
        Err(e) => e.into_axum_tuple().into_response(),
    }
}

/// Update resource license
#[utoipa::path(
    post,
    path = "/resources/license",
    request_body = UpdateResourceLicenseRequest,
    responses(
        (status = 200, body = UpdateResourceLicenseResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_license(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceLicenseRequest>,
) -> impl IntoResponse {
    match state
        .request(
            ResourceUpdateRequests::License(request),
            extract_token(&headers),
        )
        .await
    {
        Ok(ResourceUpdateResponses::License(res)) => {
            (axum::http::StatusCode::OK, Json(res)).into_response()
        }
        Ok(_) => ArunaError::DeserializeError("Internal response serialization error".to_string())
            .into_axum_tuple()
            .into_response(),
        Err(e) => e.into_axum_tuple().into_response(),
    }
}

/// Update resource labels
#[utoipa::path(
    post,
    path = "/resources/labels",
    request_body = UpdateResourceLabelsRequest,
    responses(
        (status = 200, body = UpdateResourceLabelsResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_labels(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceLabelsRequest>,
) -> impl IntoResponse {
    match state
        .request(
            ResourceUpdateRequests::Labels(request),
            extract_token(&headers),
        )
        .await
    {
        Ok(ResourceUpdateResponses::Labels(res)) => {
            (axum::http::StatusCode::OK, Json(res)).into_response()
        }
        Ok(_) => ArunaError::DeserializeError("Internal response serialization error".to_string())
            .into_axum_tuple()
            .into_response(),
        Err(e) => e.into_axum_tuple().into_response(),
    }
}

/// Update resource identifiers
#[utoipa::path(
    post,
    path = "/resources/identifiers",
    request_body = UpdateResourceIdentifiersRequest,
    responses(
        (status = 200, body = UpdateResourceIdentifiersResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_identifiers(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceIdentifiersRequest>,
) -> impl IntoResponse {
    match state
        .request(
            ResourceUpdateRequests::Identifiers(request),
            extract_token(&headers),
        )
        .await
    {
        Ok(ResourceUpdateResponses::Identifiers(res)) => {
            (axum::http::StatusCode::OK, Json(res)).into_response()
        }
        Ok(_) => ArunaError::DeserializeError("Internal response serialization error".to_string())
            .into_axum_tuple()
            .into_response(),
        Err(e) => e.into_axum_tuple().into_response(),
    }
}

/// Update resource authors
#[utoipa::path(
    post,
    path = "/resources/authors",
    request_body = UpdateResourceAuthorsRequest,
    responses(
        (status = 200, body = UpdateResourceAuthorsResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_authors(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceAuthorsRequest>,
) -> impl IntoResponse {
    match state
        .request(
            ResourceUpdateRequests::Authors(request),
            extract_token(&headers),
        )
        .await
    {
        Ok(ResourceUpdateResponses::Authors(res)) => {
            (axum::http::StatusCode::OK, Json(res)).into_response()
        }
        Ok(_) => ArunaError::DeserializeError("Internal response serialization error".to_string())
            .into_axum_tuple()
            .into_response(),
        Err(e) => e.into_axum_tuple().into_response(),
    }
}

/// Create a new resource
#[utoipa::path(
    post,
    path = "/resources/batch",
    request_body = CreateResourceBatchRequest,
    responses(
        (status = 200, body = CreateResourceBatchResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
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
#[tracing::instrument(level = "trace", skip(state))]
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
#[tracing::instrument(level = "trace", skip(state))]
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
#[tracing::instrument(level = "trace", skip(state))]
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
        (), // <-- make optional authentication
        ("auth" = []),
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn get_resource(
    State(state): State<Arc<Controller>>,
    axum_extra::extract::Query(request): axum_extra::extract::Query<GetResourcesRequest>,
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
#[tracing::instrument(level = "trace", skip(state))]
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
        (), // <-- make optional authentication
        ("auth" = []),
    ),
    tag = REALMS,
)]
#[tracing::instrument(level = "trace", skip(state))]
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
#[tracing::instrument(level = "trace", skip(state))]
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

/// Create a new group
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
#[tracing::instrument(level = "trace", skip(state))]
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
#[tracing::instrument(level = "trace", skip(state))]
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
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
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
#[tracing::instrument(level = "trace", skip(state))]
pub async fn add_user(
    Path((group_id, user_id)): Path<(Ulid, Ulid)>,
    State(state): State<Arc<Controller>>,
    Query(permission): Query<PermissionWrapper>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(
                AddUserRequest {
                    group_id,
                    user_id,
                    permission: permission.permission,
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
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn create_token(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<CreateTokenRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// List all tokens from the current user
#[utoipa::path(
    get,
    path = "/users/tokens",
    responses(
        (status = 200, body = GetTokensResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn get_tokens(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetTokensRequest {}, extract_token(&header))
            .await,
    )
}

/// Create a s3credential
#[utoipa::path(
    post,
    path = "/users/s3credentials",
    request_body = CreateS3CredentialsRequest,
    responses(
        (status = 200, body = CreateS3CredentialsResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn create_s3_credential(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<CreateS3CredentialsRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// List all tokens from the current user
#[utoipa::path(
    get,
    path = "/users/s3credentials",
    responses(
        (status = 200, body = GetS3CredentialsResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn get_s3_credentials(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetS3CredentialsRequest {}, extract_token(&header))
            .await,
    )
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
#[tracing::instrument(level = "trace", skip(state))]
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
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
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
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
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
#[tracing::instrument(level = "trace", skip(_state))]
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
        (status = 200, body = GetRealmComponentsResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = REALMS,
)]
#[tracing::instrument(level = "trace", skip(state))]
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
        ("auth" = [])
    ),
    tag = REALMS,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn add_component_to_realm(
    Path((id, component_id)): Path<(Ulid, Ulid)>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(
                AddComponentToRealmRequest {
                    realm_id: id,
                    component_id,
                },
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
        ("direction" = Option<Direction>, Query, description = "Direction of relation"),
        ("filter" = Option<Vec<u32>>, Query, description = "Filter relations by index"),
        ("offset" = Option<u32>, Query, description = "Offset for pagination"),
        ("page_size" = Option<u32>, Query, description = "Page size for pagination"),
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
#[tracing::instrument(level = "trace", skip(state))]
pub async fn get_relations(
    Path(id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    axum_extra::extract::Query(mut request): axum_extra::extract::Query<GetRelationsRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    request.node = id;
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
#[tracing::instrument(level = "trace", skip(state))]
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
#[tracing::instrument(level = "trace", skip(state))]
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
#[tracing::instrument(level = "trace", skip(state))]
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
#[tracing::instrument(level = "trace", skip(state))]
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
        ("auth" = [])
    ),
    tag = INFO,
)]
#[tracing::instrument(level = "trace", skip(state))]
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
#[tracing::instrument(level = "trace", skip(state))]
pub async fn request_group_access_realm(
    Path(realm_id): Path<Ulid>,
    Query(GroupAccessRealmRequestHelper { group_id }): Query<GroupAccessRealmRequestHelper>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(
                GroupAccessRealmRequest { realm_id, group_id },
                extract_token(&header),
            )
            .await,
    )
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
#[tracing::instrument(level = "trace", skip(state))]
pub async fn request_user_access_group(
    Path(group_id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(UserAccessGroupRequest { group_id }, extract_token(&header))
            .await,
    )
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
#[tracing::instrument(level = "trace", skip(state))]
pub async fn create_component(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<CreateComponentRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Register data for an object
#[utoipa::path(
    post,
    path = "/resources/{id}/data",
    params(
        ("id" = Ulid, Path, description = "Resource ID (Must be object)"),
    ),
    request_body = RegisterDataRequest,
    responses(
        (status = 200, body = RegisterDataResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn register_data(
    Path(id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(mut request): Json<RegisterDataRequest>,
) -> impl IntoResponse {
    request.object_id = id;
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Register data for an object
#[utoipa::path(
    get,
    path = "/resources/{id}/authorize",
    params(
        ("id" = Ulid, Path, description = "Resource ID (Must be object)"),
    ),
    request_body = (),
    responses(
        (status = 200, body = ()),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn authorize_resource(
    Path(id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(AuthorizeRequest { id }, extract_token(&header))
            .await,
    )
}

/// Register data for an object
#[utoipa::path(
    delete,
    path = "/resources/{id}",
    params(
        ("id" = Ulid, Path, description = "Resource ID"),
    ),
    request_body = (),
    responses(
        (status = 200, body = ()),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn delete_resource(
    Path(id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(DeleteRequest { id }, extract_token(&header))
            .await,
    )
}

/// Register a license
#[utoipa::path(
    post,
    path = "/license",
    request_body = CreateLicenseRequest,
    responses(
        (status = 200, body = CreateLicenseResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
    tag = LICENSE,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn create_license(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<CreateLicenseRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get license
#[utoipa::path(
    get,
    path = "/license/{id}",
    params(
        ("id" = Ulid, Path, description = "License ID"),
    ),
    responses(
        (status = 200, body = GetLicenseResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = LICENSE,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn get_license(
    Path(id): Path<Ulid>,
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetLicenseRequest { id }, extract_token(&header))
            .await,
    )
}

/// Get licenses
#[utoipa::path(
    get,
    path = "/license",
    responses(
        (status = 200, body = GetLicensesResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = LICENSE,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn get_licenses(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetLicensesRequest {}, extract_token(&header))
            .await,
    )
}
