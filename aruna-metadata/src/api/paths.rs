use crate::{
    error::ArunaMetadataError, models::requests::*, network::network_trait::Network,
    persistence::search::search::Search, transactions::controller::Controller,
};
use aruna_storage::storage::store::Store;
use axum::{
    Json,
    extract::{Query, State},
    http::HeaderMap,
    response::IntoResponse,
};
use std::sync::Arc;
use tags::*;

use super::utils::{extract_token, into_axum_response};

mod tags {
    pub const RESOURCES: &str = "resources";
    // pub const REALMS: &str = "realms";
    // pub const GROUPS: &str = "groups";
    pub const USERS: &str = "users";
    // pub const GLOBAL: &str = "global";
    pub const INFO: &str = "info";
    // pub const LICENSE: &str = "license";
}

/// Create a new resource
#[utoipa::path(
    post,
    path = "/resources",
    request_body = CreateResourceRequest,
    responses(
        (status = 200, body = CreateResourceResponse),
        ArunaMetadataError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn create_resource<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    headers: HeaderMap,
    Json(request): Json<CreateResourceRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Get resources
#[utoipa::path(
    get,
    path = "/resources",
    params(
        GetResourceRequest,
    ),
    responses(
        (status = 200, body = GetResourceResponse),
        ArunaMetadataError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = []),
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn get_resource<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    axum_extra::extract::Query(request): axum_extra::extract::Query<GetResourceRequest>,
    header: HeaderMap,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Update resource name
#[utoipa::path(
    post,
    path = "/resources/name",
    request_body = UpdateResourceNameRequest,
    responses(
        (status = 200, body = UpdateResourceNameResponse),
        ArunaMetadataError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_name<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceNameRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
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
        Ok(_) => ArunaMetadataError::DeserializeError(
            "Internal response serialization error".to_string(),
        )
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
        ArunaMetadataError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_title<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceTitleRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
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
        Ok(_) => ArunaMetadataError::DeserializeError(
            "Internal response serialization error".to_string(),
        )
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
        ArunaMetadataError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_description<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceDescriptionRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
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
        Ok(_) => ArunaMetadataError::DeserializeError(
            "Internal response serialization error".to_string(),
        )
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
        ArunaMetadataError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_visibility<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceVisibilityRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
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
        Ok(_) => ArunaMetadataError::DeserializeError(
            "Internal response serialization error".to_string(),
        )
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
        ArunaMetadataError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_license<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceLicenseRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
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
        Ok(_) => ArunaMetadataError::DeserializeError(
            "Internal response serialization error".to_string(),
        )
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
        ArunaMetadataError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_labels<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceLabelsRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
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
        Ok(_) => ArunaMetadataError::DeserializeError(
            "Internal response serialization error".to_string(),
        )
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
        ArunaMetadataError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_identifiers<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceIdentifiersRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
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
        Ok(_) => ArunaMetadataError::DeserializeError(
            "Internal response serialization error".to_string(),
        )
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
        ArunaMetadataError,
    ),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn update_resource_authors<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    headers: HeaderMap,
    Json(request): Json<UpdateResourceAuthorsRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
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
        Ok(_) => ArunaMetadataError::DeserializeError(
            "Internal response serialization error".to_string(),
        )
        .into_axum_tuple()
        .into_response(),
        Err(e) => e.into_axum_tuple().into_response(),
    }
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
        ArunaMetadataError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
    tag = INFO,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn search<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    Query(request): Query<SearchRequest>,
    header: HeaderMap,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Add a new user
#[utoipa::path(
    post,
    path = "/users",
    request_body = AddUserRequest,
    responses(
        (status = 200, body = AddUserResponse),
        ArunaMetadataError,
    ),
    security(
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn add_user<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    headers: HeaderMap,
    Json(request): Json<AddUserRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Add a new group
#[utoipa::path(
    post,
    path = "/groups",
    request_body = AddGroupRequest,
    responses(
        (status = 200, body = AddGroupResponse),
        ArunaMetadataError,
    ),
    security(
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn add_group<St, Se, N>(
    State(state): State<Arc<Controller<St, Se, N>>>,
    headers: HeaderMap,
    Json(request): Json<AddUserRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    into_axum_response(state.request(request, extract_token(&headers)).await)
}
