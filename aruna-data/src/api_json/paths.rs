use super::util::{extract_token, into_axum_response};
use crate::api_json::requests::*;
use crate::error::ArunaDataError;
use crate::controller::controller::Controller;
use aruna_storage::storage::store::Store;
use axum::{Json, extract::State, http::HeaderMap, response::IntoResponse};
use tags::*;

mod tags {
    pub const USERS: &str = "users";
    pub const RESOURCES: &str = "resources";
}

/// Create new credentials for a user
#[utoipa::path(
    post,
    path = "/users/credentials",
    request_body = CreateS3CredentialsRequest,
    responses(
        (status = 200, body = CreateS3CredentialsResponse), ArunaDataError),
    security(
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn create_s3_credentials<St>(
    State(state): State<Controller<St>>,
    headers: HeaderMap,
    Json(request): Json<CreateS3CredentialsRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
{
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Fetch credentials of a user
#[utoipa::path(
    get,
    path = "/users/credentials",
    responses(
        (status = 200, body = GetS3CredentialsResponse), ArunaDataError),
    security(
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn get_s3_credentials<St>(
    State(state): State<Controller<St>>,
    headers: HeaderMap,
    Json(request): Json<GetS3CredentialsRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
{
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Delete credentials of a user
#[utoipa::path(
    delete,
    path = "/users/credentials",
    responses(
        (status = 200, body = DeleteS3CredentialsResponse), ArunaDataError),
    security(
        ("auth" = [])
    ),
    tag = USERS,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn delete_s3_credentials<St>(
    State(state): State<Controller<St>>,
    headers: HeaderMap,
    Json(request): Json<DeleteS3CredentialsRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
{
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Register already existing data resource
#[utoipa::path(
    post,
    path = "/data/register",
    request_body = RegisterDataRequest,
    responses(
        (status = 200, body = RegisterDataResponse), ArunaDataError),
    security(
        ("auth" = [])
    ),
    tag = RESOURCES,
)]
#[tracing::instrument(level = "trace", skip(state))]
pub async fn register_data<St>(
    State(state): State<Controller<St>>,
    headers: HeaderMap,
    Json(request): Json<RegisterDataRequest>,
) -> impl IntoResponse
where
    for<'a> St: Store<'a> + 'static,
{
    into_axum_response(state.request(request, extract_token(&headers)).await)
}
