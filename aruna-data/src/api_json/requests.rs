use crate::io::controller::Controller;
use crate::api_json::request::{Request, User};
use aruna_storage::storage::store::Store;
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::ToSchema;

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct CreateS3CredentialsRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateS3CredentialsResponse {
    pub access_key_id: Ulid,
    pub secret_access_key: String,
}

#[async_trait::async_trait]
impl<St> Request<St> for CreateS3CredentialsRequest
where
    for<'a> St: Store<'a> + 'static,
{
    type Response = CreateS3CredentialsResponse;

    #[tracing::instrument(level = "trace", skip(_controller))]
    async fn forward_or_return(
        &self,
        user: &Option<String>,
        _controller: &Controller<St>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaDataError> {
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<User>,
        controller: &Controller<St>,
    ) -> Result<Self::Response, crate::error::ArunaDataError> {
        let Some(user) = user else {
            return Err(crate::error::ArunaDataError::Unauthorized);
        };

        Ok(CreateS3CredentialsResponse {
            access_key_id: todo!(),
            secret_access_key: todo!(),
        })
    }
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct GetS3CredentialsRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetS3CredentialsResponse {
    pub access_key_id: Ulid,
    pub secret_access_key: String,
}

#[async_trait::async_trait]
impl<St> Request<St> for GetS3CredentialsRequest
where
    for<'a> St: Store<'a> + 'static,
{
    type Response = GetS3CredentialsResponse;

    #[tracing::instrument(level = "trace", skip(_controller))]
    async fn forward_or_return(
        &self,
        user: &Option<String>,
        _controller: &Controller<St>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaDataError> {
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<User>,
        controller: &Controller<St>,
    ) -> Result<Self::Response, crate::error::ArunaDataError> {
        let Some(user) = user else {
            return Err(crate::error::ArunaDataError::Unauthorized);
        };

        Ok(GetS3CredentialsResponse {
            access_key_id: todo!(),
            secret_access_key: todo!(),
        })
    }
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct DeleteS3CredentialsRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct DeleteS3CredentialsResponse {}

#[async_trait::async_trait]
impl<St> Request<St> for DeleteS3CredentialsRequest
where
    for<'a> St: Store<'a> + 'static,
{
    type Response = DeleteS3CredentialsResponse;

    #[tracing::instrument(level = "trace", skip(_controller))]
    async fn forward_or_return(
        &self,
        user: &Option<String>,
        _controller: &Controller<St>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaDataError> {
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<User>,
        controller: &Controller<St>,
    ) -> Result<Self::Response, crate::error::ArunaDataError> {
        let Some(user) = user else {
            return Err(crate::error::ArunaDataError::Unauthorized);
        };

        Ok(DeleteS3CredentialsResponse {})
    }
}
