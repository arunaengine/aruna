use crate::api_json::request::{Request, User};
use crate::api_s3::auth::UserAccess;
use crate::error::ArunaDataError;
use crate::io::controller::Controller;
use crate::io::io_handler::ACCESS_DB_NAME;
use aruna_permission::UserIdentity;
use aruna_storage::storage::store::Store;
use rand::distributions::Alphanumeric;
use rand::{Rng, thread_rng};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::ToSchema;

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct CreateS3CredentialsRequest {
    group_id: String,
}

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
    ) -> Result<Option<Self::Response>, ArunaDataError> {
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<UserIdentity>,
        controller: &Controller<St>,
    ) -> Result<Self::Response, ArunaDataError> {
        let Some(user) = user else {
            return Err(ArunaDataError::Unauthorized);
        };

        let access_key_id = Ulid::new();
        let access_info = UserAccess {
            user_id: user,
            group_id: Ulid::from_string(&self.group_id)?,
            secret: thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect::<String>(),
        };

        let store_clone = controller.io_handler.store.clone();
        let access_key_id_clone = access_key_id.clone();
        let access_info_clone = access_info.clone();
        tokio::task::spawn_blocking(move || {
            let mut write_txn = store_clone.create_txn(true)?;

            // Store object info with blake3 hash as key
            store_clone.put(
                &mut write_txn,
                ACCESS_DB_NAME,
                &access_key_id_clone.to_bytes(),
                &bincode::serde::encode_to_vec(access_info_clone, bincode::config::standard())?,
            )?;

            store_clone.commit(write_txn)?;

            Ok::<(), anyhow::Error>(())
        });

        Ok(CreateS3CredentialsResponse {
            access_key_id,
            secret_access_key: access_info.secret.to_string(),
        })
    }
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct GetS3CredentialsRequest {}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct GetS3CredentialsResponse {
    pub access_key_id: String,
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
    ) -> Result<Option<Self::Response>, ArunaDataError> {
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(_controller))]
    async fn run_request(
        self,
        user: Option<UserIdentity>,
        _controller: &Controller<St>,
    ) -> Result<Self::Response, ArunaDataError> {
        let Some(_user) = user else {
            return Err(ArunaDataError::Unauthorized);
        };

        //TODO: Fetch

        Ok(GetS3CredentialsResponse {
            access_key_id: "TODO".to_string(),
            secret_access_key: "TODO".to_string(),
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
    ) -> Result<Option<Self::Response>, ArunaDataError> {
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(_controller))]
    async fn run_request(
        self,
        user: Option<UserIdentity>,
        _controller: &Controller<St>,
    ) -> Result<Self::Response, ArunaDataError> {
        let Some(_user) = user else {
            return Err(ArunaDataError::Unauthorized);
        };

        Ok(DeleteS3CredentialsResponse {})
    }
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema, Default,
)]
pub struct RegisterDataRequest {
    group_id: String,
    backend_path: String,
    bucket: Option<String>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize, ToSchema)]
pub struct RegisterDataResponse {
    path: String,
}

#[async_trait::async_trait]
impl<St> Request<St> for RegisterDataRequest
where
    for<'a> St: Store<'a> + 'static,
{
    type Response = RegisterDataResponse;

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<UserIdentity>,
        controller: &Controller<St>,
    ) -> Result<Self::Response, ArunaDataError> {
        let Some(user) = user else {
            return Err(ArunaDataError::Unauthorized);
        };

        let group_id = Ulid::from_string(&self.group_id)?;
        let frontend_path = controller
            .io_handler
            .register_backend_data(&self.backend_path, group_id)
            .await
            .map_err(|e| ArunaDataError::IoError(e.to_string()))?;

        Ok(RegisterDataResponse {
            path: frontend_path,
        })
    }

    #[tracing::instrument(level = "trace", skip(_controller))]
    async fn forward_or_return(
        &self,
        user: &Option<String>,
        _controller: &Controller<St>,
    ) -> Result<Option<Self::Response>, ArunaDataError> {
        Ok(None)
    }
}
