use super::request::Request;
use crate::{
    models::requests::{AddUserRequest, AddUserResponse, GetUserRequest, GetUserResponse},
    network::network_trait::Network,
    persistence::search::search::Search,
};
use aruna_permission::{OidcToken, Path, UserIdentity};
use aruna_storage::storage::store::Store;

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for AddUserRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = AddUserResponse;
    type AuthContext = OidcToken;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::AuthContext, crate::error::ArunaMetadataError> {
        let Some(token) = token else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        };
        let (token, user) = controller.persistence.check_oidc_token(token).await?;
        if user.is_some() {
            Err(crate::error::ArunaMetadataError::Forbidden(
                "User already exsists".to_string(),
            ))
        } else {
            Ok(token)
        }
    }

    #[tracing::instrument(level = "trace", skip(_controller))]
    async fn forward_or_return(
        &self,
        user: &Option<String>,
        _controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        auth_ctx: Self::AuthContext,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let node_id = controller.network.get_addr().await?.node_id;
        let (user, doc) = controller
            .persistence
            .add_user(node_id.as_bytes(), self.name, auth_ctx)
            .await?;

        // Store user hash in DHT
        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(user.id.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();
        controller.network.store(subject_hash).await?;

        // (for now) Replicate users to all member nodes
        let members = controller
            .network
            .get_realm_nodes()
            .await?
            .into_iter()
            .filter(|addr| addr.node_id != node_id);

        // TODO: Change this later either to a group + user path
        // or Option<Path>
        let path = Path::builder()
            .realm_id(controller.network.get_realm_key().await?)
            .build()
            .map_err(|e| crate::error::ArunaMetadataError::ServerError(e.to_string()))?;

        controller
            .sync_loop(
                crate::models::models::TypedDoc::User(doc),
                *subject_hash,
                user.id.to_bytes(),
                path,
                members,
            )
            .await?;

        Ok(AddUserResponse { user })
    }
}

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for GetUserRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = GetUserResponse;
    type AuthContext = UserIdentity;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::AuthContext, crate::error::ArunaMetadataError> {
        // TODO: Handle permissions when user is requesting other users
        let Some(token) = token else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        };
        controller.persistence.get_identity(token).await
    }

    #[tracing::instrument(level = "trace", skip(_controller))]
    async fn forward_or_return(
        &self,
        user: &Option<String>,
        _controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        // TODO: Forward when cross realm and user querying other users is implemented
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        auth_ctx: Self::AuthContext,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        // TODO: Handle other users except self
        let Some(user) = controller.persistence.get_user(&auth_ctx).await? else {
            return Err(crate::error::ArunaMetadataError::NotFound(
                "User not found".to_string(),
            ));
        };
        Ok(GetUserResponse { user })
    }
}
