use super::request::Request;
use crate::{
    models::
        requests::{AddUserRequest, AddUserResponse}
    ,
    network::network_trait::Network,
    persistence::search::search::Search,
};
use aruna_permission::{OidcToken, Path};
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
        controller.persistence.check_oidc_token(token).await
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
            .sync_loop(crate::models::models::TypedDoc::User(doc), user.id.user_ulid, path, members)
            .await?;

        Ok(AddUserResponse { user })
    }
}
