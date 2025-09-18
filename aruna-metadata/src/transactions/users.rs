use crate::error::ArunaMetadataError;
use crate::logerr;
use crate::models::requests::{ForwardResponse, Request};
use crate::{
    models::requests::{
        AddUserRequest, AddUserResponse, CreateTokenRequest, CreateTokenResponse, GetUserRequest,
        GetUserResponse,
    },
    network::network_trait::Network,
    persistence::search::generic::Search,
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

    #[tracing::instrument(level = "trace", skip(_controller, _token))]
    async fn sync_or_forward(
        &self,
        _token: &Option<String>,
        _controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: token.clone(),
            request: crate::models::requests::ForwardRequest::AddUser(self.clone()),
        };

        let nodes = controller.network.get_realm_nodes().await?;
        let self_addr = controller.network.get_addr().await?;
        let mut nodes = nodes
            .iter()
            .filter(|node_addr| node_addr.node_id != self_addr.node_id);

        let Some(first_node) = nodes.next() else {
            return Err(crate::error::ArunaMetadataError::NetworkError(
                "No node to forward request to found".to_string(),
            ));
        };

        match controller
            .network
            .forward(body, &[0u8; 32], first_node.clone())
            .await?
        {
            ForwardResponse::AddUser(response) => Ok(response?),
            e => Err(crate::error::ArunaMetadataError::NetworkError(format!(
                "Got wrong forward response {e:?}"
            ))),
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        auth_ctx: Self::AuthContext,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let node_id = controller.network.get_addr().await?.node_id;
        let realm_key = controller.network.get_realm_key().await?;
        let (user, doc) = controller
            .persistence
            .add_user(node_id.as_bytes(), &realm_key, self.name, auth_ctx)
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
                crate::models::structs::TypedDoc::User(doc),
                *subject_hash,
                user.id.to_bytes(),
                path,
                members,
                false,
            )
            .await?;
        let token = controller.persistence.create_token(&user.id, None).await?;

        Ok(AddUserResponse {
            user_id: user.id.to_string(),
            user,
            token,
        })
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
        let identity = controller.persistence.get_identity(token).await?;
        if identity != self.id {
            // TODO: Querying users that are not self is currently unimplemented
            Err(crate::error::ArunaMetadataError::Unauthorized)
        } else {
            Ok(identity)
        }
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn sync_or_forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        let identity = controller
            .persistence
            .get_identity(
                token
                    .clone()
                    .ok_or_else(|| ArunaMetadataError::Unauthorized)?,
            )
            .await
            .map_err(logerr!())?;
        if identity.realm_key != controller.network.get_realm_key().await? {
            Ok(Some(self.forward(token, controller).await?))
        } else {
            controller
                .sync_user(
                    token
                        .clone()
                        .ok_or_else(|| ArunaMetadataError::Unauthorized)?,
                )
                .await?;
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: token.clone(),
            request: crate::models::requests::ForwardRequest::GetUser(self.clone()),
        };

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(&self.id.to_bytes());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();

        let nodes = controller.network.find(subject_hash).await?;
        let self_addr = controller.network.get_addr().await?;
        let mut nodes = nodes
            .iter()
            .filter(|node_addr| node_addr.node_id != self_addr.node_id);

        let Some(first_node) = nodes.next() else {
            return Err(crate::error::ArunaMetadataError::NetworkError(
                "No node to forward request to found".to_string(),
            ));
        };

        match controller
            .network
            .forward(body, subject_hash, first_node.clone())
            .await?
        {
            ForwardResponse::GetUser(response) => Ok(response?),
            e => Err(crate::error::ArunaMetadataError::NetworkError(format!(
                "Got wrong forward response {e:?}"
            ))),
        }
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

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for CreateTokenRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = CreateTokenResponse;
    type AuthContext = UserIdentity;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::AuthContext, crate::error::ArunaMetadataError> {
        let Some(token) = token else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        };
        let identity = controller.persistence.get_identity(token).await?;
        if identity.realm_key != controller.network.get_realm_key().await? {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        } else {
            Ok(identity)
        }
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn sync_or_forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        let identity = controller
            .persistence
            .get_identity(
                token
                    .clone()
                    .ok_or_else(|| ArunaMetadataError::Unauthorized)?,
            )
            .await
            .map_err(logerr!())?;
        if identity.realm_key != controller.network.get_realm_key().await? {
            Ok(Some(self.forward(token, controller).await?))
        } else {
            controller
                .sync_user(
                    token
                        .clone()
                        .ok_or_else(|| ArunaMetadataError::Unauthorized)?,
                )
                .await?;
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let identity = controller
            .persistence
            .get_identity(
                token
                    .clone()
                    .ok_or_else(|| ArunaMetadataError::Unauthorized)?,
            )
            .await
            .map_err(logerr!())?;
        let body = crate::network::network_trait::Body::Request {
            token: token.clone(),
            request: crate::models::requests::ForwardRequest::CreateToken(self.clone()),
        };

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(&identity.to_bytes());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();

        let nodes = controller.network.find(subject_hash).await?;
        let self_addr = controller.network.get_addr().await?;
        let mut nodes = nodes
            .iter()
            .filter(|node_addr| node_addr.node_id != self_addr.node_id);

        let Some(first_node) = nodes.next() else {
            return Err(crate::error::ArunaMetadataError::NetworkError(
                "No node to forward request to found".to_string(),
            ));
        };

        match controller
            .network
            .forward(body, subject_hash, first_node.clone())
            .await?
        {
            ForwardResponse::CreateToken(response) => Ok(response?),
            e => Err(crate::error::ArunaMetadataError::NetworkError(format!(
                "Got wrong forward response {e:?}"
            ))),
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        auth_ctx: Self::AuthContext,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let token = controller
            .persistence
            .create_token(&auth_ctx, self.expiration_hours)
            .await?;
        Ok(CreateTokenResponse { token })
    }
}
