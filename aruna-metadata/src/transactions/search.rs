use super::request::Request;
use crate::{
    error::ArunaMetadataError,
    models::requests::{SearchRequest, SearchResponse},
    network::network_trait::Network,
    persistence::search::search::Search,
};
use aruna_permission::UserIdentity;
use aruna_storage::storage::store::Store;
use tracing::{error, trace};
use ulid::Ulid;

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for SearchRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = SearchResponse;
    type AuthContext = Option<UserIdentity>;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<UserIdentity>, crate::error::ArunaMetadataError> {
        if let Some(token) = token {
            let user_identity = controller.persistence.get_identity(token).await?;
            Ok(Some(user_identity))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn forward_or_return(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: token.clone(),
            request: crate::models::requests::ForwardRequest::Search(self.clone()),
        };

        let self_addr = controller.network.get_addr().await?;
        let nodes = controller.network.get_realm_nodes().await?;

        let mut results = Vec::new();

        for node in nodes {
            if node == self_addr {
                let user = self.authorize(token.clone(), controller).await?;
                results.append(&mut self.clone().run_request(user, controller).await?.resources);
            } else {
                trace!("Asking {node:?} for search");
                match controller
                    .network
                    .forward(body.clone(), &Ulid::default(), node.clone())
                    .await
                {
                    Ok(crate::models::requests::ForwardResponse::Search(response)) => {
                        results.append(&mut response?.resources);
                    }
                    e @ _ => {
                        error!(?e);
                        return Err(ArunaMetadataError::NetworkError(format!(
                            "Got wrong forward response {e:?}"
                        )));
                    }
                }
            }
        }
        Ok(Some(SearchResponse { resources: results }))
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<UserIdentity>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let user = user.map(|u| u.user_ulid);
        let resources = controller.persistence.search(user, self.query).await?;
        Ok(SearchResponse { resources })
    }
}
