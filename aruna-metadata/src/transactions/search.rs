use std::collections::HashSet;

use super::request::Request;
use crate::{
    error::ArunaMetadataError,
    models::{
        structs::Resource,
        requests::{SearchRequest, SearchResponse},
    },
    network::network_trait::Network,
    persistence::search::generic::Search,
};
use aruna_storage::storage::store::Store;
use tracing::error;
use ulid::Ulid;

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for SearchRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = SearchResponse;
    type AuthContext = Option<Vec<Ulid>>; // (Identity, Groups)

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Vec<Ulid>>, crate::error::ArunaMetadataError> {
        if let Some(token) = token {
            let user_identity = controller.get_or_sync_user(token).await?;
            match controller.persistence.get_user_groups(&user_identity).await {
                Ok(groups) => Ok(Some(groups)),
                Err(err) => {
                    error!("{err}");
                    Ok(None)
                }
            }
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

        let mut results: HashSet<Resource, ahash::RandomState> = HashSet::default();

        for node in nodes {
            if node == self_addr {
                let user = self.authorize(token.clone(), controller).await?;
                let result = self.clone().run_request(user, controller).await?;
                for r in result.resources {
                    results.insert(r);
                }
            } else {
                match controller
                    .network
                    .forward(body.clone(), &Ulid::default(), node.clone())
                    .await
                {
                    Ok(crate::models::requests::ForwardResponse::Search(response)) => {
                        let response = match response {
                            Ok(res) => res,
                            Err(err) => {
                                error!("{err}");
                                continue;
                            }
                        };
                        for r in response.resources {
                            results.insert(r);
                        }
                    }
                    e => {
                        error!(?e);
                        return Err(ArunaMetadataError::NetworkError(format!(
                            "Got wrong forward response {e:?}"
                        )));
                    }
                }
            }
        }
        Ok(Some(SearchResponse {
            resources: results.into_iter().collect(),
        }))
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        groups: Option<Vec<Ulid>>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let resources = controller.persistence.search(groups, self.query).await?;
        Ok(SearchResponse { resources })
    }
}
