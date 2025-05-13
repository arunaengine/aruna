use super::request::Request;
use crate::{
    error::ArunaMetadataError,
    models::{
        models::User,
        requests::{SearchRequest, SearchResponse},
    },
    network::network_trait::Network,
    persistence::search::search::Search,
};
use aruna_storage::storage::store::Store;
use ulid::Ulid;

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for SearchRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = SearchResponse;

    async fn forward_or_return(
        &self,
        user: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: user.clone(),
            request: crate::models::requests::ForwardRequest::Search(self.clone()),
        };

        let self_addr = controller.network.get_addr().await?;
        let nodes = controller.network.get_realm_nodes().await?;

        let mut results = Vec::new();

        for node in nodes {
            if node == self_addr {
                // TODO: Replace this with real authorization
                let user =
                    match user {
                        Some(id) => {
                            controller
                                .persistence
                                .get_user(&Ulid::from_string(&id).map_err(|e| {
                                    ArunaMetadataError::DeserializeError(e.to_string())
                                })?)
                                .await?
                        }
                        None => None,
                    };

                results.append(&mut self.clone().run_request(user, controller).await?.resources);
            } else {
                match controller
                    .network
                    .forward(body.clone(), &Ulid::default(), node.clone())
                    .await?
                {
                    crate::models::requests::ForwardResponse::Search(response) => {
                        results.append(&mut response?.resources);
                    }
                    e @ _ => {
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
        user: Option<User>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let user = user.map(|u| u.id);
        let resources = controller.persistence.search(user, self.query).await?;
        Ok(SearchResponse { resources })
    }
}
