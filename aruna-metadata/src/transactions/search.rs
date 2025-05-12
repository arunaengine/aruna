use super::request::Request;
use crate::{
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
        match controller.network.forward(body, &Ulid::new()).await? {
            Some(result) => match result {
                crate::models::requests::ForwardResponse::Search(response) => Ok(Some(response?)),
                _ => Ok(None),
            },
            None => Ok(None),
        }
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
