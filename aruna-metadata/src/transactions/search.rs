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

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for SearchRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = SearchResponse;

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
