use crate::{
    models::{
        models::User,
        requests::{SearchRequest, SearchResponse},
    },
    network::network_trait::Network,
    persistence::{persistence::Persistor, search::search::Search, storage::store::Store},
};
use super::request::Request;

impl<St, Se, P, N> Request<St, Se, N, P> for SearchRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    P: Persistor<St, Se> + 'static,
    N: Network<P, St, Se> + 'static,
{
    type Response = SearchResponse;

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<User>,
        controller: &super::controller::Controller<St, Se, N, P>,
    ) -> Result<Self::Response, crate::error::ArunaError> {
        let user = user.map(|u| u.id);
        let resources = controller.persistence.search(user, self.query).await?;
        Ok(SearchResponse { resources })
    }
}
