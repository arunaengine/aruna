use super::controller::Controller;
use crate::{
    error::ArunaError,
    models::models::User,
    network::network_trait::Network,
    persistence::{search::search::Search, storage::store::Store},
};

#[async_trait::async_trait]
pub trait Request<St, Se, N>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response;
    async fn run_request(
        self,
        requester: Option<User>,
        controller: &Controller<St, Se, N>,
    ) -> Result<Self::Response, ArunaError>;
}
