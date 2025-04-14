use crate::{
    error::ArunaError,
    models::models::User,
    network::network_trait::Network,
    persistence::{persistence::Persistor, search::search::Search, storage::store::Store},
};
use super::controller::Controller;

pub trait Request<St, Se, N, P>
where
    for<'a> St: Store<'a>,
    Se: Search,
    P: Persistor<St, Se>,
    N: Network,
{
    type Response;
    fn run_request(
        self,
        requester: Option<User>,
        controller: &Controller<St, Se, N, P>,
    ) -> impl std::future::Future<Output = Result<Self::Response, ArunaError>> + Send;
}
