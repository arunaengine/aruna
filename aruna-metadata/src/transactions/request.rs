use super::controller::Controller;
use crate::{
    error::ArunaMetadataError, network::network_trait::Network, persistence::search::search::Search,
};
use aruna_storage::storage::store::Store;


#[async_trait::async_trait]
pub trait Request<St, Se, N>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response;
    type AuthContext;

    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::AuthContext, crate::error::ArunaMetadataError>;

    async fn run_request(
        self,
        auth_result: Self::AuthContext,
        controller: &Controller<St, Se, N>,
    ) -> Result<Self::Response, ArunaMetadataError>;

    async fn forward_or_return(
        &self,
        token: &Option<String>,
        controller: &Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, ArunaMetadataError>;
}
