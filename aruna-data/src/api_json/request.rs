use crate::error::ArunaDataError;
use crate::controller::controller::Controller;
use aruna_permission::UserIdentity;
use aruna_storage::storage::store::Store;

#[async_trait::async_trait]
pub trait Request<St>
where
    for<'a> St: Store<'a> + 'static,
{
    type Response;
    async fn run_request(
        self,
        requester: Option<UserIdentity>,
        controller: &Controller<St>,
    ) -> Result<Self::Response, ArunaDataError>;

    async fn forward_or_return(
        &self,
        requester: &Option<String>,
        controller: &Controller<St>,
    ) -> Result<Option<Self::Response>, ArunaDataError>;
}
