use ulid::Ulid;
use super::request::Request;
use crate::{
    models::{
        models::User,
        requests::{AddUserRequest, AddUserResponse},
    },
    network::network_trait::Network,
    persistence::{persistence::Persistor, search::search::Search, storage::store::Store},
};

impl<St, Se, P, N> Request<St, Se, N, P> for AddUserRequest
where
    for<'a> St: Store<'a>,
    Se: Search,
    P: Persistor<St, Se>,
    N: Network,
{
    type Response = AddUserResponse;

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        _user: Option<User>,
        controller: &super::controller::Controller<St, Se, N, P>,
    ) -> Result<Self::Response, crate::error::ArunaError> {
        let user = User {
            id: Ulid::new(),
            name: self.name,
        };
        controller.persistence.add_user(user.clone()).await?;
        Ok(AddUserResponse { user })
    }
}
