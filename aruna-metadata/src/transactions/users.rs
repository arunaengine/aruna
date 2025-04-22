use super::request::Request;
use crate::{
    error::ArunaError, models::{
        models::User,
        requests::{AddUserRequest, AddUserResponse},
    }, network::network_trait::Network, persistence::{persistence::Persistor, search::search::Search, storage::store::Store}
};
use ulid::Ulid;

impl<St, Se, P, N> Request<St, Se, N, P> for AddUserRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    P: Persistor<St, Se> + 'static,
    N: Network<P, St, Se> + 'static,
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
        controller
            .persistence
            .add_user(
                controller
                    .network
                    .get_id()
                    .await?
                    .as_slice()
                    .try_into()
                    .map_err(|_e| ArunaError::ConversionError {
                        from: "Vec<u8>".to_string(),
                        to: "&[u8; 32]".to_string(),
                    })?,
                user.clone(),
            )
            .await?;
        Ok(AddUserResponse { user })
    }
}
