use super::request::Request;
use crate::{
    error::ArunaMetadataError,
    models::{
        models::User,
        requests::{AddUserRequest, AddUserResponse},
    },
    network::network_trait::Network,
    persistence::search::search::Search,
};
use ulid::Ulid;
use aruna_storage::storage::store::Store;

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for AddUserRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = AddUserResponse;

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        _user: Option<User>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
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
                    .map_err(|_e| ArunaMetadataError::ConversionError {
                        from: "Vec<u8>".to_string(),
                        to: "&[u8; 32]".to_string(),
                    })?,
                user.clone(),
            )
            .await?;
        Ok(AddUserResponse { user })
    }
}
