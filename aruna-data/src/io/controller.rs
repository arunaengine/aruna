use crate::api_json::request::{Request, User};
use crate::{IOHandler, error::ArunaDataError};
use aruna_permission::manager::PermissionManager;
use aruna_storage::storage::store::Store;
use parking_lot::RwLock;
use std::sync::Arc;
use ulid::Ulid;

pub struct Controller<St>
where
    for<'a> St: Store<'a> + 'static,
{
    pub io_handler: Arc<IOHandler<St>>,
    pub permission_manager: Arc<RwLock<PermissionManager>>,
}

impl<St> Controller<St>
where
    for<'a> St: Store<'a> + 'static,
{
    #[tracing::instrument(level = "trace", skip(io_handler, permission_manager))]
    pub fn new(
        io_handler: Arc<IOHandler<St>>,
        permission_manager: Arc<RwLock<PermissionManager>>,
    ) -> Self {
        let controller = Self {
            io_handler,
            permission_manager,
        };
        controller
    }
    #[tracing::instrument(level = "trace", skip(self, request, token))]
    pub async fn request<R: Request<St>>(
        &self,
        request: R,
        token: Option<String>,
    ) -> Result<R::Response, ArunaDataError> {
        match request.forward_or_return(&token, self).await? {
            Some(response) => Ok(response),
            None => {
                let user = match token {
                    Some(_token) => {
                        //TODO: Validate token signature
                        //let (user, group_id) = self.permission_manager.validate_token(&token).await?;

                        //TODO: Properly fetch user info from store
                        Some(User {
                            id: Ulid::from_string("01JWB4X5TY0K776QDDCHGK3KT2")?,
                            group: Ulid::from_string("01JWB4XFCRJX53Q839QMHPGSXH")?,
                            name: "John Doe".to_string(),
                        })
                    }
                    None => None,
                };

                let result = request.run_request(user, self).await?;
                Ok(result)
            }
        }
    }
}
