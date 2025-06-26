use crate::api_json::request::{Request, User};
use crate::{IOHandler, error::ArunaDataError};
use aruna_permission::manager::PermissionManager;
use aruna_permission::{TokenSystem, UserIdentity};
use aruna_storage::storage::store::Store;
use parking_lot::RwLock;
use std::sync::Arc;
use ulid::Ulid;

pub struct Controller<St>
where
    for<'a> St: Store<'a> + 'static,
{
    pub io_handler: Arc<IOHandler<St>>,
    pub permission_manager: PermissionManager,
    pub token_handler: Arc<RwLock<TokenSystem>>,
}

impl<St> Controller<St>
where
    for<'a> St: Store<'a> + 'static,
{
    #[tracing::instrument(level = "trace", skip(io_handler, permission_manager, token_handler))]
    pub fn new(
        io_handler: Arc<IOHandler<St>>,
        permission_manager: PermissionManager,
        token_handler: Arc<RwLock<TokenSystem>>,
    ) -> Self {
        let controller = Self {
            io_handler,
            permission_manager,
            token_handler,
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
                let user_identity = match token {
                    Some(token) => Some(
                        self.get_identity(token)
                            .await
                            .map_err(|_| ArunaDataError::Unauthorized)?,
                    ),
                    None => None,
                };

                let result = request.run_request(user_identity, self).await?;
                Ok(result)
            }
        }
    }

    pub async fn get_identity(&self, token: String) -> anyhow::Result<UserIdentity> {
        let store = self.io_handler.store.clone();
        let token_handler = self.token_handler.clone();
        // TODO: Query user from kademlia
        tokio::task::spawn_blocking(move || -> anyhow::Result<UserIdentity> {
            let txn = store.create_txn(false)?;
            let user_identity = token_handler
                .read()
                .get_identity(&token, store.as_ref(), &txn)?;
            store.commit(txn)?;
            Ok(user_identity)
        })
        .await?
    }
}
