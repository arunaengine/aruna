use crate::api_json::request::{Request, User};
use crate::{IOHandler, error::ArunaDataError};
use aruna_storage::storage::store::Store;
use std::sync::Arc;

pub struct Controller<St>
where
    for<'a> St: Store<'a> + 'static,
{
    pub io_handler: Arc<IOHandler<St>>,
}

impl<St> Controller<St>
where
    for<'a> St: Store<'a> + 'static,
{
    #[tracing::instrument(level = "trace", skip(io_handler))]
    pub fn new(io_handler: Arc<IOHandler<St>>) -> Self {
        let controller = Self { io_handler };
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
                // TODO: Replace this with real authentication
                let user = match token {
                    Some(_id) => {
                        /*TODO: Proper authentication
                        self.io_handler
                            .store
                            .get_user(
                                &Ulid::from_string(&id)
                                    .map_err(|e| ArunaDataError::DeserializeError(e.to_string()))?,
                            )
                            .await?
                        */
                        Some(User {
                            id: Default::default(),
                            name: "".to_string(),
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
