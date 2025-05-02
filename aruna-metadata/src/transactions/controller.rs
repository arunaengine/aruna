use super::request::Request;
use crate::{
    error::ArunaMetadataError,
    network::network_trait::Network,
    persistence::{persistence::Persistor, search::search::Search},
};
use std::sync::Arc;
use ulid::Ulid;
use aruna_storage::storage::store::Store;

pub struct Controller<St, Se, N>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    pub network: Arc<N>,
    pub persistence: Arc<Persistor<St, Se>>,
}

impl<St, Se, N> Controller<St, Se, N>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    #[tracing::instrument(level = "trace", skip(persistence, network))]
    pub fn new(persistence: Arc<Persistor<St, Se>>, network: N) -> Self {
        Self {
            persistence,
            network: Arc::new(network),
        }
    }
    #[tracing::instrument(level = "trace", skip(self, request, token))]
    pub async fn request<R: Request<St, Se, N>>(
        &self,
        request: R,
        token: Option<String>,
    ) -> Result<R::Response, ArunaMetadataError> {
        // TODO: Replace this with real authorization
        let user = match token {
            Some(id) => {
                self.persistence
                    .get_user(
                        &Ulid::from_string(&id)
                            .map_err(|e| ArunaMetadataError::DeserializeError(e.to_string()))?,
                    )
                    .await?
            }
            None => None,
        };
        let result = request.run_request(user, self).await?;
        Ok(result)
    }
}
