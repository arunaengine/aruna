use super::request::Request;
use crate::{
    error::ArunaError,
    network::network_trait::Network,
    persistence::{persistence::Persistor, search::search::Search, storage::store::Store},
};
use std::{marker::PhantomData, sync::Arc};
use ulid::Ulid;

pub struct Controller<St, Se, N, P>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    P: Persistor<St, Se> + 'static,
    N: Network<P, St, Se> + 'static,
{
    pub persistence: Arc<P>,
    pub network: Arc<N>,
    phantom_store: PhantomData<St>,
    phantom_search: PhantomData<Se>,
}

impl<St, Se, P, N> Controller<St, Se, N, P>
where
    for<'a> St: Store<'a>,
    Se: Search,
    P: Persistor<St, Se>,
    N: Network<P, St, Se>,
{
    #[tracing::instrument(level = "trace", skip(persistence, network))]
    pub fn new(persistence: Arc<P>, network: N) -> Self {
        Self {
            persistence,
            network: Arc::new(network),
            phantom_store: PhantomData,
            phantom_search: PhantomData,
        }
    }
    #[tracing::instrument(level = "trace", skip(self, request, token))]
    pub async fn request<R: Request<St, Se, N, P>>(
        &self,
        request: R,
        token: Option<String>,
    ) -> Result<R::Response, ArunaError> {
        // TODO: Replace this with real authorization
        let user = match token {
            Some(id) => {
                self.persistence
                    .get_user(
                        &Ulid::from_string(&id)
                            .map_err(|e| ArunaError::DeserializeError(e.to_string()))?,
                    )
                    .await?
            }
            None => None,
        };
        let result = request.run_request(user, self).await?;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn clear(&self) -> Result<(), ArunaError> {
        self.persistence.clear().await
    }
}
