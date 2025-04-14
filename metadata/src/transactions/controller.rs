use std::{marker::PhantomData, sync::Arc};
use ulid::Ulid;
use super::request::Request;
use crate::{
    error::ArunaError, network::network_trait::Network, persistence::{persistence::Persistor, search::search::Search, storage::store::Store}
};

pub struct Controller<St, Se, N, P>
where
    for<'a> St: Store<'a>,
    Se: Search,
    P: Persistor<St, Se>,
    N: Network,
{
    pub persistence: Arc<P>,
    pub _network: Arc<N>,
    phantom_store: PhantomData<St>,
    phantom_search: PhantomData<Se>,
}

impl<St, Se, P, N> Controller<St, Se, N, P>
where
    for<'a> St: Store<'a>,
    Se: Search,
    P: Persistor<St, Se>,
    N: Network,
{
    #[tracing::instrument(level = "trace", skip(persistence, network))]
    pub fn new(persistence: P, network: N) -> Self {
        Self {
            persistence: Arc::new(persistence),
            _network: Arc::new(network),
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

