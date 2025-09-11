use crate::logerr;
use crate::models::requests::{GetInfoRequest, GetInfoResponse, Request};
use crate::{
    error::ArunaMetadataError,
    models::{
        requests::{SearchRequest, SearchResponse},
        structs::Resource,
    },
    network::network_trait::Network,
    persistence::search::generic::Search,
};
use aruna_storage::storage::store::Store;
use data_encoding::HEXLOWER;
use std::collections::HashSet;
use tracing::error;
use ulid::Ulid;

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for SearchRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = SearchResponse;
    type AuthContext = Option<Vec<Ulid>>; // (Identity, Groups)

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Vec<Ulid>>, crate::error::ArunaMetadataError> {
        if let Some(token) = token {
            let user_identity = controller.persistence.get_identity(token).await?;
            match controller.persistence.get_user_groups(&user_identity).await {
                Ok(groups) => Ok(Some(groups)),
                Err(err) => {
                    error!("{err}");
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn sync_or_forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        if let Some(token) = token {
            controller
                .sync_user(token.clone())
                .await
                .map_err(logerr!())?;
        }
        Ok(Some(self.forward(token, controller).await?))
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: token.clone(),
            request: crate::models::requests::ForwardRequest::Search(self.clone()),
        };

        let self_addr = controller.network.get_addr().await?;
        let nodes = controller.network.get_realm_nodes().await?;

        let mut results: HashSet<Resource, ahash::RandomState> = HashSet::default();

        for node in nodes {
            if node == self_addr {
                let user = self.authorize(token.clone(), controller).await?;
                let result = self.clone().run_request(user, controller).await?;
                for r in result.resources {
                    results.insert(r);
                }
            } else {
                match controller
                    .network
                    .forward(body.clone(), &[0u8; 32], node.clone())
                    .await
                {
                    Ok(crate::models::requests::ForwardResponse::Search(response)) => {
                        let response = match response {
                            Ok(res) => res,
                            Err(err) => {
                                error!("{err}");
                                continue;
                            }
                        };
                        for r in response.resources {
                            results.insert(r);
                        }
                    }
                    e => {
                        error!(?e);
                        return Err(ArunaMetadataError::NetworkError(format!(
                            "Got wrong forward response {e:?}"
                        )));
                    }
                }
            }
        }
        Ok(SearchResponse {
            resources: results.into_iter().collect(),
        })
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        groups: Option<Vec<Ulid>>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let resources = controller.persistence.search(groups, self.query).await?;
        Ok(SearchResponse { resources })
    }
}

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for GetInfoRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = GetInfoResponse;
    type AuthContext = Option<Vec<Ulid>>; // (Identity, Groups)

    #[tracing::instrument(level = "trace", skip(_controller, _token))]
    async fn authorize(
        &self,
        _token: Option<String>,
        _controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Vec<Ulid>>, crate::error::ArunaMetadataError> {
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(_controller, _token))]
    async fn sync_or_forward(
        &self,
        _token: &Option<String>,
        _controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(_controller))]
    async fn forward(
        &self,
        token: &Option<String>,
        _controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        Err(ArunaMetadataError::NotFound(
            "Forwarding GetInfoRequest is not implemented".to_string(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        _groups: Option<Vec<Ulid>>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let realm_id = HEXLOWER.encode(&controller.network.get_realm_key().await?);
        let node_addr = controller.network.get_addr().await?;
        let node_id = node_addr.node_id.to_string();
        Ok(GetInfoResponse {
            realm_id,
            node_id,
            node_addr: serde_json::to_string(&node_addr)?,
        })
    }
}
