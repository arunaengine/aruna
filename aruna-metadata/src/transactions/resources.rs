use super::request::Request;
use crate::{
    error::ArunaMetadataError,
    models::{
        models::Resource,
        requests::{
            CreateResourceRequest, CreateResourceResponse, GetInner, GetResourceRequest,
            GetResourceResponse, ResourceUpdateRequests, ResourceUpdateResponses,
            UpdateResourceAuthorsResponse, UpdateResourceDescriptionResponse,
            UpdateResourceIdentifiersResponse, UpdateResourceLabelsResponse,
            UpdateResourceLicenseResponse, UpdateResourceNameResponse, UpdateResourceTitleResponse,
            UpdateResourceVisibilityResponse,
        },
    },
    network::network_trait::{Network, REPLICATION_POLICY},
    persistence::{persistence::Authorize, search::search::Search},
};
use aruna_permission::{Action, UserIdentity};
use aruna_storage::storage::store::Store;
use rand::seq::IteratorRandom;
use ulid::Ulid;

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for CreateResourceRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = CreateResourceResponse;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<UserIdentity>, crate::error::ArunaMetadataError> {
        let (action, id) = (Action::Write, self.parent_id);
        if let Some((i, p)) = controller.persistence.authorize(token, action, id).await? {
            Ok(Some(i))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(_controller))]
    async fn forward_or_return(
        &self,
        user: &Option<String>,
        _controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<UserIdentity>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let Some(user) = user else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        };

        let time = chrono::Utc::now().timestamp_millis();
        let time = chrono::DateTime::from_timestamp_millis(time).ok_or_else(|| {
            ArunaMetadataError::ConversionError {
                from: "i64".to_string(),
                to: "Chrono::DateTime".to_string(),
            }
        })?;
        let resource = Resource {
            id: Ulid::new(),
            name: self.name,
            title: self.title,
            description: self.description,
            revision: 0,
            variant: self.variant,
            labels: self.labels,
            identifiers: self.identifiers,
            content_len: 0,
            count: 0,
            visibility: self.visibility,
            created_at: time,
            last_modified: time,
            authors: self.authors,
            license_id: self.license_id.unwrap_or_default(),
            locked: false,
            deleted: false,
            location: Vec::new(),
            hashes: Vec::new(),
        };
        let node_id = controller.network.get_addr().await?.node_id;
        let doc = controller
            .persistence
            .add_resource(node_id.as_bytes(), &user.user_ulid, resource.clone())
            .await?;

        // Choose x = REPLICATION_POLICY random nodes of members
        // and replicate resource
        let members = controller.network.get_realm_nodes().await?;
        let realm_nodes = members
            .into_iter()
            .filter(|addr| addr.node_id != node_id)
            .choose_multiple(&mut rand::rngs::OsRng, REPLICATION_POLICY);

        controller
            .sync_loop(
                crate::models::models::TypedDoc::Resource(doc),
                resource.id,
                realm_nodes.into_iter(),
            )
            .await?;

        Ok(CreateResourceResponse { resource })
    }
}

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for GetResourceRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = GetResourceResponse;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<UserIdentity>, crate::error::ArunaMetadataError> {
        let (action, id) = (Action::Read, self.id);
        if let Some((i, p)) = controller.persistence.authorize(token, action, id).await? {
            Ok(Some(i))
        } else {
            Ok(None)
        }
    }

    async fn forward_or_return(
        &self,
        user: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: user.clone(),
            request: crate::models::requests::ForwardRequest::GetResource(self.clone()),
        };
        let self_addr = controller.network.get_addr().await?;
        let nodes = controller.network.find(&self.id).await?;

        if nodes.is_empty() || nodes.contains(&self_addr) {
            Ok(None)
        } else {
            let Some(first_node) = nodes.first() else {
                return Ok(None);
            };
            match controller
                .network
                .forward(body, &self.id, first_node.clone())
                .await?
            {
                crate::models::requests::ForwardResponse::GetResource(response) => {
                    Ok(Some(response?))
                }
                e @ _ => Err(ArunaMetadataError::NetworkError(format!(
                    "Got wrong forward response {e:?}"
                ))),
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<UserIdentity>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let Some(user) = user else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        };
        let persistor = controller.persistence.clone();
        let resource = persistor.get_resource(self.id).await?;
        Ok(GetResourceResponse { resource })
    }
}

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for ResourceUpdateRequests
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = ResourceUpdateResponses;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<UserIdentity>, crate::error::ArunaMetadataError> {
        let (action, id) = (Action::Write, self.get_id());
        if let Some((i, p)) = controller.persistence.authorize(token, action, id).await? {
            Ok(Some(i))
        } else {
            Ok(None)
        }
    }

    async fn forward_or_return(
        &self,
        user: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: user.clone(),
            request: crate::models::requests::ForwardRequest::UpdateResource(self.clone()),
        };
        let self_addr = controller.network.get_addr().await?;
        let nodes = controller.network.find(&self.get_id()).await?;

        if nodes.is_empty() || nodes.contains(&self_addr) {
            Ok(None)
        } else {
            let Some(first_node) = nodes.first() else {
                return Ok(None);
            };
            match controller
                .network
                .forward(body, &self.get_id(), first_node.clone())
                .await?
            {
                crate::models::requests::ForwardResponse::UpdateResource(response) => {
                    Ok(Some(response?))
                }
                e @ _ => Err(ArunaMetadataError::NetworkError(format!(
                    "Got wrong forward response {e:?}"
                ))),
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<UserIdentity>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let Some(user) = user else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        };

        let id = self.get_id();
        let updated = chrono::Utc::now();

        let mut resource = controller.persistence.get_resource(id).await?;
        resource.last_modified = updated;

        let response = match self {
            ResourceUpdateRequests::Name(req) => {
                resource.name = req.name;
                ResourceUpdateResponses::Name(UpdateResourceNameResponse {
                    resource: resource.clone(),
                })
            }
            ResourceUpdateRequests::Title(req) => {
                resource.title = req.title;
                ResourceUpdateResponses::Title(UpdateResourceTitleResponse {
                    resource: resource.clone(),
                })
            }
            ResourceUpdateRequests::Description(req) => {
                resource.description = req.description;
                ResourceUpdateResponses::Description(UpdateResourceDescriptionResponse {
                    resource: resource.clone(),
                })
            }
            ResourceUpdateRequests::Visibility(req) => {
                resource.visibility = req.visibility;
                ResourceUpdateResponses::Visibility(UpdateResourceVisibilityResponse {
                    resource: resource.clone(),
                })
            }
            ResourceUpdateRequests::License(req) => {
                resource.license_id = req.license_id;
                ResourceUpdateResponses::License(UpdateResourceLicenseResponse {
                    resource: resource.clone(),
                })
            }
            ResourceUpdateRequests::Labels(req) => {
                if !req.labels_to_remove.is_empty() {
                    resource
                        .labels
                        .retain(|kv| !req.labels_to_remove.contains(kv));
                }
                if !req.labels_to_add.is_empty() {
                    resource.labels.extend(req.labels_to_add);
                }
                ResourceUpdateResponses::Labels(UpdateResourceLabelsResponse {
                    resource: resource.clone(),
                })
            }
            ResourceUpdateRequests::Identifiers(req) => {
                if !req.ids_to_remove.is_empty() {
                    resource
                        .identifiers
                        .retain(|id| !req.ids_to_remove.contains(id));
                }
                if !req.ids_to_add.is_empty() {
                    resource.identifiers.extend(req.ids_to_add);
                }
                ResourceUpdateResponses::Identifiers(UpdateResourceIdentifiersResponse {
                    resource: resource.clone(),
                })
            }
            ResourceUpdateRequests::Authors(req) => {
                if !req.authors_to_remove.is_empty() {
                    resource
                        .authors
                        .retain(|a| !req.authors_to_remove.contains(a));
                }
                if !req.authors_to_add.is_empty() {
                    resource.authors.extend(req.authors_to_add);
                }
                ResourceUpdateResponses::Authors(UpdateResourceAuthorsResponse {
                    resource: resource.clone(),
                })
            }
        };

        let node_id = controller.network.get_addr().await?.node_id;
        let doc = controller
            .persistence
            .update_resource(node_id.as_bytes(), &user.user_ulid, resource.clone())
            .await?;

        // Replay update only to members that already got the object
        // In this case replicate functions as replay
        let members = controller
            .network
            .find_verified(&id)
            .await?
            .into_iter()
            .filter(|addr| addr.node_id != node_id);

        controller
            .sync_loop(
                crate::models::models::TypedDoc::Resource(doc),
                resource.id,
                members,
            )
            .await?;

        Ok(response)
    }
}
