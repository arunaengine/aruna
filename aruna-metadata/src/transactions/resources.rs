use super::request::Request;
use crate::{
    error::ArunaError,
    models::{
        models::{Resource, User},
        requests::{
            CreateResourceRequest, CreateResourceResponse, GetInner, GetResourcesRequest,
            GetResourcesResponse, ResourceUpdateRequests, ResourceUpdateResponses,
            UpdateResourceAuthorsResponse, UpdateResourceDescriptionResponse,
            UpdateResourceIdentifiersResponse, UpdateResourceLabelsResponse,
            UpdateResourceLicenseResponse, UpdateResourceNameResponse, UpdateResourceTitleResponse,
            UpdateResourceVisibilityResponse,
        },
    },
    network::network_trait::Network,
    persistence::{persistence::Authorize, search::search::Search, storage::store::Store},
};
use ulid::Ulid;

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for CreateResourceRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = CreateResourceResponse;
    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<User>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaError> {
        let Some(user) = user else {
            return Err(crate::error::ArunaError::Unauthorized);
        };
        if !controller.persistence.authorize(&user.id, &self.parent_id) {
            return Err(crate::error::ArunaError::Unauthorized);
        };

        let time = chrono::Utc::now().timestamp_millis();
        let time = chrono::DateTime::from_timestamp_millis(time).ok_or_else(|| {
            ArunaError::ConversionError {
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
            license_id: self.license_id.unwrap_or(Ulid::default()),
            locked: false,
            deleted: false,
            location: Vec::new(),
            hashes: Vec::new(),
        };
        let doc = controller
            .persistence
            .add_resource(
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
                &user.id,
                resource.clone(),
            )
            .await?;

        Ok(CreateResourceResponse { resource })
    }
}

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for GetResourcesRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = GetResourcesResponse;

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<User>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaError> {
        let Some(user) = user else {
            return Err(crate::error::ArunaError::Unauthorized);
        };
        for id in &self.ids {
            if !controller.persistence.authorize(&user.id, id) {
                return Err(crate::error::ArunaError::Unauthorized);
            };
        }
        let persistor = controller.persistence.clone();
        let res = persistor.get_resources(self.ids).await?;
        Ok(GetResourcesResponse { resources: res })
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
    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: Option<User>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaError> {
        let Some(user) = user else {
            return Err(crate::error::ArunaError::Unauthorized);
        };
        if !controller.persistence.authorize(&user.id, &self.get_id()) {
            return Err(crate::error::ArunaError::Unauthorized);
        };

        let id = self.get_id();
        let updated = chrono::Utc::now();

        let mut resource = controller
            .persistence
            .get_resources(vec![id])
            .await?
            .get(0)
            .cloned()
            .ok_or_else(|| ArunaError::NotFound(format!("Resource {} not found", id)))?;
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
                    resource.labels = resource
                        .labels
                        .into_iter()
                        .filter(|kv| !req.labels_to_remove.contains(kv))
                        .collect();
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
                    resource.identifiers = resource
                        .identifiers
                        .into_iter()
                        .filter(|id| !req.ids_to_remove.contains(id))
                        .collect();
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
                    resource.authors = resource
                        .authors
                        .into_iter()
                        .filter(|a| !req.authors_to_remove.contains(a))
                        .collect();
                }
                if !req.authors_to_add.is_empty() {
                    resource.authors.extend(req.authors_to_add);
                }
                ResourceUpdateResponses::Authors(UpdateResourceAuthorsResponse {
                    resource: resource.clone(),
                })
            }
        };

        let doc = controller
            .persistence
            .update_resource(
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
                &user.id,
                resource.clone(),
            )
            .await?;

        Ok(response)
    }
}
