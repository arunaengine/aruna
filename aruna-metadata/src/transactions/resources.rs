use crate::logerr;
use crate::models::requests::{
    CreateResourceRequest, CreateResourceResponse, ForwardResponse, Request,
};
use crate::{
    error::ArunaMetadataError,
    models::{
        conversions::ToBytes,
        requests::{
            CreateProjectRequest, CreateProjectResponse, GetInner, GetResourceRequest,
            GetResourceResponse, ResourceUpdateRequests, ResourceUpdateResponses,
            UpdateResourceAuthorsResponse, UpdateResourceDescriptionResponse,
            UpdateResourceIdentifiersResponse, UpdateResourceLabelsResponse,
            UpdateResourceLicenseResponse, UpdateResourceNameResponse, UpdateResourceTitleResponse,
            UpdateResourceVisibilityResponse,
        },
        structs::{Resource, ResourceVariant},
    },
    network::network_trait::{Network, REPLICATION_POLICY},
    persistence::{authorization::Authorize, search::generic::Search},
};
use aruna_permission::paths::PathComponent;
use aruna_permission::{Action, Path, UserIdentity, paths::PathBuilder};
use aruna_storage::storage::store::Store;
use rand::seq::IteratorRandom;
use tracing::{error, trace};
use ulid::Ulid;

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for CreateResourceRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = CreateResourceResponse;
    type AuthContext = (UserIdentity, Path);

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::AuthContext, crate::error::ArunaMetadataError> {
        let (action, id) = (Action::Write, self.parent_id);
        let identity = match token {
            Some(token) => Some(controller.persistence.get_identity(token).await?),
            None => None,
        };
        controller
            .persistence
            .authorize(identity, action, id)
            .await?
            .ok_or_else(|| ArunaMetadataError::Unauthorized)
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn sync_or_forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(self.parent_id.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();
        let nodes = controller.network.find_verified(subject_hash).await?;

        if nodes.is_empty() {
            return Err(ArunaMetadataError::NotFound(format!(
                "Parent with id {} not found",
                self.parent_id
            )));
        }
        let self_addr = controller.network.get_addr().await?;
        if !nodes.contains(&self_addr) {
            let node = nodes
                .iter()
                .filter(|addr| addr != &&self_addr)
                .next()
                .ok_or_else(|| {
                    ArunaMetadataError::NotFound(format!(
                        "Parent with id {} not found",
                        self.parent_id
                    ))
                })?;

            let response = controller
                .network
                .authorize(
                    subject_hash,
                    token.clone(),
                    Action::Write,
                    self.parent_id,
                    node.clone(),
                )
                .await?;

            trace!(?response);

            match response {
                crate::network::network_trait::AuthorizeResponse::Path(path) => {
                    let group_id = path
                        .components()
                        .iter()
                        .find_map(|component| match component {
                            PathComponent::GroupId(id) => Some(id),
                            _ => None,
                        })
                        .ok_or_else(|| {
                            ArunaMetadataError::ServerError("Group not found in path".to_string())
                        })?;
                    controller
                        .sync_user(
                            token
                                .clone()
                                .ok_or_else(|| ArunaMetadataError::Unauthorized)?,
                        )
                        .await
                        .map_err(logerr!())?;
                    controller.sync_group(*group_id).await.map_err(logerr!())?;
                    controller
                        .persistence
                        .add_path(&self.parent_id, path)
                        .await?
                }
                crate::network::network_trait::AuthorizeResponse::Public => {
                    // This should not happen, because Action::Write should never result in a Public
                    // response
                    return Err(ArunaMetadataError::Unauthorized);
                }
                crate::network::network_trait::AuthorizeResponse::Error(aruna_metadata_error) => {
                    return Err(aruna_metadata_error);
                }
            }
        }
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: token.clone(),
            request: crate::models::requests::ForwardRequest::CreateResource(self.clone()),
        };
        let self_addr = controller.network.get_addr().await?;

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(self.parent_id.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();

        let nodes = controller.network.find(subject_hash).await?;
        if nodes.is_empty() {
            return Err(ArunaMetadataError::NotFound(format!(
                "Parent with id {} not found",
                self.parent_id
            )));
        };

        let mut nodes = nodes
            .iter()
            .filter(|addr| addr.node_id != self_addr.node_id);
        let Some(first_node) = nodes.next() else {
            return Err(ArunaMetadataError::NotFound(format!(
                "Parent with id {} not found",
                self.parent_id
            )));
        };

        match controller
            .network
            .forward(body, subject_hash, first_node.clone())
            .await?
        {
            crate::models::requests::ForwardResponse::CreateResource(response) => Ok(response?),
            e => Err(ArunaMetadataError::NetworkError(format!(
                "Got wrong forward response {e:?}"
            ))),
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        auth_result: Self::AuthContext,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let user = auth_result.0;

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
            variant: match self.variant {
                crate::models::requests::CreateResourceVariant::Folder => ResourceVariant::Folder,
                crate::models::requests::CreateResourceVariant::Object => ResourceVariant::Object,
            },
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

        let path = PathBuilder::from_path(auth_result.1)
            .group_metadata_resource(vec![resource.id])
            .build()
            .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))?;
        let node_id = controller.network.get_addr().await?.node_id;
        let doc = controller
            .persistence
            .add_resource(node_id.as_bytes(), &user, path.clone(), resource.clone())
            .await?;

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(resource.id.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();
        controller.network.store(subject_hash).await?;

        // Choose x = REPLICATION_POLICY random nodes of members
        // and replicate resource
        let members = controller.network.get_realm_nodes().await?;
        let realm_nodes = members
            .into_iter()
            .filter(|addr| addr.node_id != node_id)
            .choose_multiple(&mut rand::rngs::OsRng, REPLICATION_POLICY);

        controller
            .sync_loop(
                crate::models::structs::TypedDoc::Resource(doc),
                *subject_hash,
                resource.id.to_bytes().to_vec(),
                path,
                realm_nodes.into_iter(),
                false,
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
    type AuthContext = Option<UserIdentity>;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<UserIdentity>, crate::error::ArunaMetadataError> {
        let (action, id) = (Action::Read, self.id);

        let identity = match token {
            Some(token) => Some(controller.persistence.get_identity(token).await?),
            None => None,
        };
        if let Some((i, _)) = controller
            .persistence
            .authorize(identity, action, id)
            .await?
        {
            Ok(Some(i))
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
        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(self.id.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();

        let self_addr = controller.network.get_addr().await?;
        let nodes = controller.network.find(subject_hash).await?;
        if nodes.contains(&self_addr) {
            if let Some(token) = token {
                controller
                    .sync_user(token.clone())
                    .await
                    .map_err(logerr!())?;
            }
            Ok(None)
        } else {
            Ok(Some(self.forward(token, controller).await?))
        }
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: token.clone(),
            request: crate::models::requests::ForwardRequest::GetResource(self.clone()),
        };
        let self_addr = controller.network.get_addr().await?;

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(self.id.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();

        let nodes = controller.network.find(subject_hash).await?;
        if nodes.is_empty() {
            return Err(crate::error::ArunaMetadataError::NetworkError(
                "No node to forward request to found".to_string(),
            ));
        }
        let mut nodes = nodes
            .iter()
            .filter(|addr| addr.node_id != self_addr.node_id);

        let Some(first_node) = nodes.next() else {
            return Err(crate::error::ArunaMetadataError::NetworkError(
                "No node to forward request to found".to_string(),
            ));
        };
        match controller
            .network
            .forward(body, subject_hash, first_node.clone())
            .await?
        {
            crate::models::requests::ForwardResponse::GetResource(response) => Ok(response?),
            e => Err(ArunaMetadataError::NetworkError(format!(
                "Got wrong forward response {e:?}"
            ))),
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        _user: Option<UserIdentity>, // authorize checks if resource is pub
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
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
    type AuthContext = (UserIdentity, Path);

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<(UserIdentity, Path), crate::error::ArunaMetadataError> {
        let (action, id) = (Action::Write, self.get_id());

        let identity = match token {
            Some(token) => Some(controller.persistence.get_identity(token).await?),
            None => None,
        };
        Ok(controller
            .persistence
            .authorize(identity, action, id)
            .await?
            .ok_or_else(|| ArunaMetadataError::Unauthorized)?)
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn sync_or_forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(self.get_id().to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();

        let self_addr = controller.network.get_addr().await?;
        let nodes = controller.network.find(subject_hash).await?;
        if nodes.contains(&self_addr) {
            controller
                .sync_user(
                    token
                        .clone()
                        .ok_or_else(|| ArunaMetadataError::Unauthorized)?,
                )
                .await
                .map_err(logerr!())?;
            Ok(None)
        } else {
            Ok(Some(self.forward(token, controller).await?))
        }
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: token.clone(),
            request: crate::models::requests::ForwardRequest::UpdateResource(self.clone()),
        };
        let self_addr = controller.network.get_addr().await?;

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(self.get_id().to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();

        let nodes = controller.network.find(subject_hash).await?;

        if nodes.is_empty() || nodes.contains(&self_addr) {
            return Err(crate::error::ArunaMetadataError::NetworkError(
                "No node to forward request to found".to_string(),
            ));
        } else {
            let Some(first_node) = nodes.first() else {
                return Err(crate::error::ArunaMetadataError::NetworkError(
                    "No node to forward request to found".to_string(),
                ));
            };
            match controller
                .network
                .forward(body, subject_hash, first_node.clone())
                .await?
            {
                crate::models::requests::ForwardResponse::UpdateResource(response) => Ok(response?),
                e => Err(ArunaMetadataError::NetworkError(format!(
                    "Got wrong forward response {e:?}"
                ))),
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        auth_ctx: (UserIdentity, Path),
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let (user, path) = auth_ctx;

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
            .update_resource(node_id.as_bytes(), &user, path.clone(), resource.clone())
            .await?;

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(resource.id.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();
        controller.network.store(subject.as_bytes()).await?;

        // Replay update only to members that already got the object
        // In this case replicate functions as replay
        let members = controller
            .network
            .find_verified(subject_hash)
            .await?
            .into_iter()
            .filter(|addr| addr.node_id != node_id);

        controller
            .sync_loop(
                crate::models::structs::TypedDoc::Resource(doc),
                *subject_hash,
                ToBytes::to_bytes(resource.id),
                path,
                members,
                false,
            )
            .await?;

        Ok(response)
    }
}

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for CreateProjectRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = CreateProjectResponse;
    type AuthContext = (UserIdentity, [u8; 32], Ulid);

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::AuthContext, crate::error::ArunaMetadataError> {
        let Some(token) = token else {
            return Err(ArunaMetadataError::Unauthorized);
        };
        if !controller.persistence.check_is_group(self.group_id).await? {
            return Err(ArunaMetadataError::InvalidParameter {
                name: "group_id".to_string(),
                error: "Group not found".to_string(),
            });
        };
        let (action, group_id) = (Action::Write, self.group_id);
        let realm_id = controller.network.get_realm_key().await?;

        let path = Path::builder()
            .realm_id(realm_id)
            .group_resources_wildcard(group_id)
            .build()
            .map_err(|e| {
                error!(?e);
                ArunaMetadataError::Unauthorized
            })?;

        let identity = controller.persistence.get_identity(token).await?;
        if controller
            .persistence
            .check_path(&path, &identity, action)
            .await?
        {
            Ok((identity, realm_id, group_id))
        } else {
            Err(ArunaMetadataError::Unauthorized)
        }
    }
    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn sync_or_forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        controller
            .sync_user(
                token
                    .clone()
                    .ok_or_else(|| ArunaMetadataError::Unauthorized)?,
            )
            .await?;
        controller.sync_group(self.group_id).await?;
        Ok(None)
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: token.clone(),
            request: crate::models::requests::ForwardRequest::CreateProject(self.clone()),
        };

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(&self.group_id.to_bytes());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();

        let nodes = controller.network.find(subject_hash).await?;
        let self_addr = controller.network.get_addr().await?;
        let mut nodes = nodes
            .iter()
            .filter(|node_addr| node_addr.node_id != self_addr.node_id);

        let Some(first_node) = nodes.next() else {
            return Err(crate::error::ArunaMetadataError::NetworkError(
                "No node to forward request to found".to_string(),
            ));
        };

        match controller
            .network
            .forward(body, subject_hash, first_node.clone())
            .await?
        {
            ForwardResponse::CreateProject(response) => Ok(response?),
            e => Err(crate::error::ArunaMetadataError::NetworkError(format!(
                "Got wrong forward response {e:?}"
            ))),
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        auth_result: Self::AuthContext,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let user = auth_result.0;
        let realm_id = auth_result.1;
        let group_id = auth_result.2;

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
            variant: ResourceVariant::Project,
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

        let path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(group_id, resource.id, vec![])
            .build()
            .map_err(|e| {
                error!(?e);
                ArunaMetadataError::ServerError(e.to_string())
            })?;

        let doc = controller
            .persistence
            .add_resource(node_id.as_bytes(), &user, path.clone(), resource.clone())
            .await?;

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(resource.id.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();
        controller.network.store(subject_hash).await?;

        // Choose x = REPLICATION_POLICY random nodes of members
        // and replicate resource
        let members = controller.network.get_realm_nodes().await?;
        let realm_nodes = members
            .into_iter()
            .filter(|addr| addr.node_id != node_id)
            .choose_multiple(&mut rand::rngs::OsRng, REPLICATION_POLICY);

        controller
            .sync_loop(
                crate::models::structs::TypedDoc::Resource(doc),
                *subject_hash,
                resource.id.to_bytes().to_vec(),
                path,
                realm_nodes.into_iter(),
                false,
            )
            .await?;

        Ok(CreateProjectResponse { resource })
    }
}
