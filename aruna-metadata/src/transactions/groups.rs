use std::collections::BTreeMap;

use crate::error::ArunaMetadataError;
use crate::models::requests::{ForwardResponse, GetGroupRequest, GetGroupResponse, Request};
use crate::{
    logerr,
    models::{
        conversions::ToBytes,
        requests::{
            AddGroupRequest,
            AddGroupResponse, // AddResourcesToGroupRequest, AddResourcesToGroupResponse, AddRolesToGroupRequest, AddRolesToGroupResponse,
            AddUserToGroupRequest,
            AddUserToGroupResponse,
        },
        structs::Group,
    },
    network::network_trait::Network,
    persistence::{
        authorization::Authorize, persistor::tables::GROUPS_DB_NAME, search::generic::Search,
    },
};
use aruna_permission::{Action, Path, UserIdentity};
use aruna_storage::storage::store::Store;
use tracing::trace;
use ulid::Ulid;

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for AddGroupRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = AddGroupResponse;
    type AuthContext = UserIdentity;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<UserIdentity, crate::error::ArunaMetadataError> {
        let Some(token) = token else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        };

        let realm_key = controller
            .network
            .get_realm_key()
            .await
            .map_err(logerr!())?;
        let user_identity = controller
            .persistence
            .get_identity(token)
            .await
            .map_err(logerr!())?;

        if user_identity.realm_key == realm_key {
            Ok(user_identity)
        } else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        }
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn sync_or_forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        let realm_key = controller
            .network
            .get_realm_key()
            .await
            .map_err(logerr!())?;
        let user_identity = controller
            .persistence
            .get_identity(
                token
                    .clone()
                    .ok_or_else(|| ArunaMetadataError::Unauthorized)?,
            )
            .await
            .map_err(logerr!())?;

        if user_identity.realm_key != realm_key {
            Ok(Some(self.forward(token, controller).await?))
        } else {
            controller
                .sync_user(
                    token
                        .clone()
                        .ok_or_else(|| ArunaMetadataError::Unauthorized)?,
                )
                .await
                .map_err(logerr!())?;
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let Some(token) = token else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        };

        let identity = controller
            .persistence
            .get_identity(token.clone())
            .await
            .map_err(logerr!())?;

        let realm_key = controller
            .network
            .get_realm_key()
            .await
            .map_err(logerr!())?;

        let body = crate::network::network_trait::Body::Request {
            token: Some(token.clone()),
            request: crate::models::requests::ForwardRequest::AddGroup(self.clone()),
        };

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(&identity.realm_key);
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();

        let nodes = if identity.realm_key == realm_key {
            controller.network.get_realm_nodes().await?
        } else {
            controller.network.find(subject_hash).await?
        };

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
            .forward(body, &identity.realm_key, first_node.clone())
            .await?
        {
            crate::models::requests::ForwardResponse::AddGroup(response) => Ok(response?),
            e => Err(crate::error::ArunaMetadataError::NetworkError(format!(
                "Got wrong forward response {e:?}"
            ))),
        }
    }

    #[tracing::instrument(level = "trace", skip(controller, user))]
    async fn run_request(
        self,
        user: UserIdentity,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let group_id = Ulid::new();
        let realm_key = controller.network.get_realm_key().await?;
        let group = Group {
            id: group_id,
            name: self.name,
            roles: vec!["admin".to_string(), "member".to_string()],
            members: BTreeMap::from([(user.to_string(), vec!["admin".to_string()])]),
            realm_key,
        };
        let node_id = controller.network.get_addr().await?.node_id;
        let realm_id = controller.network.get_realm_key().await?;
        let group_doc = controller
            .persistence
            .add_group(node_id.as_bytes(), &realm_id, &user, group.clone())
            .await?;

        // TODO: Store group
        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(group.id.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();
        controller.network.store(subject_hash).await?;

        // Choose x = REPLICATION_POLICY random nodes of members
        // and replicate resource
        let members = controller
            .network
            .get_realm_nodes()
            .await?
            .into_iter()
            .filter(|addr| addr.node_id != node_id);
        let path = Path::builder()
            .realm_id(realm_key)
            .group(group_id)
            .build()
            .map_err(|e| crate::error::ArunaMetadataError::ServerError(e.to_string()))?;

        controller
            .sync_loop(
                crate::models::structs::TypedDoc::Group(group_doc),
                *subject_hash,
                ToBytes::to_bytes(group.id),
                path,
                members,
            )
            .await
            .detach_all();

        Ok(AddGroupResponse { group })
    }
}

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for AddUserToGroupRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = AddUserToGroupResponse;
    type AuthContext = UserIdentity;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<UserIdentity, crate::error::ArunaMetadataError> {
        let (action, id) = (Action::Write, self.group_id);
        let identity = match token {
            Some(token) => Some(controller.persistence.get_identity(token).await?),
            None => None,
        };
        trace!(?identity);
        Ok(controller
            .persistence
            .authorize(identity, action, id)
            .await?
            .ok_or_else(|| {
                crate::error::ArunaMetadataError::Forbidden(format!(
                    "{}",
                    self.group_id.to_string()
                ))
            })?
            .0)
    }

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn sync_or_forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
        let identity = if let Some(token) = token {
            controller.persistence.get_identity(token.clone()).await?
        } else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        };

        if identity.realm_key != controller.network.get_realm_key().await? {
            trace!("Forwarding message");
            Ok(Some(self.forward(token, controller).await?))
        } else {
            let mut chunk_hasher = blake3::Hasher::new();
            chunk_hasher.update(self.group_id.to_bytes().as_slice());
            let subject = chunk_hasher.finalize();
            let subject_hash = subject.as_bytes();
            let nodes = controller.network.find_verified(subject_hash).await?;

            if !nodes.contains(&controller.network.get_addr().await?) {

                controller
                    .sync_user(
                        token
                            .clone()
                            .ok_or_else(|| ArunaMetadataError::Unauthorized)?,
                    )
                    .await
                    .map_err(logerr!())?;

                let doc = controller
                    .persistence
                    .get_or_create_doc(self.group_id.to_bytes(), GROUPS_DB_NAME)
                    .await?;

                let path = Path::builder()
                    .realm_id(controller.network.get_realm_key().await?)
                    .group(self.group_id)
                    .build()
                    .map_err(|e| crate::error::ArunaMetadataError::ServerError(e.to_string()))?;

                controller
                    .sync_loop(
                        crate::models::structs::TypedDoc::Group(doc),
                        *subject_hash,
                        self.group_id.to_bytes(),
                        path,
                        nodes.into_iter(),
                    )
                    .await
                    .join_all()
                    .await;
                controller.network.store(subject_hash).await?;
            }
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: token.clone(),
            request: crate::models::requests::ForwardRequest::AddUserToGroup(self.clone()),
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
            .await
            .map_err(logerr!())?
        {
            ForwardResponse::AddUserToGroup(response) => Ok(response.map_err(logerr!())?),
            e => Err(crate::error::ArunaMetadataError::NetworkError(format!(
                "Got wrong forward response {e:?}"
            ))),
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        user: UserIdentity,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let realm_key = controller.network.get_realm_key().await?;
        let node_key = controller.network.get_addr().await?.node_id;

        let group_doc = controller
            .persistence
            .add_user_to_group(
                node_key.as_bytes(),
                &realm_key,
                &user,
                self.user_roles,
                self.group_id,
            )
            .await?;

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(self.group_id.to_bytes().as_slice());
        let subject = chunk_hasher.finalize();
        let subject_hash = subject.as_bytes();
        controller.network.store(subject_hash).await?;

        // Choose x = REPLICATION_POLICY random nodes of members
        // and replicate resource
        let members = controller
            .network
            .get_realm_nodes()
            .await?
            .into_iter()
            .filter(|addr| addr.node_id != node_key);
        let path = Path::builder()
            .realm_id(realm_key)
            .group(self.group_id)
            .build()
            .map_err(|e| crate::error::ArunaMetadataError::ServerError(e.to_string()))?;

        controller
            .sync_loop(
                crate::models::structs::TypedDoc::Group(group_doc),
                *subject_hash,
                ToBytes::to_bytes(self.group_id),
                path,
                members,
            )
            .await
            .detach_all();

        Ok(AddUserToGroupResponse {})
    }
}

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for GetGroupRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = GetGroupResponse;
    type AuthContext = UserIdentity;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<UserIdentity, crate::error::ArunaMetadataError> {
        let (action, id) = (Action::Read, self.id);
        let identity = controller
            .persistence
            .get_identity(token.ok_or_else(|| crate::error::ArunaMetadataError::Unauthorized)?)
            .await?;
        if let Some((i, _)) = controller
            .persistence
            .authorize(Some(identity), action, id)
            .await
            .map_err(logerr!())?
        {
            Ok(i)
        } else {
            Err(crate::error::ArunaMetadataError::Unauthorized)
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
        let nodes = controller.network.find_verified(subject_hash).await?;

        if !nodes.contains(&controller.network.get_addr().await?) {
            let group_id = self.id;
            let subject_hash = subject_hash.clone();
            let token_clone = token.clone();
            let controller_clone: super::controller::Controller<St, Se, N> = controller.clone();
            tokio::spawn(async move {
                // No need to sync user and wait if we forward anyway
                controller_clone
                    .sync_user(token_clone.ok_or_else(|| ArunaMetadataError::Unauthorized)?)
                    .await
                    .map_err(logerr!())?;
                let doc = controller_clone
                    .persistence
                    .get_or_create_doc(group_id.to_bytes(), GROUPS_DB_NAME)
                    .await?;
                let realm_key = controller_clone.network.get_realm_key().await?;

                let path = Path::builder()
                    .realm_id(realm_key)
                    .group(group_id)
                    .build()
                    .map_err(|e| crate::error::ArunaMetadataError::ServerError(e.to_string()))?;

                // We need to join_all here and wait for completion to store only if sync is
                // successfull
                controller_clone
                    .sync_loop(
                        crate::models::structs::TypedDoc::Group(doc),
                        subject_hash,
                        group_id.to_bytes(),
                        path,
                        nodes.into_iter(),
                    )
                    .await
                    .join_all()
                    .await;
                controller_clone.network.store(&subject_hash).await?;
                Ok::<(), ArunaMetadataError>(())
            });

            Ok(Some(self.forward(token, controller).await?))
        } else {
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn forward(
        &self,
        token: &Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let body = crate::network::network_trait::Body::Request {
            token: token.clone(),
            request: crate::models::requests::ForwardRequest::GetGroup(self.clone()),
        };

        let mut chunk_hasher = blake3::Hasher::new();
        chunk_hasher.update(&self.id.to_bytes());
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
            .await
            .map_err(logerr!())?
        {
            ForwardResponse::GetGroup(response) => Ok(response.map_err(logerr!())?),
            e => Err(crate::error::ArunaMetadataError::NetworkError(format!(
                "Got wrong forward response {e:?}"
            ))),
        }
    }

    #[tracing::instrument(level = "trace", skip(controller))]
    async fn run_request(
        self,
        _user: UserIdentity,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let persistor = controller.persistence.clone();
        let group = persistor.get_group(self.id).await?;
        Ok(GetGroupResponse { group })
    }
}

// #[async_trait::async_trait]
// impl<St, Se, N> Request<St, Se, N> for AddRolesToGroupRequest
// where
//     for<'a> St: Store<'a> + 'static,
//     Se: Search + 'static,
//     N: Network + 'static,
// {
//     type Response = AddRolesToGroupResponse;
//     type AuthContext = Option<UserIdentity>;
//
//     #[tracing::instrument(level = "trace", skip(controller, token))]
//     async fn authorize(
//         &self,
//         token: Option<String>,
//         controller: &super::controller::Controller<St, Se, N>,
//     ) -> Result<Option<UserIdentity>, crate::error::ArunaMetadataError> {
//         let (action, id) = (Action::Write, self.group_id);
//         if let Some((i, _)) = controller.persistence.authorize(token, action, id).await? {
//             Ok(Some(i))
//         } else {
//             Ok(None)
//         }
//     }
//
//     #[tracing::instrument(level = "trace", skip(_controller))]
//     async fn forward_or_return(
//         &self,
//         user: &Option<String>,
//         _controller: &super::controller::Controller<St, Se, N>,
//     ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
//         Ok(None)
//     }
//
//     #[tracing::instrument(level = "trace", skip(controller))]
//     async fn run_request(
//         self,
//         user: Option<UserIdentity>,
//         controller: &super::controller::Controller<St, Se, N>,
//     ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
//         todo!()
//     }
// }
//
// #[async_trait::async_trait]
// impl<St, Se, N> Request<St, Se, N> for AddResourcesToGroupRequest
// where
//     for<'a> St: Store<'a> + 'static,
//     Se: Search + 'static,
//     N: Network + 'static,
// {
//     type Response = AddResourcesToGroupResponse;
//     type AuthContext = Option<UserIdentity>;
//
//     #[tracing::instrument(level = "trace", skip(controller, token))]
//     async fn authorize(
//         &self,
//         token: Option<String>,
//         controller: &super::controller::Controller<St, Se, N>,
//     ) -> Result<Option<UserIdentity>, crate::error::ArunaMetadataError> {
//         let (action, id) = (Action::Write, self.group_id);
//         if let Some((i, _)) = controller.persistence.authorize(token, action, id).await? {
//             Ok(Some(i))
//         } else {
//             Ok(None)
//         }
//     }
//
//     #[tracing::instrument(level = "trace", skip(_controller))]
//     async fn forward_or_return(
//         &self,
//         user: &Option<String>,
//         _controller: &super::controller::Controller<St, Se, N>,
//     ) -> Result<Option<Self::Response>, crate::error::ArunaMetadataError> {
//         Ok(None)
//     }
//
//     #[tracing::instrument(level = "trace", skip(controller))]
//     async fn run_request(
//         self,
//         user: Option<UserIdentity>,
//         controller: &super::controller::Controller<St, Se, N>,
//     ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
//         todo!()
//     }
// }
