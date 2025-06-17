use std::collections::BTreeMap;

use super::request::Request;
use crate::{
    models::{
        models::Group,
        requests::{
            AddGroupRequest, AddGroupResponse, AddResourcesToGroupRequest,
            AddResourcesToGroupResponse, // AddRolesToGroupRequest, AddRolesToGroupResponse,
            AddUserToGroupRequest, AddUserToGroupResponse,
        },
    },
    network::network_trait::Network,
    persistence::{persistence::Authorize, search::search::Search},
};
use aruna_permission::{Action, Path, UserIdentity};
use aruna_storage::storage::store::Store;
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
        let realm_key = controller.network.get_realm_key().await?;
        let user_identity = controller.persistence.get_identity(token).await?;

        if user_identity.realm_key == realm_key {
            Ok(user_identity)
        } else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
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
            members: BTreeMap::from([(user.user_ulid.to_string(), vec!["admin".to_string()])]),
            realm_key,
        };
        let node_id = controller.network.get_addr().await?.node_id;
        let realm_id = controller.network.get_realm_key().await?;
        let group_doc = controller
            .persistence
            .add_group(node_id.as_bytes(), &realm_id, &user, group.clone())
            .await?;

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
                crate::models::models::TypedDoc::Group(group_doc),
                group.id,
                path,
                members,
            )
            .await?;

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
        if let Some((i, _)) = controller.persistence.authorize(token, action, id).await? {
            Ok(i)
        } else {
            Err(crate::error::ArunaMetadataError::Unauthorized)
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
        user: UserIdentity,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {

        //controller.persistence.add_user_to_group(node_key, user, group);
        todo!()

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

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for AddResourcesToGroupRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = AddResourcesToGroupResponse;
    type AuthContext = Option<UserIdentity>;

    #[tracing::instrument(level = "trace", skip(controller, token))]
    async fn authorize(
        &self,
        token: Option<String>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Option<UserIdentity>, crate::error::ArunaMetadataError> {
        let (action, id) = (Action::Write, self.group_id);
        if let Some((i, _)) = controller.persistence.authorize(token, action, id).await? {
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
        todo!()
    }
}
