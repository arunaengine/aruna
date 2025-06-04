use std::collections::BTreeMap;

use super::request::Request;
use crate::{
    models::{
        models::{Group, User},
        requests::{
            AddGroupRequest, AddGroupResponse, AddResourcesToGroupRequest,
            AddResourcesToGroupResponse, AddRolesToGroupRequest, AddRolesToGroupResponse,
            AddUserToGroupRequest, AddUserToGroupResponse,
        },
    },
    network::network_trait::{Network, REPLICATION_POLICY},
    persistence::search::search::Search,
};
use aruna_storage::storage::store::Store;
use automerge::sync::Message;
use rand::seq::IteratorRandom;
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
        user: Option<User>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let Some(user) = user else {
            return Err(crate::error::ArunaMetadataError::Unauthorized);
        };
        let group_id = Ulid::new();
        let group = Group {
            id: group_id,
            name: self.name,
            roles: vec!["admin".to_string(), "member".to_string()],
            members: BTreeMap::from([(user.id.to_string(), vec!["admin".to_string()])]),
        };
        let node_id = controller.network.get_addr().await?.node_id;
        let mut group_doc = controller
            .persistence
            .add_group(node_id.as_bytes(), &user.id, group.clone())
            .await?;

        // Choose x = REPLICATION_POLICY random nodes of members
        // and replicate resource
        let members = controller
            .network
            .get_realm_nodes()
            .await?
            .into_iter()
            .filter(|addr| addr.node_id != node_id);
        for node in members {
            let mut stream = controller.network.create_stream(node.node_id).await?;
            'sync: loop {
                let sync_message = controller
                    .persistence
                    .generate_sync_message(&user.id, &mut group_doc, node.node_id.clone())
                    .await?;

                let recv_message = controller
                    .network
                    .sync(
                        &mut stream,
                        crate::network::network_trait::ReplicationSubject::Group(
                            sync_message.clone(),
                        ),
                        &user.id,
                        node.clone(),
                    )
                    .await?;

                if let Some(response) = &recv_message {
                    controller
                        .persistence
                        .receive_sync_message(
                            &user.id,
                            &mut group_doc,
                            Message::decode(response)?,
                            node.node_id.clone(),
                        )
                        .await?;
                }
                if sync_message.is_none() && recv_message.is_none() {
                    break 'sync;
                }
            }

            trace!("PERSIST TO DISK");
            controller
                .persistence
                .handle_group_merges(group_doc.save())
                .await?;

            controller.network.finish_stream(stream).await?;
        }

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
        user: Option<User>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        todo!()
    }
}

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for AddRolesToGroupRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = AddRolesToGroupResponse;

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
        user: Option<User>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        todo!()
    }
}

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for AddResourcesToGroupRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = AddResourcesToGroupResponse;

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
        user: Option<User>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        todo!()
    }
}
