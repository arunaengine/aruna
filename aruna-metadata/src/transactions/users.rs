use super::request::Request;
use crate::{
    models::{
        models::User,
        requests::{AddUserRequest, AddUserResponse},
    },
    network::network_trait::Network,
    persistence::search::search::Search,
};
use aruna_storage::storage::store::Store;
use automerge::sync::Message;
use ulid::Ulid;

#[async_trait::async_trait]
impl<St, Se, N> Request<St, Se, N> for AddUserRequest
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    type Response = AddUserResponse;

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
        _user: Option<User>,
        controller: &super::controller::Controller<St, Se, N>,
    ) -> Result<Self::Response, crate::error::ArunaMetadataError> {
        let user = User {
            id: Ulid::new(),
            name: self.name,
        };
        let node_id = controller.network.get_addr().await?.node_id;
        let mut user_doc = controller
            .persistence
            .add_user(node_id.as_bytes(), user.clone())
            .await?;

        // (for now) Replicate users to all member nodes
        let members = controller
            .network
            .get_realm_nodes()
            .await?
            .into_iter()
            .filter(|addr| addr.node_id != node_id);

        for node in members {
            'sync: loop {
                let sync_message = controller
                    .persistence
                    .generate_sync_message(&user.id, &mut user_doc, node.node_id.clone())
                    .await?;
                match sync_message {
                    Some(msg) => {
                        if let Some(response) = controller
                            .network
                            .sync(
                                crate::network::network_trait::ReplicationSubject::User(Some(msg)),
                                &user.id,
                                node.clone(),
                            )
                            .await?
                        {
                            controller
                                .persistence
                                .receive_sync_message(
                                    &user.id,
                                    &mut user_doc,
                                    Message::decode(&response)?,
                                    node.node_id.clone(),
                                )
                                .await?;
                        }
                    }
                    None => {
                        todo!("CHANGE THIS TO KEEP RECEIVING");
                        controller
                            .network
                            .sync(
                                crate::network::network_trait::ReplicationSubject::User(None),
                                &user.id,
                                node.clone(),
                            )
                            .await?;

                        controller
                            .persistence
                            .handle_user_merges(user_doc.save())
                            .await?;
                        break 'sync;
                    }
                }
            }
        }

        Ok(AddUserResponse { user })
    }
}
