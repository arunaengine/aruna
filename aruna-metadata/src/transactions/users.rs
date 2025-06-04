use super::request::Request;
use crate::{
    error::ArunaMetadataError,
    models::{
        models::User,
        requests::{AddUserRequest, AddUserResponse},
    },
    network::network_trait::Network,
    persistence::search::search::Search,
};
use aruna_storage::storage::store::Store;
use automerge::sync::Message;
use tracing::trace;
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
        let user_doc = controller
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
            let doc = user_doc.clone();
            let network = controller.network.clone();
            let persistence = controller.persistence.clone();
            tokio::spawn(async move {
                let mut counter = 0;
                let mut user_doc = doc;

                let mut stream = network.create_stream(node.node_id).await?;

                'sync: loop {
                    counter += 1;
                    let sync_message = persistence
                        .generate_sync_message(&user.id, &mut user_doc, node.node_id.clone())
                        .await?;

                    trace!("Send message {sync_message:?}");
                    let recv_message = network
                        .sync(
                            &mut stream,
                            crate::network::network_trait::ReplicationSubject::User(
                                sync_message.clone(),
                            ),
                            &user.id,
                            node.clone(),
                        )
                        .await?;

                    if let Some(response) = &recv_message {
                        persistence
                            .receive_sync_message(
                                &user.id,
                                &mut user_doc,
                                Message::decode(response)?,
                                node.node_id.clone(),
                            )
                            .await?;
                    }
                    if sync_message.is_none() && recv_message.is_none() {
                        break 'sync;
                    }
                    if counter > 100 {
                        println!("{}/{}", sync_message.is_none(), recv_message.is_none());
                        panic!("{counter}");
                    }
                }

                trace!("PERSIST TO DISK");
                persistence.handle_user_merges(user_doc.save()).await?;
                network.finish_stream(stream).await?;

                Ok::<(), ArunaMetadataError>(())
            });
        }

        Ok(AddUserResponse { user })
    }
}
