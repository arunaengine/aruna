use aruna_net::actor_handle::ReceiveStreams;
use aruna_permission::Path;
use aruna_storage::storage::store::Store;
use iroh::PublicKey;

use crate::{
    error::ArunaMetadataError,
    models::requests::{ForwardRequest, ForwardResponse, Request},
    persistence::{
        authorization::Authorize,
        persistor::tables::{GROUPS_DB_NAME, RESOURCE_DB_NAME, USER_DB_NAME},
        search::generic::Search,
    },
    transactions::controller::Controller,
};

use super::{
    network_trait::{
        AuthorizeResponse, Body, MetadataMessage, Network, ReplicationSubject, Response,
    },
    p2p_network::P2PNetwork,
    util::{read_message, send_message},
};

// All of these are rather lengthy functions (~100 locs),
// but only because of extensive matching and error returns
impl P2PNetwork {
    #[tracing::instrument(
        level = "trace",
        skip(self, message, sync_message, subject_id, recv_stream, controller)
    )]
    async fn handle_replication_messages<St, Se, N>(
        &self,
        message: MetadataMessage,
        sync_message: ReplicationSubject,
        subject_id: Vec<u8>,
        path: Path,
        recv_stream: &mut ReceiveStreams,
        controller: &Controller<St, Se, N>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a>,
        Se: Search,
        N: Network,
    {
        // Init sync
        let node_id = PublicKey::from_bytes(&message.from).map_err(|_e| {
            ArunaMetadataError::ConversionError {
                from: "&[u8]".to_string(),
                to: "PublicKey".to_string(),
            }
        })?;
        let table = match sync_message {
            ReplicationSubject::Group(_) => GROUPS_DB_NAME,
            ReplicationSubject::User(_) => USER_DB_NAME,
            ReplicationSubject::Object(_) => RESOURCE_DB_NAME,
        };
        let mut doc = controller
            .persistence
            .get_or_create_doc(subject_id.clone(), table)
            .await?;

        // Poll sync response
        let sync_response = controller
            .persistence
            .handle_replication(node_id, subject_id, path, sync_message, &mut doc)
            .await?;

        // Send response either with Some(_) or None
        send_message(
            MetadataMessage {
                from: message.to,
                to: message.from,
                subject: message.subject,
                body: Body::Response(Response::SyncResponse(sync_response)),
            },
            &mut recv_stream.send_stream,
        )
        .await?;

        // Start sync loop
        'inner: loop {
            let MetadataMessage {
                from,
                to,
                subject,
                body:
                    Body::Replicate {
                        id,
                        path,
                        sync_message,
                    },
            } = read_message(&mut recv_stream.recv_stream).await?
            else {
                return Err(ArunaMetadataError::ServerError(
                    "Unexpected Message type".to_string(),
                ));
            };

            let sync_response = controller
                .persistence
                .handle_replication(node_id, id, path.clone(), sync_message.clone(), &mut doc)
                .await?;

            send_message(
                MetadataMessage {
                    from: to,
                    to: from,
                    subject,
                    body: Body::Response(Response::SyncResponse(sync_response.clone())),
                },
                &mut recv_stream.send_stream,
            )
            .await?;

            if sync_response.is_none() {
                match sync_message {
                    ReplicationSubject::Group(None) => {
                        controller
                            .persistence
                            .handle_group_merges(doc.save())
                            .await?;
                    }
                    ReplicationSubject::User(None) => {
                        controller
                            .persistence
                            .handle_user_merges(doc.save())
                            .await?;
                    }
                    ReplicationSubject::Object(None) => {
                        controller
                            .persistence
                            .handle_object_merges(path, doc.save())
                            .await?;
                    }
                    _ => continue,
                }
                self.store(&subject).await?;
                recv_stream
                    .send_stream
                    .finish()
                    .map_err(|e| ArunaMetadataError::NetworkError(e.to_string()))?;
                break 'inner;
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, recv_stream, controller))]
    pub async fn dispatch_messages<St, Se, N>(
        &self,
        recv_stream: &mut ReceiveStreams,
        controller: &Controller<St, Se, N>,
    ) -> Result<(), ArunaMetadataError>
    where
        for<'a> St: Store<'a>,
        Se: Search,
        N: Network,
    {
        while let Ok(msg) = read_message(&mut recv_stream.recv_stream).await {
            match msg.body {
                Body::Replicate {
                    ref id,
                    ref path,
                    ref sync_message,
                } => {
                    self.handle_replication_messages(
                        msg.clone(),
                        sync_message.clone(),
                        id.clone(),
                        path.clone(),
                        recv_stream,
                        controller,
                    )
                    .await?;
                }
                Body::Request { token, request } => {
                    let body = self
                        .handle_forwarding_messages(token, controller, request)
                        .await;
                    send_message(
                        MetadataMessage {
                            from: msg.to,
                            to: msg.from,
                            subject: msg.subject,
                            body,
                        },
                        &mut recv_stream.send_stream,
                    )
                    .await?;
                }
                Body::Authorize {
                    token,
                    action,
                    resource_id,
                } => {
                    let token = match token {
                        Some(token) => Some(controller.persistence.get_identity(token).await?),
                        None => None,
                    };
                    let response = match controller
                        .persistence
                        .authorize(token, action, resource_id)
                        .await
                    {
                        Ok(Some((_, path))) => AuthorizeResponse::Path(path),
                        Ok(None) => AuthorizeResponse::Public,
                        Err(err) => AuthorizeResponse::Error(err),
                    };
                    let body = Body::Response(Response::AuthorizeResponse(response));

                    send_message(
                        MetadataMessage {
                            from: msg.to,
                            to: msg.from,
                            subject: msg.subject,
                            body,
                        },
                        &mut recv_stream.send_stream,
                    )
                    .await?;
                }
                Body::Response { .. } => {
                    // TODO: todo!("Backchannel for updated merged docs or sync protocol");
                    // Nothing to do here, there are currently no messages that send responses
                    // after replication/forwarding back
                }
                Body::Empty => {
                    // TODO: Nothing to do here, maybe remove enum variant
                }
            }
        }
        Ok(())
    }
    pub(super) async fn handle_forwarding_messages<St, Se, N>(
        &self,
        token: Option<String>,
        controller: &Controller<St, Se, N>,
        request: ForwardRequest,
    ) -> Body
    where
        for<'a> St: Store<'a>,
        Se: Search,
        N: Network,
    {
        let body = match request {
            // User requests:
            ForwardRequest::AddUser(req) => match req.authorize(token, controller).await {
                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::AddUser(req.clone().run_request(auth_ctx, controller).await),
                ))),
                Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::GetResource(Err(e)),
                ))),
            },
            ForwardRequest::GetUser(req) => match req.authorize(token, controller).await {
                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::GetUser(req.clone().run_request(auth_ctx, controller).await),
                ))),
                Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::GetResource(Err(e)),
                ))),
            },
            ForwardRequest::CreateToken(req) => match req.authorize(token, controller).await {
                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::CreateToken(
                        req.clone().run_request(auth_ctx, controller).await,
                    ),
                ))),
                Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::GetResource(Err(e)),
                ))),
            },

            // Group request:
            ForwardRequest::AddGroup(req) => match req.authorize(token, controller).await {
                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::AddGroup(req.clone().run_request(auth_ctx, controller).await),
                ))),
                Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::GetResource(Err(e)),
                ))),
            },
            ForwardRequest::AddUserToGroup(req) => match req.authorize(token, controller).await {
                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::AddUserToGroup(
                        req.clone().run_request(auth_ctx, controller).await,
                    ),
                ))),
                Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::GetResource(Err(e)),
                ))),
            },
            ForwardRequest::GetGroup(req) => match req.authorize(token, controller).await {
                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::GetGroup(req.clone().run_request(auth_ctx, controller).await),
                ))),
                Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::GetResource(Err(e)),
                ))),
            },

            // Resource requests:
            ForwardRequest::CreateProject(req) => match req.authorize(token, controller).await {
                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::CreateProject(
                        req.clone().run_request(auth_ctx, controller).await,
                    ),
                ))),
                Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::Search(Err(e)),
                ))),
            },
            ForwardRequest::CreateResource(req) => match req.authorize(token, controller).await {
                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::CreateResource(
                        req.clone().run_request(auth_ctx, controller).await,
                    ),
                ))),
                Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::Search(Err(e)),
                ))),
            },
            ForwardRequest::GetResource(req) => match req.authorize(token, controller).await {
                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::GetResource(
                        req.clone().run_request(auth_ctx, controller).await,
                    ),
                ))),
                Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::GetResource(Err(e)),
                ))),
            },
            ForwardRequest::UpdateResource(req) => match req.authorize(token, controller).await {
                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::UpdateResource(
                        req.clone().run_request(auth_ctx, controller).await,
                    ),
                ))),
                Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::UpdateResource(Err(e)),
                ))),
            },
            ForwardRequest::Search(req) => match req.authorize(token, controller).await {
                Ok(auth_ctx) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::Search(req.clone().run_request(auth_ctx, controller).await),
                ))),
                Err(e) => Body::Response(Response::ForwardResponse(Box::new(
                    ForwardResponse::Search(Err(e)),
                ))),
            },
        };
        body
    }
}
