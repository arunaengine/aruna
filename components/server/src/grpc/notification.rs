use crate::auth::permission_handler::{PermissionCheck, PermissionHandler};
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::enums::ObjectType;
use crate::notification::utils::generate_endpoint_subject;
use crate::{database::enums::DbPermissionLevel, middlelayer::db_handler::DatabaseHandler};
use aruna_rust_api::api::notification::services::v2::{
    create_stream_consumer_request::{StreamType, Target},
    event_message::MessageVariant,
    event_notification_service_server::EventNotificationService,
    AcknowledgeMessageBatchRequest, AcknowledgeMessageBatchResponse, CreateStreamConsumerRequest,
    CreateStreamConsumerResponse, DeleteStreamConsumerRequest, DeleteStreamConsumerResponse,
    EventMessage, GetEventMessageBatchRequest, GetEventMessageBatchResponse,
    GetEventMessageStreamRequest, GetEventMessageStreamResponse, ResourceTarget,
};
use aruna_rust_api::api::storage::models::v2::ResourceVariant;
use async_nats::jetstream::{consumer::DeliverPolicy, Message};
use chrono::Utc;
use diesel_ulid::DieselUlid;
use futures::StreamExt;
use log::{debug, error};
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Result, Status};

use crate::{
    database::{crud::CrudDb, dsls::notification_dsl::StreamConsumer},
    notification::{
        handler::{EventHandler, EventType},
        natsio_handler::NatsIoHandler,
        utils::{calculate_reply_hmac, parse_event_consumer_subject},
    },
    utils::grpc_utils::get_token_from_md,
};

crate::impl_grpc_server!(
    NotificationServiceImpl,
    natsio_handler: Arc<NatsIoHandler>
);

#[tonic::async_trait]
impl EventNotificationService for NotificationServiceImpl {
    /// Create a stream group which can be used to fetch event messages depending on the
    /// provided resource.
    ///
    /// ## Arguments:
    ///
    /// * `request: tonic::Request<CreateEventStreamingGroupRequest>` -
    /// Contains the needed information to create a new stream group.
    ///
    /// ## Returns:
    ///
    /// * `Result<tonic::Response<CreateEventStreamingGroupResponse>, tonic::Status>` -
    /// Response contains a vector of fetched messages; Error else.
    ///
    async fn create_stream_consumer(
        &self,
        request: tonic::Request<CreateStreamConsumerRequest>,
    ) -> Result<Response<CreateStreamConsumerResponse>, Status> {
        // Log some stuff
        log_received!(&request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request metadata
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token extraction failed"
        );

        // Extract target from inner request
        let consumer_target = inner_request
            .target
            .ok_or_else(|| Status::invalid_argument("Missing target context"))?;

        // Evaluate permission context and event type from consumer target
        let (perm_context, event_type) = extract_context_event_type_from_target(
            consumer_target,
            inner_request.include_subresources,
        )?;

        // Check permission for evaluated permission context
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![perm_context])
                .await,
            "Permission denied"
        );

        // Extract and convert delivery policy
        let deliver_policy = if let Some(stream_type) = inner_request.stream_type {
            tonic_invalid!(
                convert_stream_type(stream_type),
                "Stream type conversion failed"
            )
        } else {
            DeliverPolicy::All // Default
        };

        // Create consumer in Nats.io
        let (consumer_id, consumer_config) = tonic_internal!(
            self.natsio_handler
                .create_event_consumer(event_type, deliver_policy)
                .await,
            "Consumer creation failed"
        );

        // Create stream consumer in database
        let mut stream_consumer = StreamConsumer {
            id: consumer_id,
            user_id: Some(user_id),
            config: postgres_types::Json(consumer_config),
        };

        // Get database client
        let client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Database not available"
        );

        // Create stream consumer in database and rollback Nats.io consumer on error
        if let Err(err) = stream_consumer.create(&client).await {
            // Try delete Nats.io consumer for rollback
            let _ = self
                .natsio_handler
                .delete_event_consumer(consumer_id.to_string())
                .await;

            return Err(Status::internal(err.to_string()));
        }

        // Create gRPC response
        let response = CreateStreamConsumerResponse {
            stream_consumer: consumer_id.to_string(),
        };
        return_with_log!(response);
    }

    /// Fetch messages from an existing stream group.
    ///
    /// ## Arguments:
    ///
    /// * `request: tonic::Request<GetEventMessageBatchRequest>` -
    /// Contains the consumer id and the maximum number of messages to be fetched
    ///
    /// ## Returns:
    ///
    /// * `Result<tonic::Response<GetEventMessageBatchResponse>, tonic::Status>` -
    /// Response contains a vector of fetched messages; Error else.
    ///
    async fn get_event_message_batch(
        &self,
        request: tonic::Request<GetEventMessageBatchRequest>,
    ) -> Result<Response<GetEventMessageBatchResponse>, Status> {
        // Log some stuff
        log::info!("Received GetEventMessageBatchRequest.");
        debug!("{:?}", &request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract consumer id parameter
        let consumer_id = tonic_invalid!(
            DieselUlid::from_str(&inner_request.stream_consumer),
            "Invalid consumer id format"
        );

        // Extract token from request metadata
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token extraction failed"
        );

        // Check empty permission context just to validate registered and active user
        tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![Context::default()])
                .await,
            "Permission denied"
        );

        // Fetch stream consumer, parse subject and check specific permissions. This is shit.
        let client = &self
            .database_handler
            .database
            .get_client()
            .await
            .map_err(|e| {
                log::error!("{e}");
                Status::unavailable("Database not available.")
            })?;

        let stream_consumer = StreamConsumer::get(consumer_id, client)
            .await
            .map_err(|_| Status::aborted("Stream consumer fetch failed"))?;

        let specific_context: Context = if let Some(consumer) = stream_consumer {
            tonic_invalid!(
                parse_event_consumer_subject(&consumer.config.0.filter_subject),
                "Invalid consumer subject"
            )
            .try_into()?
        } else {
            return Err(Status::invalid_argument(format!(
                "Consumer with id {consumer_id} does not exist"
            )));
        };

        tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![specific_context])
                .await,
            "Invalid permissions"
        );

        // Fetch messages of event consumer
        let nats_messages = tonic_internal!(
            self.natsio_handler
                .get_event_consumer_messages(consumer_id.to_string(), inner_request.batch_size)
                .await,
            "Stream consumer message fetch failed"
        );

        // Convert messages and add reply
        let mut proto_messages = vec![];
        for nats_message in nats_messages {
            // Convert Nats.io message to proto message
            let mut msg_variant: MessageVariant = tonic_internal!(
                serde_json::from_slice(nats_message.message.payload.to_vec().as_slice(),),
                "Could not convert received Nats.io message"
            );
            // Create reply option
            let reply_subject = nats_message
                .reply
                .as_ref()
                .ok_or_else(|| Status::internal("Nats.io message is missing reply subject"))?;
            let msg_reply =
                calculate_reply_hmac(reply_subject, self.natsio_handler.reply_secret.clone());

            // Modify message with reply
            match msg_variant {
                MessageVariant::ResourceEvent(ref mut event) => event.reply = Some(msg_reply),
                MessageVariant::UserEvent(ref mut event) => event.reply = Some(msg_reply),
                MessageVariant::AnnouncementEvent(ref mut event) => event.reply = Some(msg_reply),
            }

            proto_messages.push(EventMessage {
                message_variant: Some(msg_variant),
            })
        }

        // Create gRPC response
        let grpc_response = Response::new(GetEventMessageBatchResponse {
            messages: proto_messages,
        });

        // Log some stuff and return response
        log::info!("Sending GetEventMessageBatchResponse back to client.");
        debug!("{:?}", &grpc_response);
        return Ok(grpc_response);
    }

    ///ToDo: Rust Doc
    type GetEventMessageStreamStream =
        ReceiverStream<Result<GetEventMessageStreamResponse, Status>>;

    ///ToDo: Rust Doc
    async fn get_event_message_stream(
        &self,
        request: tonic::Request<GetEventMessageStreamRequest>,
    ) -> Result<Response<Self::GetEventMessageStreamStream>, Status> {
        // Log some stuff
        log::info!("Received GetEventMessageBatchStreamRequest.");
        debug!("{:?}", &request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract consumer id and batch size parameter
        let consumer_id = tonic_invalid!(
            DieselUlid::from_str(&inner_request.stream_consumer),
            "Invalid consumer id format"
        );

        // Extract token from request metadata
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token extraction failed"
        );

        // Check empty permission context just to validate registered and active user
        let PermissionCheck { is_proxy, .. } = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![Context::registered()])
                .await,
            "Permission denied"
        );

        // If request is from Dataproxy: Create ephemeral consumer
        let pull_consumer = if is_proxy {
            tonic_internal!(
                self.natsio_handler
                    .create_internal_consumer(
                        DieselUlid::generate(), //  Random temp id
                        generate_endpoint_subject(&consumer_id),
                        DeliverPolicy::ByStartTime {
                            start_time: tonic_invalid!(
                                OffsetDateTime::from_unix_timestamp(
                                    Utc::now().timestamp() - (60 * 60)
                                ),
                                "Incorrect timestamp format"
                            ),
                        },
                        true,
                    )
                    .await,
                "Consumer creation failed"
            )
        } else {
            // Try to fetch stream consumer, parse subject and check specific permissions.
            let client = &self
                .database_handler
                .database
                .get_client()
                .await
                .map_err(|e| {
                    log::error!("{e}");
                    Status::unavailable("Database not available.")
                })?;

            let stream_consumer = StreamConsumer::get(consumer_id, client)
                .await
                .map_err(|_| Status::aborted("Stream consumer fetch failed"))?;

            let specific_context: Context = if let Some(consumer) = stream_consumer {
                tonic_invalid!(
                    parse_event_consumer_subject(&consumer.config.0.filter_subject),
                    "Invalid consumer subject"
                )
                .try_into()?
            } else {
                return Err(Status::invalid_argument("Stream consumer does not exist."));
            };

            tonic_auth!(
                self.authorizer
                    .check_permissions(&token, vec![specific_context])
                    .await,
                "Nope."
            );

            tonic_internal!(
                self.natsio_handler
                    .get_pull_consumer(consumer_id.to_string())
                    .await,
                "Fetching consumer failed"
            )
        };

        // Create multi-producer single-consumer channel
        let (tx, rx) = mpsc::channel(4);
        let mut message_stream = tonic_internal!(
            pull_consumer.messages().await,
            "Message stream creation failed"
        );

        // Keep-alive spawn
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            loop {
                if tx_clone
                    .send(Ok(GetEventMessageStreamResponse { message: None }))
                    .await
                    .is_err()
                {
                    error!("Keep-alive connection to DataProxy {consumer_id} terminated");
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            }
        });

        // Send messages in batches if present
        let cloned_reply_signing_secret = self.natsio_handler.reply_secret.clone();
        tokio::spawn(async move {
            debug!("Starting event notification fetch loop");
            let mut already_seen: HashSet<String, RandomState> = HashSet::default();
            loop {
                if let Some(Ok(nats_message)) = message_stream.next().await {
                    debug!("Sending message to client: {}", nats_message.subject);

                    // Deduplication time
                    if let Some(header_map) = &nats_message.headers {
                        if let Some(header_val) = header_map.get("block-id") {
                            let block_id = header_val.to_string();
                            if already_seen.contains(&block_id) {
                                let _ = nats_message.ack().await; // Acknowledge duplicate messages
                                continue;
                            } else {
                                already_seen.insert(block_id);
                            }
                        }
                    }

                    // Convert Nats.io message to proto message
                    let event_message =
                        convert_nats_message_to_proto(nats_message, &cloned_reply_signing_secret)?;

                    // Send message through stream
                    match tx
                        .send(Ok(GetEventMessageStreamResponse {
                            message: Some(event_message),
                        }))
                        .await
                    {
                        Ok(_) => {
                            log::info!("Successfully send stream response")
                        }
                        Err(err) => {
                            return Err::<GetEventMessageBatchResponse, Status>(Status::internal(
                                format!("Failed to send response: {err}"),
                            ))
                        }
                    };
                }
            }
        });

        // Create gRPC response
        let grpc_response: Response<
            ReceiverStream<std::result::Result<GetEventMessageStreamResponse, Status>>,
        > = Response::new(ReceiverStream::new(rx));

        // Log some stuff and return response
        log::info!("Sending GetEventMessageBatchStreamStreamResponse back to client.");
        debug!("{:?}", &grpc_response);
        return Ok(grpc_response);
    }

    /// Manually acknowledges the provided message in the Nats cluster.
    /// The acknowledged messages will not be included in future requests that fetch
    /// messages of a subject under which the messages would be delivered.
    ///
    /// ## Arguments:
    ///
    /// * `tonic::Request<AcknowledgeMessageBatchRequest>` - Contains the replies
    /// of all messages which shall be acknowledged.
    ///
    /// ## Returns:
    ///
    /// * `Result<tonic::Response<AcknowledgeMessageBatchResponse>, tonic::Status>` -
    /// An empty response signals success that the specific messages could be acknowledged.
    ///
    async fn acknowledge_message_batch(
        &self,
        request: tonic::Request<AcknowledgeMessageBatchRequest>,
    ) -> Result<Response<AcknowledgeMessageBatchResponse>, Status> {
        log::info!("Received AcknowledgeMessageBatchRequest.");
        debug!("{:?}", &request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request metadata
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token extraction failed"
        );

        // Check empty permission context just to validate registered and active user
        tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![Context::registered()])
                .await,
            "Permission denied"
        );

        // Acknowledge provided messages
        if let Err(err) = &self
            .natsio_handler
            .acknowledge_from_reply(inner_request.replies)
            .await
        {
            return Err(Status::aborted(err.to_string()));
        }

        // Create and return gRPC response
        let grpc_response = Response::new(AcknowledgeMessageBatchResponse {});

        log::info!("Sending AcknowledgeMessageBatchResponse back to client.");
        debug!("{:?}", &grpc_response);
        return Ok(grpc_response);
    }

    /// Deletes the notification stream group associated with the provided stream group id.
    ///
    /// ## Arguments:
    ///
    /// * `request` - Contains the stream group id to be deleted
    ///
    /// ## Returns:
    ///
    /// - `Result<tonic::Response<DeleteEventStreamingGroupResponse>, tonic::Status>` -
    /// An empty response signals deletion success; Error else.
    async fn delete_stream_consumer(
        &self,
        request: tonic::Request<DeleteStreamConsumerRequest>,
    ) -> Result<Response<DeleteStreamConsumerResponse>, Status> {
        log::info!("Received DeleteStreamConsumerRequest.");
        debug!("{:?}", &request);

        // Consume gRPC request into its parts
        let (request_metadata, _, inner_request) = request.into_parts();

        // Extract token from request metadata
        let token = tonic_auth!(
            get_token_from_md(&request_metadata),
            "Token extraction failed"
        );

        // Extract consumer id from request
        let consumer_ulid = tonic_invalid!(
            DieselUlid::from_str(&inner_request.stream_consumer),
            "Invalid stream consumer id"
        );

        // Check empty permission context just to validate registered and active user
        let _test = tonic_auth!(
            self.authorizer
                .check_permissions(&token, vec![Context::registered()])
                .await,
            "Permission denied"
        );

        // Get database client and begin transaction
        let mut client = tonic_internal!(
            self.database_handler.database.get_client().await,
            "Database not available"
        );

        let transaction =
            tonic_internal!(client.transaction().await, "Transaction creation failed");
        let transaction_client = transaction.client();

        // Fetch stream consumer to check permissions against user_id
        if let Some(stream_consumer) = tonic_internal!(
            StreamConsumer::get(consumer_ulid, transaction_client).await,
            "Stream consumer fetch failed"
        ) {
            if let Some(user_ulid) = stream_consumer.user_id {
                tonic_auth!(
                    self.authorizer
                        .check_permissions(
                            &token,
                            vec![Context::user_ctx(user_ulid, DbPermissionLevel::WRITE)]
                        )
                        .await,
                    "Permission denied"
                );
            } else {
                // What do with data proxies?
            };

            // Delete stream consumer in database
            tonic_internal!(
                stream_consumer.delete(transaction_client).await,
                "Stream consumer delete failed"
            );

            // Delete Nats.io stream consumer
            tonic_internal!(
                self.natsio_handler
                    .delete_event_consumer(stream_consumer.id.to_string())
                    .await,
                "Stream consumer delete failed"
            );
        } else {
            return Err(Status::invalid_argument("Stream consumer does not exist"));
        }

        // Commit transaction
        tonic_internal!(transaction.commit().await, "Transaction commit failed");

        // Create and return gRPC response
        let grpc_response = Response::new(DeleteStreamConsumerResponse {});

        log::info!("Sending DeleteStreamConsumerResponse back to client.");
        debug!("{:?}", &grpc_response);
        return Ok(grpc_response);
    }
}

// ------------------------------------------- //
// ----- Helper functions -------------------- //
// ------------------------------------------- //
///ToDo: Rust Doc
fn convert_stream_type(stream_type: StreamType) -> anyhow::Result<DeliverPolicy> {
    match stream_type {
        StreamType::StreamAll(_) => Ok(DeliverPolicy::All),
        StreamType::StreamFromDate(info) => {
            if let Some(timestamp) = info.timestamp {
                Ok(DeliverPolicy::ByStartTime {
                    start_time: tonic_invalid!(
                        OffsetDateTime::from_unix_timestamp(timestamp.seconds),
                        "Incorrect timestamp format"
                    ),
                })
            } else {
                Err(anyhow::anyhow!("No timestamp provided"))
            }
        }
        StreamType::StreamFromSequence(info) => Ok(DeliverPolicy::ByStartSequence {
            start_sequence: info.sequence,
        }),
    }
}

fn extract_context_event_type_from_target(
    target: Target,
    with_subresources: bool,
) -> Result<(Context, EventType), Status> {
    Ok(match target {
        Target::Resource(ResourceTarget {
            resource_id,
            resource_variant,
        }) => {
            let variant = ResourceVariant::try_from(resource_variant).map_err(|e| {
                log::error!("{e}");
                Status::invalid_argument("")
            })?;
            let variant_type =
                ObjectType::try_from(variant).map_err(|_| Status::invalid_argument(""))?;
            let event_type =
                EventType::Resource((resource_id.to_string(), variant_type, with_subresources));

            let context = Context::res_ctx(
                tonic_invalid!(
                    DieselUlid::from_str(&resource_id),
                    "Invalid resource id format"
                ),
                DbPermissionLevel::READ,
                true,
            );

            (context, event_type)
        }
        Target::User(user_id) => {
            let event_type = EventType::User(user_id.to_string());

            let context = Context::user_ctx(
                tonic_invalid!(DieselUlid::from_str(&user_id), "Invalid user id format"),
                DbPermissionLevel::READ,
            );

            (context, event_type)
        }
        Target::Announcements(_) => (Context::registered(), EventType::Announcement(None)),
        Target::All(_) => (Context::proxy(), EventType::All),
    })
}

///ToDo: Rust Doc
fn convert_nats_message_to_proto(
    nats_message: Message,
    reply_secret: &str,
) -> Result<EventMessage, Status> {
    // Deserialize message to proto message variant
    let mut message_variant = tonic_internal!(
        serde_json::from_slice(nats_message.message.payload.to_vec().as_slice(),),
        "Could not convert received Nats.io message"
    );

    // Calculate message reply
    let reply_subject = nats_message
        .reply
        .as_ref()
        .ok_or_else(|| Status::internal("Nats.io message is missing reply subject"))?;
    let msg_reply = calculate_reply_hmac(reply_subject, reply_secret.to_string());

    // Modify message with reply
    match message_variant {
        MessageVariant::ResourceEvent(ref mut event) => event.reply = Some(msg_reply),
        MessageVariant::UserEvent(ref mut event) => event.reply = Some(msg_reply),
        MessageVariant::AnnouncementEvent(ref mut event) => event.reply = Some(msg_reply),
    }

    Ok(EventMessage {
        message_variant: Some(message_variant),
    })
}

impl TryInto<Context> for EventType {
    type Error = Status;

    fn try_into(self) -> std::result::Result<Context, Self::Error> {
        match self {
            EventType::Resource((resource_id, _object_type, _)) => Ok(Context::res_ctx(
                tonic_invalid!(DieselUlid::from_str(&resource_id), "Invalid resource id"),
                DbPermissionLevel::READ,
                true,
            )),
            EventType::User(user_id) => Ok(Context::user_ctx(
                tonic_invalid!(DieselUlid::from_str(&user_id), "Invalid user id"),
                DbPermissionLevel::READ,
            )),
            EventType::Announcement(_) => Ok(Context::registered()),
            EventType::All => Ok(Context::proxy()),
        }
    }
}
