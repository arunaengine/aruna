use anyhow::anyhow;
use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::time::Duration;

use aruna_rust_api::api::notification::services::v2::announcement_event::EventVariant as AnnouncementVariant;
use aruna_rust_api::api::notification::services::v2::event_message::MessageVariant;
use aruna_rust_api::api::notification::services::v2::{
    AnnouncementEvent, EventVariant, Reply, Resource, ResourceEvent, UserEvent,
};

use aruna_rust_api::api::storage::models::v2::{ResourceVariant, User as ApiUser};
use async_nats::jetstream::consumer::{pull, Config, DeliverPolicy, PullConsumer};
use async_nats::HeaderMap;

use async_nats::jetstream::{stream::Stream, Context, Message};

use async_trait::async_trait;
use diesel_ulid::DieselUlid;
use futures::future::try_join_all;
use futures::{StreamExt, TryStreamExt};
use prost::bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::database::dsls::object_dsl::{
    Hierarchy, ObjectWithRelations, MAX_RETRIES, RETRY_TIMEOUT,
};
use crate::database::dsls::user_dsl::User;
use crate::utils::grpc_utils::{checksum_resource, checksum_user, generic_object_without_rules};

use super::handler::{EventHandler, EventStreamHandler, EventType};
use super::utils::{
    generate_announcement_message_subject, generate_announcement_subject,
    generate_endpoint_subject, generate_resource_message_subjects, generate_resource_subject,
    generate_user_message_subject, generate_user_subject, validate_reply_msg,
};

// ----- Constants used for notifications -------------------- //
pub const STREAM_NAME: &str = "AOS_STREAM";
pub const STREAM_SUBJECTS: [&str; 5] = [
    "AOS.RESOURCE.>",
    "AOS.USER.>",
    "AOS.ANNOUNCEMENT.>",
    "AOS.ENDPOINT.>",
    "AOS.SERVER.>",
];

#[derive(Deserialize, Serialize)]
// Enum for internal events that are only of interest for the ArunaServer instances
pub enum ServerEvents {
    MVREFRESH(i64), // UTC timestamp_seconds
    CACHEUPDATE(Action),
}
#[derive(Deserialize, Serialize)]
pub enum Action {
    Created(Created),
    Updated(Updated),
    Deleted(Deleted),
}
#[derive(Deserialize, Serialize)]
pub enum Created {
    Rule(DieselUlid),
    RuleBinding {
        rule_id: DieselUlid,
        origin_id: DieselUlid,
        resource_id: DieselUlid,
    },
}
#[derive(Deserialize, Serialize)]
pub enum Updated {
    Rule(DieselUlid),
    RuleBinding {
        rule_id: DieselUlid,
        origin_id: DieselUlid,
        resource_id: DieselUlid,
    },
}
#[derive(Deserialize, Serialize)]
pub enum Deleted {
    Rule(DieselUlid),
    RuleBinding {
        rule_id: DieselUlid,
        resource_id: DieselUlid,
    },
}

// ----------------------------------------------------------- //
pub struct NatsIoHandler {
    jetstream_context: Context,
    stream: Stream,
    pub reply_secret: String,
}

#[derive(Debug, Clone)]
pub struct NatsIOEventStreamHandler {
    pub consumer: PullConsumer,
}

#[async_trait::async_trait]
impl EventHandler for NatsIoHandler {
    ///ToDo: Rust Doc
    async fn register_event(
        &self,
        message_variant: MessageVariant,
        message_id: Option<&DieselUlid>,
        subject: String,
    ) -> anyhow::Result<()> {
        // Encode message
        let json_message = serde_json::to_string_pretty(&message_variant)?;
        let message_bytes = Bytes::from(json_message);

        // Create header with block_id for deduplication
        let mut message_header: HeaderMap = HeaderMap::new();
        if let Some(msg_id) = message_id {
            message_header.append("block-id", msg_id.to_string().as_str())
        }

        // Publish message on stream
        match self
            .jetstream_context
            .publish_with_headers(subject, message_header, message_bytes)
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                log::error!("{err}");
                Err(err.into())
            }
        }
    }

    ///ToDo: Rust Doc
    async fn create_event_consumer(
        &self,
        event_type: EventType,
        delivery_policy: DeliverPolicy,
    ) -> anyhow::Result<(DieselUlid, Config)> {
        // Generate stream consumer id/name
        let consumer_id = DieselUlid::generate();

        // Generate consumer subject
        let consumer_subject = match event_type {
            EventType::Resource((resource_id, resource_type, inc_sub)) => {
                generate_resource_subject(&resource_id, resource_type, inc_sub)
            }
            EventType::User(user_id) => generate_user_subject(&user_id),
            EventType::Announcement(_) => generate_announcement_subject(), // Currently all announcement messages are consumed equally
            EventType::All => "AOS.>".to_string(),
        };

        // Define consumer config
        let consumer_config = Config {
            name: Some(consumer_id.to_string()),
            durable_name: Some(consumer_id.to_string()),
            filter_subject: consumer_subject,
            deliver_policy: delivery_policy,
            ..Default::default()
        };

        // Create consumer with the generated filter subject
        self.stream.create_consumer(consumer_config.clone()).await?;

        // Return consumer id
        return Ok((consumer_id, consumer_config));
    }

    ///ToDo: Rust Doc
    async fn get_event_consumer_messages(
        &self,
        event_consumer_id: String,
        batch_size: u32,
    ) -> anyhow::Result<Vec<Message>> {
        // Get consumer from Nats.io stream
        let consumer = match self.stream.get_consumer(&event_consumer_id).await {
            Ok(consumer) => consumer,
            Err(err) => return Err(anyhow::anyhow!(err)),
        };

        // Fetch all messages and deduplicate via the block_id in the header
        let mut fetch = consumer
            .fetch()
            .max_messages(i64::MAX as usize)
            .messages()
            .await?;

        let mut messages = Vec::new();
        let mut already_seen: HashSet<String, RandomState> = HashSet::default();
        while let Some(Ok(message)) = fetch.next().await {
            match &message.headers {
                Some(header_map) => match header_map.get("block-id") {
                    Some(header_val) => {
                        let block_id = header_val.to_string();
                        if already_seen.contains(&block_id) {
                            let _ = message.ack().await; // Acknowledge duplicate messages
                        } else {
                            already_seen.insert(block_id);
                            messages.push(message)
                        }
                    }
                    None => messages.push(message), // calculate_payload_hash(&message.payload) as alternative?
                },
                None => messages.push(message), // Notifications without header actually shouldn't exist
            };
        }

        // Shorten vector to requested batch size
        messages.truncate(batch_size as usize);

        Ok(messages)
    }

    //ToDo: Rust Doc
    async fn acknowledge_from_reply(&self, replies: Vec<Reply>) -> anyhow::Result<()> {
        // Create vector to collect Nats.io acknowledge replies
        let mut reply_ack = Vec::new();
        for reply in replies {
            // Validate reply hmac
            match validate_reply_msg(reply.clone(), self.reply_secret.clone()) {
                Ok(hmac_matches) => {
                    if !hmac_matches {
                        return Err(anyhow::anyhow!(
                            "Message hmac signature did not match original signature"
                        ));
                    }
                }
                Err(err) => {
                    log::error!("{err}");
                    return Err(err);
                    //anyhow::anyhow!("Could not validate reply msg")
                }
            }

            // Acknowledge message in Nats.io
            reply_ack.push(
                self.jetstream_context
                    .publish(reply.reply.clone(), "".into()),
            );
        }

        // Check if all messages could be acknowledged
        match try_join_all(reply_ack).await {
            Ok(_) => {}
            Err(err) => {
                return {
                    log::error!("{err}");
                    Err(anyhow::anyhow!("Could not acknowledge all messages"))
                }
            }
        }

        return Ok(());
    }

    ///ToDo: Rust Doc
    async fn create_event_stream_handler(
        &self,
        event_consumer_id: String,
    ) -> anyhow::Result<Box<dyn EventStreamHandler + Send + Sync>> {
        // Fetch consumer from Nats.io stream
        let consumer = self
            .stream
            .get_consumer(&event_consumer_id)
            .await
            .map_err(|err| anyhow::anyhow!(err))?;

        // Create and return event stream handler
        let stream_handler = Box::new(NatsIOEventStreamHandler { consumer });

        return Ok(stream_handler);
    }

    async fn delete_event_consumer(&self, event_consumer_id: String) -> anyhow::Result<()> {
        // Just remove the consumer from the stream
        self.stream.delete_consumer(&event_consumer_id).await?;

        // Return empty Ok to signal success
        Ok(())
    }

    async fn wait_for_acknowledgement(&self, subject: &str) -> anyhow::Result<()> {
        let mut backoff_counter = 0;
        'outer: while backoff_counter <= *MAX_RETRIES {
            // Get consumers
            let mut info = self.stream.consumers();
            // store consumers in hashmap
            let mut hash_set = HashSet::new();
            // iterate over consumer info
            'inner: while let Some(consumer) = info.try_next().await? {
                // returns false when already in hashmap
                if hash_set.insert(consumer.name) {
                    // Match consumers to proxy ids
                    if consumer.config.filter_subject.ends_with(&subject) {
                        // Wait for sync
                        if consumer.num_ack_pending != 0 {
                            backoff_counter += 1;
                            tokio::time::sleep(Duration::from_millis(
                                RETRY_TIMEOUT.pow(backoff_counter as u32),
                            ))
                            .await;
                            continue 'outer;
                        } else {
                            // consumer is synced
                            return Ok(());
                        }
                    } else {
                        // Iterate over next consumer info
                        continue 'inner;
                    };
                } else {
                    // If all consumers are visited and none ends with wanted subject,
                    // either wait for timeout or new consumer config
                    backoff_counter += 1;
                    tokio::time::sleep(Duration::from_millis(
                        RETRY_TIMEOUT.pow(backoff_counter as u32),
                    ))
                    .await;
                    continue 'outer;
                }
            }
            // If try_next() does not return anything,
            // either wait for timeout or wait for updated consumers config
            backoff_counter += 1;
            tokio::time::sleep(Duration::from_millis(
                RETRY_TIMEOUT.pow(backoff_counter as u32),
            ))
            .await;
            continue 'outer;
        }
        Err(anyhow!("Could not sync with {subject}"))
    }
}

impl NatsIoHandler {
    /// Initialize a new Nats.io jetstream client
    pub async fn new(
        nats_client: async_nats::Client,
        secret: String,
        stream_name: Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Create Nats.io Jetstream client
        let jetstream_context = async_nats::jetstream::new(nats_client);

        // Evaluate stream name
        let stream_name = stream_name.unwrap_or_else(|| STREAM_NAME.to_string());

        // Create minimalistic stream config
        let stream_config = async_nats::jetstream::stream::Config {
            name: stream_name.clone(),
            subjects: STREAM_SUBJECTS
                .into_iter()
                .map(|subject| subject.into())
                .collect(),
            ..Default::default()
        };

        // Create stream to publish messages
        let stream = jetstream_context
            .get_or_create_stream(stream_config)
            .await?;

        Ok(NatsIoHandler {
            jetstream_context,
            stream,
            reply_secret: secret,
        })
    }

    ///ToDo: Rust Doc
    pub async fn get_pull_consumer(
        &self,
        event_consumer_id: String,
    ) -> anyhow::Result<PullConsumer> {
        // Try to get pull consumer from stream
        Ok(match self.stream.get_consumer(&event_consumer_id).await {
            Ok(consumer) => consumer,
            Err(err) => return Err(anyhow::anyhow!(err)),
        })
    }

    /// Creates a Nats.io consumer which is a little bit more customizable than its
    /// counterpart for the external users.
    pub async fn create_internal_consumer(
        &self,
        consumer_id: DieselUlid,
        consumer_subject: String,
        delivery_policy: DeliverPolicy,
        ephemeral: bool,
    ) -> anyhow::Result<PullConsumer> {
        // Define consumer config
        let consumer_config = pull::Config {
            name: Some(consumer_id.to_string()),
            filter_subject: consumer_subject.clone(),
            durable_name: if ephemeral {
                None
            } else {
                Some(consumer_id.to_string())
            },
            deliver_policy: delivery_policy,
            inactive_threshold: Duration::from_secs(86400), // Remove consumer after 1 day idle
            ..Default::default()
        };

        // Create consumer with the generated config if not already exists
        Ok(self
            .stream
            .get_or_create_consumer(&consumer_id.to_string(), consumer_config.clone())
            .await?)

        // Return consumer id
        //Ok((consumer_id, consumer_config))
    }

    /// Only send event to a specific dataproxy
    pub async fn register_dataproxy_event(
        &self,
        object: &ObjectWithRelations,
        object_hierarchies: Vec<Hierarchy>,
        event_variant: EventVariant,
        endpoint_id: DieselUlid,
        message_id: Option<&DieselUlid>,
    ) -> anyhow::Result<()> {
        // Calculate resource checksum
        let resource_checksum = tonic_internal!(
            checksum_resource(generic_object_without_rules(object.clone())),
            "Checksum calculation failed"
        );

        // Evaluate number of notifications and the corresponding subjects
        let mut subjects = generate_resource_message_subjects(object_hierarchies);

        // Add individual endpoint subjects
        subjects.push(generate_endpoint_subject(&endpoint_id));

        // Create message payload
        let message_variant = MessageVariant::ResourceEvent(ResourceEvent {
            resource: Some(Resource {
                resource_id: object.object.id.to_string(),
                persistent_resource_id: object.object.dynamic,
                checksum: resource_checksum,
                resource_variant: ResourceVariant::from(object.object.object_type) as i32,
            }),
            event_variant: event_variant as i32,
            reply: None, // Will be filled on message fetch
        });

        // Emit resource event messages
        for subject in subjects {
            self.register_event(message_variant.clone(), message_id, subject)
                .await?
        }

        Ok(())
    }

    /// Convenience function to simplify the usage of NatsIoHandler::register_event(...)
    pub async fn register_resource_event(
        &self,
        object: &ObjectWithRelations,
        object_hierarchies: Vec<Hierarchy>,
        event_variant: EventVariant,
        message_id: Option<&DieselUlid>,
    ) -> anyhow::Result<()> {
        // Calculate resource checksum
        let resource_checksum = tonic_internal!(
            checksum_resource(generic_object_without_rules(object.clone())),
            "Checksum calculation failed"
        );

        // Evaluate number of notifications and the corresponding subjects
        let mut subjects = generate_resource_message_subjects(object_hierarchies);

        // Add individual endpoint subjects
        for endpoint_ulid in &object.object.endpoints.0 {
            subjects.push(generate_endpoint_subject(endpoint_ulid.key()))
        }

        // Create message payload
        let message_variant = MessageVariant::ResourceEvent(ResourceEvent {
            resource: Some(Resource {
                resource_id: object.object.id.to_string(),
                persistent_resource_id: object.object.dynamic,
                checksum: resource_checksum,
                resource_variant: ResourceVariant::from(object.object.object_type) as i32,
            }),
            event_variant: event_variant as i32,
            reply: None, // Will be filled on message fetch
        });

        // Emit resource event messages
        for subject in subjects {
            self.register_event(message_variant.clone(), message_id, subject)
                .await?
        }

        Ok(())
    }

    /// Convenience function to simplify the usage of NatsIoHandler::register_event(...)
    pub async fn register_user_event(
        &self,
        user: &User,
        event_variant: EventVariant,
    ) -> anyhow::Result<()> {
        // Calculate user checksum
        let user_checksum = tonic_internal!(
            checksum_user(&ApiUser::from(user.clone())),
            "User checksum calculation failed"
        );

        // Generate message subject
        let mut subjects = vec![generate_user_message_subject(&user.id.to_string())];

        // Add individual endpoint subjects
        for trusted_endpoint in &user.attributes.0.trusted_endpoints {
            subjects.push(generate_endpoint_subject(trusted_endpoint.key()))
        }

        // Emit user event messages
        for subject in subjects {
            self.register_event(
                MessageVariant::UserEvent(UserEvent {
                    user_id: user.id.to_string(),
                    event_variant: event_variant as i32,
                    checksum: user_checksum.clone(),
                    reply: None,
                }),
                None,
                subject,
            )
            .await?
        }

        Ok(())
    }

    /// Convenience function to simplify the usage of NatsIoHandler::register_event(...)
    pub async fn register_announcement_event(
        &self,
        announcement_type: AnnouncementVariant,
    ) -> anyhow::Result<()> {
        // Generate announcement message subject
        let subject = generate_announcement_message_subject(&announcement_type);

        // Emit message
        self.register_event(
            MessageVariant::AnnouncementEvent(AnnouncementEvent {
                reply: None,
                event_variant: Some(announcement_type),
            }),
            None,
            subject,
        )
        .await?;

        Ok(())
    }

    /// Function that is only internally available to publish messages in Nats.io which are only
    /// of interest for all ArunaServer instances.
    pub async fn register_server_event(&self, event_variant: ServerEvents) -> anyhow::Result<()> {
        // Serialize event to JSON for message payload
        let message_json = serde_json::to_string_pretty(&event_variant)?;

        // Create subject depending on ServerEvent
        let (subject, message) = match event_variant {
            ServerEvents::MVREFRESH(_) => ("AOS.SERVER.MVREFRESH", Bytes::from(message_json)),
            ServerEvents::CACHEUPDATE(_) => ("AOS.SERVER.CACHEUPDATE", Bytes::from(message_json)),
        };

        // Publish message in Nats.io
        self.jetstream_context.publish(subject, message).await?;

        Ok(())
    }

    /// Just acknowledges the message with the raw subject which was directly provided by Nats.
    ///
    /// ## Arguments:
    /// * `reply_subject` - A valid Nats.io provided replay subject
    ///
    /// ## Returns:
    /// * `anyhow::Result<()>` - An empty Ok() response signals success; Error else.
    pub async fn acknowledge_raw(&self, reply_subject: &str) -> anyhow::Result<()> {
        self.jetstream_context
            .publish(reply_subject.to_string(), "".into())
            .await?;

        Ok(())
    }
}

#[async_trait]
impl EventStreamHandler for NatsIOEventStreamHandler {
    async fn get_event_consumer_messages(
        &self,
        max_batch_size: u32,
    ) -> anyhow::Result<Vec<Message>> {
        // Fetch messages from Nats.io
        let mut batch = self
            .consumer
            .batch()
            .expires(Duration::from_millis(250))
            .max_messages(max_batch_size as usize)
            .messages()
            .await?;

        // Convert message batch to message vector
        let mut messages = Vec::new();
        while let Some(Ok(message)) = batch.next().await {
            messages.push(message);
        }

        Ok(messages)
    }
}
