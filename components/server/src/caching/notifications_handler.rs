use std::{str::FromStr, sync::Arc};

use anyhow::{anyhow, bail};
use aruna_rust_api::api::storage::models::v2::User as ApiUser;
use aruna_rust_api::api::{
    notification::services::v2::{
        announcement_event::EventVariant as AnnEventVariant, event_message::MessageVariant,
        AnnouncementEvent, EventVariant, ResourceEvent, UserEvent,
    },
    storage::models::v2::generic_resource,
};
use async_channel::Sender;
use async_nats::jetstream::consumer::DeliverPolicy;
use chrono::Utc;
use diesel_ulid::DieselUlid;
use futures::StreamExt;
use jsonwebtoken::DecodingKey;
use log::{debug, error};
use time::OffsetDateTime;

use crate::database::dsls::pub_key_dsl::PubKey;
use crate::database::dsls::rule_dsl::{Rule, RuleBinding};
use crate::notification::natsio_handler::{Action, Created, Deleted, ServerEvents, Updated};
use crate::notification::utils::build_rule;
use crate::search::meilisearch_client::{MeilisearchClient, ObjectDocument};
use crate::utils::search_utils;
use crate::{
    database::{
        connection::Database,
        crud::CrudDb,
        dsls::{object_dsl::Object, user_dsl::User},
    },
    notification::natsio_handler::NatsIoHandler,
    utils::grpc_utils::{checksum_resource, checksum_user},
};

use super::cache::Cache;
use super::structs::PubKeyEnum;

pub struct NotificationHandler {}

impl NotificationHandler {
    ///ToDo: Rust Doc
    pub async fn new(
        database: Arc<Database>,
        cache: Arc<Cache>,
        natsio_handler: Arc<NatsIoHandler>,
        search_client: Arc<MeilisearchClient>,
        refresh_sender: Sender<i64>,
    ) -> anyhow::Result<Self> {
        // Create standard pull consumer to fetch all notifications
        /*
        let (some1, _) = natsio_handler
            .create_event_consumer(EventType::All, DeliverPolicy::All)
            .await?;
        */

        // Create ephemeral pull consumer which fetches only messages from the time of startup
        let pull_consumer = natsio_handler
            .create_internal_consumer(
                DieselUlid::generate(),
                "AOS.>".to_string(), // All event notifications
                DeliverPolicy::ByStartTime {
                    start_time: tonic_invalid!(
                        OffsetDateTime::from_unix_timestamp(Utc::now().timestamp()),
                        "Incorrect timestamp format"
                    ),
                },
                true,
            )
            .await?;

        //Todo: Persist consumer for ArunaServer instances?
        //let pull_consumer = natsio_handler.get_pull_consumer(some1.to_string()).await?;

        // Async move the consumer listening
        let mut messages = pull_consumer.messages().await?;
        let cache_clone = cache.clone();
        let database_clone = database.clone();
        let sender_arc = Arc::new(refresh_sender);
        tokio::spawn(async move {
            loop {
                if let Some(Ok(nats_message)) = messages.next().await {
                    log_received!(&nats_message);

                    if nats_message.subject.starts_with("AOS.SERVER") {
                        let msg_variant = match serde_json::from_slice(
                            nats_message.message.payload.to_vec().as_slice(),
                        ) {
                            Ok(variant) => variant,
                            Err(err) => {
                                return Err::<MessageVariant, anyhow::Error>(anyhow::anyhow!(err))
                            }
                        };

                        let _ = process_server_event(
                            &database_clone,
                            &cache_clone,
                            msg_variant,
                            sender_arc.clone(),
                        )
                        .await;
                    } else {
                        // Deserialize messages in gRPC definitions
                        let msg_variant = match serde_json::from_slice(
                            nats_message.message.payload.to_vec().as_slice(),
                        ) {
                            Ok(variant) => variant,
                            Err(err) => {
                                return Err::<MessageVariant, anyhow::Error>(anyhow::anyhow!(err))
                            }
                        };

                        // Update cache depending on message variant
                        match NotificationHandler::update_server_cache(
                            msg_variant,
                            cache_clone.clone(),
                            database_clone.clone(),
                            search_client.clone(),
                        )
                        .await
                        {
                            Ok(_) => debug!("NotificationHandler cache update successful"),
                            Err(err) => {
                                error!("NotificationHandler cache update failed: {err}")
                            }
                        }
                    }

                    // Acknowledge received message in every case because the only way cache update can fail is through the database query.
                    //   We have to trust that messages will only be sent if all database operations have been
                    //   successful in advance and the database has a consistent status in relation to the message being sent.
                    //   This means that in case of an error, the message does not represent the current state of the database,
                    //   but is faulty or the database itself is not accessible.
                    match &nats_message.reply {
                        Some(reply_subject) => {
                            if let Err(err) = natsio_handler.acknowledge_raw(reply_subject).await {
                                error!("NotificationHandler message acknowledgement failed: {err}")
                            }
                        }
                        None => error!("Nats message does not contain replay subject"),
                    };
                }
            }
        });

        // Return ... something
        Ok(NotificationHandler {})
    }

    ///ToDo: Rust Doc
    async fn update_server_cache(
        message: MessageVariant,
        cache: Arc<Cache>,
        database: Arc<Database>,
        search_client: Arc<MeilisearchClient>,
    ) -> anyhow::Result<()> {
        match message {
            MessageVariant::ResourceEvent(event) => {
                process_resource_event(event, cache, database, search_client).await?
            }
            MessageVariant::UserEvent(event) => {
                process_user_event(event, cache, database).await?;
            }
            MessageVariant::AnnouncementEvent(event) => {
                process_announcement_event(event, cache, database).await?;
            }
        }

        Ok(())
    }
}

/* -------------- Helper -------------- */
async fn process_resource_event(
    resource_event: ResourceEvent,
    cache: Arc<Cache>,
    database: Arc<Database>,
    search_client: Arc<MeilisearchClient>,
) -> anyhow::Result<()> {
    if let Some(resource) = resource_event.resource {
        // Process cache
        if let Ok(variant) = EventVariant::try_from(resource_event.event_variant) {
            // Extract resource id to validate format
            let resource_ulid = DieselUlid::from_str(&resource.resource_id)?;

            // Update resource cache for all resources affected by the received notification
            for res_ulid in match variant {
                EventVariant::Unspecified => bail!("Unspecified event variant not allowed"),
                EventVariant::Created
                | EventVariant::Available
                | EventVariant::Updated
                | EventVariant::Deleted => vec![resource_ulid],
                EventVariant::Snapshotted => {
                    let client = database.get_client().await?;
                    let mut ids = vec![resource_ulid];
                    ids.append(
                        &mut Object::fetch_subresources_by_id(&resource_ulid, &client).await?,
                    );
                    ids
                }
            } {
                if let Some(object_plus) = cache.get_wrapped_object(&res_ulid) {
                    // Convert to proto and compare checksum
                    let proto_resource: generic_resource::Resource = object_plus.clone().into();
                    let proto_checksum = checksum_resource(proto_resource.clone())?;

                    if proto_checksum != resource.checksum {
                        // Update updated object in cache and search index
                        cache.upsert_object(&res_ulid, object_plus.object_with_relations.clone());

                        // Update resource search index only for public/private resources
                        search_utils::update_search_index(
                            &search_client,
                            &cache,
                            vec![ObjectDocument::try_from(proto_resource)?],
                        )
                        .await;
                    }
                } else {
                    // Fetch object with relations from database and put into cache
                    let client = database.get_client().await?;
                    let object_plus = Object::get_object_with_relations(&res_ulid, &client).await?;

                    // Update resource search index only for public/private resources
                    search_utils::update_search_index(
                        &search_client,
                        &cache,
                        vec![ObjectDocument::from(object_plus.object.clone())],
                    )
                    .await;

                    // Add to cache
                    cache.insert_object(object_plus);
                }
            }
        } else {
            // Return error if variant is None
            bail!("Resource event variant missing")
        }
    } else {
        // Return error
        bail!("Resource event resource missing")
    }

    Ok(())
}

async fn process_user_event(
    user_event: UserEvent,
    cache: Arc<Cache>,
    database: Arc<Database>,
) -> anyhow::Result<()> {
    // Extract user id
    let user_ulid = DieselUlid::from_str(&user_event.user_id)?;

    // Process cache
    if let Ok(variant) = EventVariant::try_from(user_event.event_variant) {
        match variant {
            EventVariant::Unspecified => bail!("Unspecified user event variant not allowed"),
            EventVariant::Created | EventVariant::Updated | EventVariant::Available => {
                // Check if user already exists
                if let Some(user) = cache.get_user(&user_ulid) {
                    // Convert to proto and compare checksum
                    let proto_user = ApiUser::from(user.clone());
                    let proto_checksum = checksum_user(&proto_user)?;

                    if proto_checksum != user_event.checksum {
                        cache.update_user(&user_ulid, user);
                    }
                } else {
                    // Fetch user from database and add to cache
                    let client = database.get_client().await?;
                    if let Some(user) = User::get(user_ulid, &client).await? {
                        cache.add_user(user_ulid, user)
                    } else {
                        bail!("User does not exist")
                    }
                }
            }
            EventVariant::Deleted => cache.remove_user(&user_ulid),
            _ => {}
        }
    } else {
        // Return error if variant is None
        bail!("User event variant missing")
    }

    Ok(())
}

async fn process_announcement_event(
    announcement_event: AnnouncementEvent,
    cache: Arc<Cache>,
    database: Arc<Database>,
) -> anyhow::Result<()> {
    if let Some(variant) = announcement_event.event_variant {
        match variant {
            AnnEventVariant::NewDataProxyId(id) => {
                //Note: Endpoint cache currently not implemented
                debug!("Received NewDataProxyId announcement for: {id}")
            }
            AnnEventVariant::RemoveDataProxyId(id) => {
                debug!("Received RemoveDataProxy announcement for: {id}");

                // Removes endpoint from all users/resources in cache with full sync.
                // As the endpoint was already removed from all users and resources on the database
                //  level this should reset the cache to full consistency with the current database status.
                cache.sync_cache(database).await?
            }
            AnnEventVariant::UpdateDataProxyId(id) => {
                //Note: Endpoint cache currently not implemented
                debug!("Received UpdateDataProxyId announcement for: {id}");

                //ToDo: Update id in all users/resources and pubkeys?
            }
            AnnEventVariant::NewPubkey(serial) => {
                // Fetch pubkey from database
                let client = database.get_client().await?;
                let serial_i16: i16 = serial.try_into()?;
                let pubkey = PubKey::get(serial_i16, &client)
                    .await?
                    .ok_or_else(|| anyhow!("Could not find pub key"))?;
                let pub_pem = format!(
                    "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
                    pubkey.pubkey
                );
                let decoding_key = DecodingKey::from_ed_pem(pub_pem.as_bytes())?;

                // Insert pubkey in cache
                let cache_pubkey = match pubkey.proxy {
                    Some(endpoint_id) => {
                        PubKeyEnum::DataProxy((pubkey.pubkey, decoding_key, endpoint_id))
                    }
                    None => PubKeyEnum::Server((pubkey.pubkey, decoding_key)),
                };

                cache.add_pubkey(pubkey.id, cache_pubkey);
            }
            AnnEventVariant::RemovePubkey(serial) => cache.remove_pubkey(serial.try_into()?),
            AnnEventVariant::Downtime(info) => {
                //ToDo: Implement downtime/shutdown preparation
                //  - Set instance status to something like `HealthStatus::Shutdown`
                //  - Do not accept any new requests
                debug!("Received Downtime announcement: {info:?}")
            }
            AnnEventVariant::Version(version) => {
                debug!("Received Version announcement: {version:?}")
            }
        }
    } else {
        // Return error if variant is None
        bail!("Announcement event variant missing")
    }

    Ok(())
}

async fn process_server_event(
    db_handler: &Arc<Database>,
    cache: &Arc<Cache>,
    server_event: ServerEvents,
    sender: Arc<Sender<i64>>,
) -> anyhow::Result<()> {
    match server_event {
        ServerEvents::MVREFRESH(refresh_timestamp) => {
            if let Err(err) = sender.send(refresh_timestamp).await {
                error!("Failed to send refresh started timestamp to stats loop: {err}")
            }
        }
        ServerEvents::CACHEUPDATE(action) => match action {
            Action::Created(Created::Rule(id)) | Action::Updated(Updated::Rule(id)) => {
                let client = db_handler.get_client().await?;
                let rule = Rule::get(id, &client)
                    .await?
                    .ok_or_else(|| anyhow!("Rule not found"))?;
                let cached = build_rule(rule)?;
                cache.insert_rule(&id, cached);
            }
            Action::Created(Created::RuleBinding {
                rule_id,
                origin_id,
                resource_id,
            })
            | Action::Updated(Updated::RuleBinding {
                rule_id,
                origin_id,
                resource_id,
            }) => {
                let client = db_handler.get_client().await?;
                let binding = RuleBinding::get_by(rule_id, origin_id, resource_id, &client)
                    .await?
                    .ok_or_else(|| anyhow!("Rule not found"))?;
                let resource_ids = if binding.cascading {
                    let mut ids = vec![binding.origin_id];
                    ids.append(&mut cache.get_subresources(&binding.origin_id)?);
                    ids
                } else {
                    vec![binding.origin_id]
                };
                cache.insert_rule_binding(resource_ids, binding);
            }
            Action::Deleted(Deleted::Rule(id)) => {
                cache.delete_rule(&id);
            }
            Action::Deleted(Deleted::RuleBinding {
                resource_id,
                rule_id,
            }) => {
                cache.remove_rule_bindings(resource_id, rule_id);
            }
        },
    }

    Ok(())
}
