use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_server::{
    database::{
        crud::CrudDb,
        dsls::{
            internal_relation_dsl::InternalRelation, notification_dsl::StreamConsumer,
            object_dsl::Object,
        },
        enums::ObjectType,
    },
    notification::{
        handler::{EventHandler, EventType},
        natsio_handler::NatsIoHandler,
    },
};
use async_nats::jetstream::consumer::{Config, DeliverPolicy};
use diesel_ulid::DieselUlid;

use crate::common::init::init_database;

mod common;

#[tokio::test]
async fn create_stream_consumer() {
    // Init database connection
    let db = init_database().await;
    let client = db.get_client().await.unwrap();

    // Define stream consumer
    let mut stream_consumer = StreamConsumer {
        id: DieselUlid::generate(),
        user_id: None,
        config: postgres_types::Json(Config {
            name: Some("some_consumer".to_string()),
            durable_name: Some("some_consumer".to_string()),
            filter_subject: "some_subject".to_string(),
            ..Default::default()
        }),
    };

    // Persist stream consumer in database
    stream_consumer.create(&client).await.unwrap();

    // Validate stream consumer creation
    if let Some(created_consumer) = StreamConsumer::get(stream_consumer.id, &client)
        .await
        .unwrap()
    {
        assert_eq! {created_consumer.id, stream_consumer.id};
        assert_eq! {created_consumer.user_id, stream_consumer.user_id};
        assert_eq! {created_consumer.config, stream_consumer.config};
    } else {
        panic!("StreamConsumer should exist.")
    }
}

#[tokio::test]
async fn get_stream_consumer() {
    // Init database connection
    let db = init_database().await;
    let client = db.get_client().await.unwrap();

    // Define stream consumer
    let mut stream_consumer = StreamConsumer {
        id: DieselUlid::generate(),
        user_id: None,
        config: postgres_types::Json(Config {
            name: Some("some_consumer".to_string()),
            durable_name: Some("some_consumer".to_string()),
            filter_subject: "some_subject".to_string(),
            ..Default::default()
        }),
    };

    // Persist stream consumer in database
    stream_consumer.create(&client).await.unwrap();

    // Fetch stream consumer from database and validate equality
    if let Some(get_consumer) = StreamConsumer::get(stream_consumer.id, &client)
        .await
        .unwrap()
    {
        assert_eq! {get_consumer, stream_consumer}
    } else {
        panic!("StreamConsumer should exist.")
    }
}

#[tokio::test]
async fn delete_stream_consumer() {
    // Init database connection
    let db = init_database().await;
    let client = db.get_client().await.unwrap();

    // Define stream consumer
    let mut stream_consumer = StreamConsumer {
        id: DieselUlid::generate(),
        user_id: None,
        config: postgres_types::Json(Config {
            name: Some("some_consumer".to_string()),
            durable_name: Some("some_consumer".to_string()),
            filter_subject: "some_subject".to_string(),
            ..Default::default()
        }),
    };

    // Persist stream consumer in database
    stream_consumer.create(&client).await.unwrap();

    // Fetch stream consumer from database and validate equality
    if let Some(get_consumer) = StreamConsumer::get(stream_consumer.id, &client)
        .await
        .unwrap()
    {
        assert_eq! {get_consumer, stream_consumer}
    } else {
        panic!("StreamConsumer should exist.")
    }

    // Delete stream consumer
    stream_consumer.delete(&client).await.unwrap();

    // Try to fetch deleted stream consumer
    assert!(StreamConsumer::get(stream_consumer.id, &client)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn resource_notification_test() {
    // Init Nats.io connection
    let nats_client = async_nats::connect("0.0.0.0:4222").await.unwrap();
    let nats_handler = NatsIoHandler::new(nats_client, "ThisIsASecretToken".to_string(), None)
        .await
        .unwrap();

    // Init database connection
    let db = init_database().await;
    let client = db.get_client().await.unwrap();

    // Create random user
    let mut user = common::test_utils::new_user(vec![]);
    let random_user_id = user.id;
    user.create(&client).await.unwrap();

    // Create dummy hierarchy
    let project_001 =
        common::test_utils::new_object(random_user_id, DieselUlid::generate(), ObjectType::PROJECT);
    let project_002 =
        common::test_utils::new_object(random_user_id, DieselUlid::generate(), ObjectType::PROJECT);
    let project_003 =
        common::test_utils::new_object(random_user_id, DieselUlid::generate(), ObjectType::PROJECT);
    let collection = common::test_utils::new_object(
        random_user_id,
        DieselUlid::generate(),
        ObjectType::COLLECTION,
    );
    let dataset_001 =
        common::test_utils::new_object(random_user_id, DieselUlid::generate(), ObjectType::DATASET);
    let dataset_002 =
        common::test_utils::new_object(random_user_id, DieselUlid::generate(), ObjectType::DATASET);
    let object =
        common::test_utils::new_object(random_user_id, DieselUlid::generate(), ObjectType::OBJECT);

    let proj_coll_001 = common::test_utils::new_internal_relation(&project_001, &collection);
    let proj_coll_002 = common::test_utils::new_internal_relation(&project_002, &collection);
    let proj_data = common::test_utils::new_internal_relation(&project_003, &dataset_002);
    let coll_data_001 = common::test_utils::new_internal_relation(&collection, &dataset_001);
    let coll_data_002 = common::test_utils::new_internal_relation(&collection, &dataset_002);
    let data_obj_001 = common::test_utils::new_internal_relation(&dataset_001, &object);
    let data_obj_002 = common::test_utils::new_internal_relation(&dataset_002, &object);

    Object::batch_create(
        &vec![
            project_001.clone(),
            project_002.clone(),
            project_003.clone(),
            collection,
            dataset_001,
            dataset_002,
            object.clone(),
        ],
        &client,
    )
    .await
    .unwrap();

    InternalRelation::batch_create(
        &vec![
            proj_coll_001,
            proj_coll_002,
            proj_data,
            coll_data_001,
            coll_data_002,
            data_obj_001,
            data_obj_002,
        ],
        &client,
    )
    .await
    .unwrap();

    // Create stream consumer in Nats.io for all of the projects
    let (proj_001_consumer_id, _) = nats_handler
        .create_event_consumer(
            EventType::Resource((project_001.id.to_string(), ObjectType::PROJECT, true)),
            DeliverPolicy::All,
        )
        .await
        .unwrap();
    let (proj_002_consumer_id, _) = nats_handler
        .create_event_consumer(
            EventType::Resource((project_002.id.to_string(), ObjectType::PROJECT, true)),
            DeliverPolicy::All,
        )
        .await
        .unwrap();
    let (proj_003_consumer_id, _) = nats_handler
        .create_event_consumer(
            EventType::Resource((project_003.id.to_string(), ObjectType::PROJECT, true)),
            DeliverPolicy::All,
        )
        .await
        .unwrap();

    // Send notification for object creation
    let object_plus = Object::get_object_with_relations(&object.id, &client)
        .await
        .unwrap();

    // Warm up database query layer
    object.fetch_object_hierarchies(&client).await.unwrap();

    // Performance time.
    use std::time::Instant;
    let now = Instant::now();
    let object_hierarchies = object.fetch_object_hierarchies(&client).await.unwrap();
    let elapsed = now.elapsed();
    println!("Path fetch: {elapsed:.2?}");

    let now = Instant::now();
    nats_handler
        .register_resource_event(
            &object_plus,
            object_hierarchies,
            EventVariant::Created,
            Some(&DieselUlid::generate()),
        )
        .await
        .unwrap();
    let elapsed = now.elapsed();
    println!("Notification emit: {elapsed:.2?}");

    // Give Nats time to process the published messages as Github Actions machines are slow...
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Evaluate number of messages received
    let proj_001_messages = nats_handler
        .get_event_consumer_messages(proj_001_consumer_id.to_string(), 10)
        .await
        .unwrap();
    assert_eq!(proj_001_messages.len(), 1); // Actually 2 were send but deduplicated

    let proj_002_messages = nats_handler
        .get_event_consumer_messages(proj_002_consumer_id.to_string(), 10)
        .await
        .unwrap();

    assert_eq!(proj_002_messages.len(), 1); // Actually 2 were send but deduplicated

    let proj_003_messages = nats_handler
        .get_event_consumer_messages(proj_003_consumer_id.to_string(), 10)
        .await
        .unwrap();

    assert_eq!(proj_003_messages.len(), 1);
}
