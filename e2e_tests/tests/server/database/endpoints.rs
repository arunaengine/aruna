use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::endpoint_dsl::{Endpoint, HostConfigs};
use aruna_server::database::enums::{EndpointStatus, EndpointVariant, ObjectType};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::str::FromStr;
use tokio_postgres::GenericClient;

use crate::common::init::init_database;
use crate::common::init_server::Component;
use crate::common::test_utils::USER1_ULID;
use crate::common::{infra, test_utils};

#[tokio::test]
async fn create_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do endpoint stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let ep_id = DieselUlid::generate();
    let doc_obj = DieselUlid::generate();

    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let mut create_doc = test_utils::new_object(user_id, doc_obj, ObjectType::OBJECT);
    create_doc.create(client).await.unwrap();

    let mut endpoint = Endpoint {
        id: ep_id,
        name: "create_test".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: Some(doc_obj),
        is_public: true,
        status: EndpointStatus::AVAILABLE,
    };
    endpoint.create(client).await.unwrap();

    let new = Endpoint::get(ep_id, client).await.unwrap().unwrap();
    assert_eq!(endpoint, new);
}

#[tokio::test]
async fn delete_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do endpoint stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let ep_id = DieselUlid::generate();
    let doc_obj = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let mut create_doc = test_utils::new_object(user_id, doc_obj, ObjectType::OBJECT);
    create_doc.create(client).await.unwrap();

    let mut endpoint = Endpoint {
        id: ep_id,
        name: "delete_test".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: Some(doc_obj),
        is_public: true,
        status: EndpointStatus::AVAILABLE,
    };
    endpoint.create(client).await.unwrap();

    Endpoint::delete_by_id(&ep_id, client).await.unwrap();
    assert!(Endpoint::get(ep_id, client).await.unwrap().is_none());
}

#[tokio::test]
async fn get_by_tests() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do endpoint stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let ep_id = DieselUlid::generate();
    let doc_obj = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let mut create_doc = test_utils::new_object(user_id, doc_obj, ObjectType::OBJECT);
    create_doc.create(client).await.unwrap();
    let unique_name = DieselUlid::generate().to_string(); // Endpoint names need to be unique
    let mut endpoint = Endpoint {
        id: ep_id,
        name: unique_name.clone(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: Some(doc_obj),
        is_public: true,
        status: EndpointStatus::AVAILABLE,
    };
    endpoint.create(client).await.unwrap();

    let new = Endpoint::get_by_name(unique_name, client)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(endpoint, new);
}
