use crate::common::{init, init_server};
use aruna_rust_api::api::storage::services::v2::{
    CreateEndpointRequest, DeleteEndpointRequest, GetEndpointRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::endpoint_dsl::{Endpoint, HostConfigs};
use aruna_server::database::dsls::pub_key_dsl::PubKey;
use aruna_server::database::enums::{EndpointStatus, EndpointVariant};
use aruna_server::middlelayer::endpoints_request_types::{CreateEP, DeleteEP, GetEP};
use diesel_ulid::DieselUlid;
use postgres_types::Json;

#[tokio::test]
async fn test_create_ep() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();
    let mut pk = PubKey {
        id: 0,
        proxy: None,
        pubkey: "MCowBQYDK2VwAyEAnwnN68pHig/AXGyFb2IttslBN93+72kBRSf3vCmSi7w=".to_string(),
    };
    pk.create(&client).await.unwrap();

    // test
    let request = CreateEP(CreateEndpointRequest {
        name: "endpoint_test".to_string(),
        ep_variant: 1,
        is_public: true,
        pubkey: "MCowBQYDK2VwAyEAWBBLB9+sOZ4pSjM7U3DCSoq5R4xQYG4W27iwI1QoMN0=".to_string(),
        host_configs: vec![],
    });

    let (ep, pk) = db_handler.create_endpoint(request).await.unwrap();
    assert_eq!(ep.name, "endpoint_test".to_string());
    assert_eq!(ep.endpoint_variant, EndpointVariant::PERSISTENT);
    assert!(ep.is_public);
    assert!(ep.host_config.0 .0.is_empty());
    assert!(pk.proxy.is_some());
    assert_eq!(
        pk.pubkey,
        "MCowBQYDK2VwAyEAWBBLB9+sOZ4pSjM7U3DCSoq5R4xQYG4W27iwI1QoMN0=".to_string()
    );
    ep.delete(&client).await.unwrap();
}

#[tokio::test]
async fn test_get_ep() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();
    let ep_id = DieselUlid::generate();
    let mut pk = PubKey {
        id: 5001,
        proxy: Some(ep_id),
        pubkey: "MCowBQYDK2VwAyEAskJBFNbcuMzONfHosX1+60kFejaIVJdM8kr13IL/69U=".to_string(),
    };
    let mut endpoint = Endpoint {
        id: ep_id,
        name: "get_test".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: None,
        is_public: false,
        status: EndpointStatus::INITIALIZING,
    };
    endpoint.create(&client).await.unwrap();
    pk.create(&client).await.unwrap();

    // test
    let request_by_id = GetEP(GetEndpointRequest {
        endpoint: Some(
            aruna_rust_api::api::storage::services::v2::get_endpoint_request::Endpoint::EndpointId(
                ep_id.to_string(),
            ),
        ),
    });
    let request_by_name = GetEP(GetEndpointRequest {
        endpoint: Some(
            aruna_rust_api::api::storage::services::v2::get_endpoint_request::Endpoint::EndpointName(
               "get_test".to_string(), 
            ),
        ),
    });
    let by_id = db_handler.get_endpoint(request_by_id).await.unwrap();
    let by_name = db_handler.get_endpoint(request_by_name).await.unwrap();
    assert_eq!(endpoint, by_id);
    assert_eq!(endpoint, by_name);
    endpoint.delete(&client).await.unwrap();
}

#[tokio::test]
async fn test_get_all() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();
    let ep_one = DieselUlid::generate();
    let ep_two = DieselUlid::generate();
    let ep_three = DieselUlid::generate();
    let endpoint_one = Endpoint {
        id: ep_one,
        name: "all_test_one".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: None,
        is_public: false,
        status: EndpointStatus::AVAILABLE,
    };
    let endpoint_two = Endpoint {
        id: ep_two,
        name: "all_test_two".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: None,
        is_public: false,
        status: EndpointStatus::AVAILABLE,
    };
    let endpoint_three = Endpoint {
        id: ep_three,
        name: "all_test_three".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: None,
        is_public: false,
        status: EndpointStatus::AVAILABLE,
    };
    let mut eps = [endpoint_one, endpoint_two, endpoint_three];
    for ep in eps.iter_mut() {
        ep.create(&client).await.unwrap();
    }
    // test
    let all = db_handler.get_endpoints().await.unwrap();
    assert!(eps.iter().all(|ep| all.contains(ep)));
    for ep in eps {
        ep.delete(&client).await.unwrap();
    }
}

#[tokio::test]
async fn test_delete_ep() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();
    let ep = DieselUlid::generate();
    let mut endpoint = Endpoint {
        id: ep,
        name: "delete_test".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: None,
        is_public: false,
        status: EndpointStatus::AVAILABLE,
    };
    endpoint.create(&client).await.unwrap();

    // test
    let request = DeleteEP(DeleteEndpointRequest {
        endpoint_id: ep.to_string(),
    });
    db_handler.delete_endpoint(request).await.unwrap();
    assert!(Endpoint::get(ep, &client).await.unwrap().is_none());
}
#[tokio::test]
async fn test_get_default_ep() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();
    let ep = DieselUlid::generate();
    let mut endpoint = Endpoint {
        id: ep,
        name: "default_test".to_string(),
        host_config: Json(HostConfigs(Vec::new())),
        endpoint_variant: EndpointVariant::PERSISTENT,
        documentation_object: None,
        is_public: false,
        status: EndpointStatus::AVAILABLE,
    };
    endpoint.create(&client).await.unwrap();
}
