use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v2::{EndpointHostConfig, EndpointHostVariant, EndpointVariant},
    services::v2::{
        endpoint_service_server::EndpointService, get_endpoint_request, CreateEndpointRequest,
        GetEndpointRequest, GetEndpointsRequest,
    },
};
use diesel_ulid::DieselUlid;
use tonic::Request;

use crate::common::{
    init,
    init_server::init_endpoint_service,
    test_utils::{add_token, ADMIN_OIDC_TOKEN, DEFAULT_ENDPOINT_ULID, USER1_OIDC_TOKEN},
};

#[tokio::test]
async fn grpc_create_endpoint() {
    // Init gRPC EndpointService
    let infra = init::init_test_environment(true).await;
    let endpoint_service = init_endpoint_service(&infra.postgres, &infra.nats).await;

    // Create endpoint request
    let create_request = CreateEndpointRequest {
        name: "Dummy-Endpoint-001".to_string(),
        ep_variant: EndpointVariant::Persistent as i32,
        is_public: true,
        pubkey: "MCowBQYDK2VwAyEADKwfn6ZRS0On6akwcR0XE5mF4kMhi8Rv+nsHKD/9+MQ=".to_string(),
        host_configs: vec![],
    };

    // Try create endpoint with insufficient permissions
    let grpc_request = add_token(Request::new(create_request.clone()), USER1_OIDC_TOKEN);

    let create_response = endpoint_service.create_endpoint(grpc_request).await;

    assert!(create_response.is_err());

    // Try create endpoint with sufficient permissions
    let grpc_request = add_token(Request::new(create_request.clone()), ADMIN_OIDC_TOKEN);

    let create_response = endpoint_service
        .create_endpoint(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_endpoint = create_response.endpoint.unwrap();
    let _proto_endpoint_id = DieselUlid::from_str(&proto_endpoint.id).unwrap();
    assert!(proto_endpoint.is_public);
    assert_eq!(proto_endpoint.name, create_request.name);
    assert_eq!(proto_endpoint.ep_variant(), EndpointVariant::Persistent);
    assert_eq!(proto_endpoint.host_configs, vec![])
}

#[tokio::test]
async fn grpc_get_endpoint() {
    // Init gRPC EndpointService
    let infra = init::init_test_environment(true).await;
    let endpoint_service = init_endpoint_service(&infra.postgres, &infra.nats).await;

    // Create endpoint request
    let create_request = CreateEndpointRequest {
        name: "Dummy-Endpoint-002".to_string(),
        ep_variant: EndpointVariant::Persistent as i32,
        is_public: false,
        pubkey: "MCowBQYDK2VwAyEAaFkg6e2cLgyB7mfL9zxTB+P9x9cP567okYHm7o9/7Wg=".to_string(),
        host_configs: vec![EndpointHostConfig {
            url: "https://proxy.wherever.aruna-storage.org".to_string(),
            is_primary: false,
            ssl: true,
            public: true,
            host_variant: EndpointHostVariant::Grpc as i32,
        }],
    };

    // Try create endpoint with sufficient permissions
    let grpc_request = add_token(Request::new(create_request.clone()), ADMIN_OIDC_TOKEN);

    let private_endpoint = endpoint_service
        .create_endpoint(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .endpoint
        .unwrap();

    // Get public endpoint without authorization
    let mut get_request = GetEndpointRequest {
        endpoint: Some(get_endpoint_request::Endpoint::EndpointId(
            DEFAULT_ENDPOINT_ULID.to_string(),
        )),
    };

    let public_endpoint = endpoint_service
        .get_endpoint(Request::new(get_request.clone()))
        .await
        .unwrap()
        .into_inner()
        .endpoint
        .unwrap();

    assert_eq!(&public_endpoint.id, DEFAULT_ENDPOINT_ULID);

    // Try get private endpoint with insufficient permissions
    get_request.endpoint = Some(get_endpoint_request::Endpoint::EndpointId(
        private_endpoint.id.to_string(),
    ));
    let grpc_request = add_token(Request::new(get_request.clone()), USER1_OIDC_TOKEN);

    let response = endpoint_service.get_endpoint(grpc_request).await;

    assert!(response.is_err());

    // Try get private endpoint with sufficient permissions
    let grpc_request = add_token(Request::new(get_request.clone()), ADMIN_OIDC_TOKEN);

    let private_endpoint_again = endpoint_service
        .get_endpoint(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .endpoint
        .unwrap();

    assert_eq!(private_endpoint_again, private_endpoint);

    // Try get all endpoints without authorization
    let get_request = GetEndpointsRequest {};

    let endpoints = endpoint_service
        .get_endpoints(Request::new(get_request.clone()))
        .await
        .unwrap()
        .into_inner()
        .endpoints;

    assert!(endpoints.contains(&public_endpoint));
    assert!(!endpoints.contains(&private_endpoint));

    // Try get all endpoints as global admin
    let grpc_request = add_token(Request::new(get_request.clone()), ADMIN_OIDC_TOKEN);

    let endpoints = endpoint_service
        .get_endpoints(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .endpoints;

    assert!(endpoints.contains(&public_endpoint));
    assert!(endpoints.contains(&private_endpoint));
}
