use crate::common::test_utils::{add_token, rand_string};
use aruna_rust_api::api::storage::models::v2::{DataClass, Project};
use aruna_rust_api::api::storage::services::v2::endpoint_service_client::EndpointServiceClient;
use aruna_rust_api::api::storage::services::v2::object_service_client::ObjectServiceClient;
use aruna_rust_api::api::storage::services::v2::project_service_client::ProjectServiceClient;
use aruna_rust_api::api::storage::services::v2::storage_status_service_client::StorageStatusServiceClient;
use aruna_rust_api::api::storage::services::v2::user_service_client::UserServiceClient;
use aruna_rust_api::api::storage::services::v2::{
    CreateApiTokenRequest, CreateApiTokenResponse, CreateProjectRequest,
    CreateS3CredentialsUserTokenRequest, CreateS3CredentialsUserTokenResponse, GetEndpointsRequest,
    GetEndpointsResponse, GetObjectRequest, GetObjectResponse, GetPubkeysRequest,
    GetPubkeysResponse, GetStorageStatusRequest, GetStorageStatusResponse, GetUserRequest,
    GetUserResponse,
};
use aruna_server::database::dsls::license_dsl::ALL_RIGHTS_RESERVED;
use tonic::transport::Channel;
use tonic::Request;

pub async fn get_info_service_client(server_port: u16) -> StorageStatusServiceClient<Channel> {
    // Create connection to the Aruna instance via gRPC
    let endpoint = Channel::from_shared(format!("http://0.0.0.0:{server_port}")).unwrap();
    let channel = endpoint.connect().await.unwrap();

    // Create the individual client services
    StorageStatusServiceClient::new(channel.clone())
}

// ----- Project service functions -----//
pub async fn get_project_client(server_port: u16) -> ProjectServiceClient<Channel> {
    // Create connection to the Aruna instance via gRPC
    let endpoint = Channel::from_shared(format!("http://0.0.0.0:{server_port}")).unwrap();
    let channel = endpoint.connect().await.unwrap();

    // Create the individual client services
    ProjectServiceClient::new(channel.clone())
}

pub async fn create_project(server_port: u16, token: &str) -> Project {
    // Create request with token
    let project_name = rand_string(32).to_lowercase();
    let create_request = CreateProjectRequest {
        name: project_name.to_string(),
        title: "title-test".to_string(),
        description: "".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: DataClass::Private as i32,
        preferred_endpoint: "".to_string(),
        default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        authors: vec![aruna_rust_api::api::storage::models::v2::Author {
            first_name: "Jane".to_string(),
            last_name: "Doe".to_string(),
            email: Some("jane.doe@test.org".to_string()),
            orcid: None,
            id: None,
        }],
    };

    let grpc_request = add_token(Request::new(create_request), token);

    // Create project via gRPC service
    let mut project_service = get_project_client(server_port).await;
    let proto_project = project_service
        .create_project(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .project
        .unwrap();

    assert!(!proto_project.id.is_empty());
    assert_eq!(proto_project.name, project_name);

    proto_project
}
// ----- End project service functions -----//

// ----- User service functions ----- //
pub async fn get_user_client(server_port: u16) -> UserServiceClient<Channel> {
    // Create connection to the Aruna instance via gRPC
    let endpoint = Channel::from_shared(format!("http://0.0.0.0:{server_port}")).unwrap();
    let channel = endpoint.connect().await.unwrap();

    // Create the individual client services
    UserServiceClient::new(channel.clone())
}

pub async fn get_user(server_port: u16, token: &str) -> GetUserResponse {
    // Create request
    let request = add_token(
        Request::new(GetUserRequest {
            user_id: "".to_string(),
        }),
        token,
    );

    // Send request against server
    let mut client = get_user_client(server_port).await;
    client.get_user(request).await.unwrap().into_inner()
}

pub async fn create_api_token(server_port: u16, token: &str) -> CreateApiTokenResponse {
    // Create request
    let request = add_token(
        Request::new(CreateApiTokenRequest {
            name: "Test pers token".to_string(),
            permission: None,
            expires_at: None,
        }),
        token,
    );

    // Send request against server
    let mut client = get_user_client(server_port).await;
    client.create_api_token(request).await.unwrap().into_inner()
}

pub async fn create_s3_credentials(
    token: &str,
    server_port: u16,
    endpoint_ulid: &str,
) -> CreateS3CredentialsUserTokenResponse {
    // Create request
    let request = add_token(
        Request::new(CreateS3CredentialsUserTokenRequest {
            endpoint_id: endpoint_ulid.to_string(),
        }),
        token,
    );

    // Send request against server
    let mut client = get_user_client(server_port).await;
    client
        .create_s3_credentials_user_token(request)
        .await
        .unwrap()
        .into_inner()
}
// ----- End user service functions ----- //

// ----- Object service functions ----- //
pub async fn get_object_client(server_port: u16) -> ObjectServiceClient<Channel> {
    // Create connection to the Aruna instance via gRPC
    let endpoint = Channel::from_shared(format!("http://0.0.0.0:{server_port}")).unwrap();
    let channel = endpoint.connect().await.unwrap();

    // Create the individual client services
    ObjectServiceClient::new(channel.clone())
}

pub async fn get_object(server_port: u16, token: &str, object_ulid: &str) -> GetObjectResponse {
    // Create request
    dbg!(object_ulid);
    let request = add_token(
        Request::new(GetObjectRequest {
            object_id: object_ulid.to_string(),
        }),
        token,
    );

    // Send request against server
    let mut client = get_object_client(server_port).await;
    client.get_object(request).await.unwrap().into_inner()
}
// ----- End object service functions ----- //

pub async fn get_server_status(server_port: u16) -> GetStorageStatusResponse {
    // Create connection to the Aruna instance via gRPC
    let endpoint = Channel::from_shared(format!("http://0.0.0.0:{server_port}")).unwrap();
    let channel = endpoint.connect().await.unwrap();
    let mut service = StorageStatusServiceClient::new(channel.clone());

    let req = Request::new(GetStorageStatusRequest {});
    service.get_storage_status(req).await.unwrap().into_inner()
}

pub async fn get_pubkeys(server_port: u16) -> GetPubkeysResponse {
    // Create connection to the Aruna instance via gRPC
    let endpoint = Channel::from_shared(format!("http://0.0.0.0:{server_port}")).unwrap();
    let channel = endpoint.connect().await.unwrap();
    let mut service = StorageStatusServiceClient::new(channel.clone());

    let req = Request::new(GetPubkeysRequest {});
    service.get_pubkeys(req).await.unwrap().into_inner()
}

pub async fn get_endpoints(
    server_port: u16,
) -> Result<tonic::Response<GetEndpointsResponse>, tonic::Status> {
    // Create connection to the Aruna instance via gRPC
    let endpoint = Channel::from_shared(format!("http://0.0.0.0:{server_port}")).unwrap();
    let channel = endpoint.connect().await.unwrap();
    let mut service = EndpointServiceClient::new(channel.clone());

    let req = Request::new(GetEndpointsRequest {});
    service.get_endpoints(req).await
}
