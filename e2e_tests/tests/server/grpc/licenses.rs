use crate::common::{
    init, init_server,
    test_utils::{add_token, USER1_OIDC_TOKEN},
};
use aruna_rust_api::api::storage::{
    models::v2::{DataClass, License},
    services::v2::{
        collection_service_server::CollectionService, dataset_service_server::DatasetService,
        license_service_server::LicenseService, object_service_server::ObjectService,
        project_service_server::ProjectService, CreateCollectionRequest, CreateDatasetRequest,
        CreateLicenseRequest, CreateObjectRequest, CreateProjectRequest, GetLicenseRequest,
        ListLicensesRequest,
    },
};
use aruna_server::database::dsls::license_dsl::ALL_RIGHTS_RESERVED;
use itertools::Itertools;

#[tokio::test]
async fn create_and_get_licenses() {
    // Init
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create
    let create_request = CreateLicenseRequest {
        tag: "grpc_license_test".to_string(),
        name: "grpc license test".to_string(),
        text: "Tests grpc licenses".to_string(),
        url: "test.org/grpc-test-license".to_string(),
    };
    let second_create_request = CreateLicenseRequest {
        tag: "second_grpc_license_test".to_string(),
        name: "grpc license test".to_string(),
        text: "Tests grpc licenses".to_string(),
        url: "test.org/grpc-test-license".to_string(),
    };
    let first_response = server
        .license_service
        .create_license(add_token(
            tonic::Request::new(create_request.clone()),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap();
    let second_response = server
        .license_service
        .create_license(add_token(
            tonic::Request::new(second_create_request.clone()),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap();
    assert_eq!(first_response.into_inner().tag, create_request.tag.clone());
    assert_eq!(
        second_response.into_inner().tag,
        second_create_request.tag.clone()
    );

    // Get
    let get_request = GetLicenseRequest {
        tag: create_request.tag.clone(),
    };
    let get_response = server
        .license_service
        .get_license(add_token(
            tonic::Request::new(get_request),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner();
    let license = get_response.license.unwrap();
    assert_eq!(license.tag, create_request.tag);
    assert_eq!(license.name, create_request.name);
    assert_eq!(license.text, create_request.text);
    assert_eq!(license.url, create_request.url);

    // List
    let list_request = ListLicensesRequest {};
    let list_response = server
        .license_service
        .list_licenses(add_token(
            tonic::Request::new(list_request),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner();
    let compare_one = License {
        tag: create_request.tag,
        name: create_request.name,
        text: create_request.text,
        url: create_request.url,
    };
    let compare_two = License {
        tag: second_create_request.tag,
        name: second_create_request.name,
        text: second_create_request.text,
        url: second_create_request.url,
    };
    assert!(list_response.licenses.iter().contains(&compare_one));
    assert!(list_response.licenses.iter().contains(&compare_two));
}

#[tokio::test]
async fn default_licenses() {
    // Init
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    let create_license_request = CreateLicenseRequest {
        tag: "default_grpc_license_test".to_string(),
        name: "grpc default licenses test".to_string(),
        text: "Tests default licenses on grpc level".to_string(),
        url: "test.org/default-grpc-test-license".to_string(),
    };
    let second_create_license_request = CreateLicenseRequest {
        tag: "second_default_grpc_license_test".to_string(),
        name: "grpc default license test".to_string(),
        text: "Tests default licenses on grpc level".to_string(),
        url: "test.org/second_defaultgrpc-test-license".to_string(),
    };

    // Create project without licenses
    let create_project_with_defaults_request = CreateProjectRequest {
        name: "default-license-test-project".to_string(),
        title: "".to_string(),
        description: String::new(),
        key_values: Vec::new(),
        relations: Vec::new(),
        data_class: DataClass::Public as i32,
        preferred_endpoint: String::new(),
        metadata_license_tag: String::new(),
        default_data_license_tag: String::new(),
        authors: vec![],
    };
    let response = server
        .project_service
        .create_project(add_token(
            tonic::Request::new(create_project_with_defaults_request),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .project
        .unwrap();
    assert_eq!(
        response.metadata_license_tag,
        ALL_RIGHTS_RESERVED.to_string()
    );
    assert_eq!(
        response.default_data_license_tag,
        ALL_RIGHTS_RESERVED.to_string()
    );

    // Create licenses and project with license
    let mut create_project_request = CreateProjectRequest {
        name: "license-test-project".to_string(),
        title: "".to_string(),
        description: String::new(),
        key_values: Vec::new(),
        relations: Vec::new(),
        data_class: DataClass::Public as i32,
        preferred_endpoint: String::new(),
        metadata_license_tag: String::new(),
        default_data_license_tag: String::new(),
        authors: vec![],
    };
    let license_tag = server
        .license_service
        .create_license(add_token(
            tonic::Request::new(create_license_request),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .tag;
    let second_license_tag = server
        .license_service
        .create_license(add_token(
            tonic::Request::new(second_create_license_request),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .tag;
    create_project_request
        .metadata_license_tag
        .clone_from(&license_tag);
    create_project_request
        .default_data_license_tag
        .clone_from(&license_tag);
    let project = server
        .project_service
        .create_project(add_token(
            tonic::Request::new(create_project_request),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .project
        .unwrap();
    assert_eq!(project.metadata_license_tag, license_tag);
    assert_eq!(project.default_data_license_tag, license_tag);

    // Test if lower-hierarchy resources get parent licenses
    let create_collection_request = CreateCollectionRequest {
        name: "licenses_test_collection".to_string(),
        title: "".to_string(),
        description: String::new(),
        key_values: Vec::new(),
        relations: Vec::new(),
        data_class: DataClass::Public as i32,
        metadata_license_tag: None,
        default_data_license_tag: None,
        authors: vec![],
        parent: Some(aruna_rust_api::api::storage::services::v2::create_collection_request::Parent::ProjectId(project.id)),
    };
    let collection = server
        .collection_service
        .create_collection(add_token(
            tonic::Request::new(create_collection_request),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();
    assert_eq!(
        project.metadata_license_tag,
        collection.metadata_license_tag
    );
    assert_eq!(
        project.default_data_license_tag,
        collection.default_data_license_tag
    );

    // Test if overwrite sets new defaults for lower hierarchies
    let create_dataset_request = CreateDatasetRequest {
        name: "licenses_test_dataset".to_string(),
        title: "".to_string(),
        description: String::new(),
        key_values: Vec::new(),
        relations: Vec::new(),
        data_class: DataClass::Public as i32,
        metadata_license_tag: None,
        default_data_license_tag: Some(second_license_tag.clone()),
        authors: vec![],
        parent: Some(aruna_rust_api::api::storage::services::v2::create_dataset_request::Parent::CollectionId(collection.id)),
    };
    let dataset = server
        .dataset_service
        .create_dataset(add_token(
            tonic::Request::new(create_dataset_request),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();
    assert_eq!(dataset.metadata_license_tag, license_tag);
    assert_eq!(dataset.default_data_license_tag, second_license_tag);
    let create_object_request = CreateObjectRequest {
        name: "licenses_test_object".to_string(),
        title: "".to_string(),
        description: String::new(),
        key_values: Vec::new(),
        relations: Vec::new(),
        data_class: DataClass::Public as i32,
        metadata_license_tag: String::new(),
        data_license_tag: String::new(),
        hashes: Vec::new(),
        parent: Some(
            aruna_rust_api::api::storage::services::v2::create_object_request::Parent::DatasetId(
                dataset.id,
            ),
        ),
        authors: vec![],
    };
    let object = server
        .object_service
        .create_object(add_token(
            tonic::Request::new(create_object_request),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .object
        .unwrap();
    assert_eq!(object.metadata_license_tag, dataset.metadata_license_tag);
    assert_eq!(object.data_license_tag, dataset.default_data_license_tag);
}
