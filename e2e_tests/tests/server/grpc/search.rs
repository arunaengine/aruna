use aruna_rust_api::api::storage::{
    models::v2::DataClass,
    services::v2::{
        collection_service_server::CollectionService, project_service_server::ProjectService,
        search_service_server::SearchService, user_service_server::UserService,
        CreateCollectionRequest, CreateProjectRequest, GetPersonalNotificationsRequest,
        GetResourceRequest, GetResourcesRequest, PersonalNotificationVariant, Reference,
        ReferenceType, RequestResourceAccessRequest,
    },
};
use aruna_server::database::{dsls::license_dsl::ALL_RIGHTS_RESERVED, enums::ObjectType};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use tonic::Request;

use crate::common::{
    init, init_server,
    test_utils::{
        add_token, fast_track_grpc_collection_create, fast_track_grpc_project_create, rand_string,
        INVALID_OIDC_TOKEN, USER1_OIDC_TOKEN, USER1_ULID, USER2_OIDC_TOKEN, USER2_ULID,
    },
};

#[tokio::test]
async fn grpc_request_access() {
    // Init gRPC services
    let infra = init::init_test_environment(false).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random project
    let project = fast_track_grpc_project_create(&server.project_service, USER1_OIDC_TOKEN).await;

    let project_ulid = DieselUlid::from_str(&project.id).unwrap();
    let user2_ulid = DieselUlid::from_str(USER2_ULID).unwrap();

    // Request access of non-existing resource
    let mut inner_request = RequestResourceAccessRequest {
        resource_id: DieselUlid::generate().to_string(),
        message: "My message to you.".to_string(),
    };

    let grpc_request = add_token(Request::new(inner_request.clone()), USER2_OIDC_TOKEN);

    let response = server
        .search_service
        .request_resource_access(grpc_request)
        .await;

    assert!(response.is_err());

    // Request access of without token
    inner_request.resource_id = project_ulid.to_string();

    let grpc_request = Request::new(inner_request.clone());

    let response = server
        .search_service
        .request_resource_access(grpc_request)
        .await;

    assert!(response.is_err());

    // Request access of resource
    let grpc_request = add_token(Request::new(inner_request.clone()), USER2_OIDC_TOKEN);

    server
        .search_service
        .request_resource_access(grpc_request)
        .await
        .unwrap();

    // Validate personal notification of user1
    let grpc_request = add_token(
        Request::new(GetPersonalNotificationsRequest {}),
        USER1_OIDC_TOKEN,
    );

    let notifications = server
        .user_service
        .get_personal_notifications(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .notifications;

    let user2 = server.cache.get_user(&user2_ulid).unwrap();

    // Have to loop all notifications as id is unknown
    let mut validated = false;
    for notification in &notifications {
        if notification.message
            == format!(
                "{} ({}) requests access for {:?} {} ({})",
                user2.display_name,
                user2.id,
                ObjectType::PROJECT,
                project.name,
                project_ulid
            )
        {
            assert_eq!(
                notification.variant,
                PersonalNotificationVariant::AccessRequested as i32
            );
            assert!(notification.refs.contains(&Reference {
                ref_type: ReferenceType::Resource as i32,
                ref_name: project.name.clone(),
                ref_value: project_ulid.to_string()
            }));
            assert!(notification.refs.contains(&Reference {
                ref_type: ReferenceType::User as i32,
                ref_name: "another-test-user".to_string(),
                ref_value: USER2_ULID.to_string()
            }));
            validated = true;
            break;
        }
    }
    if !validated {
        panic!("Revoked permission personal notification does not exist")
    }
}
#[tokio::test]
async fn get_resource() {
    // Init
    let infra = init::init_test_environment(false).await;
    let server = init_server::init_server_services(&infra).await;

    // Create public project
    let create_request = CreateProjectRequest {
        name: rand_string(32).to_lowercase(),
        title: "".to_string(),
        description: "".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: DataClass::Public as i32,
        default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        preferred_endpoint: "".to_string(),
        authors: vec![],
    };
    let grpc_request = add_token(Request::new(create_request), USER1_OIDC_TOKEN);
    let public_project = server
        .project_service
        .create_project(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .project
        .unwrap();

    // Create private project and collection
    let private_project =
        fast_track_grpc_project_create(&server.project_service, USER1_OIDC_TOKEN).await;
    let parent =
        aruna_rust_api::api::storage::services::v2::create_collection_request::Parent::ProjectId(
            private_project.id.to_string(),
        );
    let private_collection = fast_track_grpc_collection_create(
        &server.collection_service,
        USER1_OIDC_TOKEN,
        parent.clone(),
    )
    .await;

    // Create confidential collection
    let create_request = CreateCollectionRequest {
        name: rand_string(32),
        title: "".to_string(),
        description: "".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: DataClass::Confidential as i32,
        parent: Some(parent),
        default_data_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        metadata_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        authors: vec![],
    };
    let grpc_request = add_token(Request::new(create_request), USER1_OIDC_TOKEN);
    let confidential_collection = server
        .collection_service
        .create_collection(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    // ------------------- TESTS ----------------------------------
    //
    // Test access without permissions and token
    //
    let public_request = GetResourceRequest {
        resource_id: public_project.id.to_string(),
    };
    let response = server
        .search_service
        .get_resource(Request::new(public_request.clone()))
        .await
        .unwrap()
        .into_inner()
        .resource
        .unwrap()
        .resource
        .unwrap()
        .resource
        .unwrap();
    let project = match response {
        aruna_rust_api::api::storage::models::v2::generic_resource::Resource::Project(project) => {
            project
        }
        _ => panic!("This should be a project"),
    };
    // Should contain created by and endpoint
    assert_eq!(project.created_by, public_project.created_by.to_string());
    assert_eq!(project.endpoints, public_project.endpoints);

    // Should not contain created_by and endpoint
    let private_request = GetResourcesRequest {
        resource_ids: vec![
            private_project.id.to_string(),
            private_collection.id.to_string(),
        ],
    };
    let response = server
        .search_service
        .get_resources(Request::new(private_request.clone()))
        .await
        .unwrap()
        .into_inner()
        .resources;
    let project = response
        .iter()
        .find_map(
            |res| match res.resource.as_ref().unwrap().resource.as_ref().unwrap() {
                aruna_rust_api::api::storage::models::v2::generic_resource::Resource::Project(
                    proj,
                ) => Some(proj),
                _ => None,
            },
        )
        .unwrap();
    let collection =
        response
            .iter()
            .find_map(|res| {
                match res.resource.as_ref().unwrap().resource.as_ref().unwrap() {
                aruna_rust_api::api::storage::models::v2::generic_resource::Resource::Collection(
                   col
                ) => Some(col),
                _ => None,
            }
            })
            .unwrap();
    assert!(project.endpoints.is_empty());
    assert_eq!(project.created_by, DieselUlid::default().to_string());
    assert!(collection.endpoints.is_empty());
    assert_eq!(collection.created_by, DieselUlid::default().to_string());

    // This should error with permission denied
    let confidential_request = GetResourceRequest {
        resource_id: confidential_collection.id.to_string(),
    };
    let response = server
        .search_service
        .get_resource(Request::new(confidential_request.clone()))
        .await;
    assert!(response.is_err());

    //
    // Test with token and without permissions
    //
    let response = server
        .search_service
        .get_resource(add_token(
            Request::new(public_request.clone()),
            USER2_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .resource
        .unwrap()
        .resource
        .unwrap()
        .resource
        .unwrap();
    let project = match response {
        aruna_rust_api::api::storage::models::v2::generic_resource::Resource::Project(project) => {
            project
        }
        _ => panic!("This should be a project"),
    };

    // Same as above
    assert_eq!(project.created_by, public_project.created_by.to_string());
    assert_eq!(project.endpoints, public_project.endpoints);
    let response = server
        .search_service
        .get_resources(add_token(
            Request::new(private_request.clone()),
            INVALID_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .resources;
    let project = response
        .iter()
        .find_map(
            |res| match res.resource.as_ref().unwrap().resource.as_ref().unwrap() {
                aruna_rust_api::api::storage::models::v2::generic_resource::Resource::Project(
                    proj,
                ) => Some(proj),
                _ => None,
            },
        )
        .unwrap();
    let collection =
        response
            .iter()
            .find_map(|res| {
                match res.resource.as_ref().unwrap().resource.as_ref().unwrap() {
                aruna_rust_api::api::storage::models::v2::generic_resource::Resource::Collection(
                   col
                ) => Some(col),
                _ => None,
            }
            })
            .unwrap();
    assert!(project.endpoints.is_empty());
    assert_eq!(project.created_by, DieselUlid::default().to_string());
    assert!(collection.endpoints.is_empty());
    assert_eq!(collection.created_by, DieselUlid::default().to_string());
    let response = server
        .search_service
        .get_resource(add_token(
            Request::new(confidential_request.clone()),
            INVALID_OIDC_TOKEN,
        ))
        .await;
    assert!(response.is_err());

    //
    // Test with token and permissions
    //
    let response = server
        .search_service
        .get_resource(add_token(Request::new(public_request), USER1_OIDC_TOKEN))
        .await
        .unwrap()
        .into_inner()
        .resource
        .unwrap()
        .resource
        .unwrap()
        .resource
        .unwrap();
    let project = match response {
        aruna_rust_api::api::storage::models::v2::generic_resource::Resource::Project(project) => {
            project
        }
        _ => panic!("This should be a project"),
    };

    // Same as above
    assert_eq!(project.created_by, public_project.created_by.to_string());
    assert_eq!(project.endpoints, public_project.endpoints);
    let response = server
        .search_service
        .get_resources(add_token(Request::new(private_request), USER1_OIDC_TOKEN))
        .await
        .unwrap()
        .into_inner()
        .resources;
    let project = response
        .iter()
        .find_map(
            |res| match res.resource.as_ref().unwrap().resource.as_ref().unwrap() {
                aruna_rust_api::api::storage::models::v2::generic_resource::Resource::Project(
                    proj,
                ) => Some(proj),
                _ => None,
            },
        )
        .unwrap();
    let collection =
        response
            .iter()
            .find_map(|res| {
                match res.resource.as_ref().unwrap().resource.as_ref().unwrap() {
                aruna_rust_api::api::storage::models::v2::generic_resource::Resource::Collection(
                   col
                ) => Some(col),
                _ => None,
            }
            })
            .unwrap();
    assert!(!project.endpoints.is_empty());
    assert_eq!(project.created_by, USER1_ULID);
    assert!(!collection.endpoints.is_empty());
    assert_eq!(collection.created_by, USER1_ULID);
    let response = server
        .search_service
        .get_resource(add_token(
            Request::new(confidential_request),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .resource
        .unwrap()
        .resource
        .unwrap()
        .resource
        .unwrap();
    let confidential_collection = match response {
        aruna_rust_api::api::storage::models::v2::generic_resource::Resource::Collection(col) => {
            col
        }
        _ => panic!("This should be a collection"),
    };
    assert!(!confidential_collection.endpoints.is_empty());
    assert_eq!(confidential_collection.created_by, USER1_ULID);
}
