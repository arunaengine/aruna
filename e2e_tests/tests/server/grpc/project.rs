use std::str::FromStr;

use aruna_rust_api::api::storage::models::v2::Author;
use aruna_rust_api::api::storage::{
    models::v2::{DataClass, DataEndpoint, FullSync, KeyValue, KeyValueVariant, Status},
    services::v2::{
        project_service_server::ProjectService, ArchiveProjectRequest, CreateProjectRequest,
        DeleteProjectRequest, GetProjectRequest, GetProjectsRequest, UpdateProjectDataClassRequest,
        UpdateProjectDescriptionRequest, UpdateProjectKeyValuesRequest, UpdateProjectNameRequest,
    },
};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use tonic::Request;

use crate::common::{
    init, init_server,
    test_utils::{
        add_token, fast_track_grpc_project_create, rand_string, ADMIN_OIDC_TOKEN,
        DEFAULT_ENDPOINT_ULID, USER1_OIDC_TOKEN,
    },
};
use aruna_server::database::{
    crud::CrudDb,
    dsls::{license_dsl::ALL_RIGHTS_RESERVED, object_dsl::Object},
    enums::ObjectStatus,
};

#[tokio::test]
async fn grpc_create_project() {
    // Init ProjectService
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create request with OIDC token in header
    let project_name = rand_string(32).to_lowercase();

    let create_request = CreateProjectRequest {
        name: project_name.to_string(),
        title: "this-is-a-title-test".to_string(),
        description: "Something".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: DataClass::Public as i32,
        preferred_endpoint: "".to_string(),
        default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        authors: vec![Author {
            first_name: "A".to_string(),
            last_name: "B".to_string(),
            email: Some("C".to_string()),
            orcid: None,
            id: None,
        }],
    };

    let grpc_request = add_token(Request::new(create_request), ADMIN_OIDC_TOKEN);

    // Create project via gRPC service
    let create_response = server
        .project_service
        .create_project(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_project = create_response.project.unwrap();

    assert!(!proto_project.id.is_empty());
    assert_eq!(proto_project.name, project_name);
    assert_eq!(&proto_project.description, "Something");
    assert_eq!(proto_project.key_values, vec![]);
    assert_eq!(proto_project.data_class, 1);
    assert_eq!(proto_project.relations, vec![]);
    assert_eq!(proto_project.status, Status::Available as i32);
    assert!(proto_project.dynamic);
    assert_eq!(&proto_project.title, "this-is-a-title-test");
    assert!(proto_project.authors.iter().contains(&Author {
        first_name: "A".to_string(),
        last_name: "B".to_string(),
        email: Some("C".to_string()),
        orcid: None,
        id: None,
    }));
    assert_eq!(
        proto_project.endpoints,
        vec![DataEndpoint {
            id: DEFAULT_ENDPOINT_ULID.to_string(),
            variant: Some(
                aruna_rust_api::api::storage::models::v2::data_endpoint::Variant::FullSync(
                    FullSync {}
                )
            ),
            status: None
        }]
    );
}

#[tokio::test]
async fn grpc_get_projects() {
    // Init ProjectService
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create multiple projects
    let project_01 =
        fast_track_grpc_project_create(&server.project_service, USER1_OIDC_TOKEN).await;
    let project_02 =
        fast_track_grpc_project_create(&server.project_service, USER1_OIDC_TOKEN).await;

    // Fetch single project
    let get_request = GetProjectRequest {
        project_id: project_01.id.to_string(),
    };

    let grpc_request = add_token(Request::new(get_request), USER1_OIDC_TOKEN);

    let get_response = server
        .project_service
        .get_project(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_project = get_response.project.unwrap();

    assert_eq!(project_01, proto_project);

    // Fetch all projects
    let get_all_request = GetProjectsRequest {
        project_ids: vec![project_01.id.to_string(), project_02.id.to_string()],
    };

    let grpc_request = add_token(Request::new(get_all_request), USER1_OIDC_TOKEN);

    let get_all_response = server
        .project_service
        .get_projects(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_projects = get_all_response.projects;

    for project in vec![project_01, project_02] {
        assert!(proto_projects.contains(&project))
    }
}

#[tokio::test]
async fn grpc_update_project() {
    // Init ProjectService
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create multiple projects
    let project = fast_track_grpc_project_create(&server.project_service, USER1_OIDC_TOKEN).await;

    // Update project name
    let update_name_request = add_token(
        Request::new(UpdateProjectNameRequest {
            project_id: project.id.to_string(),
            name: "updated-name".to_string(),
        }),
        ADMIN_OIDC_TOKEN,
    );

    let updated_project = server
        .project_service
        .update_project_name(update_name_request)
        .await
        .unwrap()
        .into_inner()
        .project
        .unwrap();

    assert_ne!(project.name, updated_project.name);
    assert_eq!(updated_project.name, "updated-name".to_string());

    // Update project description
    let update_description_request = add_token(
        Request::new(UpdateProjectDescriptionRequest {
            project_id: project.id.to_string(),
            description: "Updated description.".to_string(),
        }),
        ADMIN_OIDC_TOKEN,
    );

    let updated_project = server
        .project_service
        .update_project_description(update_description_request)
        .await
        .unwrap()
        .into_inner()
        .project
        .unwrap();

    assert_ne!(project.description, updated_project.description);
    assert_eq!(
        updated_project.description,
        "Updated description.".to_string()
    );

    // Update project key-values (add)
    let add_kv_request = add_token(
        Request::new(UpdateProjectKeyValuesRequest {
            project_id: project.id.to_string(),
            add_key_values: vec![KeyValue {
                key: "validated".to_string(),
                value: "true".to_string(),
                variant: KeyValueVariant::Label as i32,
            }],
            remove_key_values: vec![],
        }),
        ADMIN_OIDC_TOKEN,
    );

    let updated_project = server
        .project_service
        .update_project_key_values(add_kv_request)
        .await
        .unwrap()
        .into_inner()
        .project
        .unwrap();

    assert_ne!(project.key_values, updated_project.key_values);
    assert_eq!(
        updated_project.key_values,
        vec![KeyValue {
            key: "validated".to_string(),
            value: "true".to_string(),
            variant: KeyValueVariant::Label as i32,
        }]
    );

    // Update project key-values (combine add and remove)
    let update_kv_request = add_token(
        Request::new(UpdateProjectKeyValuesRequest {
            project_id: project.id.to_string(),
            add_key_values: vec![KeyValue {
                key: "validated".to_string(),
                value: "false".to_string(),
                variant: KeyValueVariant::Label as i32,
            }],
            remove_key_values: updated_project.key_values, // Remove all
        }),
        ADMIN_OIDC_TOKEN,
    );

    let updated_project = server
        .project_service
        .update_project_key_values(update_kv_request)
        .await
        .unwrap()
        .into_inner()
        .project
        .unwrap();

    assert_ne!(project.key_values, updated_project.key_values);
    assert_eq!(
        updated_project.key_values,
        vec![KeyValue {
            key: "validated".to_string(),
            value: "false".to_string(),
            variant: KeyValueVariant::Label as i32,
        }]
    );

    // Update project dataclass (Error)
    let mut update_dataclass_request = UpdateProjectDataClassRequest {
        project_id: project.id.to_string(),
        data_class: DataClass::Workspace as i32,
    };

    let grpc_request = add_token(
        Request::new(update_dataclass_request.clone()),
        ADMIN_OIDC_TOKEN,
    );

    let updated_project = server
        .project_service
        .update_project_data_class(grpc_request)
        .await;

    assert!(updated_project.is_err()); // Cannot tighten dataclass

    // Update project dataclass
    update_dataclass_request.data_class = DataClass::Public as i32;

    let grpc_request = add_token(Request::new(update_dataclass_request), ADMIN_OIDC_TOKEN);

    let updated_project = server
        .project_service
        .update_project_data_class(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .project
        .unwrap();

    assert_ne!(project.data_class, updated_project.data_class);
    assert_eq!(updated_project.data_class, DataClass::Public as i32);

    // Archive project
    let archive_project_request = add_token(
        Request::new(ArchiveProjectRequest {
            project_id: project.id,
        }),
        ADMIN_OIDC_TOKEN,
    );

    let updated_project = server
        .project_service
        .archive_project(archive_project_request)
        .await
        .unwrap()
        .into_inner()
        .project
        .unwrap();

    assert!(!updated_project.dynamic)
}

#[tokio::test]
async fn grpc_delete_project() {
    // Init ProjectService
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random project
    let project = fast_track_grpc_project_create(&server.project_service, USER1_OIDC_TOKEN).await;
    let project_ulid = DieselUlid::from_str(&project.id).unwrap();

    // Delete random project
    let delete_project_request = add_token(
        Request::new(DeleteProjectRequest {
            project_id: project.id.to_string(),
        }),
        ADMIN_OIDC_TOKEN,
    );

    server
        .project_service
        .delete_project(delete_project_request)
        .await
        .unwrap();

    // Fetch project and check status
    let deleted_project = Object::get(project_ulid, &server.db_conn.get_client().await.unwrap())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(deleted_project.object_status, ObjectStatus::DELETED)
}
