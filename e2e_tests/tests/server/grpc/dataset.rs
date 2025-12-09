use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v2::{
        relation::Relation::Internal, DataClass, DataEndpoint, FullSync, InternalRelation,
        InternalRelationVariant, KeyValue, KeyValueVariant, Relation, RelationDirection,
        ResourceVariant, Status,
    },
    services::v2::{
        create_collection_request, create_dataset_request::Parent,
        dataset_service_server::DatasetService, CreateDatasetRequest, DeleteDatasetRequest,
        GetDatasetRequest, GetDatasetsRequest, SnapshotDatasetRequest,
        UpdateDatasetDataClassRequest, UpdateDatasetDescriptionRequest,
        UpdateDatasetKeyValuesRequest, UpdateDatasetNameRequest,
    },
};
use diesel_ulid::DieselUlid;
use tonic::Request;

use crate::common::{
    init, init_server,
    test_utils::{
        add_token, fast_track_grpc_collection_create, fast_track_grpc_dataset_create,
        fast_track_grpc_get_dataset, fast_track_grpc_permission_add,
        fast_track_grpc_project_create, ADMIN_OIDC_TOKEN, DEFAULT_ENDPOINT_ULID, USER1_OIDC_TOKEN,
        USER1_ULID,
    },
};
use aruna_server::database::{dsls::license_dsl::ALL_RIGHTS_RESERVED, enums::DbPermissionLevel};

#[tokio::test]
async fn grpc_create_dataset() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random project
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;

    // Create request with OIDC token in header
    let mut inner_request = CreateDatasetRequest {
        name: "test-dataset".to_string(),
        title: "".to_string(),
        description: "Test Description".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: DataClass::Private as i32,
        parent: Some(Parent::ProjectId(project.id.to_string())),
        default_data_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        metadata_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        authors: vec![],
    };
    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    // Create dataset in project via gRPC service
    let create_response = server
        .dataset_service
        .create_dataset(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_dataset = create_response.dataset.unwrap();

    assert!(!proto_dataset.id.is_empty());
    assert_eq!(proto_dataset.name, "test-dataset");
    assert_eq!(&proto_dataset.description, "Test Description");
    assert_eq!(proto_dataset.key_values, vec![]);
    assert_eq!(
        proto_dataset.relations,
        vec![Relation {
            relation: Some(Internal(InternalRelation {
                resource_id: project.id.to_string(),
                resource_variant: ResourceVariant::Project as i32,
                defined_variant: InternalRelationVariant::BelongsTo as i32,
                custom_variant: None,
                direction: RelationDirection::Inbound as i32,
            }))
        }]
    );
    assert_eq!(proto_dataset.data_class, 2);
    assert_eq!(proto_dataset.status, Status::Available as i32);
    assert!(proto_dataset.dynamic);
    assert_eq!(
        proto_dataset.endpoints,
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

    // Create dataset in collection via gRPC service
    let collection = fast_track_grpc_collection_create(
        &server.collection_service,
        ADMIN_OIDC_TOKEN,
        create_collection_request::Parent::ProjectId(project.id.clone()),
    )
    .await;

    inner_request.parent = Some(Parent::CollectionId(collection.id.to_string()));

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let create_response = server
        .dataset_service
        .create_dataset(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_dataset = create_response.dataset.unwrap();

    assert!(!proto_dataset.id.is_empty());
    assert_eq!(proto_dataset.name, "test-dataset");
    assert_eq!(&proto_dataset.description, "Test Description");
    assert_eq!(proto_dataset.key_values, vec![]);
    assert_eq!(
        proto_dataset.relations,
        vec![Relation {
            relation: Some(Internal(InternalRelation {
                resource_id: collection.id.to_string(),
                resource_variant: ResourceVariant::Collection as i32,
                defined_variant: InternalRelationVariant::BelongsTo as i32,
                custom_variant: None,
                direction: RelationDirection::Inbound as i32,
            }))
        }]
    );
    assert_eq!(proto_dataset.data_class, 2);
    assert_eq!(proto_dataset.status, Status::Available as i32);
    assert!(proto_dataset.dynamic);
    assert_eq!(
        proto_dataset.endpoints,
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
async fn grpc_get_dataset() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Get normal user id as DieselUlid
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();

    // Create random project and dataset
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;

    // Create dataset
    let dataset = fast_track_grpc_dataset_create(
        &server.dataset_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;
    let dataset_ulid = DieselUlid::from_str(&dataset.id).unwrap();

    // Try get Dataset that does not exist
    let mut get_request = GetDatasetRequest {
        dataset_id: DieselUlid::generate().to_string(),
    };

    let tokenless_request = add_token(Request::new(get_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server.dataset_service.get_dataset(tokenless_request).await;

    assert!(response.is_err());

    // Try get Dataset without token
    get_request.dataset_id.clone_from(&dataset.id);

    let response = server
        .dataset_service
        .get_dataset(Request::new(get_request.clone()))
        .await;

    assert!(response.is_err());

    // Get Dataset without permissions
    let grpc_request = add_token(Request::new(get_request.clone()), USER1_OIDC_TOKEN);

    let response = server.dataset_service.get_dataset(grpc_request).await;

    assert!(response.is_err());

    // Get Dataset with permissions
    let grpc_request = add_token(Request::new(get_request.clone()), USER1_OIDC_TOKEN);

    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &dataset_ulid,
        DbPermissionLevel::READ,
    )
    .await;

    let proto_dataset = server
        .dataset_service
        .get_dataset(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();

    // Validate returned dataset
    assert_eq!(dataset.id, proto_dataset.id);
    assert_eq!(dataset.name, proto_dataset.name);
    assert_eq!(dataset.description, proto_dataset.description);
    assert_eq!(dataset.key_values, proto_dataset.key_values);
    assert_eq!(
        proto_dataset.relations,
        vec![Relation {
            relation: Some(Internal(InternalRelation {
                resource_id: project.id.to_string(),
                resource_variant: ResourceVariant::Project as i32,
                defined_variant: InternalRelationVariant::BelongsTo as i32,
                custom_variant: None,
                direction: RelationDirection::Inbound as i32,
            }))
        }]
    );
    assert_eq!(dataset.data_class, proto_dataset.data_class);
    assert_eq!(dataset.status, proto_dataset.status);
    assert_eq!(dataset.dynamic, proto_dataset.dynamic);
}

#[tokio::test]
async fn grpc_get_datasets() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Get normal user id as DieselUlid
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();

    // Create random project and dataset
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;

    // Create datasets
    let dataset_01 = fast_track_grpc_dataset_create(
        &server.dataset_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;
    let dataset_02 = fast_track_grpc_dataset_create(
        &server.dataset_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;
    let dataset_01_ulid = DieselUlid::from_str(&dataset_01.id).unwrap();
    let dataset_02_ulid = DieselUlid::from_str(&dataset_02.id).unwrap();

    // Try get Datasets that does not exist
    let mut inner_request = GetDatasetsRequest {
        dataset_ids: vec![
            DieselUlid::generate().to_string(),
            DieselUlid::generate().to_string(),
        ],
    };

    let tokenless_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server.dataset_service.get_datasets(tokenless_request).await;

    assert!(response.is_err());

    // Try get Datasets without token
    inner_request.dataset_ids = vec![dataset_01_ulid.to_string(), dataset_02_ulid.to_string()];

    let response = server
        .dataset_service
        .get_datasets(Request::new(inner_request.clone()))
        .await;

    assert!(response.is_err());

    // Get Dataset without permissions
    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server.dataset_service.get_datasets(grpc_request).await;

    assert!(response.is_err());

    // Get Datasets with one dataset without sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &dataset_01_ulid,
        DbPermissionLevel::READ,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server.dataset_service.get_datasets(grpc_request).await;

    assert!(response.is_err());

    // Get Datasets with one dataset without sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &dataset_02_ulid,
        DbPermissionLevel::READ,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_datasets = server
        .dataset_service
        .get_datasets(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .datasets;

    // Validate returned datasets
    assert_eq!(proto_datasets.len(), 2);

    for dataset_id in [&dataset_01.id, &dataset_02.id] {
        let dataset =
            fast_track_grpc_get_dataset(&server.dataset_service, ADMIN_OIDC_TOKEN, dataset_id)
                .await;

        assert!(proto_datasets.contains(&dataset))
    }
}

#[tokio::test]
async fn grpc_update_dataset_name() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Dataset
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let dataset = fast_track_grpc_dataset_create(
        &server.dataset_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let dataset_ulid = DieselUlid::from_str(&dataset.id).unwrap();

    // Update Dataset without token
    let mut inner_request = UpdateDatasetNameRequest {
        dataset_id: dataset.id.to_string(),
        name: "updated-name".to_string(),
    };

    let grpc_request = Request::new(inner_request.clone());

    let response = server
        .dataset_service
        .update_dataset_name(grpc_request)
        .await;

    assert!(response.is_err());

    // Update non-existing Dataset
    inner_request.dataset_id = DieselUlid::generate().to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .dataset_service
        .update_dataset_name(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Dataset without sufficient permissions
    inner_request.dataset_id = dataset.id.to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .dataset_service
        .update_dataset_name(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Dataset with sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &dataset_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_dataset = server
        .dataset_service
        .update_dataset_name(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();

    assert_eq!(dataset.id, proto_dataset.id);
    assert_eq!(&proto_dataset.name, "updated-name");
}

#[tokio::test]
async fn grpc_update_dataset_description() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Dataset
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let dataset = fast_track_grpc_dataset_create(
        &server.dataset_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let dataset_ulid = DieselUlid::from_str(&dataset.id).unwrap();

    // Update Dataset without token
    let mut inner_request = UpdateDatasetDescriptionRequest {
        dataset_id: dataset.id.to_string(),
        description: "Updated description".to_string(),
    };

    let grpc_request = Request::new(inner_request.clone());

    let response = server
        .dataset_service
        .update_dataset_description(grpc_request)
        .await;

    assert!(response.is_err());

    // Update non-existing Dataset
    inner_request.dataset_id = DieselUlid::generate().to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .dataset_service
        .update_dataset_description(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Dataset without sufficient permissions
    inner_request.dataset_id = dataset.id.to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .dataset_service
        .update_dataset_description(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Dataset with sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &dataset_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_dataset = server
        .dataset_service
        .update_dataset_description(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();

    assert_eq!(dataset.id, proto_dataset.id);
    assert_eq!(&proto_dataset.description, "Updated description");
}

#[tokio::test]
async fn grpc_update_dataset_dataclass() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Dataset
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let dataset = fast_track_grpc_dataset_create(
        &server.dataset_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let dataset_ulid = DieselUlid::from_str(&dataset.id).unwrap();

    // Change dataclass of non-existing dataset
    let mut inner_request = UpdateDatasetDataClassRequest {
        dataset_id: DieselUlid::generate().to_string(),
        data_class: DataClass::Public as i32,
    };

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .dataset_service
        .update_dataset_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change dataclass without token
    inner_request.dataset_id = dataset_ulid.to_string();

    let grpc_request = Request::new(inner_request.clone());

    let response = server
        .dataset_service
        .update_dataset_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change dataclass without sufficient permissions
    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .dataset_service
        .update_dataset_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change to stricter dataclass with sufficient permissions
    inner_request.data_class = DataClass::Confidential as i32;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .dataset_service
        .update_dataset_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change to more relaxed dataclass with sufficient permissions
    inner_request.data_class = DataClass::Public as i32;

    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &dataset_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_dataset = server
        .dataset_service
        .update_dataset_data_class(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();

    assert_eq!(dataset.id, proto_dataset.id);
    assert_eq!(proto_dataset.data_class, DataClass::Public as i32);
}

#[tokio::test]
async fn grpc_update_dataset_kvs() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Dataset
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let dataset = fast_track_grpc_dataset_create(
        &server.dataset_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let dataset_ulid = DieselUlid::from_str(&dataset.id).unwrap();

    // Change key-values of non-existing Dataset
    let mut inner_request = UpdateDatasetKeyValuesRequest {
        dataset_id: DieselUlid::generate().to_string(),
        add_key_values: vec![KeyValue {
            key: "SomeKey".to_string(),
            value: "SomeValue".to_string(),
            variant: KeyValueVariant::Label as i32,
        }],
        remove_key_values: vec![],
    };

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .dataset_service
        .update_dataset_key_values(grpc_request)
        .await;

    assert!(response.is_err());

    // Change key-values without token
    inner_request.dataset_id = dataset_ulid.to_string();

    let grpc_request = Request::new(inner_request.clone());

    let response = server
        .dataset_service
        .update_dataset_key_values(grpc_request)
        .await;

    assert!(response.is_err());

    // Change key-values without sufficient permissions
    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .dataset_service
        .update_dataset_key_values(grpc_request)
        .await;

    assert!(response.is_err());

    // Add key-value duplicates
    inner_request.add_key_values.push(KeyValue {
        key: "SomeKey".to_string(),
        value: "SomeValue".to_string(),
        variant: KeyValueVariant::Label as i32,
    });

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let proto_dataset = server
        .dataset_service
        .update_dataset_key_values(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();

    assert_eq!(dataset.id, proto_dataset.id);
    assert_eq!(proto_dataset.key_values.len(), 2);
    for kv in proto_dataset.key_values {
        assert_eq!(
            kv,
            KeyValue {
                key: "SomeKey".to_string(),
                value: "SomeValue".to_string(),
                variant: KeyValueVariant::Label as i32,
            }
        )
    }

    // Remove one of the duplicate key-values
    inner_request.add_key_values = vec![];
    inner_request.remove_key_values = vec![KeyValue {
        key: "SomeKey".to_string(),
        value: "SomeValue".to_string(),
        variant: KeyValueVariant::Label as i32,
    }];

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let proto_dataset = server
        .dataset_service
        .update_dataset_key_values(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();

    assert_eq!(dataset.id, proto_dataset.id);
    assert_eq!(proto_dataset.key_values.len(), 1);
    assert_eq!(
        proto_dataset.key_values,
        vec![KeyValue {
            key: "SomeKey".to_string(),
            value: "SomeValue".to_string(),
            variant: KeyValueVariant::Label as i32,
        }]
    );

    // Add key-values with sufficient permissions
    inner_request.add_key_values = vec![KeyValue {
        key: "SomeKey".to_string(),
        value: "SomeValue2".to_string(),
        variant: KeyValueVariant::Label as i32,
    }];
    inner_request.remove_key_values = vec![];

    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &dataset_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_dataset = server
        .dataset_service
        .update_dataset_key_values(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();

    assert_eq!(dataset.id, proto_dataset.id);
    assert!(proto_dataset.key_values.contains(&KeyValue {
        key: "SomeKey".to_string(),
        value: "SomeValue".to_string(),
        variant: KeyValueVariant::Label as i32,
    }));
    assert!(proto_dataset.key_values.contains(&KeyValue {
        key: "SomeKey".to_string(),
        value: "SomeValue2".to_string(),
        variant: KeyValueVariant::Label as i32,
    }));

    // Remove key-values with sufficient permissions
    inner_request.add_key_values = vec![];
    inner_request.remove_key_values = vec![KeyValue {
        key: "SomeKey".to_string(),
        value: "SomeValue".to_string(),
        variant: KeyValueVariant::Label as i32,
    }];

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_dataset = server
        .dataset_service
        .update_dataset_key_values(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();

    assert_eq!(dataset.id, proto_dataset.id);
    assert_eq!(
        proto_dataset.key_values,
        vec![KeyValue {
            key: "SomeKey".to_string(),
            value: "SomeValue2".to_string(),
            variant: KeyValueVariant::Label as i32,
        }]
    );

    // Update key-values with sufficient permissions
    inner_request.add_key_values = vec![KeyValue {
        key: "SomeKey".to_string(),
        value: "SomeValue3".to_string(),
        variant: KeyValueVariant::Label as i32,
    }];
    inner_request.remove_key_values = vec![KeyValue {
        key: "SomeKey".to_string(),
        value: "SomeValue2".to_string(),
        variant: KeyValueVariant::Label as i32,
    }];

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_dataset = server
        .dataset_service
        .update_dataset_key_values(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();

    assert_eq!(dataset.id, proto_dataset.id);
    assert_eq!(
        proto_dataset.key_values,
        vec![KeyValue {
            key: "SomeKey".to_string(),
            value: "SomeValue3".to_string(),
            variant: KeyValueVariant::Label as i32,
        }]
    );
}

#[tokio::test]
async fn grpc_delete_dataset() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Dataset
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let dataset = fast_track_grpc_dataset_create(
        &server.dataset_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let dataset_ulid = DieselUlid::from_str(&dataset.id).unwrap();

    // Delete non-existing Dataset
    let mut inner_request = DeleteDatasetRequest {
        dataset_id: DieselUlid::generate().to_string(),
    };

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server.dataset_service.delete_dataset(grpc_request).await;

    assert!(response.is_err()); // Dataset does not exist

    // Delete Dataset without token
    inner_request.dataset_id = dataset_ulid.to_string();

    let grpc_request = Request::new(inner_request.clone());

    let response = server.dataset_service.delete_dataset(grpc_request).await;

    assert!(response.is_err());

    // Delete Dataset without sufficient permissions
    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server.dataset_service.delete_dataset(grpc_request).await;

    assert!(response.is_err());

    // Delete Dataset with sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &dataset_ulid,
        DbPermissionLevel::ADMIN,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    server
        .dataset_service
        .delete_dataset(grpc_request)
        .await
        .unwrap();

    let deleted_dataset = server
        .dataset_service
        .get_dataset(add_token(
            Request::new(GetDatasetRequest {
                dataset_id: dataset_ulid.to_string(),
            }),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();

    assert_eq!(deleted_dataset.id, dataset.id);
    assert_eq!(deleted_dataset.status, Status::Deleted as i32)

    //ToDo: Try delete non-empty Dataset
}

#[tokio::test]
async fn grpc_snapshot_dataset() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Dataset
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let dataset = fast_track_grpc_dataset_create(
        &server.dataset_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let dataset_ulid = DieselUlid::from_str(&dataset.id).unwrap();

    // Snapshot non-existing Dataset
    let mut inner_request = SnapshotDatasetRequest {
        dataset_id: DieselUlid::generate().to_string(),
    };

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server.dataset_service.snapshot_dataset(grpc_request).await;

    assert!(response.is_err());

    // Snapshot Dataset without token
    inner_request.dataset_id = dataset_ulid.to_string();

    let grpc_request = Request::new(inner_request.clone());

    let response = server.dataset_service.snapshot_dataset(grpc_request).await;

    assert!(response.is_err());

    // Snapshot Dataset without sufficient permissions
    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server.dataset_service.snapshot_dataset(grpc_request).await;

    assert!(response.is_err());

    // Snapshot Dataset with sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &dataset_ulid,
        DbPermissionLevel::ADMIN,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_dataset = server
        .dataset_service
        .snapshot_dataset(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap();

    assert_ne!(dataset.id, proto_dataset.id);
    assert!(dataset.dynamic);
    assert!(!proto_dataset.dynamic);
    assert!(proto_dataset.relations.contains(&Relation {
        relation: Some(Internal(InternalRelation {
            resource_id: dataset.id,
            resource_variant: ResourceVariant::Dataset as i32,
            defined_variant: InternalRelationVariant::Version as i32,
            custom_variant: None,
            direction: RelationDirection::Outbound as i32,
        }))
    }))

    //ToDo: Snapshot non-empty dataset
}
