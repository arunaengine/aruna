use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v2::{
        relation::Relation::Internal, DataClass, DataEndpoint, FullSync, InternalRelation,
        InternalRelationVariant, KeyValue, KeyValueVariant, Relation, RelationDirection,
        ResourceVariant, Status,
    },
    services::v2::{
        collection_service_server::CollectionService, create_collection_request::Parent,
        CreateCollectionRequest, DeleteCollectionRequest, GetCollectionRequest,
        GetCollectionsRequest, SnapshotCollectionRequest, UpdateCollectionDataClassRequest,
        UpdateCollectionDescriptionRequest, UpdateCollectionKeyValuesRequest,
        UpdateCollectionNameRequest,
    },
};
use diesel_ulid::DieselUlid;
use tonic::Request;

use crate::common::{
    init, init_server,
    test_utils::{
        add_token, fast_track_grpc_collection_create, fast_track_grpc_get_collection,
        fast_track_grpc_permission_add, fast_track_grpc_project_create, ADMIN_OIDC_TOKEN,
        DEFAULT_ENDPOINT_ULID, USER1_OIDC_TOKEN, USER1_ULID,
    },
};
use aruna_server::database::{dsls::license_dsl::ALL_RIGHTS_RESERVED, enums::DbPermissionLevel};

#[tokio::test]
async fn grpc_create_collection() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random project
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;

    // Create request with OIDC token in header
    let create_request = CreateCollectionRequest {
        name: "test-collection".to_string(),
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
    let grpc_request = add_token(Request::new(create_request), ADMIN_OIDC_TOKEN);

    // Create project via gRPC service
    let create_response = &server
        .collection_service
        .create_collection(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_collection = create_response.collection.as_ref().unwrap();

    assert!(!proto_collection.id.is_empty());
    assert_eq!(proto_collection.name, "test-collection");
    assert_eq!(&proto_collection.description, "Test Description");
    assert_eq!(proto_collection.key_values, vec![]);
    assert_eq!(
        proto_collection.relations,
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
    assert_eq!(proto_collection.data_class, 2);
    assert_eq!(proto_collection.status, Status::Available as i32);
    assert!(proto_collection.dynamic);
    assert_eq!(
        proto_collection.endpoints,
        vec![DataEndpoint {
            id: DEFAULT_ENDPOINT_ULID.to_string(),
            variant: Some(
                aruna_rust_api::api::storage::models::v2::data_endpoint::Variant::FullSync(
                    FullSync {}
                )
            ),
            status: None,
        }]
    );
}

#[tokio::test]
async fn grpc_get_collection() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Get normal user id as DieselUlid
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();

    // Create random project and collection
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;

    // Create collection
    let collection = fast_track_grpc_collection_create(
        &server.collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;
    let collection_ulid = DieselUlid::from_str(&collection.id).unwrap();

    // Try get Collection that does not exist
    let mut get_request = GetCollectionRequest {
        collection_id: DieselUlid::generate().to_string(),
    };

    let tokenless_request = add_token(Request::new(get_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .collection_service
        .get_collection(tokenless_request)
        .await;

    assert!(response.is_err());

    // Try get Collection without token
    get_request.collection_id.clone_from(&collection.id);

    let response = server
        .collection_service
        .get_collection(Request::new(get_request.clone()))
        .await;

    assert!(response.is_err());

    // Get Collection without permissions
    let grpc_request = add_token(Request::new(get_request.clone()), USER1_OIDC_TOKEN);

    let response = server.collection_service.get_collection(grpc_request).await;

    assert!(response.is_err());

    // Get Collection with permissions
    let grpc_request = add_token(Request::new(get_request.clone()), USER1_OIDC_TOKEN);

    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_ulid,
        DbPermissionLevel::READ,
    )
    .await;

    let proto_collection = server
        .collection_service
        .get_collection(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    // Validate returned collection
    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(collection.name, proto_collection.name);
    assert_eq!(collection.description, proto_collection.description);
    assert_eq!(collection.key_values, proto_collection.key_values);
    assert_eq!(
        proto_collection.relations,
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
    assert_eq!(collection.data_class, proto_collection.data_class);
    assert_eq!(collection.status, proto_collection.status);
    assert_eq!(collection.dynamic, proto_collection.dynamic);
}

#[tokio::test]
async fn grpc_get_collections() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Get normal user id as DieselUlid
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();

    // Create random project and collection
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;

    // Create collections
    let collection_01 = fast_track_grpc_collection_create(
        &server.collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;
    let collection_02 = fast_track_grpc_collection_create(
        &server.collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;
    let collection_01_ulid = DieselUlid::from_str(&collection_01.id).unwrap();
    let collection_02_ulid = DieselUlid::from_str(&collection_02.id).unwrap();

    // Try get Collections that does not exist
    let mut inner_request = GetCollectionsRequest {
        collection_ids: vec![
            DieselUlid::generate().to_string(),
            DieselUlid::generate().to_string(),
        ],
    };

    let tokenless_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .collection_service
        .get_collections(tokenless_request)
        .await;

    assert!(response.is_err());

    // Try get Collections without token
    inner_request.collection_ids = vec![
        collection_01_ulid.to_string(),
        collection_02_ulid.to_string(),
    ];

    let response = server
        .collection_service
        .get_collections(Request::new(inner_request.clone()))
        .await;

    assert!(response.is_err());

    // Get Collection without permissions
    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .collection_service
        .get_collections(grpc_request)
        .await;

    assert!(response.is_err());

    // Get Collections with one collection without sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_01_ulid,
        DbPermissionLevel::READ,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .collection_service
        .get_collections(grpc_request)
        .await;

    assert!(response.is_err());

    // Get Collections with one collection without sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_02_ulid,
        DbPermissionLevel::READ,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_collections = server
        .collection_service
        .get_collections(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collections;

    // Validate returned collections
    assert_eq!(proto_collections.len(), 2);

    for collection_id in [&collection_01.id, &collection_02.id] {
        let collection = fast_track_grpc_get_collection(
            &server.collection_service,
            ADMIN_OIDC_TOKEN,
            collection_id,
        )
        .await;

        assert!(proto_collections.contains(&collection))
    }
}

#[tokio::test]
async fn grpc_update_collection_name() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Collection
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let collection = fast_track_grpc_collection_create(
        &server.collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let collection_ulid = DieselUlid::from_str(&collection.id).unwrap();

    // Update Collection without token
    let mut inner_request = UpdateCollectionNameRequest {
        collection_id: collection.id.to_string(),
        name: "updated-name".to_string(),
    };

    let grpc_request = Request::new(inner_request.clone());

    let response = server
        .collection_service
        .update_collection_name(grpc_request)
        .await;

    assert!(response.is_err());

    // Update non-existing Collection
    inner_request.collection_id = DieselUlid::generate().to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .collection_service
        .update_collection_name(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Collection without sufficient permissions
    inner_request.collection_id = collection.id.to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .collection_service
        .update_collection_name(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Collection with sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_collection = server
        .collection_service
        .update_collection_name(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(&proto_collection.name, "updated-name");
}

#[tokio::test]
async fn grpc_update_collection_description() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Collection
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let collection = fast_track_grpc_collection_create(
        &server.collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let collection_ulid = DieselUlid::from_str(&collection.id).unwrap();

    // Update Collection without token
    let mut inner_request = UpdateCollectionDescriptionRequest {
        collection_id: collection.id.to_string(),
        description: "Updated description".to_string(),
    };

    let grpc_request = Request::new(inner_request.clone());

    let response = server
        .collection_service
        .update_collection_description(grpc_request)
        .await;

    assert!(response.is_err());

    // Update non-existing Collection
    inner_request.collection_id = DieselUlid::generate().to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .collection_service
        .update_collection_description(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Collection without sufficient permissions
    inner_request.collection_id = collection.id.to_string();

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .collection_service
        .update_collection_description(grpc_request)
        .await;

    assert!(response.is_err());

    // Update Collection with sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_collection = server
        .collection_service
        .update_collection_description(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(&proto_collection.description, "Updated description");
}

#[tokio::test]
async fn grpc_update_collection_dataclass() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Collection
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let collection = fast_track_grpc_collection_create(
        &server.collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let collection_ulid = DieselUlid::from_str(&collection.id).unwrap();

    // Change dataclass of non-existing collection
    let mut inner_request = UpdateCollectionDataClassRequest {
        collection_id: DieselUlid::generate().to_string(),
        data_class: DataClass::Public as i32,
    };

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .collection_service
        .update_collection_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change dataclass without token
    inner_request.collection_id = collection_ulid.to_string();

    let grpc_request = Request::new(inner_request.clone());

    let response = server
        .collection_service
        .update_collection_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change dataclass without sufficient permissions
    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .collection_service
        .update_collection_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change to stricter dataclass with sufficient permissions
    inner_request.data_class = DataClass::Confidential as i32;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .collection_service
        .update_collection_data_class(grpc_request)
        .await;

    assert!(response.is_err());

    // Change to more relaxed dataclass with sufficient permissions
    inner_request.data_class = DataClass::Public as i32;

    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_collection = server
        .collection_service
        .update_collection_data_class(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(proto_collection.data_class, DataClass::Public as i32);
}

#[tokio::test]
async fn grpc_update_collection_keyvalues() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Collection
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let collection = fast_track_grpc_collection_create(
        &server.collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let collection_ulid = DieselUlid::from_str(&collection.id).unwrap();

    // Change key-values of non-existing Collection
    let mut inner_request = UpdateCollectionKeyValuesRequest {
        collection_id: DieselUlid::generate().to_string(),
        add_key_values: vec![KeyValue {
            key: "SomeKey".to_string(),
            value: "SomeValue".to_string(),
            variant: KeyValueVariant::Label as i32,
        }],
        remove_key_values: vec![],
    };

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .collection_service
        .update_collection_key_values(grpc_request)
        .await;

    assert!(response.is_err());

    // Change key-values without token
    inner_request.collection_id = collection_ulid.to_string();

    let grpc_request = Request::new(inner_request.clone());

    let response = server
        .collection_service
        .update_collection_key_values(grpc_request)
        .await;

    assert!(response.is_err());

    // Change key-values without sufficient permissions
    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .collection_service
        .update_collection_key_values(grpc_request)
        .await;

    assert!(response.is_err());

    // Add key-value duplicates
    inner_request.add_key_values.push(KeyValue {
        key: "SomeKey".to_string(),
        value: "SomeValue".to_string(),
        variant: KeyValueVariant::Label as i32,
    });

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let proto_collection = server
        .collection_service
        .update_collection_key_values(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(proto_collection.key_values.len(), 2);
    for kv in proto_collection.key_values {
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

    let proto_collection = server
        .collection_service
        .update_collection_key_values(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(proto_collection.key_values.len(), 1);
    assert_eq!(
        proto_collection.key_values,
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
        &collection_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_collection = server
        .collection_service
        .update_collection_key_values(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(collection.id, proto_collection.id);
    assert!(proto_collection.key_values.contains(&KeyValue {
        key: "SomeKey".to_string(),
        value: "SomeValue".to_string(),
        variant: KeyValueVariant::Label as i32,
    }));
    assert!(proto_collection.key_values.contains(&KeyValue {
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

    let proto_collection = server
        .collection_service
        .update_collection_key_values(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(
        proto_collection.key_values,
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

    let proto_collection = server
        .collection_service
        .update_collection_key_values(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(collection.id, proto_collection.id);
    assert_eq!(
        proto_collection.key_values,
        vec![KeyValue {
            key: "SomeKey".to_string(),
            value: "SomeValue3".to_string(),
            variant: KeyValueVariant::Label as i32,
        }]
    );
}

#[tokio::test]
async fn grpc_delete_collection() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Collection
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let collection = fast_track_grpc_collection_create(
        &server.collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let collection_ulid = DieselUlid::from_str(&collection.id).unwrap();

    // Delete non-existing Collection
    let mut inner_request = DeleteCollectionRequest {
        collection_id: DieselUlid::generate().to_string(),
    };

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .collection_service
        .delete_collection(grpc_request)
        .await;

    assert!(response.is_err()); // Collection does not exist.

    // Delete Collection without token
    inner_request.collection_id = collection_ulid.to_string();

    let grpc_request = Request::new(inner_request.clone());

    let response = server
        .collection_service
        .delete_collection(grpc_request)
        .await;

    assert!(response.is_err());

    // Delete Collection without sufficient permissions
    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .collection_service
        .delete_collection(grpc_request)
        .await;

    assert!(response.is_err());

    // Delete Collection with sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_ulid,
        DbPermissionLevel::ADMIN,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    server
        .collection_service
        .delete_collection(grpc_request)
        .await
        .unwrap();

    let deleted_collection = server
        .collection_service
        .get_collection(add_token(
            Request::new(GetCollectionRequest {
                collection_id: collection_ulid.to_string(),
            }),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_eq!(deleted_collection.id, collection.id);
    assert_eq!(deleted_collection.status, Status::Deleted as i32)

    //ToDo: Try delete non-empty Collection
}

#[tokio::test]
async fn grpc_snapshot_collection() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random Project + Collection
    let project = fast_track_grpc_project_create(&server.project_service, ADMIN_OIDC_TOKEN).await;
    let collection = fast_track_grpc_collection_create(
        &server.collection_service,
        ADMIN_OIDC_TOKEN,
        Parent::ProjectId(project.id.clone()),
    )
    .await;

    // Create user/resource ulids
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let collection_ulid = DieselUlid::from_str(&collection.id).unwrap();

    // Snapshot non-existing Collection
    let mut inner_request = SnapshotCollectionRequest {
        collection_id: DieselUlid::generate().to_string(),
    };

    let grpc_request = add_token(Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = server
        .collection_service
        .snapshot_collection(grpc_request)
        .await;

    assert!(response.is_err());

    // Snapshot Collection without token
    inner_request.collection_id = collection_ulid.to_string();

    let grpc_request = Request::new(inner_request.clone());

    let response = server
        .collection_service
        .snapshot_collection(grpc_request)
        .await;

    assert!(response.is_err());

    // Snapshot Collection without sufficient permissions
    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = server
        .collection_service
        .snapshot_collection(grpc_request)
        .await;

    assert!(response.is_err());

    // Snapshot Collection with sufficient permissions
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &collection_ulid,
        DbPermissionLevel::ADMIN,
    )
    .await;

    let grpc_request = add_token(Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let proto_collection = server
        .collection_service
        .snapshot_collection(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap();

    assert_ne!(collection.id, proto_collection.id);
    assert!(collection.dynamic);
    assert!(!proto_collection.dynamic);
    assert!(proto_collection.relations.contains(&Relation {
        relation: Some(Internal(InternalRelation {
            resource_id: collection.id,
            resource_variant: ResourceVariant::Collection as i32,
            defined_variant: InternalRelationVariant::Version as i32,
            custom_variant: None,
            direction: RelationDirection::Outbound as i32,
        }))
    }))

    //ToDo: Snapshot non-empty collection
}
