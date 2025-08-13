use crate::common::init::init_database_handler_middlelayer;
use crate::common::test_utils;
use aruna_rust_api::api::storage::models::v2::{
    relation, InternalRelationVariant, Relation, RelationDirection, ResourceVariant,
};
use aruna_rust_api::api::storage::services::v2::create_collection_request::Parent as CollectionParent;
use aruna_rust_api::api::storage::services::v2::create_dataset_request::Parent as DatasetParent;
use aruna_rust_api::api::storage::services::v2::create_object_request::Parent as ObjectParent;
use aruna_rust_api::api::storage::services::v2::{
    CreateCollectionRequest, CreateDatasetRequest, CreateObjectRequest, CreateProjectRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::internal_relation_dsl::INTERNAL_RELATION_VARIANT_METADATA;
use aruna_server::database::dsls::license_dsl::ALL_RIGHTS_RESERVED;
use aruna_server::database::dsls::object_dsl::{EndpointInfo, Object};
use aruna_server::database::enums::{DataClass, ObjectStatus, ObjectType, ReplicationStatus};
use aruna_server::middlelayer::create_request_types::CreateRequest;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use rand::distr::Alphanumeric;
use rand::{rng as thread_rng, Rng};

fn random_name() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}
#[tokio::test]
async fn create_project() {
    // init
    let db_handler = init_database_handler_middlelayer().await;

    // create user
    let mut user = test_utils::new_user(vec![]);
    user.create(&db_handler.database.get_client().await.unwrap())
        .await
        .unwrap();
    // Default endpoint:
    let default_endpoint = DieselUlid::generate();
    let project_name = random_name().to_lowercase();

    // test requests
    let request = CreateRequest::Project(
        CreateProjectRequest {
            name: project_name.clone(),
            title: "".to_string(),
            description: "test".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: 1,
            preferred_endpoint: "".to_string(),
            metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            authors: vec![],
        },
        default_endpoint.to_string(),
    );
    let (proj, _) = db_handler
        .create_resource(request, user.id, false)
        .await
        .unwrap();

    assert_eq!(proj.object.created_by, user.id);
    assert_eq!(proj.object.object_type, ObjectType::PROJECT);
    assert_eq!(proj.object.name, project_name);
    assert_eq!(proj.object.object_status, ObjectStatus::AVAILABLE);
    assert_eq!(proj.object.data_class, DataClass::PUBLIC);
    assert_eq!(proj.object.description, "test".to_string());
    assert_eq!(proj.object.revision_number, 0);
    assert_eq!(proj.object.count, 1);
    assert!(proj.object.dynamic);
    assert!(proj.object.hashes.0 .0.is_empty());
    assert!(proj.object.key_values.0 .0.is_empty());
    assert!(proj.object.external_relations.0 .0.is_empty());
    assert!(proj.object.endpoints.0.into_iter().contains(&(
        default_endpoint,
        EndpointInfo {
            replication: aruna_server::database::enums::ReplicationType::FullSync,
            status: None,
        }
    )));
    assert!(proj.inbound.0.is_empty());
    assert!(proj.inbound_belongs_to.0.is_empty());
    assert!(proj.outbound.0.is_empty());
    assert!(proj.outbound_belongs_to.0.is_empty());
}
#[tokio::test]
async fn create_collection() {
    // init
    let db_handler = init_database_handler_middlelayer().await;
    let client = &db_handler.database.get_client().await.unwrap();

    // create user
    let mut user = test_utils::new_user(vec![]);
    user.create(client).await.unwrap();

    // default endpoint:
    let default_endpoint = DieselUlid::generate();

    // create parent
    let parent_name = random_name().to_lowercase();
    let parent = CreateRequest::Project(
        CreateProjectRequest {
            name: parent_name,
            title: "".to_string(),
            description: "test".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: 1,
            preferred_endpoint: "".to_string(),
            metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            authors: vec![],
        },
        default_endpoint.to_string(),
    );
    let (parent, _) = db_handler
        .create_resource(parent, user.id, false)
        .await
        .unwrap();
    db_handler.cache.add_object(parent.clone());

    // test requests
    let collection_name = random_name();
    let request = CreateRequest::Collection(CreateCollectionRequest {
        name: collection_name.clone(),
        title: "".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: 1,
        parent: Some(CollectionParent::ProjectId(parent.object.id.to_string())),
        metadata_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        default_data_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        authors: vec![],
    });
    let (coll, _) = db_handler
        .create_resource(request, user.id, false)
        .await
        .unwrap();

    assert_eq!(coll.object.created_by, user.id);
    assert_eq!(coll.object.object_type, ObjectType::COLLECTION);
    assert_eq!(coll.object.name, collection_name);
    assert_eq!(coll.object.object_status, ObjectStatus::AVAILABLE);
    assert_eq!(coll.object.data_class, DataClass::PUBLIC);
    assert_eq!(coll.object.description, "test".to_string());
    assert_eq!(coll.object.revision_number, 0);
    assert_eq!(coll.object.count, 1);
    assert!(coll.object.title.is_empty());
    assert!(coll.object.authors.0.is_empty());
    assert!(coll.object.dynamic);
    assert!(coll.object.hashes.0 .0.is_empty());
    assert!(coll.object.key_values.0 .0.is_empty());
    assert!(coll.object.external_relations.0 .0.is_empty());
    assert!(coll.object.endpoints.0.into_iter().contains(&(
        default_endpoint,
        EndpointInfo {
            replication: aruna_server::database::enums::ReplicationType::FullSync,
            status: None,
        }
    )));
    assert!(coll.inbound.0.is_empty());
    assert!(coll.inbound_belongs_to.0.get(&parent.object.id).is_some());
    assert!(coll.outbound.0.is_empty());
    assert!(coll.outbound_belongs_to.0.is_empty());
}
#[tokio::test]
async fn create_dataset() {
    // init
    let db_handler = init_database_handler_middlelayer().await;
    let client = &db_handler.database.get_client().await.unwrap();
    // create user
    let mut user = test_utils::new_user(vec![]);
    user.create(client).await.unwrap();

    // endpoint
    let default_endpoint = DieselUlid::generate();
    // create parent
    let parent_name = random_name().to_lowercase();
    let parent = CreateRequest::Project(
        CreateProjectRequest {
            name: parent_name,
            title: "".to_string(),
            description: "test".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: 1,
            preferred_endpoint: "".to_string(),
            metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            authors: vec![],
        },
        default_endpoint.to_string(),
    );
    let (parent, _) = db_handler
        .create_resource(parent, user.id, false)
        .await
        .unwrap();
    db_handler.cache.add_object(parent.clone());

    // test requests
    let dataset_name = random_name();
    let request = CreateRequest::Dataset(CreateDatasetRequest {
        name: dataset_name.clone(),
        title: "".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: 1,
        parent: Some(DatasetParent::ProjectId(parent.object.id.to_string())),
        metadata_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        default_data_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        authors: vec![],
    });
    let (ds, _) = db_handler
        .create_resource(request, user.id, false)
        .await
        .unwrap();

    assert_eq!(ds.object.created_by, user.id);
    assert_eq!(ds.object.object_type, ObjectType::DATASET);
    assert_eq!(ds.object.name, dataset_name);
    assert_eq!(ds.object.object_status, ObjectStatus::AVAILABLE);
    assert_eq!(ds.object.data_class, DataClass::PUBLIC);
    assert_eq!(ds.object.description, "test".to_string());
    assert_eq!(ds.object.revision_number, 0);
    assert_eq!(ds.object.count, 1);
    assert!(ds.object.title.is_empty());
    assert!(ds.object.authors.0.is_empty());
    assert!(ds.object.dynamic);
    assert!(ds.object.hashes.0 .0.is_empty());
    assert!(ds.object.key_values.0 .0.is_empty());
    assert!(ds.object.external_relations.0 .0.is_empty());
    assert!(ds.object.endpoints.0.into_iter().contains(&(
        default_endpoint,
        EndpointInfo {
            replication: aruna_server::database::enums::ReplicationType::FullSync,
            status: None,
        }
    )));
    assert!(ds.inbound.0.is_empty());
    assert!(ds.inbound_belongs_to.0.get(&parent.object.id).is_some());
    assert!(ds.outbound.0.is_empty());
    assert!(ds.outbound_belongs_to.0.is_empty());
}
#[tokio::test]
async fn create_object() {
    // init
    let db_handler = init_database_handler_middlelayer().await;
    let client = &db_handler.database.get_client().await.unwrap();
    let cache = &db_handler.cache;

    // create user
    let mut user = test_utils::new_user(vec![]);
    user.create(client).await.unwrap();

    // let not default endpoint
    let endpoint = DieselUlid::generate();
    // create parent
    let failing_parent = CreateRequest::Project(
        CreateProjectRequest {
            name: "project".to_string(),
            title: "".to_string(),
            description: "test".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: 1,
            preferred_endpoint: endpoint.to_string(),
            metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            authors: vec![],
        },
        DieselUlid::generate().to_string(),
    );
    // Should fail because endpoint does not exist
    assert!(db_handler
        .create_resource(failing_parent, user.id, false)
        .await
        .is_err());

    let default_endpoint = DieselUlid::generate();
    let parent_name = random_name().to_lowercase();
    let parent = CreateRequest::Project(
        CreateProjectRequest {
            name: parent_name,
            title: "".to_string(),
            description: "test".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: 1,
            preferred_endpoint: "".to_string(),
            metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            authors: vec![],
        },
        default_endpoint.to_string(),
    );
    let (parent, _) = db_handler
        .create_resource(parent, user.id, false)
        .await
        .unwrap();
    cache.add_object(parent.clone());

    // test requests
    let object_name = random_name();
    let request = CreateRequest::Object(CreateObjectRequest {
        name: object_name.clone(),
        title: "".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: 1,
        hashes: vec![],
        parent: Some(ObjectParent::ProjectId(parent.object.id.to_string())),
        metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        authors: vec![],
    });
    let (obj, _) = db_handler
        .create_resource(request, user.id, false)
        .await
        .unwrap();

    assert_eq!(obj.object.created_by, user.id);
    assert_eq!(obj.object.object_type, ObjectType::OBJECT);
    assert_eq!(obj.object.name, object_name);
    assert_eq!(obj.object.object_status, ObjectStatus::INITIALIZING);
    assert_eq!(obj.object.data_class, DataClass::PUBLIC);
    assert_eq!(obj.object.description, "test".to_string());
    assert_eq!(obj.object.revision_number, 0);
    assert_eq!(obj.object.count, 1);
    assert!(obj.object.title.is_empty());
    assert!(obj.object.authors.0.is_empty());
    assert!(!obj.object.dynamic);
    assert!(obj.object.hashes.0 .0.is_empty());
    assert!(obj.object.key_values.0 .0.is_empty());
    assert!(obj.object.external_relations.0 .0.is_empty());
    assert!(obj.object.endpoints.0.into_iter().contains(&(
        default_endpoint,
        EndpointInfo {
            replication: aruna_server::database::enums::ReplicationType::FullSync,
            status: Some(ReplicationStatus::Waiting),
        }
    )));
    assert!(obj.inbound.0.is_empty());
    assert!(obj.inbound_belongs_to.0.get(&parent.object.id).is_some());
    assert!(obj.outbound.0.is_empty());
    assert!(obj.outbound_belongs_to.0.is_empty());
}
#[tokio::test]
async fn create_object_with_relations() {
    // init
    let db_handler = init_database_handler_middlelayer().await;
    let client = &db_handler.database.get_client().await.unwrap();
    let cache = &db_handler.cache;

    // create user
    let mut user = test_utils::new_user(vec![]);
    user.create(client).await.unwrap();

    let default_endpoint = DieselUlid::generate();
    let parent_name = random_name().to_lowercase();
    let parent = CreateRequest::Project(
        CreateProjectRequest {
            name: parent_name,
            title: "".to_string(),
            description: "test".to_string(),
            key_values: vec![],
            relations: vec![],
            data_class: 1,
            preferred_endpoint: "".to_string(),
            metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            default_data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
            authors: vec![],
        },
        default_endpoint.to_string(),
    );
    let (parent, _) = db_handler
        .create_resource(parent, user.id, false)
        .await
        .unwrap();
    cache.add_object(parent.clone());

    // test requests
    // Create first object
    let object_name = random_name();
    let request = CreateRequest::Object(CreateObjectRequest {
        name: object_name.clone(),
        title: "".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: 1,
        hashes: vec![],
        parent: Some(ObjectParent::ProjectId(parent.object.id.to_string())),
        metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        authors: vec![],
    });
    let (obj_1, _) = db_handler
        .create_resource(request, user.id, false)
        .await
        .unwrap();

    // Create second object
    let object_name = random_name();
    let request = CreateRequest::Object(CreateObjectRequest {
        name: object_name.clone(),
        title: "".to_string(),
        description: "test".to_string(),
        key_values: vec![],
        relations: vec![Relation {
            relation: Some(relation::Relation::Internal(
                aruna_rust_api::api::storage::models::v2::InternalRelation {
                    resource_id: obj_1.object.id.to_string(),
                    resource_variant: ResourceVariant::Object as i32,
                    defined_variant: InternalRelationVariant::Metadata as i32,
                    custom_variant: None,
                    direction: RelationDirection::Outbound as i32,
                },
            )),
        }],
        data_class: 1,
        hashes: vec![],
        parent: Some(ObjectParent::ProjectId(parent.object.id.to_string())),
        metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        authors: vec![],
    });
    let (obj_2, _) = db_handler
        .create_resource(request, user.id, false)
        .await
        .unwrap();

    let updated_obj_1 = Object::get_object_with_relations(&obj_1.object.id, client)
        .await
        .unwrap();
    let updated_obj_1_cached = db_handler.cache.get_object(&obj_1.object.id).unwrap();

    assert_eq!(updated_obj_1, updated_obj_1_cached);
    let outbound_relation = obj_2.outbound.0.iter().next().unwrap();
    // Check if relation is set in second object
    assert_eq!(
        outbound_relation.relation_name,
        INTERNAL_RELATION_VARIANT_METADATA
    );
    assert_eq!(outbound_relation.target_pid, obj_1.object.id);
    assert_eq!(outbound_relation.target_pid, updated_obj_1.object.id);
    assert_eq!(outbound_relation.target_pid, updated_obj_1_cached.object.id);

    // Check if relation is set in database for first object
    let outbound_relation = updated_obj_1.inbound.0.iter().next().unwrap();
    assert_eq!(
        outbound_relation.relation_name,
        INTERNAL_RELATION_VARIANT_METADATA
    );
    assert_eq!(outbound_relation.origin_pid, obj_2.object.id);

    // Check if relation is set in cache for first object
    let outbound_relation = updated_obj_1_cached.inbound.0.iter().next().unwrap();
    assert_eq!(
        outbound_relation.relation_name,
        INTERNAL_RELATION_VARIANT_METADATA
    );
    assert_eq!(outbound_relation.origin_pid, obj_2.object.id);
}
