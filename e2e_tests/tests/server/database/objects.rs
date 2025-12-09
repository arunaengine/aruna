use crate::common::init::init_database;
use crate::common::init_server::Component;
use crate::common::test_utils::USER1_ULID;
use crate::common::{infra, test_utils};
use aruna_server::database::dsls::internal_relation_dsl::InternalRelation;
use aruna_server::database::dsls::license_dsl::ALL_RIGHTS_RESERVED;
use aruna_server::database::dsls::object_dsl::{
    DefinedVariant, EndpointInfo, ExternalRelation, Hierarchy, KeyValue, KeyValueVariant,
};
use aruna_server::database::enums::{DataClass, ObjectStatus, ObjectType, ReplicationStatus};
use aruna_server::database::{
    crud::CrudDb,
    dsls::object_dsl::{ExternalRelations, KeyValues, Object, ObjectWithRelations},
    enums::ObjectMapping,
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::str::FromStr;
use tokio_postgres::GenericClient;

#[tokio::test]
async fn fetch_object_paths() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do objects stuff on database
    let client = db.get_client().await.unwrap();

    // Create random user
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    // Create dummy hierarchy
    let project_001 = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::PROJECT);
    let project_002 = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::PROJECT);
    let project_003 = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::PROJECT);

    let collection =
        test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::COLLECTION);
    let dataset_001 = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::DATASET);
    let dataset_002 = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::DATASET);
    let object = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::OBJECT);

    let proj_coll_001 = test_utils::new_internal_relation(&project_001, &collection);
    let proj_coll_002 = test_utils::new_internal_relation(&project_002, &collection);
    let proj_data = test_utils::new_internal_relation(&project_003, &dataset_002);
    let coll_data_001 = test_utils::new_internal_relation(&collection, &dataset_001);
    let coll_data_002 = test_utils::new_internal_relation(&collection, &dataset_002);
    let data_obj_001 = test_utils::new_internal_relation(&dataset_001, &object);
    let data_obj_002 = test_utils::new_internal_relation(&dataset_002, &object);

    Object::batch_create(
        &vec![
            project_001.clone(),
            project_002.clone(),
            project_003.clone(),
            collection.clone(),
            dataset_001.clone(),
            dataset_002.clone(),
            object.clone(),
        ],
        &client,
    )
    .await
    .unwrap();

    InternalRelation::batch_create(
        &vec![
            proj_coll_001,
            proj_coll_002,
            proj_data,
            coll_data_001,
            coll_data_002,
            data_obj_001,
            data_obj_002,
        ],
        &client,
    )
    .await
    .unwrap();

    // Fetch object hierarchies including "Performance measurement" recursive
    use std::time::Instant;
    let now = Instant::now();
    let result = object.fetch_object_hierarchies(&client).await.unwrap();
    let elapsed = now.elapsed();
    tracing::debug!("Hierarchy traversal elapsed: {elapsed:.2?}");

    assert_eq!(result.len(), 5);

    for hierarchy in vec![
        Hierarchy {
            project_id: project_001.id.to_string(),
            collection_id: Some(collection.id.to_string()),
            dataset_id: Some(dataset_001.id.to_string()),
            object_id: Some(object.id.to_string()),
        },
        Hierarchy {
            project_id: project_001.id.to_string(),
            collection_id: Some(collection.id.to_string()),
            dataset_id: Some(dataset_002.id.to_string()),
            object_id: Some(object.id.to_string()),
        },
        Hierarchy {
            project_id: project_002.id.to_string(),
            collection_id: Some(collection.id.to_string()),
            dataset_id: Some(dataset_001.id.to_string()),
            object_id: Some(object.id.to_string()),
        },
        Hierarchy {
            project_id: project_002.id.to_string(),
            collection_id: Some(collection.id.to_string()),
            dataset_id: Some(dataset_002.id.to_string()),
            object_id: Some(object.id.to_string()),
        },
        Hierarchy {
            project_id: project_003.id.to_string(),
            collection_id: None,
            dataset_id: Some(dataset_002.id.to_string()),
            object_id: Some(object.id.to_string()),
        },
    ] {
        assert!(result.contains(&hierarchy))
    }
}

#[tokio::test]
async fn fetch_object_subresources() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do objects stuff on database
    let client = db.get_client().await.unwrap();

    // Create random user
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    // Create dummy hierarchy
    let project_1 = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::PROJECT);
    let project_2 = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::PROJECT);
    let project_3 = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::PROJECT);

    let collection =
        test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::COLLECTION);
    let dataset_1 = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::DATASET);
    let dataset_2 = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::DATASET);
    let object = test_utils::new_object(user_id, DieselUlid::generate(), ObjectType::OBJECT);

    let proj_coll_1 = test_utils::new_internal_relation(&project_1, &collection);
    let proj_coll_2 = test_utils::new_internal_relation(&project_2, &collection);
    let proj_data = test_utils::new_internal_relation(&project_3, &dataset_2);
    let coll_data_1 = test_utils::new_internal_relation(&collection, &dataset_1);
    let coll_data_2 = test_utils::new_internal_relation(&collection, &dataset_2);
    let data_obj_1 = test_utils::new_internal_relation(&dataset_1, &object);
    let data_obj_2 = test_utils::new_internal_relation(&dataset_2, &object);

    Object::batch_create(
        &vec![
            project_1.clone(),
            project_2.clone(),
            project_3.clone(),
            collection.clone(),
            dataset_1.clone(),
            dataset_2.clone(),
            object.clone(),
        ],
        &client,
    )
    .await
    .unwrap();

    InternalRelation::batch_create(
        &vec![
            proj_coll_1,
            proj_coll_2,
            proj_data,
            coll_data_1,
            coll_data_2,
            data_obj_1,
            data_obj_2,
        ],
        &client,
    )
    .await
    .unwrap();

    // Fetch subresources of projects
    let project_1_sub = project_1.fetch_subresources(&client).await.unwrap();
    let project_2_sub = project_2.fetch_subresources(&client).await.unwrap();
    let project_3_sub = project_3.fetch_subresources(&client).await.unwrap();

    assert_eq!(project_1_sub.len(), 4);
    assert!(project_1_sub.contains(&collection.id));
    assert!(project_1_sub.contains(&dataset_1.id));
    assert!(project_1_sub.contains(&dataset_2.id));
    assert!(project_1_sub.contains(&object.id));

    assert_eq!(project_2_sub.len(), 4);
    assert!(project_2_sub.contains(&collection.id));
    assert!(project_2_sub.contains(&dataset_1.id));
    assert!(project_2_sub.contains(&dataset_2.id));
    assert!(project_2_sub.contains(&object.id));

    assert_eq!(project_3_sub.len(), 2);
    assert!(project_2_sub.contains(&dataset_2.id));
    assert!(project_2_sub.contains(&object.id));

    // Fetch subresources of collection
    let collection_sub = collection.fetch_subresources(&client).await.unwrap();

    assert_eq!(collection_sub.len(), 3);
    assert!(collection_sub.contains(&dataset_1.id));
    assert!(collection_sub.contains(&dataset_2.id));
    assert!(collection_sub.contains(&object.id));

    // Fetch subresources of datasets
    let dataset_01_sub = dataset_1.fetch_subresources(&client).await.unwrap();
    let dataset_02_sub = dataset_2.fetch_subresources(&client).await.unwrap();

    assert_eq!(dataset_01_sub.len(), 1);
    assert!(dataset_01_sub.contains(&object.id));

    assert_eq!(dataset_02_sub.len(), 1);
    assert!(dataset_02_sub.contains(&object.id));

    // Fetch subresources of object
    let object_sub = object.fetch_subresources(&client).await.unwrap();

    assert_eq!(object_sub.len(), 0);
}

#[tokio::test]
async fn create_object() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do objects stuff on database
    let client = db.get_client().await.unwrap();
    let obj_id = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let mut create_object = test_utils::new_object(user_id, obj_id, ObjectType::OBJECT);
    create_object.create(&client).await.unwrap();

    let get_obj = Object::get(obj_id, &client).await.unwrap().unwrap();
    assert_eq!(get_obj, create_object);
}

#[tokio::test]
async fn get_object_with_relations_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do objects stuff on database
    let client = db.get_client().await.unwrap();

    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let dataset_id = DieselUlid::generate();
    let collection_one = DieselUlid::generate();
    let collection_two = DieselUlid::generate();
    let object_one = DieselUlid::generate();
    let object_two = DieselUlid::generate();
    let object_id_map = vec![
        ObjectMapping::DATASET(dataset_id),
        ObjectMapping::COLLECTION(collection_one),
        ObjectMapping::COLLECTION(collection_two),
        ObjectMapping::OBJECT(object_one),
        ObjectMapping::OBJECT(object_two),
    ];
    let object_vec = object_id_map
        .iter()
        .map(|mapping| match mapping {
            ObjectMapping::PROJECT(id)
            | ObjectMapping::COLLECTION(id)
            | ObjectMapping::DATASET(id)
            | ObjectMapping::OBJECT(id) => *id,
        })
        .collect::<Vec<_>>();

    let create_dataset = test_utils::new_object(user_id, dataset_id, ObjectType::DATASET);
    let create_collection_one =
        test_utils::new_object(user_id, collection_one, ObjectType::COLLECTION);
    let create_collection_two =
        test_utils::new_object(user_id, collection_two, ObjectType::COLLECTION);
    let create_object_one = test_utils::new_object(user_id, object_one, ObjectType::OBJECT);
    let create_object_two = test_utils::new_object(user_id, object_two, ObjectType::OBJECT);

    let create_relation_one =
        test_utils::new_internal_relation(&create_collection_one, &create_dataset);
    let create_relation_two =
        test_utils::new_internal_relation(&create_collection_two, &create_dataset);
    let create_relation_three =
        test_utils::new_internal_relation(&create_dataset, &create_object_one);
    let create_relation_four =
        test_utils::new_internal_relation(&create_dataset, &create_object_two);

    let creates = vec![
        create_dataset.clone(),
        create_object_one.clone(),
        create_object_two.clone(),
        create_collection_one.clone(),
        create_collection_two.clone(),
    ];

    Object::batch_create(&creates, &client).await.unwrap();

    let rels = vec![
        create_relation_one.clone(),
        create_relation_two.clone(),
        create_relation_three.clone(),
        create_relation_four.clone(),
    ];
    InternalRelation::batch_create(&rels, &client)
        .await
        .unwrap();

    let compare_owr = ObjectWithRelations {
        object: create_dataset,
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::from_iter([
            (create_relation_one.origin_pid, create_relation_one.clone()),
            (create_relation_two.origin_pid, create_relation_two.clone()),
        ])),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::from_iter([
            (
                create_relation_three.target_pid,
                create_relation_three.clone(),
            ),
            (
                create_relation_four.target_pid,
                create_relation_four.clone(),
            ),
        ])),
    };
    let object_with_relations = Object::get_object_with_relations(&dataset_id, &client)
        .await
        .unwrap();
    assert_eq!(object_with_relations, compare_owr);

    let objects_with_relations = Object::get_objects_with_relations(&object_vec, &client)
        .await
        .unwrap();

    assert!(!objects_with_relations.is_empty());
    let compare_collection_one = ObjectWithRelations {
        object: create_collection_one,
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::default()),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::from_iter([(
            create_relation_one.target_pid,
            create_relation_one,
        )])),
    };
    let compare_collection_two = ObjectWithRelations {
        object: create_collection_two,
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::default()),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::from_iter([(
            create_relation_two.target_pid,
            create_relation_two,
        )])),
    };
    let compare_object_one = ObjectWithRelations {
        object: create_object_one,
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::from_iter([(
            create_relation_three.origin_pid,
            create_relation_three,
        )])),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::default()),
    };
    let compare_object_two = ObjectWithRelations {
        object: create_object_two,
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::from_iter([(
            create_relation_four.origin_pid,
            create_relation_four,
        )])),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::default()),
    };
    let compare_owrs = vec![
        compare_collection_one,
        compare_collection_two,
        compare_owr,
        compare_object_one,
        compare_object_two,
    ];
    assert!(objects_with_relations
        .iter()
        .all(|o| compare_owrs.contains(o)));
}

#[tokio::test]
async fn test_keyvals() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do objects stuff on database
    let client = db.get_client().await.unwrap();

    let obj_id = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let mut create_object = test_utils::new_object(user_id, obj_id, ObjectType::OBJECT);
    create_object.create(&client).await.unwrap();

    let kv = KeyValue {
        key: "one".to_string(),
        value: "two".to_string(),
        variant: KeyValueVariant::LABEL,
    };
    Object::add_key_value(&obj_id, &client, kv.clone())
        .await
        .unwrap();

    let object = Object::get(obj_id, &client).await.unwrap().unwrap();
    let test_object = create_object.clone();
    let comp_obj = Object {
        id: obj_id,
        revision_number: create_object.revision_number,
        name: create_object.name,
        title: create_object.title,
        description: create_object.description,
        created_at: create_object.created_at,
        created_by: create_object.created_by,
        content_len: create_object.content_len,
        count: create_object.count,
        key_values: Json(KeyValues(vec![kv.clone()])),
        object_status: create_object.object_status,
        data_class: create_object.data_class,
        object_type: create_object.object_type,
        external_relations: create_object.external_relations,
        hashes: create_object.hashes,
        dynamic: create_object.dynamic,
        endpoints: create_object.endpoints,
        data_license: ALL_RIGHTS_RESERVED.to_string(),
        metadata_license: ALL_RIGHTS_RESERVED.to_string(),
        authors: create_object.authors,
    };
    assert_eq!(object, comp_obj);
    object.remove_key_value(&client, kv).await.unwrap();
    let object = Object::get(obj_id, &client).await.unwrap().unwrap();
    let comp_obj = Object {
        id: obj_id,
        revision_number: test_object.revision_number,
        name: test_object.name,
        title: test_object.title,
        description: test_object.description,
        created_at: test_object.created_at,
        created_by: test_object.created_by,
        content_len: test_object.content_len,
        count: test_object.count,
        key_values: Json(KeyValues(Vec::new())),
        object_status: test_object.object_status,
        data_class: test_object.data_class,
        object_type: test_object.object_type,
        external_relations: test_object.external_relations,
        hashes: test_object.hashes,
        dynamic: test_object.dynamic,
        endpoints: test_object.endpoints,
        data_license: ALL_RIGHTS_RESERVED.to_string(),
        metadata_license: ALL_RIGHTS_RESERVED.to_string(),
        authors: test_object.authors,
    };
    assert_eq!(object, comp_obj);
}

#[tokio::test]
async fn test_external_relations() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do objects stuff on database
    let mut client = db.get_client().await.unwrap();
    let transaction = client.transaction().await.unwrap();

    let client = transaction.client();

    let obj_id = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let mut create_object = test_utils::new_object(user_id, obj_id, ObjectType::OBJECT);
    create_object.create(client).await.unwrap();
    let url = ExternalRelation {
        identifier: "test.test/abc".to_string(),
        defined_variant: DefinedVariant::URL,
        custom_variant: None,
    };
    let id = ExternalRelation {
        identifier: "a.b/c".to_string(),
        defined_variant: DefinedVariant::IDENTIFIER,
        custom_variant: None,
    };
    let custom = ExternalRelation {
        identifier: "ThIs Is A cUsToM fLaG".to_string(),
        defined_variant: DefinedVariant::CUSTOM,
        custom_variant: Some("This is not a URL or an identifier".to_string()),
    };
    let rels = vec![url.clone(), id.clone(), custom.clone()];
    Object::add_external_relations(&obj_id, client, rels.clone())
        .await
        .unwrap();
    let mut compare_obj = Object {
        id: obj_id,
        revision_number: create_object.revision_number,
        name: create_object.name,
        title: create_object.title,
        description: create_object.description,
        created_at: create_object.created_at,
        created_by: create_object.created_by,
        content_len: create_object.content_len,
        count: create_object.count,
        key_values: create_object.key_values,
        object_status: create_object.object_status,
        data_class: create_object.data_class,
        object_type: create_object.object_type,
        external_relations: Json(ExternalRelations(DashMap::from_iter(
            rels.clone().into_iter().map(|r| (r.identifier.clone(), r)),
        ))),
        hashes: create_object.hashes,
        dynamic: create_object.dynamic,
        endpoints: create_object.endpoints,
        data_license: ALL_RIGHTS_RESERVED.to_string(),
        metadata_license: ALL_RIGHTS_RESERVED.to_string(),
        authors: create_object.authors,
    };
    let obj = Object::get(obj_id, client).await.unwrap().unwrap();
    assert_eq!(compare_obj, obj);
    let rm_rels = vec![custom, id];
    let remain_rels = vec![url];
    Object::remove_external_relation(&obj_id, client, rm_rels)
        .await
        .unwrap();
    let rm = Object::get(obj_id, client).await.unwrap().unwrap();
    transaction.commit().await.unwrap();
    compare_obj.external_relations = Json(ExternalRelations(DashMap::from_iter(
        remain_rels.into_iter().map(|e| (e.identifier.clone(), e)),
    )));
    assert_eq!(compare_obj, rm);
}

#[tokio::test]
async fn test_updates() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do objects stuff on database
    let mut client = db.get_client().await.unwrap();
    let transaction = client.transaction().await.unwrap();

    let client = transaction.client();

    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let obj_id = DieselUlid::generate();
    let dat_id = DieselUlid::generate();
    let col_id = DieselUlid::generate();
    let proj_id = DieselUlid::generate();

    let mut create_object = test_utils::new_object(user_id, obj_id, ObjectType::OBJECT);
    let mut create_dataset = test_utils::new_object(user_id, dat_id, ObjectType::DATASET);
    let mut create_collection = test_utils::new_object(user_id, col_id, ObjectType::COLLECTION);
    let mut create_project = test_utils::new_object(user_id, proj_id, ObjectType::PROJECT);
    create_object.create(client).await.unwrap();
    create_dataset.create(client).await.unwrap();
    create_collection.create(client).await.unwrap();
    create_project.create(client).await.unwrap();

    // Update Object
    create_object.description = "This is a new description.".to_string();
    create_object.data_class = DataClass::PUBLIC;
    create_object.key_values = Json(KeyValues(vec![KeyValue {
        key: "NewKey".to_string(),
        value: "NewValue".to_string(),
        variant: KeyValueVariant::LABEL,
    }]));
    create_object.update(client).await.unwrap();
    let updated_object = Object::get(obj_id, client).await.unwrap().unwrap();
    assert_eq!(updated_object, create_object);

    // Update Dataset name
    let new_name = "new_dataset_name.xyz".to_string();
    Object::update_name(dat_id, new_name.clone(), client)
        .await
        .unwrap();
    let updated_dataset = Object::get(dat_id, client).await.unwrap().unwrap();
    create_dataset.name = new_name;
    assert_eq!(updated_dataset, create_dataset);

    // Update Collection description
    let new_description = "New description".to_string();
    Object::update_description(col_id, new_description.clone(), client)
        .await
        .unwrap();
    let updated_collection = Object::get(col_id, client).await.unwrap().unwrap();
    create_collection.description = new_description;
    assert_eq!(updated_collection, create_collection);

    // Update Project dataclass
    let new_dataclass = DataClass::PUBLIC;
    Object::update_dataclass(proj_id, new_dataclass.clone(), client)
        .await
        .unwrap();
    let updated_project = Object::get(proj_id, client).await.unwrap().unwrap();
    create_project.data_class = new_dataclass;
    assert_eq!(updated_project, create_project);

    let new_title = "NewTitle".to_string();
    Object::update_title(&obj_id, new_title.clone(), client)
        .await
        .unwrap();
    let new = Object::get(obj_id, client).await.unwrap().unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(new.title, new_title)
}

#[tokio::test]
async fn test_delete() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do objects stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let mut obj_ids = Vec::new();
    for _ in 1..5 {
        obj_ids.push(DieselUlid::generate());
    }
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let mut objects = Vec::new();
    for id in &obj_ids {
        objects.push(test_utils::new_object(user_id, *id, ObjectType::OBJECT));
    }
    Object::batch_create(&objects, client).await.unwrap();
    let objects = Object::get_objects(&obj_ids, client).await.unwrap();
    for o in objects {
        assert_eq!(o.object_status, ObjectStatus::AVAILABLE);
    }
    Object::set_deleted(&obj_ids, client).await.unwrap();
    let deleted = Object::get_objects(&obj_ids, client).await.unwrap();
    for o in deleted {
        assert_eq!(o.object_status, ObjectStatus::DELETED);
    }
}

#[tokio::test]
async fn archive_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do objects stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let dataset_id = DieselUlid::generate();
    let collection_one = DieselUlid::generate();
    let collection_two = DieselUlid::generate();
    let object_one = DieselUlid::generate();
    let object_two = DieselUlid::generate();
    let object_mapping = vec![
        ObjectMapping::DATASET(dataset_id),
        ObjectMapping::COLLECTION(collection_one),
        ObjectMapping::COLLECTION(collection_two),
        ObjectMapping::OBJECT(object_one),
        ObjectMapping::OBJECT(object_two),
    ];
    let object_vec = object_mapping
        .iter()
        .map(|om| match om {
            ObjectMapping::PROJECT(id)
            | ObjectMapping::COLLECTION(id)
            | ObjectMapping::DATASET(id)
            | ObjectMapping::OBJECT(id) => *id,
        })
        .collect::<Vec<_>>();
    let archive = object_vec.clone();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let create_dataset = test_utils::new_object(user_id, dataset_id, ObjectType::DATASET);
    let create_collection_one =
        test_utils::new_object(user_id, collection_one, ObjectType::COLLECTION);
    let create_collection_two =
        test_utils::new_object(user_id, collection_two, ObjectType::COLLECTION);
    let create_object_one = test_utils::new_object(user_id, object_one, ObjectType::OBJECT);
    let create_object_two = test_utils::new_object(user_id, object_two, ObjectType::OBJECT);

    let create_relation_one =
        test_utils::new_internal_relation(&create_collection_one, &create_dataset);
    let create_relation_two =
        test_utils::new_internal_relation(&create_collection_two, &create_dataset);
    let create_relation_three =
        test_utils::new_internal_relation(&create_dataset, &create_object_one);
    let create_relation_four =
        test_utils::new_internal_relation(&create_dataset, &create_object_two);

    let creates = vec![
        create_dataset.clone(),
        create_object_one.clone(),
        create_object_two.clone(),
        create_collection_one.clone(),
        create_collection_two.clone(),
    ];

    Object::batch_create(&creates, client).await.unwrap();

    let rels = vec![
        create_relation_one.clone(),
        create_relation_two.clone(),
        create_relation_three.clone(),
        create_relation_four.clone(),
    ];
    InternalRelation::batch_create(&rels, client).await.unwrap();

    // Test archive
    let archived_objects = Object::archive(&archive, client).await.unwrap();
    for o in archived_objects {
        assert!(!o.dynamic);
    }
}

#[tokio::test]
async fn add_remove_endpoint_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do objects stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    // Create objects with endpoints
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let project_id = DieselUlid::generate();
    let collection_id = DieselUlid::generate();
    let object_id = DieselUlid::generate();

    let endpoint1 = DieselUlid::generate();
    let endpoint2 = DieselUlid::generate();
    let endpoint3 = DieselUlid::generate();

    let replication = aruna_server::database::enums::ReplicationType::FullSync;
    let mut project = test_utils::new_object(user_id, project_id, ObjectType::PROJECT);
    project.endpoints = Json(DashMap::from_iter([(
        endpoint1,
        EndpointInfo {
            replication,
            status: None,
        },
    )]));

    let mut collection = test_utils::new_object(user_id, collection_id, ObjectType::COLLECTION);
    collection.endpoints = Json(DashMap::from_iter([
        (
            endpoint1,
            EndpointInfo {
                replication,
                status: None,
            },
        ),
        (
            endpoint2,
            EndpointInfo {
                replication,
                status: None,
            },
        ),
    ]));

    let mut object = test_utils::new_object(user_id, object_id, ObjectType::OBJECT);
    object.endpoints = Json(DashMap::from_iter([
        (
            endpoint1,
            EndpointInfo {
                replication,
                status: Some(ReplicationStatus::Waiting),
            },
        ),
        (
            endpoint2,
            EndpointInfo {
                replication,
                status: Some(ReplicationStatus::Waiting),
            },
        ),
    ]));

    Object::batch_create(
        &vec![project.clone(), collection.clone(), object.clone()],
        client,
    )
    .await
    .unwrap();

    // Create relations
    let p_c_r = test_utils::new_internal_relation(&project, &collection);
    let c_o_r = test_utils::new_internal_relation(&collection, &object);

    InternalRelation::batch_create(&vec![p_c_r, c_o_r], client)
        .await
        .unwrap();

    // Validate resources have a single endpoint
    for resource_id in [project_id, collection_id, object_id] {
        let resource = Object::get(resource_id, client).await.unwrap().unwrap();

        if resource.id == project_id {
            assert_eq!(resource.endpoints.0.len(), 1);
            assert!(resource.endpoints.0.contains_key(&endpoint1));
        } else {
            assert_eq!(resource.endpoints.0.len(), 2);
            assert!(resource.endpoints.0.contains_key(&endpoint1));
            assert!(resource.endpoints.0.contains_key(&endpoint2));
        }
    }

    // Add another endpoint to the Object
    let object = Object::add_endpoint(
        client,
        &object_id,
        &endpoint3,
        EndpointInfo {
            replication,
            status: Some(ReplicationStatus::Waiting),
        },
    )
    .await
    .unwrap();

    assert_eq!(object.endpoints.0.len(), 3);
    assert!(object.endpoints.0.contains_key(&endpoint1));
    assert!(object.endpoints.0.contains_key(&endpoint2));
    assert!(object.endpoints.0.contains_key(&endpoint3));

    // Remove third endpoint from object
    let object = Object::remove_endpoint(client, &object_id, &endpoint3)
        .await
        .unwrap();
    assert_eq!(object.endpoints.0.len(), 2);
    assert!(object.endpoints.0.contains_key(&endpoint1));
    assert!(object.endpoints.0.contains_key(&endpoint2));

    // Remove second endpoint from all resources
    let resources = Object::remove_endpoint_from_objects(client, &endpoint2)
        .await
        .unwrap();

    for resource in resources {
        if [project_id, collection_id, object_id].contains(&resource.id) {
            assert_eq!(resource.endpoints.0.len(), 1);
            assert!(resource.endpoints.0.contains_key(&endpoint1));
        }
    }

    // Remove third endpoint from object
    let object = Object::remove_endpoint(client, &object_id, &endpoint3)
        .await
        .unwrap();
    assert_eq!(object.endpoints.0.len(), 1);
    assert!(object.endpoints.0.contains_key(&endpoint1));

    // Try to remove last remaining endpoint --> Error
    for resource_id in [project_id, collection_id, object_id] {
        let resource = Object::remove_endpoint(client, &resource_id, &endpoint1)
            .await
            .unwrap();
        assert!(resource.endpoints.0.is_empty());
    }
}
