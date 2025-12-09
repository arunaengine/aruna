use crate::common::init::init_database;
use crate::common::init_server::Component;
use crate::common::test_utils::USER1_ULID;
use crate::common::{infra, test_utils};
use aruna_server::database::dsls::license_dsl::ALL_RIGHTS_RESERVED;
use aruna_server::database::dsls::object_dsl::{DefinedVariant, ExternalRelation};
use aruna_server::database::enums::{ObjectMapping, ObjectType};
use aruna_server::database::{
    crud::CrudDb,
    dsls::{
        internal_relation_dsl::InternalRelation,
        object_dsl::{ExternalRelations, Object, ObjectWithRelations},
    },
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use postgres_types::Json;
use std::str::FromStr;
use tokio_postgres::GenericClient;

#[tokio::test]
async fn test_external_relations() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do relations stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

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
    compare_obj.external_relations = Json(ExternalRelations(DashMap::from_iter(
        remain_rels.into_iter().map(|e| (e.identifier.clone(), e)),
    )));
    assert_eq!(compare_obj, rm);
}

#[tokio::test]
async fn get_object_with_relations_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do relations stuff on database
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
    let object_with_relations = Object::get_object_with_relations(&dataset_id, client)
        .await
        .unwrap();
    assert_eq!(object_with_relations, compare_owr);

    let objects_with_relations = Object::get_objects_with_relations(&object_vec, client)
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
async fn delete_relations() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do relations stuff on database
    let client = db.get_client().await.unwrap();

    let client = client.client();

    let dataset_id = DieselUlid::generate();
    let object_id = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let create_object = test_utils::new_object(user_id, object_id, ObjectType::OBJECT);
    let create_dataset = test_utils::new_object(user_id, dataset_id, ObjectType::OBJECT);

    let relation_one = test_utils::new_internal_relation(&create_dataset, &create_object);
    let mut relation_two = test_utils::new_internal_relation(&create_dataset, &create_object);
    relation_two.relation_name = "ORIGIN".to_string();
    let mut relation_three = test_utils::new_internal_relation(&create_dataset, &create_object);
    relation_three.relation_name = "VERSION".to_string();
    let mut relation_four = test_utils::new_internal_relation(&create_dataset, &create_object);
    relation_four.relation_name = "METADATA".to_string();
    let mut relation_five = test_utils::new_internal_relation(&create_dataset, &create_object);
    relation_five.relation_name = "POLICY".to_string();
    let rel_ids = vec![
        relation_one.id,
        relation_two.id,
        relation_three.id,
        relation_four.id,
        relation_five.id,
    ];
    let rel_vec = vec![
        relation_one,
        relation_two,
        relation_three,
        relation_four,
        relation_five,
    ];

    Object::batch_create(&vec![create_dataset, create_object], client)
        .await
        .unwrap();
    InternalRelation::batch_create(&rel_vec, client)
        .await
        .unwrap();

    let object_with_relations = Object::get_object_with_relations(&object_id, client)
        .await
        .unwrap();
    let dataset_with_relations = Object::get_object_with_relations(&dataset_id, client)
        .await
        .unwrap();
    assert!(!object_with_relations.inbound_belongs_to.0.is_empty());
    assert!(!object_with_relations.inbound.0.is_empty());
    assert!(!dataset_with_relations.outbound_belongs_to.0.is_empty());
    assert!(!dataset_with_relations.outbound.0.is_empty());
    InternalRelation::batch_delete(&rel_ids, client)
        .await
        .unwrap();
    let object_without_relations = Object::get_object_with_relations(&object_id, client)
        .await
        .unwrap();
    let dataset_without_relations = Object::get_object_with_relations(&dataset_id, client)
        .await
        .unwrap();

    assert!(object_without_relations.inbound_belongs_to.0.is_empty());
    assert!(object_without_relations.inbound.0.is_empty());
    assert!(dataset_without_relations.outbound_belongs_to.0.is_empty());
    assert!(dataset_without_relations.outbound.0.is_empty());
}

#[tokio::test]
async fn get_by() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do relations stuff on database
    let client = db.get_client().await.unwrap();

    let client = client.client();

    let dataset_id = DieselUlid::generate();
    let object_id = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let create_project = test_utils::new_object(user_id, object_id, ObjectType::PROJECT);
    let create_collection = test_utils::new_object(user_id, dataset_id, ObjectType::COLLECTION);

    let relation_one = test_utils::new_internal_relation(&create_project, &create_collection);

    let rel_vec = [relation_one.clone()];
    Object::batch_create(&vec![create_project, create_collection], client)
        .await
        .unwrap();
    InternalRelation::batch_create(&[relation_one], client)
        .await
        .unwrap();

    let all = InternalRelation::get_all_by_id(&dataset_id, client)
        .await
        .unwrap();

    assert!(all.iter().all(|r| rel_vec.iter().contains(r)))
}
