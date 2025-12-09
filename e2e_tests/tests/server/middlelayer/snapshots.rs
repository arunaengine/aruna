use crate::common::test_utils::USER1_ULID;
use crate::common::{init, init_server, test_utils};
use aruna_rust_api::api::storage::services::v2::{
    ArchiveProjectRequest, SnapshotCollectionRequest, SnapshotDatasetRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_VERSION,
};
use aruna_server::database::dsls::object_dsl::Object;
use aruna_server::database::enums::{ObjectMapping, ObjectType};
use aruna_server::middlelayer::snapshot_request_types::SnapshotRequest;
use diesel_ulid::DieselUlid;
use std::str::FromStr;

#[tokio::test]
async fn test_archive() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let p_id = DieselUlid::generate();
    let ulids = vec![
        ObjectMapping::PROJECT(p_id),
        ObjectMapping::COLLECTION(DieselUlid::generate()),
        ObjectMapping::COLLECTION(DieselUlid::generate()),
        ObjectMapping::DATASET(DieselUlid::generate()),
        ObjectMapping::DATASET(DieselUlid::generate()),
        ObjectMapping::OBJECT(DieselUlid::generate()),
        ObjectMapping::OBJECT(DieselUlid::generate()),
    ];

    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let mut objects = Vec::new();
    for u in &ulids {
        match u {
            ObjectMapping::PROJECT(id) => {
                objects.push(test_utils::new_object(user_id, *id, ObjectType::PROJECT))
            }
            ObjectMapping::COLLECTION(id) => {
                objects.push(test_utils::new_object(user_id, *id, ObjectType::COLLECTION))
            }
            ObjectMapping::DATASET(id) => {
                objects.push(test_utils::new_object(user_id, *id, ObjectType::DATASET))
            }
            ObjectMapping::OBJECT(id) => {
                objects.push(test_utils::new_object(user_id, *id, ObjectType::OBJECT))
            }
        }
    }
    let p_c1 = test_utils::new_internal_relation(&objects[0], &objects[1]);
    let p_c2 = test_utils::new_internal_relation(&objects[0], &objects[2]);
    let c1_d1 = test_utils::new_internal_relation(&objects[1], &objects[3]);
    let c2_d2 = test_utils::new_internal_relation(&objects[2], &objects[4]);
    let d1_o1 = test_utils::new_internal_relation(&objects[3], &objects[5]);
    let d2_o2 = test_utils::new_internal_relation(&objects[4], &objects[6]);
    let rels = vec![p_c1, p_c2, c1_d1, c2_d2, d1_o1, d2_o2];

    let client = db_handler.database.get_client().await.unwrap();
    Object::batch_create(&objects, &client).await.unwrap();
    InternalRelation::batch_create(&rels, &client)
        .await
        .unwrap();

    // test
    let request = SnapshotRequest::Project(ArchiveProjectRequest {
        project_id: p_id.to_string(),
    });
    let (_, archive) = db_handler.snapshot(request).await.unwrap();
    assert!(archive.iter().all(|o| !o.object.dynamic));
}
#[tokio::test]
async fn test_snapshot_collection() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let collection_id = DieselUlid::generate();
    let d1_id = DieselUlid::generate();
    let d2_id = DieselUlid::generate();
    let o1_id = DieselUlid::generate();
    let o2_id = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let mut collection = test_utils::new_object(user_id, collection_id, ObjectType::COLLECTION);
    let mut ds_1 = test_utils::new_object(user_id, d1_id, ObjectType::DATASET);
    let mut ds_2 = test_utils::new_object(user_id, d2_id, ObjectType::DATASET);
    collection.dynamic = true;
    ds_1.dynamic = true;
    ds_2.dynamic = true;
    let object_1 = test_utils::new_object(user_id, o1_id, ObjectType::OBJECT);
    let object_2 = test_utils::new_object(user_id, o2_id, ObjectType::OBJECT);
    let c_d1 = test_utils::new_internal_relation(&collection, &ds_1);
    let c_d2 = test_utils::new_internal_relation(&collection, &ds_2);
    let d1_o1 = test_utils::new_internal_relation(&ds_1, &object_1);
    let d2_o2 = test_utils::new_internal_relation(&ds_2, &object_2);
    let rels = vec![c_d1, c_d2, d1_o1, d2_o2];
    let objects = vec![collection, ds_1, ds_2, object_1, object_2];
    let client = db_handler.database.get_client().await.unwrap();

    Object::batch_create(&objects, &client).await.unwrap();
    InternalRelation::batch_create(&rels, &client)
        .await
        .unwrap();

    // test
    let request = SnapshotRequest::Collection(SnapshotCollectionRequest {
        collection_id: collection_id.to_string(),
    });
    let (new, snapshot) = db_handler.snapshot(request).await.unwrap();
    assert!(snapshot.iter().all(|o| !o.object.dynamic));
    let old_coll = Object::get_object_with_relations(&collection_id, &client)
        .await
        .unwrap();
    let old_ds_1 = Object::get(d1_id, &client).await.unwrap().unwrap();
    let old_ds_2 = Object::get(d2_id, &client).await.unwrap().unwrap();
    assert!(old_coll.object.dynamic);
    assert!(old_ds_1.dynamic);
    assert!(old_ds_2.dynamic);

    let version = old_coll
        .inbound
        .0
        .iter()
        .find(|r| r.relation_name == *INTERNAL_RELATION_VARIANT_VERSION)
        .unwrap();
    assert_eq!(version.target_pid, collection_id);
    assert_eq!(version.origin_pid, new);
}
#[tokio::test]
async fn test_snapshot_dataset() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let dataset_id = DieselUlid::generate();
    let o1_id = DieselUlid::generate();
    let o2_id = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let mut dataset = test_utils::new_object(user_id, dataset_id, ObjectType::DATASET);
    dataset.dynamic = true;
    let object_1 = test_utils::new_object(user_id, o1_id, ObjectType::OBJECT);
    let object_2 = test_utils::new_object(user_id, o2_id, ObjectType::OBJECT);
    let d_o1 = test_utils::new_internal_relation(&dataset, &object_1);
    let d_o2 = test_utils::new_internal_relation(&dataset, &object_2);
    let rels = vec![d_o1, d_o2];
    let objects = vec![dataset, object_1, object_2];
    let client = db_handler.database.get_client().await.unwrap();

    Object::batch_create(&objects, &client).await.unwrap();
    InternalRelation::batch_create(&rels, &client)
        .await
        .unwrap();

    // test
    let request = SnapshotRequest::Dataset(SnapshotDatasetRequest {
        dataset_id: dataset_id.to_string(),
    });
    let (new, snapshot) = db_handler.snapshot(request).await.unwrap();
    assert!(snapshot.iter().all(|o| !o.object.dynamic));
    let old = Object::get_object_with_relations(&dataset_id, &client)
        .await
        .unwrap();
    assert!(old.object.dynamic);

    let version = old
        .inbound
        .0
        .iter()
        .find(|r| r.relation_name == *INTERNAL_RELATION_VARIANT_VERSION)
        .unwrap();
    assert_eq!(version.target_pid, dataset_id);
    assert_eq!(version.origin_pid, new);
}
