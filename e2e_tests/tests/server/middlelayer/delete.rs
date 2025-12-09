use crate::common::test_utils::{new_internal_relation, new_object, USER1_ULID};
use crate::common::{init, init_server};
use aruna_rust_api::api::storage::services::v2::{
    DeleteCollectionRequest, DeleteDatasetRequest, DeleteObjectRequest, DeleteProjectRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO, INTERNAL_RELATION_VARIANT_VERSION,
};
use aruna_server::database::dsls::object_dsl::Object;
use aruna_server::database::enums::{ObjectStatus, ObjectType};
use aruna_server::middlelayer::delete_request_types::DeleteRequest;
use diesel_ulid::DieselUlid;
use std::str::FromStr;

#[tokio::test]
async fn delete_project() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();

    // get user + project
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let project_id = DieselUlid::generate();
    let mut project = new_object(user_id, project_id, ObjectType::PROJECT);
    project.create(&client).await.unwrap();

    // test request
    let delete_request = DeleteRequest::Project(DeleteProjectRequest {
        project_id: project_id.to_string(),
    });
    db_handler.delete_resource(delete_request).await.unwrap();
    assert_eq!(
        Object::get(project_id, &client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
}

#[tokio::test]
async fn delete_collection() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = &db_handler.database.get_client().await.unwrap();

    // get user + collection
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let collection_id = DieselUlid::generate();
    let mut collection = new_object(user_id, collection_id, ObjectType::COLLECTION);
    collection.create(client).await.unwrap();

    // Test request
    let delete_request = DeleteRequest::Collection(DeleteCollectionRequest {
        collection_id: collection_id.to_string(),
    });
    db_handler.delete_resource(delete_request).await.unwrap();
    assert_eq!(
        Object::get(collection_id, client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
}

#[tokio::test]
async fn delete_dataset() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = &db_handler.database.get_client().await.unwrap();

    // get user + dataset
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let dataset_id = DieselUlid::generate();
    let mut dataset = new_object(user_id, dataset_id, ObjectType::DATASET);
    dataset.create(client).await.unwrap();

    // Test request
    let delete_request = DeleteRequest::Dataset(DeleteDatasetRequest {
        dataset_id: dataset_id.to_string(),
    });
    db_handler.delete_resource(delete_request).await.unwrap();
    assert_eq!(
        Object::get(dataset_id, client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
}

#[tokio::test]
async fn delete_object() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = &db_handler.database.get_client().await.unwrap();

    // get user + objects
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let object_id = DieselUlid::generate();
    let object_v1_id = DieselUlid::generate();
    let object_v2_id = DieselUlid::generate();
    let mut objects: Vec<Object> = Vec::new();

    for o in [object_id, object_v1_id, object_v2_id] {
        objects.push(new_object(user_id, o, ObjectType::OBJECT));
    }
    let proj_id = DieselUlid::generate();
    objects.push(new_object(user_id, proj_id, ObjectType::PROJECT));

    let proj_relations = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: proj_id,
        origin_type: ObjectType::PROJECT,
        relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
        target_pid: object_id,
        target_type: ObjectType::OBJECT,
        target_name: objects[0].name.to_string(),
    };

    let relation_one = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: object_v1_id,
        origin_type: ObjectType::OBJECT,
        relation_name: INTERNAL_RELATION_VARIANT_VERSION.to_string(),
        target_pid: object_id,
        target_type: ObjectType::OBJECT,
        target_name: objects[0].name.to_string(),
    };
    let relation_two = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: object_v2_id,
        origin_type: ObjectType::OBJECT,
        relation_name: INTERNAL_RELATION_VARIANT_VERSION.to_string(),
        target_pid: object_id,
        target_type: ObjectType::OBJECT,
        target_name: objects[1].name.to_string(),
    };
    Object::batch_create(&objects, client).await.unwrap();
    InternalRelation::batch_create(&vec![proj_relations, relation_one, relation_two], client)
        .await
        .unwrap();

    // Test request
    let mut inner_request = DeleteObjectRequest {
        object_id: object_id.to_string(),
        with_revisions: false,
    };

    let result = db_handler
        .delete_resource(DeleteRequest::Object(inner_request.clone()))
        .await;
    assert!(result.is_err()); // Undeleted versions exist

    inner_request.with_revisions = true;
    db_handler
        .delete_resource(DeleteRequest::Object(inner_request))
        .await
        .unwrap();

    assert_eq!(
        Object::get(object_id, client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
    assert_eq!(
        Object::get(object_v1_id, client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
    assert_eq!(
        Object::get(object_v2_id, client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
}

#[tokio::test]
async fn delete_hierarchies() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = &db_handler.database.get_client().await.unwrap();

    // Init user
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    // Create hierarchy
    let project_01 = new_object(user_id, DieselUlid::generate(), ObjectType::PROJECT);
    let project_02 = new_object(user_id, DieselUlid::generate(), ObjectType::PROJECT);
    let collection_01 = new_object(user_id, DieselUlid::generate(), ObjectType::COLLECTION);
    let collection_02 = new_object(user_id, DieselUlid::generate(), ObjectType::COLLECTION);
    let dataset_01 = new_object(user_id, DieselUlid::generate(), ObjectType::DATASET);
    let dataset_02 = new_object(user_id, DieselUlid::generate(), ObjectType::DATASET);
    let object_01 = new_object(user_id, DieselUlid::generate(), ObjectType::OBJECT);
    let object_02 = new_object(user_id, DieselUlid::generate(), ObjectType::OBJECT);
    let object_03 = new_object(user_id, DieselUlid::generate(), ObjectType::OBJECT);

    let p1_c1 = new_internal_relation(&project_01, &collection_01);
    let p1_c2 = new_internal_relation(&project_01, &collection_02);
    let p2_c2 = new_internal_relation(&project_02, &collection_02);
    let c1_d1 = new_internal_relation(&collection_01, &dataset_01);
    let c2_d1 = new_internal_relation(&collection_02, &dataset_01);
    let c2_d2 = new_internal_relation(&collection_02, &dataset_02);
    let d1_o1 = new_internal_relation(&dataset_01, &object_01);
    let d1_o2 = new_internal_relation(&dataset_01, &object_02);
    let d2_o2 = new_internal_relation(&dataset_02, &object_02);
    let d2_o3 = new_internal_relation(&dataset_02, &object_03);

    Object::batch_create(
        &vec![
            project_01.clone(),
            project_02,
            collection_01.clone(),
            collection_02.clone(),
            dataset_01.clone(),
            dataset_02.clone(),
            object_01.clone(),
            object_02.clone(),
            object_03.clone(),
        ],
        client,
    )
    .await
    .unwrap();

    InternalRelation::batch_create(
        &vec![
            p1_c1, p1_c2, p2_c2, c1_d1, c2_d1, c2_d2, d1_o1, d1_o2, d2_o2, d2_o3,
        ],
        client,
    )
    .await
    .unwrap();

    // Sync cache with database
    db_handler
        .cache
        .sync_cache(db_handler.database.clone())
        .await
        .unwrap();

    // Delete object_01
    //  - Assert object_status is DELETED
    //  - Assert inbound_belongs_to and outbound_belongs_to is empty
    //  - Assert inbound contains DELETED relation to parent
    let deleted_resources = db_handler
        .delete_resource(DeleteRequest::Object(DeleteObjectRequest {
            object_id: object_01.id.to_string(),
            with_revisions: false,
        }))
        .await
        .unwrap();
    assert_eq!(deleted_resources.len(), 1); // Only object deleted

    let del_obj = db_handler.cache.get_object(&object_01.id).unwrap(); // deleted_objects.first().unwrap();
    assert_eq!(del_obj.object.object_status, ObjectStatus::DELETED);
    assert!(del_obj.inbound_belongs_to.0.is_empty());
    assert!(del_obj.outbound_belongs_to.0.is_empty());
    assert!(del_obj.outbound.0.is_empty());

    assert_eq!(del_obj.inbound.0.len(), 1);
    for del_rel in del_obj.inbound.0 {
        assert_eq!(del_rel.1.origin_pid, dataset_01.id);
        assert_eq!(del_rel.1.target_pid, object_01.id);
        assert_eq!(&del_rel.1.relation_name, "DELETED")
    }

    // Try delete dataset_01 -> Error(object_02 has multiple parents)
    let result = db_handler
        .delete_resource(DeleteRequest::Dataset(DeleteDatasetRequest {
            dataset_id: dataset_01.id.to_string(),
        }))
        .await;
    assert!(result.is_err());

    // Delete object_02
    let deleted_resources = db_handler
        .delete_resource(DeleteRequest::Object(DeleteObjectRequest {
            object_id: object_02.id.to_string(),
            with_revisions: false,
        }))
        .await
        .unwrap();
    assert_eq!(deleted_resources.len(), 1);

    let del_obj = db_handler.cache.get_object(&object_02.id).unwrap(); // deleted_objects.first().unwrap();
    assert_eq!(del_obj.object.object_status, ObjectStatus::DELETED);
    assert!(del_obj.inbound_belongs_to.0.is_empty());
    assert!(del_obj.outbound_belongs_to.0.is_empty());

    // Delete dataset_01
    //  - Assert dataset_01 object_status is DELETED
    //  - Assert inbound_belongs_to and outbound_belongs_to is empty
    //  - Assert relations parents and children are DELETED
    let deleted_objects = db_handler
        .delete_resource(DeleteRequest::Dataset(DeleteDatasetRequest {
            dataset_id: dataset_01.id.to_string(),
        }))
        .await
        .unwrap();
    assert_eq!(deleted_objects.len(), 1); // Only dataset got deleted

    let del_obj = db_handler.cache.get_object(&dataset_01.id).unwrap();
    assert_eq!(del_obj.object.object_status, ObjectStatus::DELETED);
    assert!(del_obj.inbound_belongs_to.0.is_empty());
    assert!(del_obj.outbound_belongs_to.0.is_empty());

    assert_eq!(del_obj.inbound.0.len(), 2);
    for del_rel in del_obj.inbound.0 {
        assert_eq!(&del_rel.1.relation_name, "DELETED")
    }
    assert_eq!(del_obj.outbound.0.len(), 2);
    for del_rel in del_obj.outbound.0 {
        assert_eq!(&del_rel.1.relation_name, "DELETED")
    }

    // Delete collection_02
    //  - Assert collection_02 status is DELETED
    //  - Assert inbound_belongs_to and outbound_belongs_to is empty
    //  - Assert relations to dataset_01, dataset_02 and project_02 are DELETED
    let deleted_objects = db_handler
        .delete_resource(DeleteRequest::Dataset(DeleteDatasetRequest {
            dataset_id: collection_02.id.to_string(),
        }))
        .await
        .unwrap();
    assert_eq!(deleted_objects.len(), 3); // collection_02, dataset_02 and object_03

    let del_obj = db_handler.cache.get_object(&object_03.id).unwrap(); // deleted_objects.first().unwrap();
    assert_eq!(del_obj.object.object_status, ObjectStatus::DELETED);
    assert!(del_obj.inbound_belongs_to.0.is_empty());
    assert!(del_obj.outbound_belongs_to.0.is_empty());
    assert!(del_obj.outbound.0.is_empty());

    assert_eq!(del_obj.inbound.0.len(), 1);
    for del_rel in del_obj.inbound.0 {
        assert_eq!(del_rel.1.origin_pid, dataset_02.id);
        assert_eq!(del_rel.1.target_pid, object_03.id);
        assert_eq!(&del_rel.1.relation_name, "DELETED")
    }

    for resource_id in [&collection_02.id, &dataset_02.id] {
        let del_obj = db_handler.cache.get_object(resource_id).unwrap();
        assert_eq!(del_obj.object.object_status, ObjectStatus::DELETED);
        assert!(del_obj.inbound_belongs_to.0.is_empty());
        assert!(del_obj.outbound_belongs_to.0.is_empty());

        assert_eq!(
            del_obj.inbound.0.len(),
            if resource_id == &collection_02.id {
                2
            } else {
                1
            }
        );
        for del_rel in del_obj.inbound.0 {
            assert_eq!(&del_rel.1.relation_name, "DELETED")
        }
        assert_eq!(del_obj.outbound.0.len(), 2);
        for del_rel in del_obj.outbound.0 {
            assert_eq!(&del_rel.1.relation_name, "DELETED")
        }
    }

    // Delete project_01
    //  - Assert project_01 status is DELETED
    //  - Assert inbound_belongs_to and outbound_belongs_to is empty
    let deleted_objects = db_handler
        .delete_resource(DeleteRequest::Dataset(DeleteDatasetRequest {
            dataset_id: project_01.id.to_string(),
        }))
        .await
        .unwrap();
    assert_eq!(deleted_objects.len(), 2); // project_01, collection_01

    let del_obj = db_handler.cache.get_object(&project_01.id).unwrap(); // deleted_objects.first().unwrap();
    assert_eq!(del_obj.object.object_status, ObjectStatus::DELETED);
    assert!(del_obj.inbound_belongs_to.0.is_empty());
    assert!(del_obj.outbound_belongs_to.0.is_empty());
    assert!(del_obj.inbound.0.is_empty());

    assert_eq!(del_obj.outbound.0.len(), 2);
    for del_rel in del_obj.inbound.0 {
        assert_eq!(del_rel.1.origin_pid, project_01.id);
        assert!([collection_01.id, collection_02.id].contains(&del_rel.1.target_pid));
        assert_eq!(&del_rel.1.relation_name, "DELETED")
    }

    let del_obj = db_handler.cache.get_object(&collection_01.id).unwrap();
    assert_eq!(del_obj.object.object_status, ObjectStatus::DELETED);
    assert!(del_obj.inbound_belongs_to.0.is_empty());
    assert!(del_obj.outbound_belongs_to.0.is_empty());

    assert_eq!(del_obj.inbound.0.len(), 1);
    for del_rel in del_obj.inbound.0 {
        assert_eq!(del_rel.1.origin_pid, project_01.id);
        assert_eq!(del_rel.1.target_pid, collection_01.id);
        assert_eq!(&del_rel.1.relation_name, "DELETED")
    }
    assert_eq!(del_obj.outbound.0.len(), 1);
    for del_rel in del_obj.outbound.0 {
        assert_eq!(del_rel.1.origin_pid, collection_01.id);
        assert_eq!(del_rel.1.target_pid, dataset_01.id);
        assert_eq!(&del_rel.1.relation_name, "DELETED")
    }
}
