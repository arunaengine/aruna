use crate::common::init::init_database_handler_middlelayer;
use crate::common::test_utils;
use aruna_rust_api::api::storage::models::v2::{Hash as APIHash, KeyValue as APIKeyValue};
use aruna_rust_api::api::storage::services::v2::{
    update_object_request::Parent, UpdateCollectionDataClassRequest,
    UpdateCollectionDescriptionRequest, UpdateCollectionKeyValuesRequest,
    UpdateCollectionNameRequest, UpdateDatasetDataClassRequest, UpdateDatasetDescriptionRequest,
    UpdateDatasetKeyValuesRequest, UpdateDatasetNameRequest, UpdateObjectRequest,
    UpdateProjectDataClassRequest, UpdateProjectDescriptionRequest, UpdateProjectKeyValuesRequest,
    UpdateProjectNameRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::license_dsl::{License, ALL_RIGHTS_RESERVED};
use aruna_server::database::dsls::object_dsl::{KeyValue, KeyValueVariant, KeyValues, Object};
use aruna_server::database::enums::{DataClass, ObjectMapping, ObjectStatus, ObjectType};
use aruna_server::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use postgres_types::Json;

#[tokio::test]
async fn test_update_dataclass() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;
    let resources = vec![
        ObjectMapping::PROJECT(DieselUlid::generate()),
        ObjectMapping::COLLECTION(DieselUlid::generate()),
        ObjectMapping::DATASET(DieselUlid::generate()),
    ];
    let mut user = test_utils::new_user(resources.clone());
    let mut objects = Vec::new();
    for r in resources {
        objects.push(test_utils::object_from_mapping(user.id, r));
    }
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();
    Object::batch_create(&objects, &client).await.unwrap();

    // Test
    assert!(
        Object::get_objects(&objects.iter().map(|o| o.id).collect(), &client)
            .await
            .unwrap()
            .iter()
            .all(|o| o.data_class == DataClass::PRIVATE)
    );
    for r in objects {
        match r.object_type {
            ObjectType::PROJECT => {
                let request = DataClassUpdate::Project(UpdateProjectDataClassRequest {
                    project_id: r.id.to_string(),
                    data_class: 1,
                });
                db_handler.update_dataclass(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client)
                        .await
                        .unwrap()
                        .unwrap()
                        .data_class,
                    DataClass::PUBLIC
                );
            }
            ObjectType::COLLECTION => {
                let request = DataClassUpdate::Collection(UpdateCollectionDataClassRequest {
                    collection_id: r.id.to_string(),
                    data_class: 1,
                });
                db_handler.update_dataclass(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client)
                        .await
                        .unwrap()
                        .unwrap()
                        .data_class,
                    DataClass::PUBLIC
                );
            }
            ObjectType::DATASET => {
                let request = DataClassUpdate::Dataset(UpdateDatasetDataClassRequest {
                    dataset_id: r.id.to_string(),
                    data_class: 3,
                });
                assert!(db_handler.update_dataclass(request).await.is_err());
            }
            _ => panic!(),
        };
    }
}
#[tokio::test]
async fn test_update_name() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;
    let resources = vec![
        ObjectMapping::PROJECT(DieselUlid::generate()),
        ObjectMapping::COLLECTION(DieselUlid::generate()),
        ObjectMapping::DATASET(DieselUlid::generate()),
    ];
    let mut user = test_utils::new_user(resources.clone());
    let mut objects = Vec::new();
    for r in resources {
        objects.push(test_utils::object_from_mapping(user.id, r));
    }
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();
    Object::batch_create(&objects, &client).await.unwrap();

    // Test
    assert!(
        Object::get_objects(&objects.iter().map(|o| o.id).collect(), &client)
            .await
            .unwrap()
            .iter()
            .all(|o| o.name == *"a")
    );
    for r in objects {
        match r.object_type {
            ObjectType::PROJECT => {
                let request = NameUpdate::Project(UpdateProjectNameRequest {
                    project_id: r.id.to_string(),
                    name: "project-name".to_string(),
                });
                db_handler.update_name(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client).await.unwrap().unwrap().name,
                    *"project-name"
                );
            }
            ObjectType::COLLECTION => {
                let request = NameUpdate::Collection(UpdateCollectionNameRequest {
                    collection_id: r.id.to_string(),
                    name: "collection_name".to_string(),
                });
                db_handler.update_name(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client).await.unwrap().unwrap().name,
                    *"collection_name"
                );
            }
            ObjectType::DATASET => {
                let request = NameUpdate::Dataset(UpdateDatasetNameRequest {
                    dataset_id: r.id.to_string(),
                    name: "dataset_name".to_string(),
                });
                db_handler.update_name(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client).await.unwrap().unwrap().name,
                    *"dataset_name"
                );
            }
            _ => panic!(),
        };
    }
}
#[tokio::test]
async fn test_update_description() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;
    let resources = vec![
        ObjectMapping::PROJECT(DieselUlid::generate()),
        ObjectMapping::COLLECTION(DieselUlid::generate()),
        ObjectMapping::DATASET(DieselUlid::generate()),
    ];
    let mut user = test_utils::new_user(resources.clone());
    let mut objects = Vec::new();
    for r in resources {
        objects.push(test_utils::object_from_mapping(user.id, r));
    }
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();
    Object::batch_create(&objects, &client).await.unwrap();

    // Test
    assert!(
        Object::get_objects(&objects.iter().map(|o| o.id).collect(), &client)
            .await
            .unwrap()
            .iter()
            .all(|o| o.description == *"b")
    );
    for r in objects {
        match r.object_type {
            ObjectType::PROJECT => {
                let request = DescriptionUpdate::Project(UpdateProjectDescriptionRequest {
                    project_id: r.id.to_string(),
                    description: "project_description".to_string(),
                });
                db_handler.update_description(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client)
                        .await
                        .unwrap()
                        .unwrap()
                        .description,
                    *"project_description"
                );
            }
            ObjectType::COLLECTION => {
                let request = DescriptionUpdate::Collection(UpdateCollectionDescriptionRequest {
                    collection_id: r.id.to_string(),
                    description: "collection_description".to_string(),
                });
                db_handler.update_description(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client)
                        .await
                        .unwrap()
                        .unwrap()
                        .description,
                    *"collection_description"
                );
            }
            ObjectType::DATASET => {
                let request = DescriptionUpdate::Dataset(UpdateDatasetDescriptionRequest {
                    dataset_id: r.id.to_string(),
                    description: "dataset_description".to_string(),
                });
                db_handler.update_description(request).await.unwrap();
                assert_eq!(
                    Object::get(r.id, &client)
                        .await
                        .unwrap()
                        .unwrap()
                        .description,
                    *"dataset_description"
                );
            }
            _ => panic!(),
        };
    }
}
#[tokio::test]
async fn test_update_keyvals() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;
    let resources = vec![
        ObjectMapping::PROJECT(DieselUlid::generate()),
        ObjectMapping::COLLECTION(DieselUlid::generate()),
        ObjectMapping::DATASET(DieselUlid::generate()),
    ];
    let mut user = test_utils::new_user(resources.clone());
    let mut objects = Vec::new();
    let kv = KeyValue {
        key: "DELETE".to_string(),
        value: "This key will be deleted".to_string(),
        variant: KeyValueVariant::LABEL,
    };
    for r in resources {
        let mut o = test_utils::object_from_mapping(user.id, r);
        o.key_values = Json(KeyValues(vec![kv.clone()]));
        objects.push(o);
    }
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();
    Object::batch_create(&objects, &client).await.unwrap();

    // Test
    assert!(
        Object::get_objects(&objects.iter().map(|o| o.id).collect(), &client)
            .await
            .unwrap()
            .into_iter()
            .all(|o| o.key_values.0 .0.contains(&kv))
    );
    let valid = APIKeyValue {
        key: "ADDED".to_string(),
        value: "This label gets added".to_string(),
        variant: 1,
    };
    let valid_converted = KeyValue {
        key: "ADDED".to_string(),
        value: "This label gets added".to_string(),
        variant: KeyValueVariant::LABEL,
    };
    let static_kv = APIKeyValue {
        key: "ADDED".to_string(),
        value: "This label gets added".to_string(),
        variant: 2,
    };
    let static_converted = KeyValue {
        key: "ADDED".to_string(),
        value: "This label gets added".to_string(),
        variant: KeyValueVariant::STATIC_LABEL,
    };
    let deleted = APIKeyValue {
        key: "DELETE".to_string(),
        value: "This key will be deleted".to_string(),
        variant: 1,
    };
    for r in objects {
        match r.object_type {
            ObjectType::PROJECT => {
                let request = KeyValueUpdate::Project(UpdateProjectKeyValuesRequest {
                    project_id: r.id.to_string(),
                    add_key_values: vec![valid.clone(), static_kv.clone()],
                    remove_key_values: vec![deleted.clone()],
                });
                db_handler.update_keyvals(request).await.unwrap();
                assert!(Object::get(r.id, &client)
                    .await
                    .unwrap()
                    .unwrap()
                    .key_values
                    .0
                     .0
                    .contains(&valid_converted),);
                assert!(Object::get(r.id, &client)
                    .await
                    .unwrap()
                    .unwrap()
                    .key_values
                    .0
                     .0
                    .contains(&static_converted),);
                let err = KeyValueUpdate::Project(UpdateProjectKeyValuesRequest {
                    project_id: r.id.to_string(),
                    add_key_values: vec![],
                    remove_key_values: vec![static_kv.clone()],
                });
                assert!(db_handler.update_keyvals(err).await.is_err());
            }
            ObjectType::COLLECTION => {
                let request = KeyValueUpdate::Collection(UpdateCollectionKeyValuesRequest {
                    collection_id: r.id.to_string(),
                    add_key_values: vec![valid.clone(), static_kv.clone()],
                    remove_key_values: vec![deleted.clone()],
                });
                db_handler.update_keyvals(request).await.unwrap();
                assert!(Object::get(r.id, &client)
                    .await
                    .unwrap()
                    .unwrap()
                    .key_values
                    .0
                     .0
                    .contains(&valid_converted),);
                assert!(Object::get(r.id, &client)
                    .await
                    .unwrap()
                    .unwrap()
                    .key_values
                    .0
                     .0
                    .contains(&static_converted),);
                let err = KeyValueUpdate::Collection(UpdateCollectionKeyValuesRequest {
                    collection_id: r.id.to_string(),
                    add_key_values: vec![],
                    remove_key_values: vec![static_kv.clone()],
                });
                assert!(db_handler.update_keyvals(err).await.is_err());
            }
            ObjectType::DATASET => {
                let request = KeyValueUpdate::Dataset(UpdateDatasetKeyValuesRequest {
                    dataset_id: r.id.to_string(),
                    add_key_values: vec![valid.clone(), static_kv.clone()],
                    remove_key_values: vec![deleted.clone()],
                });
                db_handler.update_keyvals(request).await.unwrap();
                assert!(Object::get(r.id, &client)
                    .await
                    .unwrap()
                    .unwrap()
                    .key_values
                    .0
                     .0
                    .contains(&valid_converted),);
                assert!(Object::get(r.id, &client)
                    .await
                    .unwrap()
                    .unwrap()
                    .key_values
                    .0
                     .0
                    .contains(&static_converted),);
                let err = KeyValueUpdate::Dataset(UpdateDatasetKeyValuesRequest {
                    dataset_id: r.id.to_string(),
                    add_key_values: vec![],
                    remove_key_values: vec![static_kv.clone()],
                });
                assert!(db_handler.update_keyvals(err).await.is_err());
            }
            _ => panic!(),
        };
    }
}

#[tokio::test]
async fn update_object_test() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;
    let object_id = DieselUlid::generate();
    let object_mapping = ObjectMapping::OBJECT(object_id);
    let parent_id = DieselUlid::generate();
    let parent_mapping = ObjectMapping::PROJECT(parent_id);
    let mut user = test_utils::new_user(vec![object_mapping]);
    let mut object = test_utils::object_from_mapping(user.id, object_mapping);
    let mut parent = test_utils::object_from_mapping(user.id, parent_mapping);
    let mut relation = test_utils::new_internal_relation(&parent, &object);
    object.key_values.0 .0.push(KeyValue {
        key: "to_delete".to_string(),
        value: "deleted".to_string(),
        variant: KeyValueVariant::LABEL,
    });
    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();
    // Cache must be updated too
    object.create(&client).await.unwrap();
    parent.create(&client).await.unwrap();
    relation.create(&client).await.unwrap();
    // cache update
    let updates = Object::get_objects_with_relations(&vec![object_id, parent_id], &client)
        .await
        .unwrap();
    for o in updates {
        db_handler.cache.add_object(o)
    }

    // test
    let update_request = UpdateObjectRequest {
        object_id: object_id.to_string(),
        name: None,
        description: Some("A new description".to_string()),
        add_key_values: vec![APIKeyValue {
            key: "New".to_string(),
            value: "value".to_string(),
            variant: 1,
        }],
        remove_key_values: vec![],
        data_class: 1,
        hashes: vec![],
        parent: None,
        force_revision: false,
        data_license_tag: None,
        metadata_license_tag: None,
    };

    // Test in place update
    let (updated, is_new) = db_handler
        .update_grpc_object(update_request, user.id, false)
        .await
        .unwrap();
    assert!(!is_new);
    assert_eq!(updated.object.id, object_id);
    assert_eq!(updated.object.description, "A new description".to_string());
    assert!(updated.object.key_values.0 .0.contains(&KeyValue {
        key: "New".to_string(),
        value: "value".to_string(),
        variant: KeyValueVariant::LABEL,
    }));
    assert_eq!(updated.object.data_class, DataClass::PUBLIC);
    assert!(updated.inbound_belongs_to.0.contains_key(&parent_id));
    let trigger_new_request = UpdateObjectRequest {
        object_id: object_id.to_string(),
        name: Some("new_name".to_string()),
        description: None,
        add_key_values: vec![],
        remove_key_values: vec![APIKeyValue {
            key: "to_delete".to_string(),
            value: "deleted".to_string(),
            variant: 1,
        }],
        data_class: 1,
        hashes: vec![APIHash {
            alg: 1,
            hash: "dd98d701915b2bc5aad5dc9190194844".to_string(),
        }],
        parent: None,
        force_revision: false,
        data_license_tag: None,
        metadata_license_tag: None,
    };

    // test new revision update
    let (new, is_new) = db_handler
        .update_grpc_object(trigger_new_request, user.id, false)
        .await
        .unwrap();
    assert!(is_new);
    assert_ne!(new.object.id, object_id);
    assert_eq!(new.object.name, "new_name".to_string());
    assert!(!new.object.key_values.0 .0.iter().contains(&KeyValue {
        key: "to_delete".to_string(),
        value: "deleted".to_string(),
        variant: KeyValueVariant::LABEL,
    }));
    assert!(!new.object.hashes.0 .0.is_empty());

    // test force revision updates
    let force_new_revision = UpdateObjectRequest {
        object_id: new.object.id.to_string(),
        name: None,
        description: None,
        add_key_values: vec![],
        remove_key_values: vec![],
        data_class: 0,
        hashes: vec![],
        parent: None,
        force_revision: true,
        metadata_license_tag: None,
        data_license_tag: None,
    };

    let (new_2, is_new_2) = db_handler
        .update_grpc_object(force_new_revision, user.id, false)
        .await
        .unwrap();
    assert!(is_new_2);
    assert_eq!(new_2.object.revision_number, new.object.revision_number + 1);
    assert_eq!(new_2.object.object_status, ObjectStatus::INITIALIZING);

    // test license update
    let license_update = UpdateObjectRequest {
        object_id: new.object.id.to_string(),
        name: None,
        description: None,
        add_key_values: vec![],
        remove_key_values: vec![],
        data_class: 0,
        hashes: vec![],
        parent: None,
        force_revision: true,
        metadata_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
        data_license_tag: Some(ALL_RIGHTS_RESERVED.to_string()),
    };
    let (license_updated, is_new) = db_handler
        .update_grpc_object(license_update.clone(), user.id, false)
        .await
        .unwrap();
    assert!(is_new);
    assert_eq!(
        license_update.metadata_license_tag,
        Some(license_updated.object.metadata_license)
    );
    assert_eq!(
        license_update.data_license_tag,
        Some(license_updated.object.data_license)
    )
}

#[tokio::test]
async fn update_initializing_object_in_place_test() {
    let db_handler = init_database_handler_middlelayer().await;
    let object_id = DieselUlid::generate();
    let object_mapping = ObjectMapping::OBJECT(object_id);
    let old_parent_id = DieselUlid::generate();
    let old_parent_mapping = ObjectMapping::PROJECT(old_parent_id);
    let new_parent_id = DieselUlid::generate();
    let new_parent_mapping = ObjectMapping::PROJECT(new_parent_id);
    let mut user =
        test_utils::new_user(vec![object_mapping, old_parent_mapping, new_parent_mapping]);
    let mut object = test_utils::object_from_mapping(user.id, object_mapping);
    let mut old_parent = test_utils::object_from_mapping(user.id, old_parent_mapping);
    let mut new_parent = test_utils::object_from_mapping(user.id, new_parent_mapping);
    let mut relation = test_utils::new_internal_relation(&old_parent, &object);
    let hash_value = "dd98d701915b2bc5aad5dc9190194844".to_string();
    let metadata_license_tag = format!("stage-meta-{}", DieselUlid::generate());
    let data_license_tag = format!("stage-data-{}", DieselUlid::generate());
    let mut metadata_license = License {
        tag: metadata_license_tag.clone(),
        name: "Staging metadata license".to_string(),
        text: "Staging metadata license".to_string(),
        url: "https://example.com/staging-metadata".to_string(),
    };
    let mut data_license = License {
        tag: data_license_tag.clone(),
        name: "Staging data license".to_string(),
        text: "Staging data license".to_string(),
        url: "https://example.com/staging-data".to_string(),
    };

    object.object_status = ObjectStatus::INITIALIZING;
    object.revision_number = 7;
    object.key_values.0 .0.push(KeyValue {
        key: "to_delete".to_string(),
        value: "deleted".to_string(),
        variant: KeyValueVariant::LABEL,
    });
    old_parent.name = "old-parent".to_string();
    new_parent.name = "new-parent".to_string();

    let client = db_handler.database.get_client().await.unwrap();
    user.create(&client).await.unwrap();
    metadata_license.create(&client).await.unwrap();
    data_license.create(&client).await.unwrap();
    object.create(&client).await.unwrap();
    old_parent.create(&client).await.unwrap();
    new_parent.create(&client).await.unwrap();
    relation.create(&client).await.unwrap();

    let updates =
        Object::get_objects_with_relations(&vec![object_id, old_parent_id, new_parent_id], &client)
            .await
            .unwrap();
    for o in updates {
        db_handler.cache.add_object(o)
    }

    let request = UpdateObjectRequest {
        object_id: object_id.to_string(),
        name: Some("staging-name".to_string()),
        description: Some("staging-description".to_string()),
        add_key_values: vec![APIKeyValue {
            key: "New".to_string(),
            value: "value".to_string(),
            variant: 1,
        }],
        remove_key_values: vec![APIKeyValue {
            key: "to_delete".to_string(),
            value: "deleted".to_string(),
            variant: 1,
        }],
        data_class: 1,
        hashes: vec![APIHash {
            alg: 1,
            hash: hash_value.clone(),
        }],
        parent: Some(Parent::ProjectId(new_parent_id.to_string())),
        force_revision: true,
        metadata_license_tag: Some(metadata_license_tag.clone()),
        data_license_tag: Some(data_license_tag.clone()),
    };

    let (updated, is_new) = db_handler
        .update_grpc_object(request, user.id, false)
        .await
        .unwrap();

    assert!(!is_new);
    assert_eq!(updated.object.id, object_id);
    assert_eq!(updated.object.revision_number, 7);
    assert_eq!(updated.object.object_status, ObjectStatus::INITIALIZING);
    assert_eq!(updated.object.name, "staging-name".to_string());
    assert_eq!(
        updated.object.description,
        "staging-description".to_string()
    );
    assert_eq!(updated.object.data_class, DataClass::PUBLIC);
    assert_eq!(updated.object.metadata_license, metadata_license_tag);
    assert_eq!(updated.object.data_license, data_license_tag);
    assert!(updated.object.key_values.0 .0.contains(&KeyValue {
        key: "New".to_string(),
        value: "value".to_string(),
        variant: KeyValueVariant::LABEL,
    }));
    assert!(!updated.object.key_values.0 .0.contains(&KeyValue {
        key: "to_delete".to_string(),
        value: "deleted".to_string(),
        variant: KeyValueVariant::LABEL,
    }));
    assert_eq!(updated.object.hashes.0 .0.len(), 1);
    assert!(updated
        .object
        .hashes
        .0
         .0
        .iter()
        .any(|hash| hash.hash == hash_value));
    assert!(!updated.inbound_belongs_to.0.contains_key(&old_parent_id));
    assert!(updated.inbound_belongs_to.0.contains_key(&new_parent_id));

    let rename_request = UpdateObjectRequest {
        object_id: object_id.to_string(),
        name: Some("staging-name-2".to_string()),
        description: None,
        add_key_values: vec![],
        remove_key_values: vec![],
        data_class: 0,
        hashes: vec![],
        parent: Some(Parent::ProjectId(new_parent_id.to_string())),
        force_revision: true,
        metadata_license_tag: None,
        data_license_tag: None,
    };

    let (renamed, renamed_is_new) = db_handler
        .update_grpc_object(rename_request, user.id, false)
        .await
        .unwrap();

    assert!(!renamed_is_new);
    assert_eq!(renamed.object.id, object_id);
    assert_eq!(renamed.object.name, "staging-name-2".to_string());
    assert_eq!(
        renamed
            .inbound_belongs_to
            .0
            .get(&new_parent_id)
            .unwrap()
            .target_name,
        "staging-name-2".to_string()
    );
}
