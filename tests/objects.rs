use aruna_server::database::{
    crud::CrudDb,
    dsls::{
        internal_relation_dsl::InternalRelation,
        object_dsl::{ExternalRelations, Hashes, KeyValues, Object, ObjectWithRelations},
        relation_type_dsl::RelationType,
        user_dsl::{User, UserAttributes},
    },
};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::collections::HashMap;

mod init_db;

#[tokio::test]
async fn create_object() {
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    let obj_id = DieselUlid::generate();

    let attributes = Json(UserAttributes {
        global_admin: false,
        service_account: false,
        custom_attributes: Vec::new(),
        tokens: HashMap::new(),
        permissions: HashMap::from([(
            obj_id,
            aruna_server::database::enums::DbPermissionLevel::WRITE,
        )]),
    });

    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes,
        active: true,
    };

    user.create(&client).await.unwrap();

    let create_object = Object {
        id: obj_id,
        revision_number: 0,
        name: "a".to_string(),
        description: "b".to_string(),
        count: 1,
        created_at: None,
        content_len: 1337,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::CONFIDENTIAL,
        object_type: aruna_server::database::enums::ObjectType::OBJECT,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: Json(Hashes(Vec::new())),
        dynamic: false,
        endpoints: Json(HashMap::new()),
    };
    create_object.create(&client).await.unwrap();
    let get_obj = Object::get(obj_id, &client).await.unwrap().unwrap();
    assert!(get_obj == create_object);
}
#[tokio::test]
async fn get_object_with_relations_test() {
    let db = crate::init_db::init_db().await;
    let mut client = db.get_client().await.unwrap();
    let transaction = client
        .transaction()
        .await
        .map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unavailable("Database not avaliable.")
        })
        .unwrap();

    let client = transaction.client();

    let dataset_id = DieselUlid::generate();
    let collection_one = DieselUlid::generate();
    let collection_two = DieselUlid::generate();
    let object_one = DieselUlid::generate();
    let object_two = DieselUlid::generate();
    let object_vec = [
        dataset_id,
        collection_one,
        collection_two,
        object_one,
        object_two,
    ];

    let attributes = Json(UserAttributes {
        global_admin: false,
        service_account: false,
        custom_attributes: Vec::new(),
        tokens: HashMap::new(),
        permissions: object_vec
            .iter()
            .map(|o| (*o, aruna_server::database::enums::DbPermissionLevel::WRITE))
            .collect(),
    });

    let user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        external_id: None,
        email: "aja".to_string(),
        attributes,
        active: true,
    };

    user.create(client).await.unwrap();

    let insert = "INSERT INTO relation_types (relation_name) VALUES ($1);";
    let prepared = client.prepare(insert).await.unwrap();
    let rel_type = RelationType {
        relation_name: "BELONGS_TO".to_string(),
    };

    client
        .execute(&prepared, &[&rel_type.relation_name])
        .await
        .unwrap();

    let create_dataset = Object {
        id: dataset_id,
        revision_number: 0,
        name: "dataset".to_string(),
        description: "test".to_string(),
        count: 2,
        created_at: None,
        content_len: 0,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::PUBLIC,
        object_type: aruna_server::database::enums::ObjectType::DATASET,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: Json(Hashes(Vec::new())),
        dynamic: true,
        endpoints: Json(HashMap::new()),
    };
    let create_collection_one = Object {
        id: collection_one,
        revision_number: 0,
        name: "collection_one".to_string(),
        description: "test".to_string(),
        count: 3,
        created_at: None,
        content_len: 0,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::PUBLIC,
        object_type: aruna_server::database::enums::ObjectType::COLLECTION,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: Json(Hashes(Vec::new())),
        dynamic: true,
        endpoints: Json(HashMap::new()),
    };
    let create_collection_two = Object {
        id: collection_two,
        revision_number: 0,
        name: "collection_two".to_string(),
        description: "test".to_string(),
        count: 3,
        created_at: None,
        content_len: 0,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::PUBLIC,
        object_type: aruna_server::database::enums::ObjectType::COLLECTION,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: Json(Hashes(Vec::new())),
        dynamic: true,
        endpoints: Json(HashMap::new()),
    };
    let create_object_one = Object {
        id: object_one,
        revision_number: 0,
        name: "object_one".to_string(),
        description: "test".to_string(),
        count: 1,
        created_at: None,
        content_len: 0,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::PUBLIC,
        object_type: aruna_server::database::enums::ObjectType::OBJECT,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: Json(Hashes(Vec::new())),
        dynamic: false,
        endpoints: Json(HashMap::new()),
    };
    let create_object_two = Object {
        id: object_two,
        revision_number: 0,
        name: "object_two".to_string(),
        description: "test".to_string(),
        count: 1,
        created_at: None,
        content_len: 0,
        created_by: user.id,
        key_values: Json(KeyValues(vec![])),
        object_status: aruna_server::database::enums::ObjectStatus::AVAILABLE,
        data_class: aruna_server::database::enums::DataClass::PUBLIC,
        object_type: aruna_server::database::enums::ObjectType::OBJECT,
        external_relations: Json(ExternalRelations(vec![])),
        hashes: Json(Hashes(Vec::new())),
        dynamic: false,
        endpoints: Json(HashMap::new()),
    };
    let creates = vec![
        create_dataset.clone(),
        create_object_one,
        create_object_two,
        create_collection_one,
        create_collection_two,
    ];

    for c in creates {
        c.create(client).await.unwrap();
    }
    let create_relation_one = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: collection_one,
        origin_type: aruna_server::database::enums::ObjectType::COLLECTION,
        target_pid: dataset_id,
        target_type: aruna_server::database::enums::ObjectType::DATASET,
        is_persistent: true,
        relation_name: "BELONGS_TO".to_string(),
    };
    let create_relation_two = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: collection_two,
        origin_type: aruna_server::database::enums::ObjectType::COLLECTION,
        target_pid: dataset_id,
        target_type: aruna_server::database::enums::ObjectType::DATASET,
        is_persistent: true,
        relation_name: "BELONGS_TO".to_string(),
    };
    let create_relation_three = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: dataset_id,
        origin_type: aruna_server::database::enums::ObjectType::DATASET,
        target_pid: object_one,
        target_type: aruna_server::database::enums::ObjectType::OBJECT,
        is_persistent: true,
        relation_name: "BELONGS_TO".to_string(),
    };
    let create_relation_four = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: dataset_id,
        origin_type: aruna_server::database::enums::ObjectType::DATASET,
        target_pid: object_two,
        target_type: aruna_server::database::enums::ObjectType::OBJECT,
        is_persistent: true,
        relation_name: "BELONGS_TO".to_string(),
    };
    let rels = vec![
        create_relation_one.clone(),
        create_relation_two.clone(),
        create_relation_three.clone(),
        create_relation_four.clone(),
    ];
    for r in rels {
        dbg!(&r);
        r.create(client).await.unwrap();
    }
    // let compare_owr = ObjectWithRelations {
    //     object: create_dataset,
    //     inbound: Json(Inbound(vec![create_relation_one, create_relation_two])),
    //     outbound: Json(Outbound(vec![
    //         create_relation_three,
    //         create_relation_four.clone(),
    //     ])),
    // };
    // let object_with_relations = Object::get_object_with_relations(&dataset_id, client)
    //     .await
    //     .unwrap();

    // dbg!(&object_with_relations);
    // dbg!(&compare_owr);
    // assert!(object_with_relations == compare_owr);
}
