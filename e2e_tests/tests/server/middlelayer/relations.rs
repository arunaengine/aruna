use crate::common::test_utils::USER1_ULID;
use crate::common::{init, init_server, test_utils};
use aruna_rust_api::api::storage::models::v2::relation::Relation as RelationEnum;
use aruna_rust_api::api::storage::models::v2::InternalRelation as APIInternalRelation;
use aruna_rust_api::api::storage::models::v2::Relation;
use aruna_rust_api::api::storage::models::v2::{
    ExternalRelation as APIExternalRelation, InternalRelationVariant, RelationDirection,
    ResourceVariant,
};
use aruna_rust_api::api::storage::services::v2::ModifyRelationsRequest;
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO, INTERNAL_RELATION_VARIANT_METADATA,
    INTERNAL_RELATION_VARIANT_VERSION,
};
use aruna_server::database::dsls::object_dsl::ObjectWithRelations;
use aruna_server::database::dsls::object_dsl::{DefinedVariant, ExternalRelation, Object};
use aruna_server::database::enums::ObjectType;
use aruna_server::middlelayer::relations_request_types::ModifyRelations;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use postgres_types::Json;
use std::str::FromStr;

#[tokio::test]
async fn test_modify() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();
    let origin = DieselUlid::generate();
    let target = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let objects = vec![
        test_utils::new_object(user_id, origin, ObjectType::DATASET),
        test_utils::new_object(user_id, target, ObjectType::OBJECT),
    ];
    Object::batch_create(&objects, &client).await.unwrap();
    let belongs_to = test_utils::new_internal_relation(&objects[0], &objects[1]);
    let other = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: origin,
        origin_type: ObjectType::DATASET,
        relation_name: INTERNAL_RELATION_VARIANT_METADATA.to_string(),
        target_pid: target,
        target_type: ObjectType::OBJECT,
        target_name: objects[1].name.to_string(),
    };
    InternalRelation::batch_create(&vec![belongs_to, other], &client)
        .await
        .unwrap();

    // test
    let rel_mod_one = Relation {
        relation: Some(RelationEnum::External(APIExternalRelation {
            identifier: "test.test".to_string(),
            defined_variant: 1, // URL
            custom_variant: None,
        })),
    };
    let rel_mod_two = Relation {
        relation: Some(RelationEnum::Internal(APIInternalRelation {
            resource_id: target.to_string(),
            defined_variant: 5, // POLICY
            custom_variant: None,
            resource_variant: 4,
            direction: RelationDirection::Outbound as i32,
        })),
    };
    let rel_del_one = Relation {
        relation: Some(RelationEnum::Internal(APIInternalRelation {
            resource_id: target.to_string(),
            defined_variant: InternalRelationVariant::Metadata as i32, // BELONGS_TO
            custom_variant: None,
            resource_variant: 4,
            direction: RelationDirection::Outbound as i32,
        })),
    };
    let request = ModifyRelations(ModifyRelationsRequest {
        resource_id: origin.to_string(),
        add_relations: vec![rel_mod_one, rel_mod_two],
        remove_relations: vec![rel_del_one],
    });

    for obj in objects {
        db_handler.cache.add_object(ObjectWithRelations {
            object: obj.clone(),
            inbound: Json(DashMap::default()),
            inbound_belongs_to: Json(DashMap::default()),
            outbound: Json(DashMap::default()),
            outbound_belongs_to: Json(DashMap::default()),
        });
    }

    let (obj, mod_lab) = db_handler.get_resource(request).await.unwrap();
    let owr = db_handler
        .modify_relations(obj, mod_lab.relations_to_add, mod_lab.relations_to_remove)
        .await
        .unwrap();
    assert!(owr.inbound.0.is_empty());
    assert!(owr.inbound_belongs_to.0.is_empty());
    assert_eq!(owr.outbound.0.len(), 1);
    assert_eq!(owr.outbound_belongs_to.0.len(), 1);
    assert!(owr
        .object
        .external_relations
        .0
         .0
        .into_iter()
        .map(|i| i.1)
        .contains(&ExternalRelation {
            identifier: "test.test".to_string(),
            defined_variant: DefinedVariant::URL,
            custom_variant: None,
        }))
}
#[tokio::test]
async fn test_modify_relations_constraint() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();

    // create user and objects
    let project_id = DieselUlid::generate();
    let legacy_project_id = DieselUlid::generate();
    let collection_id = DieselUlid::generate();
    let object_id = DieselUlid::generate();
    let old_object_id = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let mut project = test_utils::new_object(user_id, project_id, ObjectType::PROJECT);
    let mut legacy_project =
        test_utils::new_object(user_id, legacy_project_id, ObjectType::PROJECT);
    let mut collection = test_utils::new_object(user_id, collection_id, ObjectType::COLLECTION);
    let mut object = test_utils::new_object(user_id, object_id, ObjectType::OBJECT);
    let mut old_object = test_utils::new_object(user_id, old_object_id, ObjectType::OBJECT);
    project.create(&client).await.unwrap();
    legacy_project.create(&client).await.unwrap();
    collection.create(&client).await.unwrap();
    object.create(&client).await.unwrap();
    old_object.create(&client).await.unwrap();

    // create relations
    let proj_col = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: project_id,
        origin_type: ObjectType::PROJECT,
        relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
        target_pid: collection_id,
        target_type: ObjectType::COLLECTION,
        target_name: collection.name.clone(),
    };
    let col_obj = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: collection_id,
        origin_type: ObjectType::COLLECTION,
        relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
        target_pid: object_id,
        target_type: ObjectType::OBJECT,
        target_name: object.name.clone(),
    };
    let version = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: old_object_id,
        origin_type: ObjectType::OBJECT,
        relation_name: INTERNAL_RELATION_VARIANT_VERSION.to_string(),
        target_pid: object_id,
        target_type: ObjectType::OBJECT,
        target_name: object.name.clone(),
    };
    let legacy_relation = InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: legacy_project_id,
        origin_type: ObjectType::PROJECT,
        relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
        target_pid: old_object_id,
        target_type: ObjectType::OBJECT,
        target_name: old_object.name.clone(),
    };
    InternalRelation::batch_create(
        &vec![
            proj_col.clone(),
            col_obj.clone(),
            version.clone(),
            legacy_relation.clone(),
        ],
        &client,
    )
    .await
    .unwrap();
    let project_wr = ObjectWithRelations {
        object: project.clone(),
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::default()),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::from_iter([(
            proj_col.target_pid,
            proj_col.clone(),
        )])),
    };
    let collection_wr = ObjectWithRelations {
        object: collection.clone(),
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::from_iter([(
            proj_col.origin_pid,
            proj_col.clone(),
        )])),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::from_iter([(col_obj.target_pid, col_obj.clone())])),
    };
    let object_wr = ObjectWithRelations {
        object: object.clone(),
        inbound: Json(DashMap::from_iter([(version.id, version.clone())])),
        inbound_belongs_to: Json(DashMap::from_iter([(col_obj.origin_pid, col_obj.clone())])),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::default()),
    };
    let legacy_project_wr = ObjectWithRelations {
        object: legacy_project.clone(),
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::default()),
        outbound: Json(DashMap::default()),
        outbound_belongs_to: Json(DashMap::from_iter([(
            legacy_relation.target_pid,
            legacy_relation.clone(),
        )])),
    };
    let old_wr = ObjectWithRelations {
        object: old_object.clone(),
        inbound: Json(DashMap::default()),
        inbound_belongs_to: Json(DashMap::default()),
        outbound: Json(DashMap::from_iter([(version.id, version.clone())])),
        outbound_belongs_to: Json(DashMap::default()),
    };
    for owr in [
        project_wr,
        collection_wr,
        object_wr,
        legacy_project_wr,
        old_wr,
    ] {
        db_handler.cache.upsert_object(&owr.object.id, owr.clone());
    }

    // test constraints

    // 1. collection relation to project cannot be removed
    let invalid = Relation {
        relation: Some(RelationEnum::Internal(APIInternalRelation {
            resource_id: collection_id.to_string(),
            defined_variant: InternalRelationVariant::BelongsTo as i32,
            custom_variant: None,
            resource_variant: ResourceVariant::Collection as i32,
            direction: RelationDirection::Outbound as i32,
        })),
    };
    let request = ModifyRelations(ModifyRelationsRequest {
        resource_id: project_id.to_string(),
        add_relations: vec![],
        remove_relations: vec![invalid],
    });

    let (obj, mod_lab) = db_handler.get_resource(request).await.unwrap();
    assert!(db_handler
        .modify_relations(obj, mod_lab.relations_to_add, mod_lab.relations_to_remove)
        .await
        .is_err());

    // 2. new object despite having a version cannot be removed, because version is ingoing
    let invalid = Relation {
        relation: Some(RelationEnum::Internal(APIInternalRelation {
            resource_id: object.id.to_string(),
            defined_variant: InternalRelationVariant::BelongsTo as i32,
            custom_variant: None,
            resource_variant: ResourceVariant::Object as i32,
            direction: RelationDirection::Outbound as i32,
        })),
    };
    let request = ModifyRelations(ModifyRelationsRequest {
        resource_id: collection.id.to_string(),
        add_relations: vec![],
        remove_relations: vec![invalid],
    });

    let (obj, mod_lab) = db_handler.get_resource(request).await.unwrap();
    assert!(db_handler
        .modify_relations(obj, mod_lab.relations_to_add, mod_lab.relations_to_remove)
        .await
        .is_err());

    // 3. removing legacy_project -> old_object relation should work, because there are valid version relations
    let valid = Relation {
        relation: Some(RelationEnum::Internal(APIInternalRelation {
            resource_id: old_object_id.to_string(),
            defined_variant: InternalRelationVariant::BelongsTo as i32,
            custom_variant: None,
            resource_variant: ResourceVariant::Object as i32,
            direction: RelationDirection::Outbound as i32,
        })),
    };
    let request = ModifyRelations(ModifyRelationsRequest {
        resource_id: legacy_project_id.to_string(),
        add_relations: vec![],
        remove_relations: vec![valid],
    });

    let (obj, mod_lab) = db_handler.get_resource(request).await.unwrap();
    assert!(db_handler
        .modify_relations(obj, mod_lab.relations_to_add, mod_lab.relations_to_remove)
        .await
        .is_ok());
    assert!(db_handler
        .cache
        .get_object(&legacy_project_id)
        .unwrap()
        .outbound_belongs_to
        .0
        .is_empty());
    assert!(
        Object::get_object_with_relations(&legacy_project_id, &client)
            .await
            .unwrap()
            .outbound_belongs_to
            .0
            .is_empty()
    );
}
