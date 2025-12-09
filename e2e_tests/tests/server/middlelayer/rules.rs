use crate::common::test_utils::{rand_string, USER1_ULID};
use crate::common::{init, init_server};
use aruna_rust_api::api::storage::models::v2::DataClass;
use aruna_rust_api::api::storage::services::v2::create_object_request::Parent;
use aruna_rust_api::api::storage::services::v2::{
    create_collection_request, CreateCollectionRequest, CreateObjectRequest, CreateProjectRequest,
    CreateRuleBindingRequest, CreateRuleRequest, DeleteRuleBindingRequest,
    UpdateCollectionDescriptionRequest, UpdateRuleRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::license_dsl::ALL_RIGHTS_RESERVED;
use aruna_server::database::dsls::rule_dsl::{Rule, RuleBinding};
use aruna_server::middlelayer::create_request_types::CreateRequest;
use aruna_server::middlelayer::rule_request_types::{
    CreateRule, CreateRuleBinding, DeleteRuleBinding, UpdateRule,
};
use aruna_server::middlelayer::update_request_types::DescriptionUpdate;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use std::str::FromStr;

#[tokio::test]
async fn create_rule() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;

    // create rule
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let request = CreateRuleRequest {
        rule: "1 == 1".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler
        .create_rule(CreateRule(request.clone()), user_id)
        .await
        .unwrap();

    // Get rule
    let client = db_handler.database.get_client().await.unwrap();
    let rule = Rule::get(rule_id, &client).await.unwrap().unwrap();
    assert_eq!(&request.rule, &rule.rule_expressions);
    assert_eq!(&request.description, &rule.description);
    assert_eq!(&request.public, &rule.is_public);
    assert_eq!(user_id, rule.owner_id);
}

#[tokio::test]
async fn update_rule() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;

    // create rule
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let request = CreateRuleRequest {
        rule: "1 == 1".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler
        .create_rule(CreateRule(request.clone()), user_id)
        .await
        .unwrap();
    let cached_rule = db_handler.cache.get_rule(&rule_id).unwrap();

    // Update rule
    let request = UpdateRuleRequest {
        id: rule_id.to_string(),
        rule: "false".to_string(),
        description: "new".to_string(),
        public: false,
    };
    let updated = db_handler
        .update_rule(UpdateRule(request.clone()), cached_rule)
        .await
        .unwrap();

    // Get rule
    assert_eq!(&request.rule, &updated.rule.rule_expressions);
    assert_eq!(&request.description, &updated.rule.description);
    assert_eq!(&request.public, &updated.rule.is_public);
    assert_eq!(user_id, updated.rule.owner_id);
}

#[tokio::test]
async fn delete_rule() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;

    // create rule
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let request = CreateRuleRequest {
        rule: "false".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler
        .create_rule(CreateRule(request.clone()), user_id)
        .await
        .unwrap();
    let cached_rule = db_handler.cache.get_rule(&rule_id).unwrap();

    // Delete rule
    db_handler.delete_rule(&cached_rule).await.unwrap();

    // Get rule
    assert!(db_handler.cache.get_rule(&rule_id).is_none());
    let client = db_handler.database.get_client().await.unwrap();
    assert!(Rule::get(rule_id, &client).await.unwrap().is_none());
}
#[tokio::test]
async fn create_and_delete_rule_binding() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;

    // create rule
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let request = CreateRuleRequest {
        rule: "false".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler
        .create_rule(CreateRule(request.clone()), user_id)
        .await
        .unwrap();
    let _cached_rule = db_handler.cache.get_rule(&rule_id).unwrap();

    // create object
    let default_endpoint = DieselUlid::generate();
    let project_name = rand_string(30).to_lowercase();
    let request = CreateRequest::Project(
        CreateProjectRequest {
            name: project_name,
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
    let (project, _) = db_handler
        .create_resource(request, user_id, false)
        .await
        .unwrap();

    // create rule binding
    let request = CreateRuleBinding(CreateRuleBindingRequest {
        rule_id: rule_id.to_string(),
        object_id: project.object.id.to_string(),
        cascading: true,
    });
    db_handler.create_rule_binding(request).await.unwrap();

    // Check binding
    assert!(db_handler
        .cache
        .get_rule_bindings(&project.object.id)
        .unwrap()
        .contains(&RuleBinding {
            rule_id,
            origin_id: project.object.id,
            object_id: project.object.id,
            cascading: true,
        }));
    let client = db_handler.database.get_client().await.unwrap();
    assert!(RuleBinding::all(&client)
        .await
        .unwrap()
        .contains(&RuleBinding {
            rule_id,
            origin_id: project.object.id,
            object_id: project.object.id,
            cascading: true,
        }));

    // Delete binding
    let request = DeleteRuleBinding(DeleteRuleBindingRequest {
        rule_id: rule_id.to_string(),
        object_id: project.object.id.to_string(),
    });
    db_handler.delete_rule_binding(request).await.unwrap();

    // Check deletion
    assert!(db_handler
        .cache
        .get_rule_bindings(&project.object.id)
        .is_none());
    assert!(!RuleBinding::all(&client)
        .await
        .unwrap()
        .contains(&RuleBinding {
            rule_id,
            origin_id: project.object.id,
            object_id: project.object.id,
            cascading: true,
        }));
}

#[tokio::test]
async fn evaluate_and_update_rules() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;

    // create rule
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let request = CreateRuleRequest {
        rule: "object.object.title.startsWith('a valid title')".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler
        .create_rule(CreateRule(request.clone()), user_id)
        .await
        .unwrap();
    let _cached_rule = db_handler.cache.get_rule(&rule_id).unwrap();

    // create object
    let default_endpoint = DieselUlid::generate();
    let project_name = rand_string(30).to_lowercase();
    let request = CreateRequest::Project(
        CreateProjectRequest {
            name: project_name,
            title: "a valid title for a project".to_string(),
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
    let (project, _) = db_handler
        .create_resource(request, user_id, false)
        .await
        .unwrap();

    // create rule binding
    let request = CreateRuleBinding(CreateRuleBindingRequest {
        rule_id: rule_id.to_string(),
        object_id: project.object.id.to_string(),
        cascading: true,
    });
    db_handler.create_rule_binding(request).await.unwrap();

    // create invalid subresource
    let object_name = rand_string(10);
    let request = CreateRequest::Object(CreateObjectRequest {
        name: object_name,
        title: "invalid title for an object".to_string(),
        description: "abc".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: 1,
        hashes: vec![],
        metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        authors: vec![],
        parent: Some(Parent::ProjectId(project.object.id.to_string())),
    });
    let response = db_handler.create_resource(request, user_id, false).await;
    assert!(response.is_err());

    // create valid request
    let object_name = rand_string(10);
    let request = CreateRequest::Object(CreateObjectRequest {
        name: object_name,
        title: "a valid title for an object".to_string(),
        description: "abc".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: 1,
        hashes: vec![],
        metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        authors: vec![],
        parent: Some(Parent::ProjectId(project.object.id.to_string())),
    });
    let response = db_handler.create_resource(request, user_id, false).await;
    // Check rule evaluation
    assert!(response.is_ok());
    let object = response.unwrap().0.object;
    // check rule inheritance
    assert!(db_handler
        .cache
        .get_rule_bindings(&object.id)
        .unwrap()
        .iter()
        .contains(&RuleBinding {
            rule_id,
            origin_id: project.object.id,
            object_id: object.id,
            cascading: true,
        }));

    // Check not-inherited rules
    let request = CreateRuleRequest {
        rule: "object.object.description == 'abc'".to_string(),
        description: "test rule".to_string(),
        public: true,
    };
    let rule_id = db_handler
        .create_rule(CreateRule(request.clone()), user_id)
        .await
        .unwrap();
    let _cached_rule = db_handler.cache.get_rule(&rule_id).unwrap();
    let collection_name = rand_string(10);
    let request = CreateRequest::Collection(CreateCollectionRequest {
        name: collection_name,
        title: "a valid title for a collection".to_string(),
        description: "abc".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: DataClass::Public as i32,
        metadata_license_tag: None,
        default_data_license_tag: None,
        authors: vec![],
        parent: Some(create_collection_request::Parent::ProjectId(
            project.object.id.to_string(),
        )),
    });
    let (collection, _) = db_handler
        .create_resource(request, user_id, false)
        .await
        .unwrap();
    let request = CreateRuleBinding(CreateRuleBindingRequest {
        rule_id: rule_id.to_string(),
        object_id: collection.object.id.to_string(),
        cascading: false,
    });
    db_handler.create_rule_binding(request).await.unwrap();
    // Try to change description
    let request = UpdateCollectionDescriptionRequest {
        collection_id: collection.object.id.to_string(),
        description: "def".to_string(),
    };
    assert!(db_handler
        .update_description(DescriptionUpdate::Collection(request))
        .await
        .is_err());
    // Create subresource without inherited rule
    let object_name = rand_string(10);
    let request = CreateRequest::Object(CreateObjectRequest {
        name: object_name,
        title: "a valid title for an object".to_string(),
        description: "xzy".to_string(),
        key_values: vec![],
        relations: vec![],
        data_class: DataClass::Public as i32,
        hashes: vec![],
        metadata_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        data_license_tag: ALL_RIGHTS_RESERVED.to_string(),
        authors: vec![],
        parent: Some(Parent::CollectionId(collection.object.id.to_string())),
    });
    let (object, _) = db_handler
        .create_resource(request, user_id, false)
        .await
        .unwrap();
    assert!(db_handler
        .cache
        .get_rule_bindings(&object.object.id)
        .unwrap()
        .iter()
        .filter(|binding| binding.rule_id == rule_id)
        .collect::<Vec<&RuleBinding>>()
        .is_empty());
}
