use crate::common::infra;
use crate::common::init::init_database;
use crate::common::init_server::Component;
use crate::common::test_utils::{new_object, USER1_ULID};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::internal_relation_dsl::InternalRelation;
use aruna_server::database::dsls::rule_dsl::{Rule, RuleBinding};
use aruna_server::database::enums::ObjectType;
use diesel_ulid::DieselUlid;
use std::str::FromStr;

#[tokio::test]
async fn test_rules() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do rules stuff on database
    let client = db.get_client().await.unwrap();
    let project = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let mut new_project = new_object(user_id, project, ObjectType::PROJECT);
    new_project.create(&client).await.unwrap();

    // DB calls:
    // - Create
    let mut new = Rule {
        id: DieselUlid::generate(),
        rule_expressions: "true".to_string(),
        description: "Create test".to_string(),
        owner_id: user_id,
        is_public: false,
    };
    let mut second = Rule {
        id: DieselUlid::generate(),
        rule_expressions: "true".to_string(),
        description: "Second create test".to_string(),
        owner_id: user_id,
        is_public: true,
    };
    new.create(&client).await.unwrap();
    second.create(&client).await.unwrap();

    // - Get
    let new_created = Rule::get(new.id, &client).await.unwrap().unwrap();
    assert_eq!(new_created, new);

    // - All
    let all = Rule::all(&client).await.unwrap();
    assert!(all.contains(&new));
    assert!(all.contains(&second));

    // - Delete
    second.delete(&client).await.unwrap();
    let all = Rule::all(&client).await.unwrap();
    assert!(!all.contains(&second));

    // - Update
    let updated = Rule {
        id: new.id,
        rule_expressions: "false".to_string(),
        description: "udpated description".to_string(),
        owner_id: user_id,
        is_public: true,
    };
    updated.update(&client).await.unwrap();
    let updated_created = Rule::get(new.id, &client).await.unwrap().unwrap();
    assert_eq!(updated, updated_created);
}

#[tokio::test]
async fn test_rule_bindings() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do rules stuff on database
    let client = db.get_client().await.unwrap();
    let project = DieselUlid::generate();
    let object = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    let mut new_project = new_object(user_id, project, ObjectType::PROJECT);
    let mut new_object = new_object(user_id, object, ObjectType::OBJECT);
    new_project.create(&client).await.unwrap();
    new_object.create(&client).await.unwrap();
    InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: project,
        origin_type: ObjectType::PROJECT,
        relation_name: "BELONGS_TO".to_string(),
        target_pid: object,
        target_type: ObjectType::OBJECT,
        target_name: new_object.name,
    }
    .create(&client)
    .await
    .unwrap();
    let mut new = Rule {
        id: DieselUlid::generate(),
        rule_expressions: "true".to_string(),
        description: "Create test".to_string(),
        owner_id: user_id,
        is_public: false,
    };
    let mut second = Rule {
        id: DieselUlid::generate(),
        rule_expressions: "true".to_string(),
        description: "Second create test".to_string(),
        owner_id: user_id,
        is_public: true,
    };
    new.create(&client).await.unwrap();
    second.create(&client).await.unwrap();

    // DB Calls
    // - Create
    let mut new_binding = RuleBinding {
        rule_id: new.id,
        origin_id: project,
        object_id: project,
        cascading: true,
    };
    let mut second_binding = RuleBinding {
        rule_id: new.id,
        origin_id: object,
        object_id: object,
        cascading: true,
    };
    new_binding.create(&client).await.unwrap();
    second_binding.create(&client).await.unwrap();

    // - Get:
    // Always fails, because schema has defined 3 primary keys, and trait requires to return just one object, which is not always one.
    // Instead of returning only the first object, failing seems better, because choosing only the first entry is unexpected when using get
    assert!(RuleBinding::get(new_binding.rule_id, &client)
        .await
        .is_err());

    // - All
    let all = RuleBinding::all(&client).await.unwrap();
    assert!(all.contains(&new_binding));
    assert!(all.contains(&second_binding));

    // - Delete
    second_binding.delete(&client).await.unwrap();
    let all = RuleBinding::all(&client).await.unwrap();
    assert!(!all.contains(&second_binding));

    // - DeleteBy (rule id and origin id)
    RuleBinding::delete_by(new.id, project, &client)
        .await
        .unwrap();
    let all = RuleBinding::all(&client).await.unwrap();
    assert!(!all.contains(&new_binding));
}
