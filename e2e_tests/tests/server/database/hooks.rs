use crate::common::init::init_database;
use crate::common::init_server::Component;
use crate::common::test_utils::{USER1_ULID, USER2_ULID};
use crate::common::{infra, test_utils};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::hook_dsl::{
    Filter, Hook, HookVariant, HookWithAssociatedProject, Trigger, TriggerVariant,
};
use aruna_server::database::dsls::object_dsl::{KeyValue, KeyValueVariant};
use aruna_server::database::enums::ObjectType;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::str::FromStr;

#[tokio::test]
async fn create_hook() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do hooks stuff on database
    let client = db.get_client().await.unwrap();
    let proj_id = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    // Create hook
    let hook_id = DieselUlid::generate();
    let mut hook = Hook {
        id: hook_id,
        name: "HookName".to_string(),
        description: "SOME_DESCRIPTION".to_string(),
        owner: user_id,
        project_ids: vec![proj_id],
        trigger: Json(Trigger {
            variant: TriggerVariant::HOOK_ADDED,
            filter: vec![Filter::KeyValue(KeyValue {
                key: "TEST_KEY".to_string(),
                value: "TEST_VALUE".to_string(),
                variant: KeyValueVariant::HOOK,
            })],
        }),
        timeout: chrono::Utc::now()
            .naive_utc()
            .checked_add_days(chrono::Days::new(1))
            .unwrap(),
        hook: Json(HookVariant::Internal(
            aruna_server::database::dsls::hook_dsl::InternalHook::AddLabel {
                key: "HOOK_STATUS".to_string(),
                value: "HOOK_TRIGGERED_SUCCESSFULL".to_string(),
            },
        )),
    };

    let mut create_project = test_utils::new_object(user_id, proj_id, ObjectType::PROJECT);
    create_project.create(&client).await.unwrap();
    hook.create(&client).await.unwrap();
    let created = Hook::get(hook.id, &client).await.unwrap().unwrap();
    assert_eq!(hook, created);
}
#[tokio::test]
pub async fn get_hooks() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do hooks stuff on database
    let client = db.get_client().await.unwrap();
    let proj_id = DieselUlid::generate();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    // Create hooks
    // Hooks for association query tests
    let other_hook_ids = vec![
        (DieselUlid::generate(), DieselUlid::generate()),
        (DieselUlid::generate(), DieselUlid::generate()),
        (DieselUlid::generate(), DieselUlid::generate()),
    ];
    let (_, projects): (Vec<DieselUlid>, Vec<DieselUlid>) =
        other_hook_ids.clone().into_iter().unzip();
    let other_user_id = DieselUlid::from_str(USER2_ULID).unwrap();

    let mut other_hooks = Vec::new();
    for (hook_id, proj_id) in other_hook_ids.clone() {
        let mut hook = Hook {
            id: hook_id,
            name: "HookName".to_string(),
            description: "SOME_DESCRIPTION".to_string(),
            owner: other_user_id,
            project_ids: projects.clone(),
            trigger: Json(Trigger {
                variant: TriggerVariant::HOOK_ADDED,
                filter: vec![Filter::KeyValue(KeyValue {
                    key: "TEST_KEY".to_string(),
                    value: "TEST_VALUE".to_string(),
                    variant: KeyValueVariant::HOOK,
                })],
            }),
            timeout: chrono::Utc::now()
                .naive_utc()
                .checked_add_days(chrono::Days::new(1))
                .unwrap(),
            hook: Json(HookVariant::Internal(
                aruna_server::database::dsls::hook_dsl::InternalHook::AddLabel {
                    key: "HOOK_STATUS".to_string(),
                    value: "HOOK_TRIGGERED_SUCCESSFULL".to_string(),
                },
            )),
        };

        let mut create_project = test_utils::new_object(user_id, proj_id, ObjectType::PROJECT);
        create_project.create(&client).await.unwrap();
        hook.create(&client).await.unwrap();
        other_hooks.push(hook);
    }
    let hook_id = DieselUlid::generate();
    let mut hook = Hook {
        id: hook_id,
        name: "HookName".to_string(),
        description: "SOME_DESCRIPTION".to_string(),
        owner: user_id,
        project_ids: vec![proj_id],
        trigger: Json(Trigger {
            variant: TriggerVariant::HOOK_ADDED,
            filter: vec![Filter::KeyValue(KeyValue {
                key: "TEST_KEY".to_string(),
                value: "TEST_VALUE".to_string(),
                variant: KeyValueVariant::HOOK,
            })],
        }),
        timeout: chrono::Utc::now()
            .naive_utc()
            .checked_add_days(chrono::Days::new(1))
            .unwrap(),
        hook: Json(HookVariant::Internal(
            aruna_server::database::dsls::hook_dsl::InternalHook::AddLabel {
                key: "HOOK_STATUS".to_string(),
                value: "HOOK_TRIGGERED_SUCCESSFULL".to_string(),
            },
        )),
    };

    let mut create_project = test_utils::new_object(user_id, proj_id, ObjectType::PROJECT);
    create_project.create(&client).await.unwrap();
    hook.create(&client).await.unwrap();

    // List hooks
    // -> Lists hooks with queried projects
    let hook_with_project = Hook::get_hooks_for_projects(&vec![proj_id], &client)
        .await
        .unwrap();
    assert_eq!(hook_with_project.len(), 1);
    let hook_with_project = &hook_with_project[0];
    let other = HookWithAssociatedProject {
        id: hook.id,
        name: hook.name.clone(),
        description: hook.description.clone(),
        project_ids: hook.project_ids.clone(),
        owner: hook.owner,
        trigger: hook.trigger.clone(),
        timeout: hook.timeout,
        hook: hook.hook.clone(),
        project_id: proj_id,
    };
    assert_eq!(hook_with_project, &other);
    // -> Test with associated hook
    let other_hooks_with_projects = Hook::get_hooks_for_projects(&projects, &client)
        .await
        .unwrap();
    let map = other_hooks_with_projects
        .into_iter()
        .map(|h| (h.id, h.project_id))
        .collect::<Vec<(DieselUlid, DieselUlid)>>();
    // 3 hooks with 3 project ids each
    // searching for these 3 projects = 9 results
    assert_eq!(map.len(), 9);
    assert!(map.contains(&other_hook_ids[0]));
    assert!(map.contains(&other_hook_ids[1]));
    assert!(map.contains(&other_hook_ids[2]));
    // Collect each combination
    let mut one = Vec::new();
    let mut two = Vec::new();
    let mut three = Vec::new();
    for (hook, proj) in map {
        if hook == other_hook_ids[0].0 {
            one.push(proj);
        } else if hook == other_hook_ids[1].0 {
            two.push(proj)
        } else if hook == other_hook_ids[2].0 {
            three.push(proj)
        }
    }
    assert_eq!(one.len(), 3);
    assert!(one.contains(&projects[0]));
    assert!(one.contains(&projects[1]));
    assert!(one.contains(&projects[2]));
    assert_eq!(two.len(), 3);
    assert!(two.contains(&projects[0]));
    assert!(two.contains(&projects[1]));
    assert!(two.contains(&projects[2]));
    assert_eq!(three.len(), 3);
    assert!(three.contains(&projects[0]));
    assert!(three.contains(&projects[1]));
    assert!(three.contains(&projects[2]));

    let projects = Hook::get_project_from_hook(&hook_id, &client)
        .await
        .unwrap();
    // --> Lists projects associated with hook
    assert!(projects.contains(&proj_id));

    let hooks = Hook::list_hooks(&proj_id, &client).await.unwrap();
    // --> Lists all hooks that contain project_id
    assert_eq!(hooks.len(), 1);
    assert_eq!(hooks[0], hook);

    let owned_hooks = Hook::list_owned(&user_id, &client).await.unwrap();
    let otherowned_hooks = Hook::list_owned(&other_user_id, &client).await.unwrap();
    assert_eq!(hooks.len(), 1);
    assert_eq!(owned_hooks[0], hook);
    assert_eq!(otherowned_hooks.len(), 3);
    assert!(otherowned_hooks.contains(&other_hooks[0]));
    assert!(otherowned_hooks.contains(&other_hooks[1]));
    assert!(otherowned_hooks.contains(&other_hooks[2]));

    assert!(Hook::exists(&vec![hook_id], &client).await.is_ok());
    // -> Checks if hook exists
}
