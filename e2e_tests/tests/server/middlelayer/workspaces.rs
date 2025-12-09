use crate::common::test_utils::{USER1_ULID, USER2_ULID};
use crate::common::{
    endpoint_mock, init,
    init_server::{init_permission_handler, init_token_handler},
    init_server,
    test_utils::rand_string,
};
use aruna_rust_api::api::storage::{
    models::v2::EndpointHostConfig,
    services::v2::{
        ClaimWorkspaceRequest, CreateEndpointRequest, CreateWorkspaceRequest,
        CreateWorkspaceTemplateRequest,
    },
};
use aruna_server::{
    auth::structs::Context,
    database::{
        crud::CrudDb,
        dsls::{
            hook_dsl::{Filter, Hook, HookVariant, InternalHook, Trigger, TriggerVariant},
            object_dsl::{KeyValue, KeyValueVariant, Object},
            user_dsl::User,
        },
        enums::{DataClass, ObjectStatus},
    },
    middlelayer::{
        endpoints_request_types::CreateEP,
        workspace_request_types::{CreateTemplate, CreateWorkspace},
    },
};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::{net::SocketAddr, str::FromStr};

#[tokio::test]
async fn create_and_delete_template() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;

    // Create template without specified endpoints and hooks
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let request = CreateTemplate(CreateWorkspaceTemplateRequest {
        owner_id: user_id.to_string(),
        prefix: "abc".to_string(),
        name: "middlelayer_template_test".to_string(),
        hook_ids: vec![],
        description: "some_desc".to_string(),
        endpoint_ids: vec![],
        rules: vec![],
    });
    let workspace_id = db_handler
        .create_workspace_template(request, user_id)
        .await
        .unwrap();
    db_handler.get_ws_template(&workspace_id).await.unwrap();
    // Delete template
    db_handler
        .delete_workspace_template(workspace_id.to_string(), &user_id)
        .await
        .unwrap();
    db_handler.get_ws_template(&workspace_id).await.unwrap_err();
}

#[tokio::test]
async fn get_templates() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;

    // Create ws templates
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let mut ids = Vec::new();
    for _ in 1..10 {
        let temp = CreateTemplate(CreateWorkspaceTemplateRequest {
            owner_id: user_id.to_string(),
            prefix: "abc".to_string(),
            name: rand_string(10),
            hook_ids: vec![],
            description: rand_string(10),
            endpoint_ids: vec![],
            rules: vec![],
        });
        ids.push(
            db_handler
                .create_workspace_template(temp, user_id)
                .await
                .unwrap(),
        );
    }
    // list owned
    let temps = db_handler.get_owned_ws(&user_id).await.unwrap();
    assert_eq!(temps.len(), ids.len());
    assert!(temps.iter().all(|t| t.owner == user_id));
}

#[tokio::test]
async fn create_and_delete_workspace() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();

    let token_handler =
        init_token_handler(db_handler.database.clone(), db_handler.cache.clone()).await;
    let authorizer = init_permission_handler(db_handler.cache.clone(), token_handler).await;

    // Create & mock endpoints
    // -> Default endpoint
    let default_endpoint = "01H81W0ZMB54YEP5711Q2BK46V".to_string();
    let default_task = endpoint_mock::start_server(
        db_handler.natsio_handler.clone(),
        "0.0.0.0:50052".parse::<SocketAddr>().unwrap(),
        default_endpoint.clone(),
    )
    .await
    .unwrap();
    // -> Custom endpoint
    let request = CreateEP(CreateEndpointRequest {
        name: "workspace_test_endpoint".to_string(),
        ep_variant: 1,
        is_public: true,
        pubkey: "MCowBQYDK2VwAyEAWom2nuIGH8K9rMP++evpPghluJmbgxtI+d8sweePzJ8=".to_string(),
        host_configs: vec![EndpointHostConfig {
            url: "http://localhost:50098".to_string(),
            is_primary: true,
            ssl: false,
            public: true,
            host_variant: 1,
        }],
    });
    let (ep, _pk) = db_handler.create_endpoint(request).await.unwrap();
    let second_task = endpoint_mock::start_server(
        db_handler.natsio_handler.clone(),
        "0.0.0.0:50098".parse::<SocketAddr>().unwrap(),
        ep.id.to_string(),
    )
    .await
    .unwrap();

    // Create hooks
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let mut hook = Hook {
        id: DieselUlid::generate(),
        name: "test".to_string(),
        description: "".to_string(),
        project_ids: vec![],
        owner: user_id,
        trigger: Json(Trigger {
            variant: TriggerVariant::RESOURCE_CREATED,
            filter: vec![Filter::KeyValue(KeyValue {
                key: "some_key".to_string(),
                value: "some_value".to_string(),
                variant: KeyValueVariant::LABEL,
            })],
        }),
        timeout: chrono::Utc::now()
            .naive_utc()
            .checked_add_days(chrono::Days::new(1))
            .unwrap(),
        hook: Json(HookVariant::Internal(InternalHook::AddLabel {
            key: "test".to_string(),
            value: "succeeded".to_string(),
        })),
    };
    hook.create(&client).await.unwrap();
    // Create templates
    // -> Invalid because hook invalid
    let invalid = CreateTemplate(CreateWorkspaceTemplateRequest {
        owner_id: user_id.to_string(),
        prefix: "abc".to_string(),
        name: rand_string(10),
        hook_ids: vec![DieselUlid::generate().to_string()],
        description: rand_string(10),
        endpoint_ids: vec![],
        rules: vec![],
    });
    // -> Template with default endpoint
    let default_endpoint_template = CreateTemplate(CreateWorkspaceTemplateRequest {
        owner_id: user_id.to_string(),
        prefix: "abc".to_string(),
        name: rand_string(10),
        hook_ids: vec![],
        description: rand_string(10),
        endpoint_ids: vec![],
        rules: vec![],
    });
    // -> Template with custom endpoint
    let custom_endpoint_template = CreateTemplate(CreateWorkspaceTemplateRequest {
        owner_id: user_id.to_string(),
        prefix: "abc".to_string(),
        name: rand_string(10),
        hook_ids: vec![],
        description: rand_string(10),
        endpoint_ids: vec![ep.id.to_string()],
        rules: vec![],
    });
    assert!(db_handler
        .create_workspace_template(invalid, user_id)
        .await
        .is_err());
    let default_template_id = db_handler
        .create_workspace_template(default_endpoint_template, user_id)
        .await
        .unwrap();
    let custom_template_id = db_handler
        .create_workspace_template(custom_endpoint_template, user_id)
        .await
        .unwrap();

    // Create workspace instances
    let default_workspace = CreateWorkspace(CreateWorkspaceRequest {
        workspace_template: default_template_id.to_string(),
        description: "This is a workspace with default endpoint config".to_string(),
    });
    let custom_workspace = CreateWorkspace(CreateWorkspaceRequest {
        workspace_template: custom_template_id.to_string(),
        description: "This is a workspace with custom endpoint config".to_string(),
    });
    let (default_instance_id, .., token_one) = db_handler
        .create_workspace(
            authorizer.clone(),
            default_workspace,
            default_endpoint.clone(),
        )
        .await
        .unwrap();
    let (custom_instance_id, .., token_two) = db_handler
        .create_workspace(
            authorizer.clone(),
            custom_workspace,
            default_endpoint.clone(),
        )
        .await
        .unwrap();
    let default_instance = Object::get(default_instance_id, &client)
        .await
        .unwrap()
        .unwrap();
    let custom_instance = Object::get(custom_instance_id, &client)
        .await
        .unwrap()
        .unwrap();
    assert!(default_instance
        .endpoints
        .0
        .contains_key(&DieselUlid::from_str(&default_endpoint).unwrap()));
    assert!(custom_instance.endpoints.0.contains_key(&ep.id));
    assert_eq!(default_instance.data_class, DataClass::WORKSPACE);
    assert_eq!(custom_instance.data_class, DataClass::WORKSPACE);
    let default_service_account = authorizer
        .check_permissions(
            &token_one,
            vec![Context::res_ctx(
                default_instance_id,
                aruna_server::database::enums::DbPermissionLevel::APPEND,
                true,
            )],
        )
        .await
        .unwrap();
    authorizer
        .check_permissions(
            &token_two,
            vec![Context::res_ctx(
                default_instance_id,
                aruna_server::database::enums::DbPermissionLevel::APPEND,
                true,
            )],
        )
        .await
        .unwrap_err();
    let custom_service_account = authorizer
        .check_permissions(
            &token_two,
            vec![Context::res_ctx(
                custom_instance_id,
                aruna_server::database::enums::DbPermissionLevel::APPEND,
                true,
            )],
        )
        .await
        .unwrap();
    authorizer
        .check_permissions(
            &token_one,
            vec![Context::res_ctx(
                custom_instance_id,
                aruna_server::database::enums::DbPermissionLevel::APPEND,
                true,
            )],
        )
        .await
        .unwrap_err();

    // Delete workspace instances
    db_handler
        .delete_workspace(default_instance_id, default_service_account)
        .await
        .unwrap();
    db_handler
        .delete_workspace(custom_instance_id, custom_service_account)
        .await
        .unwrap();
    assert!(User::get(default_service_account, &client)
        .await
        .unwrap()
        .is_none());
    assert!(User::get(custom_service_account, &client)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        Object::get(default_instance_id, &client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
    assert_eq!(
        Object::get(custom_instance_id, &client)
            .await
            .unwrap()
            .unwrap()
            .object_status,
        ObjectStatus::DELETED
    );
    default_task.abort();
    second_task.abort();
}
#[tokio::test]
pub async fn claim_workspace() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();

    let token_handler =
        init_token_handler(db_handler.database.clone(), db_handler.cache.clone()).await;
    let authorizer = init_permission_handler(db_handler.cache.clone(), token_handler).await;

    let creator_id = DieselUlid::from_str(USER1_ULID).unwrap();
    let user_id = DieselUlid::from_str(USER2_ULID).unwrap();

    /*
    let mut creator = test_utils::new_user(vec![]);
    let mut user = test_utils::new_user(vec![]);
    creator.create(&client).await.unwrap();
    user.create(&client).await.unwrap();
    */

    // Create template
    // -> Custom endpoint
    let request = CreateEP(CreateEndpointRequest {
        name: "claim_workspace_test".to_string(),
        ep_variant: 1,
        is_public: true,
        pubkey: "MCowBQYDK2VwAyEAfztFcLicUgdeSGuvIPOtQd6qDFkLpvE9dAyx4zPp8uc=".to_string(),
        host_configs: vec![EndpointHostConfig {
            url: "http://localhost:50099".to_string(),
            is_primary: true,
            ssl: false,
            public: true,
            host_variant: 1,
        }],
    });
    let (ep, _pk) = db_handler.create_endpoint(request).await.unwrap();
    let endpoint_task = endpoint_mock::start_server(
        db_handler.natsio_handler.clone(),
        "0.0.0.0:50099".parse::<SocketAddr>().unwrap(),
        ep.id.to_string(),
    )
    .await
    .unwrap();
    let template = CreateTemplate(CreateWorkspaceTemplateRequest {
        owner_id: creator_id.to_string(),
        prefix: "test".to_string(),
        name: "claim_test".to_string(),
        hook_ids: vec![],
        description: "abc".to_string(),
        endpoint_ids: vec![ep.id.to_string()],
        rules: vec![],
    });
    let template_id = db_handler
        .create_workspace_template(template, creator_id)
        .await
        .unwrap();

    // Create instance
    let request = CreateWorkspace(CreateWorkspaceRequest {
        workspace_template: template_id.to_string(),
        description: "instance description".to_string(),
    });
    let (workspace_id, .., token) = db_handler
        .create_workspace(authorizer, request, ep.id.to_string())
        .await
        .unwrap();
    let ws = Object::get(workspace_id, &client).await.unwrap().unwrap();

    assert_eq!(ws.data_class, DataClass::WORKSPACE);
    assert_eq!(ws.created_by, creator_id);

    // Claim instance
    let request = ClaimWorkspaceRequest {
        workspace_id: workspace_id.to_string(),
        token,
    };
    db_handler.claim_workspace(request, user_id).await.unwrap();

    let claimed = Object::get(workspace_id, &client).await.unwrap().unwrap();
    assert_eq!(claimed.data_class, DataClass::PRIVATE);
    assert_eq!(claimed.created_by, user_id);
    endpoint_task.abort();
}
