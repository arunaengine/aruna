use crate::common::test_utils::USER1_ULID;
use crate::common::{init, init_server};
use aruna_rust_api::api::storage::services::v2::{
    ActivateUserRequest, DeactivateUserRequest, RegisterUserRequest, UpdateUserDisplayNameRequest,
    UpdateUserEmailRequest,
};
use aruna_server::database::crud::CrudDb;
use aruna_server::database::dsls::user_dsl::{OIDCMapping, User};
use aruna_server::middlelayer::user_request_types::{
    ActivateUser, DeactivateUser, RegisterUser, UpdateUserEmail, UpdateUserName,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;

#[tokio::test]
async fn test_register_user() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;

    let display_name = "test_name".to_string();
    let email = "test.test@test.org".to_string();
    let request = RegisterUser(RegisterUserRequest {
        display_name: display_name.clone(),
        email: email.clone(),
        project: "".to_string(),
    });
    let oidc = OIDCMapping {
        external_id: "123456".to_string(),
        oidc_name: "http://localhost:1998/realms/test".to_string(),
    };
    let user = db_handler.register_user(request, oidc).await.unwrap();

    assert_eq!(user.email, email);
    assert_eq!(user.display_name, display_name);
    assert!(!user.active);
    assert!(!user.attributes.0.global_admin);
    assert!(!user.attributes.0.service_account);
    assert!(user.attributes.0.tokens.is_empty());
    assert!(user.attributes.0.trusted_endpoints.is_empty());
    assert!(user.attributes.0.custom_attributes.is_empty());
    assert!(user.attributes.0.permissions.is_empty());
    let db_user = User::get(user.id, &db_handler.database.get_client().await.unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(db_user, user);
}

#[tokio::test]
async fn test_toggle_user() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;

    let client = db_handler.database.get_client().await.unwrap();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    // test deactivation
    let request = DeactivateUser(DeactivateUserRequest {
        user_id: user_id.to_string(),
    });
    let user = db_handler.deactivate_user(request).await.unwrap();
    assert!(!user.active);
    let db_user = User::get(user_id, &client).await.unwrap().unwrap();
    assert!(!db_user.active);

    // Test activation
    let request = ActivateUser(ActivateUserRequest {
        user_id: user_id.to_string(),
    });
    let user = db_handler.activate_user(request).await.unwrap();
    assert!(user.active);
    let db_user = User::get(user_id, &client).await.unwrap().unwrap();
    assert!(db_user.active);
}

#[tokio::test]
async fn test_update_display_name() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    // Test display name update
    let new_display_name = "updated_name".to_string();
    let request = UpdateUserName(UpdateUserDisplayNameRequest {
        new_display_name: new_display_name.clone(),
    });
    let new = db_handler
        .update_display_name(request, user_id)
        .await
        .unwrap();
    assert_eq!(&new.display_name, &new_display_name);
    let db_user = User::get(user_id, &client).await.unwrap().unwrap();
    assert_eq!(&db_user.display_name, &new_display_name);
}

#[tokio::test]
async fn test_update_email() {
    // Init
    let infra = init::init_test_environment(true).await;
    let db_handler =
        init_server::init_database_handler_middlelayer(&infra.postgres, &infra.nats).await;
    let client = db_handler.database.get_client().await.unwrap();
    let user_id = DieselUlid::from_str(USER1_ULID).unwrap();

    // Test email update
    let new_email = "updated@test.org".to_string();
    let request = UpdateUserEmail(UpdateUserEmailRequest {
        user_id: user_id.to_string(),
        new_email: new_email.clone(),
    });
    let new = db_handler.update_email(request, user_id).await.unwrap();
    assert_eq!(&new.email, &new_email);
    let db_user = User::get(user_id, &client).await.unwrap().unwrap();
    assert_eq!(&db_user.email, &new_email);
}
