use std::{collections::HashMap, str::FromStr};

use crate::common::init::init_database;
use crate::common::init_server::Component;
use crate::common::{
    infra,
    test_utils::{ADMIN_USER_ULID, USER1_ULID, USER2_ULID},
};
use aruna_server::database::{
    crud::CrudDb,
    dsls::{
        persistent_notification_dsl::{
            NotificationReference, NotificationReferences, PersistentNotification,
        },
        user_dsl::{APIToken, User, UserAttributes},
    },
    enums::{
        DbPermissionLevel, NotificationReferenceType, ObjectMapping, PersistentNotificationVariant,
    },
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use tokio_postgres::GenericClient;

#[tokio::test]
async fn create_user_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let mut user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        first_name: "".to_string(),
        last_name: "".to_string(),
        email: "aja".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: false,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            custom_attributes: vec![],
            permissions: DashMap::default(),
            external_ids: vec![],
            pubkey: "".to_string(),
            data_proxy_attribute: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Validate creation
    if let Some(created_user) = User::get(user.id, &client).await.unwrap() {
        assert_eq!(user.id, created_user.id);
        assert_eq!(user.display_name, created_user.display_name);
        assert_eq!(user.email, created_user.email);
        assert_eq!(user.attributes.0, created_user.attributes.0);
        assert_eq!(user.active, created_user.active);
    } else {
        panic!("User should exist.")
    }
}

#[tokio::test]
async fn update_user_name_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let mut user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        first_name: "".to_string(),
        last_name: "".to_string(),
        email: "aja".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
            custom_attributes: vec![],
            external_ids: vec![],
            pubkey: "".to_string(),
            data_proxy_attribute: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Update user display name
    User::update_display_name(&client, &user.id, "tja")
        .await
        .unwrap();

    // Validate update
    if let Some(updated_user) = User::get(user.id, &client).await.unwrap() {
        assert_eq!(user.id, updated_user.id);
        assert_eq!(updated_user.display_name, "tja");
        assert_eq!(user.email, updated_user.email);
        assert_eq!(user.attributes, updated_user.attributes);
        assert_eq!(user.active, updated_user.active);
    } else {
        panic!("Object should still exist.");
    }
}

#[tokio::test]
async fn update_user_email_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let mut user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        first_name: "".to_string(),
        last_name: "".to_string(),
        email: "aja".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
            custom_attributes: vec![],
            external_ids: vec![],
            pubkey: "".to_string(),
            data_proxy_attribute: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Update user email
    User::update_email(&client, &user.id, "willy_nilly")
        .await
        .unwrap();

    // Validate update
    if let Some(updated_user) = User::get(user.id, &client).await.unwrap() {
        assert_eq!(user.id, updated_user.id);
        assert_eq!(user.display_name, updated_user.display_name);
        assert_eq!(updated_user.email, "willy_nilly");
        assert_eq!(user.attributes, updated_user.attributes);
        assert_eq!(user.active, updated_user.active);
    } else {
        panic!("Object should still exist.");
    }
}

#[tokio::test]
async fn update_user_admin_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let mut user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        first_name: "".to_string(),
        last_name: "".to_string(),
        email: "aja".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
            custom_attributes: vec![],
            external_ids: vec![],
            pubkey: "".to_string(),
            data_proxy_attribute: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Update user email
    User::set_user_global_admin(&client, &user.id, true)
        .await
        .unwrap();

    // Validate update
    if let Some(updated_user) = User::get(user.id, &client).await.unwrap() {
        assert_eq!(user.id, updated_user.id);
        assert_eq!(user.display_name, updated_user.display_name);
        assert_eq!(user.email, updated_user.email);
        assert!(updated_user.attributes.0.global_admin);
        assert_eq!(user.active, updated_user.active);
    } else {
        panic!("Object should still exist.");
    }
}

#[tokio::test]
async fn update_user_service_account_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let mut user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        first_name: "".to_string(),
        last_name: "".to_string(),
        email: "aja".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
            custom_attributes: vec![],
            external_ids: vec![],
            pubkey: "".to_string(),
            data_proxy_attribute: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Update user email
    User::set_user_service_account(&client, &user.id, true)
        .await
        .unwrap();

    // Validate update
    if let Some(updated_user) = User::get(user.id, &client).await.unwrap() {
        assert_eq!(user.id, updated_user.id);
        assert_eq!(user.display_name, updated_user.display_name);
        assert_eq!(user.email, updated_user.email);
        assert!(updated_user.attributes.0.service_account);
        assert_eq!(user.active, updated_user.active);
    } else {
        panic!("Object should still exist.");
    }
}

#[tokio::test]
async fn delete_user_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let mut user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        first_name: "".to_string(),
        last_name: "".to_string(),
        email: "aja".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
            custom_attributes: vec![],
            external_ids: vec![],
            pubkey: "".to_string(),
            data_proxy_attribute: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    // Delete user
    user.delete(&client).await.unwrap();

    // Validate update
    if User::get(user.id, &client).await.unwrap().is_some() {
        panic!("User should not exist anymore")
    }
}

#[tokio::test]
async fn add_permission_user_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();

    // Define and create user in database
    let mut user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        first_name: "".to_string(),
        last_name: "".to_string(),
        email: "aja".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
            custom_attributes: vec![],
            external_ids: vec![],
            pubkey: "".to_string(),
            data_proxy_attribute: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    User::add_user_permission(
        &client,
        &user.id,
        [(
            DieselUlid::generate(),
            ObjectMapping::PROJECT(DbPermissionLevel::ADMIN),
        )]
        .into_iter()
        .collect(),
    )
    .await
    .unwrap();

    User::add_user_permission(
        &client,
        &user.id,
        [(
            DieselUlid::generate(),
            ObjectMapping::COLLECTION(DbPermissionLevel::READ),
        )]
        .into_iter()
        .collect(),
    )
    .await
    .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .permissions
            .len(),
        2
    );
}

#[tokio::test]
async fn remove_user_permission_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();

    let perm1 = DieselUlid::generate();
    let perm2 = DieselUlid::generate();
    let perm3 = DieselUlid::generate();
    // Define and create user in database
    let mut user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        first_name: "".to_string(),
        last_name: "".to_string(),
        email: "aja".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::from_iter([
                (perm1, ObjectMapping::PROJECT(DbPermissionLevel::ADMIN)),
                (perm2, ObjectMapping::COLLECTION(DbPermissionLevel::READ)),
                (perm3, ObjectMapping::COLLECTION(DbPermissionLevel::WRITE)),
            ]),
            custom_attributes: vec![],
            external_ids: vec![],
            pubkey: "".to_string(),
            data_proxy_attribute: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .permissions
            .len(),
        3
    );

    User::remove_user_permission(&client, &user.id, &perm2)
        .await
        .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .permissions
            .len(),
        2
    );

    User::remove_user_permission(&client, &user.id, &perm1)
        .await
        .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .permissions
            .len(),
        1
    );

    User::remove_user_permission(&client, &user.id, &perm3)
        .await
        .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .permissions
            .len(),
        0
    );
}

#[tokio::test]
async fn update_user_permission_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();

    let project_id = DieselUlid::generate();

    // Define and create user in database
    let mut user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        first_name: "".to_string(),
        last_name: "".to_string(),
        email: "aja".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: true,
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            permissions: DashMap::default(),
            custom_attributes: vec![],
            external_ids: vec![],
            pubkey: "".to_string(),
            data_proxy_attribute: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    let user = User::add_user_permission(
        &client,
        &user.id,
        [(project_id, ObjectMapping::PROJECT(DbPermissionLevel::ADMIN))]
            .into_iter()
            .collect(),
    )
    .await
    .unwrap();

    assert_eq!(user.get_permissions(None).unwrap().0.len(), 1);
    assert!(user
        .get_permissions(None)
        .unwrap()
        .0
        .contains(&(project_id, DbPermissionLevel::ADMIN)));

    let user = User::update_user_permission(
        &client,
        &user.id,
        &project_id,
        ObjectMapping::PROJECT(DbPermissionLevel::WRITE),
    )
    .await
    .unwrap();

    assert_eq!(user.get_permissions(None).unwrap().0.len(), 1);
    assert!(user
        .get_permissions(None)
        .unwrap()
        .0
        .contains(&(project_id, DbPermissionLevel::WRITE)));
}

#[tokio::test]
async fn user_token_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();

    let perm1 = DieselUlid::generate();
    let perm2 = DieselUlid::generate();
    let perm3 = DieselUlid::generate();
    // Define and create user in database
    let mut user = User {
        id: DieselUlid::generate(),
        display_name: "aha".to_string(),
        first_name: "".to_string(),
        last_name: "".to_string(),
        email: "aja".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: true,
            permissions: DashMap::default(),
            trusted_endpoints: DashMap::default(),
            external_ids: vec![],
            pubkey: "".to_string(),
            tokens: [
                (
                    perm1,
                    APIToken {
                        pub_key: 1,
                        name: "test".to_string(),
                        created_at: chrono::Utc::now().naive_utc(),
                        expires_at: chrono::Utc::now().naive_utc(),
                        object_id: Some(ObjectMapping::PROJECT(DieselUlid::generate())),
                        user_rights: DbPermissionLevel::ADMIN,
                    },
                ),
                (
                    perm2,
                    APIToken {
                        pub_key: 1,
                        name: "test".to_string(),
                        created_at: chrono::Utc::now().naive_utc(),
                        expires_at: chrono::Utc::now().naive_utc(),
                        object_id: Some(ObjectMapping::COLLECTION(DieselUlid::generate())),
                        user_rights: DbPermissionLevel::ADMIN,
                    },
                ),
                (
                    perm3,
                    APIToken {
                        pub_key: 1,
                        name: "test".to_string(),
                        created_at: chrono::Utc::now().naive_utc(),
                        expires_at: chrono::Utc::now().naive_utc(),
                        object_id: Some(ObjectMapping::DATASET(DieselUlid::generate())),
                        user_rights: DbPermissionLevel::ADMIN,
                    },
                ),
            ]
            .into_iter()
            .collect(),
            custom_attributes: vec![],
            data_proxy_attribute: vec![],
        }),
        active: true,
    };

    user.create(&client).await.unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .tokens
            .len(),
        3
    );

    User::remove_user_token(&client, &user.id, &perm2)
        .await
        .unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .tokens
            .len(),
        2
    );

    User::remove_all_tokens(&client, &user.id).await.unwrap();

    assert_eq!(
        User::get(user.id, &client)
            .await
            .unwrap()
            .unwrap()
            .attributes
            .0
            .tokens
            .len(),
        0
    );
}

#[tokio::test]
async fn user_status_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let id = DieselUlid::generate();

    let mut user = User {
        id,
        display_name: "aha".to_string(),
        first_name: "".to_string(),
        last_name: "".to_string(),
        email: "aja".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: true,
            permissions: DashMap::default(),
            trusted_endpoints: DashMap::default(),
            tokens: DashMap::default(),
            custom_attributes: Vec::new(),
            external_ids: vec![],
            pubkey: "".to_string(),
            data_proxy_attribute: vec![],
        }),
        active: false,
    };
    user.create(client).await.unwrap();

    User::activate_user(client, &id).await.unwrap();
    assert!(User::get(id, client).await.unwrap().unwrap().active);

    User::deactivate_user(client, &id).await.unwrap();
    assert!(!User::get(id, client).await.unwrap().unwrap().active);
}

#[tokio::test]
async fn persistent_notification_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let notification_ulid = DieselUlid::generate();
    let resource_ulid = DieselUlid::generate();
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();

    // Create notification in database
    let mut pers_notification = PersistentNotification {
        id: notification_ulid,
        user_id: user_ulid,
        notification_variant: PersistentNotificationVariant::PERMISSION_GRANTED,
        message: format!("Permission granted for example.file ({resource_ulid})"),
        refs: Json(NotificationReferences(vec![NotificationReference {
            reference_type: NotificationReferenceType::Resource,
            reference_name: "example.file".to_string(),
            reference_value: resource_ulid.to_string(),
        }])),
    };
    pers_notification.create(client).await.unwrap();

    // Fetch notification
    let validation = PersistentNotification::get(notification_ulid, client)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(pers_notification.id, validation.id);
    assert_eq!(pers_notification.user_id, validation.user_id);
    assert_eq!(
        pers_notification.notification_variant,
        validation.notification_variant
    );
    assert_eq!(
        pers_notification.message,
        format!("Permission granted for example.file ({resource_ulid})")
    );
    assert_eq!(pers_notification.refs, validation.refs);

    // Acknowledge notification
    PersistentNotification::acknowledge_user_notifications(&vec![notification_ulid], client)
        .await
        .unwrap();

    assert!(PersistentNotification::get(notification_ulid, client)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn add_token_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();

    User::add_user_token(
        client,
        &user_ulid,
        HashMap::from_iter([(
            DieselUlid::generate(),
            &APIToken {
                pub_key: 1,
                name: "mytoken".to_string(),
                created_at: chrono::Utc::now().naive_utc(),
                expires_at: chrono::Utc::now().naive_utc(),
                object_id: None,
                user_rights: DbPermissionLevel::NONE,
            },
        )]),
    )
    .await
    .unwrap();

    //ToDo extend test
}

#[tokio::test]
async fn add_remove_trusted_endpoint_test() {
    // Create database and load schema + initial data
    let postgres = infra::postgres(false).await;
    let (_, db) = init_database(&postgres, Component::Server).await;

    // Do users stuff on database
    let client = db.get_client().await.unwrap();
    let client = client.client();

    let user1_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let user2_ulid = DieselUlid::from_str(USER2_ULID).unwrap();
    let sentinel_user_ulid = DieselUlid::from_str(ADMIN_USER_ULID).unwrap();

    let endpoint1 = DieselUlid::generate();
    let endpoint2 = DieselUlid::generate();
    let endpoint3 = DieselUlid::generate();

    // Add first endpoint to user1
    let user1 = User::add_trusted_endpoint(client, &user1_ulid, &endpoint1)
        .await
        .unwrap();
    assert_eq!(user1.attributes.0.trusted_endpoints.len(), 1);
    assert!(user1
        .attributes
        .0
        .trusted_endpoints
        .contains_key(&endpoint1));

    // Add second endpoint to user1
    let user1 = User::add_trusted_endpoint(client, &user1_ulid, &endpoint2)
        .await
        .unwrap();
    assert_eq!(user1.attributes.0.trusted_endpoints.len(), 2);
    assert!(user1
        .attributes
        .0
        .trusted_endpoints
        .contains_key(&endpoint1));
    assert!(user1
        .attributes
        .0
        .trusted_endpoints
        .contains_key(&endpoint2));

    // Add first endpoint to user2
    let user2 = User::add_trusted_endpoint(client, &user2_ulid, &endpoint2)
        .await
        .unwrap();
    assert_eq!(user2.attributes.0.trusted_endpoints.len(), 1);
    assert!(user2
        .attributes
        .0
        .trusted_endpoints
        .contains_key(&endpoint2));

    // Add totem endpoint to admin user
    let sentinel_user = User::add_trusted_endpoint(client, &sentinel_user_ulid, &endpoint3)
        .await
        .unwrap();
    assert_eq!(sentinel_user.attributes.0.trusted_endpoints.len(), 1);
    assert!(sentinel_user
        .attributes
        .0
        .trusted_endpoints
        .contains_key(&endpoint3));

    // Remove non-existing endpoint
    let users = User::remove_endpoint_from_users(client, &DieselUlid::generate())
        .await
        .unwrap();

    for user in users {
        if user.id == user1_ulid {
            assert_eq!(user.attributes.0.trusted_endpoints.len(), 2);
            assert!(user.attributes.0.trusted_endpoints.contains_key(&endpoint1));
            assert!(user.attributes.0.trusted_endpoints.contains_key(&endpoint2))
        } else if user.id == user2_ulid {
            assert_eq!(user.attributes.0.trusted_endpoints.len(), 1);
            assert!(user.attributes.0.trusted_endpoints.contains_key(&endpoint2))
        } else if user.id == sentinel_user_ulid {
            assert_eq!(user.attributes.0.trusted_endpoints.len(), 1);
            assert!(user.attributes.0.trusted_endpoints.contains_key(&endpoint3))
        }
    }

    // Remove first endpoint
    let users = User::remove_endpoint_from_users(client, &endpoint1)
        .await
        .unwrap();

    for user in users {
        if user.id == user1_ulid || user.id == user2_ulid {
            assert_eq!(user.attributes.0.trusted_endpoints.len(), 1);
            assert!(user.attributes.0.trusted_endpoints.contains_key(&endpoint2))
        } else if user.id == sentinel_user_ulid {
            assert_eq!(user.attributes.0.trusted_endpoints.len(), 1);
            assert!(user.attributes.0.trusted_endpoints.contains_key(&endpoint3))
        }
    }

    // Remove second endpoint
    let users = User::remove_endpoint_from_users(client, &endpoint2)
        .await
        .unwrap();

    for user in users {
        if user.id == user1_ulid || user.id == user2_ulid {
            assert_eq!(user.attributes.0.trusted_endpoints.len(), 0)
        } else if user.id == sentinel_user_ulid {
            assert_eq!(user.attributes.0.trusted_endpoints.len(), 1);
            assert!(user.attributes.0.trusted_endpoints.contains_key(&endpoint3))
        }
    }

    // Remove single endpoint from test_user
    let totem_user = User::remove_trusted_endpoint(client, &sentinel_user_ulid, &endpoint3)
        .await
        .unwrap();
    assert_eq!(totem_user.attributes.0.trusted_endpoints.len(), 0)
}
