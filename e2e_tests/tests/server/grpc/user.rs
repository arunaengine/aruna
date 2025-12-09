use std::str::FromStr;

use aruna_rust_api::api::storage::services::v2::{
    user_service_server::UserService, AcknowledgePersonalNotificationsRequest,
    CreateApiTokenRequest, GetPersonalNotificationsRequest, PersonalNotificationVariant, Reference,
    ReferenceType,
};
use aruna_server::database::enums::DbPermissionLevel;
use diesel_ulid::DieselUlid;
use itertools::Itertools;

use crate::common::{
    init, init_server,
    test_utils::{
        add_token, fast_track_grpc_permission_add, fast_track_grpc_permission_delete,
        fast_track_grpc_project_create, ADMIN_OIDC_TOKEN, USER1_OIDC_TOKEN, USER2_OIDC_TOKEN,
        USER2_ULID,
    },
};

#[tokio::test]
async fn grpc_personal_notifications() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Create random project
    let project = fast_track_grpc_project_create(&server.project_service, USER1_OIDC_TOKEN).await;
    let project_ulid = DieselUlid::from_str(&project.id).unwrap();
    let user2_ulid = DieselUlid::from_str(USER2_ULID).unwrap();

    let inner_request = GetPersonalNotificationsRequest {};

    // Get personal notifications without token
    let grpc_request = tonic::Request::new(inner_request.clone());

    let response = server
        .user_service
        .get_personal_notifications(grpc_request)
        .await;

    assert!(response.is_err());

    // Get personal notifications of correct user (specific for this test project)
    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let notifications = server
        .user_service
        .get_personal_notifications(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .notifications
        .into_iter()
        .filter(|n| n.message.contains(&project_ulid.to_string()))
        .collect_vec();

    assert!(notifications.is_empty());

    // Grant another user permission for project
    fast_track_grpc_permission_add(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user2_ulid,
        &project_ulid,
        DbPermissionLevel::WRITE,
    )
    .await;

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER2_OIDC_TOKEN);

    let notifications = server
        .user_service
        .get_personal_notifications(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .notifications;

    // Have to loop all notifications as id is unknown
    let mut validated = false;
    for notification in notifications {
        if notification.message
            == format!(
                "Permission granted for {} ({})",
                &project.name, project_ulid
            )
        {
            assert_eq!(
                notification.variant,
                PersonalNotificationVariant::PermissionGranted as i32
            );
            assert_eq!(
                notification.refs,
                vec![Reference {
                    ref_type: ReferenceType::Resource as i32,
                    ref_name: project.name.clone(),
                    ref_value: project_ulid.to_string()
                }]
            );
            validated = true;
            break;
        }
    }
    if !validated {
        panic!("Granted permission personal notification does not exist")
    } else {
        validated = false
    }

    // Revoke another user permission for project
    fast_track_grpc_permission_delete(
        &server.auth_service,
        ADMIN_OIDC_TOKEN,
        &user2_ulid,
        &project_ulid,
    )
    .await;

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER2_OIDC_TOKEN);

    let notifications = server
        .user_service
        .get_personal_notifications(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .notifications;

    // Have to loop all notifications as id is unknown
    for notification in &notifications {
        if notification.message
            == format!(
                "Permission revoked for {} ({})",
                &project.name, project_ulid
            )
        {
            assert_eq!(
                notification.variant,
                PersonalNotificationVariant::PermissionRevoked as i32
            );
            assert_eq!(
                notification.refs,
                vec![Reference {
                    ref_type: ReferenceType::Resource as i32,
                    ref_name: project.name.clone(),
                    ref_value: project_ulid.to_string()
                }]
            );
            validated = true;
            break;
        }
    }
    if !validated {
        panic!("Revoked permission personal notification does not exist")
    }

    // Acknowledge all notifications
    let notification_ids = notifications.into_iter().map(|n| n.id).collect_vec();

    let grpc_request = add_token(
        tonic::Request::new(AcknowledgePersonalNotificationsRequest { notification_ids }),
        USER2_OIDC_TOKEN,
    );

    server
        .user_service
        .acknowledge_personal_notifications(grpc_request)
        .await
        .unwrap();

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER2_OIDC_TOKEN);

    assert!(server
        .user_service
        .get_personal_notifications(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .notifications
        .is_empty());
}

#[tokio::test]
async fn grpc_add_token() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let server = init_server::init_server_services(&infra).await;

    // Get personal notifications of non-existing user
    let inner_request = CreateApiTokenRequest {
        name: "my_second_token".to_string(),
        permission: None,
        expires_at: None,
    };

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let _ = server
        .user_service
        .create_api_token(grpc_request)
        .await
        .unwrap();

    //ToDo extend test
}
