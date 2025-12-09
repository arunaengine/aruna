use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v2::{permission::ResourceId, Permission, PermissionLevel},
    services::v2::{
        authorization_service_server::AuthorizationService, create_collection_request,
        user_service_server::UserService, CreateAuthorizationRequest, DeleteAuthorizationRequest,
        GetAuthorizationsRequest, GetUserRequest, UpdateAuthorizationRequest, UserPermission,
    },
};
use aruna_server::database::{
    crud::CrudDb,
    dsls::user_dsl::User,
    enums::{DbPermissionLevel, ObjectMapping},
};
use diesel_ulid::DieselUlid;

use crate::common::{
    init, init_server,
    test_utils::{
        add_token, fast_track_grpc_collection_create, fast_track_grpc_permission_add,
        fast_track_grpc_project_create, ADMIN_OIDC_TOKEN, ADMIN_USER_ULID, USER1_OIDC_TOKEN,
        USER1_ULID,
    },
};

#[tokio::test]
async fn grpc_create_authorization() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let service_block = init_server::init_server_services(&infra).await;

    // Create random project
    let project =
        fast_track_grpc_project_create(&service_block.project_service, ADMIN_OIDC_TOKEN).await;
    let project_ulid = DieselUlid::from_str(&project.id).unwrap();

    // Get normal user
    let user = service_block
        .user_service
        .get_user(add_token(
            tonic::Request::new(GetUserRequest {
                user_id: "".to_string(),
            }),
            USER1_OIDC_TOKEN,
        ))
        .await
        .unwrap()
        .into_inner()
        .user
        .unwrap();

    // User does not have permission on created project
    assert!(!user
        .attributes
        .unwrap()
        .personal_permissions
        .contains(&Permission {
            permission_level: PermissionLevel::Admin as i32,
            resource_id: Some(ResourceId::ProjectId(project.id.clone()))
        }));

    // Add permission to user who does not exist
    let mut inner_request = CreateAuthorizationRequest {
        resource_id: project.id.clone(),
        user_id: DieselUlid::generate().to_string(),
        permission_level: PermissionLevel::Read as i32,
    };

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .create_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    // Add permission to user without token
    inner_request.user_id.clone_from(&user.id);

    let response = service_block
        .auth_service
        .create_authorization(tonic::Request::new(inner_request.clone()))
        .await;

    assert!(response.is_err());

    // Add permission to user without sufficient permission
    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .create_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    // Add permission to user
    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .create_authorization(grpc_request)
        .await
        .unwrap()
        .into_inner();

    // Fetch updated user from cache and database
    let cache_user = service_block
        .cache
        .get_user(&DieselUlid::from_str(&user.id).unwrap())
        .unwrap();
    let db_user = User::get(
        DieselUlid::from_str(&user.id).unwrap(),
        &service_block.db_conn.get_client().await.unwrap(),
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(response.user_id, cache_user.id.to_string());
    assert_eq!(response.user_id, db_user.id.to_string());
    assert_eq!(
        cache_user
            .attributes
            .0
            .permissions
            .get(&project_ulid)
            .unwrap()
            .value(),
        &ObjectMapping::PROJECT(DbPermissionLevel::READ)
    );

    assert_eq!(response.user_name, cache_user.display_name);
    assert_eq!(response.user_name, db_user.display_name);
    assert_eq!(
        db_user
            .attributes
            .0
            .permissions
            .get(&project_ulid)
            .unwrap()
            .value(),
        &ObjectMapping::PROJECT(DbPermissionLevel::READ)
    );

    assert_eq!(response.resource_id, project.id);
    assert_eq!(response.permission_level(), PermissionLevel::Read);
}

#[tokio::test]
async fn grpc_get_authorization() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let service_block = init_server::init_server_services(&infra).await;

    // Create random project
    let project =
        fast_track_grpc_project_create(&service_block.project_service, ADMIN_OIDC_TOKEN).await;
    let project_ulid = DieselUlid::from_str(&project.id).unwrap();

    // Create ULID from user id
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();

    // Get authorization of resource who does not exist
    let mut inner_request = GetAuthorizationsRequest {
        resource_id: DieselUlid::generate().to_string(),
        recursive: false,
    };

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let authorizations = service_block
        .auth_service
        .get_authorizations(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .authorizations;

    assert!(authorizations.is_empty());

    // Get authorization of resource without token
    inner_request.resource_id = project.id;

    let response = service_block
        .auth_service
        .get_authorizations(tonic::Request::new(inner_request.clone()))
        .await;

    assert!(response.is_err());

    // Get authorization of resource without sufficient permissions
    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .get_authorizations(grpc_request)
        .await;

    assert!(response.is_err());

    // Get authorization of resource with sufficient permissions
    fast_track_grpc_permission_add(
        &service_block.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &project_ulid,
        DbPermissionLevel::ADMIN,
    )
    .await;

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let authorizations = service_block
        .auth_service
        .get_authorizations(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .authorizations;

    assert_eq!(authorizations.len(), 1);
    assert!(authorizations[0].user_permission.contains(&UserPermission {
        user_id: USER1_ULID.to_string(),
        user_name: "test-user".to_string(),
        permission_level: PermissionLevel::Admin as i32,
    }));
    assert!(authorizations[0].user_permission.contains(&UserPermission {
        user_id: ADMIN_USER_ULID.to_string(),
        user_name: "test-admin".to_string(),
        permission_level: PermissionLevel::Admin as i32,
    }));

    // Get authorization of resource and its subresources
    inner_request.recursive = true;

    let collection = fast_track_grpc_collection_create(
        &service_block.collection_service,
        USER1_OIDC_TOKEN,
        create_collection_request::Parent::ProjectId(project_ulid.to_string()),
    )
    .await;

    // Add separate permission for subresource
    fast_track_grpc_permission_add(
        &service_block.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &DieselUlid::from_str(&collection.id).unwrap(),
        DbPermissionLevel::READ,
    )
    .await;

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let authorizations = service_block
        .auth_service
        .get_authorizations(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .authorizations;

    assert_eq!(authorizations.len(), 2);
    for authorization in authorizations {
        if authorization.resource_id == project_ulid.to_string() {
            assert!(authorization.user_permission.contains(&UserPermission {
                user_id: USER1_ULID.to_string(),
                user_name: "test-user".to_string(),
                permission_level: PermissionLevel::Admin as i32,
            }));
            assert!(authorization.user_permission.contains(&UserPermission {
                user_id: ADMIN_USER_ULID.to_string(),
                user_name: "test-admin".to_string(),
                permission_level: PermissionLevel::Admin as i32,
            }));
        } else if authorization.resource_id == collection.id {
            assert!(authorization.user_permission.contains(&UserPermission {
                user_id: USER1_ULID.to_string(),
                user_name: "test-user".to_string(),
                permission_level: PermissionLevel::Read as i32,
            }));
        } else {
            panic!(
                "Authorizations should not contain this id {}",
                authorization.resource_id
            )
        }
    }
}

#[tokio::test]
async fn grpc_update_authorization() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let service_block = init_server::init_server_services(&infra).await;

    // Create random project
    let project =
        fast_track_grpc_project_create(&service_block.project_service, ADMIN_OIDC_TOKEN).await;
    let project_ulid = DieselUlid::from_str(&project.id).unwrap();

    // Create ULID from user id
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();
    let admin_ulid = DieselUlid::from_str(ADMIN_USER_ULID).unwrap();

    // Update authorization with non-existing user
    let mut inner_request = UpdateAuthorizationRequest {
        resource_id: project.id.clone(),
        user_id: DieselUlid::generate().to_string(),
        permission_level: PermissionLevel::Read as i32,
    };

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .update_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    // Update authorization to non-existing resource
    inner_request.resource_id = DieselUlid::generate().to_string();
    inner_request.user_id = admin_ulid.to_string();

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .update_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    // Update authorization without token
    inner_request.resource_id = project_ulid.to_string();

    let grpc_request = tonic::Request::new(inner_request.clone());

    let response = service_block
        .auth_service
        .update_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    // Update authorization without sufficient permissions
    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .update_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    // Update authorization with sufficient permissions
    fast_track_grpc_permission_add(
        &service_block.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &DieselUlid::from_str(&project.id).unwrap(),
        DbPermissionLevel::ADMIN,
    )
    .await;

    let user = service_block.cache.get_user(&user_ulid).unwrap();
    assert_eq!(
        user.attributes
            .0
            .permissions
            .get(&DieselUlid::from_str(&project.id).unwrap())
            .unwrap()
            .value(),
        &ObjectMapping::PROJECT(DbPermissionLevel::ADMIN)
    );

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let authorization = service_block
        .auth_service
        .update_authorization(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .user_permission
        .unwrap();

    let admin_user = service_block.cache.get_user(&admin_ulid).unwrap();
    assert_eq!(
        authorization,
        UserPermission {
            user_id: admin_ulid.to_string(),
            user_name: admin_user.display_name,
            permission_level: PermissionLevel::Read as i32
        }
    );

    assert!(admin_user
        .attributes
        .0
        .permissions
        .contains_key(&project_ulid));
    assert_eq!(
        admin_user
            .attributes
            .0
            .permissions
            .get(&project_ulid)
            .unwrap()
            .value(),
        &ObjectMapping::PROJECT(DbPermissionLevel::READ)
    );
}

#[tokio::test]
async fn grpc_delete_authorization() {
    // Init gRPC services
    let infra = init::init_test_environment(true).await;
    let service_block = init_server::init_server_services(&infra).await;

    // Create random project
    let project =
        fast_track_grpc_project_create(&service_block.project_service, ADMIN_OIDC_TOKEN).await;

    // Create ULID from user id
    let user_ulid = DieselUlid::from_str(USER1_ULID).unwrap();

    // Add permission for another user to project
    fast_track_grpc_permission_add(
        &service_block.auth_service,
        ADMIN_OIDC_TOKEN,
        &user_ulid,
        &DieselUlid::from_str(&project.id).unwrap(),
        DbPermissionLevel::WRITE,
    )
    .await;

    let user = service_block.cache.get_user(&user_ulid).unwrap();
    assert_eq!(
        user.attributes
            .0
            .permissions
            .get(&DieselUlid::from_str(&project.id).unwrap())
            .unwrap()
            .value(),
        &ObjectMapping::PROJECT(DbPermissionLevel::WRITE)
    );

    // Remove permission for user who does not exist
    let mut inner_request = DeleteAuthorizationRequest {
        resource_id: project.id.clone(),
        user_id: DieselUlid::generate().to_string(),
    };

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .delete_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    // Remove permission for resource which does not exist
    inner_request.resource_id = DieselUlid::generate().to_string();
    inner_request.user_id = user_ulid.to_string();

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let _ = service_block
        .auth_service
        .delete_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    // Remove permission without token
    inner_request.resource_id.clone_from(&project.id);

    let grpc_request = tonic::Request::new(inner_request.clone());

    let response = service_block
        .auth_service
        .delete_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    // Remove permission without sufficient permissions
    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER1_OIDC_TOKEN);

    let response = service_block
        .auth_service
        .delete_authorization(grpc_request)
        .await;

    assert!(response.is_err());

    // Remove permission with sufficient permissions
    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), ADMIN_OIDC_TOKEN);

    let _ = service_block
        .auth_service
        .delete_authorization(grpc_request)
        .await
        .unwrap();

    let user = service_block.cache.get_user(&user_ulid).unwrap();

    assert!(user
        .attributes
        .0
        .permissions
        .get(&DieselUlid::from_str(&project.id).unwrap())
        .is_none());
}
