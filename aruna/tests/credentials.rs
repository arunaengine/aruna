mod shared;

use aruna_api::routes::credentials::CreateS3PathRestriction;
use aruna_core::structs::{PathRestriction, Permission, blob_group_permission_path};
use reqwest::StatusCode;
use shared::{
    TestResult, create_bearer_token, create_group_via_http,
    create_s3_credentials_with_restrictions_via_http, get_user_access, sign_scoped_bearer_token,
    spawn_seed_node,
};

fn create_request_restriction(pattern: String, permission: Permission) -> CreateS3PathRestriction {
    CreateS3PathRestriction {
        pattern,
        permission: permission.to_string(),
    }
}

async fn post_credentials(
    base_url: &str,
    bearer_token: &str,
    group_id: &str,
    path_restrictions: Option<Vec<CreateS3PathRestriction>>,
) -> TestResult<reqwest::Response> {
    Ok(reqwest::Client::new()
        .post(format!("{base_url}/api/v1/users/credentials"))
        .bearer_auth(bearer_token)
        .json(
            &aruna_api::routes::credentials::CreateS3CredentialsRequest {
                group_id: group_id.to_string(),
                expires_in_seconds: Some(600),
                path_restrictions,
            },
        )
        .send()
        .await?)
}

#[tokio::test]
async fn scoped_auth_without_request_restrictions_inherits_group_scope() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_via_http(&seed.base_url, &admin_token, "credentials-scope-a").await?;
    let group_root =
        blob_group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
    let delegated_path = format!("{group_root}/folder/**");
    let scoped_token = sign_scoped_bearer_token(
        &seed,
        seed.user_id,
        vec![PathRestriction {
            pattern: delegated_path.clone(),
            permission: Permission::WRITE,
        }],
    )?;

    let credentials = create_s3_credentials_with_restrictions_via_http(
        &seed.base_url,
        &scoped_token,
        &group.group_id,
        None,
    )
    .await?;
    let access = get_user_access(seed.context.as_ref(), &credentials.access_key_id).await?;

    assert_eq!(
        access.path_restrictions,
        Some(vec![PathRestriction {
            pattern: delegated_path,
            permission: Permission::WRITE,
        }])
    );

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn scoped_auth_with_narrower_request_restrictions_stores_narrowed_scope() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_via_http(&seed.base_url, &admin_token, "credentials-scope-b").await?;
    let group_root =
        blob_group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
    let auth_scope = format!("{group_root}/folder/**");
    let request_scope = format!("{group_root}/folder/narrow/**");
    let scoped_token = sign_scoped_bearer_token(
        &seed,
        seed.user_id,
        vec![PathRestriction {
            pattern: auth_scope,
            permission: Permission::WRITE,
        }],
    )?;

    let credentials = create_s3_credentials_with_restrictions_via_http(
        &seed.base_url,
        &scoped_token,
        &group.group_id,
        Some(vec![create_request_restriction(
            request_scope.clone(),
            Permission::WRITE,
        )]),
    )
    .await?;
    let access = get_user_access(seed.context.as_ref(), &credentials.access_key_id).await?;

    assert_eq!(
        access.path_restrictions,
        Some(vec![PathRestriction {
            pattern: request_scope,
            permission: Permission::WRITE,
        }])
    );

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn broader_request_than_auth_scope_is_rejected() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_via_http(&seed.base_url, &admin_token, "credentials-scope-c").await?;
    let group_root =
        blob_group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
    let auth_scope = format!("{group_root}/folder/narrow/**");
    let requested_scope = format!("{group_root}/folder/**");
    let scoped_token = sign_scoped_bearer_token(
        &seed,
        seed.user_id,
        vec![PathRestriction {
            pattern: auth_scope,
            permission: Permission::WRITE,
        }],
    )?;

    let response = post_credentials(
        &seed.base_url,
        &scoped_token,
        &group.group_id,
        Some(vec![create_request_restriction(
            requested_scope,
            Permission::WRITE,
        )]),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn read_only_effective_scope_is_rejected() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_via_http(&seed.base_url, &admin_token, "credentials-scope-d").await?;
    let group_root =
        blob_group_permission_path(seed.realm_id, group.group_id.parse()?, seed.net.node_id());
    let auth_scope = format!("{group_root}/folder/**");
    let scoped_token = sign_scoped_bearer_token(
        &seed,
        seed.user_id,
        vec![PathRestriction {
            pattern: auth_scope.clone(),
            permission: Permission::WRITE,
        }],
    )?;

    let response = post_credentials(
        &seed.base_url,
        &scoped_token,
        &group.group_id,
        Some(vec![create_request_restriction(
            auth_scope,
            Permission::READ,
        )]),
    )
    .await?;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn scoped_token_for_other_group_is_rejected() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group_a =
        create_group_via_http(&seed.base_url, &admin_token, "credentials-scope-e-a").await?;
    let group_b =
        create_group_via_http(&seed.base_url, &admin_token, "credentials-scope-e-b").await?;
    let group_a_root =
        blob_group_permission_path(seed.realm_id, group_a.group_id.parse()?, seed.net.node_id());
    let scoped_token = sign_scoped_bearer_token(
        &seed,
        seed.user_id,
        vec![PathRestriction {
            pattern: format!("{group_a_root}/folder/**"),
            permission: Permission::WRITE,
        }],
    )?;

    let response = post_credentials(&seed.base_url, &scoped_token, &group_b.group_id, None).await?;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    seed.shutdown().await;
    Ok(())
}
