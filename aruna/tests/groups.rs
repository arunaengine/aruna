mod shared;

use aruna_api::routes::groups::{
    AddGroupMemberRequest, CreateGroupRoleRequest, GroupInfoResponse, GroupMembersResponse,
    GroupRolesResponse, RoleResponse,
};
use aruna_core::UserId;
use reqwest::StatusCode;
use shared::{TestResult, create_bearer_token, create_group_via_http, spawn_seed_node};
use std::collections::HashMap;
use ulid::Ulid;

async fn get_members(base_url: &str, token: &str, group_id: &str) -> TestResult<reqwest::Response> {
    Ok(reqwest::Client::new()
        .get(format!("{base_url}/api/v1/groups/{group_id}/members"))
        .bearer_auth(token)
        .send()
        .await?)
}

fn role_by_name<'a>(roles: &'a [RoleResponse], name: &str) -> &'a RoleResponse {
    roles
        .iter()
        .find(|role| role.name == name)
        .unwrap_or_else(|| panic!("role {name} missing"))
}

#[tokio::test]
async fn membership_lifecycle_with_invite_and_leave() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_via_http(&seed.base_url, &admin_token, "membership-flow").await?;

    let member_id = UserId::local(Ulid::new(), seed.realm_id);
    let member_token = create_bearer_token(
        seed.context.as_ref(),
        member_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;

    // Non-members cannot read the member list.
    let response = get_members(&seed.base_url, &member_token, &group.group_id).await?;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    // Admin invites the member with the default "user" role.
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/groups/{}/members",
            seed.base_url, group.group_id
        ))
        .bearer_auth(&admin_token)
        .json(&AddGroupMemberRequest {
            user_id: member_id.to_string(),
            role_ids: None,
        })
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let roles: GroupRolesResponse = response.json().await?;
    let user_role = role_by_name(&roles.roles, "user");
    assert!(
        user_role
            .assigned_users
            .as_ref()
            .unwrap()
            .contains(&member_id.to_string())
    );

    // The new member can now read the member list.
    let response = get_members(&seed.base_url, &member_token, &group.group_id).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let members: GroupMembersResponse = response.json().await?;
    assert_eq!(members.members.len(), 2);

    // The member leaves again.
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/groups/{}/leave",
            seed.base_url, group.group_id
        ))
        .bearer_auth(&member_token)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = get_members(&seed.base_url, &admin_token, &group.group_id).await?;
    let members: GroupMembersResponse = response.json().await?;
    assert_eq!(members.members.len(), 1);
    assert_eq!(members.members[0].user_id, seed.user_id.to_string());

    // The last admin cannot leave their own group.
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/groups/{}/leave",
            seed.base_url, group.group_id
        ))
        .bearer_auth(&admin_token)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn role_management_rejects_foreign_paths_and_protects_admin() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_via_http(&seed.base_url, &admin_token, "role-flow").await?;
    let group_id = &group.group_id;
    let realm_id = seed.realm_id;

    // A permission path outside the group is privilege escalation.
    let response = reqwest::Client::new()
        .post(format!("{}/api/v1/groups/{group_id}/roles", seed.base_url))
        .bearer_auth(&admin_token)
        .json(&CreateGroupRoleRequest {
            name: "escalation".to_string(),
            permissions: HashMap::from([(format!("/{realm_id}/admin/**"), "write".to_string())]),
            assigned_users: Vec::new(),
        })
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // A valid scoped role can be created and deleted.
    let response = reqwest::Client::new()
        .post(format!("{}/api/v1/groups/{group_id}/roles", seed.base_url))
        .bearer_auth(&admin_token)
        .json(&CreateGroupRoleRequest {
            name: "data-reader".to_string(),
            permissions: HashMap::from([(
                format!("/{realm_id}/g/{group_id}/data/**"),
                "read".to_string(),
            )]),
            assigned_users: Vec::new(),
        })
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    let role: RoleResponse = response.json().await?;
    assert_eq!(role.name, "data-reader");

    let response = reqwest::Client::new()
        .delete(format!(
            "{}/api/v1/groups/{group_id}/roles/{}",
            seed.base_url, role.role_id
        ))
        .bearer_auth(&admin_token)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // The admin role is undeletable.
    let admin_role_id = role_by_name(&group.roles, "admin").role_id.clone();
    let response = reqwest::Client::new()
        .delete(format!(
            "{}/api/v1/groups/{group_id}/roles/{admin_role_id}",
            seed.base_url
        ))
        .bearer_auth(&admin_token)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn open_group_endpoints_hide_member_lists_from_non_members() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    let group = create_group_via_http(&seed.base_url, &admin_token, "privacy-flow").await?;

    let outsider_token = create_bearer_token(
        seed.context.as_ref(),
        UserId::local(Ulid::new(), seed.realm_id),
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;

    let response = reqwest::Client::new()
        .get(format!(
            "{}/api/v1/groups/{}",
            seed.base_url, group.group_id
        ))
        .bearer_auth(&outsider_token)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let info: GroupInfoResponse = response.json().await?;
    assert!(!info.roles.is_empty());
    assert!(info.roles.iter().all(|role| role.assigned_users.is_none()));

    // Members keep seeing the assignments.
    let response = reqwest::Client::new()
        .get(format!(
            "{}/api/v1/groups/{}",
            seed.base_url, group.group_id
        ))
        .bearer_auth(&admin_token)
        .send()
        .await?;
    let info: GroupInfoResponse = response.json().await?;
    assert!(
        info.roles.iter().any(|role| role.assigned_users.is_some()
            && !role.assigned_users.as_ref().unwrap().is_empty())
    );

    seed.shutdown().await;
    Ok(())
}
