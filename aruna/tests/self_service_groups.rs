mod shared;

use aruna_api::routes::groups::CreateGroupRequest;
use aruna_core::UserId;
use reqwest::StatusCode;
use shared::{TestResult, create_bearer_token, spawn_seed_node};
use ulid::Ulid;

async fn post_group(base_url: &str, token: &str, name: &str) -> TestResult<reqwest::Response> {
    Ok(reqwest::Client::new()
        .post(format!("{base_url}/api/v1/groups"))
        .bearer_auth(token)
        .json(&CreateGroupRequest {
            name: name.to_string(),
        })
        .send()
        .await?)
}

#[tokio::test]
async fn fresh_user_creates_groups_up_to_the_cap() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let member_token = create_bearer_token(
        seed.context.as_ref(),
        UserId::local(Ulid::new(), seed.realm_id),
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;

    // The default realm quota caps self-service creation at 3 owned groups.
    for i in 0..3 {
        let response = post_group(&seed.base_url, &member_token, &format!("workspace-{i}")).await?;
        assert_eq!(response.status(), StatusCode::CREATED, "create {i}");
    }
    let response = post_group(&seed.base_url, &member_token, "workspace-over-cap").await?;
    assert_eq!(response.status(), StatusCode::CONFLICT);

    // Realm admins are exempt from the cap.
    let admin_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;
    for i in 0..4 {
        let response = post_group(&seed.base_url, &admin_token, &format!("admin-{i}")).await?;
        assert_eq!(response.status(), StatusCode::CREATED, "admin create {i}");
    }

    seed.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn concurrent_creates_cannot_slip_past_the_cap() -> TestResult<()> {
    let seed = spawn_seed_node().await?;
    let member_token = create_bearer_token(
        seed.context.as_ref(),
        UserId::local(Ulid::new(), seed.realm_id),
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;

    let attempts: Vec<_> = (0..6)
        .map(|i| {
            let base_url = seed.base_url.clone();
            let token = member_token.clone();
            tokio::spawn(async move {
                match post_group(&base_url, &token, &format!("race-{i}")).await {
                    Ok(response) => Ok(response.status()),
                    Err(error) => Err(error.to_string()),
                }
            })
        })
        .collect();
    let mut statuses = Vec::new();
    for attempt in attempts {
        statuses.push(attempt.await?);
    }

    let created = statuses
        .iter()
        .filter(|status| matches!(status, Ok(status) if *status == StatusCode::CREATED))
        .count();
    let conflicts = statuses
        .iter()
        .filter(|status| matches!(status, Ok(status) if *status == StatusCode::CONFLICT))
        .count();
    let unexpected: Vec<_> = statuses
        .iter()
        .filter(|status| {
            !matches!(
                status,
                Ok(status) if *status == StatusCode::CREATED || *status == StatusCode::CONFLICT
            )
        })
        .collect();
    assert!(unexpected.is_empty(), "unexpected responses: {statuses:?}");
    assert!(created >= 1, "at least one create succeeds: {statuses:?}");
    assert!(created <= 3, "cap held under concurrency: {statuses:?}");
    assert_eq!(
        conflicts,
        statuses.len() - created,
        "failed concurrent attempts return conflict: {statuses:?}"
    );

    seed.shutdown().await;
    Ok(())
}
