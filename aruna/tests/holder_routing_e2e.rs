// End-to-end proof of the #263 Definition of Done: create on node A and read on
// node B works through real REST for every document-scoped endpoint, with the
// cross-node hop served by holder routing rather than by local replication.
//
// A two-node realm at the default replica factor (3) has both nodes hold every
// shard, so a read on B always serves locally and routing is never exercised. To
// make routing REAL, the realm's default placement strategy is pinned to
// replica_count = 1 before the group/user work, so each group and user shard has
// exactly one holder; the queried node is deliberately the NON-holder, so its
// answer can only have come over the holder proxy. Metadata registry records ride
// their own everywhere strategy (untouched), so metadata proves the
// registry-everywhere read path; the notification inbox is replica-1 by
// construction, so listing from the non-holder always routes.
mod shared;

use aruna_api::routes::groups::{AddGroupMemberRequest, CreateGroupResponse};
use aruna_api::routes::metadata::{CreateMetadataRequest, CreateMetadataScaffoldRequest};
use aruna_api::routes::notifications::NotificationListResponse;
use aruna_api::routes::users::{GetUserResponse, UpdateUserRequest};
use aruna_core::NodeId;
use aruna_core::UserId;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{REALM_CONFIG_KEYSPACE, USER_KEYSPACE};
use aruna_core::onboarding::OnboardingMode;
use aruna_core::structs::{
    Actor, NodeCapabilities, NotificationClass, NotificationKind, NotificationRecord,
    RealmConfigDocument, TokenClaims, User,
};
use aruna_operations::driver::drive;
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::notifications::inbox::upsert_inbox_records;
use aruna_operations::notifications::placement::resolve_inbox_holder;
use aruna_operations::placement::{placement_ref_for_target, resolve_shard_holders};
use aruna_storage::StorageHandle;
use byteview::ByteView;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::StatusCode;
use shared::{
    JoinerNode, SeedNode, TestResult, create_bearer_token, create_group_via_http,
    create_onboarding_secret_via_http, get_group_via_http, spawn_full_joiner_node,
    spawn_full_seed_node, wait_for_realm_nodes, wait_until,
};
use std::collections::HashMap;
use std::time::Duration;
use ulid::Ulid;

fn to_err(error: impl std::fmt::Display) -> Box<dyn std::error::Error> {
    std::io::Error::other(error.to_string()).into()
}

#[tokio::test]
async fn holder_routing_end_to_end_through_rest() -> TestResult<()> {
    let seed = spawn_full_seed_node().await?;
    let onboarding_secret = create_onboarding_secret_via_http(&seed, OnboardingMode::Local).await?;
    let joiner = spawn_full_joiner_node(&seed, onboarding_secret).await?;
    wait_for_realm_nodes(
        &[seed.context.as_ref(), joiner.context.as_ref()],
        &seed.realm_id,
        2,
    )
    .await?;

    let seed_token = create_bearer_token(
        seed.context.as_ref(),
        seed.user_id,
        seed.realm_id,
        seed.capabilities.clone(),
    )
    .await?;

    // Metadata rides the everywhere registry strategy and (still at replica 3) an
    // everywhere graph too, so create-on-A / read-on-B is proven here before the
    // replica-1 switch narrows the group/user graph placements.
    metadata_phase(&seed, &joiner, &seed_token).await?;

    // Pin the realm default strategy to a single replica on both nodes so each
    // group/user shard has exactly one holder and the reads that follow must route.
    let config = install_replica_one(&seed, &joiner).await?;
    let seed_node = seed.net.node_id();
    let joiner_node = joiner.config.node_id;

    group_phase(&seed, &joiner, &seed_token, &config, seed_node).await?;
    user_phase(&seed, &joiner, &seed_token, &config).await?;
    notification_phase(&seed, &joiner, &seed_token, &config, seed_node).await?;

    // 503 path: a routed call whose only holder is the joiner returns Service
    // Unavailable with Retry-After once that holder is stopped.
    let orphan_group = group_id_with_holder(&config, joiner_node);
    joiner.shutdown().await;
    unavailable_phase(&seed, &seed_token, orphan_group).await?;

    seed.shutdown().await;
    Ok(())
}

// Writes a replica-1 default strategy into both nodes' realm config so exactly
// one node holds each group/user shard; the everywhere registry/admin strategy is
// left untouched.
async fn install_replica_one(
    seed: &SeedNode,
    joiner: &JoinerNode,
) -> TestResult<RealmConfigDocument> {
    let realm_id = seed.realm_id;
    let mut config = drive(
        GetRealmConfigOperation::new(realm_id),
        seed.context.as_ref(),
    )
    .await
    .map_err(to_err)?;
    let default_id = config
        .default_strategy_id
        .ok_or_else(|| to_err("realm has no default strategy"))?;
    let strategy = config
        .strategies
        .iter_mut()
        .find(|strategy| strategy.strategy_id == default_id)
        .ok_or_else(|| to_err("default strategy missing from realm config"))?;
    strategy.replica_count = Some(1);

    for (context, node_id) in [
        (seed.context.as_ref(), seed.net.node_id()),
        (joiner.context.as_ref(), joiner.config.node_id),
    ] {
        let actor = Actor {
            node_id,
            user_id: UserId::nil(realm_id),
            realm_id,
        };
        let bytes = config.to_bytes(&actor).map_err(to_err)?;
        write_realm_config(&context.storage_handle, realm_id.as_bytes().to_vec(), bytes).await?;
    }
    seed.net
        .refresh_realm_peers_from_document(&config)
        .await
        .map_err(to_err)?;
    joiner
        .net
        .refresh_realm_peers_from_document(&config)
        .await
        .map_err(to_err)?;
    Ok(config)
}

async fn write_realm_config(
    storage: &StorageHandle,
    key: Vec<u8>,
    value: Vec<u8>,
) -> TestResult<()> {
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: REALM_CONFIG_KEYSPACE.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(to_err(format!(
            "unexpected realm config write event: {other:?}"
        ))),
    }
}

// Create-on-A / read-on-B for a group. Groups are created on the seed until one
// whose single holder is the seed itself; the read is then served on the joiner
// (a non-holder) by routing back to the seed, before any local replication, so
// the immediate success can only be the holder proxy at work.
async fn group_phase(
    seed: &SeedNode,
    joiner: &JoinerNode,
    token: &str,
    config: &RealmConfigDocument,
    seed_node: NodeId,
) -> TestResult<()> {
    let group =
        create_seed_held_group(seed, token, config, seed_node, "holder-routing-group").await?;

    // Immediate single-shot read on the non-holder joiner: routes to the seed.
    let fetched = get_group_via_http(&joiner.base_url, token, &group.group_id).await?;
    assert_eq!(fetched.group_id, group.group_id);
    assert_eq!(fetched.display_name, group.display_name);
    Ok(())
}

// Creates groups on the seed until one whose sole replica-1 holder is the seed
// itself. The seed then holds the group and its authorization document locally,
// so routed reads/writes converge deterministically rather than racing the
// group's replication to a remote holder.
async fn create_seed_held_group(
    seed: &SeedNode,
    token: &str,
    config: &RealmConfigDocument,
    seed_node: NodeId,
    name_prefix: &str,
) -> TestResult<CreateGroupResponse> {
    for attempt in 0..40 {
        let group =
            create_group_via_http(&seed.base_url, token, &format!("{name_prefix}-{attempt}"))
                .await?;
        let group_id = Ulid::from_string(&group.group_id).map_err(to_err)?;
        if group_holder(config, group_id) == seed_node {
            return Ok(group);
        }
    }
    Err(to_err(
        "no group hashed to the seed holder within the retry bound",
    ))
}

// Update-on-A / read-on-B for a user document. The bootstrap admin has no User
// document (only OIDC registration mints one), so a fresh user is seeded on its
// sole replica-1 holder. Both the routed update and the routed read are issued
// from the NON-holder with the realm-admin bearer, so each crosses the holder
// proxy to the node that actually holds the document.
async fn user_phase(
    seed: &SeedNode,
    joiner: &JoinerNode,
    token: &str,
    config: &RealmConfigDocument,
) -> TestResult<()> {
    let realm_id = seed.realm_id;
    let target = UserId::new(Ulid::new(), realm_id);
    let holder = user_holder(config, target);
    let (holder_context, holder_node, non_holder) = if holder == seed.net.node_id() {
        (seed.context.as_ref(), seed.net.node_id(), &joiner.base_url)
    } else {
        (
            joiner.context.as_ref(),
            joiner.config.node_id,
            &seed.base_url,
        )
    };
    seed_user_document(
        &holder_context.storage_handle,
        holder_node,
        realm_id,
        target,
    )
    .await?;

    let user_id = target.to_string();
    let key = "holder-routing-attr";
    let value = "routed-value";

    let updated = patch_user(non_holder, token, &user_id, key, value).await?;
    assert_eq!(updated.attributes.get(key).map(String::as_str), Some(value));

    let fetched = get_user(non_holder, token, &user_id).await?;
    assert_eq!(fetched.attributes.get(key).map(String::as_str), Some(value));
    Ok(())
}

async fn seed_user_document(
    storage: &StorageHandle,
    node_id: NodeId,
    realm_id: aruna_core::structs::RealmId,
    user_id: UserId,
) -> TestResult<()> {
    let user = User {
        user_id,
        name: "Routed User".to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: Default::default(),
    };
    let actor = Actor {
        node_id,
        user_id,
        realm_id,
    };
    let bytes = user.to_bytes(&actor).map_err(to_err)?;
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: USER_KEYSPACE.to_string(),
            key: ByteView::from(user_id.to_bytes()),
            value: ByteView::from(bytes),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => Ok(()),
        other => Err(to_err(format!("unexpected user write event: {other:?}"))),
    }
}

// Create-on-A / read-on-B for metadata. A scaffold document and a `profiles/`
// document are created on the seed; the joiner reads each back once the
// everywhere registry record materializes locally.
async fn metadata_phase(seed: &SeedNode, joiner: &JoinerNode, token: &str) -> TestResult<()> {
    let group = create_group_via_http(&seed.base_url, token, "holder-routing-meta-group").await?;

    for path in ["datasets/routed-run", "profiles/routed-profile"] {
        let created =
            create_metadata_scaffold(&seed.base_url, token, &group.group_id, path).await?;
        wait_until(
            &format!("metadata `{path}` visible on the joiner"),
            Duration::from_secs(20),
            Duration::from_millis(150),
            || {
                let url = joiner.base_url.clone();
                let token = token.to_string();
                let document_id = created.clone();
                async move { metadata_get_status(&url, &token, &document_id).await == StatusCode::OK }
            },
        )
        .await?;
    }
    Ok(())
}

// A real group-membership change emits an inbox notification for the added user;
// listing it via the node that does NOT hold that user's (replica-1) inbox routes
// the read to the inbox holder.
async fn notification_phase(
    seed: &SeedNode,
    joiner: &JoinerNode,
    token: &str,
    config: &RealmConfigDocument,
    seed_node: NodeId,
) -> TestResult<()> {
    // The group is held by the seed, so the routed add-member call converges on a
    // node that already holds the group authorization document.
    let group =
        create_seed_held_group(seed, token, config, seed_node, "holder-routing-notify").await?;
    let user_role = group
        .roles
        .iter()
        .find(|role| role.name == "user")
        .map(|role| role.role_id.clone())
        .ok_or_else(|| to_err("group has no user role"))?;

    let member = UserId::new(Ulid::new(), seed.realm_id);
    add_group_member(
        &seed.base_url,
        token,
        &group.group_id,
        &member.to_string(),
        &user_role,
    )
    .await?;

    let inbox_holder = inbox_holder(config, member);
    let non_holder = if inbox_holder == seed.net.node_id() {
        &joiner.base_url
    } else {
        &seed.base_url
    };
    let member_token = mint_unrestricted_bearer(seed, member)?;

    wait_until(
        "routed notification list surfaces the membership notification",
        Duration::from_secs(30),
        Duration::from_millis(200),
        || {
            let url = non_holder.to_string();
            let token = member_token.clone();
            async move {
                list_notifications(&url, &token)
                    .await
                    .map(|list| !list.notifications.is_empty())
                    .unwrap_or(false)
            }
        },
    )
    .await?;

    let listed = list_notifications(non_holder, &member_token).await?;
    assert!(
        listed
            .notifications
            .iter()
            .any(|entry| entry.group_id.as_deref() == Some(group.group_id.as_str())),
        "the routed list must carry the membership notification: {:?}",
        listed.notifications
    );

    // The inbox holder derives the recipient from the bearer, never a wire claim:
    // an inbox record for a different user seeded on the holder never leaks here.
    let intruder = NotificationRecord {
        notification_id: Ulid::new(),
        recipient: UserId::new(Ulid::new(), seed.realm_id),
        class: NotificationClass::Transient,
        kind: NotificationKind::AddedToGroup {
            group_id: Ulid::new(),
            actor_user_id: seed.user_id,
        },
        created_at_ms: aruna_core::util::unix_timestamp_millis(),
        read_at_ms: None,
    };
    let holder_context = if inbox_holder == seed.net.node_id() {
        seed.context.as_ref()
    } else {
        joiner.context.as_ref()
    };
    upsert_inbox_records(
        &holder_context.storage_handle,
        std::slice::from_ref(&intruder),
    )
    .await
    .map_err(to_err)?;
    let listed = list_notifications(non_holder, &member_token).await?;
    assert!(
        listed
            .notifications
            .iter()
            .all(|entry| entry.id != intruder.notification_id.to_string()),
        "another user's inbox record must never leak to the member's routed list"
    );
    Ok(())
}

// A routed call whose sole holder is unreachable answers 503 with Retry-After.
async fn unavailable_phase(seed: &SeedNode, token: &str, orphan_group: Ulid) -> TestResult<()> {
    let response = reqwest::Client::new()
        .get(format!("{}/api/v1/groups/{}", seed.base_url, orphan_group))
        .bearer_auth(token)
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        response
            .headers()
            .contains_key(reqwest::header::RETRY_AFTER),
        "a 503 from an unreachable holder must carry Retry-After"
    );
    Ok(())
}

fn group_holder(config: &RealmConfigDocument, group_id: Ulid) -> NodeId {
    let placement = placement_ref_for_target(config, &DocumentSyncTarget::Group { group_id }, None);
    resolve_shard_holders(config, &placement)
        .into_iter()
        .next()
        .expect("group shard resolves a holder under replica 1")
}

fn user_holder(config: &RealmConfigDocument, user_id: UserId) -> NodeId {
    let placement = placement_ref_for_target(config, &DocumentSyncTarget::User { user_id }, None);
    resolve_shard_holders(config, &placement)
        .into_iter()
        .next()
        .expect("user shard resolves a holder under replica 1")
}

fn inbox_holder(config: &RealmConfigDocument, user_id: UserId) -> NodeId {
    resolve_inbox_holder(&user_id, config)
        .ok()
        .flatten()
        .expect("inbox resolves a holder")
}

fn group_id_with_holder(config: &RealmConfigDocument, target: NodeId) -> Ulid {
    for _ in 0..100_000 {
        let group_id = Ulid::new();
        if group_holder(config, group_id) == target {
            return group_id;
        }
    }
    panic!("no group hashed to the target holder within the sampling bound");
}

fn mint_unrestricted_bearer(seed: &SeedNode, user_id: UserId) -> TestResult<String> {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    let claims = TokenClaims {
        sub: user_id.to_string(),
        iss: seed.realm_id.to_string(),
        iat: now,
        exp: now + 600,
        jti: Ulid::new().to_string(),
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
    };
    let NodeCapabilities::Management {
        realm_encoding_key, ..
    } = &seed.capabilities
    else {
        return Err(to_err("seed node must use management capabilities"));
    };
    Ok(encode(
        &Header::new(Algorithm::EdDSA),
        &claims,
        &EncodingKey::from_ed_pem(realm_encoding_key)?,
    )?)
}

async fn patch_user(
    base_url: &str,
    token: &str,
    user_id: &str,
    key: &str,
    value: &str,
) -> TestResult<GetUserResponse> {
    let response = reqwest::Client::new()
        .patch(format!("{base_url}/api/v1/users/{user_id}"))
        .bearer_auth(token)
        .json(&UpdateUserRequest {
            name: None,
            set_attributes: HashMap::from([(key.to_string(), value.to_string())]),
            remove_attributes: Vec::new(),
        })
        .send()
        .await?;
    if response.status() != StatusCode::OK {
        return Err(to_err(format!(
            "unexpected patch user status: {}",
            response.status()
        )));
    }
    Ok(response.json().await?)
}

async fn get_user(base_url: &str, token: &str, user_id: &str) -> TestResult<GetUserResponse> {
    let response = reqwest::Client::new()
        .get(format!("{base_url}/api/v1/users/{user_id}"))
        .bearer_auth(token)
        .send()
        .await?;
    if response.status() != StatusCode::OK {
        return Err(to_err(format!(
            "unexpected get user status: {}",
            response.status()
        )));
    }
    Ok(response.json().await?)
}

async fn create_metadata_scaffold(
    base_url: &str,
    token: &str,
    group_id: &str,
    path: &str,
) -> TestResult<String> {
    let request = CreateMetadataRequest::Scaffold(CreateMetadataScaffoldRequest {
        group_id: group_id.to_string(),
        path: path.to_string(),
        name: "Routed Document".to_string(),
        description: "Created on A, read on B".to_string(),
        date_published: "2026-07-07".to_string(),
        license: "https://creativecommons.org/licenses/by/4.0/".to_string(),
        public: true,
    });
    let response = reqwest::Client::new()
        .post(format!("{base_url}/api/v1/metadata"))
        .bearer_auth(token)
        .json(&request)
        .send()
        .await?;
    if response.status() != StatusCode::CREATED {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(to_err(format!(
            "unexpected create metadata status: {status} body={body}"
        )));
    }
    let value: serde_json::Value = response.json().await?;
    value
        .get("document_id")
        .and_then(|id| id.as_str())
        .map(str::to_string)
        .ok_or_else(|| to_err("create metadata response missing document_id"))
}

async fn metadata_get_status(base_url: &str, token: &str, document_id: &str) -> StatusCode {
    reqwest::Client::new()
        .get(format!("{base_url}/api/v1/metadata/{document_id}"))
        .bearer_auth(token)
        .send()
        .await
        .map(|response| response.status())
        .unwrap_or(StatusCode::BAD_GATEWAY)
}

async fn add_group_member(
    base_url: &str,
    token: &str,
    group_id: &str,
    user_id: &str,
    role_id: &str,
) -> TestResult<()> {
    let response = reqwest::Client::new()
        .post(format!("{base_url}/api/v1/groups/{group_id}/members"))
        .bearer_auth(token)
        .json(&AddGroupMemberRequest {
            user_id: user_id.to_string(),
            role_ids: Some(vec![role_id.to_string()]),
        })
        .send()
        .await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(to_err(format!(
            "unexpected add member status: {status} body={body}"
        )));
    }
    Ok(())
}

async fn list_notifications(base_url: &str, token: &str) -> TestResult<NotificationListResponse> {
    let response = reqwest::Client::new()
        .get(format!("{base_url}/api/v1/notifications"))
        .bearer_auth(token)
        .send()
        .await?;
    if response.status() != StatusCode::OK {
        return Err(to_err(format!(
            "unexpected list notifications status: {}",
            response.status()
        )));
    }
    Ok(response.json().await?)
}
