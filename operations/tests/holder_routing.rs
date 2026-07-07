use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use aruna_core::NodeId;
use aruna_core::auth::TRUSTED_REALMS_LIST_KEY;
use aruna_core::document::DocumentSyncTarget;
use aruna_core::effects::StorageEffect;
use aruna_core::events::{Event, StorageEvent};
use aruna_core::keyspaces::{
    API_STATE_KEYSPACE, AUTH_KEYSPACE, GROUP_KEYSPACE, METADATA_DOCUMENT_INDEX_KEYSPACE,
    REALM_CONFIG_KEYSPACE, USER_KEYSPACE,
};
use aruna_core::storage_entries::metadata_document_key;
use aruna_core::structs::{
    Actor, BindingScope, DEFAULT_SHARD_COUNT, DocumentClass, Group, GroupAuthorizationDocument,
    MetadataRegistryRecord, NotificationClass, NotificationKind, NotificationRecord, Permission,
    PlacementOverride, PlacementStrategy, RealmAuthorizationDocument, RealmConfigDocument, RealmId,
    RealmNodeKind, Role, StrategyBinding, TokenClaims, User,
};
use aruna_core::types::UserId;
use aruna_core::util::unix_timestamp_millis;
use aruna_net::{DiscoveryMethod, NetConfig, NetHandle, RelayMethod};
use aruna_operations::driver::DriverContext;
use aruna_operations::incoming::initialize_net_incoming;
use aruna_operations::notifications::inbox::upsert_inbox_records;
use aruna_operations::notifications::placement::resolve_inbox_holder;
use aruna_operations::placement::{
    placement_ref_for_target, resolve_shard_holders, shard_subject_bytes,
};
use aruna_operations::process_placements::process_shard_placements;
use aruna_operations::routing::dispatch::{HolderRoutingError, dispatch_holder_call};
use aruna_operations::routing::protocol::{
    GroupCall, GroupReply, MetadataCall, MetadataMutation, NotificationCall, NotificationReply,
    ProxiedCall, ProxiedReply, UserCall, UserReply,
};
use aruna_operations::task_incoming::initialize_task_incoming;
use aruna_storage::{FjallStorage, StorageHandle};
use aruna_tasks::TaskHandle;
use byteview::ByteView;
use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::EncodePrivateKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use tempfile::TempDir;
use ulid::Ulid;

struct TestNode {
    _temp_dir: TempDir,
    net: NetHandle,
    context: Arc<DriverContext>,
}

// A non-holder origin proxies a group read to the shard's holder, round-tripping
// the document with a valid bearer and being refused with an invalid one.
#[tokio::test]
async fn non_holder_dispatch_round_trips_group_and_refuses_invalid_bearer() {
    let (realm_key, realm_id, user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 2).await;
    let (origin, holder) = (&nodes[0], &nodes[1]);

    let config = config_with_replica(realm_id, &node_ids(&nodes), 1);
    install_config(&nodes, &config).await;
    persist_trusted_realm(holder, realm_id).await;

    // A group whose single holder is the other node, so the origin must forward.
    let (group_id, holders) =
        sample_group(&config, |h| h.len() == 1 && h[0] == holder.net.node_id());
    assert!(!holders.contains(&origin.net.node_id()));

    let (group, authorization) = make_group(group_id, realm_id, user_id);
    write_group(holder, &group, &authorization).await;

    let token = sign_token(&realm_key, &token_claims(realm_id, user_id));
    let reply = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Group(GroupCall::Get { group_id }),
    )
    .await
    .expect("valid bearer round-trips the group");
    let ProxiedReply::Group(reply) = reply else {
        panic!("expected a group reply");
    };
    let GroupReply::Document {
        group: fetched,
        authorization: fetched_auth,
    } = *reply
    else {
        panic!("expected a group document reply");
    };
    assert_eq!(fetched, group);
    assert_eq!(fetched_auth, authorization);

    let error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some("not-a-valid-jwt"),
        ProxiedCall::Group(GroupCall::Get { group_id }),
    )
    .await
    .expect_err("invalid bearer is refused by the holder");
    assert!(
        matches!(error, HolderRoutingError::Unauthorized(_)),
        "unexpected error: {error:?}"
    );

    shutdown(nodes).await;
}

// Every holder of the shard is unreachable: dispatch reports Unavailable.
#[tokio::test]
async fn all_holders_unreachable_is_unavailable() {
    let (_realm_key, realm_id, _user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 1).await;
    let origin = &nodes[0];

    // The sole holder of some shards is a phantom node the origin cannot dial.
    let phantom = iroh::SecretKey::from_bytes(&[201u8; 32]).public();
    let config = config_with_replica(realm_id, &[origin.net.node_id(), phantom], 1);
    install_config(&nodes, &config).await;

    let (group_id, holders) = sample_group(&config, |h| h.len() == 1 && h[0] == phantom);
    assert!(!holders.contains(&origin.net.node_id()));

    let error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some("not-a-valid-jwt"),
        ProxiedCall::Group(GroupCall::Get { group_id }),
    )
    .await
    .expect_err("no reachable holder");
    assert!(
        matches!(error, HolderRoutingError::Unavailable),
        "unexpected error: {error:?}"
    );

    shutdown(nodes).await;
}

// The origin contacts holders in resolver rank order: the document lives only on
// the rank-1 holder, yet the rank-0 holder is reached first and its authoritative
// NotFound stops the walk.
#[tokio::test]
async fn dispatch_respects_rank_order() {
    let (realm_key, realm_id, user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 3).await;
    let origin = &nodes[0];

    let config = config_with_replica(realm_id, &node_ids(&nodes), 2);
    install_config(&nodes, &config).await;
    for node in &nodes[1..] {
        persist_trusted_realm(node, realm_id).await;
    }

    // Two holders, neither of them the origin.
    let (group_id, holders) = sample_group(&config, |h| {
        h.len() == 2 && !h.contains(&origin.net.node_id())
    });
    let placement =
        placement_ref_for_target(&config, &DocumentSyncTarget::Group { group_id }, None);
    assert_eq!(holders, resolve_shard_holders(&config, &placement));

    // Seed the document only on the rank-1 holder.
    let rank_one = node_by_id(&nodes, holders[1]);
    let (group, authorization) = make_group(group_id, realm_id, user_id);
    write_group(rank_one, &group, &authorization).await;

    let token = sign_token(&realm_key, &token_claims(realm_id, user_id));
    let error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Group(GroupCall::Get { group_id }),
    )
    .await
    .expect_err("the rank-0 holder is reached first and lacks the document");
    assert!(
        matches!(error, HolderRoutingError::NotFound),
        "unexpected error: {error:?}"
    );

    shutdown(nodes).await;
}

// A routed group write (a role mutation) is carried, with its role payload, to
// the shard holder, which validates the forwarded bearer and drives the write
// operation locally. The holder holds no authorization document for the sampled
// group, so the driven mutation resolves to an authoritative NotFound — proving
// the write reached the holder's operation rather than stranding in the origin's
// outbox.
#[tokio::test]
async fn group_add_role_write_routes_to_holder() {
    let (realm_key, realm_id, user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 2).await;
    let (origin, holder) = (&nodes[0], &nodes[1]);

    let config = config_with_replica(realm_id, &node_ids(&nodes), 1);
    install_config(&nodes, &config).await;
    persist_trusted_realm(holder, realm_id).await;

    let (group_id, holders) =
        sample_group(&config, |h| h.len() == 1 && h[0] == holder.net.node_id());
    assert!(!holders.contains(&origin.net.node_id()));

    let role = Role {
        role_id: Ulid::new(),
        name: "auditor".to_string(),
        permissions: HashMap::from([(
            format!("/{realm_id}/g/{group_id}/meta/**"),
            Permission::READ,
        )]),
        assigned_users: HashSet::new(),
    };

    let token = sign_token(&realm_key, &token_claims(realm_id, user_id));
    let error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Group(GroupCall::AddRole {
            group_id,
            role: Box::new(role),
        }),
    )
    .await
    .expect_err("a routed write to a group the holder lacks resolves to NotFound");
    assert!(
        matches!(error, HolderRoutingError::NotFound),
        "unexpected error: {error:?}"
    );

    shutdown(nodes).await;
}

// A non-holder origin proxies a self user-document read to the inbox's shard
// holder; the target serves the identity carried by the forwarded bearer.
#[tokio::test]
async fn user_read_document_routes_to_holder() {
    let (realm_key, realm_id, _user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 2).await;
    let (origin, holder) = (&nodes[0], &nodes[1]);

    let config = config_with_replica(realm_id, &node_ids(&nodes), 1);
    install_config(&nodes, &config).await;
    persist_trusted_realm(holder, realm_id).await;

    let (target_user, holders) = sample_user(&config, realm_id, |h| {
        h.len() == 1 && h[0] == holder.net.node_id()
    });
    assert!(!holders.contains(&origin.net.node_id()));

    let user = make_user(target_user, "Alice");
    write_user(holder, &user).await;

    let token = sign_token(&realm_key, &token_claims(realm_id, target_user));
    let reply = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::User(UserCall::ReadDocument {
            user_id: target_user,
        }),
    )
    .await
    .expect("valid bearer round-trips the user document");
    let ProxiedReply::User(reply) = reply else {
        panic!("expected a user reply");
    };
    let UserReply::User(fetched) = *reply else {
        panic!("expected a user document");
    };
    assert_eq!(fetched, user);

    shutdown(nodes).await;
}

// A stale holder view: the origin's config pins the shard at a node that no
// longer holds it. That target answers NotHolder from its own view and never
// re-forwards, so the true holder (which alone has the document) is never
// contacted. The origin refreshes its config once, sees the same stale view,
// and ends Unavailable rather than serving the document.
#[tokio::test]
async fn stale_holder_view_never_reforwards_and_ends_unavailable() {
    let (realm_key, realm_id, user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 3).await;
    let (origin, target, truth) = (&nodes[0], &nodes[1], &nodes[2]);

    // One base config so both views share the strategy id and shard identity;
    // only the pinned holder differs between them.
    let base = config_with_replica(realm_id, &node_ids(&nodes), 1);
    let group_id = Ulid::new();
    let placement = placement_ref_for_target(&base, &DocumentSyncTarget::Group { group_id }, None);
    let subject = shard_subject_bytes(&placement);

    let mut origin_config = base.clone();
    origin_config.placement_overrides = vec![pin_shard(&subject, target.net.node_id())];
    let mut target_config = base.clone();
    target_config.placement_overrides = vec![pin_shard(&subject, truth.net.node_id())];

    install_config_for(origin, &origin_config).await;
    install_config_for(target, &target_config).await;
    persist_trusted_realm(target, realm_id).await;

    // The document lives ONLY on the true holder. Were the target to re-forward,
    // the origin would receive it; it never does.
    let (group, authorization) = make_group(group_id, realm_id, user_id);
    write_group(truth, &group, &authorization).await;

    let token = sign_token(&realm_key, &token_claims(realm_id, user_id));
    let error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Group(GroupCall::Get { group_id }),
    )
    .await
    .expect_err("a stale holder view exhausts the refresh-once retry");
    assert!(
        matches!(error, HolderRoutingError::Unavailable),
        "unexpected error: {error:?}"
    );

    shutdown(nodes).await;
}

// A reachable rank-0 holder that lacks the document answers an authoritative
// NotFound and the walk stops there. The rank-1 holder is an unreachable
// phantom, so a fall-through past the NotFound would surface Unavailable
// instead — asserting NotFound proves the 404 is definitive.
#[tokio::test]
async fn reachable_holder_missing_document_is_definitive_not_found() {
    let (realm_key, realm_id, user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 2).await;
    let (origin, rank_zero) = (&nodes[0], &nodes[1]);
    let phantom = iroh::SecretKey::from_bytes(&[202u8; 32]).public();

    let config = config_with_replica(
        realm_id,
        &[origin.net.node_id(), rank_zero.net.node_id(), phantom],
        2,
    );
    install_config(&nodes, &config).await;
    persist_trusted_realm(rank_zero, realm_id).await;

    // Holders are exactly the reachable rank-0 node then the unreachable phantom.
    let (group_id, holders) = sample_group(&config, |h| {
        h.len() == 2 && h[0] == rank_zero.net.node_id() && h[1] == phantom
    });
    assert!(!holders.contains(&origin.net.node_id()));

    // No document is seeded, so the reachable holder answers NotFound.
    let token = sign_token(&realm_key, &token_claims(realm_id, user_id));
    let error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Group(GroupCall::Get { group_id }),
    )
    .await
    .expect_err("a reachable holder without the document is a definitive 404");
    assert!(
        matches!(error, HolderRoutingError::NotFound),
        "unexpected error: {error:?}"
    );

    shutdown(nodes).await;
}

// A routed group membership write from a non-holder lands on the holder's
// operation, is authorized against the forwarded bearer, and mutates the
// authorization document; a subsequent routed read observes the new member.
#[tokio::test]
async fn group_add_member_write_routes_and_is_readable() {
    let (realm_key, realm_id, owner) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 2).await;
    let (origin, holder) = (&nodes[0], &nodes[1]);

    let config = config_with_replica(realm_id, &node_ids(&nodes), 1);
    install_config_with_geneses(&nodes, &config, realm_id).await;
    persist_trusted_realm(holder, realm_id).await;

    let (group_id, holders) =
        sample_group(&config, |h| h.len() == 1 && h[0] == holder.net.node_id());
    assert!(!holders.contains(&origin.net.node_id()));

    // The owner holds the default admin role granting group-wide WRITE.
    let group = Group {
        display_name: "team".to_string(),
        group_id,
        realm_id,
        roles: HashSet::new(),
        owner,
    };
    let authorization =
        GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
    let user_role_id = *authorization
        .roles
        .iter()
        .find(|(_, role)| role.name == "user")
        .map(|(role_id, _)| role_id)
        .expect("default group has a user role");
    write_group(holder, &group, &authorization).await;
    // The holder's permission check reads the realm authorization document first;
    // the group admin role (assigned to the owner) supplies the actual grant.
    write_realm_auth(holder, realm_id).await;

    let member = UserId::local(Ulid::new(), realm_id);
    let token = sign_token(&realm_key, &token_claims(realm_id, owner));

    let reply = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Group(GroupCall::AddMember {
            group_id,
            user_id: member,
            role_ids: HashSet::from([user_role_id]),
        }),
    )
    .await
    .expect("routed add-member converges on the holder");
    let ProxiedReply::Group(reply) = reply else {
        panic!("expected a group reply");
    };
    assert!(
        matches!(*reply, GroupReply::Authorization(_)),
        "expected an authorization reply"
    );

    let read = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Group(GroupCall::Get { group_id }),
    )
    .await
    .expect("routed read of the mutated group");
    let ProxiedReply::Group(read) = read else {
        panic!("expected a group reply");
    };
    let GroupReply::Document {
        authorization: doc, ..
    } = *read
    else {
        panic!("expected a group document reply");
    };
    assert!(
        doc.roles
            .get(&user_role_id)
            .expect("user role present")
            .assigned_users
            .contains(&member),
        "the routed write must be readable back through the holder"
    );

    shutdown(nodes).await;
}

// The inbox arm derives the served recipient from the validated bearer, never
// the wire claim: a List whose wire recipient is another user returns only the
// bearer subject's inbox, so the other user's seeded record is never disclosed.
#[tokio::test]
async fn notification_list_serves_bearer_subject_not_wire_recipient() {
    let (realm_key, realm_id, _user) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 2).await;
    let (origin, holder) = (&nodes[0], &nodes[1]);

    let config = config_with_replica(realm_id, &node_ids(&nodes), 1);
    install_config(&nodes, &config).await;
    persist_trusted_realm(holder, realm_id).await;

    // `victim`'s inbox lives on the holder; `caller` is the bearer subject.
    let victim = sample_inbox_user(&config, holder.net.node_id(), realm_id);
    let caller = UserId::local(Ulid::new(), realm_id);

    let seeded = NotificationRecord {
        notification_id: Ulid::new(),
        recipient: victim,
        class: NotificationClass::Transient,
        kind: NotificationKind::AddedToGroup {
            group_id: Ulid::new(),
            actor_user_id: caller,
        },
        created_at_ms: unix_timestamp_millis(),
        read_at_ms: None,
    };
    upsert_inbox_records(
        &holder.context.storage_handle,
        std::slice::from_ref(&seeded),
    )
    .await
    .expect("seed victim inbox on the holder");

    // The wire recipient steers routing to the victim's inbox holder, but the
    // bearer subject is `caller`; the holder serves `caller`'s (empty) inbox.
    let token = sign_token(&realm_key, &token_claims(realm_id, caller));
    let reply = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Notification(NotificationCall::List {
            recipient: victim,
            cursor: None,
            limit: 50,
        }),
    )
    .await
    .expect("routed notification list");
    let ProxiedReply::Notification(reply) = reply else {
        panic!("expected a notification reply");
    };
    let NotificationReply::List { records, .. } = *reply else {
        panic!("expected a notification list reply");
    };
    assert!(
        records
            .iter()
            .all(|record| record.notification_id != seeded.notification_id),
        "the victim's record must not leak to a bearer for another user"
    );
    assert!(
        records.is_empty(),
        "the bearer subject has an empty inbox: {records:?}"
    );

    shutdown(nodes).await;
}

// A routed membership removal that would strip a group's last admin must reach
// the holder's operation and come back as a typed Conflict (409), not a flattened
// authorization rejection — the status the review found regressed.
#[tokio::test]
async fn group_remove_last_admin_routes_conflict() {
    let (realm_key, realm_id, owner) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 2).await;
    let (origin, holder) = (&nodes[0], &nodes[1]);

    let config = config_with_replica(realm_id, &node_ids(&nodes), 1);
    install_config_with_geneses(&nodes, &config, realm_id).await;
    persist_trusted_realm(holder, realm_id).await;

    let (group_id, holders) =
        sample_group(&config, |h| h.len() == 1 && h[0] == holder.net.node_id());
    assert!(!holders.contains(&origin.net.node_id()));

    let group = Group {
        display_name: "team".to_string(),
        group_id,
        realm_id,
        roles: HashSet::new(),
        owner,
    };
    let authorization =
        GroupAuthorizationDocument::new_default_group_doc(owner, realm_id, group_id);
    let admin_role_id = *authorization
        .roles
        .iter()
        .find(|(_, role)| role.name == "admin")
        .map(|(role_id, _)| role_id)
        .expect("default group has an admin role");
    write_group(holder, &group, &authorization).await;
    write_realm_auth(holder, realm_id).await;

    let token = sign_token(&realm_key, &token_claims(realm_id, owner));
    let error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Group(GroupCall::RemoveMember {
            group_id,
            user_id: owner,
            role_ids: Some(HashSet::from([admin_role_id])),
        }),
    )
    .await
    .expect_err("removing the last admin is a conflict");
    assert!(
        matches!(error, HolderRoutingError::Conflict(_)),
        "unexpected error: {error:?}"
    );

    shutdown(nodes).await;
}

// A routed self user-update with an invalid display name reaches the holder's
// validation and returns a typed BadRequest (400) rather than a flattened refusal.
#[tokio::test]
async fn user_update_invalid_name_routes_bad_request() {
    let (realm_key, realm_id, _user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 2).await;
    let (origin, holder) = (&nodes[0], &nodes[1]);

    let config = config_with_replica(realm_id, &node_ids(&nodes), 1);
    install_config(&nodes, &config).await;
    persist_trusted_realm(holder, realm_id).await;

    let (target_user, holders) = sample_user(&config, realm_id, |h| {
        h.len() == 1 && h[0] == holder.net.node_id()
    });
    assert!(!holders.contains(&origin.net.node_id()));

    let user = make_user(target_user, "Alice");
    write_user(holder, &user).await;

    let token = sign_token(&realm_key, &token_claims(realm_id, target_user));
    let error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::User(UserCall::Update {
            user_id: target_user.to_string(),
            name: Some("   ".to_string()),
            set_attributes: HashMap::new(),
            remove_attributes: Vec::new(),
        }),
    )
    .await
    .expect_err("an empty display name is a bad request");
    assert!(
        matches!(error, HolderRoutingError::BadRequest(_)),
        "unexpected error: {error:?}"
    );

    shutdown(nodes).await;
}

// A by-id metadata mutation from a non-holder must resolve the SAME strategy the
// create resolved. Under a `profiles/` MetadataPathPrefix binding (replica-1) the
// document's shard lives on one holder, while the registry class (everywhere)
// ranks a different node first. The record is seeded only on the origin and the
// profile-strategy holder — never on the registry-class front-runner — so a route
// that used the registry class would land on a node lacking the record (NotFound),
// while the correct profile-strategy route lands on the holder that has it and
// denies the write (Unauthorized). Asserting Unauthorized proves it routed by the
// record's path, not the registry class.
#[tokio::test]
async fn metadata_by_id_mutation_routes_to_path_prefix_holder() {
    let (realm_key, realm_id, user_id) = realm_fixture();
    let nodes = meshed_nodes(realm_id, 3).await;
    let origin = &nodes[0];
    let profile_holder = nodes[1].net.node_id();
    let registry_front_runner = nodes[2].net.node_id();

    let config = config_with_profiles_binding(realm_id, &node_ids(&nodes));
    install_config(&nodes, &config).await;
    persist_trusted_realm(&nodes[1], realm_id).await;

    let group_id = Ulid::new();
    let document_id = sample_profile_document(
        &config,
        group_id,
        origin.net.node_id(),
        profile_holder,
        registry_front_runner,
    );

    let record = MetadataRegistryRecord {
        realm_id,
        group_id,
        document_id,
        document_path: "profiles/x".to_string(),
        graph_iri: MetadataRegistryRecord::graph_iri_for(document_id),
        public: false,
        permission_path: MetadataRegistryRecord::permission_path_for(
            &realm_id,
            group_id,
            "profiles/x",
            document_id,
        ),
        holder_node_ids: vec![profile_holder],
        created_at_ms: unix_timestamp_millis(),
        updated_at_ms: unix_timestamp_millis(),
        last_event_id: Ulid::new(),
    };
    // Everywhere-placed record: present on the origin (so it loads the strategy
    // key) and on the profile holder — but deliberately NOT on the registry-class
    // front-runner, which the buggy route would reach first.
    write_document_record(origin, &record).await;
    write_document_record(&nodes[1], &record).await;

    let token = sign_token(&realm_key, &token_claims(realm_id, user_id));

    let update_error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Metadata(MetadataCall::Update {
            document_id,
            public: None,
            mutation: MetadataMutation::ReplaceRoCrate {
                jsonld: "{}".to_string(),
            },
        }),
    )
    .await
    .expect_err("the routed update reaches the profile holder and is denied write");
    assert!(
        matches!(update_error, HolderRoutingError::Unauthorized(_)),
        "update routed to the wrong holder: {update_error:?}"
    );

    let delete_error = dispatch_holder_call(
        origin.context.as_ref(),
        origin.net.node_id(),
        Some(&token),
        ProxiedCall::Metadata(MetadataCall::Delete { document_id }),
    )
    .await
    .expect_err("the routed delete reaches the profile holder and is denied write");
    assert!(
        matches!(delete_error, HolderRoutingError::Unauthorized(_)),
        "delete routed to the wrong holder: {delete_error:?}"
    );

    shutdown(nodes).await;
}

fn config_with_profiles_binding(realm_id: RealmId, node_ids: &[NodeId]) -> RealmConfigDocument {
    let everywhere = PlacementStrategy {
        strategy_id: Ulid::new(),
        name: "everywhere".to_string(),
        replica_count: None,
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: DEFAULT_SHARD_COUNT,
    };
    let profiles = PlacementStrategy {
        strategy_id: Ulid::new(),
        name: "profiles".to_string(),
        replica_count: Some(1),
        distinct_locations: false,
        affinity: Vec::new(),
        shard_count: DEFAULT_SHARD_COUNT,
    };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.default_strategy_id = Some(everywhere.strategy_id);
    config.strategy_bindings = vec![
        StrategyBinding {
            scope: BindingScope::Class(DocumentClass::MetadataRegistry),
            strategy_id: everywhere.strategy_id,
        },
        StrategyBinding {
            scope: BindingScope::MetadataPathPrefix("profiles/".to_string()),
            strategy_id: profiles.strategy_id,
        },
    ];
    config.strategies = vec![everywhere, profiles];
    for node_id in node_ids {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config
}

// Samples a metadata document whose profile-strategy holder is exactly
// `profile_holder` (with the origin forwarding) while the registry class ranks
// `registry_front_runner` first among remotes — the divergence that makes the
// regression observable.
fn sample_profile_document(
    config: &RealmConfigDocument,
    group_id: Ulid,
    origin: NodeId,
    profile_holder: NodeId,
    registry_front_runner: NodeId,
) -> Ulid {
    for _ in 0..200_000 {
        let document_id = Ulid::new();
        let target = DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        };
        let profile_holders = resolve_shard_holders(
            config,
            &placement_ref_for_target(config, &target, Some("profiles/x")),
        );
        if profile_holders != vec![profile_holder] {
            continue;
        }
        let registry_holders =
            resolve_shard_holders(config, &placement_ref_for_target(config, &target, None));
        let first_remote = registry_holders
            .iter()
            .copied()
            .find(|node| *node != origin);
        if first_remote == Some(registry_front_runner) && registry_front_runner != profile_holder {
            return document_id;
        }
    }
    panic!("no profile document matched the routing divergence within the sampling bound");
}

async fn write_document_record(node: &TestNode, record: &MetadataRegistryRecord) {
    write_storage(
        &node.context.storage_handle,
        METADATA_DOCUMENT_INDEX_KEYSPACE,
        metadata_document_key(record.document_id).to_vec(),
        postcard::to_allocvec(record).expect("registry record serializes"),
    )
    .await;
}

async fn write_realm_auth(node: &TestNode, realm_id: RealmId) {
    let doc = RealmAuthorizationDocument::new_default_realm_doc(realm_id);
    write_storage(
        &node.context.storage_handle,
        AUTH_KEYSPACE,
        realm_id.as_bytes().to_vec(),
        postcard::to_allocvec(&doc).expect("realm auth doc serializes"),
    )
    .await;
}

fn pin_shard(subject: &[u8], node_id: NodeId) -> PlacementOverride {
    PlacementOverride {
        subject: subject.to_vec(),
        pinned: vec![node_id],
        excluded: Vec::new(),
        strategy_id: None,
    }
}

async fn install_config_for(node: &TestNode, config: &RealmConfigDocument) {
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: UserId::nil(config.realm_id),
        realm_id: config.realm_id,
    };
    let bytes = config.to_bytes(&actor).expect("config serializes");
    write_storage(
        &node.context.storage_handle,
        REALM_CONFIG_KEYSPACE,
        config.realm_id.as_bytes().to_vec(),
        bytes,
    )
    .await;
    node.net
        .refresh_realm_peers_from_document(config)
        .await
        .expect("refresh realm peers");
}

async fn install_config_with_geneses(
    nodes: &[TestNode],
    config: &RealmConfigDocument,
    realm_id: RealmId,
) {
    install_config(nodes, config).await;
    for node in nodes {
        process_shard_placements(&node.context, realm_id, node.net.node_id()).await;
    }
}

fn sample_inbox_user(config: &RealmConfigDocument, holder: NodeId, realm_id: RealmId) -> UserId {
    for _ in 0..100_000 {
        let user_id = UserId::local(Ulid::new(), realm_id);
        if matches!(resolve_inbox_holder(&user_id, config), Ok(Some(node)) if node == holder) {
            return user_id;
        }
    }
    panic!("no user hashed to the inbox holder within the sampling bound");
}

fn make_user(user_id: UserId, name: &str) -> User {
    User {
        user_id,
        name: name.to_string(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: Default::default(),
    }
}

async fn write_user(node: &TestNode, user: &User) {
    let actor = Actor {
        node_id: node.net.node_id(),
        user_id: user.user_id,
        realm_id: user.user_id.realm_id,
    };
    write_storage(
        &node.context.storage_handle,
        USER_KEYSPACE,
        user.user_id.to_bytes().to_vec(),
        user.to_bytes(&actor).expect("user serializes"),
    )
    .await;
}

fn sample_user<F>(
    config: &RealmConfigDocument,
    realm_id: RealmId,
    predicate: F,
) -> (UserId, Vec<NodeId>)
where
    F: Fn(&[NodeId]) -> bool,
{
    for _ in 0..100_000 {
        let user_id = UserId::local(Ulid::new(), realm_id);
        let placement =
            placement_ref_for_target(config, &DocumentSyncTarget::User { user_id }, None);
        let holders = resolve_shard_holders(config, &placement);
        if predicate(&holders) {
            return (user_id, holders);
        }
    }
    panic!("no user matched the holder predicate within the sampling bound");
}

fn realm_fixture() -> (SigningKey, RealmId, UserId) {
    let mut rng = jsonwebtoken::signature::rand_core::OsRng;
    let signing_key = SigningKey::generate(&mut rng);
    let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
    let user_id = UserId::local(Ulid::new(), realm_id);
    (signing_key, realm_id, user_id)
}

fn token_claims(realm_id: RealmId, user_id: UserId) -> TokenClaims {
    let now = chrono::Utc::now().timestamp().max(0) as u64;
    TokenClaims {
        sub: user_id.to_string(),
        iss: realm_id.to_string(),
        iat: now,
        exp: now + 600,
        jti: Ulid::new().to_string(),
        restrictions: None,
        issuer_pubkey: None,
        delegation_signature: None,
    }
}

fn sign_token(signing_key: &SigningKey, claims: &TokenClaims) -> String {
    let key_pem = signing_key.to_pkcs8_pem(LineEnding::LF).unwrap();
    encode(
        &Header::new(Algorithm::EdDSA),
        claims,
        &EncodingKey::from_ed_pem(key_pem.as_bytes()).unwrap(),
    )
    .unwrap()
}

fn config_with_replica(
    realm_id: RealmId,
    node_ids: &[NodeId],
    replica: u32,
) -> RealmConfigDocument {
    let mut config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    config.seed_default_placement();
    if let Some(strategy_id) = config.default_strategy_id
        && let Some(strategy) = config
            .strategies
            .iter_mut()
            .find(|strategy| strategy.strategy_id == strategy_id)
    {
        strategy.replica_count = Some(replica);
    }
    for node_id in node_ids {
        config.ensure_node(*node_id, RealmNodeKind::Server);
    }
    config
}

fn make_group(
    group_id: Ulid,
    realm_id: RealmId,
    owner: UserId,
) -> (Group, GroupAuthorizationDocument) {
    let group = Group {
        display_name: "team".to_string(),
        group_id,
        realm_id,
        roles: HashSet::new(),
        owner,
    };
    let authorization = GroupAuthorizationDocument {
        group_id,
        roles: HashMap::new(),
    };
    (group, authorization)
}

fn sample_group<F>(config: &RealmConfigDocument, predicate: F) -> (Ulid, Vec<NodeId>)
where
    F: Fn(&[NodeId]) -> bool,
{
    for _ in 0..100_000 {
        let group_id = Ulid::new();
        let placement =
            placement_ref_for_target(config, &DocumentSyncTarget::Group { group_id }, None);
        let holders = resolve_shard_holders(config, &placement);
        if predicate(&holders) {
            return (group_id, holders);
        }
    }
    panic!("no group matched the holder predicate within the sampling bound");
}

fn node_ids(nodes: &[TestNode]) -> Vec<NodeId> {
    nodes.iter().map(|node| node.net.node_id()).collect()
}

fn node_by_id(nodes: &[TestNode], node_id: NodeId) -> &TestNode {
    nodes
        .iter()
        .find(|node| node.net.node_id() == node_id)
        .expect("holder is one of the spawned nodes")
}

async fn meshed_nodes(realm_id: RealmId, count: usize) -> Vec<TestNode> {
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        nodes.push(spawn_node(realm_id).await);
    }
    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            nodes[i]
                .net
                .add_peer_addr(nodes[j].net.endpoint_addr())
                .await;
            nodes[j]
                .net
                .add_peer_addr(nodes[i].net.endpoint_addr())
                .await;
        }
    }
    nodes
}

async fn spawn_node(realm_id: RealmId) -> TestNode {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let storage =
        FjallStorage::open(temp_dir.path().to_str().expect("temp path")).expect("storage opens");
    let net = NetHandle::new(
        NetConfig {
            bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
            realm_id,
            discovery_method: DiscoveryMethod::None,
            relay_method: RelayMethod::None,
            ..NetConfig::default()
        },
        storage.clone(),
    )
    .await
    .expect("net handle");
    let task_handle = TaskHandle::new();
    let context = Arc::new(DriverContext {
        storage_handle: storage,
        net_handle: Some(net.clone()),
        blob_handle: None,
        metadata_handle: None,
        task_handle: Some(task_handle.clone()),
    });
    initialize_net_incoming(context.clone());
    initialize_task_incoming(context.clone(), task_handle).await;
    TestNode {
        _temp_dir: temp_dir,
        net,
        context,
    }
}

async fn install_config(nodes: &[TestNode], config: &RealmConfigDocument) {
    for node in nodes {
        let actor = Actor {
            node_id: node.net.node_id(),
            user_id: UserId::nil(config.realm_id),
            realm_id: config.realm_id,
        };
        let bytes = config.to_bytes(&actor).expect("config serializes");
        write_storage(
            &node.context.storage_handle,
            REALM_CONFIG_KEYSPACE,
            config.realm_id.as_bytes().to_vec(),
            bytes,
        )
        .await;
        node.net
            .refresh_realm_peers_from_document(config)
            .await
            .expect("refresh realm peers");
    }
}

async fn persist_trusted_realm(node: &TestNode, realm_id: RealmId) {
    let trusted: HashSet<RealmId> = HashSet::from([realm_id]);
    let bytes = postcard::to_allocvec(&trusted).expect("trusted realms serialize");
    write_storage(
        &node.context.storage_handle,
        API_STATE_KEYSPACE,
        TRUSTED_REALMS_LIST_KEY.to_vec(),
        bytes,
    )
    .await;
}

async fn write_group(node: &TestNode, group: &Group, authorization: &GroupAuthorizationDocument) {
    let key = group.group_id.to_bytes().to_vec();
    write_storage(
        &node.context.storage_handle,
        GROUP_KEYSPACE,
        key.clone(),
        postcard::to_allocvec(group).expect("group serializes"),
    )
    .await;
    write_storage(
        &node.context.storage_handle,
        AUTH_KEYSPACE,
        key,
        postcard::to_allocvec(authorization).expect("auth doc serializes"),
    )
    .await;
}

async fn write_storage(storage: &StorageHandle, key_space: &str, key: Vec<u8>, value: Vec<u8>) {
    match storage
        .send_storage_effect(StorageEffect::Write {
            key_space: key_space.to_string(),
            key: ByteView::from(key),
            value: ByteView::from(value),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::WriteResult { .. }) => {}
        other => panic!("unexpected write event for {key_space}: {other:?}"),
    }
}

async fn shutdown(nodes: Vec<TestNode>) {
    for node in nodes {
        node.net.shutdown().await;
    }
}
