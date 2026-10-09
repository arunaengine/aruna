//! Tests that receiving nodes admit a federated user's group and self-revocation events only.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::auth::{bearer_token_hash, user_cutoff_expiry, user_cutoff_hash};
use aruna_core::join_request::{JoinDecision, JoinDecisionKind, JoinRequest};
use aruna_core::keyspaces::FEDERATION_KEYSPACE;
use std::collections::BTreeSet;

struct Realm {
    realm_id: RealmId,
    group_id: Ulid,
    role_id: Ulid,
    owner: Actor,
    /// A user of realm 72, acting through a node of realm 71.
    foreign: Actor,
}

impl Realm {
    fn event(
        &self,
        actor: &Actor,
        seq: u64,
        target: AdminDocumentTarget,
        op: AdminDocumentOperation,
    ) -> AdminDocumentEvent {
        let mut event = test_admin_event(Ulid::generate(), target, actor, seq, op);
        event.observed.advance(actor.node_id, seq - 1);
        event
    }

    fn group(&self, actor: &Actor, seq: u64, op: AdminDocumentOperation) -> AdminDocumentEvent {
        let target = AdminDocumentTarget::Group {
            group_id: self.group_id,
        };
        self.event(actor, seq, target, op)
    }

    fn config(&self, actor: &Actor, seq: u64, op: AdminDocumentOperation) -> AdminDocumentEvent {
        let target = AdminDocumentTarget::RealmConfig {
            realm_id: self.realm_id,
        };
        self.event(actor, seq, target, op)
    }

    fn revocation(&self, actor: &Actor, seq: u64, token_owner: UserId) -> AdminDocumentEvent {
        self.config(
            actor,
            seq,
            AdminDocumentOperation::ConfigTokenRevoked {
                token_hash: bearer_token_hash(&format!("token of {token_owner}")),
                expires_at: unix_timestamp_secs() + 3_600,
                token_owner,
            },
        )
    }
}

fn document_target(event: &AdminDocumentEvent) -> DocumentTarget {
    match &event.target {
        AdminDocumentTarget::Group { group_id } => DocumentTarget::GroupAuthorization {
            group_id: *group_id,
        },
        AdminDocumentTarget::Realm { realm_id } => DocumentTarget::RealmAuthorization {
            realm_id: *realm_id,
        },
        AdminDocumentTarget::RealmConfig { realm_id } => DocumentTarget::RealmConfig {
            realm_id: *realm_id,
        },
        AdminDocumentTarget::User { user_id } => DocumentTarget::User { user_id: *user_id },
    }
}

/// Validates the event as a receiving node of the realm and applies it when accepted.
async fn receive(
    storage: &StorageHandle,
    realm_id: RealmId,
    event: AdminDocumentEvent,
) -> AdminEventValidation {
    let target = document_target(&event);
    let placement = admin_test_placement();
    let topic_id = target.sync_topic_id(realm_id, &placement);
    let outcome = validate_admin_event(
        storage,
        topic_id,
        ::irokle::actor_id_for(topic_id, node_to_peer(&event.origin_node_id)),
        &target,
        &event,
        realm_id,
        &placement,
        &sign_as_origin(&event, &placement),
        &mut ConfigValidationCache::default(),
    )
    .await
    .expect("validation runs");
    if outcome == AdminEventValidation::Accepted {
        apply_admin_operation(storage, target, event)
            .await
            .expect("accepted event applies");
    }
    outcome
}

/// A realm with two server nodes and a group with one member role, owned by a local user.
async fn federated_realm(storage: &StorageHandle) -> Realm {
    let realm_id = RealmId::from_bytes([71; 32]);
    let owner = test_actor(
        8,
        UserId::local(Ulid::from_parts(710, 1), realm_id),
        realm_id,
    );
    let foreign_user = UserId::new(Ulid::from_parts(710, 1), RealmId::from_bytes([72; 32]));
    let realm = Realm {
        realm_id,
        group_id: Ulid::from_parts(711, 1),
        role_id: Ulid::from_parts(712, 1),
        foreign: test_actor(9, foreign_user, realm_id),
        owner,
    };
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(realm.owner.node_id, RealmNodeKind::Server);
    config.ensure_node(realm.foreign.node_id, RealmNodeKind::Server);
    batch_write_to(
        storage,
        vec![target_write_entry(
            DocumentTarget::RealmConfig { realm_id },
            config.to_bytes(&realm.owner).unwrap().into(),
        )],
    )
    .await
    .unwrap();
    let created = realm.group(
        &realm.owner,
        1,
        AdminDocumentOperation::GroupCreated {
            realm_id,
            display_name: "Shared".into(),
            owner: realm.owner.user_id,
        },
    );
    assert_eq!(
        receive(storage, realm_id, created).await,
        AdminEventValidation::Accepted
    );
    let subtree = aruna_core::permission_path::role_subtree_root(realm_id, realm.group_id);
    let role = realm.group(
        &realm.owner,
        2,
        AdminDocumentOperation::GroupRoleCreated {
            role: admin_role(
                realm.role_id,
                "member",
                &format!("{subtree}/data/**"),
                Permission::READ,
            ),
        },
    );
    assert_eq!(
        receive(storage, realm_id, role).await,
        AdminEventValidation::Accepted
    );
    realm
}

fn members(auth: &GroupAuthorizationDocument, role_id: Ulid) -> HashSet<UserId> {
    auth.roles
        .get(&role_id)
        .map(|role| role.assigned_users.clone())
        .unwrap_or_default()
}

#[tokio::test]
async fn admits_federated_membership() {
    // Join request, approval, assignment, self-removal, self-revocation and admin cutoff.
    let (_dir, storage) = test_storage();
    let realm = federated_realm(&storage).await;
    let (realm_id, foreign) = (realm.realm_id, realm.foreign.user_id);
    let request = JoinRequest {
        request_id: Ulid::from_parts(713, 1),
        group_id: realm.group_id,
        user_id: foreign,
        message: None,
        created_at: 1,
    };
    let joined = realm.group(
        &realm.foreign,
        1,
        AdminDocumentOperation::GroupJoinRequested {
            request: request.clone(),
        },
    );
    assert_eq!(
        receive(&storage, realm_id, joined).await,
        AdminEventValidation::Accepted
    );
    let approved = realm.group(
        &realm.owner,
        3,
        AdminDocumentOperation::GroupJoinDecided {
            decision: JoinDecision {
                request_id: request.request_id,
                user_id: foreign,
                kind: JoinDecisionKind::Approved,
                decided_by: realm.owner.user_id,
                reason: None,
                decided_at: 2,
                role_ids: BTreeSet::from([realm.role_id]),
            },
        },
    );
    assert_eq!(
        receive(&storage, realm_id, approved).await,
        AdminEventValidation::Accepted
    );
    assert!(
        members(
            &read_group_auth(&storage, realm.group_id).await,
            realm.role_id
        )
        .contains(&foreign)
    );
    // The local user with the same ULID gained nothing.
    let twin = realm.owner.user_id;
    assert_eq!(twin.user_ulid, foreign.user_ulid);
    assert!(
        !members(
            &read_group_auth(&storage, realm.group_id).await,
            realm.role_id
        )
        .contains(&twin)
    );

    let assigned = realm.group(
        &realm.owner,
        4,
        AdminDocumentOperation::GroupAssignmentAdded {
            role_id: realm.role_id,
            user_id: foreign,
        },
    );
    assert_eq!(
        receive(&storage, realm_id, assigned).await,
        AdminEventValidation::Accepted
    );
    let left = realm.group(
        &realm.foreign,
        2,
        AdminDocumentOperation::GroupAssignmentRemoved {
            role_id: realm.role_id,
            user_id: foreign,
        },
    );
    assert_eq!(
        receive(&storage, realm_id, left).await,
        AdminEventValidation::Accepted
    );
    assert!(
        !members(
            &read_group_auth(&storage, realm.group_id).await,
            realm.role_id
        )
        .contains(&foreign)
    );

    let own = realm.revocation(&realm.foreign, 3, foreign);
    let own_hash = match &own.op {
        AdminDocumentOperation::ConfigTokenRevoked { token_hash, .. } => token_hash.clone(),
        _ => unreachable!(),
    };
    assert_eq!(
        receive(&storage, realm_id, own).await,
        AdminEventValidation::Accepted
    );
    let cutoff = realm.config(
        &realm.owner,
        5,
        AdminDocumentOperation::ConfigTokenRevoked {
            token_hash: user_cutoff_hash(&foreign),
            expires_at: user_cutoff_expiry(unix_timestamp_secs() + REVOCATION_GRACE_SECS),
            token_owner: foreign,
        },
    );
    assert_eq!(
        receive(&storage, realm_id, cutoff).await,
        AdminEventValidation::Accepted
    );
    let config = stored_realm_config(&storage, realm_id).await;
    let now = unix_timestamp_secs();
    assert!(config.token_revoked(&own_hash, now));
    assert!(config.user_cutoff(&foreign, now).is_some());
}

#[tokio::test]
async fn keeps_foreign_actors_out() {
    // Foreign actors stay out of user, realm-authorization and unrelated config documents.
    let (_dir, storage) = test_storage();
    let realm = federated_realm(&storage).await;
    let (realm_id, foreign) = (realm.realm_id, realm.foreign.user_id);
    let rejected = |outcome: AdminEventValidation| {
        assert!(
            matches!(outcome, AdminEventValidation::Rejected(_)),
            "{outcome:?}"
        );
    };
    rejected(
        receive(
            &storage,
            realm_id,
            realm.revocation(&realm.foreign, 1, realm.owner.user_id),
        )
        .await,
    );
    let renamed = realm.event(
        &realm.foreign,
        1,
        AdminDocumentTarget::User { user_id: foreign },
        AdminDocumentOperation::UserNameSet {
            name: "Mallory".into(),
        },
    );
    rejected(receive(&storage, realm_id, renamed).await);
    let realm_role = realm.event(
        &realm.foreign,
        1,
        AdminDocumentTarget::Realm { realm_id },
        AdminDocumentOperation::RealmAssignmentAdded {
            role_id: realm.role_id,
            user_id: foreign,
        },
    );
    rejected(receive(&storage, realm_id, realm_role).await);
    let described = realm.config(
        &realm.foreign,
        1,
        AdminDocumentOperation::ConfigDescriptionSet {
            description: "Taken over".into(),
        },
    );
    rejected(receive(&storage, realm_id, described).await);
    // Realm roles stay local even when a local administrator assigns them.
    let local_grant = realm.event(
        &realm.owner,
        1,
        AdminDocumentTarget::Realm { realm_id },
        AdminDocumentOperation::RealmAssignmentAdded {
            role_id: realm.role_id,
            user_id: foreign,
        },
    );
    rejected(receive(&storage, realm_id, local_grant).await);
    // A foreign user who is not a member cannot change the group.
    let realm_auth = RealmAuthorizationDocument::default_realm_doc(realm_id);
    batch_write_to(
        &storage,
        vec![target_write_entry(
            DocumentTarget::RealmAuthorization { realm_id },
            realm_auth.to_bytes(&realm.owner).unwrap().into(),
        )],
    )
    .await
    .unwrap();
    let renamed_group = realm.group(
        &realm.foreign,
        1,
        AdminDocumentOperation::DisplayNameSet {
            display_name: "Mine".into(),
        },
    );
    rejected(receive(&storage, realm_id, renamed_group).await);
    // Applying a foreign revocation of another user's token fails as well.
    let event = realm.revocation(&realm.foreign, 1, realm.owner.user_id);
    assert!(
        apply_admin_operation(&storage, document_target(&event), event)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn linked_logins_replicate() {
    // Only the owner links; a second claim makes the login unusable; owner removal resolves it.
    use aruna_core::link::{alias_claims_key, alias_owner};
    let (_dir, storage) = test_storage();
    let realm = federated_realm(&storage).await;
    let (realm_id, foreign) = (realm.realm_id, realm.foreign.user_id);
    let second = Actor {
        user_id: UserId::local(Ulid::from_parts(714, 1), realm_id),
        ..realm.owner.clone()
    };
    let alias = |actor: &Actor, user_id: UserId, seq: u64, added: bool| {
        let op = if added {
            AdminDocumentOperation::UserAliasAdded { alias: foreign }
        } else {
            AdminDocumentOperation::UserAliasRemoved { alias: foreign }
        };
        realm.event(actor, seq, AdminDocumentTarget::User { user_id }, op)
    };
    let claims = || async {
        let bytes = storage_read_from(
            &storage,
            FEDERATION_KEYSPACE.to_string(),
            alias_claims_key(&foreign).into(),
        )
        .await
        .unwrap();
        bytes
            .map(|bytes| postcard::from_bytes::<BTreeSet<UserId>>(&bytes).unwrap())
            .unwrap_or_default()
    };
    let (first, other) = (realm.owner.user_id, second.user_id);
    let rejected = |outcome: AdminEventValidation| {
        assert!(
            matches!(outcome, AdminEventValidation::Rejected(_)),
            "{outcome:?}"
        );
    };
    // Neither the foreign login itself nor another local user may add the link.
    rejected(receive(&storage, realm_id, alias(&realm.foreign, first, 1, true)).await);
    rejected(receive(&storage, realm_id, alias(&second, first, 1, true)).await);
    let linked = alias(&realm.owner, first, 1, true);
    assert_eq!(
        receive(&storage, realm_id, linked).await,
        AdminEventValidation::Accepted
    );
    assert_eq!(alias_owner(&claims().await), Some(first));
    // A conflicting replicated claim disables the login instead of picking an account.
    let claimed = alias(&second, other, 1, true);
    assert_eq!(
        receive(&storage, realm_id, claimed).await,
        AdminEventValidation::Accepted
    );
    assert_eq!(claims().await, BTreeSet::from([first, other]));
    assert_eq!(alias_owner(&claims().await), None);
    let removed = alias(&realm.owner, first, 2, false);
    assert_eq!(
        receive(&storage, realm_id, removed).await,
        AdminEventValidation::Accepted
    );
    assert_eq!(alias_owner(&claims().await), Some(other));
}
