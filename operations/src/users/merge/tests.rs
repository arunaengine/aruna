//! Merge decisions and transitions, tested without I/O.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::{MergeError, MergeUsersConfig, MergeUsersOperation, plan};
use aruna_core::admin_documents::AdminDocumentOperation;
use aruna_core::document::{DocumentOutboxEvent, DocumentOutboxRecord, DocumentTarget};
use aruna_core::effects::{Effect, StorageEffect};
use aruna_core::events::{Event, StorageEvent, SubOperationEvent};
use aruna_core::keyspaces::{AUTH_KEYSPACE, SUBJECT_INDEX_KEYSPACE, SYNC_OUTBOX_KEYSPACE};
use aruna_core::operation::Operation;
use aruna_core::storage_entries::subject_index_value;
use aruna_core::structs::identity::auth::{
    Actor, AuthContext, Permission, Role, SessionKind, SessionRef,
};
use aruna_core::structs::identity::group::GroupAuthorizationDocument;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmId, RealmNodeKind};
use aruna_core::structs::identity::user::User;
use aruna_core::task::{TaskEvent, TaskKey};
use aruna_core::types::{Effects, TxnId, Value};
use aruna_core::user::validation::{
    DEACTIVATED_ATTRIBUTE, MERGED_INTO_ATTRIBUTE, MERGED_PREFIX, SERVICE_GROUP_ATTRIBUTE,
};
use aruna_core::{NodeId, UserId};
use byteview::ByteView;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;
use ulid::Ulid;

const NOW: u64 = 1_000_000;
const REALM: RealmId = RealmId([9u8; 32]);

fn id(seed: u8) -> UserId {
    UserId::local(Ulid::from_bytes([seed; 16]), REALM)
}

fn node() -> NodeId {
    iroh::SecretKey::from_bytes(&[9u8; 32]).public()
}

fn actor(user_id: UserId) -> Actor {
    Actor {
        node_id: node(),
        user_id,
        realm_id: REALM,
    }
}

fn portal(user_id: UserId) -> AuthContext {
    AuthContext {
        user_id,
        realm_id: REALM,
        path_restrictions: None,
        session: Some(SessionRef {
            sid: Ulid::generate().to_string(),
            kind: SessionKind::Portal,
            name: None,
            via: None,
        }),
    }
}

fn user(seed: u8, subjects: &[&str]) -> User {
    User {
        user_id: id(seed),
        name: format!("user {seed}"),
        subject_ids: subjects.iter().map(ToString::to_string).collect(),
        alias_user_ids: Default::default(),
        attributes: Default::default(),
    }
}

fn config(survivor: UserId, merged: UserId) -> MergeUsersConfig {
    MergeUsersConfig {
        actor: actor(survivor),
        survivor: portal(survivor),
        survivor_login: Some(NOW - 10),
        merged: portal(merged),
        merged_login: Some(NOW - 10),
        merged_live: true,
        merged_token: None,
        vault_deadline: Duration::from_secs(1),
        now: NOW,
    }
}

fn bytes(user: &User) -> Option<Value> {
    Some(user.to_bytes(&actor(user.user_id)).unwrap().into())
}

fn read(values: Vec<Option<Value>>) -> Event {
    Event::Storage(StorageEvent::BatchReadResult {
        values: values
            .into_iter()
            .map(|value| (ByteView::from(Vec::new()), value))
            .collect(),
    })
}

fn role(name: &str, permission: Permission, users: &[UserId]) -> Role {
    Role {
        role_id: Ulid::generate(),
        name: name.to_string(),
        permissions: HashMap::from([("/realm/g/1/**".to_string(), permission)]),
        assigned_users: users.iter().copied().collect::<HashSet<_>>(),
    }
}

fn roles(list: Vec<Role>) -> HashMap<Ulid, Role> {
    list.into_iter().map(|role| (role.role_id, role)).collect()
}

fn writes(effects: &Effects) -> Vec<(String, ByteView, ByteView)> {
    match effects.first() {
        Some(Effect::Storage(StorageEffect::BatchWrite { writes, .. })) => writes.clone(),
        other => panic!("expected a batch write, got {other:?}"),
    }
}

fn written<'a>(
    rows: &'a [(String, ByteView, ByteView)],
    key_space: &str,
    key: &[u8],
) -> &'a ByteView {
    &rows
        .iter()
        .find(|(space, row_key, _)| space == key_space && row_key.as_ref() == key)
        .unwrap_or_else(|| panic!("no row in {key_space}"))
        .2
}

#[test]
fn refuses_unmergeable_accounts() {
    let survivor = user(1, &[]);
    assert_eq!(
        plan::check_accounts(&survivor, &survivor),
        Err(MergeError::SameAccount)
    );
    let mut inactive = user(2, &[]);
    inactive
        .attributes
        .insert(DEACTIVATED_ATTRIBUTE.to_string(), "true".to_string());
    assert_eq!(
        plan::check_accounts(&survivor, &inactive),
        Err(MergeError::Inactive)
    );
    let mut service = user(3, &[]);
    service.attributes.insert(
        SERVICE_GROUP_ATTRIBUTE.to_string(),
        Ulid::generate().to_string(),
    );
    assert_eq!(
        plan::check_accounts(&survivor, &service),
        Err(MergeError::ServiceAccount)
    );
    let mut other = user(4, &[]);
    other.user_id.realm_id = RealmId([3u8; 32]);
    assert_eq!(
        plan::check_accounts(&survivor, &other),
        Err(MergeError::OtherRealm)
    );
}

#[test]
fn repeat_is_recognized() {
    let survivor = user(1, &["subject"]);
    let mut merged = user(2, &[]);
    merged
        .attributes
        .insert(DEACTIVATED_ATTRIBUTE.to_string(), "true".to_string());
    merged.attributes.insert(
        MERGED_INTO_ATTRIBUTE.to_string(),
        survivor.user_id.to_string(),
    );
    assert_eq!(plan::check_accounts(&survivor, &merged), Ok(true));
}

#[test]
fn conflict_keeps_higher_role() {
    let (survivor, merged) = (id(1), id(2));
    let member = role("member", Permission::READ, &[survivor]);
    let admin = role("admin", Permission::WRITE, &[merged]);
    let roles_up = roles(vec![member.clone(), admin.clone()]);
    let (handed, held) = plan::handed_roles(&roles_up, survivor, merged);
    assert_eq!(handed, vec![admin.role_id]);
    assert_eq!(held, vec![admin.role_id]);

    let admin = role("admin", Permission::WRITE, &[survivor]);
    let member = role("member", Permission::READ, &[merged]);
    let roles_down = roles(vec![member.clone(), admin]);
    let (handed, held) = plan::handed_roles(&roles_down, survivor, merged);
    assert!(handed.is_empty());
    assert_eq!(held, vec![member.role_id]);
}

#[test]
fn copies_missing_attributes() {
    let mut survivor = user(1, &[]);
    survivor
        .attributes
        .insert("ui.theme".to_string(), "dark".to_string());
    let mut merged = user(2, &[]);
    merged
        .attributes
        .insert("ui.theme".to_string(), "light".to_string());
    merged
        .attributes
        .insert("orcid".to_string(), "0000-0002-1825-0097".to_string());
    merged
        .attributes
        .insert(format!("{MERGED_PREFIX}{}", Ulid::nil()), id(5).to_string());
    assert_eq!(
        plan::copied_attributes(&survivor, &merged),
        vec![("orcid".to_string(), "0000-0002-1825-0097".to_string())]
    );
}

#[test]
fn claims_move_to_survivor() {
    let claims = plan::moved_claims(BTreeSet::from([id(2)]), id(1), id(2));
    assert_eq!(claims, BTreeSet::from([id(1)]));
}

#[test]
fn stale_login_refused() {
    let mut stale = config(id(1), id(2));
    stale.survivor_login = Some(NOW - 600);
    let mut operation = MergeUsersOperation::new(stale);
    assert!(operation.start().is_empty());
    assert_eq!(operation.finalize(), Err(MergeError::StaleLogin));

    let mut other = config(id(1), id(2));
    other.merged.realm_id = RealmId([3u8; 32]);
    let mut operation = MergeUsersOperation::new(other);
    operation.start();
    assert_eq!(operation.finalize(), Err(MergeError::OtherRealm));
}

#[test]
fn cut_token_refused() {
    let mut cut = config(id(1), id(2));
    cut.merged_live = false;
    let mut operation = MergeUsersOperation::new(cut);
    operation.start();
    operation.step(read(vec![bytes(&user(1, &[])), bytes(&user(2, &["s"]))]));
    assert_eq!(operation.finalize(), Err(MergeError::TokenInvalid));
}

#[test]
fn held_vault_refused() {
    let mut operation = MergeUsersOperation::new(config(id(1), id(2)));
    operation.start();
    let effects = operation.step(read(vec![bytes(&user(1, &[])), bytes(&user(2, &["s"]))]));
    assert!(matches!(effects.first(), Some(Effect::SubOperation(_))));
    operation.step(Event::SubOperation(SubOperationEvent::VaultHeld {
        result: Ok(true),
    }));
    assert_eq!(operation.finalize(), Err(MergeError::VaultPresent));
}

#[test]
fn repeat_only_cuts_off() {
    let mut merged = user(2, &[]);
    merged
        .attributes
        .insert(MERGED_INTO_ATTRIBUTE.to_string(), id(1).to_string());
    merged
        .attributes
        .insert(DEACTIVATED_ATTRIBUTE.to_string(), "true".to_string());
    let mut repeat = config(id(1), id(2));
    repeat.merged_live = false;
    repeat.merged_login = None;
    let mut operation = MergeUsersOperation::new(repeat);
    operation.start();
    let effects = operation.step(read(vec![bytes(&user(1, &["s"])), bytes(&merged)]));
    assert!(matches!(effects.first(), Some(Effect::SubOperation(_))));
    operation.step(Event::SubOperation(SubOperationEvent::TokenRevoked {
        result: Ok(()),
    }));
    let outcome = operation.finalize().unwrap();
    assert!(outcome.repeated);
    assert_eq!(outcome.subjects, 0);
}

/// Drives a full merge: B holds a subject, a group role, a device and a secondary of its own.
#[test]
fn merge_moves_everything() {
    let (a, b, c) = (id(1), id(2), id(3));
    let survivor = user(1, &["subject-a"]);
    let mut merged = user(2, &["subject-b"]);
    merged
        .attributes
        .insert("orcid".to_string(), "0000-0002-1825-0097".to_string());
    merged
        .attributes
        .insert(format!("{MERGED_PREFIX}{}", c.user_ulid), c.to_string());
    let mut secondary = user(3, &[]);
    secondary
        .attributes
        .insert(MERGED_INTO_ATTRIBUTE.to_string(), b.to_string());
    let device = iroh::SecretKey::from_bytes(&[4u8; 32]).public();
    let mut realm = RealmConfigDocument::new(REALM, Vec::new(), 3);
    realm.ensure_node(node(), RealmNodeKind::Management);
    realm.ensure_node(device, RealmNodeKind::User { owner: b });
    let admin = role("admin", Permission::WRITE, &[b]);
    let group_id = Ulid::from_bytes([7u8; 16]);
    let group = GroupAuthorizationDocument {
        group_id,
        roles: roles(vec![admin.clone()]),
        policies: Vec::new(),
    };
    let txn_id = TxnId::generate();

    let mut operation = MergeUsersOperation::new(config(a, b));
    operation.start();
    operation.step(read(vec![bytes(&survivor), bytes(&merged)]));
    operation.step(Event::SubOperation(SubOperationEvent::VaultHeld {
        result: Ok(false),
    }));
    operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    let realm_bytes = Some(realm.to_bytes(&actor(a)).unwrap().into());
    let scan = operation.step(read(vec![
        bytes(&survivor),
        bytes(&merged),
        None,
        None,
        None,
        None,
        realm_bytes,
        None,
        None,
        None,
    ]));
    assert!(matches!(
        scan.first(),
        Some(Effect::Storage(StorageEffect::Iter { key_space, .. })) if key_space == AUTH_KEYSPACE
    ));
    operation.step(Event::Storage(StorageEvent::IterResult {
        values: vec![(
            ByteView::from(group_id.to_bytes().to_vec()),
            group.to_bytes(&actor(b)).unwrap().into(),
        )],
        next_start_after: None,
    }));
    // Group state, the secondary's row, state and revision, then the subject's claims and index.
    let staged = operation.step(read(vec![
        None,
        bytes(&secondary),
        None,
        None,
        None,
        Some(subject_index_value(b)),
    ]));
    let rows = writes(&staged);

    let stored = |user_id: UserId| {
        let target = DocumentTarget::User { user_id };
        User::from_bytes(written(
            &rows,
            target.storage_keyspace(),
            &target.storage_key(),
        ))
        .unwrap()
    };
    let (a_after, b_after, c_after) = (stored(a), stored(b), stored(c));
    assert!(a_after.subject_ids.contains(&"subject-b".to_string()));
    assert_eq!(a_after.merged_user_ids(), vec![b, c]);
    assert_eq!(
        a_after.attributes.get("orcid").map(String::as_str),
        Some("0000-0002-1825-0097")
    );
    assert!(b_after.subject_ids.is_empty() && b_after.is_deactivated());
    assert_eq!(b_after.merged_into(), Some(a));
    assert!(b_after.merged_user_ids().is_empty());
    assert_eq!(c_after.merged_into(), Some(a));
    assert_eq!(
        written(&rows, SUBJECT_INDEX_KEYSPACE, b"subject-b"),
        &subject_index_value(a)
    );

    let group_target = DocumentTarget::GroupAuthorization { group_id };
    let group_after = GroupAuthorizationDocument::from_bytes(written(
        &rows,
        AUTH_KEYSPACE,
        &group_target.storage_key(),
    ))
    .unwrap();
    let admin_after = &group_after.roles[&admin.role_id];
    assert!(admin_after.assigned_users.contains(&a));
    assert!(!admin_after.assigned_users.contains(&b));

    let config_target = DocumentTarget::RealmConfig { realm_id: REALM };
    let realm_after = RealmConfigDocument::from_bytes(written(
        &rows,
        config_target.storage_keyspace(),
        &config_target.storage_key(),
    ))
    .unwrap();
    let owner = realm_after
        .nodes
        .iter()
        .find(|node| node.node_id == device.to_string())
        .and_then(|node| node.kind.owner());
    assert_eq!(owner, Some(a));

    // The group grant is authored by B, so peers check that B held the role.
    let grant = rows
        .iter()
        .filter(|(space, _, _)| space == SYNC_OUTBOX_KEYSPACE)
        .filter_map(|(_, _, value)| postcard::from_bytes::<DocumentOutboxRecord>(value).ok())
        .filter_map(|record| match record.event {
            DocumentOutboxEvent::AdminOperation { event, .. } => Some(*event),
            _ => None,
        })
        .find(|event| {
            matches!(event.op, AdminDocumentOperation::GroupAssignmentAdded { user_id, .. } if user_id == a)
        })
        .expect("the role grant is queued");
    assert_eq!(grant.actor.user_id, b);

    let mut effects = operation.step(Event::Storage(StorageEvent::BatchWriteResult {
        entries: Vec::new(),
    }));
    while let Some(Effect::Storage(
        StorageEffect::BatchDelete { .. } | StorageEffect::BatchRead { .. },
    )) = effects.first()
    {
        effects = match effects.first() {
            Some(Effect::Storage(StorageEffect::BatchDelete { .. })) => {
                operation.step(Event::Storage(StorageEvent::BatchDeleteResult {
                    entries: Vec::new(),
                }))
            }
            _ => operation.step(read(Vec::new())),
        };
    }
    assert!(matches!(effects.first(), Some(Effect::SubOperation(_))));
    operation.step(Event::SubOperation(SubOperationEvent::EpochsMarked {
        result: Ok(()),
    }));
    operation.step(Event::Storage(StorageEvent::TransactionCommitted {
        txn_id,
    }));
    operation.step(Event::Task(TaskEvent::TimerScheduled {
        key: TaskKey::DrainSyncOutbox,
        after: Duration::ZERO,
    }));
    operation.step(Event::SubOperation(SubOperationEvent::TokenRevoked {
        result: Ok(()),
    }));
    let outcome = operation.finalize().unwrap();
    assert_eq!(
        (
            outcome.subjects,
            outcome.groups,
            outcome.devices,
            outcome.secondaries
        ),
        (1, 1, 1, 1)
    );
    assert!(!outcome.repeated);
}
