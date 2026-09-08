use super::*;
use aruna_core::UserId;
use aruna_core::admin_document_reducer::AdminDocumentReducerState;
use aruna_core::admin_documents::AdminDocumentRoleDefinition;
use aruna_core::document::DocumentSyncOutboxRecord;
use aruna_core::keyspaces::DOCUMENT_SYNC_OUTBOX_KEYSPACE;
use aruna_core::structs::RealmId;

#[test]
fn approval_is_atomic() {
    let realm_id = RealmId::from_bytes([7; 32]);
    let group_id = Ulid::from_bytes([3; 16]);
    let actor = Actor {
        node_id: iroh::SecretKey::from_bytes(&[1; 32]).public(),
        user_id: UserId::local(Ulid::from_bytes([1; 16]), realm_id),
        realm_id,
    };
    let member = Actor {
        user_id: UserId::local(Ulid::from_bytes([2; 16]), realm_id),
        ..actor.clone()
    };
    let auth_doc =
        GroupAuthorizationDocument::new_default_group_doc(actor.user_id, realm_id, group_id);
    let user_role = auth_doc
        .roles
        .values()
        .find(|role| role.name == "user")
        .unwrap()
        .role_id;
    let group = Group {
        group_id,
        realm_id,
        display_name: "Group".into(),
        owner: actor.user_id,
        roles: auth_doc.roles.keys().copied().collect(),
    };
    let mut reducer = AdminDocumentReducerState::new(AdminDocumentTarget::Group { group_id });
    for role in auth_doc.roles.values() {
        reducer
            .apply_operation(
                &actor,
                AdminDocumentOperation::GroupRoleCreated {
                    role: AdminDocumentRoleDefinition::from(role),
                },
            )
            .unwrap();
    }
    let request_id = Ulid::from_bytes([5; 16]);
    reducer
        .apply_operation(
            &member,
            AdminDocumentOperation::GroupJoinRequested {
                request: JoinRequest {
                    request_id,
                    group_id,
                    user_id: member.user_id,
                    message: None,
                    created_at: 1,
                },
            },
        )
        .unwrap();
    let config = RealmConfigDocument::default_for_realm(realm_id, Vec::new());
    let mut operation = GroupJoinOperation::new(GroupJoinInput {
        actor: actor.clone(),
        auth: AuthContext {
            user_id: actor.user_id,
            realm_id,
            path_restrictions: None,
            session: None,
        },
        group_id,
        action: JoinAction::Decide {
            request_id,
            approve: true,
            role_ids: BTreeSet::new(),
            reason: None,
        },
        now_ms: 2,
    });
    assert!(matches!(
        operation.start().as_slice(),
        [Effect::SubOperation(_)]
    ));
    operation.step(Event::SubOperation(
        SubOperationEvent::AuthorizationResult { allowed: Ok(true) },
    ));
    let txn_id = Ulid::from_bytes([6; 16]);
    operation.step(Event::Storage(StorageEvent::TransactionStarted { txn_id }));
    let values = vec![
        (
            group_id.to_bytes().to_vec().into(),
            Some(group.to_bytes(&actor).unwrap().into()),
        ),
        (
            group_id.to_bytes().to_vec().into(),
            Some(auth_doc.to_bytes(&actor).unwrap().into()),
        ),
        (
            admin_document_reducer_state_key(&reducer.target),
            Some(
                admin_document_reducer_state_write_entry(&reducer)
                    .unwrap()
                    .2,
            ),
        ),
        (
            realm_id.as_bytes().to_vec().into(),
            Some(config.to_bytes(&actor).unwrap().into()),
        ),
    ];
    let effects = operation.step(Event::Storage(StorageEvent::BatchReadResult { values }));
    let [
        Effect::Storage(StorageEffect::BatchWrite {
            writes,
            txn_id: Some(write_txn),
        }),
    ] = effects.as_slice()
    else {
        panic!("expected one atomic batch: {effects:?}");
    };
    assert_eq!(*write_txn, txn_id);
    let value = |space: &str| {
        &writes
            .iter()
            .find(|(keyspace, _, _)| keyspace == space)
            .unwrap()
            .2
    };
    let auth_doc = GroupAuthorizationDocument::from_bytes(value(AUTH_KEYSPACE)).unwrap();
    assert!(
        auth_doc.roles[&user_role]
            .assigned_users
            .contains(&member.user_id)
    );
    let reducer =
        decode_admin_document_reducer_state(value(ADMIN_DOCUMENT_STATE_KEYSPACE)).unwrap();
    assert_eq!(
        reducer.join_requests()[0].decision.as_ref().unwrap().kind,
        JoinDecisionKind::Approved
    );
    let outbox: DocumentSyncOutboxRecord =
        postcard::from_bytes(value(DOCUMENT_SYNC_OUTBOX_KEYSPACE)).unwrap();
    assert!(
        matches!(outbox.event, DocumentSyncOutboxEvent::AdminOperation { event, .. }
        if matches!(event.op, AdminDocumentOperation::GroupJoinDecided { .. }))
    );
    assert!(!operation.is_complete());
    let effects = operation.step(Event::Storage(StorageEvent::Error {
        error: StorageError::TransactionConflict,
    }));
    assert!(matches!(effects.as_slice(),
        [Effect::Storage(StorageEffect::AbortTransaction { txn_id: aborted })] if *aborted == txn_id));
    assert_eq!(
        operation.finalize(),
        Err(GroupJoinError::Storage(StorageError::TransactionConflict))
    );
}
