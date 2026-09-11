use super::*;

#[tokio::test]
async fn concurrent_user_subject_claims_converge_and_promote_on_removal() {
    let (_left_dir, left) = test_storage();
    let (_right_dir, right) = test_storage();
    let realm_id = RealmId::from_bytes([8; 32]);
    let mut user_ids = [
        UserId::local(Ulid::from_parts(20, 1), realm_id),
        UserId::local(Ulid::from_parts(21, 1), realm_id),
    ];
    user_ids.sort();
    let subject_id = "shared-subject".to_string();
    let actors = [
        test_actor(20, user_ids[0], realm_id),
        test_actor(21, user_ids[1], realm_id),
    ];
    let additions = actors.each_ref().map(|actor| {
        test_admin_event(
            Ulid::generate(),
            AdminDocumentTarget::User {
                user_id: actor.user_id,
            },
            actor,
            1,
            AdminDocumentOperation::UserSubjectIdAdded {
                subject_id: subject_id.clone(),
            },
        )
    });

    for (storage, order) in [(&left, [0, 1]), (&right, [1, 0])] {
        for index in order {
            apply_user_admin_document_operation_to_storage(
                storage,
                DocumentSyncTarget::User {
                    user_id: user_ids[index],
                },
                additions[index].clone(),
            )
            .await
            .expect("subject claim applies");
        }
    }

    for storage in [&left, &right] {
        let claims = read_storage_value(
            storage,
            USER_SUBJECT_CLAIMS_KEYSPACE,
            subject_index_key(&subject_id),
        )
        .await
        .expect("subject claims exist");
        assert_eq!(
            postcard::from_bytes::<BTreeSet<UserId>>(&claims).expect("claims decode"),
            BTreeSet::from(user_ids)
        );
        assert_eq!(
            read_storage_value(
                storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key(&subject_id),
            )
            .await,
            Some(subject_index_value(user_ids[0]))
        );
    }

    let mut removal = test_admin_event(
        Ulid::generate(),
        AdminDocumentTarget::User {
            user_id: user_ids[0],
        },
        &actors[0],
        2,
        AdminDocumentOperation::UserSubjectIdRemoved {
            subject_id: subject_id.clone(),
        },
    );
    removal.observed.advance(actors[0].node_id, 1);
    for storage in [&left, &right] {
        apply_user_admin_document_operation_to_storage(
            storage,
            DocumentSyncTarget::User {
                user_id: user_ids[0],
            },
            removal.clone(),
        )
        .await
        .expect("canonical subject removal applies");
        assert_eq!(
            read_storage_value(
                storage,
                USER_SUBJECT_INDEX_KEYSPACE,
                subject_index_key(&subject_id),
            )
            .await,
            Some(subject_index_value(user_ids[1]))
        );
    }
}

#[tokio::test]
async fn group_role_seed_then_assignment_admin_operations_materialize_existing_auth_doc() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([9; 32]);
    let group_id = Ulid::from_parts(1, 1);
    let role_id = Ulid::from_parts(2, 2);
    let assigned_user_id = UserId::local(Ulid::from_parts(3, 3), realm_id);
    let actor = Actor {
        node_id: node(8),
        user_id: UserId::local(Ulid::from_parts(4, 4), realm_id),
        realm_id,
    };
    let auth_doc = GroupAuthorizationDocument {
        group_id,
        policies: Vec::new(),
        roles: HashMap::from([(
            role_id,
            Role {
                role_id,
                name: "member".to_string(),
                permissions: HashMap::from([("/datasets".to_string(), Permission::READ)]),
                assigned_users: HashSet::new(),
            },
        )]),
    };
    storage_batch_write_to(
        &storage,
        vec![(
            AUTH_KEYSPACE.to_string(),
            group_id.to_bytes().into(),
            auth_doc
                .to_bytes(&actor)
                .expect("auth doc serializes")
                .into(),
        )],
    )
    .await
    .expect("auth doc writes");

    let target = AdminDocumentTarget::Group { group_id };
    let seed_event = AdminDocumentEvent {
        event_id: Ulid::from_parts(5, 5),
        target: target.clone(),
        origin_node_id: actor.node_id,
        origin_seq: 1,
        observed: AdminDocumentClock::default(),
        actor: actor.clone(),
        op: AdminDocumentOperation::GroupRoleAdded { role_id },
    };
    apply_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::GroupAuthorization { group_id },
        seed_event,
    )
    .await
    .expect("role seed applies");

    let add_event = AdminDocumentEvent {
        event_id: Ulid::from_parts(6, 6),
        target: target.clone(),
        origin_node_id: actor.node_id,
        origin_seq: 2,
        observed: AdminDocumentClock::default(),
        actor: actor.clone(),
        op: AdminDocumentOperation::GroupRoleUserAssignmentAdded {
            role_id,
            user_id: assigned_user_id,
        },
    };
    apply_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::GroupAuthorization { group_id },
        add_event,
    )
    .await
    .expect("add assignment applies");

    let stored_auth_doc = read_storage_value(&storage, AUTH_KEYSPACE, group_id.to_bytes().into())
        .await
        .expect("auth doc exists");
    let stored_auth_doc =
        GroupAuthorizationDocument::from_bytes(&stored_auth_doc).expect("auth doc decodes");
    assert!(
        stored_auth_doc.roles[&role_id]
            .assigned_users
            .contains(&assigned_user_id)
    );
    let reducer_state = read_storage_value(
        &storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE,
        admin_document_reducer_state_key(&target),
    )
    .await
    .expect("reducer state exists");
    let reducer_state: AdminDocumentReducerState =
        postcard::from_bytes(&reducer_state).expect("reducer state decodes");
    assert!(reducer_state.conflicts.is_empty());
    assert_eq!(
        reducer_state.materialized_group_roles(),
        BTreeSet::from([role_id])
    );
    let assignment_path = group_role_user_assignment_path(&role_id, &assigned_user_id);
    assert_eq!(
        reducer_state
            .user_subject_ids
            .get(&assignment_path)
            .and_then(|version| version.value.clone()),
        Some(assigned_user_id.to_string())
    );

    let remove_event = AdminDocumentEvent {
        event_id: Ulid::from_parts(7, 7),
        target,
        origin_node_id: actor.node_id,
        origin_seq: 3,
        observed: AdminDocumentClock::default(),
        actor,
        op: AdminDocumentOperation::GroupRoleUserAssignmentRemoved {
            role_id,
            user_id: assigned_user_id,
        },
    };
    apply_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::GroupAuthorization { group_id },
        remove_event,
    )
    .await
    .expect("remove assignment applies");

    let stored_auth_doc = read_storage_value(&storage, AUTH_KEYSPACE, group_id.to_bytes().into())
        .await
        .expect("auth doc exists");
    let stored_auth_doc =
        GroupAuthorizationDocument::from_bytes(&stored_auth_doc).expect("auth doc decodes");
    assert!(
        !stored_auth_doc.roles[&role_id]
            .assigned_users
            .contains(&assigned_user_id)
    );
    let reducer_state = read_storage_value(
        &storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE,
        admin_document_reducer_state_key(&AdminDocumentTarget::Group { group_id }),
    )
    .await
    .expect("reducer state exists");
    let reducer_state: AdminDocumentReducerState =
        postcard::from_bytes(&reducer_state).expect("reducer state decodes");
    assert_eq!(
        reducer_state
            .user_subject_ids
            .get(&assignment_path)
            .and_then(|version| version.value.clone()),
        None
    );
}
