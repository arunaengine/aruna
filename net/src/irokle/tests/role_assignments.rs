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

#[tokio::test]
async fn group_role_create_admin_operation_bootstraps_auth_doc_and_overlays_assignments() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([12; 32]);
    let group_id = Ulid::from_parts(31, 1);
    let role_id = Ulid::from_parts(32, 2);
    let assigned_user_id = UserId::local(Ulid::from_parts(33, 3), realm_id);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(34, 4), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::Group { group_id };
    let document_target = DocumentSyncTarget::GroupAuthorization { group_id };

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(35, 5),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::GroupRoleUserAssignmentAdded {
                role_id,
                user_id: assigned_user_id,
            },
        ),
    )
    .await
    .expect("assignment state applies before role exists");
    storage_batch_delete_to(
        &storage,
        vec![(
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
        )],
    )
    .await
    .expect("transient empty auth doc deletes");

    let role = test_admin_role_definition(
        role_id,
        "Group data steward",
        "/datasets/**",
        Permission::WRITE,
    );
    apply_admin_document_operation_to_storage(
        &storage,
        document_target,
        test_admin_event(
            Ulid::from_parts(36, 6),
            target.clone(),
            &actor,
            2,
            AdminDocumentOperation::GroupRoleCreated { role },
        ),
    )
    .await
    .expect("role create applies without pre-existing auth doc");

    let auth_doc = read_group_auth_doc(&storage, group_id).await;
    let auth_role = &auth_doc.roles[&role_id];
    assert_eq!(auth_role.name, "Group data steward");
    assert_eq!(
        auth_role.permissions,
        HashMap::from([("/datasets/**".to_string(), Permission::WRITE)])
    );
    assert_eq!(auth_role.assigned_users, HashSet::from([assigned_user_id]));

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
    assert_eq!(
        reducer_state.materialized_group_role_user_assignments(),
        BTreeMap::from([(role_id, BTreeSet::from([assigned_user_id]))])
    );
}

#[tokio::test]
async fn group_assignment_conflict_resolution_deletes_stale_conflict_and_materializes() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([11; 32]);
    let group_id = Ulid::from_parts(21, 1);
    let role_id = Ulid::from_parts(22, 2);
    let assigned_user_id = UserId::local(Ulid::from_parts(23, 3), realm_id);
    let actor = Actor {
        node_id: node(8),
        user_id: UserId::local(Ulid::from_parts(24, 4), realm_id),
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
    let assignment_path = group_role_user_assignment_path(&role_id, &assigned_user_id);
    let conflict_key = admin_document_reducer_conflict_key(&target, &assignment_path);
    let add_origin = node(9);
    let remove_origin = node(10);
    let add_event = AdminDocumentEvent {
        event_id: Ulid::from_parts(25, 5),
        target: target.clone(),
        origin_node_id: add_origin,
        origin_seq: 1,
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
    let conflicting_remove_event = AdminDocumentEvent {
        event_id: Ulid::from_parts(26, 6),
        target: target.clone(),
        origin_node_id: remove_origin,
        origin_seq: 1,
        observed: AdminDocumentClock::default(),
        actor: actor.clone(),
        op: AdminDocumentOperation::GroupRoleUserAssignmentRemoved {
            role_id,
            user_id: assigned_user_id,
        },
    };
    apply_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::GroupAuthorization { group_id },
        conflicting_remove_event,
    )
    .await
    .expect("conflicting remove applies");

    assert!(
        read_storage_value(
            &storage,
            ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
            conflict_key.clone(),
        )
        .await
        .is_some()
    );
    assert!(
        !read_group_auth_doc(&storage, group_id).await.roles[&role_id]
            .assigned_users
            .contains(&assigned_user_id)
    );

    let resolving_remove_event = AdminDocumentEvent {
        event_id: Ulid::from_parts(27, 7),
        target: target.clone(),
        origin_node_id: node(11),
        origin_seq: 1,
        observed: AdminDocumentClock::default()
            .with_observed(add_origin, 1)
            .with_observed(remove_origin, 1),
        actor,
        op: AdminDocumentOperation::GroupRoleUserAssignmentRemoved {
            role_id,
            user_id: assigned_user_id,
        },
    };
    apply_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::GroupAuthorization { group_id },
        resolving_remove_event,
    )
    .await
    .expect("resolving remove applies");

    assert_eq!(
        read_storage_value(&storage, ADMIN_DOCUMENT_CONFLICT_KEYSPACE, conflict_key).await,
        None
    );
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
        admin_document_reducer_state_key(&target),
    )
    .await
    .expect("reducer state exists");
    let reducer_state: AdminDocumentReducerState =
        postcard::from_bytes(&reducer_state).expect("reducer state decodes");
    assert!(reducer_state.conflicts.is_empty());
}

#[tokio::test]
async fn realm_assignment_conflicting_add_removes_existing_grant() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([14; 32]);
    let role_id = Ulid::from_parts(51, 1);
    let assigned_user_id = UserId::local(Ulid::from_parts(52, 2), realm_id);
    let actor = Actor {
        node_id: node(8),
        user_id: UserId::local(Ulid::from_parts(53, 3), realm_id),
        realm_id,
    };
    let target = AdminDocumentTarget::Realm { realm_id };
    let document_target = DocumentSyncTarget::RealmAuthorization { realm_id };
    let auth_doc = RealmAuthorizationDocument {
        realm_id,
        roles: HashMap::from([(role_id, test_role(role_id, [assigned_user_id]))]),
        operation_restrictions: Default::default(),
    };
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            document_target.clone(),
            auth_doc
                .to_bytes(&actor)
                .expect("auth doc serializes")
                .into(),
        )],
    )
    .await
    .expect("auth doc writes");

    let remove_origin = node(9);
    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        AdminDocumentEvent {
            event_id: Ulid::from_parts(54, 4),
            target: target.clone(),
            origin_node_id: remove_origin,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor: actor.clone(),
            op: AdminDocumentOperation::RealmRoleUserAssignmentRemoved {
                role_id,
                user_id: assigned_user_id,
            },
        },
    )
    .await
    .expect("remove assignment applies");
    storage_batch_write_to(
        &storage,
        vec![target_write_entry(
            document_target.clone(),
            auth_doc
                .to_bytes(&actor)
                .expect("auth doc serializes")
                .into(),
        )],
    )
    .await
    .expect("stale auth doc grant rewrites");

    let add_origin = node(10);
    let assignment_path = realm_role_user_assignment_path(&role_id, &assigned_user_id);
    apply_admin_document_operation_to_storage(
        &storage,
        document_target,
        AdminDocumentEvent {
            event_id: Ulid::from_parts(55, 5),
            target: target.clone(),
            origin_node_id: add_origin,
            origin_seq: 1,
            observed: AdminDocumentClock::default(),
            actor,
            op: AdminDocumentOperation::RealmRoleUserAssignmentAdded {
                role_id,
                user_id: assigned_user_id,
            },
        },
    )
    .await
    .expect("conflicting add applies");

    assert!(
        read_storage_value(
            &storage,
            ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
            admin_document_reducer_conflict_key(&target, &assignment_path),
        )
        .await
        .is_some()
    );
    assert!(
        !read_realm_auth_doc(&storage, realm_id).await.roles[&role_id]
            .assigned_users
            .contains(&assigned_user_id)
    );
}

#[tokio::test]
async fn realm_role_seed_then_assignment_admin_operations_materialize_existing_auth_doc() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([10; 32]);
    let role_id = Ulid::from_parts(2, 2);
    let assigned_user_id = UserId::local(Ulid::from_parts(3, 3), realm_id);
    let actor = Actor {
        node_id: node(8),
        user_id: UserId::local(Ulid::from_parts(4, 4), realm_id),
        realm_id,
    };
    let auth_doc = RealmAuthorizationDocument {
        realm_id,
        roles: HashMap::from([(
            role_id,
            Role {
                role_id,
                name: "realm_member".to_string(),
                permissions: HashMap::from([("/datasets".to_string(), Permission::READ)]),
                assigned_users: HashSet::new(),
            },
        )]),
        operation_restrictions: HashMap::new(),
    };
    storage_batch_write_to(
        &storage,
        vec![(
            AUTH_KEYSPACE.to_string(),
            (*realm_id.as_bytes()).into(),
            auth_doc
                .to_bytes(&actor)
                .expect("auth doc serializes")
                .into(),
        )],
    )
    .await
    .expect("auth doc writes");

    let target = AdminDocumentTarget::Realm { realm_id };
    let seed_event = AdminDocumentEvent {
        event_id: Ulid::from_parts(5, 5),
        target: target.clone(),
        origin_node_id: actor.node_id,
        origin_seq: 1,
        observed: AdminDocumentClock::default(),
        actor: actor.clone(),
        op: AdminDocumentOperation::RealmRoleAdded { role_id },
    };
    apply_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::RealmAuthorization { realm_id },
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
        op: AdminDocumentOperation::RealmRoleUserAssignmentAdded {
            role_id,
            user_id: assigned_user_id,
        },
    };
    apply_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::RealmAuthorization { realm_id },
        add_event,
    )
    .await
    .expect("add assignment applies");

    let stored_auth_doc =
        read_storage_value(&storage, AUTH_KEYSPACE, (*realm_id.as_bytes()).into())
            .await
            .expect("auth doc exists");
    let stored_auth_doc =
        RealmAuthorizationDocument::from_bytes(&stored_auth_doc).expect("auth doc decodes");
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
        reducer_state.materialized_realm_roles(),
        BTreeSet::from([role_id])
    );
    let assignment_path = realm_role_user_assignment_path(&role_id, &assigned_user_id);
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
        op: AdminDocumentOperation::RealmRoleUserAssignmentRemoved {
            role_id,
            user_id: assigned_user_id,
        },
    };
    apply_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::RealmAuthorization { realm_id },
        remove_event,
    )
    .await
    .expect("remove assignment applies");

    let stored_auth_doc =
        read_storage_value(&storage, AUTH_KEYSPACE, (*realm_id.as_bytes()).into())
            .await
            .expect("auth doc exists");
    let stored_auth_doc =
        RealmAuthorizationDocument::from_bytes(&stored_auth_doc).expect("auth doc decodes");
    assert!(
        !stored_auth_doc.roles[&role_id]
            .assigned_users
            .contains(&assigned_user_id)
    );
    let reducer_state = read_storage_value(
        &storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE,
        admin_document_reducer_state_key(&AdminDocumentTarget::Realm { realm_id }),
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
