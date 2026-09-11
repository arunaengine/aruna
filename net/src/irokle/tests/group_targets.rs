use super::*;

#[test]
fn visibility_conflicts_private() {
    let realm_id = RealmId::from_bytes([44; 32]);
    let user_id = UserId::local(Ulid::from_parts(210, 1), realm_id);
    let mut reducer = AdminDocumentReducerState::new(AdminDocumentTarget::User { user_id });
    let mut event = test_admin_event(
        Ulid::from_parts(211, 1),
        reducer.target.clone(),
        &test_actor(1, user_id, realm_id),
        1,
        AdminDocumentOperation::UserAttributeSet {
            key: "profile.visibility.name".into(),
            value: "public".into(),
        },
    );
    reducer.apply(&event).unwrap();
    event = test_admin_event(
        Ulid::from_parts(212, 1),
        reducer.target.clone(),
        &test_actor(2, user_id, realm_id),
        1,
        AdminDocumentOperation::UserAttributeSet {
            key: "profile.visibility.name".into(),
            value: "private".into(),
        },
    );
    reducer.apply(&event).unwrap();
    let user = materialize_user_admin_document_operation(user_id, None, &reducer, &event);
    assert_eq!(user.attributes["profile.visibility.name"], "private");
    assert!(!user.field_public("name"));
}

#[tokio::test]
async fn user_name_and_attribute_conflicts_fail_closed_incrementally() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([44; 32]);
    let user_id = UserId::local(Ulid::from_parts(210, 1), realm_id);
    let target = AdminDocumentTarget::User { user_id };

    apply_conflicting_user_name_and_attribute(&storage, user_id, realm_id).await;

    let user = read_user_doc(&storage, user_id).await;
    assert_eq!(user.name, "");
    assert!(!user.attributes.contains_key("department"));
    assert!(
        read_storage_value(
            &storage,
            ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
            admin_document_reducer_conflict_key(&target, USER_NAME_PATH),
        )
        .await
        .is_some()
    );
    assert!(
        read_storage_value(
            &storage,
            ADMIN_DOCUMENT_CONFLICT_KEYSPACE,
            admin_document_reducer_conflict_key(&target, &user_attribute_path("department")),
        )
        .await
        .is_some()
    );
}

#[tokio::test]
async fn group_created_admin_operation_bootstraps_missing_group_doc() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([35; 32]);
    let group_id = Ulid::from_parts(190, 1);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(191, 1), realm_id),
        realm_id,
    );

    apply_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::GroupAuthorization { group_id },
        test_admin_event(
            Ulid::from_parts(192, 1),
            AdminDocumentTarget::Group { group_id },
            &actor,
            1,
            AdminDocumentOperation::GroupCreated {
                realm_id,
                display_name: "Reduced group".to_string(),
                owner: actor.user_id,
            },
        ),
    )
    .await
    .expect("group create applies");

    assert_eq!(
        read_group_doc(&storage, group_id).await,
        Group {
            display_name: "Reduced group".to_string(),
            group_id,
            realm_id,
            owner: actor.user_id,
            roles: HashSet::new(),
        }
    );
    assert!(
        read_storage_value(
            &storage,
            GROUP_OWNER_INDEX_KEYSPACE,
            group_owner_index_key(actor.user_id, group_id).into(),
        )
        .await
        .is_some()
    );
}

#[tokio::test]
async fn rename_updates_row() {
    // A replicated rename re-materializes the stored group label.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([37; 32]);
    let group_id = Ulid::from_parts(198, 1);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(199, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::Group { group_id };
    let document_target = DocumentSyncTarget::GroupAuthorization { group_id };

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(200, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::GroupCreated {
                realm_id,
                display_name: "Engineering".to_string(),
                owner: actor.user_id,
            },
        ),
    )
    .await
    .expect("group create applies");

    let mut observed = AdminDocumentClock::default();
    observed.advance(actor.node_id, 1);
    let mut rename = test_admin_event(
        Ulid::from_parts(201, 1),
        target,
        &actor,
        2,
        AdminDocumentOperation::GroupDisplayNameSet {
            display_name: "Platform".to_string(),
        },
    );
    rename.observed = observed;
    apply_admin_document_operation_to_storage(&storage, document_target, rename)
        .await
        .expect("rename applies");

    assert_eq!(
        read_group_doc(&storage, group_id).await.display_name,
        "Platform"
    );
}

#[tokio::test]
async fn admits_group_rename() {
    let (_dir, storage) = test_storage();
    let (realm_id, group_id, actor) = seed_rename_group(&storage, None).await;
    assert_eq!(
        validate_rename(&storage, realm_id, group_id, &actor).await,
        AdminEventValidation::Accepted
    );
}

#[tokio::test]
async fn rejects_foreign_rename() {
    // The stored group belongs to another realm, so its label is not ours.
    let (_dir, storage) = test_storage();
    let foreign = RealmId::from_bytes([39; 32]);
    let (realm_id, group_id, actor) = seed_rename_group(&storage, Some(foreign)).await;
    assert!(matches!(
        validate_rename(&storage, realm_id, group_id, &actor).await,
        AdminEventValidation::Rejected(_)
    ));
}

/// Seeds a publisher-capable realm config plus one group owned by the actor.
/// `group_realm` overrides the realm the stored group claims.
async fn seed_rename_group(
    storage: &StorageHandle,
    group_realm: Option<RealmId>,
) -> (RealmId, Ulid, Actor) {
    let realm_id = RealmId::from_bytes([38; 32]);
    let group_id = Ulid::from_parts(202, 1);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(203, 1), realm_id),
        realm_id,
    );
    let mut config = RealmConfigDocument::new(realm_id, Vec::new(), 3);
    config.ensure_node(actor.node_id, RealmNodeKind::Server);
    let group = Group {
        display_name: "Engineering".to_string(),
        group_id,
        realm_id: group_realm.unwrap_or(realm_id),
        owner: actor.user_id,
        roles: HashSet::new(),
    };
    storage_batch_write_to(
        storage,
        vec![
            target_write_entry(
                DocumentSyncTarget::RealmConfig { realm_id },
                config.to_bytes(&actor).expect("config serializes").into(),
            ),
            (
                GROUP_KEYSPACE.to_string(),
                group_id.to_bytes().into(),
                group.to_bytes(&actor).expect("group serializes").into(),
            ),
        ],
    )
    .await
    .expect("fixture writes");
    (realm_id, group_id, actor)
}

async fn validate_rename(
    storage: &StorageHandle,
    realm_id: RealmId,
    group_id: Ulid,
    actor: &Actor,
) -> AdminEventValidation {
    let document_target = DocumentSyncTarget::GroupAuthorization { group_id };
    let placement = admin_test_placement();
    let topic_id = document_target.sync_topic_id(realm_id, &placement);
    let event = test_admin_event(
        Ulid::from_parts(204, 1),
        AdminDocumentTarget::Group { group_id },
        actor,
        1,
        AdminDocumentOperation::GroupDisplayNameSet {
            display_name: "Platform".to_string(),
        },
    );
    validate_replicated_admin_event(
        storage,
        topic_id,
        irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&actor.node_id)),
        &document_target,
        &event,
        realm_id,
        &placement,
        &sign_as_origin(&event, &placement),
        &mut ConfigValidationCache::default(),
    )
    .await
    .expect("validation runs")
}

#[tokio::test]
async fn group_role_create_admin_operation_after_group_created_updates_group_roles() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([36; 32]);
    let group_id = Ulid::from_parts(193, 1);
    let role_id = Ulid::from_parts(194, 1);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(195, 1), realm_id),
        realm_id,
    );
    let target = AdminDocumentTarget::Group { group_id };
    let document_target = DocumentSyncTarget::GroupAuthorization { group_id };

    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(196, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::GroupCreated {
                realm_id,
                display_name: "Reduced group".to_string(),
                owner: actor.user_id,
            },
        ),
    )
    .await
    .expect("group create applies");
    apply_admin_document_operation_to_storage(
        &storage,
        document_target,
        test_admin_event(
            Ulid::from_parts(197, 1),
            target,
            &actor,
            2,
            AdminDocumentOperation::GroupRoleCreated {
                role: test_admin_role_definition(
                    role_id,
                    "Reduced group role",
                    "/group/reduced/**",
                    Permission::WRITE,
                ),
            },
        ),
    )
    .await
    .expect("role create applies");

    assert_eq!(
        read_group_doc(&storage, group_id).await.roles,
        HashSet::from([role_id])
    );
    assert!(
        read_group_auth_doc(&storage, group_id)
            .await
            .roles
            .contains_key(&role_id)
    );
}

#[tokio::test]
async fn replicated_role_confined() {
    // An allowed publisher cannot replicate a role granting outside its group.
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([71; 32]);
    let group_id = Ulid::from_parts(210, 1);
    let role_id = Ulid::from_parts(211, 1);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(212, 1), realm_id),
        realm_id,
    );
    let document_target = DocumentSyncTarget::GroupAuthorization { group_id };
    let placement = admin_test_placement();
    let topic_id = document_target.sync_topic_id(realm_id, &placement);
    let actor_id = irokle_crate::actor_id_for(topic_id, node_id_to_peer_id(&actor.node_id));

    let event = test_admin_event(
        Ulid::from_parts(213, 1),
        AdminDocumentTarget::Group { group_id },
        &actor,
        1,
        AdminDocumentOperation::GroupRoleCreated {
            role: test_admin_role_definition(role_id, "escalated", "/**", Permission::WRITE),
        },
    );

    let outcome = validate_replicated_admin_event(
        &storage,
        topic_id,
        actor_id,
        &document_target,
        &event,
        realm_id,
        &placement,
        &sign_as_origin(&event, &placement),
        &mut ConfigValidationCache::default(),
    )
    .await
    .expect("validation runs");
    assert!(matches!(outcome, AdminEventValidation::Rejected(_)));
}

#[tokio::test]
async fn group_role_create_admin_operation_updates_existing_group_roles() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([27; 32]);
    let group_id = Ulid::from_parts(160, 1);
    let existing_role_id = Ulid::from_parts(161, 1);
    let role_id = Ulid::from_parts(162, 1);
    let conflicted_role_id = Ulid::from_parts(163, 1);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(164, 1), realm_id),
        realm_id,
    );
    let group = Group {
        display_name: "Durable group".to_string(),
        group_id,
        realm_id,
        owner: actor.user_id,
        roles: HashSet::from([existing_role_id, conflicted_role_id]),
    };
    storage_batch_write_to(
        &storage,
        vec![(
            GROUP_KEYSPACE.to_string(),
            group_id.to_bytes().into(),
            group.to_bytes(&actor).expect("group serializes").into(),
        )],
    )
    .await
    .expect("group writes");

    let target = AdminDocumentTarget::Group { group_id };
    let document_target = DocumentSyncTarget::GroupAuthorization { group_id };
    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(165, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::GroupRoleCreated {
                role: test_admin_role_definition(
                    role_id,
                    "Reduced group role",
                    "/group/reduced/**",
                    Permission::WRITE,
                ),
            },
        ),
    )
    .await
    .expect("role create applies");

    let conflict_actor_a = test_actor(
        9,
        UserId::local(Ulid::from_parts(166, 1), realm_id),
        realm_id,
    );
    let conflict_actor_b = test_actor(
        10,
        UserId::local(Ulid::from_parts(167, 1), realm_id),
        realm_id,
    );
    apply_admin_document_operation_to_storage(
        &storage,
        document_target.clone(),
        test_admin_event(
            Ulid::from_parts(168, 1),
            target.clone(),
            &conflict_actor_a,
            1,
            AdminDocumentOperation::GroupRoleCreated {
                role: test_admin_role_definition(
                    conflicted_role_id,
                    "First conflicted role",
                    "/group/conflict-a/**",
                    Permission::READ,
                ),
            },
        ),
    )
    .await
    .expect("first conflict role applies");
    apply_admin_document_operation_to_storage(
        &storage,
        document_target,
        test_admin_event(
            Ulid::from_parts(169, 1),
            target,
            &conflict_actor_b,
            1,
            AdminDocumentOperation::GroupRoleCreated {
                role: test_admin_role_definition(
                    conflicted_role_id,
                    "Second conflicted role",
                    "/group/conflict-b/**",
                    Permission::WRITE,
                ),
            },
        ),
    )
    .await
    .expect("second conflict role applies");

    let stored_group = read_group_doc(&storage, group_id).await;
    assert_eq!(stored_group.display_name, group.display_name);
    assert_eq!(stored_group.realm_id, realm_id);
    assert_eq!(
        stored_group.roles,
        HashSet::from([existing_role_id, role_id])
    );
}

#[tokio::test]
async fn group_role_create_admin_operation_does_not_create_missing_group() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([28; 32]);
    let group_id = Ulid::from_parts(170, 1);
    let role_id = Ulid::from_parts(171, 1);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(172, 1), realm_id),
        realm_id,
    );

    apply_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::GroupAuthorization { group_id },
        test_admin_event(
            Ulid::from_parts(173, 1),
            AdminDocumentTarget::Group { group_id },
            &actor,
            1,
            AdminDocumentOperation::GroupRoleCreated {
                role: test_admin_role_definition(
                    role_id,
                    "Reduced group role",
                    "/group/reduced/**",
                    Permission::WRITE,
                ),
            },
        ),
    )
    .await
    .expect("role create applies");

    assert_eq!(
        read_storage_value(&storage, GROUP_KEYSPACE, group_id.to_bytes().into()).await,
        None
    );
    assert!(
        read_group_auth_doc(&storage, group_id)
            .await
            .roles
            .contains_key(&role_id)
    );
}

#[tokio::test]
async fn group_role_remove_admin_operation_updates_group_and_auth_docs() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([37; 32]);
    let group_id = Ulid::from_parts(198, 1);
    let role_id = Ulid::from_parts(199, 1);
    let assigned_user_id = UserId::local(Ulid::from_parts(200, 1), realm_id);
    let actor = test_actor(
        8,
        UserId::local(Ulid::from_parts(201, 1), realm_id),
        realm_id,
    );
    let group = Group {
        display_name: "Durable group".to_string(),
        group_id,
        realm_id,
        owner: actor.user_id,
        roles: HashSet::from([role_id]),
    };
    let auth_doc = GroupAuthorizationDocument {
        group_id,
        policies: Vec::new(),
        roles: HashMap::from([(
            role_id,
            Role {
                role_id,
                name: "custom_role".to_string(),
                permissions: HashMap::from([("/group/custom/**".to_string(), Permission::READ)]),
                assigned_users: HashSet::from([assigned_user_id]),
            },
        )]),
    };
    storage_batch_write_to(
        &storage,
        vec![
            (
                GROUP_KEYSPACE.to_string(),
                group_id.to_bytes().into(),
                group.to_bytes(&actor).expect("group serializes").into(),
            ),
            (
                AUTH_KEYSPACE.to_string(),
                group_id.to_bytes().into(),
                auth_doc
                    .to_bytes(&actor)
                    .expect("auth doc serializes")
                    .into(),
            ),
        ],
    )
    .await
    .expect("group and auth docs write");

    let target = AdminDocumentTarget::Group { group_id };
    apply_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::GroupAuthorization { group_id },
        test_admin_event(
            Ulid::from_parts(202, 1),
            target.clone(),
            &actor,
            1,
            AdminDocumentOperation::GroupRoleRemoved { role_id },
        ),
    )
    .await
    .expect("role remove applies");

    assert!(
        !read_group_doc(&storage, group_id)
            .await
            .roles
            .contains(&role_id)
    );
    assert!(
        !read_group_auth_doc(&storage, group_id)
            .await
            .roles
            .contains_key(&role_id)
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
    assert!(!reducer_state.materialized_group_roles().contains(&role_id));
}

#[tokio::test]
async fn user_admin_operation_applies_reducer_state_and_materializes_user() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([7; 32]);
    let user_id = UserId::local(Ulid::from_parts(1, 1), realm_id);
    let actor = Actor {
        node_id: node(8),
        user_id,
        realm_id,
    };
    let original = User {
        user_id,
        name: "Alice".to_string(),
        subject_ids: vec!["subject-1".to_string()],
        alias_user_ids: Default::default(),
        attributes: HashMap::from([("department".to_string(), "physics".to_string())]),
    };
    storage_batch_write_to(
        &storage,
        vec![(
            USER_KEYSPACE.to_string(),
            user_id.to_bytes().into(),
            original.to_bytes(&actor).expect("user serializes").into(),
        )],
    )
    .await
    .expect("original user writes");

    let event = AdminDocumentEvent {
        event_id: Ulid::from_parts(2, 1),
        target: AdminDocumentTarget::User { user_id },
        origin_node_id: actor.node_id,
        origin_seq: 1,
        observed: AdminDocumentClock::default(),
        actor,
        op: AdminDocumentOperation::UserNameSet {
            name: "Alice Updated".to_string(),
        },
    };
    apply_user_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::User { user_id },
        event,
    )
    .await
    .expect("admin operation applies");

    let stored_user = read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into())
        .await
        .expect("user exists");
    let user = User::from_bytes(&stored_user).expect("user decodes");
    assert_eq!(user.name, "Alice Updated");
    assert_eq!(user.attributes["department"], "physics");
    let reducer_state = read_storage_value(
        &storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE,
        admin_document_reducer_state_key(&AdminDocumentTarget::User { user_id }),
    )
    .await
    .expect("reducer state exists");
    let reducer_state: AdminDocumentReducerState =
        postcard::from_bytes(&reducer_state).expect("reducer state decodes");
    assert_eq!(
        reducer_state.materialized_user_name().as_deref(),
        Some("Alice Updated")
    );
    assert_eq!(
        read_storage_value(
            &storage,
            USER_SUBJECT_INDEX_KEYSPACE,
            subject_index_key("subject-1")
        )
        .await,
        Some(subject_index_value(user_id))
    );
}

#[tokio::test]
async fn stale_user_admin_operation_is_recorded_without_rematerializing_older_value() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([7; 32]);
    let user_id = UserId::local(Ulid::from_parts(3, 1), realm_id);
    let actor = Actor {
        node_id: node(8),
        user_id,
        realm_id,
    };
    let target = AdminDocumentTarget::User { user_id };
    let newer = AdminDocumentEvent {
        event_id: Ulid::from_parts(4, 2),
        target: target.clone(),
        origin_node_id: actor.node_id,
        origin_seq: 2,
        observed: AdminDocumentClock::default(),
        actor: actor.clone(),
        op: AdminDocumentOperation::UserNameSet {
            name: "newer".to_string(),
        },
    };
    let older = AdminDocumentEvent {
        event_id: Ulid::from_parts(4, 1),
        target: target.clone(),
        origin_node_id: actor.node_id,
        origin_seq: 1,
        observed: AdminDocumentClock::default(),
        actor,
        op: AdminDocumentOperation::UserNameSet {
            name: "older".to_string(),
        },
    };

    for event in [newer, older.clone(), older.clone()] {
        apply_user_admin_document_operation_to_storage(
            &storage,
            DocumentSyncTarget::User { user_id },
            event,
        )
        .await
        .expect("out-of-order admin operation applies");
    }

    let stored_user = read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into())
        .await
        .expect("user exists");
    let user = User::from_bytes(&stored_user).expect("user decodes");
    assert_eq!(user.name, "newer");
    let reducer_state = read_storage_value(
        &storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE,
        admin_document_reducer_state_key(&target),
    )
    .await
    .expect("reducer state exists");
    let reducer_state: AdminDocumentReducerState =
        postcard::from_bytes(&reducer_state).expect("reducer state decodes");
    assert_eq!(reducer_state.applied_event_ids.len(), 2);
    assert!(reducer_state.applied_event_ids.contains(&older.event_id));
    assert_eq!(reducer_state.clock.sequence_for(&older.origin_node_id), 2);
}

#[tokio::test]
async fn user_subject_add_admin_operation_creates_user_and_subject_index() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([7; 32]);
    let user_id = UserId::local(Ulid::from_parts(10, 1), realm_id);
    let actor = Actor {
        node_id: node(8),
        user_id,
        realm_id,
    };
    let event = AdminDocumentEvent {
        event_id: Ulid::from_parts(11, 1),
        target: AdminDocumentTarget::User { user_id },
        origin_node_id: actor.node_id,
        origin_seq: 1,
        observed: AdminDocumentClock::default(),
        actor,
        op: AdminDocumentOperation::UserSubjectIdAdded {
            subject_id: "subject-created".to_string(),
        },
    };

    apply_user_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::User { user_id },
        event,
    )
    .await
    .expect("subject add applies");

    let stored_user = read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into())
        .await
        .expect("user exists");
    let user = User::from_bytes(&stored_user).expect("user decodes");
    assert_eq!(user.subject_ids, vec!["subject-created".to_string()]);
    assert_eq!(
        read_storage_value(
            &storage,
            USER_SUBJECT_INDEX_KEYSPACE,
            subject_index_key("subject-created")
        )
        .await,
        Some(subject_index_value(user_id))
    );
    let claims = read_storage_value(
        &storage,
        USER_SUBJECT_CLAIMS_KEYSPACE,
        subject_index_key("subject-created"),
    )
    .await
    .expect("subject claims exist");
    assert_eq!(
        postcard::from_bytes::<BTreeSet<UserId>>(&claims).expect("claims decode"),
        BTreeSet::from([user_id])
    );
}

#[tokio::test]
async fn user_subject_remove_admin_operation_deletes_stale_subject_index() {
    let (_dir, storage) = test_storage();
    let realm_id = RealmId::from_bytes([7; 32]);
    let user_id = UserId::local(Ulid::from_parts(12, 1), realm_id);
    let actor = Actor {
        node_id: node(8),
        user_id,
        realm_id,
    };
    let original = User {
        user_id,
        name: "Alice".to_string(),
        subject_ids: vec!["subject-removed".to_string()],
        alias_user_ids: Default::default(),
        attributes: Default::default(),
    };
    storage_batch_write_to(
        &storage,
        vec![
            (
                USER_KEYSPACE.to_string(),
                user_id.to_bytes().into(),
                original.to_bytes(&actor).expect("user serializes").into(),
            ),
            (
                USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                subject_index_key("subject-removed"),
                subject_index_value(user_id),
            ),
        ],
    )
    .await
    .expect("original user and index write");

    let event = AdminDocumentEvent {
        event_id: Ulid::from_parts(13, 1),
        target: AdminDocumentTarget::User { user_id },
        origin_node_id: actor.node_id,
        origin_seq: 1,
        observed: AdminDocumentClock::default(),
        actor,
        op: AdminDocumentOperation::UserSubjectIdRemoved {
            subject_id: "subject-removed".to_string(),
        },
    };
    apply_user_admin_document_operation_to_storage(
        &storage,
        DocumentSyncTarget::User { user_id },
        event,
    )
    .await
    .expect("subject remove applies");

    let stored_user = read_storage_value(&storage, USER_KEYSPACE, user_id.to_bytes().into())
        .await
        .expect("user exists");
    let user = User::from_bytes(&stored_user).expect("user decodes");
    assert!(user.subject_ids.is_empty());
    assert_eq!(
        read_storage_value(
            &storage,
            USER_SUBJECT_INDEX_KEYSPACE,
            subject_index_key("subject-removed")
        )
        .await,
        None
    );
}
