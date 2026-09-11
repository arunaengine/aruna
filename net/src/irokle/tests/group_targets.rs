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
