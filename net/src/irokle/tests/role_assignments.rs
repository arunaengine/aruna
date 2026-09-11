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
