use super::*;

pub(in crate::document_sync) fn target_write_entry(
    target: DocumentSyncTarget,
    value: Value,
) -> (String, ByteView, Value) {
    (
        target.storage_keyspace().to_string(),
        target.storage_key(),
        value,
    )
}

pub(in crate::document_sync) async fn apply_create_event(
    storage: &StorageHandle,
    event: &MetadataCreateEventRecord,
    target: DocumentSyncTarget,
    bytes: Vec<u8>,
) -> Result<()> {
    for _ in 0..2 {
        let txn_id = start_storage_transaction(storage).await?;
        match create_fence_txn(storage, event, txn_id).await {
            Ok(true) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Ok(());
            }
            Ok(false) => {}
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
        let writes = vec![(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            ByteView::from(bytes.clone()),
        )];
        match storage_batch_delete_and_write_in_transaction(storage, txn_id, Vec::new(), writes)
            .await
        {
            Ok(()) => return Ok(()),
            Err(NetError::Storage(StorageError::TransactionConflict)) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
    }
    Err(NetError::Dht(
        "metadata create admission conflicted twice".to_string(),
    ))
}

pub(in crate::document_sync) fn overlay_group_reducer_materialization(
    group: &mut Group,
    reducer_state: &AdminDocumentReducerState,
) {
    if !reducer_state
        .conflicts
        .contains_key(GROUP_DISPLAY_NAME_PATH)
        && let Some(display_name) = reducer_state.materialized_group_display_name()
    {
        group.display_name = display_name;
    }

    if !reducer_state.conflicts.contains_key(GROUP_REALM_ID_PATH)
        && let Some(realm_id) = reducer_state.materialized_group_realm_id()
    {
        group.realm_id = realm_id;
    }

    if !reducer_state.conflicts.contains_key(GROUP_OWNER_PATH)
        && let Some(owner) = reducer_state.materialized_group_owner()
    {
        group.owner = owner;
    }

    overlay_group_role_set_reducer_materialization(group, reducer_state);
}

fn overlay_group_role_set_reducer_materialization(
    group: &mut Group,
    reducer_state: &AdminDocumentReducerState,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some(role_id) = group_role_id_from_path(path) {
            group.roles.remove(&role_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some(role_id) = group_role_id_from_path(path) else {
            continue;
        };
        group.roles.remove(&role_id);
        if version.value.is_some() && !reducer_state.conflicts.contains_key(path) {
            group.roles.insert(role_id);
        }
    }
}

fn group_metadata_conflicted(reducer_state: &AdminDocumentReducerState) -> bool {
    reducer_state
        .conflicts
        .contains_key(GROUP_DISPLAY_NAME_PATH)
        || reducer_state.conflicts.contains_key(GROUP_REALM_ID_PATH)
        || reducer_state.conflicts.contains_key(GROUP_OWNER_PATH)
}

pub(in crate::document_sync) fn group_reducer_materialized_group(
    group_id: Ulid,
    reducer_state: &AdminDocumentReducerState,
) -> Option<Group> {
    if group_metadata_conflicted(reducer_state) {
        return None;
    }

    Some(Group {
        display_name: reducer_state.materialized_group_display_name()?,
        group_id,
        realm_id: reducer_state.materialized_group_realm_id()?,
        owner: reducer_state.materialized_group_owner()?,
        roles: reducer_state
            .materialized_group_roles()
            .into_iter()
            .collect(),
    })
}

pub(in crate::document_sync) fn overlay_group_authorization_role_assignment_reducer_materialization(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    role_id: RoleId,
) {
    overlay_group_authorization_assignment_reducer_materialization(
        auth_doc,
        reducer_state,
        Some(role_id),
    );
}

fn overlay_group_authorization_assignment_reducer_materialization(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    only_role_id: Option<RoleId>,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some((role_id, user_id)) = group_role_user_assignment_from_path(path)
            && only_role_id.is_none_or(|only_role_id| only_role_id == role_id)
            && let Some(role) = auth_doc.roles.get_mut(&role_id)
        {
            role.assigned_users.remove(&user_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some((role_id, user_id)) = group_role_user_assignment_from_path(path) else {
            continue;
        };
        if only_role_id.is_some_and(|only_role_id| only_role_id != role_id) {
            continue;
        }
        let Some(role) = auth_doc.roles.get_mut(&role_id) else {
            continue;
        };
        role.assigned_users.remove(&user_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if version
            .value
            .as_deref()
            .and_then(|value| UserId::from_string(value).ok())
            .is_some_and(|materialized_user_id| materialized_user_id == user_id)
        {
            role.assigned_users.insert(user_id);
        }
    }
}

pub(in crate::document_sync) fn overlay_realm_authorization_role_assignment_reducer_materialization(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    role_id: RoleId,
) {
    overlay_realm_authorization_assignment_reducer_materialization(
        auth_doc,
        reducer_state,
        Some(role_id),
    );
}

fn overlay_realm_authorization_assignment_reducer_materialization(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    only_role_id: Option<RoleId>,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some((role_id, user_id)) = realm_role_user_assignment_from_path(path)
            && only_role_id.is_none_or(|only_role_id| only_role_id == role_id)
            && let Some(role) = auth_doc.roles.get_mut(&role_id)
        {
            role.assigned_users.remove(&user_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some((role_id, user_id)) = realm_role_user_assignment_from_path(path) else {
            continue;
        };
        if only_role_id.is_some_and(|only_role_id| only_role_id != role_id) {
            continue;
        }
        let Some(role) = auth_doc.roles.get_mut(&role_id) else {
            continue;
        };
        role.assigned_users.remove(&user_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if version
            .value
            .as_deref()
            .and_then(|value| UserId::from_string(value).ok())
            .is_some_and(|materialized_user_id| materialized_user_id == user_id)
        {
            role.assigned_users.insert(user_id);
        }
    }
}
