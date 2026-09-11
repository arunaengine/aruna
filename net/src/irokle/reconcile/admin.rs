use super::*;

pub(in crate::document_sync) async fn apply_admin_document_operation_to_storage(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    match (&document_target, &event.target) {
        (DocumentSyncTarget::User { .. }, AdminDocumentTarget::User { .. }) => {
            apply_user_admin_document_operation_to_storage(storage, document_target, event).await
        }
        (DocumentSyncTarget::GroupAuthorization { .. }, AdminDocumentTarget::Group { .. }) => {
            apply_group_authorization_admin_document_operation_to_storage(
                storage,
                document_target,
                event,
            )
            .await
        }
        (DocumentSyncTarget::RealmAuthorization { .. }, AdminDocumentTarget::Realm { .. }) => {
            apply_realm_authorization_admin_document_operation_to_storage(
                storage,
                document_target,
                event,
            )
            .await
        }
        (DocumentSyncTarget::RealmConfig { .. }, AdminDocumentTarget::RealmConfig { .. }) => {
            apply_realm_config_admin_document_operation_to_storage(storage, document_target, event)
                .await
        }
        _ => Err(NetError::Bootstrap(
            "admin document operation target does not match document sync target".to_string(),
        )),
    }
}

pub(in crate::document_sync) async fn persist_stale_admin_document_event(
    storage: &StorageHandle,
    apply_status: AdminDocumentApplyStatus,
    reducer_state: &AdminDocumentReducerState,
) -> Result<bool> {
    match apply_status {
        AdminDocumentApplyStatus::Applied => Ok(false),
        AdminDocumentApplyStatus::Duplicate => Ok(true),
        AdminDocumentApplyStatus::Redundant | AdminDocumentApplyStatus::StaleOriginSequence => {
            storage_batch_write_to(
                storage,
                vec![
                    admin_document_reducer_state_write_entry(reducer_state)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                ],
            )
            .await?;
            Ok(true)
        }
    }
}

pub(in crate::document_sync) async fn apply_user_admin_document_operation_to_storage(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    let DocumentSyncTarget::User { user_id } = document_target.clone() else {
        return Err(NetError::Bootstrap(
            "admin document operation sync only supports user targets".to_string(),
        ));
    };
    let AdminDocumentTarget::User {
        user_id: event_user_id,
    } = event.target.clone()
    else {
        return Err(NetError::Bootstrap(
            "admin document operation payload target is not a user".to_string(),
        ));
    };
    if event_user_id != user_id {
        return Err(NetError::Bootstrap(format!(
            "replicated user admin operation target {user_id} does not match payload user id {event_user_id}"
        )));
    }
    if !matches!(
        &event.op,
        AdminDocumentOperation::UserNameSet { .. }
            | AdminDocumentOperation::UserSubjectIdAdded { .. }
            | AdminDocumentOperation::UserSubjectIdRemoved { .. }
            | AdminDocumentOperation::UserAttributeSet { .. }
            | AdminDocumentOperation::UserAttributeRemoved { .. }
    ) {
        return Err(NetError::Bootstrap(
            "admin document operation sync only supports user name, subject, and attribute updates"
                .to_string(),
        ));
    }
    let changed_subject_id = match &event.op {
        AdminDocumentOperation::UserSubjectIdAdded { subject_id }
        | AdminDocumentOperation::UserSubjectIdRemoved { subject_id } => Some(subject_id.clone()),
        _ => None,
    };

    let previous_state = storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        admin_document_reducer_state_key(&event.target),
    )
    .await?
    .map(|bytes| decode_admin_document_reducer_state(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    let apply_status = reducer_state
        .apply(&event)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if persist_stale_admin_document_event(storage, apply_status, &reducer_state).await? {
        return Ok(());
    }

    let previous_user = storage_read_from(
        storage,
        document_target.storage_keyspace().to_string(),
        document_target.storage_key(),
    )
    .await?
    .map(|bytes| User::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let user = materialize_user_admin_document_operation(
        user_id,
        previous_user.as_ref(),
        &reducer_state,
        &event,
    );

    let mut writes = vec![
        (
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
            user.to_bytes(&event.actor)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into(),
        ),
        admin_document_reducer_state_write_entry(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    ];
    writes.extend(
        admin_document_conflict_write_entries(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );

    let deletes =
        stale_admin_document_conflict_delete_entries(previous_state.as_ref(), Some(&reducer_state));
    let subject_ids = changed_subject_id
        .map(|subject_id| vec![subject_id])
        .unwrap_or_else(|| user.subject_ids.clone());
    if subject_ids.is_empty() {
        return storage_batch_delete_and_write_transactionally(storage, deletes, writes).await;
    }

    // A transient SSI conflict must never become stream-fatal: an aborted
    // inbound apply leaves ops without meta and wedges the topic. Local
    // interleavings are finite, so retry with yields; the bound stays a
    // safety valve against a genuine livelock.
    for _ in 0..APPLY_CONFLICT_ATTEMPTS {
        tokio::task::yield_now().await;
        let txn_id = start_storage_transaction(storage).await?;
        let mut attempt_writes = writes.clone();
        let mut attempt_deletes = deletes.clone();
        for subject_id in &subject_ids {
            let subject_key = subject_index_key(subject_id);
            let mut claims = match storage_read_from_transaction(
                storage,
                USER_SUBJECT_CLAIMS_KEYSPACE.to_string(),
                subject_key.clone(),
                Some(txn_id),
            )
            .await?
            {
                Some(bytes) => postcard::from_bytes::<BTreeSet<UserId>>(&bytes)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                None => {
                    let mut claims = BTreeSet::new();
                    if let Some(bytes) = storage_read_from_transaction(
                        storage,
                        USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                        subject_key.clone(),
                        Some(txn_id),
                    )
                    .await?
                    {
                        claims.insert(
                            UserId::from_storage_key(&bytes)
                                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                        );
                    }
                    claims
                }
            };
            if user.subject_ids.contains(subject_id) {
                claims.insert(user_id);
            } else {
                claims.remove(&user_id);
            }

            if let Some(canonical_user_id) = claims.first().copied() {
                attempt_writes.push((
                    USER_SUBJECT_CLAIMS_KEYSPACE.to_string(),
                    subject_key.clone(),
                    postcard::to_allocvec(&claims)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?
                        .into(),
                ));
                attempt_writes.push((
                    USER_SUBJECT_INDEX_KEYSPACE.to_string(),
                    subject_key,
                    subject_index_value(canonical_user_id),
                ));
            } else {
                attempt_deletes.push((
                    USER_SUBJECT_CLAIMS_KEYSPACE.to_string(),
                    subject_key.clone(),
                ));
                attempt_deletes.push((USER_SUBJECT_INDEX_KEYSPACE.to_string(), subject_key));
            }
        }
        match storage_batch_delete_and_write_in_transaction(
            storage,
            txn_id,
            attempt_deletes,
            attempt_writes,
        )
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
        "user subject claim apply conflict retries exhausted".to_string(),
    ))
}
