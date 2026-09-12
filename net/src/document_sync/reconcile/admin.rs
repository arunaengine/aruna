use super::*;

pub(crate) async fn apply_admin_operation(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    match (&document_target, &event.target) {
        (DocumentSyncTarget::User { .. }, AdminDocumentTarget::User { .. }) => {
            apply_user_operation(storage, document_target, event).await
        }
        (DocumentSyncTarget::GroupAuthorization { .. }, AdminDocumentTarget::Group { .. }) => {
            apply_group_authorization(storage, document_target, event).await
        }
        (DocumentSyncTarget::RealmAuthorization { .. }, AdminDocumentTarget::Realm { .. }) => {
            apply_realm_authorization(storage, document_target, event).await
        }
        (DocumentSyncTarget::RealmConfig { .. }, AdminDocumentTarget::RealmConfig { .. }) => {
            apply_realm_config(storage, document_target, event).await
        }
        _ => Err(NetError::Bootstrap(
            "admin document operation target does not match document sync target".to_string(),
        )),
    }
}

pub(in crate::document_sync) async fn persist_stale_event(
    storage: &StorageHandle,
    apply_status: AdminDocumentApplyStatus,
    reducer_state: &AdminDocumentReducerState,
) -> Result<bool> {
    match apply_status {
        AdminDocumentApplyStatus::Applied => Ok(false),
        AdminDocumentApplyStatus::Duplicate => Ok(true),
        AdminDocumentApplyStatus::Redundant | AdminDocumentApplyStatus::StaleOriginSequence => {
            batch_write_to(
                storage,
                vec![
                    reducer_state_entry(reducer_state)
                        .map_err(|error| NetError::Bootstrap(error.to_string()))?,
                ],
            )
            .await?;
            Ok(true)
        }
    }
}

pub(in crate::document_sync) async fn apply_user_operation(
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
        reducer_state_key(&event.target),
    )
    .await?
    .map(|bytes| decode_reducer_state(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    let apply_status = reducer_state
        .apply(&event)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if persist_stale_event(storage, apply_status, &reducer_state).await? {
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
    let user = materialize_user_operation(user_id, previous_user.as_ref(), &reducer_state, &event);

    let mut writes = vec![
        (
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
            user.to_bytes(&event.actor)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into(),
        ),
        reducer_state_entry(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    ];
    writes.extend(
        conflict_write_entries(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );

    let deletes = stale_conflict_deletes(previous_state.as_ref(), Some(&reducer_state));
    let subject_ids = changed_subject_id
        .map(|subject_id| vec![subject_id])
        .unwrap_or_else(|| user.subject_ids.clone());
    if subject_ids.is_empty() {
        return replace_batch_transactionally(storage, deletes, writes).await;
    }

    // A transient SSI conflict must never wedge the topic: retry with yields,
    // bounded as a livelock safety valve.
    for _ in 0..APPLY_CONFLICT_ATTEMPTS {
        tokio::task::yield_now().await;
        let txn_id = start_storage_transaction(storage).await?;
        let mut attempt_writes = writes.clone();
        let mut attempt_deletes = deletes.clone();
        for subject_id in &subject_ids {
            let subject_key = subject_index_key(subject_id);
            let mut claims = match transaction_read(
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
                    if let Some(bytes) = transaction_read(
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
        match replace_batch_in(storage, txn_id, attempt_deletes, attempt_writes).await {
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

pub(in crate::document_sync) async fn group_reducer_entries(
    storage: &StorageHandle,
    group_id: Ulid,
    reducer_state: &AdminDocumentReducerState,
) -> Result<Vec<(String, ByteView, Value)>> {
    let target = DocumentSyncTarget::Group { group_id };
    let group =
        match storage_read_from(storage, GROUP_KEYSPACE.to_string(), target.storage_key()).await? {
            Some(bytes) => {
                let mut group = Group::from_bytes(&bytes)
                    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                if group.group_id != group_id {
                    return Err(NetError::Bootstrap(format!(
                        "stored group document id {group_id} does not match payload group id {}",
                        group.group_id
                    )));
                }
                overlay_group_state(&mut group, reducer_state);
                group
            }
            None => {
                let Some(group) = materialized_group(group_id, reducer_state) else {
                    return Ok(Vec::new());
                };
                group
            }
        };

    Ok(vec![
        target_write_entry(
            target,
            postcard::to_allocvec(&group)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into(),
        ),
        (
            GROUP_OWNER_INDEX_KEYSPACE.to_string(),
            owner_group_key(group.owner, group.group_id).into(),
            ByteView::from(Vec::new()),
        ),
    ])
}

pub(in crate::document_sync) async fn apply_group_authorization(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    let DocumentSyncTarget::GroupAuthorization { group_id } = document_target.clone() else {
        return Err(NetError::Bootstrap(
            "group admin operation sync only supports group authorization targets".to_string(),
        ));
    };
    let AdminDocumentTarget::Group {
        group_id: event_group_id,
    } = event.target.clone()
    else {
        return Err(NetError::Bootstrap(
            "admin document operation payload target is not a group".to_string(),
        ));
    };
    if event_group_id != group_id {
        return Err(NetError::Bootstrap(format!(
            "replicated group admin operation target {group_id} does not match payload group id {event_group_id}"
        )));
    }
    if !matches!(
        &event.op,
        AdminDocumentOperation::GroupCreated { .. }
            | AdminDocumentOperation::GroupRoleAdded { .. }
            | AdminDocumentOperation::GroupRoleCreated { .. }
            | AdminDocumentOperation::GroupRoleRemoved { .. }
            | AdminDocumentOperation::GroupRoleUserAssignmentAdded { .. }
            | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { .. }
            | AdminDocumentOperation::GroupPoliciesSet { .. }
            | AdminDocumentOperation::GroupJoinRequested { .. }
            | AdminDocumentOperation::GroupJoinDecided { .. }
            | AdminDocumentOperation::GroupDisplayNameSet { .. }
    ) {
        return Err(NetError::Bootstrap(
            "group admin operation sync only supports group creation, renames, role seeds, role creation/removal, role user assignment updates, and policy updates"
                .to_string(),
        ));
    }

    let previous_state = storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        reducer_state_key(&event.target),
    )
    .await?
    .map(|bytes| decode_reducer_state(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    let apply_status = reducer_state
        .apply(&event)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if persist_stale_event(storage, apply_status, &reducer_state).await? {
        return Ok(());
    }

    let previous_auth_doc = storage_read_from(
        storage,
        document_target.storage_keyspace().to_string(),
        document_target.storage_key(),
    )
    .await?
    .map(|bytes| GroupAuthorizationDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut auth_doc = previous_auth_doc.unwrap_or_else(|| GroupAuthorizationDocument {
        group_id,
        roles: Default::default(),
        policies: Default::default(),
    });
    materialize_group_authorization(&mut auth_doc, &reducer_state, &event);
    let group_writes = group_reducer_entries(storage, group_id, &reducer_state).await?;

    let mut writes = vec![
        (
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
            auth_doc
                .to_bytes(&event.actor)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into(),
        ),
        reducer_state_entry(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    ];
    writes.extend(group_writes);
    writes.extend(
        conflict_write_entries(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );

    let stale_conflict_deletes =
        stale_conflict_deletes(previous_state.as_ref(), Some(&reducer_state));
    replace_batch_transactionally(storage, stale_conflict_deletes, writes).await
}

pub(in crate::document_sync) async fn apply_realm_authorization(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    let DocumentSyncTarget::RealmAuthorization { realm_id } = document_target.clone() else {
        return Err(NetError::Bootstrap(
            "realm admin operation sync only supports realm authorization targets".to_string(),
        ));
    };
    let AdminDocumentTarget::Realm {
        realm_id: event_realm_id,
    } = event.target.clone()
    else {
        return Err(NetError::Bootstrap(
            "admin document operation payload target is not a realm".to_string(),
        ));
    };
    if event_realm_id != realm_id {
        return Err(NetError::Bootstrap(format!(
            "replicated realm admin operation target {realm_id} does not match payload realm id {event_realm_id}"
        )));
    }
    if !matches!(
        &event.op,
        AdminDocumentOperation::RealmRoleAdded { .. }
            | AdminDocumentOperation::RealmRoleCreated { .. }
            | AdminDocumentOperation::RealmRoleUserAssignmentAdded { .. }
            | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { .. }
    ) {
        return Err(NetError::Bootstrap(
            "realm admin operation sync only supports role seeds, role creation, and role user assignment updates"
                .to_string(),
        ));
    }

    let previous_state = storage_read_from(
        storage,
        ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
        reducer_state_key(&event.target),
    )
    .await?
    .map(|bytes| decode_reducer_state(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut reducer_state = previous_state
        .clone()
        .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
    let apply_status = reducer_state
        .apply(&event)
        .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    if persist_stale_event(storage, apply_status, &reducer_state).await? {
        return Ok(());
    }

    let previous_auth_doc = storage_read_from(
        storage,
        document_target.storage_keyspace().to_string(),
        document_target.storage_key(),
    )
    .await?
    .map(|bytes| RealmAuthorizationDocument::from_bytes(&bytes))
    .transpose()
    .map_err(|error| NetError::Bootstrap(error.to_string()))?;
    let mut auth_doc = previous_auth_doc.unwrap_or_else(|| RealmAuthorizationDocument {
        realm_id,
        roles: Default::default(),
        operation_restrictions: Default::default(),
    });
    materialize_realm_authorization(&mut auth_doc, &reducer_state, &event);

    let mut writes = vec![
        (
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
            auth_doc
                .to_bytes(&event.actor)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
                .into(),
        ),
        reducer_state_entry(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    ];
    writes.extend(
        conflict_write_entries(&reducer_state)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?,
    );

    let stale_conflict_deletes =
        stale_conflict_deletes(previous_state.as_ref(), Some(&reducer_state));
    replace_batch_transactionally(storage, stale_conflict_deletes, writes).await
}

/// Realm-config ops the reducer stores as order-insensitive immutable values
/// and whose validation no other such op can influence: a consecutive run of
/// them may apply as one read-reduce-write cycle instead of one per event.
pub(in crate::document_sync) fn coalescible_config_op(op: &AdminDocumentOperation) -> bool {
    matches!(
        op,
        AdminDocumentOperation::RealmConfigCandidateMapPublished { .. }
            | AdminDocumentOperation::RealmConfigActivationsInitialized { .. }
            | AdminDocumentOperation::RealmConfigTransitionStarted { .. }
            | AdminDocumentOperation::RealmConfigTransitionBarrierReported { .. }
            | AdminDocumentOperation::RealmConfigTransitionProofSubmitted { .. }
            | AdminDocumentOperation::RealmConfigTransitionAborted { .. }
            | AdminDocumentOperation::RealmConfigTransitionBucketForced { .. }
            | AdminDocumentOperation::RealmConfigTransitionStallReported { .. }
            | AdminDocumentOperation::RealmConfigTransitionDrainReported { .. }
    )
}

/// Flushes a buffered run of coalescible realm-config events, if any, and
/// drops the validation snapshot the applied events just outdated.
pub(in crate::document_sync) async fn flush_config_run(
    storage: &StorageHandle,
    run: &mut Option<(DocumentSyncTarget, Vec<AdminDocumentEvent>)>,
    validation_cache: &mut ConfigValidationCache,
) -> Result<()> {
    if let Some((target, events)) = run.take() {
        apply_config_events(storage, target, events).await?;
        validation_cache.invalidate();
    }
    Ok(())
}

/// Applies a run of coalescible realm-config events in one transaction. A
/// transition replicates hundreds of values, so paying for state, document,
/// materialization and commit once avoids quadratic reducer rewrites.
pub(in crate::document_sync) async fn apply_config_events(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    events: Vec<AdminDocumentEvent>,
) -> Result<()> {
    let DocumentSyncTarget::RealmConfig { realm_id } = document_target.clone() else {
        return Err(NetError::Bootstrap(
            "realm config admin operation sync only supports realm config targets".to_string(),
        ));
    };
    for event in &events {
        let AdminDocumentTarget::RealmConfig {
            realm_id: event_realm_id,
        } = event.target
        else {
            return Err(NetError::Bootstrap(
                "admin document operation payload target is not a realm config".to_string(),
            ));
        };
        if event_realm_id != realm_id {
            return Err(NetError::Bootstrap(format!(
                "replicated realm config admin operation target {realm_id} does not match payload realm id {event_realm_id}"
            )));
        }
        if !coalescible_config_op(&event.op) {
            return Err(NetError::Bootstrap(
                "realm config event run only supports transition updates".to_string(),
            ));
        }
        if event.origin_node_id != event.actor.node_id
            || event.actor.realm_id != realm_id
            || event.actor.user_id.realm_id != realm_id
        {
            return Err(NetError::Bootstrap(
                "realm config event actor and origin do not match the target realm".to_string(),
            ));
        }
    }
    let Some(actor) = events.last().map(|event| event.actor.clone()) else {
        return Ok(());
    };

    // A transient SSI conflict must never wedge the topic: retry with yields,
    // bounded as a livelock safety valve.
    for _ in 0..APPLY_CONFLICT_ATTEMPTS {
        tokio::task::yield_now().await;
        let raw_now = unix_timestamp_secs();
        let txn_id = start_storage_transaction(storage).await?;
        let previous_state = match transaction_read(
            storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
            reducer_state_key(&AdminDocumentTarget::RealmConfig { realm_id }),
            Some(txn_id),
        )
        .await
        {
            Ok(value) => match value
                .map(|bytes| decode_reducer_state(&bytes))
                .transpose()
                .map_err(|error| NetError::Bootstrap(error.to_string()))
            {
                Ok(value) => value,
                Err(error) => return Err(abort_error(storage, txn_id, error).await),
            },
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        };
        let previous_config = match transaction_read(
            storage,
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
            Some(txn_id),
        )
        .await
        {
            Ok(value) => match value
                .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
                .transpose()
                .map_err(|error| NetError::Bootstrap(error.to_string()))
            {
                Ok(value) => value,
                Err(error) => return Err(abort_error(storage, txn_id, error).await),
            },
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        };

        let effective_now = previous_state
            .as_ref()
            .map_or(raw_now, |state| state.revocation_floor.max(raw_now));
        let mut reducer_state = previous_state.clone().unwrap_or_else(|| {
            AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig { realm_id })
        });
        for event in &events {
            if let Err(error) = reducer_state.apply(event) {
                return Err(
                    abort_error(storage, txn_id, NetError::Bootstrap(error.to_string())).await,
                );
            }
        }
        reducer_state.advance_revocation_floor(effective_now);
        let needs_index = needs_revocation_index(
            false,
            previous_config.is_some(),
            &reducer_state,
            effective_now,
        );
        let mut revocation_index =
            needs_index.then(|| reducer_state.revocation_index(effective_now));
        if let Some(index) = revocation_index.as_mut() {
            index.compact(&mut reducer_state);
        }

        let (config, config_changed) = match previous_config {
            Some(mut config) => {
                if config.realm_id != realm_id {
                    return Err(
                        abort_error(
                            storage,
                            txn_id,
                            NetError::Bootstrap(format!(
                                "stored realm config document id {realm_id} does not match payload realm id {}",
                                config.realm_id
                            )),
                        )
                        .await,
                    );
                }
                let before = config.clone();
                overlay_realm_config(
                    &mut config,
                    &reducer_state,
                    effective_now,
                    unix_timestamp_millis(),
                    revocation_index.as_ref(),
                );
                let changed = config != before;
                (Some(config), changed)
            }
            None => {
                let config = materialized_realm_config(
                    realm_id,
                    &reducer_state,
                    effective_now,
                    unix_timestamp_millis(),
                    revocation_index.as_ref(),
                );
                let changed = config.is_some();
                (config, changed)
            }
        };
        if previous_state
            .as_ref()
            .is_some_and(|previous| previous == &reducer_state)
            && !config_changed
        {
            abort_txn(storage, txn_id).await?;
            return Ok(());
        }

        let mut writes = Vec::new();
        if config_changed && let Some(config) = config {
            let bytes = match config.to_bytes(&actor) {
                Ok(bytes) => bytes,
                Err(error) => {
                    return Err(abort_error(
                        storage,
                        txn_id,
                        NetError::Bootstrap(error.to_string()),
                    )
                    .await);
                }
            };
            writes.push((
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                bytes.into(),
            ));
        }
        let reducer_write = match reducer_state_entry(&reducer_state) {
            Ok(write) => write,
            Err(error) => {
                return Err(
                    abort_error(storage, txn_id, NetError::Bootstrap(error.to_string())).await,
                );
            }
        };
        writes.push(reducer_write);
        if previous_state
            .as_ref()
            .is_none_or(|previous| previous.conflicts != reducer_state.conflicts)
        {
            let conflict_writes = match conflict_write_entries(&reducer_state) {
                Ok(writes) => writes,
                Err(error) => {
                    return Err(abort_error(
                        storage,
                        txn_id,
                        NetError::Bootstrap(error.to_string()),
                    )
                    .await);
                }
            };
            writes.extend(conflict_writes);
        }

        let stale_conflict_deletes =
            stale_conflict_deletes(previous_state.as_ref(), Some(&reducer_state));
        match replace_batch_in(storage, txn_id, stale_conflict_deletes, writes).await {
            Ok(()) => return Ok(()),
            Err(NetError::Storage(StorageError::TransactionConflict)) => {
                abort_txn(storage, txn_id).await?;
            }
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        }
    }
    Err(NetError::Dht(
        "realm config admin operation conflict retries exhausted".to_string(),
    ))
}

pub(in crate::document_sync) fn materialize_group_authorization(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    event: &AdminDocumentEvent,
) {
    if let AdminDocumentOperation::GroupJoinDecided { decision } = &event.op {
        for role_id in &decision.role_ids {
            overlay_group_role(auth_doc, reducer_state, *role_id);
        }
        return;
    }
    if let AdminDocumentOperation::GroupPoliciesSet { .. } = &event.op {
        if !reducer_state
            .conflicts
            .contains_key(aruna_core::reducer::GROUP_POLICIES_PATH)
            && let Some(policies) = reducer_state.materialized_group_policies()
        {
            auth_doc.policies = policies;
        }
        return;
    }

    if let AdminDocumentOperation::GroupRoleCreated { role } = &event.op {
        materialize_group_role(auth_doc, reducer_state, role);
        return;
    }

    if let AdminDocumentOperation::GroupRoleRemoved { role_id } = &event.op {
        auth_doc.roles.remove(role_id);
        return;
    }

    let (role_id, user_id) = match &event.op {
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id } => {
            (role_id, user_id)
        }
        _ => return,
    };
    let path = group_user_path(role_id, user_id);
    if reducer_state.conflicts.contains_key(&path) {
        if let Some(role) = auth_doc.roles.get_mut(role_id) {
            role.assigned_users.remove(user_id);
        }
        return;
    }
    let Some(role) = auth_doc.roles.get_mut(role_id) else {
        return;
    };
    let assigned = reducer_state
        .user_subject_ids
        .get(&path)
        .and_then(|version| version.value.as_deref())
        .and_then(|value| UserId::from_string(value).ok())
        .is_some_and(|materialized_user_id| materialized_user_id == *user_id);
    if assigned {
        role.assigned_users.insert(*user_id);
    } else {
        role.assigned_users.remove(user_id);
    }
}

pub(in crate::document_sync) fn materialize_group_role(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    role: &AdminDocumentRoleDefinition,
) {
    let role_path = group_role_path(&role.role_id);
    if reducer_state.conflicts.contains_key(&role_path)
        || !reducer_state
            .materialized_group_roles()
            .contains(&role.role_id)
    {
        auth_doc.roles.remove(&role.role_id);
        return;
    }

    let assigned_users = auth_doc
        .roles
        .get(&role.role_id)
        .map(|role| role.assigned_users.clone())
        .unwrap_or_default();
    auth_doc.roles.insert(
        role.role_id,
        Role {
            role_id: role.role_id,
            name: role.name.clone(),
            permissions: role
                .permissions
                .iter()
                .map(|(path, permission)| (path.clone(), permission.clone()))
                .collect(),
            assigned_users,
        },
    );
    overlay_group_role(auth_doc, reducer_state, role.role_id);
}

pub(in crate::document_sync) fn materialize_realm_authorization(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    event: &AdminDocumentEvent,
) {
    if let AdminDocumentOperation::RealmRoleCreated { role } = &event.op {
        materialize_realm_role(auth_doc, reducer_state, role);
        return;
    }

    let (role_id, user_id) = match &event.op {
        AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { role_id, user_id } => {
            (role_id, user_id)
        }
        _ => return,
    };
    let path = realm_user_path(role_id, user_id);
    if reducer_state.conflicts.contains_key(&path) {
        if let Some(role) = auth_doc.roles.get_mut(role_id) {
            role.assigned_users.remove(user_id);
        }
        return;
    }
    let Some(role) = auth_doc.roles.get_mut(role_id) else {
        return;
    };
    let assigned = reducer_state
        .user_subject_ids
        .get(&path)
        .and_then(|version| version.value.as_deref())
        .and_then(|value| UserId::from_string(value).ok())
        .is_some_and(|materialized_user_id| materialized_user_id == *user_id);
    if assigned {
        role.assigned_users.insert(*user_id);
    } else {
        role.assigned_users.remove(user_id);
    }
}

pub(in crate::document_sync) fn materialize_realm_role(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    role: &AdminDocumentRoleDefinition,
) {
    let role_path = realm_role_path(&role.role_id);
    if reducer_state.conflicts.contains_key(&role_path)
        || !reducer_state
            .materialized_realm_roles()
            .contains(&role.role_id)
    {
        auth_doc.roles.remove(&role.role_id);
        return;
    }

    let assigned_users = auth_doc
        .roles
        .get(&role.role_id)
        .map(|role| role.assigned_users.clone())
        .unwrap_or_default();
    auth_doc.roles.insert(
        role.role_id,
        Role {
            role_id: role.role_id,
            name: role.name.clone(),
            permissions: role
                .permissions
                .iter()
                .map(|(path, permission)| (path.clone(), permission.clone()))
                .collect(),
            assigned_users,
        },
    );
    overlay_realm_role(auth_doc, reducer_state, role.role_id);
}

pub(in crate::document_sync) fn materialize_user_operation(
    user_id: UserId,
    previous_user: Option<&User>,
    reducer_state: &AdminDocumentReducerState,
    event: &AdminDocumentEvent,
) -> User {
    let mut user = previous_user.cloned().unwrap_or_else(|| User {
        user_id,
        name: String::new(),
        subject_ids: Vec::new(),
        alias_user_ids: Default::default(),
        attributes: Default::default(),
    });

    match &event.op {
        AdminDocumentOperation::UserNameSet { .. } => {
            if reducer_state.conflicts.contains_key(USER_NAME_PATH) {
                user.name.clear();
            } else if let Some(name) = reducer_state.materialized_user_name() {
                user.name = name;
            }
        }
        AdminDocumentOperation::UserSubjectIdAdded { subject_id }
        | AdminDocumentOperation::UserSubjectIdRemoved { subject_id } => {
            let path = user_subject_path(subject_id);
            let materialized_subject_id = if reducer_state.conflicts.contains_key(&path) {
                None
            } else {
                reducer_state
                    .user_subject_ids
                    .get(subject_id)
                    .and_then(|version| version.value.clone())
            };

            user.subject_ids.retain(|candidate| candidate != subject_id);
            if let Some(materialized_subject_id) = materialized_subject_id
                && !user.subject_ids.contains(&materialized_subject_id)
            {
                user.subject_ids.push(materialized_subject_id);
            }
        }
        AdminDocumentOperation::UserAttributeSet { key, .. }
        | AdminDocumentOperation::UserAttributeRemoved { key } => {
            let path = user_attribute_path(key);
            if reducer_state.conflicts.contains_key(&path) {
                if key.starts_with(aruna_core::user_profile::VISIBILITY_PREFIX) {
                    user.attributes.insert(key.clone(), "private".to_string());
                } else {
                    user.attributes.remove(key);
                }
            } else {
                match reducer_state
                    .user_attributes
                    .get(key)
                    .and_then(|version| version.value.clone())
                {
                    Some(value) => {
                        user.attributes.insert(key.clone(), value);
                    }
                    None => {
                        user.attributes.remove(key);
                    }
                }
            }
        }
        _ => {}
    }

    user
}

pub(in crate::document_sync) const APPLY_CONFLICT_ATTEMPTS: usize = 64;

async fn apply_realm_config(
    storage: &StorageHandle,
    document_target: DocumentSyncTarget,
    event: AdminDocumentEvent,
) -> Result<()> {
    let DocumentSyncTarget::RealmConfig { realm_id } = document_target.clone() else {
        return Err(NetError::Bootstrap(
            "realm config admin operation sync only supports realm config targets".to_string(),
        ));
    };
    let AdminDocumentTarget::RealmConfig {
        realm_id: event_realm_id,
    } = event.target.clone()
    else {
        return Err(NetError::Bootstrap(
            "admin document operation payload target is not a realm config".to_string(),
        ));
    };
    if event_realm_id != realm_id {
        return Err(NetError::Bootstrap(format!(
            "replicated realm config admin operation target {realm_id} does not match payload realm id {event_realm_id}"
        )));
    }
    if !matches!(
        &event.op,
        AdminDocumentOperation::RealmConfigNodeEnsured { .. }
            | AdminDocumentOperation::RealmConfigNodeRemoved { .. }
            | AdminDocumentOperation::RealmConfigOidcProviderUpserted { .. }
            | AdminDocumentOperation::RealmConfigOidcProviderRemoved { .. }
            | AdminDocumentOperation::RealmConfigSettingsSet { .. }
            | AdminDocumentOperation::RealmConfigDescriptionSet { .. }
            | AdminDocumentOperation::RealmConfigQuotaSet { .. }
            | AdminDocumentOperation::RealmConfigNodePlacementSet { .. }
            | AdminDocumentOperation::RealmConfigNodePlacementRemoved { .. }
            | AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { .. }
            | AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { .. }
            | AdminDocumentOperation::RealmConfigDefaultStrategySet { .. }
            | AdminDocumentOperation::RealmConfigJobFamilySet { .. }
            | AdminDocumentOperation::RealmConfigStrategyBindingSet { .. }
            | AdminDocumentOperation::RealmConfigStrategyBindingRemoved { .. }
            | AdminDocumentOperation::RealmConfigPlacementOverrideSet { .. }
            | AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { .. }
            | AdminDocumentOperation::RealmConfigPlacementBindingAppended { .. }
            | AdminDocumentOperation::RealmConfigHandleRangeGranted { .. }
            | AdminDocumentOperation::RealmConfigBandPoolAssigned { .. }
            | AdminDocumentOperation::RealmConfigPoliciesSet { .. }
            | AdminDocumentOperation::RealmConfigCandidateMapPublished { .. }
            | AdminDocumentOperation::RealmConfigActivationsInitialized { .. }
            | AdminDocumentOperation::RealmConfigTransitionStarted { .. }
            | AdminDocumentOperation::RealmConfigTransitionBarrierReported { .. }
            | AdminDocumentOperation::RealmConfigTransitionProofSubmitted { .. }
            | AdminDocumentOperation::RealmConfigTransitionAborted { .. }
            | AdminDocumentOperation::RealmConfigTransitionBucketForced { .. }
            | AdminDocumentOperation::RealmConfigTransitionStallReported { .. }
            | AdminDocumentOperation::RealmConfigTransitionDrainReported { .. }
            | AdminDocumentOperation::RealmConfigComputeSet { .. }
            | AdminDocumentOperation::RealmConfigTokenRevoked { .. }
    ) {
        return Err(NetError::Bootstrap(
            "realm config admin operation sync only supports node membership updates, OIDC provider updates, settings updates, description updates, quota updates, placement updates, transition updates, policy updates, compute updates, and token revocations"
                .to_string(),
        ));
    }

    let is_revocation = matches!(
        &event.op,
        AdminDocumentOperation::RealmConfigTokenRevoked { .. }
    );
    if event.origin_node_id != event.actor.node_id
        || event.actor.realm_id != realm_id
        || event.actor.user_id.realm_id != realm_id
    {
        return Err(NetError::Bootstrap(
            "realm config event actor and origin do not match the target realm".to_string(),
        ));
    }

    // A transient SSI conflict must never wedge the topic: retry with yields,
    // bounded as a livelock safety valve.
    for _ in 0..APPLY_CONFLICT_ATTEMPTS {
        tokio::task::yield_now().await;
        let raw_now = unix_timestamp_secs();
        let txn_id = start_storage_transaction(storage).await?;
        let previous_state = match transaction_read(
            storage,
            ADMIN_DOCUMENT_STATE_KEYSPACE.to_string(),
            reducer_state_key(&event.target),
            Some(txn_id),
        )
        .await
        {
            Ok(value) => match value
                .map(|bytes| decode_reducer_state(&bytes))
                .transpose()
                .map_err(|error| NetError::Bootstrap(error.to_string()))
            {
                Ok(value) => value,
                Err(error) => return Err(abort_error(storage, txn_id, error).await),
            },
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        };
        let previous_config = match transaction_read(
            storage,
            document_target.storage_keyspace().to_string(),
            document_target.storage_key(),
            Some(txn_id),
        )
        .await
        {
            Ok(value) => match value
                .map(|bytes| RealmConfigDocument::from_bytes(&bytes))
                .transpose()
                .map_err(|error| NetError::Bootstrap(error.to_string()))
            {
                Ok(value) => value,
                Err(error) => return Err(abort_error(storage, txn_id, error).await),
            },
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        };

        if is_revocation {
            let valid = revocation_origin_known(
                previous_config.as_ref(),
                previous_state.as_ref(),
                &event,
                realm_id,
            );
            if !valid {
                return Err(
                    abort_error(
                        storage,
                        txn_id,
                        NetError::Bootstrap(
                            "revocation origin is not an onboarded realm node in the transaction snapshot"
                                .to_string(),
                        ),
                    )
                    .await,
                );
            }
            if let AdminDocumentOperation::RealmConfigTokenRevoked {
                token_hash,
                expires_at,
                token_owner,
            } = &event.op
                && (!aruna_core::auth::valid_token_hash(token_hash)
                    || !valid_revocation_expiry(*expires_at, raw_now)
                    || token_owner.is_nil()
                    || token_owner.realm_id != realm_id)
            {
                return Err(abort_error(
                    storage,
                    txn_id,
                    NetError::Bootstrap(
                        "replicated revocation has invalid hash, expiry, or owner".to_string(),
                    ),
                )
                .await);
            }
        }

        let effective_now = previous_state
            .as_ref()
            .map_or(raw_now, |state| state.revocation_floor.max(raw_now));
        let mut reducer_state = previous_state
            .clone()
            .unwrap_or_else(|| AdminDocumentReducerState::new(event.target.clone()));
        let needs_index = needs_revocation_index(
            is_revocation,
            previous_config.is_some(),
            &reducer_state,
            effective_now,
        );
        let mut revocation_index =
            needs_index.then(|| reducer_state.revocation_index(effective_now));
        if is_revocation {
            let Some(index) = revocation_index.as_mut() else {
                return Err(abort_error(
                    storage,
                    txn_id,
                    NetError::Bootstrap("revocation index was not admitted".to_string()),
                )
                .await);
            };
            if let Err(error) = reducer_state.apply_revocation_event(&event, index) {
                return Err(
                    abort_error(storage, txn_id, NetError::Bootstrap(error.to_string())).await,
                );
            }
        } else if let Err(error) = reducer_state.apply(&event) {
            return Err(abort_error(storage, txn_id, NetError::Bootstrap(error.to_string())).await);
        }
        reducer_state.advance_revocation_floor(effective_now);
        if let Some(index) = revocation_index.as_mut() {
            index.compact(&mut reducer_state);
        }

        let (config, config_changed) = match previous_config {
            Some(mut config) => {
                if config.realm_id != realm_id {
                    return Err(
                        abort_error(
                            storage,
                            txn_id,
                            NetError::Bootstrap(format!(
                                "stored realm config document id {realm_id} does not match payload realm id {}",
                                config.realm_id
                            )),
                        )
                        .await,
                    );
                }
                let before = config.clone();
                overlay_realm_config(
                    &mut config,
                    &reducer_state,
                    effective_now,
                    unix_timestamp_millis(),
                    revocation_index.as_ref(),
                );
                let changed = config != before;
                (Some(config), changed)
            }
            None => {
                let config = materialized_realm_config(
                    realm_id,
                    &reducer_state,
                    effective_now,
                    unix_timestamp_millis(),
                    revocation_index.as_ref(),
                );
                let changed = config.is_some();
                (config, changed)
            }
        };
        if previous_state
            .as_ref()
            .is_some_and(|previous| previous == &reducer_state)
            && !config_changed
        {
            abort_txn(storage, txn_id).await?;
            return Ok(());
        }

        let mut writes = Vec::new();
        if config_changed && let Some(config) = config {
            let bytes = match config.to_bytes(&event.actor) {
                Ok(bytes) => bytes,
                Err(error) => {
                    return Err(abort_error(
                        storage,
                        txn_id,
                        NetError::Bootstrap(error.to_string()),
                    )
                    .await);
                }
            };
            writes.push((
                document_target.storage_keyspace().to_string(),
                document_target.storage_key(),
                bytes.into(),
            ));
        }
        let reducer_write = match reducer_state_entry(&reducer_state) {
            Ok(write) => write,
            Err(error) => {
                return Err(
                    abort_error(storage, txn_id, NetError::Bootstrap(error.to_string())).await,
                );
            }
        };
        writes.push(reducer_write);
        if previous_state
            .as_ref()
            .is_none_or(|previous| previous.conflicts != reducer_state.conflicts)
        {
            let conflict_writes = match conflict_write_entries(&reducer_state) {
                Ok(writes) => writes,
                Err(error) => {
                    return Err(abort_error(
                        storage,
                        txn_id,
                        NetError::Bootstrap(error.to_string()),
                    )
                    .await);
                }
            };
            writes.extend(conflict_writes);
        }

        let stale_conflict_deletes =
            stale_conflict_deletes(previous_state.as_ref(), Some(&reducer_state));
        match replace_batch_in(storage, txn_id, stale_conflict_deletes, writes).await {
            Ok(()) => return Ok(()),
            Err(NetError::Storage(StorageError::TransactionConflict)) => {
                abort_txn(storage, txn_id).await?;
            }
            Err(error) => return Err(abort_error(storage, txn_id, error).await),
        }
    }
    Err(NetError::Dht(
        "realm config admin operation conflict retries exhausted".to_string(),
    ))
}
