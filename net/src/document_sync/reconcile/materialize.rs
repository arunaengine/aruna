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
        match replace_batch_in(storage, txn_id, Vec::new(), writes).await {
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

pub(in crate::document_sync) fn overlay_group_state(
    group: &mut Group,
    reducer_state: &AdminDocumentReducerState,
) {
    if !reducer_state
        .conflicts
        .contains_key(GROUP_DISPLAY_NAME_PATH)
        && let Some(display_name) = reducer_state.materialized_group_name()
    {
        group.display_name = display_name;
    }

    if !reducer_state.conflicts.contains_key(GROUP_REALM_ID_PATH)
        && let Some(realm_id) = reducer_state.materialized_group_realm()
    {
        group.realm_id = realm_id;
    }

    if !reducer_state.conflicts.contains_key(GROUP_OWNER_PATH)
        && let Some(owner) = reducer_state.materialized_group_owner()
    {
        group.owner = owner;
    }

    overlay_group_roles(group, reducer_state);
}

fn overlay_group_roles(group: &mut Group, reducer_state: &AdminDocumentReducerState) {
    for path in reducer_state.conflicts.keys() {
        if let Some(role_id) = parse_group_role(path) {
            group.roles.remove(&role_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some(role_id) = parse_group_role(path) else {
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

pub(in crate::document_sync) fn materialized_group(
    group_id: Ulid,
    reducer_state: &AdminDocumentReducerState,
) -> Option<Group> {
    if group_metadata_conflicted(reducer_state) {
        return None;
    }

    Some(Group {
        display_name: reducer_state.materialized_group_name()?,
        group_id,
        realm_id: reducer_state.materialized_group_realm()?,
        owner: reducer_state.materialized_group_owner()?,
        roles: reducer_state
            .materialized_group_roles()
            .into_iter()
            .collect(),
    })
}

pub(in crate::document_sync) fn overlay_group_role(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    role_id: RoleId,
) {
    overlay_group_assignments(auth_doc, reducer_state, Some(role_id));
}

fn overlay_group_assignments(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    only_role_id: Option<RoleId>,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some((role_id, user_id)) = parse_group_assignment(path)
            && only_role_id.is_none_or(|only_role_id| only_role_id == role_id)
            && let Some(role) = auth_doc.roles.get_mut(&role_id)
        {
            role.assigned_users.remove(&user_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some((role_id, user_id)) = parse_group_assignment(path) else {
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

pub(in crate::document_sync) fn overlay_realm_role(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    role_id: RoleId,
) {
    overlay_realm_assignments(auth_doc, reducer_state, Some(role_id));
}

fn overlay_realm_assignments(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentReducerState,
    only_role_id: Option<RoleId>,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some((role_id, user_id)) = parse_realm_assignment(path)
            && only_role_id.is_none_or(|only_role_id| only_role_id == role_id)
            && let Some(role) = auth_doc.roles.get_mut(&role_id)
        {
            role.assigned_users.remove(&user_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some((role_id, user_id)) = parse_realm_assignment(path) else {
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

pub(in crate::document_sync) fn overlay_realm_config(
    config: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentReducerState,
    now: u64,
    now_ms: u64,
    revocation_index: Option<&RevocationIndex>,
) {
    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_METADATA_REPLICATION_PATH)
        && let Some(metadata_replication) = reducer_state.materialized_metadata_replication()
    {
        config.metadata_replication = metadata_replication;
    }

    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_DISCOVERY_PATH)
        && let Some(discovery) = reducer_state.materialized_realm_discovery()
    {
        config.discovery = discovery;
    }

    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_DESCRIPTION_PATH)
        && let Some(description) = reducer_state.materialized_realm_description()
    {
        config.description = description;
    }

    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_QUOTA_PATH)
        && let Some(quota) = reducer_state.materialized_realm_quota()
    {
        config.quota = quota;
    }

    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_POLICIES_PATH)
        && let Some(request_policies) = reducer_state.materialized_realm_policies()
    {
        config.request_policies = request_policies;
    }

    if !reducer_state
        .conflicts
        .contains_key(REALM_CONFIG_COMPUTE_PATH)
        && let Some(compute) = reducer_state.materialized_realm_compute()
    {
        config.compute = compute;
    }

    config.revocation_floor = config.revocation_floor.max(reducer_state.revocation_floor);
    // Without an index, the existing set remains fail-closed until each entry's expiry.
    if let Some(revocation_index) = revocation_index {
        // Union, so a locally accepted revocation is never dropped because the
        // replicated set has not carried it yet.
        config.merge_revocation_index(revocation_index, now);
    }

    for path in reducer_state.conflicts.keys() {
        if let Some(node_id) = parse_config_node(path) {
            remove_config_node(config, &node_id);
        }
    }

    for node_id in reducer_state.removed_config_nodes() {
        remove_config_node(config, &node_id);
    }

    for (node_id, kind) in reducer_state.materialized_config_nodes() {
        let path = config_node_path(&node_id);
        if reducer_state.conflicts.contains_key(&path) {
            remove_config_node(config, &node_id);
            continue;
        }
        config.ensure_node(node_id, kind);
    }

    let materialized_providers = reducer_state.materialized_oidc_providers();
    for path in reducer_state.conflicts.keys() {
        if let Some(provider_id) = parse_config_oidc(path) {
            remove_oidc_provider(config, provider_id);
        }
    }

    for path in reducer_state.user_subject_ids.keys() {
        let Some(provider_id) = parse_config_oidc(path) else {
            continue;
        };
        remove_oidc_provider(config, provider_id);
        if reducer_state.conflicts.contains_key(path) {
            continue;
        }
        if let Some(provider) = materialized_providers.get(provider_id) {
            config.oidc_providers.push(provider.clone());
        }
    }

    // Revocations use the seconds clock; release needs milliseconds, or a
    // report reduced within the completion's second stays unreleased.
    overlay_placement(config, reducer_state, now_ms);
}

pub(in crate::document_sync) fn materialized_realm_config(
    realm_id: RealmId,
    reducer_state: &AdminDocumentReducerState,
    now: u64,
    now_ms: u64,
    revocation_index: Option<&RevocationIndex>,
) -> Option<RealmConfigDocument> {
    let metadata_replication = reducer_state.materialized_metadata_replication()?;
    let discovery = reducer_state.materialized_realm_discovery()?;
    let mut config = RealmConfigDocument {
        realm_id,
        compute: Default::default(),
        metadata_replication,
        oidc_providers: Vec::new(),
        discovery,
        nodes: Vec::new(),
        quota: reducer_state.materialized_realm_quota().unwrap_or_default(),
        request_policies: reducer_state
            .materialized_realm_policies()
            .unwrap_or_default(),
        description: String::new(),
        placement_map: Vec::new(),
        strategies: Vec::new(),
        default_strategy_id: None,
        // Rebuilt from the reducer, never reset: the overlay below would restore
        // it anyway, and a hardcoded nil here would defeat family routing.
        job_family_strategy_id: reducer_state
            .materialized_family_strategy()
            .unwrap_or_else(Ulid::nil),
        strategy_bindings: Vec::new(),
        placement_overrides: Vec::new(),
        placement_bindings: Vec::new(),
        placement_handle_ranges: Vec::new(),
        band_pools: Vec::new(),
        candidate_maps: Vec::new(),
        placement_activations: Vec::new(),
        placement_transitions: Vec::new(),
        revoked_tokens: Vec::new(),
        revocation_floor: reducer_state.revocation_floor,
    };
    overlay_realm_config(&mut config, reducer_state, now, now_ms, revocation_index);
    Some(config)
}

pub(in crate::document_sync) fn needs_revocation_index(
    is_revocation: bool,
    config_present: bool,
    reducer_state: &AdminDocumentReducerState,
    now: u64,
) -> bool {
    is_revocation || !config_present || reducer_state.revocation_compaction_due(now)
}

fn remove_config_node(config: &mut RealmConfigDocument, node_id: &NodeId) {
    let node_id = node_id.to_string();
    config.nodes.retain(|node| node.node_id != node_id);
}

fn remove_oidc_provider(config: &mut RealmConfigDocument, provider_id: &str) {
    config
        .oidc_providers
        .retain(|provider| provider.id != provider_id);
}

pub(in crate::document_sync) fn reduced_admin_target(
    target: &DocumentSyncTarget,
) -> Option<AdminDocumentTarget> {
    match target {
        DocumentSyncTarget::User { user_id } => {
            Some(AdminDocumentTarget::User { user_id: *user_id })
        }
        DocumentSyncTarget::Group { group_id } => Some(AdminDocumentTarget::Group {
            group_id: *group_id,
        }),
        DocumentSyncTarget::GroupAuthorization { group_id } => Some(AdminDocumentTarget::Group {
            group_id: *group_id,
        }),
        DocumentSyncTarget::RealmAuthorization { realm_id } => Some(AdminDocumentTarget::Realm {
            realm_id: *realm_id,
        }),
        DocumentSyncTarget::RealmConfig { realm_id } => Some(AdminDocumentTarget::RealmConfig {
            realm_id: *realm_id,
        }),
        _ => None,
    }
}
