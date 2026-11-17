//! Overlays reduced admin state onto the stored group, role and realm config documents.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::HashMap;

use super::*;
use crate::document::DocumentTarget;
use crate::structs::identity::auth::Role;
use crate::structs::identity::group::{Group, GroupAuthorizationDocument};
use crate::structs::identity::realm::RealmAuthorizationDocument;
use crate::structs::identity::user::User;

pub fn overlay_group_state(group: &mut Group, reducer_state: &AdminDocumentState) {
    if !reducer_state.conflicts.contains_key(DISPLAY_NAME_PATH)
        && let Some(display_name) = reducer_state.materialized_group_name()
    {
        group.display_name = display_name;
    }

    if !reducer_state.conflicts.contains_key(REALM_ID_PATH)
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

fn overlay_group_roles(group: &mut Group, reducer_state: &AdminDocumentState) {
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

fn group_metadata_conflicted(reducer_state: &AdminDocumentState) -> bool {
    reducer_state.conflicts.contains_key(DISPLAY_NAME_PATH)
        || reducer_state.conflicts.contains_key(REALM_ID_PATH)
        || reducer_state.conflicts.contains_key(GROUP_OWNER_PATH)
}

pub fn materialized_group(group_id: Ulid, reducer_state: &AdminDocumentState) -> Option<Group> {
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

pub fn overlay_group_role(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentState,
    role_id: RoleId,
) {
    overlay_assignments(
        &mut auth_doc.roles,
        reducer_state,
        role_id,
        parse_group_assignment,
    );
}

pub fn overlay_realm_role(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentState,
    role_id: RoleId,
) {
    overlay_assignments(
        &mut auth_doc.roles,
        reducer_state,
        role_id,
        parse_realm_assignment,
    );
}

// Group and realm documents store assignments the same way; only the path layout differs.
fn overlay_assignments(
    roles: &mut HashMap<RoleId, Role>,
    reducer_state: &AdminDocumentState,
    only_role_id: RoleId,
    parse: fn(&str) -> Option<(RoleId, UserId)>,
) {
    for path in reducer_state.conflicts.keys() {
        if let Some((role_id, user_id)) = parse(path)
            && only_role_id == role_id
            && let Some(role) = roles.get_mut(&role_id)
        {
            role.assigned_users.remove(&user_id);
        }
    }

    for (path, version) in &reducer_state.user_subject_ids {
        let Some((role_id, user_id)) = parse(path) else {
            continue;
        };
        if only_role_id != role_id {
            continue;
        }
        let Some(role) = roles.get_mut(&role_id) else {
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

pub fn overlay_realm_config(
    config: &mut RealmConfigDocument,
    reducer_state: &AdminDocumentState,
    now: u64,
    now_ms: u64,
    revocation_index: Option<&RevocationIndex>,
) {
    if !reducer_state
        .conflicts
        .contains_key(METADATA_REPLICATION_PATH)
        && let Some(metadata_replication) = reducer_state.materialized_metadata_replication()
    {
        config.metadata_replication = metadata_replication;
    }

    if !reducer_state.conflicts.contains_key(CONFIG_DISCOVERY_PATH)
        && let Some(discovery) = reducer_state.materialized_realm_discovery()
    {
        config.discovery = discovery;
    }

    if !reducer_state
        .conflicts
        .contains_key(CONFIG_DESCRIPTION_PATH)
        && let Some(description) = reducer_state.materialized_realm_description()
    {
        config.description = description;
    }

    if !reducer_state.conflicts.contains_key(CONFIG_QUOTA_PATH)
        && let Some(quota) = reducer_state.materialized_realm_quota()
    {
        config.quota = quota;
    }

    if !reducer_state.conflicts.contains_key(CONFIG_POLICIES_PATH)
        && let Some(request_policies) = reducer_state.materialized_realm_policies()
    {
        config.request_policies = request_policies;
    }

    if !reducer_state.conflicts.contains_key(CONFIG_COMPUTE_PATH)
        && let Some(compute) = reducer_state.materialized_realm_compute()
    {
        config.compute = compute;
    }

    if !reducer_state.conflicts.contains_key(CONFIG_FEDERATION_PATH)
        && let Some(federation) = reducer_state.materialized_realm_federation()
    {
        config.federation = Some(federation);
    }

    if !reducer_state.conflicts.contains_key(CONFIG_PORTAL_PATH)
        && let Some(features) = reducer_state.materialized_realm_portal()
    {
        config.portal_features = features;
    }
    if !reducer_state.conflicts.contains_key(CONFIG_REPLICAS_PATH)
        && let Some(replicas) = reducer_state.materialized_realm_replicas()
    {
        config.replica_quota = replicas;
    }
    config.email = reducer_state.materialized_realm_email();
    config.vault = reducer_state.materialized_realm_vault();
    config.secrets = reducer_state.materialized_realm_secrets();
    config.plugins = reducer_state.materialized_plugins();
    config.installs = reducer_state.materialized_installs();

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
        match materialized_providers.get(provider_id) {
            Some(provider) if !reducer_state.conflicts.contains_key(path) => {
                config.upsert_provider(provider.clone())
            }
            _ => remove_oidc_provider(config, provider_id),
        }
    }

    // Revocations use the seconds clock; release needs milliseconds, or a
    // report reduced within the completion's second stays unreleased.
    overlay_placement(config, reducer_state, now_ms);
}

pub fn materialized_realm_config(
    realm_id: RealmId,
    reducer_state: &AdminDocumentState,
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
        family_strategy_id: reducer_state
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
        federation: None,
        portal_features: Default::default(),
        replica_quota: Default::default(),
        email: None,
        vault: Vec::new(),
        secrets: Vec::new(),
        plugins: Vec::new(),
        installs: Vec::new(),
    };
    overlay_realm_config(&mut config, reducer_state, now, now_ms, revocation_index);
    Some(config)
}

pub fn needs_revocation_index(
    is_revocation: bool,
    config_present: bool,
    reducer_state: &AdminDocumentState,
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

pub fn reduced_admin_target(target: &DocumentTarget) -> Option<AdminDocumentTarget> {
    match target {
        DocumentTarget::User { user_id } => Some(AdminDocumentTarget::User { user_id: *user_id }),
        DocumentTarget::Group { group_id } => Some(AdminDocumentTarget::Group {
            group_id: *group_id,
        }),
        DocumentTarget::GroupAuthorization { group_id } => Some(AdminDocumentTarget::Group {
            group_id: *group_id,
        }),
        DocumentTarget::RealmAuthorization { realm_id } => Some(AdminDocumentTarget::Realm {
            realm_id: *realm_id,
        }),
        DocumentTarget::RealmConfig { realm_id } => Some(AdminDocumentTarget::RealmConfig {
            realm_id: *realm_id,
        }),
        _ => None,
    }
}

/// Applies one replicated group authorization event to the stored document.
pub fn materialize_group_authorization(
    auth_doc: &mut GroupAuthorizationDocument,
    reducer_state: &AdminDocumentState,
    event: &AdminDocumentEvent,
) {
    match &event.op {
        AdminDocumentOperation::GroupJoinDecided { decision } => {
            for role_id in &decision.role_ids {
                overlay_group_role(auth_doc, reducer_state, *role_id);
            }
        }
        AdminDocumentOperation::GroupPoliciesSet { .. } => {
            if !reducer_state.conflicts.contains_key(GROUP_POLICIES_PATH)
                && let Some(policies) = reducer_state.materialized_group_policies()
            {
                auth_doc.policies = policies;
            }
        }
        AdminDocumentOperation::GroupRoleCreated { role } => {
            let live = reducer_state
                .materialized_group_roles()
                .contains(&role.role_id);
            if materialize_role(
                &mut auth_doc.roles,
                reducer_state,
                role,
                &group_role_path(&role.role_id),
                live,
            ) {
                overlay_group_role(auth_doc, reducer_state, role.role_id);
            }
        }
        AdminDocumentOperation::GroupRoleRemoved { role_id } => {
            auth_doc.roles.remove(role_id);
        }
        AdminDocumentOperation::GroupAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::GroupAssignmentRemoved { role_id, user_id } => {
            let path = group_user_path(role_id, user_id);
            materialize_assignment(
                &mut auth_doc.roles,
                reducer_state,
                &path,
                *role_id,
                *user_id,
            );
        }
        _ => {}
    }
}

/// Applies one replicated realm authorization event to the stored document.
pub fn materialize_realm_authorization(
    auth_doc: &mut RealmAuthorizationDocument,
    reducer_state: &AdminDocumentState,
    event: &AdminDocumentEvent,
) {
    match &event.op {
        AdminDocumentOperation::RealmRoleCreated { role } => {
            let live = reducer_state
                .materialized_realm_roles()
                .contains(&role.role_id);
            if materialize_role(
                &mut auth_doc.roles,
                reducer_state,
                role,
                &realm_role_path(&role.role_id),
                live,
            ) {
                overlay_realm_role(auth_doc, reducer_state, role.role_id);
            }
        }
        AdminDocumentOperation::RealmAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::RealmAssignmentRemoved { role_id, user_id } => {
            let path = realm_user_path(role_id, user_id);
            materialize_assignment(
                &mut auth_doc.roles,
                reducer_state,
                &path,
                *role_id,
                *user_id,
            );
        }
        _ => {}
    }
}

// Writes a role definition while keeping its assignments; returns false when the role is
// conflicted or no longer live, which removes it.
fn materialize_role(
    roles: &mut HashMap<RoleId, Role>,
    reducer_state: &AdminDocumentState,
    role: &AdminRoleDefinition,
    role_path: &str,
    live: bool,
) -> bool {
    if reducer_state.conflicts.contains_key(role_path) || !live {
        roles.remove(&role.role_id);
        return false;
    }
    let assigned_users = roles
        .get(&role.role_id)
        .map(|role| role.assigned_users.clone())
        .unwrap_or_default();
    roles.insert(
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
    true
}

fn materialize_assignment(
    roles: &mut HashMap<RoleId, Role>,
    reducer_state: &AdminDocumentState,
    path: &str,
    role_id: RoleId,
    user_id: UserId,
) {
    let Some(role) = roles.get_mut(&role_id) else {
        return;
    };
    if reducer_state.conflicts.contains_key(path) {
        role.assigned_users.remove(&user_id);
        return;
    }
    let assigned = reducer_state
        .user_subject_ids
        .get(path)
        .and_then(|version| version.value.as_deref())
        .and_then(|value| UserId::from_string(value).ok())
        .is_some_and(|materialized_user_id| materialized_user_id == user_id);
    if assigned {
        role.assigned_users.insert(user_id);
    } else {
        role.assigned_users.remove(&user_id);
    }
}

pub fn materialize_user_operation(
    user_id: UserId,
    previous_user: Option<&User>,
    reducer_state: &AdminDocumentState,
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
        AdminDocumentOperation::SubjectIdAdded { subject_id }
        | AdminDocumentOperation::SubjectIdRemoved { subject_id } => {
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
        AdminDocumentOperation::UserAliasAdded { alias }
        | AdminDocumentOperation::UserAliasRemoved { alias } => {
            if reducer_state.materialized_alias(alias) {
                user.alias_user_ids.insert(*alias);
            } else {
                user.alias_user_ids.remove(alias);
            }
        }
        AdminDocumentOperation::UserAttributeSet { key, .. }
        | AdminDocumentOperation::UserAttributeRemoved { key } => {
            let path = user_attribute_path(key);
            if reducer_state.conflicts.contains_key(&path) {
                if key.starts_with(crate::user::profile::VISIBILITY_PREFIX) {
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
