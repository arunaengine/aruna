use super::*;

pub(super) fn node(seed: u8) -> NodeId {
    iroh::SecretKey::from_bytes(&[seed; 32]).public()
}

pub(super) fn realm_id_seed(seed: u8) -> RealmId {
    RealmId::from_bytes([seed; 32])
}

pub(super) fn realm_id() -> RealmId {
    realm_id_seed(9)
}

pub(super) fn group_id() -> GroupId {
    Ulid::from_bytes([7u8; 16])
}

pub(super) fn role_id(seed: u8) -> RoleId {
    Ulid::from_bytes([seed; 16])
}

pub(super) fn role_definition(role_id: RoleId, name: &str) -> AdminDocumentRoleDefinition {
    AdminDocumentRoleDefinition {
        role_id,
        name: name.to_string(),
        permissions: BTreeMap::from([
            ("/dataset/**".to_string(), Permission::READ),
            ("/project/admin/**".to_string(), Permission::WRITE),
        ]),
    }
}

pub(super) fn oidc_provider(id: &str, issuer_suffix: &str) -> OidcProviderConfig {
    OidcProviderConfig {
        id: id.to_string(),
        issuer: format!("https://issuer.example/{issuer_suffix}"),
        audience: "aruna".to_string(),
        discovery_url: format!(
            "https://issuer.example/{issuer_suffix}/.well-known/openid-configuration"
        ),
    }
}

pub(super) fn user_id_seed(seed: u8) -> UserId {
    UserId::local(Ulid::from_bytes([seed; 16]), realm_id())
}

pub(super) fn user_id() -> UserId {
    user_id_seed(8)
}

pub(super) fn actor(origin_node_id: NodeId) -> Actor {
    Actor {
        node_id: origin_node_id,
        user_id: user_id(),
        realm_id: realm_id(),
    }
}

pub(super) fn user_state() -> AdminDocumentReducerState {
    AdminDocumentReducerState::new(AdminDocumentTarget::User { user_id: user_id() })
}

pub(super) fn group_state() -> AdminDocumentReducerState {
    AdminDocumentReducerState::new(AdminDocumentTarget::Group {
        group_id: group_id(),
    })
}

pub(super) fn realm_state() -> AdminDocumentReducerState {
    AdminDocumentReducerState::new(AdminDocumentTarget::Realm {
        realm_id: realm_id(),
    })
}

pub(super) fn realm_config_state() -> AdminDocumentReducerState {
    AdminDocumentReducerState::new(AdminDocumentTarget::RealmConfig {
        realm_id: realm_id(),
    })
}

pub(super) fn event(
    event_seed: u8,
    origin_node_id: NodeId,
    origin_seq: u64,
    observed: AdminDocumentClock,
    op: AdminDocumentOperation,
) -> AdminDocumentEvent {
    AdminDocumentEvent {
        event_id: Ulid::from_bytes([event_seed; 16]),
        target: AdminDocumentTarget::User { user_id: user_id() },
        origin_node_id,
        origin_seq,
        observed,
        actor: actor(origin_node_id),
        op,
    }
}

pub(super) fn group_event(
    event_seed: u8,
    origin_node_id: NodeId,
    origin_seq: u64,
    observed: AdminDocumentClock,
    op: AdminDocumentOperation,
) -> AdminDocumentEvent {
    AdminDocumentEvent {
        event_id: Ulid::from_bytes([event_seed; 16]),
        target: AdminDocumentTarget::Group {
            group_id: group_id(),
        },
        origin_node_id,
        origin_seq,
        observed,
        actor: actor(origin_node_id),
        op,
    }
}

pub(super) fn realm_event(
    event_seed: u8,
    origin_node_id: NodeId,
    origin_seq: u64,
    observed: AdminDocumentClock,
    op: AdminDocumentOperation,
) -> AdminDocumentEvent {
    AdminDocumentEvent {
        event_id: Ulid::from_bytes([event_seed; 16]),
        target: AdminDocumentTarget::Realm {
            realm_id: realm_id(),
        },
        origin_node_id,
        origin_seq,
        observed,
        actor: actor(origin_node_id),
        op,
    }
}

pub(super) fn realm_config_event(
    event_seed: u8,
    origin_node_id: NodeId,
    origin_seq: u64,
    observed: AdminDocumentClock,
    op: AdminDocumentOperation,
) -> AdminDocumentEvent {
    AdminDocumentEvent {
        event_id: Ulid::from_bytes([event_seed; 16]),
        target: AdminDocumentTarget::RealmConfig {
            realm_id: realm_id(),
        },
        origin_node_id,
        origin_seq,
        observed,
        actor: actor(origin_node_id),
        op,
    }
}

pub(super) fn set_attr(
    event_seed: u8,
    origin_seed: u8,
    key: &str,
    value: &str,
) -> AdminDocumentEvent {
    event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserAttributeSet {
            key: key.to_string(),
            value: value.to_string(),
        },
    )
}

pub(super) fn set_name(event_seed: u8, origin_seed: u8, name: &str) -> AdminDocumentEvent {
    event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserNameSet {
            name: name.to_string(),
        },
    )
}

pub(super) fn add_subject(event_seed: u8, origin_seed: u8, subject_id: &str) -> AdminDocumentEvent {
    event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserSubjectIdAdded {
            subject_id: subject_id.to_string(),
        },
    )
}

pub(super) fn remove_subject(
    event_seed: u8,
    origin_seed: u8,
    subject_id: &str,
) -> AdminDocumentEvent {
    event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::UserSubjectIdRemoved {
            subject_id: subject_id.to_string(),
        },
    )
}

pub(super) fn create_group(
    event_seed: u8,
    origin_seed: u8,
    display_name: &str,
    realm_id: RealmId,
) -> AdminDocumentEvent {
    group_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::GroupCreated {
            realm_id,
            display_name: display_name.to_string(),
            owner: user_id_seed(5),
        },
    )
}

pub(super) fn rename_group(
    event_seed: u8,
    origin_seed: u8,
    origin_seq: u64,
    display_name: &str,
) -> AdminDocumentEvent {
    // Every rename observes the create, so only renames conflict with renames.
    let mut observed = AdminDocumentClock::default();
    observed.advance(node(1), 1);
    if origin_seq > 1 {
        observed.advance(node(origin_seed), origin_seq - 1);
    }
    group_event(
        event_seed,
        node(origin_seed),
        origin_seq,
        observed,
        AdminDocumentOperation::GroupDisplayNameSet {
            display_name: display_name.to_string(),
        },
    )
}

pub(super) fn add_group_role(
    event_seed: u8,
    origin_seed: u8,
    role_id: RoleId,
) -> AdminDocumentEvent {
    group_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::GroupRoleAdded { role_id },
    )
}

pub(super) fn create_group_role(
    event_seed: u8,
    origin_seed: u8,
    role: AdminDocumentRoleDefinition,
) -> AdminDocumentEvent {
    group_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::GroupRoleCreated { role },
    )
}

pub(super) fn remove_group_role(
    event_seed: u8,
    origin_seed: u8,
    role_id: RoleId,
) -> AdminDocumentEvent {
    group_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::GroupRoleRemoved { role_id },
    )
}

pub(super) fn assign_group_user(
    event_seed: u8,
    origin_seed: u8,
    role_id: RoleId,
    user_id: UserId,
) -> AdminDocumentEvent {
    group_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id },
    )
}

pub(super) fn remove_group_assignment(
    event_seed: u8,
    origin_seed: u8,
    role_id: RoleId,
    user_id: UserId,
) -> AdminDocumentEvent {
    group_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id },
    )
}

pub(super) fn add_realm_role(
    event_seed: u8,
    origin_seed: u8,
    role_id: RoleId,
) -> AdminDocumentEvent {
    realm_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmRoleAdded { role_id },
    )
}

pub(super) fn create_realm_role(
    event_seed: u8,
    origin_seed: u8,
    role: AdminDocumentRoleDefinition,
) -> AdminDocumentEvent {
    realm_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmRoleCreated { role },
    )
}

pub(super) fn assign_realm_user(
    event_seed: u8,
    origin_seed: u8,
    role_id: RoleId,
    user_id: UserId,
) -> AdminDocumentEvent {
    realm_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id },
    )
}

pub(super) fn remove_realm_assignment(
    event_seed: u8,
    origin_seed: u8,
    role_id: RoleId,
    user_id: UserId,
) -> AdminDocumentEvent {
    realm_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmRoleUserAssignmentRemoved { role_id, user_id },
    )
}

pub(super) fn ensure_realm_node(
    event_seed: u8,
    origin_seed: u8,
    node_id: NodeId,
    kind: RealmNodeKind,
) -> AdminDocumentEvent {
    realm_config_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigNodeEnsured { node_id, kind },
    )
}

pub(super) fn upsert_oidc_provider(
    event_seed: u8,
    origin_seed: u8,
    provider: OidcProviderConfig,
) -> AdminDocumentEvent {
    realm_config_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider },
    )
}

pub(super) fn set_realm_settings(
    event_seed: u8,
    origin_seed: u8,
    metadata_replication: MetadataReplicationConfig,
    discovery: RealmDiscoveryConfig,
) -> AdminDocumentEvent {
    realm_config_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigSettingsSet {
            metadata_replication,
            discovery,
        },
    )
}

pub(super) fn set_realm_description(
    event_seed: u8,
    origin_seed: u8,
    description: &str,
) -> AdminDocumentEvent {
    realm_config_event(
        event_seed,
        node(origin_seed),
        1,
        AdminDocumentClock::default(),
        AdminDocumentOperation::RealmConfigDescriptionSet {
            description: description.to_string(),
        },
    )
}
