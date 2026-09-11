use super::*;

pub const USER_NAME_PATH: &str = "user.name";
pub const GROUP_DISPLAY_NAME_PATH: &str = "group.display_name";
pub const GROUP_REALM_ID_PATH: &str = "group.realm_id";
pub const GROUP_OWNER_PATH: &str = "group.owner";
pub const GROUP_POLICIES_PATH: &str = "group.policies";
pub const REALM_CONFIG_METADATA_REPLICATION_PATH: &str =
    "realm_config.settings.metadata_replication";
pub const REALM_CONFIG_DISCOVERY_PATH: &str = "realm_config.settings.discovery";
pub const REALM_CONFIG_DESCRIPTION_PATH: &str = "realm_config.description";
pub const REALM_CONFIG_QUOTA_PATH: &str = "realm_config.quota";
pub const REALM_CONFIG_COMPUTE_PATH: &str = "realm_config.compute";
pub const REALM_CONFIG_POLICIES_PATH: &str = "realm_config.request_policies";
pub const REALM_CONFIG_DEFAULT_STRATEGY_PATH: &str = "realm_config.placement.default_strategy";
pub const REALM_CONFIG_JOB_FAMILY_PATH: &str = "realm_config.placement.job_family_strategy";
pub const REALM_CONFIG_REVOKED_TOKENS_PATH: &str = "realm_config.revoked_tokens";

pub(super) fn event_observes_dot(event: &AdminDocumentEvent, dot: &AdminDocumentDot) -> bool {
    event.observed.observes(dot)
        || (event.origin_node_id == dot.origin_node_id && event.origin_seq > dot.origin_seq)
}

pub(super) fn operation_paths(op: &AdminDocumentOperation) -> Vec<String> {
    match op {
        AdminDocumentOperation::GroupRoleAdded { role_id }
        | AdminDocumentOperation::GroupRoleCreated {
            role: AdminDocumentRoleDefinition { role_id, .. },
        }
        | AdminDocumentOperation::GroupRoleRemoved { role_id } => vec![group_role_path(role_id)],
        AdminDocumentOperation::GroupRoleUserAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::GroupRoleUserAssignmentRemoved { role_id, user_id } => {
            vec![group_role_user_assignment_path(role_id, user_id)]
        }
        AdminDocumentOperation::GroupJoinRequested { request } => {
            vec![crate::join_request::request_path(request.request_id)]
        }
        AdminDocumentOperation::GroupJoinDecided { decision } => {
            let mut paths = vec![crate::join_request::decision_path(decision.request_id)];
            paths.extend(
                decision
                    .role_ids
                    .iter()
                    .map(|role_id| group_role_user_assignment_path(role_id, &decision.user_id)),
            );
            paths
        }
        AdminDocumentOperation::UserAttributeSet { key, .. }
        | AdminDocumentOperation::UserAttributeRemoved { key } => vec![user_attribute_path(key)],
        AdminDocumentOperation::UserNameSet { .. } => vec![USER_NAME_PATH.to_string()],
        AdminDocumentOperation::UserSubjectIdAdded { subject_id }
        | AdminDocumentOperation::UserSubjectIdRemoved { subject_id } => {
            vec![user_subject_id_path(subject_id)]
        }
        AdminDocumentOperation::RealmRoleAdded { role_id }
        | AdminDocumentOperation::RealmRoleCreated {
            role: AdminDocumentRoleDefinition { role_id, .. },
        } => vec![realm_role_path(role_id)],
        AdminDocumentOperation::RealmRoleUserAssignmentAdded { role_id, user_id }
        | AdminDocumentOperation::RealmRoleUserAssignmentRemoved { role_id, user_id } => {
            vec![realm_role_user_assignment_path(role_id, user_id)]
        }
        AdminDocumentOperation::RealmConfigNodeEnsured { node_id, .. }
        | AdminDocumentOperation::RealmConfigNodeRemoved { node_id } => {
            vec![realm_config_node_path(node_id)]
        }
        AdminDocumentOperation::RealmConfigOidcProviderUpserted { provider } => {
            vec![realm_config_oidc_provider_path(&provider.id)]
        }
        AdminDocumentOperation::RealmConfigOidcProviderRemoved { provider_id } => {
            vec![realm_config_oidc_provider_path(provider_id)]
        }
        AdminDocumentOperation::RealmConfigSettingsSet { .. } => vec![
            REALM_CONFIG_METADATA_REPLICATION_PATH.to_string(),
            REALM_CONFIG_DISCOVERY_PATH.to_string(),
        ],
        AdminDocumentOperation::GroupCreated { .. } => vec![
            GROUP_DISPLAY_NAME_PATH.to_string(),
            GROUP_REALM_ID_PATH.to_string(),
            GROUP_OWNER_PATH.to_string(),
        ],
        AdminDocumentOperation::GroupDisplayNameSet { .. } => {
            vec![GROUP_DISPLAY_NAME_PATH.to_string()]
        }
        AdminDocumentOperation::RealmConfigDescriptionSet { .. } => {
            vec![REALM_CONFIG_DESCRIPTION_PATH.to_string()]
        }
        AdminDocumentOperation::RealmConfigQuotaSet { .. } => {
            vec![REALM_CONFIG_QUOTA_PATH.to_string()]
        }
        AdminDocumentOperation::RealmConfigComputeSet { .. } => {
            vec![REALM_CONFIG_COMPUTE_PATH.to_string()]
        }
        AdminDocumentOperation::RealmConfigPoliciesSet { .. } => {
            vec![REALM_CONFIG_POLICIES_PATH.to_string()]
        }
        AdminDocumentOperation::GroupPoliciesSet { .. } => {
            vec![GROUP_POLICIES_PATH.to_string()]
        }
        AdminDocumentOperation::RealmConfigNodePlacementSet { entry } => {
            vec![realm_config_placement_node_path(&entry.node_id)]
        }
        AdminDocumentOperation::RealmConfigNodePlacementRemoved { node_id } => {
            vec![realm_config_placement_node_path(node_id)]
        }
        AdminDocumentOperation::RealmConfigPlacementStrategyUpserted { strategy } => {
            vec![realm_config_placement_strategy_path(&strategy.strategy_id)]
        }
        AdminDocumentOperation::RealmConfigPlacementStrategyRemoved { strategy_id } => {
            vec![realm_config_placement_strategy_path(strategy_id)]
        }
        AdminDocumentOperation::RealmConfigDefaultStrategySet { .. } => {
            vec![REALM_CONFIG_DEFAULT_STRATEGY_PATH.to_string()]
        }
        AdminDocumentOperation::RealmConfigJobFamilySet { .. } => {
            vec![REALM_CONFIG_JOB_FAMILY_PATH.to_string()]
        }
        AdminDocumentOperation::RealmConfigStrategyBindingSet { binding } => {
            vec![realm_config_strategy_binding_path(&binding.scope)]
        }
        AdminDocumentOperation::RealmConfigStrategyBindingRemoved { scope } => {
            vec![realm_config_strategy_binding_path(scope)]
        }
        AdminDocumentOperation::RealmConfigPlacementOverrideSet { record } => {
            vec![realm_config_placement_override_path(&record.subject)]
        }
        AdminDocumentOperation::RealmConfigPlacementOverrideRemoved { subject } => {
            vec![realm_config_placement_override_path(subject)]
        }
        AdminDocumentOperation::RealmConfigPlacementBindingAppended { binding } => {
            vec![placement_binding_path(binding.handle)]
        }
        AdminDocumentOperation::RealmConfigCandidateMapPublished { map } => {
            vec![candidate_map_path(map.epoch)]
        }
        AdminDocumentOperation::RealmConfigActivationsInitialized { strategy_id, .. } => {
            vec![activation_path(strategy_id)]
        }
        AdminDocumentOperation::RealmConfigTransitionStarted { plan } => {
            vec![transition_path(&plan.transition_id)]
        }
        AdminDocumentOperation::RealmConfigTransitionBarrierReported {
            transition_id,
            bucket,
            reported_by,
            ..
        } => {
            vec![transition_barrier_path(transition_id, *bucket, reported_by)]
        }
        AdminDocumentOperation::RealmConfigTransitionProofSubmitted {
            transition_id,
            proof,
            ..
        } => {
            vec![transition_proof_path(
                transition_id,
                proof.bucket,
                &proof.holder,
            )]
        }
        AdminDocumentOperation::RealmConfigTransitionAborted { transition_id } => {
            vec![transition_abort_path(transition_id)]
        }
        AdminDocumentOperation::RealmConfigTransitionBucketForced {
            transition_id,
            bucket,
            ..
        } => {
            vec![transition_force_path(transition_id, *bucket)]
        }
        AdminDocumentOperation::RealmConfigTransitionStallReported {
            transition_id,
            bucket,
            reported_by,
            ..
        } => {
            vec![transition_stall_path(transition_id, *bucket, reported_by)]
        }
        AdminDocumentOperation::RealmConfigTransitionDrainReported {
            transition_id,
            bucket,
            reported_by,
        } => {
            vec![transition_drain_path(transition_id, *bucket, reported_by)]
        }
        AdminDocumentOperation::RealmConfigHandleRangeGranted { range } => {
            vec![handle_range_path(range.range_id)]
        }
        AdminDocumentOperation::RealmConfigBandPoolAssigned { pool } => {
            vec![band_pool_path(pool.pool_id)]
        }
        AdminDocumentOperation::RealmConfigTokenRevoked {
            token_hash,
            expires_at,
            token_owner,
        } => {
            vec![revoked_token_path(token_hash, *expires_at, token_owner)]
        }
    }
}

pub(super) fn role_definition_value(role: &AdminDocumentRoleDefinition) -> String {
    serde_json::to_string(role).expect("admin document role definition serializes")
}

pub fn user_attribute_path(key: &str) -> String {
    format!("user.attributes.{key}")
}

pub fn user_subject_id_path(subject_id: &str) -> String {
    format!("user.subject_ids.{subject_id}")
}

pub fn group_role_path(role_id: &RoleId) -> String {
    format!("group.roles.{role_id}")
}

pub fn group_role_user_assignment_path(role_id: &RoleId, user_id: &UserId) -> String {
    format!("group.roles.{role_id}.assigned_users.{user_id}")
}

pub fn realm_role_path(role_id: &RoleId) -> String {
    format!("realm.roles.{role_id}")
}

pub fn realm_role_user_assignment_path(role_id: &RoleId, user_id: &UserId) -> String {
    format!("realm.roles.{role_id}.assigned_users.{user_id}")
}

pub fn realm_config_node_path(node_id: &NodeId) -> String {
    format!("realm_config.nodes.{node_id}")
}

pub fn realm_config_oidc_provider_path(provider_id: &str) -> String {
    format!("realm_config.oidc_providers.{provider_id}")
}
pub fn binding_scope_key(scope: &BindingScope) -> String {
    match scope {
        BindingScope::Realm => "realm".to_string(),
        BindingScope::Group(group_id) => format!("group:{group_id}"),
        BindingScope::Class(class) => match class {
            DocumentClass::Admin => "class:admin",
            DocumentClass::Group => "class:group",
            DocumentClass::User => "class:user",
            DocumentClass::Metadata => "class:metadata",
            DocumentClass::MetadataRegistry => "class:metadata_registry",
            DocumentClass::JobControl => "class:job_control",
            DocumentClass::PlacementPolicy => "class:placement_policy",
        }
        .to_string(),
        BindingScope::MetadataPathPrefix(prefix) => format!(
            "metadata_path_prefix:{}",
            MetadataRegistryRecord::normalize_document_path(prefix)
        ),
    }
}

pub(super) fn normalized_binding_scope(scope: &BindingScope) -> BindingScope {
    match scope {
        BindingScope::MetadataPathPrefix(prefix) => BindingScope::MetadataPathPrefix(
            MetadataRegistryRecord::normalize_document_path(prefix),
        ),
        BindingScope::Realm => BindingScope::Realm,
        BindingScope::Group(group_id) => BindingScope::Group(*group_id),
        BindingScope::Class(class) => BindingScope::Class(*class),
    }
}

pub(super) fn normalized_strategy_binding(binding: &StrategyBinding) -> StrategyBinding {
    StrategyBinding {
        scope: normalized_binding_scope(&binding.scope),
        strategy_id: binding.strategy_id,
    }
}

pub(super) fn metadata_replication_value(metadata_replication: &MetadataReplicationConfig) -> String {
    serde_json::to_string(metadata_replication)
        .expect("admin document metadata replication config serializes")
}

pub(super) fn realm_discovery_value(discovery: &RealmDiscoveryConfig) -> String {
    serde_json::to_string(discovery).expect("admin document realm discovery config serializes")
}

pub(super) fn policies_value(policies: &[crate::request_policy::RequestPolicy]) -> String {
    serde_json::to_string(policies).expect("admin document policies serialize")
}

pub(super) fn policies_from_value(value: &str) -> Option<Vec<crate::request_policy::RequestPolicy>> {
    serde_json::from_str(value).ok()
}

pub(super) fn quota_value(quota: &QuotaConfig) -> String {
    serde_json::to_string(&supported_quota(quota)).expect("admin document quota config serializes")
}

pub(super) fn supported_quota(quota: &QuotaConfig) -> QuotaConfig {
    let mut quota = quota.clone();
    quota.group_overrides.sort_by_key(|over| over.group_id);
    quota
        .user_group_cap_overrides
        .sort_by_key(|over| over.user_id);
    quota
}

pub(super) fn placement_entry_value(entry: &NodePlacementEntry) -> String {
    serde_json::to_string(entry).expect("admin document placement entry serializes")
}

pub(super) fn placement_strategy_value(strategy: &PlacementStrategy) -> String {
    serde_json::to_string(strategy).expect("admin document placement strategy serializes")
}

pub(super) fn strategy_binding_value(binding: &StrategyBinding) -> String {
    serde_json::to_string(&normalized_strategy_binding(binding))
        .expect("admin document strategy binding serializes")
}

pub(super) fn placement_override_value(record: &PlacementOverride) -> String {
    serde_json::to_string(record).expect("admin document placement override serializes")
}

pub(super) fn placement_binding_value(binding: &PlacementBinding) -> String {
    serde_json::to_string(binding).expect("admin document placement binding serializes")
}

pub(super) fn oidc_provider_value(provider: &OidcProviderConfig) -> String {
    serde_json::to_string(provider).expect("admin document OIDC provider config serializes")
}

pub(super) fn realm_node_kind_value(kind: &RealmNodeKind) -> String {
    serde_json::to_string(kind).expect("realm node kind serializes")
}

pub fn group_role_id_from_path(path: &str) -> Option<RoleId> {
    let role_id = path.strip_prefix("group.roles.")?;

    if role_id.contains(".assigned_users.") {
        return None;
    }

    Ulid::from_string(role_id).ok()
}

pub fn group_role_user_assignment_from_path(path: &str) -> Option<(RoleId, UserId)> {
    let path = path.strip_prefix("group.roles.")?;
    let (role_id, user_id) = path.split_once(".assigned_users.")?;

    Some((
        Ulid::from_string(role_id).ok()?,
        UserId::from_string(user_id).ok()?,
    ))
}

pub(super) fn group_role_user_assignment_role_id_from_path(path: &str) -> Option<RoleId> {
    group_role_user_assignment_from_path(path).map(|(role_id, _)| role_id)
}

pub fn realm_role_id_from_path(path: &str) -> Option<RoleId> {
    let role_id = path.strip_prefix("realm.roles.")?;

    if role_id.contains(".assigned_users.") {
        return None;
    }

    Ulid::from_string(role_id).ok()
}

pub fn realm_role_user_assignment_from_path(path: &str) -> Option<(RoleId, UserId)> {
    let path = path.strip_prefix("realm.roles.")?;
    let (role_id, user_id) = path.split_once(".assigned_users.")?;

    Some((
        Ulid::from_string(role_id).ok()?,
        UserId::from_string(user_id).ok()?,
    ))
}

pub(super) fn realm_role_user_assignment_role_id_from_path(path: &str) -> Option<RoleId> {
    realm_role_user_assignment_from_path(path).map(|(role_id, _)| role_id)
}

pub fn realm_config_node_id_from_path(path: &str) -> Option<NodeId> {
    let node_id = path.strip_prefix("realm_config.nodes.")?;
    NodeId::from_str(node_id).ok()
}

pub fn realm_config_oidc_provider_id_from_path(path: &str) -> Option<&str> {
    path.strip_prefix("realm_config.oidc_providers.")
}

pub(super) fn oidc_provider_from_value(value: &str) -> Option<OidcProviderConfig> {
    serde_json::from_str(value).ok()
}

pub(super) fn metadata_replication_from_value(value: &str) -> Option<MetadataReplicationConfig> {
    serde_json::from_str(value).ok()
}

pub(super) fn realm_discovery_from_value(value: &str) -> Option<RealmDiscoveryConfig> {
    serde_json::from_str(value).ok()
}

/// The compute configuration is stored canonically: link and quota order must
/// not decide whether two publishers agree.
pub(super) fn compute_value(compute: &RealmComputeConfig) -> String {
    serde_json::to_string(&canonical_compute(compute))
        .expect("admin document compute config serializes")
}

pub(super) fn canonical_compute(compute: &RealmComputeConfig) -> RealmComputeConfig {
    let mut compute = compute.clone();
    compute
        .links
        .sort_by(|left, right| (&left.from, &left.to).cmp(&(&right.from, &right.to)));
    compute.group_quotas.sort_by_key(|entry| entry.group_id);
    compute
}

pub(super) fn compute_from_value(value: &str) -> Option<RealmComputeConfig> {
    serde_json::from_str(value)
        .ok()
        .map(|compute| canonical_compute(&compute))
}

pub(super) fn quota_from_value(value: &str) -> Option<QuotaConfig> {
    serde_json::from_str(value)
        .ok()
        .map(|quota| supported_quota(&quota))
}
pub(super) fn realm_node_kind_from_value(value: &str) -> Option<RealmNodeKind> {
    serde_json::from_str(value).ok()
}
