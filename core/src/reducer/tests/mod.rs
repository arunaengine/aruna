use super::{
    AdminDocumentApplyStatus, AdminDocumentAttributeVersion, AdminDocumentConflict,
    AdminDocumentConflictValue, AdminDocumentReducerError, AdminDocumentReducerState,
    GROUP_DISPLAY_NAME_PATH, GROUP_REALM_ID_PATH, REALM_CONFIG_DEFAULT_STRATEGY_PATH,
    REALM_CONFIG_DESCRIPTION_PATH, REALM_CONFIG_DISCOVERY_PATH,
    REALM_CONFIG_METADATA_REPLICATION_PATH, REALM_CONFIG_QUOTA_PATH, USER_NAME_PATH,
    binding_scope_key, group_role_id_from_path, group_role_path,
    group_role_user_assignment_from_path, group_role_user_assignment_path, handle_range_path,
    metadata_replication_value, oidc_provider_value,
    overlay_realm_config_placement_reducer_materialization, placement_binding_handle,
    placement_binding_path, realm_config_node_id_from_path, realm_config_node_path,
    realm_config_oidc_provider_id_from_path, realm_config_oidc_provider_path,
    realm_config_placement_node_id_from_path, realm_config_placement_node_path,
    realm_config_placement_strategy_id_from_path, realm_config_placement_strategy_path,
    realm_config_strategy_binding_path, realm_config_strategy_binding_scope_key_from_path,
    realm_discovery_value, realm_node_kind_from_value, realm_node_kind_value,
    realm_role_id_from_path, realm_role_path, realm_role_user_assignment_from_path,
    realm_role_user_assignment_path, revoked_token_path, role_definition_value,
    user_attribute_path, user_subject_id_path,
};
use crate::admin_documents::{
    AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentRoleDefinition,
    AdminDocumentTarget,
};
use crate::auth::REVOCATION_GRACE_SECS;
use crate::identifiers::PlacementHandle;
use crate::structs::{
    Actor, AffinityEffect, AffinityRule, BindingError, BindingScope, BucketPlan, CandidateMapNode,
    CandidatePlacementMap, CompletionProof, DocumentClass, FIRST_GRANTABLE_HANDLE,
    GroupQuotaOverride, HandleRange, KIND_LABEL_KEY, LabelMatch, MAX_PLACEMENT_SHARD_COUNT,
    MetadataReplicationConfig, NodePlacementEntry, OidcProviderConfig, Permission,
    PlacementBinding, PlacementOverride, PlacementScope, PlacementStrategy, ProofClaim,
    QuotaConfig, RealmConfigDocument, RealmDiscoveryConfig, RealmId, RealmNodeKind,
    STORAGE_CLASS_LABEL_PREFIX, StrategyBinding, TransitionLimits, TransitionPlan,
    TransitionStatus, UserGroupCapOverride,
};
use crate::types::{GroupId, RoleId};
use crate::user_validation::UserAttributeValidationError;
use crate::{NodeId, UserId};
use std::collections::{BTreeMap, BTreeSet};
use ulid::Ulid;

mod fixtures;
use fixtures::*;
mod group;
mod placement;
mod realm_config;
mod revocation;
mod roles;
mod transitions;
mod user;
