use super::{
    AdminDocumentApplyStatus, AdminDocumentAttributeVersion, AdminDocumentConflict,
    AdminDocumentConflictValue, AdminDocumentReducerError, AdminDocumentReducerState,
    GROUP_DISPLAY_NAME_PATH, GROUP_REALM_ID_PATH, REALM_CONFIG_DEFAULT_STRATEGY_PATH,
    REALM_CONFIG_DESCRIPTION_PATH, REALM_CONFIG_DISCOVERY_PATH,
    REALM_CONFIG_METADATA_REPLICATION_PATH, REALM_CONFIG_QUOTA_PATH, USER_NAME_PATH,
    binding_scope_key, config_node_path, config_oidc_path, decode_node_kind, group_role_path,
    group_user_path, handle_range_path, metadata_replication_value, node_kind_value,
    oidc_provider_value, overlay_placement, parse_config_node, parse_config_oidc,
    parse_group_assignment, parse_group_role, parse_placement_node, parse_placement_strategy,
    parse_realm_assignment, parse_realm_role, parse_strategy_scope, placement_binding_handle,
    placement_binding_path, placement_node_path, placement_strategy_path, realm_discovery_value,
    realm_role_path, realm_user_path, revoked_token_path, role_definition_value,
    strategy_binding_path, user_attribute_path, user_subject_path,
};
use crate::admin_documents::{
    AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation, AdminDocumentRoleDefinition,
    AdminDocumentTarget,
};
use crate::auth::REVOCATION_GRACE_SECS;
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
use crate::structured_id::PlacementHandle;
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
