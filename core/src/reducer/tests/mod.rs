use super::{
    AdminApplyStatus, AdminAttributeVersion, AdminConflict, AdminConflictValue, AdminDocumentError,
    AdminDocumentState, CONFIG_DESCRIPTION_PATH, CONFIG_DISCOVERY_PATH, CONFIG_QUOTA_PATH,
    CONFIG_STRATEGY_PATH, DISPLAY_NAME_PATH, METADATA_REPLICATION_PATH, REALM_ID_PATH,
    USER_NAME_PATH, binding_scope_key, config_node_path, config_oidc_path, decode_node_kind,
    group_role_path, group_user_path, handle_range_path, metadata_replication_value,
    node_kind_value, oidc_provider_value, overlay_placement, parse_config_node, parse_config_oidc,
    parse_group_assignment, parse_group_role, parse_placement_node, parse_placement_strategy,
    parse_realm_assignment, parse_realm_role, parse_strategy_scope, placement_binding_handle,
    placement_binding_path, placement_node_path, placement_strategy_path, realm_discovery_value,
    realm_role_path, realm_user_path, revoked_token_path, role_definition_value,
    strategy_binding_path, user_attribute_path, user_subject_path,
};
use crate::admin_documents::{AdminDocumentClock, AdminDocumentEvent, AdminDocumentOperation};
use crate::auth::REVOCATION_GRACE_SECS;
use crate::structs::identity::realm::{
    GroupQuotaOverride, MetadataReplicationConfig, QuotaConfig, RealmConfigDocument,
    RealmDiscoveryConfig, RealmNodeKind, UserCapOverride,
};
use crate::structs::placement::binding_directory::BindingError;
use crate::structs::placement::placement_record::{
    AffinityEffect, AffinityRule, BindingScope, DocumentClass, FIRST_GRANTABLE_HANDLE, HandleRange,
    LabelMatch, MAX_SHARD_COUNT, NodePlacementEntry, PlacementBinding, PlacementOverride,
    PlacementScope, PlacementStrategy, StrategyBinding,
};
use crate::structs::placement::placement_transition::{
    BucketPlan, CandidateMapNode, CandidatePlacementMap, CompletionProof, ProofClaim,
    TransitionLimits, TransitionPlan, TransitionStatus,
};
use crate::structs::storage::node_info::{CLASS_LABEL_PREFIX, KIND_LABEL_KEY};
use crate::structured_id::PlacementHandle;
use crate::user_validation::UserAttributeError;
use crate::{NodeId, UserId};
use std::collections::{BTreeMap, BTreeSet};
use ulid::Ulid;

use crate::tests::reducer::*;
mod group;
mod placement;
mod realm_config;
mod revocation;
mod roles;
mod transitions;
mod user;
