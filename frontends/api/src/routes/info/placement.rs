//! Serves the realm placement routes and their request and response types.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::require_realm_admin;
use crate::auth::{RealmAuth, ensure_permission};
use crate::error::{ServerError, ServerResult};
use crate::lookup::actor;
use crate::server::state::ServerState;
use aruna_clock::unix_timestamp_millis;
use aruna_core::errors::StorageError;
use aruna_core::placement::transition::transition_health;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::structs::placement::record::PlacementScope;
use aruna_ops_identity::document_repository::GetConfigOperation;
use aruna_ops_identity::placement::allocate_handle::HandleAllocationError;
use aruna_ops_identity::placement::mutation::PlacementMutationOperation;
use aruna_ops_identity::placement::provision::ProvisionBindingOperation;
use aruna_ops_identity::realm::mutate_placement::{
    MutatePlacementConfig, MutatePlacementError, RealmPlacementMutation,
};
use aruna_runner::driver::drive;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ulid::Ulid;
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmPlacementConfigResponse)]
pub struct RealmPlacementResponse {
    pub strategies: Vec<RealmPlacementStrategy>,
    pub default_strategy_id: Option<String>,
    #[serde(rename = "job_family_strategy_id")]
    pub family_strategy_id: String,
    pub bindings: Vec<RealmBinding>,
    pub overrides: Vec<RealmPlacementOverride>,
    pub transitions: RealmHealthResponse,
}

/// Health of the realm's in-flight placement transitions. Counts only: nothing
/// here changes where a request routes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmTransitionHealthResponse)]
pub struct RealmHealthResponse {
    pub active: usize,
    pub incomplete_buckets: usize,
    pub stalled_buckets: usize,
    /// Transitions still incomplete after a day.
    pub overdue: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmPlacementStrategy {
    pub strategy_id: String,
    pub name: String,
    pub replica_count: Option<u32>,
    pub distinct_locations: bool,
    pub affinity: Vec<RealmAffinityRule>,
    pub shard_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmPlacementAffinityRule)]
pub struct RealmAffinityRule {
    pub key: String,
    pub value: String,
    pub effect: RealmAffinityEffect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schema(as = RealmPlacementAffinityEffect)]
pub enum RealmAffinityEffect {
    Filter,
    Multiply { permille: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmPlacementBinding)]
pub struct RealmBinding {
    pub scope: RealmBindingScope,
    pub strategy_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schema(as = RealmPlacementBindingScope)]
pub enum RealmBindingScope {
    Realm,
    Group { group_id: String },
    Class { document_class: RealmPlacementClass },
    MetadataPathPrefix { prefix: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[schema(as = RealmPlacementDocumentClass)]
pub enum RealmPlacementClass {
    Admin,
    Group,
    User,
    Metadata,
    MetadataRegistry,
    JobControl,
    PlacementPolicy,
    UserVault,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmPlacementOverride {
    pub subject: String,
    pub pinned: Vec<String>,
    pub excluded: Vec<String>,
    pub strategy_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "mutation", rename_all = "snake_case", deny_unknown_fields)]
#[schema(as = RealmPlacementMutationRequest)]
pub enum RealmPlacementRequest {
    UpsertStrategy {
        strategy: RealmPlacementStrategy,
    },
    RemoveStrategy {
        strategy_id: String,
    },
    SetDefaultStrategy {
        strategy_id: String,
    },
    SetBinding {
        binding: RealmBinding,
    },
    RemoveBinding {
        scope: RealmBindingScope,
    },
    SetOverride {
        placement_override: RealmPlacementOverride,
    },
    RemoveOverride {
        subject: String,
    },
    ProvisionMetadataBinding {
        strategy_id: String,
        group_id: Option<String>,
    },
    SetNodeAttributes {
        node_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        location: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        labels: Option<Vec<RealmPlacementLabel>>,
    },
}

/// One node placement label. Absent from a request means unchanged; a present
/// list replaces the whole set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmPlacementNodeLabel)]
pub struct RealmPlacementLabel {
    pub key: String,
    pub value: String,
}

enum RealmPlacementAction {
    Mutation(RealmPlacementMutation),
    Provision {
        strategy_id: Ulid,
        group_id: Option<Ulid>,
    },
}

impl RealmPlacementResponse {
    fn from_document(document: &RealmConfigDocument) -> Self {
        let health = transition_health(document, unix_timestamp_millis());
        Self {
            transitions: RealmHealthResponse {
                active: health.active,
                incomplete_buckets: health.incomplete_buckets,
                stalled_buckets: health.stalled_buckets,
                overdue: health.overdue,
            },
            strategies: document
                .strategies
                .iter()
                .map(RealmPlacementStrategy::from)
                .collect(),
            default_strategy_id: document.default_strategy_id.map(|id| id.to_string()),
            family_strategy_id: document.family_strategy_id.to_string(),
            bindings: document
                .strategy_bindings
                .iter()
                .map(RealmBinding::from)
                .collect(),
            overrides: document
                .placement_overrides
                .iter()
                .map(RealmPlacementOverride::from)
                .collect(),
        }
    }
}

impl From<&aruna_core::structs::placement::record::PlacementStrategy> for RealmPlacementStrategy {
    fn from(strategy: &aruna_core::structs::placement::record::PlacementStrategy) -> Self {
        Self {
            strategy_id: strategy.strategy_id.to_string(),
            name: strategy.name.clone(),
            replica_count: strategy.replica_count,
            distinct_locations: strategy.distinct_locations,
            affinity: strategy
                .affinity
                .iter()
                .map(|rule| RealmAffinityRule {
                    key: rule.matcher.key.clone(),
                    value: rule.matcher.value.clone(),
                    effect: match rule.effect {
                        aruna_core::structs::placement::record::AffinityEffect::Filter => {
                            RealmAffinityEffect::Filter
                        }
                        aruna_core::structs::placement::record::AffinityEffect::Multiply {
                            permille,
                        } => RealmAffinityEffect::Multiply { permille },
                    },
                })
                .collect(),
            shard_count: strategy.shard_count,
        }
    }
}

impl RealmPlacementStrategy {
    fn into_core(self) -> ServerResult<aruna_core::structs::placement::record::PlacementStrategy> {
        Ok(aruna_core::structs::placement::record::PlacementStrategy {
            strategy_id: parse_ulid(&self.strategy_id, "strategy_id")?,
            name: self.name,
            replica_count: self.replica_count,
            distinct_locations: self.distinct_locations,
            affinity: self
                .affinity
                .into_iter()
                .map(
                    |rule| aruna_core::structs::placement::record::AffinityRule {
                        matcher: aruna_core::structs::placement::record::LabelMatch {
                            key: rule.key,
                            value: rule.value,
                        },
                        effect: match rule.effect {
                            RealmAffinityEffect::Filter => {
                                aruna_core::structs::placement::record::AffinityEffect::Filter
                            }
                            RealmAffinityEffect::Multiply { permille } => {
                                aruna_core::structs::placement::record::AffinityEffect::Multiply {
                                    permille,
                                }
                            }
                        },
                    },
                )
                .collect(),
            shard_count: self.shard_count,
        })
    }
}

impl From<&aruna_core::structs::placement::record::StrategyBinding> for RealmBinding {
    fn from(binding: &aruna_core::structs::placement::record::StrategyBinding) -> Self {
        Self {
            scope: RealmBindingScope::from(&binding.scope),
            strategy_id: binding.strategy_id.to_string(),
        }
    }
}

impl RealmBinding {
    fn into_core(self) -> ServerResult<aruna_core::structs::placement::record::StrategyBinding> {
        Ok(aruna_core::structs::placement::record::StrategyBinding {
            scope: self.scope.into_core()?,
            strategy_id: parse_ulid(&self.strategy_id, "strategy_id")?,
        })
    }
}

impl From<&aruna_core::structs::placement::record::BindingScope> for RealmBindingScope {
    fn from(scope: &aruna_core::structs::placement::record::BindingScope) -> Self {
        match scope {
            aruna_core::structs::placement::record::BindingScope::Realm => Self::Realm,
            aruna_core::structs::placement::record::BindingScope::Group(group_id) => Self::Group {
                group_id: group_id.to_string(),
            },
            aruna_core::structs::placement::record::BindingScope::Class(document_class) => {
                Self::Class {
                    document_class: RealmPlacementClass::from(*document_class),
                }
            }
            aruna_core::structs::placement::record::BindingScope::MetadataPathPrefix(prefix) => {
                Self::MetadataPathPrefix {
                    prefix: prefix.clone(),
                }
            }
        }
    }
}

impl RealmBindingScope {
    fn into_core(self) -> ServerResult<aruna_core::structs::placement::record::BindingScope> {
        Ok(match self {
            Self::Realm => aruna_core::structs::placement::record::BindingScope::Realm,
            Self::Group { group_id } => {
                aruna_core::structs::placement::record::BindingScope::Group(parse_ulid(
                    &group_id, "group_id",
                )?)
            }
            Self::Class { document_class } => {
                aruna_core::structs::placement::record::BindingScope::Class(document_class.into())
            }
            Self::MetadataPathPrefix { prefix } => {
                aruna_core::structs::placement::record::BindingScope::MetadataPathPrefix(prefix)
            }
        })
    }
}

impl From<aruna_core::structs::placement::record::DocumentClass> for RealmPlacementClass {
    fn from(document_class: aruna_core::structs::placement::record::DocumentClass) -> Self {
        match document_class {
            aruna_core::structs::placement::record::DocumentClass::Admin => Self::Admin,
            aruna_core::structs::placement::record::DocumentClass::Group => Self::Group,
            aruna_core::structs::placement::record::DocumentClass::User => Self::User,
            aruna_core::structs::placement::record::DocumentClass::Metadata => Self::Metadata,
            aruna_core::structs::placement::record::DocumentClass::MetadataRegistry => {
                Self::MetadataRegistry
            }
            aruna_core::structs::placement::record::DocumentClass::JobControl => Self::JobControl,
            aruna_core::structs::placement::record::DocumentClass::PlacementPolicy => {
                Self::PlacementPolicy
            }
            aruna_core::structs::placement::record::DocumentClass::UserVault => Self::UserVault,
        }
    }
}

impl From<RealmPlacementClass> for aruna_core::structs::placement::record::DocumentClass {
    fn from(document_class: RealmPlacementClass) -> Self {
        match document_class {
            RealmPlacementClass::Admin => Self::Admin,
            RealmPlacementClass::Group => Self::Group,
            RealmPlacementClass::User => Self::User,
            RealmPlacementClass::Metadata => Self::Metadata,
            RealmPlacementClass::MetadataRegistry => Self::MetadataRegistry,
            RealmPlacementClass::JobControl => Self::JobControl,
            RealmPlacementClass::PlacementPolicy => Self::PlacementPolicy,
            RealmPlacementClass::UserVault => Self::UserVault,
        }
    }
}

impl From<&aruna_core::structs::placement::record::PlacementOverride> for RealmPlacementOverride {
    fn from(record: &aruna_core::structs::placement::record::PlacementOverride) -> Self {
        Self {
            subject: hex::encode(&record.subject),
            pinned: record.pinned.iter().map(ToString::to_string).collect(),
            excluded: record.excluded.iter().map(ToString::to_string).collect(),
            strategy_id: record.strategy_id.map(|id| id.to_string()),
        }
    }
}

impl RealmPlacementOverride {
    fn into_core(self) -> ServerResult<aruna_core::structs::placement::record::PlacementOverride> {
        Ok(aruna_core::structs::placement::record::PlacementOverride {
            subject: parse_subject(&self.subject)?,
            pinned: parse_node_ids(self.pinned, "pinned")?,
            excluded: parse_node_ids(self.excluded, "excluded")?,
            strategy_id: self
                .strategy_id
                .map(|id| parse_ulid(&id, "strategy_id"))
                .transpose()?,
        })
    }
}

impl RealmPlacementRequest {
    fn into_core(self) -> ServerResult<RealmPlacementAction> {
        let mutation = match self {
            Self::UpsertStrategy { strategy } => {
                RealmPlacementMutation::UpsertStrategy(strategy.into_core()?)
            }
            Self::RemoveStrategy { strategy_id } => {
                RealmPlacementMutation::RemoveStrategy(parse_ulid(&strategy_id, "strategy_id")?)
            }
            Self::SetDefaultStrategy { strategy_id } => {
                RealmPlacementMutation::SetDefaultStrategy(parse_ulid(&strategy_id, "strategy_id")?)
            }
            Self::SetBinding { binding } => {
                RealmPlacementMutation::SetBinding(binding.into_core()?)
            }
            Self::RemoveBinding { scope } => {
                RealmPlacementMutation::RemoveBinding(scope.into_core()?)
            }
            Self::SetOverride { placement_override } => {
                RealmPlacementMutation::SetOverride(placement_override.into_core()?)
            }
            Self::RemoveOverride { subject } => {
                RealmPlacementMutation::RemoveOverride(parse_subject(&subject)?)
            }
            Self::SetNodeAttributes {
                node_id,
                location,
                labels,
            } => RealmPlacementMutation::SetNodeAttributes {
                node_id: parse_node_id(&node_id)?,
                location,
                labels: labels.map(|labels| {
                    labels
                        .into_iter()
                        .map(|label| (label.key, label.value))
                        .collect()
                }),
            },
            Self::ProvisionMetadataBinding {
                strategy_id,
                group_id,
            } => {
                return Ok(RealmPlacementAction::Provision {
                    strategy_id: parse_ulid(&strategy_id, "strategy_id")?,
                    group_id: group_id
                        .map(|group_id| parse_ulid(&group_id, "group_id"))
                        .transpose()?,
                });
            }
        };
        Ok(RealmPlacementAction::Mutation(mutation))
    }
}

fn parse_ulid(value: &str, field: &str) -> ServerResult<Ulid> {
    Ulid::from_string(value)
        .map_err(|_| ServerError::BadRequestReason(format!("invalid {field}: {value}")))
}

fn parse_subject(value: &str) -> ServerResult<Vec<u8>> {
    hex::decode(value)
        .map_err(|_| ServerError::BadRequestReason("subject must be valid hex".to_string()))
}

fn parse_node_id(value: &str) -> ServerResult<aruna_core::id::NodeId> {
    value
        .parse::<aruna_core::id::NodeId>()
        .map_err(|_| ServerError::BadRequestReason(format!("invalid node_id: {value}")))
}

fn parse_node_ids(values: Vec<String>, field: &str) -> ServerResult<Vec<aruna_core::id::NodeId>> {
    values
        .into_iter()
        .map(|value| {
            value.parse::<aruna_core::id::NodeId>().map_err(|_| {
                ServerError::BadRequestReason(format!("invalid node id in {field}: {value}"))
            })
        })
        .collect()
}

#[utoipa::path(
    get,
    path = "/system/realm/placement",
    tag = "system/realm",
    summary = "Read record placement strategies, bindings and overrides",
    description = r#"Returns the record placement strategies as stored in this node's copy of the realm configuration.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- Carries the defined strategies with their replica count, distinctness requirement, affinity rules
  and shard count; the default strategy; the immutable job-family strategy id; the bindings that
  map a scope (the realm, a group, a document class or a metadata path prefix) to a strategy; and
  the per-subject overrides that pin or exclude individual nodes.
- Strategies place metadata records and job families. They never decide where S3 object bytes may
  live; that is what the placement policy documents under `/data/placement/policies` do.
- This is a rule set, not a placement result: it says how replicas are chosen, not where any
  particular document currently sits.

**Limits**
- The strategy named by `job_family_strategy_id` cannot be removed or have its shard count
  reshaped, and a strategy a binding or override still references cannot be removed."#,
    responses(
        (
            status = 200,
            description = "The realm's record placement strategies as this management node has them",
            body = RealmPlacementResponse,
            example = json!({
                "strategies": [
                    {
                        "strategy_id": "01JABCDEF0123456789ABCDEFG",
                        "name": "three-replicas-across-sites",
                        "replica_count": 3,
                        "distinct_locations": true,
                        "affinity": [{"key": "zone", "value": "dc-a", "effect": {"kind": "multiply", "permille": 1500}}],
                        "shard_count": 16
                    }
                ],
                "default_strategy_id": "01JABCDEF0123456789ABCDEFG",
                "job_family_strategy_id": "01JABCDEF0123456789ABCDEFG",
                "bindings": [
                    {"scope": {"kind": "class", "document_class": "metadata"}, "strategy_id": "01JABCDEF0123456789ABCDEFG"}
                ],
                "overrides": [
                    {
                        "subject": "0102030405060708",
                        "pinned": ["1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978"],
                        "excluded": [],
                        "strategy_id": null
                    }
                ],
                "transitions": {
                    "active": 1,
                    "incomplete_buckets": 2,
                    "stalled_buckets": 0,
                    "overdue": 0
                }
            })
        ),
        (status = 403, description = "Caller is not a realm config admin", body = crate::error::ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = crate::error::ErrorResponse),
        (status = 500, description = "The stored realm configuration could not be read or decoded here", body = crate::error::ErrorResponse),
        (status = 503, description = "Called on a node that is not a management node and no management node was reachable; code `no_management_node`", body = crate::error::ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_realm_placement(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<RealmPlacementResponse>)> {
    require_realm_admin(&state, auth).await?;
    let document = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| match error {
        aruna_ops_identity::document_repository::GetConfigError::DocumentNotFound => {
            ServerError::NotFound
        }
        other => ServerError::InternalError(other.into()),
    })?;
    Ok((
        StatusCode::OK,
        Json(RealmPlacementResponse::from_document(&document)),
    ))
}

#[utoipa::path(
    patch,
    path = "/system/realm/placement",
    tag = "system/realm",
    summary = "Apply one change to the record placement strategies",
    description = r#"Applies exactly one change to the realm's record placement strategies and returns all of them.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- The body carries exactly one change, selected by its `mutation` field: define or replace a
  strategy, remove one, set the default, set or remove a binding for a scope, set or remove a
  per-subject override, edit a node's placement attributes, or provision a metadata binding for a
  strategy.
- `set_node_attributes` edits the location and labels of a node the realm already places. An
  omitted field keeps its stored value and a present `labels` list replaces the whole set. A real
  change advances that node's storage subject, which makes it revalidate its registered copies and
  quarantine the ones its new attributes no longer admit, and it serves no governed data until that
  walk finishes. Submitting the values already stored changes nothing.
- Provisioning is idempotent: an existing binding for the same scope and strategy is returned
  unchanged instead of allocating a second one.
- The whole strategy set after the change is returned, so a client never has to re-read to learn
  the new state.
- The change is written to the replicated realm configuration: it is durable here when the response
  is sent and reaches the other realm nodes asynchronously. It moves no data by itself; existing
  replicas are relocated by later placement work.

**Limits**
- The strategy named by `job_family_strategy_id` cannot be removed or have its shard count
  reshaped; those mutations fail with `JobFamilyImmutable`.
- `set_node_attributes` refuses an unknown node, a location longer than 64 bytes, a derived label
  key (`aruna-engine.org/kind`, `.../location`, `.../node`, `.../storage-class/*`), and any edit
  while that node is draining."#,
    request_body(
        content = RealmPlacementRequest,
        description = "Exactly one placement change, discriminated by `mutation`. Ids are ULIDs, node ids are hex-encoded, an override `subject` is a hex-encoded key prefix",
        examples(
            ("Define a strategy" = (
                summary = "Create or replace a strategy that spreads three replicas over distinct locations",
                value = json!({
                    "mutation": "upsert_strategy",
                    "strategy": {
                        "strategy_id": "01JABCDEF0123456789ABCDEFG",
                        "name": "three-replicas-across-sites",
                        "replica_count": 3,
                        "distinct_locations": true,
                        "affinity": [{"key": "zone", "value": "dc-a", "effect": {"kind": "filter"}}],
                        "shard_count": 16
                    }
                })
            )),
            ("Bind a scope" = (
                summary = "Place every metadata document of one group with that strategy",
                value = json!({
                    "mutation": "set_binding",
                    "binding": {
                        "scope": {"kind": "group", "group_id": "01JMETADATA0123456789ABCDE"},
                        "strategy_id": "01JABCDEF0123456789ABCDEFG"
                    }
                })
            )),
            ("Provision a metadata binding" = (
                summary = "Idempotently allocate the realm-wide metadata binding for a strategy",
                value = json!({
                    "mutation": "provision_metadata_binding",
                    "strategy_id": "01JABCDEF0123456789ABCDEFG",
                    "group_id": null
                })
            )),
            ("Edit node attributes" = (
                summary = "Move a node to another location and replace its labels; the node revalidates its copies",
                value = json!({
                    "mutation": "set_node_attributes",
                    "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                    "location": "eu-west",
                    "labels": [{"key": "tier", "value": "hot"}]
                })
            ))
        )
    ),
    responses(
        (
            status = 200,
            description = "The complete strategy set after the change was applied",
            body = RealmPlacementResponse,
            example = json!({
                "strategies": [
                    {
                        "strategy_id": "01JABCDEF0123456789ABCDEFG",
                        "name": "three-replicas-across-sites",
                        "replica_count": 3,
                        "distinct_locations": true,
                        "affinity": [{"key": "zone", "value": "dc-a", "effect": {"kind": "filter"}}],
                        "shard_count": 16
                    }
                ],
                "default_strategy_id": "01JABCDEF0123456789ABCDEFG",
                "job_family_strategy_id": "01JABCDEF0123456789ABCDEFG",
                "bindings": [
                    {"scope": {"kind": "realm"}, "strategy_id": "01JABCDEF0123456789ABCDEFG"}
                ],
                "overrides": [],
                "transitions": {
                    "active": 0,
                    "incomplete_buckets": 0,
                    "stalled_buckets": 0,
                    "overdue": 0
                }
            })
        ),
        (status = 400, description = "Malformed body, an id that is not a ULID, a node id or subject that does not decode, an unknown strategy, a node the realm does not place, a location or label the placement map refuses, or a change the realm configuration rejects as invalid", body = crate::error::ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = crate::error::ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = crate::error::ErrorResponse),
        (status = 409, description = "The strategy is still referenced by a binding or override (`StrategyReferenced`), so removing it would leave a dangling reference, the placement handle space is exhausted, or another update of the realm configuration won the race; the caller may retry", body = crate::error::ErrorResponse),
        (status = 500, description = "The realm configuration could not be read or written here", body = crate::error::ErrorResponse),
        crate::openapi::RelayErrors
    ),
    security(("bearer_auth" = []))
)]
pub async fn mutate_realm_placement(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    request: Result<Json<RealmPlacementRequest>, JsonRejection>,
) -> ServerResult<(StatusCode, Json<RealmPlacementResponse>)> {
    let Json(request) =
        request.map_err(|error| ServerError::BadRequestReason(error.body_text()))?;
    let action = request.into_core()?;
    let actor = actor(&state, &auth);
    let context = state.get_ctx();
    let document = match action {
        RealmPlacementAction::Mutation(mutation) => {
            // Request policies live at this boundary; the operation only checks roles.
            ensure_permission(
                &state,
                &auth,
                policy_admin_path(actor.realm_id),
                Permission::WRITE,
            )
            .await?;
            drive(
                PlacementMutationOperation::new(
                    MutatePlacementConfig { actor, mutation },
                    Some(auth),
                    context.node_and_realm().map(|(node_id, _)| node_id),
                ),
                &context,
            )
            .await
            .map_err(ServerError::from)?
        }
        RealmPlacementAction::Provision {
            strategy_id,
            group_id,
        } => {
            // Handle allocation carries no authorization of its own.
            require_realm_admin(&state, Some(auth)).await?;
            let scope = group_id
                .map(PlacementScope::Group)
                .unwrap_or(PlacementScope::Realm(actor.realm_id));
            drive(
                ProvisionBindingOperation::new(actor.clone(), scope, strategy_id),
                context.as_ref(),
            )
            .await
            .map_err(ServerError::from)?;
            drive(GetConfigOperation::new(actor.realm_id), &context)
                .await
                .map_err(|error| match error {
                    aruna_ops_identity::document_repository::GetConfigError::DocumentNotFound => {
                        ServerError::NotFound
                    }
                    other => ServerError::InternalError(other.into()),
                })?
        }
    };
    Ok((
        StatusCode::OK,
        Json(RealmPlacementResponse::from_document(&document)),
    ))
}

impl From<HandleAllocationError> for ServerError {
    fn from(error: HandleAllocationError) -> Self {
        match error {
            HandleAllocationError::StrategyNotFound(strategy_id) => ServerError::BadRequestReason(
                format!("placement strategy {strategy_id} does not exist"),
            ),
            HandleAllocationError::PlacementHandleExhausted { .. } => {
                ServerError::Conflict("placement handle space is exhausted".to_string())
            }
            HandleAllocationError::Append(error) => ServerError::from(error),
            HandleAllocationError::ReadConfig(
                aruna_ops_identity::document_repository::GetConfigError::DocumentNotFound,
            ) => ServerError::NotFound,
            HandleAllocationError::Storage(StorageError::TransactionConflict) => {
                ServerError::Conflict(
                    "concurrent placement provisioning conflict; retry".to_string(),
                )
            }
            HandleAllocationError::Storage(error) => error.into(),
            other => ServerError::InternalError(other.into()),
        }
    }
}

impl From<MutatePlacementError> for ServerError {
    fn from(error: MutatePlacementError) -> Self {
        match error {
            MutatePlacementError::ConfigMissing => ServerError::NotFound,
            MutatePlacementError::InvalidInput(reason) => ServerError::BadRequestReason(reason),
            error @ (MutatePlacementError::AdminDocumentError(_)
            | MutatePlacementError::EmptyShardHolders { .. }
            | MutatePlacementError::UnknownTransition { .. }
            | MutatePlacementError::ForceWithoutProof { .. }) => {
                ServerError::BadRequestReason(error.to_string())
            }
            MutatePlacementError::Unauthorized { .. } => ServerError::Forbidden,
            MutatePlacementError::StrategyReferenced { strategy_id } => ServerError::Conflict(
                format!("placement strategy {strategy_id} is currently referenced"),
            ),
            MutatePlacementError::JobFamilyImmutable { strategy_id } => ServerError::Conflict(
                format!("placement strategy {strategy_id} is the immutable job-family strategy"),
            ),
            error @ MutatePlacementError::TransitionInFlight { .. } => {
                ServerError::Conflict(error.to_string())
            }
            MutatePlacementError::StorageError(StorageError::TransactionConflict) => {
                ServerError::Conflict(
                    "concurrent realm placement update conflict; retry".to_string(),
                )
            }
            MutatePlacementError::StorageError(error) => error.into(),
            other => ServerError::InternalError(other.into()),
        }
    }
}
