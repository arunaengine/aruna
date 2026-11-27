//! Serves the realm feature policy and the features in effect for a caller's node and group.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use aruna_core::structs::identity::auth::Permission;
use aruna_core::structs::identity::features::{Feature, FeatureMode, resolve};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_core::structs::storage::blob::group_permission_path;
use aruna_ops_identity::document_repository::{GetConfigError, GetConfigOperation};
use aruna_ops_identity::realm::set_portal::{SetPortalConfig, SetPortalOperation};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Query, State};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{RealmAuth, ensure_permission};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::lookup::actor;
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .routes(routes!(get_features, set_features))
        .routes(routes!(effective_features))
}

/// The realm feature policy by feature key; a missing key uses its built-in default.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FeaturePolicy {
    pub features: BTreeMap<String, FeatureSetting>,
}

/// How the realm sets one feature.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum FeatureSetting {
    /// The realm default with per-group exceptions, keyed by group id.
    Scoped {
        default: bool,
        #[serde(default)]
        groups: BTreeMap<String, bool>,
    },
    /// Off in the whole realm; group exceptions do not apply.
    ForcedOff,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct EffectiveQuery {
    /// Resolve group exceptions for this group, as a 26-character ULID.
    pub group_id: Option<String>,
}

/// Every feature's state on this node, for the named group or at realm level.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EffectiveFeatures {
    pub node_id: String,
    pub group_id: Option<String>,
    pub features: BTreeMap<String, FeatureState>,
}

/// One feature's state and the layer that decided it.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FeatureState {
    /// `on`, `off`, or `unknown` while the realm config cannot be read.
    pub state: String,
    /// `node`, `realm`, `group`, `default`, `dependency` or `unreadable`.
    pub reason: String,
    /// The feature this one needs, when that one is off.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dependency: Option<String>,
}

#[utoipa::path(
    get,
    path = "/system/realm/features",
    tag = "system/realm",
    summary = "Read the realm feature policy",
    description = r#"Returns the stored realm feature policy with every group exception.

**Authentication**: realm bearer token with READ on the realm configuration admin path.

**Behavior**
- A feature missing from the map uses its built-in default: on, except `applets` and `automations`.
- Feature keys: `runs`, `workflows`, `notebooks`, `applets`, `vault`, `assistant`, `plugins`,
  `connectors`, `shares`, `placement`, `profiles`, `repositories`, `s3-keys`, `watches`,
  `federation`, `automations`. `workflows` and `notebooks` need `runs`; `automations` needs
  `workflows`."#,
    responses(
        (status = 200, description = "The stored feature policy", body = FeaturePolicy,
            example = json!({"features": {"runs": {"mode": "scoped", "default": true,
                "groups": {"01JABCDEF0123456789ABCDEFG": false}}, "shares": {"mode": "forced_off"}}})),
        (status = 403, description = "Caller is not a realm config admin", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn get_features(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
) -> ServerResult<Json<FeaturePolicy>> {
    let admin = policy_admin_path(state.get_realm_id());
    ensure_permission(&state, &auth, admin, Permission::READ).await?;
    let config = drive(GetConfigOperation::new(auth.realm_id), &state.get_ctx())
        .await
        .map_err(config_error)?;
    Ok(Json(policy_body(&config.features)))
}

#[utoipa::path(
    put,
    path = "/system/realm/features",
    tag = "system/realm",
    summary = "Replace the realm feature policy",
    description = r#"Replaces the realm feature policy.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- The body replaces the stored map. A feature absent from it uses its built-in default.
- `forced_off` turns a feature off in the whole realm and ignores its group exceptions.
- Turning a feature off deletes nothing: running work finishes and stored data stays readable,
  while new starts and writes are refused with `403 feature_disabled`."#,
    request_body(
        content = FeaturePolicy,
        description = "The complete feature policy to store",
        example = json!({"features": {"runs": {"mode": "scoped", "default": true,
                "groups": {"01JABCDEF0123456789ABCDEFG": false}}, "shares": {"mode": "forced_off"}}})
    ),
    responses(
        (status = 200, description = "The stored feature policy", body = FeaturePolicy,
            example = json!({"features": {"runs": {"mode": "scoped", "default": true,
                "groups": {"01JABCDEF0123456789ABCDEFG": false}}, "shares": {"mode": "forced_off"}}})),
        (status = 400, description = "An unknown feature key or a malformed group id", body = ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse),
        (status = 409, description = "Another update of the realm configuration won the race; retry", body = ErrorResponse),
        crate::openapi::RelayErrors
    ),
    security(("bearer_auth" = []))
)]
pub async fn set_features(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Json(request): Json<FeaturePolicy>,
) -> ServerResult<Json<FeaturePolicy>> {
    let admin = policy_admin_path(state.get_realm_id());
    ensure_permission(&state, &auth, admin, Permission::WRITE).await?;
    let policy = policy_modes(request)?;
    let stored = drive(
        SetPortalOperation::new(SetPortalConfig {
            actor: actor(&state, &auth),
            auth_context: auth,
            features: None,
            policy: Some(policy),
        }),
        &state.get_ctx(),
    )
    .await
    .map_err(ServerError::from)?;
    Ok(Json(policy_body(&stored.features)))
}

#[utoipa::path(
    get,
    path = "/system/realm/features/effective",
    tag = "system/realm",
    summary = "Read the features in effect",
    description = r#"Returns the state of every feature on this node, for one group or at realm level.

**Authentication**: any realm bearer token. With `group_id`, READ on that group or on the realm
configuration admin path.

**Behavior**
- `state` is `on` or `off`, and `unknown` with reason `unreadable` while this node cannot read
  the realm configuration. A node-level `off` stays known.
- `reason` names the deciding layer: `node`, `realm`, `group`, `default` or `dependency`, which
  also names the needed feature in `dependency`.
- The user attribute `ui.feature.<key>` only hides a feature in the portal; it never turns one on."#,
    params(EffectiveQuery),
    responses(
        (status = 200, description = "Every feature with its state", body = EffectiveFeatures,
            example = json!({"node_id": "5a0b...", "group_id": "01JABCDEF0123456789ABCDEFG",
                "features": {"runs": {"state": "on", "reason": "default"},
                    "workflows": {"state": "off", "reason": "dependency", "dependency": "runs"}}})),
        (status = 400, description = "A malformed group id", body = ErrorResponse),
        (status = 403, description = "The caller cannot read the named group", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn effective_features(
    State(state): State<Arc<ServerState>>,
    RealmAuth(auth): RealmAuth,
    Query(query): Query<EffectiveQuery>,
) -> ServerResult<Json<EffectiveFeatures>> {
    let group = (query.group_id.as_deref())
        .map(|group| Ulid::from_string(group).map_err(|_| ServerError::BadRequest))
        .transpose()?;
    if let Some(group) = group {
        let (realm, node) = (state.get_realm_id(), state.get_node_id());
        let member = group_permission_path(realm, group, node);
        if ensure_permission(&state, &auth, member, Permission::READ)
            .await
            .is_err()
        {
            let admin = policy_admin_path(realm);
            ensure_permission(&state, &auth, admin, Permission::READ).await?;
        }
    }
    let off = state.disabled_features();
    let config = drive(GetConfigOperation::new(auth.realm_id), &state.get_ctx())
        .await
        .ok();
    let features = (Feature::ALL.into_iter())
        .map(|feature| {
            let state = match &config {
                Some(config) => known(resolve(config, &off, feature, group.as_ref())),
                None => node_only(&off, feature),
            };
            (feature.name().to_string(), state)
        })
        .collect();
    Ok(Json(EffectiveFeatures {
        node_id: state.get_node_id().to_string(),
        group_id: group.map(|group| group.to_string()),
        features,
    }))
}

fn known(resolved: aruna_core::structs::identity::features::Resolved) -> FeatureState {
    FeatureState {
        state: if resolved.on { "on" } else { "off" }.to_string(),
        reason: resolved.scope.name().to_string(),
        dependency: resolved.dependency.map(|parent| parent.name().to_string()),
    }
}

/// Without the realm config only this node's own off list is certain.
fn node_only(off: &std::collections::BTreeSet<Feature>, feature: Feature) -> FeatureState {
    let parent = feature.parent().filter(|parent| off.contains(parent));
    match (off.contains(&feature), parent) {
        (true, _) => FeatureState {
            state: "off".to_string(),
            reason: "node".to_string(),
            dependency: None,
        },
        (false, Some(parent)) => FeatureState {
            state: "off".to_string(),
            reason: "dependency".to_string(),
            dependency: Some(parent.name().to_string()),
        },
        (false, None) => FeatureState {
            state: "unknown".to_string(),
            reason: "unreadable".to_string(),
            dependency: None,
        },
    }
}

fn config_error(error: GetConfigError) -> ServerError {
    match error {
        GetConfigError::DocumentNotFound => ServerError::NotFound,
        other => ServerError::InternalError(other.into()),
    }
}

fn policy_body(features: &BTreeMap<Feature, FeatureMode>) -> FeaturePolicy {
    let features = (features.iter())
        .map(|(feature, mode)| {
            let setting = match mode {
                FeatureMode::Scoped { default, groups } => FeatureSetting::Scoped {
                    default: *default,
                    groups: (groups.iter())
                        .map(|(group, on)| (group.to_string(), *on))
                        .collect(),
                },
                FeatureMode::ForcedOff => FeatureSetting::ForcedOff,
            };
            (feature.name().to_string(), setting)
        })
        .collect();
    FeaturePolicy { features }
}

fn policy_modes(body: FeaturePolicy) -> ServerResult<BTreeMap<Feature, FeatureMode>> {
    let mut modes = BTreeMap::new();
    for (name, setting) in body.features {
        let feature = Feature::from_name(&name)
            .ok_or_else(|| ServerError::BadRequestReason(format!("unknown feature `{name}`")))?;
        let mode = match setting {
            FeatureSetting::Scoped { default, groups } => {
                let groups = (groups.into_iter())
                    .map(|(group, on)| match Ulid::from_string(&group) {
                        Ok(group) => Ok((group, on)),
                        Err(_) => Err(ServerError::BadRequestReason(format!(
                            "group id `{group}` is not a ULID"
                        ))),
                    })
                    .collect::<ServerResult<_>>()?;
                FeatureMode::Scoped { default, groups }
            }
            FeatureSetting::ForcedOff => FeatureMode::ForcedOff,
        };
        modes.insert(feature, mode);
    }
    Ok(modes)
}

#[cfg(test)]
#[path = "features_tests.rs"]
mod tests;
