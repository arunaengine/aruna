//! Enforces the feature policy: a feature that is off refuses new starts and writes with `403`.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;
use aruna_core::structs::identity::features::{Feature, resolve};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::GroupId;
use aruna_ops_data::metadata::profile::rules::PROFILE_DOCUMENT_PREFIX;
use aruna_ops_data::s3::object::member::rest::API_PREFIX;
use aruna_ops_identity::document_repository::GetConfigOperation;
use aruna_runner::driver::drive;
use axum::extract::{MatchedPath, Request, State};
use axum::http::Method;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use std::sync::Arc;
use ulid::Ulid;

/// Requests a feature gates: the method, or `None` for every POST, PUT and PATCH, then a route
/// template or the prefix of its templates. DELETE stays open, so owners can always clean up.
pub(crate) const GATES: &[(Option<&str>, &str, Feature)] = &[
    (None, "/access/users/me/vault", Feature::Vault),
    (Some("GET"), "/access/users/me/vault", Feature::Vault),
    (None, "/access/users/me/secrets", Feature::Vault),
    (
        Some("GET"),
        "/access/users/me/secrets/{name}/value",
        Feature::Vault,
    ),
    (None, "/data/groups/{group_id}/secrets", Feature::Vault),
    (None, "/system/realm/secrets", Feature::Vault),
    (None, "/system/node/secrets", Feature::Vault),
    (None, "/system/assistant", Feature::Assistant),
    (
        Some("GET"),
        "/system/assistant/providers/{id}/proxy/{*path}",
        Feature::Assistant,
    ),
    (None, "/access/users/me/assistant", Feature::Assistant),
    (None, "/plugins", Feature::Plugins),
    (None, "/plugin-installs", Feature::Plugins),
    (None, "/plugin-hooks", Feature::Plugins),
    (
        None,
        "/data/groups/{group_id}/connectors",
        Feature::Connectors,
    ),
    (
        None,
        "/access/groups/{id}/service-accounts/{account_id}/credentials",
        Feature::S3Keys,
    ),
    (None, "/system/notifications/watches", Feature::Watches),
    (
        Some("PUT"),
        "/data/buckets/{bucket}/placement",
        Feature::Placement,
    ),
    (Some("PATCH"), "/system/realm/placement", Feature::Placement),
    (
        Some("PUT"),
        "/access/groups/{id}/storage-location",
        Feature::Placement,
    ),
    (
        Some("PUT"),
        "/metadata/{document_id}/storage-location",
        Feature::Placement,
    ),
    (
        None,
        "/metadata/groups/{group_id}/repositories",
        Feature::Repositories,
    ),
    (None, "/metadata/repository/imports", Feature::Repositories),
    (
        None,
        "/metadata/{document_id}/repository",
        Feature::Repositories,
    ),
    (None, "/federation", Feature::Federation),
    (
        None,
        "/metadata/{document_id}/profile/validation/revalidate",
        Feature::Profiles,
    ),
    (
        None,
        "/metadata/profile/validation/preview",
        Feature::Profiles,
    ),
];

/// Refuses the call when `feature` is off on this node or for `group`, at realm level without
/// one. A realm config that cannot be read refuses it too.
pub(crate) async fn require(
    state: &ServerState,
    feature: Feature,
    group: Option<GroupId>,
) -> ServerResult<()> {
    let config = realm_config(state).await?;
    let resolved = resolve(&config, &state.disabled_features(), feature, group.as_ref());
    match resolved.on {
        true => Ok(()),
        false => Err(ServerError::FeatureOff(feature, resolved)),
    }
}

async fn realm_config(state: &ServerState) -> ServerResult<RealmConfigDocument> {
    let read = GetConfigOperation::new(state.get_realm_id());
    drive(read, &state.get_ctx()).await.map_err(|_| {
        ServerError::ServiceUnavailableReason("the realm feature policy cannot be read".to_string())
    })
}

/// Runs the gates of [`GATES`] and, for writes to a document under `profiles/`, the `profiles`
/// gate. The group comes from the path: a group id, a bucket or a document.
pub(crate) async fn gate_middleware(
    State(state): State<Arc<ServerState>>,
    request: Request,
    next: Next,
) -> Response {
    let Some(matched) = request.extensions().get::<MatchedPath>() else {
        return next.run(request).await;
    };
    let template = matched.as_str();
    let template = template.strip_prefix(API_PREFIX).unwrap_or(template);
    let method = request.method();
    let write = [Method::POST, Method::PUT, Method::PATCH].contains(method);
    let mut features = gates(method, template);
    let document = write && template.starts_with("/metadata/{document_id}");
    if features.is_empty() && !document {
        return next.run(request).await;
    }
    let path = request.uri().path();
    let path = path.strip_prefix(API_PREFIX).unwrap_or(path);
    let (group, profile) = match path_group(&state, template, path).await {
        Ok(resolved) => resolved,
        Err(error) => return error.into_response(),
    };
    if profile {
        features.push(Feature::Profiles);
    }
    for feature in features {
        if let Err(error) = require(&state, feature, group).await {
            return error.into_response();
        }
    }
    next.run(request).await
}

/// The features of [`GATES`] that gate `method` on the route `template`.
pub(crate) fn gates(method: &Method, template: &str) -> Vec<Feature> {
    let write = [Method::POST, Method::PUT, Method::PATCH].contains(method);
    (GATES.iter())
        .filter(|(gate, prefix, _)| {
            let applies = gate.map_or(write, |gate| gate == method.as_str());
            let under = template.strip_prefix(prefix);
            applies && under.is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
        })
        .map(|(_, _, feature)| *feature)
        .collect()
}

/// Resolves the decoded group path parameter and whether it names a `profiles/` document,
/// rejecting malformed ids or lookup failures rather than falling back to realm defaults.
async fn path_group(
    state: &ServerState,
    template: &str,
    path: &str,
) -> ServerResult<(Option<GroupId>, bool)> {
    let pairs = template.split('/').zip(path.split('/'));
    let mut previous = "";
    for (name, raw) in pairs {
        match (previous, name) {
            (_, "{group_id}") | ("groups", "{id}") => {
                let value = decode_segment(raw)?;
                let id = Ulid::from_string(&value)
                    .map_err(|_| ServerError::BadRequestReason("group_id is not a ULID".into()))?;
                return Ok((Some(id), false));
            }
            (_, "{bucket}") => {
                let value = decode_segment(raw)?;
                let group = crate::routes::access::groups::get_bucket_group(state, &value).await?;
                return Ok((group, false));
            }
            (_, "{document_id}") => {
                let value = decode_segment(raw)?;
                let id = Ulid::from_string(&value).map_err(|_| {
                    ServerError::BadRequestReason("document_id is not a ULID".into())
                })?;
                return document_group(state, id).await;
            }
            _ => previous = name,
        }
    }
    Ok((None, false))
}

fn decode_segment(raw: &str) -> ServerResult<String> {
    percent_encoding::percent_decode_str(raw)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| ServerError::BadRequestReason("malformed path parameter".into()))
}

async fn document_group(
    state: &ServerState,
    document_id: Ulid,
) -> ServerResult<(Option<GroupId>, bool)> {
    match crate::metadata::load_document_record(state, document_id).await {
        Ok(record) => {
            let profile = record.document_path.starts_with(PROFILE_DOCUMENT_PREFIX);
            Ok((Some(record.group_id), profile))
        }
        Err(ServerError::NotFound) => Ok((None, false)),
        Err(error) => Err(error),
    }
}

/// Refuses a write to a document under `profiles/` while `profiles` is off for its group.
pub(crate) async fn profile_write(state: &ServerState, document_id: Ulid) -> ServerResult<()> {
    match document_group(state, document_id).await? {
        (group, true) => require(state, Feature::Profiles, group).await,
        (_, false) => Ok(()),
    }
}

/// Refuses creating a document under `profiles/` while `profiles` is off for `group`.
pub(crate) async fn profile_create(
    state: &ServerState,
    group: GroupId,
    path: &str,
) -> ServerResult<()> {
    match path.starts_with(PROFILE_DOCUMENT_PREFIX) {
        true => require(state, Feature::Profiles, Some(group)).await,
        false => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::realms::admin_state;
    use aruna_core::structs::identity::features::FeatureMode;
    use axum::body::to_bytes;
    use axum::http::StatusCode;

    #[test]
    fn gates_writes_only() {
        let vault = vec![Feature::Vault];
        let secrets = "/data/groups/{group_id}/secrets/{name}";
        assert_eq!(gates(&Method::PUT, secrets), vault);
        assert!(gates(&Method::GET, secrets).is_empty());
        assert!(gates(&Method::DELETE, secrets).is_empty());
        let value = "/access/users/me/secrets/{name}/value";
        assert_eq!(gates(&Method::GET, value), vault);
        assert!(gates(&Method::GET, "/access/users/me/secrets").is_empty());
        // A prefix only covers whole segments.
        assert!(gates(&Method::POST, "/pluginsx").is_empty());
        // Short-lived S3 sessions power the portal and are never gated.
        for method in [Method::POST, Method::PUT, Method::GET] {
            assert!(gates(&method, "/access/s3/sessions").is_empty());
            assert!(gates(&method, "/access/s3/sessions/{access_key_id}/refresh").is_empty());
        }
    }

    #[test]
    fn gates_name_routes() {
        // Every gate names a documented route, so a renamed route cannot drop its gate.
        let documented: Vec<String> =
            (crate::routes::rest_openapi().paths.paths.into_keys()).collect();
        for (_, prefix, feature) in GATES {
            let prefix = prefix.replace("{*path}", "{path}");
            let named = (documented.iter())
                .any(|path| path == &prefix || path.starts_with(&format!("{prefix}/")));
            assert!(named, "gate {prefix} of {feature:?} names no route");
        }
    }

    #[tokio::test]
    async fn refusal_names_feature() {
        let (_dir, state, _admin) = admin_state().await;
        let group = Ulid::generate();
        require(&state, Feature::Shares, Some(group)).await.unwrap();

        let mut config = realm_config(&state).await.unwrap();
        config
            .features
            .insert(Feature::Shares, FeatureMode::ForcedOff);
        let resolved = resolve(&config, &state.disabled_features(), Feature::Shares, None);
        let response = ServerError::FeatureOff(Feature::Shares, resolved).into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"], "feature_disabled");
        assert_eq!(body["feature"], "shares");
        assert_eq!(body["scope"], "realm");
        assert!(body["message"].as_str().unwrap().contains("shares"));

        // The node's own off list refuses without any realm setting.
        let state = (*state)
            .clone()
            .with_disabled_features([Feature::Shares].into());
        let error = require(&state, Feature::Shares, Some(group))
            .await
            .unwrap_err();
        assert!(matches!(error, ServerError::FeatureOff(Feature::Shares, _)));
        // No node hosts applets yet.
        let error = require(&state, Feature::Applets, None).await.unwrap_err();
        assert!(matches!(
            error,
            ServerError::FeatureOff(Feature::Applets, _)
        ));
    }
}
