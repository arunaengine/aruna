//! Serves applets: records and their lifecycle, and the settings of their hosting node.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod builds;
pub mod domains;
pub mod edge;
pub mod lifecycle;
pub mod node;
pub mod notices;
pub mod preview;
pub mod records;
pub mod review;
pub mod runtime;
pub mod signin;
pub mod source;
pub mod wire;

use std::sync::Arc;

use aruna_core::applet::record::{Applet, Serving};
use aruna_core::applet::settings::HostingSettings;
use aruna_core::structs::identity::features::{Feature, resolve};
use aruna_core::structs::identity::realm::RealmConfigDocument;
use aruna_core::types::GroupId;
use aruna_ops_identity::document_repository::GetConfigOperation;
use aruna_ops_work::applets::AppletError;
use aruna_runner::driver::drive;
use ulid::Ulid;
use utoipa_axum::router::OpenApiRouter;

use crate::error::{ServerError, ServerResult};
use crate::server::state::ServerState;

/// Items of a page when the caller names no limit.
const DEFAULT_LIMIT: usize = 50;
/// The most items one page holds.
const MAX_LIMIT: usize = 500;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    (records::router())
        .merge(lifecycle::router())
        .merge(review::router())
        .merge(source::router())
        .merge(builds::router())
        .merge(domains::router())
        .merge(edge::router())
        .merge(preview::router())
        .merge(notices::router())
        .merge(runtime::router())
        .merge(signin::router())
        .merge(node::router())
}

pub(crate) fn applet_error(error: AppletError) -> ServerError {
    match error {
        AppletError::NotFound => ServerError::NotFound,
        AppletError::Forbidden => ServerError::Forbidden,
        AppletError::Invalid(reason) => ServerError::BadRequestReason(reason),
        AppletError::Conflict(reason) => ServerError::Conflict(reason),
        AppletError::Limited(reason) => ServerError::Refused(
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            "applet_limited",
            reason,
        ),
        AppletError::Elsewhere(node) => ServerError::Conflict(format!(
            "the applet is hosted on node {node}; send changes there"
        )),
        AppletError::Storage(reason) => ServerError::InternalError(reason.into()),
    }
}

pub(crate) fn applet_id(text: &str) -> ServerResult<Ulid> {
    Ulid::from_string(text).map_err(|_| ServerError::NotFound)
}

pub(crate) fn group_id(text: &str) -> ServerResult<GroupId> {
    Ulid::from_string(text)
        .map_err(|_| ServerError::BadRequestReason("group_id is not a ULID".to_string()))
}

/// The configured hosting settings of this node; a node that hosts no applets refuses the call.
pub(crate) fn hosting(state: &ServerState) -> ServerResult<&HostingSettings> {
    (state.applet_settings().hosting.as_ref()).ok_or_else(|| {
        ServerError::ServiceUnavailableReason("this node hosts no applets".to_string())
    })
}

/// Effective hosting settings against `config`, disabled if any realm OIDC provider shares a site.
pub(crate) fn hosting_for<'a>(
    state: &'a ServerState,
    config: &RealmConfigDocument,
) -> Option<&'a HostingSettings> {
    let hosting = state.applet_settings().hosting.as_ref()?;
    match hosting.provider_conflict(&config.oidc_providers) {
        Some(host) => {
            tracing::warn!(
                applet_domain = %hosting.applet_domain,
                oidc_host = %host,
                "applet hosting disabled: domain shares a registered domain with a realm OIDC provider"
            );
            None
        }
        None => Some(hosting),
    }
}

/// Effective hosting settings of this node after checking the current realm OIDC providers.
pub(crate) async fn effective_hosting(state: &ServerState) -> ServerResult<&HostingSettings> {
    let hosting = hosting(state)?;
    let config = realm_config(state).await?;
    if let Some(host) = hosting.provider_conflict(&config.oidc_providers) {
        return Err(ServerError::ServiceUnavailableReason(format!(
            "applet hosting is disabled because {} shares a registered domain with OIDC provider {host}",
            hosting.applet_domain
        )));
    }
    Ok(hosting)
}

pub(super) async fn realm_config(state: &ServerState) -> ServerResult<RealmConfigDocument> {
    drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|error| ServerError::ServiceUnavailableReason(error.to_string()))
}

/// Whether the applets feature is on for `group_id` on this node.
pub(crate) async fn realm_feature(state: &ServerState, group_id: GroupId) -> ServerResult<bool> {
    let config = realm_config(state).await?;
    Ok(resolve(
        &config,
        &state.disabled_features(),
        Feature::Applets,
        Some(&group_id),
    )
    .on)
}

/// Refuses work on applets of `group_id` while the applets feature is off for it.
pub(crate) async fn require_feature(state: &ServerState, group_id: GroupId) -> ServerResult<()> {
    crate::features::require(state, Feature::Applets, Some(group_id)).await
}

/// Normalized hosts of this node's portal, API, S3 and realm OIDC providers.
pub(crate) async fn protected_hosts(state: &ServerState) -> Vec<String> {
    let mut hosts = state
        .applet_settings()
        .hosting
        .as_ref()
        .map(|hosting| hosting.protected_hosts.clone())
        .unwrap_or_default();
    if let Ok(config) = realm_config(state).await {
        for provider in &config.oidc_providers {
            for raw in [&provider.issuer, &provider.discovery_url] {
                let host = url::Url::parse(raw.trim())
                    .ok()
                    .and_then(|url| url.host_str().map(str::to_string))
                    .and_then(|host| aruna_core::applet::hosts::normalize_host(&host).ok());
                hosts.extend(host);
            }
        }
    }
    hosts.sort();
    hosts.dedup();
    hosts
}

/// What the edge serves for `applet` now, from the feature state and this node's effective hosting.
pub(crate) async fn serving(state: &ServerState, applet: &Applet) -> ServerResult<Serving> {
    let config = realm_config(state).await?;
    let feature = resolve(
        &config,
        &state.disabled_features(),
        Feature::Applets,
        Some(&applet.group_id),
    )
    .on;
    Ok(applet.serving(feature, hosting_for(state, &config).is_some()))
}

fn limit(asked: Option<usize>) -> usize {
    asked.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::UnrestrictedAuth;
    use crate::tests::realms::admin_state;
    use aruna_core::applet::record::{Access, Applet, Embedding, Grants, Lifecycle};
    use aruna_core::applet::settings::{AppletSettings, DEFAULT_ORIGINS, HostingSettings};
    use aruna_core::effects::StorageEffect;
    use aruna_core::keyspaces::REALM_CONFIG_KEYSPACE;
    use aruna_core::structs::identity::auth::Actor;
    use aruna_core::structs::identity::features::FeatureMode;
    use aruna_core::structs::identity::realm::OidcProviderConfig;
    use aruna_ops_work::applets::create::{CreateAppletOperation, Hosting, NewApplet};
    use axum::Json;
    use axum::extract::{Path, Query, State};
    use axum::http::{HeaderMap, HeaderValue};

    /// SHA-256 of `"edge-secret"`.
    const EDGE_SECRET_SHA256: [u8; 32] = [
        11, 134, 54, 23, 211, 15, 2, 235, 44, 200, 226, 242, 103, 135, 189, 243, 74, 25, 160, 107,
        122, 183, 44, 125, 167, 33, 133, 237, 115, 79, 206, 19,
    ];

    fn sample_hosting() -> HostingSettings {
        HostingSettings {
            applet_domain: "apps.example.net".into(),
            edge_key_sha256: EDGE_SECRET_SHA256,
            custom_domains: true,
            guests: false,
            embedding: false,
            max_origins: DEFAULT_ORIGINS,
            portal_origin: Some("https://portal.example.org".into()),
            protected_hosts: Vec::new(),
        }
    }

    fn applets_on() -> FeatureMode {
        FeatureMode::Scoped {
            default: true,
            groups: Default::default(),
        }
    }

    async fn save_realm(
        state: &ServerState,
        user_id: aruna_core::user::id::UserId,
        cfg: &RealmConfigDocument,
    ) {
        let actor = Actor {
            node_id: state.get_node_id(),
            user_id,
            realm_id: state.get_realm_id(),
        };
        state
            .get_ctx()
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: REALM_CONFIG_KEYSPACE.to_string(),
                key: state.get_realm_id().as_bytes().to_vec().into(),
                value: cfg.to_bytes(&actor).unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    #[tokio::test]
    async fn disables_conflicting_hosting() {
        let (_dir, base, admin) = admin_state().await;
        let state = Arc::new((*base).clone().with_applets(AppletSettings {
            hosting: Some(sample_hosting()),
            building: None,
        }));
        let group = Ulid::generate();
        let mut config = realm_config(&state).await.unwrap();
        config.features.insert(Feature::Applets, applets_on());
        save_realm(&state, admin, &config).await;

        assert!(effective_hosting(&state).await.is_ok());
        let applet = Applet {
            applet_id: Ulid::generate(),
            group_id: group,
            name: "Bakta".into(),
            slug: "bakta".into(),
            host: "bakta.apps.example.net".into(),
            repository: None,
            host_node: state.get_node_id(),
            lifecycle: Lifecycle::Live,
            access: Access::Public,
            live: Some(1),
            versions: Vec::new(),
            drafts: Vec::new(),
            grants: Grants::default(),
            embedding: Embedding::default(),
            guests: None,
            domains: Vec::new(),
            pending: None,
            account: None,
            role_id: None,
            visitor_salt: [0; 32],
            created_by: admin,
            created_at_ms: 0,
            revision: 1,
        };
        assert_eq!(
            serving(&state, &applet).await.unwrap(),
            Serving::Live { version: 1 }
        );

        let mut headers = HeaderMap::new();
        headers.insert("x-aruna-edge-key", HeaderValue::from_static("edge-secret"));
        let first = notices::notices(
            State(state.clone()),
            headers.clone(),
            Query(notices::NoticeQuery {
                after: None,
                wait_s: Some(0),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(first.reset);

        // Adding a realm OIDC provider on the same site disables hosting and resets edge caches.
        config.oidc_providers.push(OidcProviderConfig {
            id: "idp".into(),
            issuer: "https://auth.example.net/realms/main".into(),
            audience: "client".into(),
            discovery_url: "https://auth.example.net/.well-known/openid-configuration".into(),
        });
        save_realm(&state, admin, &config).await;

        assert!(hosting_for(&state, &config).is_none());
        assert!(matches!(
            effective_hosting(&state).await,
            Err(ServerError::ServiceUnavailableReason(_))
        ));
        assert_eq!(serving(&state, &applet).await.unwrap(), Serving::Gone);

        let flipped = notices::notices(
            State(state.clone()),
            headers,
            Query(notices::NoticeQuery {
                after: Some(first.cursor),
                wait_s: Some(0),
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(flipped.reset, "policy flip must reset edge decision cache");
    }

    #[tokio::test]
    async fn gates_commit_source() {
        let (_dir, base, admin) = admin_state().await;
        let state = Arc::new((*base).clone().with_applets(AppletSettings {
            hosting: Some(sample_hosting()),
            building: None,
        }));
        let group = Ulid::generate();
        let auth = aruna_runner::testing::seed_access(
            &state.get_ctx(),
            (state.get_realm_id(), group, state.get_node_id()),
            "/denied",
        )
        .await;

        // Create an applet while Applets is enabled, then disable Applets and attempt a commit.
        let mut config = realm_config(&state).await.unwrap();
        config.features.insert(Feature::Applets, applets_on());
        save_realm(&state, admin, &config).await;
        let created = drive(
            CreateAppletOperation::new(
                &auth,
                NewApplet {
                    group_id: group,
                    name: "Bakta".into(),
                    slug: "bakta".into(),
                    repository: None,
                },
                Hosting {
                    node: state.get_node_id(),
                    applet_domain: "apps.example.net".into(),
                    visitor_salt: [1; 32],
                },
            ),
            &state.get_ctx(),
        )
        .await
        .unwrap();

        config
            .features
            .insert(Feature::Applets, FeatureMode::ForcedOff);
        save_realm(&state, admin, &config).await;
        let err = source::commit_source(
            State(state),
            UnrestrictedAuth(auth),
            Path(created.applet_id.to_string()),
            Json(source::CommitRequest {
                base: "deadbeef".into(),
                message: "edit".into(),
                files: Default::default(),
            }),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, ServerError::FeatureOff(Feature::Applets, _)),
            "expected FeatureOff for Applets, got {err:?}"
        );
    }
}
