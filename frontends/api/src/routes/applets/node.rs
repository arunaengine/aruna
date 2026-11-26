//! Reports this node's applet hosting and building settings and the library catalogue.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::applet::settings::CataloguePackage;
use axum::Json;
use axum::extract::{Query, State};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::{group_id, realm_feature};
use crate::auth::RealmAuth;
use crate::error::{ErrorResponse, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(applet_settings))
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct SettingsQuery {
    /// A group whose applets feature state to report.
    pub group_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct HostingBody {
    pub applet_domain: String,
    pub custom_domains: bool,
    pub guests: bool,
    pub embedding: bool,
    pub max_origins: usize,
    pub portal_origin: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct PackageBody {
    pub name: String,
    pub version: String,
    pub role: String,
    pub license: String,
    pub license_source: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct BuildingBody {
    pub backend: String,
    pub image: String,
    /// Whether builds may install packages through the operator's registry mirror.
    pub mirror: bool,
    pub max_drafts: usize,
    pub draft_ttl_hours: u64,
    /// Libraries agents should use first. Defaults to steer towards, never an allow-list.
    pub catalogue: Vec<PackageBody>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(example = json!({
    "hosting": {"applet_domain": "apps.example.net", "custom_domains": true, "guests": false,
        "embedding": true, "max_origins": 8, "portal_origin": "https://portal.example.org"},
    "building": {"backend": "k8s-build", "image": "harbor.example/aruna/applet-builder@sha256:0d5c2f4e6a8b0c1d3e5f7a9b1c3d5e7f9a1b3c5d7e9f1a3b5c7d9e1f3a5b7c9d",
        "mirror": false, "max_drafts": 6, "draft_ttl_hours": 24,
        "catalogue": [{"name": "vue", "version": "3.5.42", "role": "framework", "license": "MIT",
            "license_source": "npm:vue@3.5.42"}]},
    "feature_on": true
}))]
pub struct AppletSettingsBody {
    /// Present when this node hosts applets; the portal offers "New applet" then.
    pub hosting: Option<HostingBody>,
    /// Present when this node builds applets; the portal offers the assistant chat then.
    pub building: Option<BuildingBody>,
    /// Whether the applets feature is on for the asked group.
    pub feature_on: Option<bool>,
}

fn package(package: &CataloguePackage) -> PackageBody {
    PackageBody {
        name: package.name.clone(),
        version: package.version.clone(),
        role: package.role.clone(),
        license: package.license.clone(),
        license_source: package.license_source.clone(),
    }
}

#[utoipa::path(
    get,
    path = "/system/applets",
    tag = "applets",
    summary = "Report this node's applet settings",
    description = r#"Reports whether this node hosts and builds applets, with its limits and the library catalogue.

**Authentication**: realm bearer token.

**Behavior**
- Hosting and building are separate: a node may host applets released from static files without
  any build backend.
- The catalogue lists libraries the builder ships; agents should use them first, but any other
  package may still be added and shows on the approval card."#,
    params(SettingsQuery),
    responses(
        (status = 200, description = "The applet settings", body = AppletSettingsBody),
        (status = 400, description = "A malformed group id", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn applet_settings(
    State(state): State<Arc<ServerState>>,
    RealmAuth(_auth): RealmAuth,
    Query(query): Query<SettingsQuery>,
) -> ServerResult<Json<AppletSettingsBody>> {
    let settings = state.applet_settings();
    let feature_on = match query.group_id.as_deref() {
        Some(text) => Some(realm_feature(&state, group_id(text)?).await?),
        None => None,
    };
    Ok(Json(AppletSettingsBody {
        hosting: settings.hosting.as_ref().map(|hosting| HostingBody {
            applet_domain: hosting.applet_domain.clone(),
            custom_domains: hosting.custom_domains,
            guests: hosting.guests,
            embedding: hosting.embedding,
            max_origins: hosting.max_origins,
            portal_origin: hosting.portal_origin.clone(),
        }),
        building: settings.building.as_ref().map(|building| BuildingBody {
            backend: building.backend.clone(),
            image: building.image.clone(),
            mirror: building.mirror.is_some(),
            max_drafts: building.max_drafts,
            draft_ttl_hours: building.draft_ttl_hours,
            catalogue: building.catalogue.iter().map(package).collect(),
        }),
        feature_on,
    }))
}
