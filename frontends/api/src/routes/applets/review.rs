//! Shows approvers what approving a version changes compared with the version that serves now.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::applet::diff::{ApprovalDiff, approval_diff};
use aruna_core::applet::manifest::Package;
use aruna_core::applet::record::{Applet, ApprovalRequest, VersionState};
use aruna_core::applet::settings::{CataloguePackage, default_catalogue};
use aruna_ops_work::applets::backends::SandboxOperation;
use aruna_ops_work::applets::{AppletAccessOperation, Need};
use aruna_runner::driver::drive;
use axum::Json;
use axum::extract::{Path, State};
use serde::Serialize;
use utoipa::ToSchema;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::edge::read_manifest;
use super::wire::{AccessBody, GrantsBody};
use super::{applet_error, applet_id};
use crate::auth::UnrestrictedAuth;
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new().routes(routes!(version_diff))
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletDiffPackage)]
pub struct PackageBody {
    pub name: String,
    pub version: String,
    /// The version the builder's catalogue pins, when the package is in it.
    pub catalogue_version: Option<String>,
    /// The license checked for the catalogue entry; unknown outside the catalogue.
    pub license: Option<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletDiffPackages)]
pub struct PackagesBody {
    pub added: Vec<PackageBody>,
    pub removed: Vec<PackageBody>,
    /// Each with its new version.
    pub changed: Vec<PackageBody>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletDiffFiles)]
pub struct FilesBody {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
}

#[derive(Clone, Debug, Serialize, ToSchema)]
#[schema(as = AppletDiff, example = json!({
    "version": 3, "base": 2, "live": 2, "pending": true,
    "commit_before": "4b825dc642cb6eb9a060e54bf8d69288fbee4904",
    "commit_after": "9fceb02d0ae598e95dc970b74767f19372d61af8",
    "access_before": "group_members", "access_after": "public",
    "files": {"added": ["assets/plot.js"], "removed": [], "changed": ["index.html"]},
    "packages": {"added": [{"name": "echarts", "version": "6.1.0",
        "catalogue_version": "6.1.0", "license": "Apache-2.0"}], "removed": [], "changed": []},
    "grants_before": {"read": [], "references": [], "start": null, "submissions": null,
        "limits": {}},
    "grants_after": {"read": [], "references": [], "start": null, "submissions": null,
        "limits": {}},
    "origins_added": [], "domains": ["bakta.lab.org"], "unsandboxed": false
}))]
pub struct DiffBody {
    pub version: u32,
    /// The version compared against: the live one, else the newest earlier approved one.
    pub base: Option<u32>,
    pub live: Option<u32>,
    /// Whether the grants and access shown are a pending request for this version.
    pub pending: bool,
    pub commit_before: Option<String>,
    pub commit_after: Option<String>,
    pub access_before: AccessBody,
    pub access_after: AccessBody,
    pub files: FilesBody,
    pub packages: PackagesBody,
    pub grants_before: GrantsBody,
    pub grants_after: GrantsBody,
    pub origins_added: Vec<String>,
    pub domains: Vec<String>,
    /// Work the applet starts may run on a backend without a kernel sandbox.
    pub unsandboxed: bool,
}

fn packages(list: &[Package], catalogue: &[CataloguePackage]) -> Vec<PackageBody> {
    (list.iter())
        .map(|package| {
            let entry = catalogue.iter().find(|entry| entry.name == package.name);
            PackageBody {
                name: package.name.clone(),
                version: package.version.clone(),
                catalogue_version: entry.map(|entry| entry.version.clone()),
                license: entry.map(|entry| entry.license.clone()),
            }
        })
        .collect()
}

fn diff_body(
    diff: ApprovalDiff,
    base: Option<u32>,
    pending: bool,
    catalogue: &[CataloguePackage],
) -> DiffBody {
    DiffBody {
        version: diff.version,
        base,
        live: diff.live,
        pending,
        commit_before: diff.commits.0,
        commit_after: diff.commits.1,
        access_before: diff.access.0.into(),
        access_after: diff.access.1.into(),
        files: FilesBody {
            added: diff.files.added,
            removed: diff.files.removed,
            changed: diff.files.changed,
        },
        packages: PackagesBody {
            added: packages(&diff.packages.added, catalogue),
            removed: packages(&diff.packages.removed, catalogue),
            changed: packages(&diff.packages.changed, catalogue),
        },
        grants_before: GrantsBody::from(&diff.grants.0),
        grants_after: GrantsBody::from(&diff.grants.1),
        origins_added: diff.origins_added,
        domains: diff.domains,
        unsandboxed: diff.unsandboxed,
    }
}

/// The version `version` is compared against: the live one, else the newest earlier approved.
fn base_version(applet: &Applet, version: u32) -> Option<u32> {
    let earlier = (applet.versions.iter())
        .filter(|entry| entry.state == VersionState::Approved && entry.version < version)
        .map(|entry| entry.version)
        .max();
    applet.live.filter(|live| *live != version).or(earlier)
}

/// The pending request for `version`, else the applet's current access and grants unchanged.
fn request(applet: &Applet, version: u32) -> (ApprovalRequest, bool) {
    match &applet.pending {
        Some(pending) if pending.version == version => (pending.clone(), true),
        _ => (
            ApprovalRequest {
                version,
                access: applet.access,
                grants: applet.grants.clone(),
                origins: applet.embedding.origins.clone(),
                requested_by: applet.created_by,
                requested_at_ms: 0,
            },
            false,
        ),
    }
}

#[utoipa::path(
    get,
    path = "/applets/{applet_id}/versions/{version}/diff",
    tag = "applets",
    summary = "Show what approving a version changes",
    description = r#"Compares one version with the version that serves now, for the approval card.

**Authentication**: realm bearer token without path restrictions, with WRITE on the group's held
applet bucket on the applet's host node.

**Behavior**
- The base is the live version, else the newest earlier approved version; without one every file
  and package counts as added.
- Files compare by BLAKE3 digest; packages come from each version's `package-lock.json` and show
  the builder catalogue's version and license where the package is in the catalogue.
- Access, grants and origins come from the pending request for this version, else they show the
  applet's current settings unchanged (`pending` is false then).
- `unsandboxed` is true when the applet may start work and some backend of this node declares
  no kernel sandbox."#,
    params(
        ("applet_id" = String, Path, description = "Applet id; an unparseable id is 404"),
        ("version" = u32, Path, description = "Version number")
    ),
    responses(
        (status = 200, description = "The changes approval would make", body = DiffBody),
        (status = 403, description = "No WRITE on the held bucket", body = ErrorResponse),
        (status = 404, description = "No such applet or version, or a taken down version", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn version_diff(
    State(state): State<Arc<ServerState>>,
    UnrestrictedAuth(auth): UnrestrictedAuth,
    Path((id, version)): Path<(String, u32)>,
) -> ServerResult<Json<DiffBody>> {
    let read = AppletAccessOperation::new(&auth, applet_id(&id)?, Need::Publish);
    let applet = drive(read, &state.get_ctx()).await.map_err(applet_error)?;
    let entry = (applet.version(version))
        .filter(|entry| entry.state != VersionState::TakenDown)
        .ok_or(ServerError::NotFound)?;
    let next = read_manifest(&state, &applet, &entry.manifest).await?;
    let base = base_version(&applet, version);
    let before = match base.and_then(|base| applet.version(base)) {
        Some(entry) => Some(read_manifest(&state, &applet, &entry.manifest).await?),
        None => None,
    };
    let (pending, requested) = request(&applet, version);
    let backends = drive(SandboxOperation, &state.get_ctx())
        .await
        .map_err(applet_error)?;
    let open = backends.is_empty() || backends.iter().any(|backend| !backend.sandbox);
    let unsandboxed = pending.grants.start.is_some() && open;
    let diff = approval_diff(&applet, &pending, before.as_ref(), &next, unsandboxed);
    let catalogue = (state.applet_settings().building.as_ref())
        .map_or_else(default_catalogue, |building| building.catalogue.clone());
    Ok(Json(diff_body(diff, base, requested, &catalogue)))
}
