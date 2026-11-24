//! Request and response bodies of the applet routes and their mapping to the applet record.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use aruna_core::applet::record::{
    Access, Applet, ApprovalRequest, CustomDomain, DomainState, Folder, Grants, GuestPolicy,
    Lifecycle, Limits, Serving, StartGrant, VersionRef, VersionState,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;
use utoipa::{IntoParams, ToSchema};

use crate::error::ServerError;
use crate::jobs::rfc3339;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleBody {
    Private,
    WaitingForApproval,
    Live,
    Paused,
    TakenDown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AccessBody {
    Private,
    GroupMembers,
    Public,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServingBody {
    Live,
    Preview,
    FeaturePaused,
    Paused,
    Gone,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VersionStateBody {
    Private,
    Approved,
    TakenDown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DomainStateBody {
    Pending,
    Active,
    Suspended,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StartBody {
    Tool { tool_id: String, version: String },
    Workflow { workflow_id: String, version: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FolderBody {
    pub bucket: String,
    pub prefix: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct LimitsBody {
    pub cpu_cores: Option<u32>,
    pub ram_bytes: Option<u64>,
    pub walltime_ms: Option<u64>,
    pub upload_bytes: Option<u64>,
    pub visitor_hourly: Option<u32>,
    pub visitor_daily: Option<u32>,
    pub applet_daily: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(default)]
pub struct GrantsBody {
    /// Permission path patterns inside the group that the applet reads.
    pub read: Vec<String>,
    pub references: Vec<String>,
    pub start: Option<StartBody>,
    pub submissions: Option<FolderBody>,
    pub limits: LimitsBody,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GuestBody {
    pub in_frames: bool,
    pub cpu_cores: u32,
    pub ram_bytes: u64,
    pub walltime_ms: u64,
    pub file_bytes: u64,
    pub hourly: u32,
    pub daily: u32,
    pub expiry_days: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct VersionBody {
    pub version: u32,
    pub commit: Option<String>,
    pub state: VersionStateBody,
    pub created_by: String,
    pub created_at: String,
    pub approved_by: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DomainBody {
    pub host: String,
    pub state: DomainStateBody,
    /// The TXT record `_aruna-verify.<host>` must hold this value.
    pub txt_value: String,
    pub checked_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PendingBody {
    pub version: u32,
    pub access: AccessBody,
    pub grants: GrantsBody,
    pub origins: Vec<String>,
    pub requested_by: String,
    pub requested_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EmbeddingBody {
    pub enabled: bool,
    pub origins: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "applet_id": "01K8W1B3D5F7H9KMQ1S3V5X7Z2", "group_id": "01JABCDEF0123456789ABCDEFG",
    "name": "Bakta web", "slug": "bakta", "host": "bakta.apps.example.org", "repository": null,
    "host_node": "f3a1b2c3d4e5f60718293a4b5c6d7e8f9091a2b3c4d5e6f708192a3b4c5d6e7f",
    "lifecycle": "live", "access": "public", "serving": "live", "live_version": 1, "versions": [],
    "grants": {"read": [], "references": [], "start": null, "submissions": null, "limits": {
        "cpu_cores": null, "ram_bytes": null, "walltime_ms": null, "upload_bytes": null,
        "visitor_hourly": null, "visitor_daily": null, "applet_daily": null}},
    "embedding": {"enabled": false, "origins": []}, "guests": null, "domains": [], "pending": null,
    "account": null, "created_by": "01K8W1A0000000000000000000",
    "created_at": "2026-11-24T08:00:00+00:00", "revision": 6
}))]
pub struct AppletBody {
    pub applet_id: String,
    pub group_id: String,
    pub name: String,
    pub slug: String,
    pub host: String,
    pub repository: Option<String>,
    pub host_node: String,
    pub lifecycle: LifecycleBody,
    pub access: AccessBody,
    /// What the edge serves now; a live applet whose feature is off shows `feature_paused`.
    pub serving: ServingBody,
    pub live_version: Option<u32>,
    pub versions: Vec<VersionBody>,
    pub grants: GrantsBody,
    pub embedding: EmbeddingBody,
    pub guests: Option<GuestBody>,
    pub domains: Vec<DomainBody>,
    pub pending: Option<PendingBody>,
    pub account: Option<String>,
    pub created_by: String,
    pub created_at: String,
    /// Raised by every change; send it back as `expected_revision` to change the applet.
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct AppletPageBody {
    pub items: Vec<AppletBody>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct AppletQuery {
    /// The group whose applets to list.
    pub group_id: String,
    /// Items of one page, 50 when absent and at most 500.
    pub limit: Option<usize>,
    /// `next_cursor` of the previous page.
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct CreateAppletRequest {
    pub group_id: String,
    pub name: String,
    /// One DNS label: the applet's host is `<slug>.<applet domain>`.
    pub slug: String,
    /// A metadata document of the group whose Git repository holds the source.
    pub repository: Option<String>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(example = json!({"expected_revision": 6}))]
pub struct RevisionBody {
    /// The `revision` the caller last read; a newer record refuses the change with 409.
    pub expected_revision: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(example = json!({"version": 2, "expected_revision": 5}))]
pub struct VersionAction {
    pub version: u32,
    pub expected_revision: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(example = json!({"version": 2, "expected_revision": 7}))]
pub struct TakedownRequest {
    /// Takes only this version down; without it the whole applet goes down for good.
    pub version: Option<u32>,
    pub expected_revision: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
pub struct ApprovalRequestBody {
    pub version: u32,
    pub access: AccessBody,
    #[serde(default)]
    pub grants: GrantsBody,
    /// Exact `https://` origins that may frame the applet once approved.
    #[serde(default)]
    pub origins: Vec<String>,
    pub expected_revision: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(example = json!({"enabled": true, "origins": ["https://lab.example.org"], "expected_revision": 7}))]
pub struct EmbeddingRequest {
    pub enabled: bool,
    /// Approved origins to keep; an origin not approved yet needs a request for approval.
    pub origins: Vec<String>,
    pub expected_revision: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, ToSchema)]
#[schema(example = json!({"guests": {"in_frames": false, "cpu_cores": 2, "ram_bytes": 4000000000u64,
    "walltime_ms": 3600000, "file_bytes": 100000000, "hourly": 5, "daily": 20, "expiry_days": 30},
    "expected_revision": 7}))]
pub struct GuestsRequest {
    /// The policy to apply; `null` switches guest submissions off.
    pub guests: Option<GuestBody>,
    pub expected_revision: Option<u64>,
}

fn ulid_text(text: &str, name: &str) -> Result<Ulid, ServerError> {
    Ulid::from_string(text)
        .map_err(|_| ServerError::BadRequestReason(format!("{name} is not a ULID")))
}

impl From<Lifecycle> for LifecycleBody {
    fn from(value: Lifecycle) -> Self {
        match value {
            Lifecycle::Private => Self::Private,
            Lifecycle::WaitingForApproval => Self::WaitingForApproval,
            Lifecycle::Live => Self::Live,
            Lifecycle::Paused => Self::Paused,
            Lifecycle::TakenDown => Self::TakenDown,
        }
    }
}

impl From<Access> for AccessBody {
    fn from(value: Access) -> Self {
        match value {
            Access::Private => Self::Private,
            Access::GroupMembers => Self::GroupMembers,
            Access::Public => Self::Public,
        }
    }
}

impl From<AccessBody> for Access {
    fn from(value: AccessBody) -> Self {
        match value {
            AccessBody::Private => Self::Private,
            AccessBody::GroupMembers => Self::GroupMembers,
            AccessBody::Public => Self::Public,
        }
    }
}

impl From<Serving> for ServingBody {
    fn from(value: Serving) -> Self {
        match value {
            Serving::Live { .. } => Self::Live,
            Serving::Preview { .. } => Self::Preview,
            Serving::FeaturePaused => Self::FeaturePaused,
            Serving::Paused => Self::Paused,
            Serving::Gone => Self::Gone,
        }
    }
}

impl From<&Grants> for GrantsBody {
    fn from(grants: &Grants) -> Self {
        let limits = &grants.limits;
        Self {
            read: grants.read.clone(),
            references: grants.references.clone(),
            start: grants.start.as_ref().map(|start| match start {
                StartGrant::Tool { tool_id, version } => StartBody::Tool {
                    tool_id: tool_id.clone(),
                    version: version.clone(),
                },
                StartGrant::Workflow {
                    workflow_id,
                    version,
                } => StartBody::Workflow {
                    workflow_id: workflow_id.to_string(),
                    version: *version,
                },
            }),
            submissions: grants.submissions.as_ref().map(|folder| FolderBody {
                bucket: folder.bucket.clone(),
                prefix: folder.prefix.clone(),
            }),
            limits: LimitsBody {
                cpu_cores: limits.cpu_cores,
                ram_bytes: limits.ram_bytes,
                walltime_ms: limits.walltime_ms,
                upload_bytes: limits.upload_bytes,
                visitor_hourly: limits.visitor_hourly,
                visitor_daily: limits.visitor_daily,
                applet_daily: limits.applet_daily,
            },
        }
    }
}

impl GrantsBody {
    /// The grants, with every read pattern confined to the group's own subtree.
    pub fn grants(self, group_root: &str) -> Result<Grants, ServerError> {
        let confined =
            |pattern: &String| aruna_core::permission_path::role_path_confined(pattern, group_root);
        if let Some(pattern) = self
            .read
            .iter()
            .chain(&self.references)
            .find(|p| !confined(p))
        {
            return Err(ServerError::BadRequestReason(format!(
                "{pattern} is not a path of the applet's group"
            )));
        }
        let start = match self.start {
            Some(StartBody::Tool { tool_id, version }) => {
                Some(StartGrant::Tool { tool_id, version })
            }
            Some(StartBody::Workflow {
                workflow_id,
                version,
            }) => Some(StartGrant::Workflow {
                workflow_id: ulid_text(&workflow_id, "workflow_id")?,
                version,
            }),
            None => None,
        };
        let limits = self.limits;
        Ok(Grants {
            read: self.read,
            references: self.references,
            start,
            submissions: self.submissions.map(|folder| Folder {
                bucket: folder.bucket,
                prefix: folder.prefix,
            }),
            limits: Limits {
                cpu_cores: limits.cpu_cores,
                ram_bytes: limits.ram_bytes,
                walltime_ms: limits.walltime_ms,
                upload_bytes: limits.upload_bytes,
                visitor_hourly: limits.visitor_hourly,
                visitor_daily: limits.visitor_daily,
                applet_daily: limits.applet_daily,
            },
        })
    }
}

impl From<&GuestPolicy> for GuestBody {
    fn from(policy: &GuestPolicy) -> Self {
        Self {
            in_frames: policy.in_frames,
            cpu_cores: policy.cpu_cores,
            ram_bytes: policy.ram_bytes,
            walltime_ms: policy.walltime_ms,
            file_bytes: policy.file_bytes,
            hourly: policy.hourly,
            daily: policy.daily,
            expiry_days: policy.expiry_days,
        }
    }
}

impl GuestBody {
    pub fn policy(self, enabled_by: aruna_core::user::id::UserId) -> GuestPolicy {
        GuestPolicy {
            enabled_by,
            in_frames: self.in_frames,
            cpu_cores: self.cpu_cores,
            ram_bytes: self.ram_bytes,
            walltime_ms: self.walltime_ms,
            file_bytes: self.file_bytes,
            hourly: self.hourly,
            daily: self.daily,
            expiry_days: self.expiry_days,
        }
    }
}

fn version_body(entry: &VersionRef) -> VersionBody {
    VersionBody {
        version: entry.version,
        commit: entry.commit.clone(),
        state: match entry.state {
            VersionState::Private => VersionStateBody::Private,
            VersionState::Approved => VersionStateBody::Approved,
            VersionState::TakenDown => VersionStateBody::TakenDown,
        },
        created_by: entry.created_by.to_string(),
        created_at: rfc3339(entry.created_at_ms),
        approved_by: entry.approved_by.map(|user| user.to_string()),
    }
}

fn domain_body(domain: &CustomDomain) -> DomainBody {
    DomainBody {
        host: domain.host.clone(),
        state: match domain.state {
            DomainState::Pending => DomainStateBody::Pending,
            DomainState::Active => DomainStateBody::Active,
            DomainState::Suspended => DomainStateBody::Suspended,
        },
        txt_value: aruna_core::applet::hosts::verify_value(&domain.nonce),
        checked_at: rfc3339(domain.checked_at_ms),
    }
}

fn pending_body(request: &ApprovalRequest) -> PendingBody {
    PendingBody {
        version: request.version,
        access: request.access.into(),
        grants: GrantsBody::from(&request.grants),
        origins: request.origins.clone(),
        requested_by: request.requested_by.to_string(),
        requested_at: rfc3339(request.requested_at_ms),
    }
}

impl AppletBody {
    /// The record as callers see it, with the serving state of this moment.
    pub fn new(applet: &Applet, serving: Serving) -> Self {
        Self {
            applet_id: applet.applet_id.to_string(),
            group_id: applet.group_id.to_string(),
            name: applet.name.clone(),
            slug: applet.slug.clone(),
            host: applet.host.clone(),
            repository: applet.repository.map(|id| id.to_string()),
            host_node: applet.host_node.to_string(),
            lifecycle: applet.lifecycle.into(),
            access: applet.access.into(),
            serving: serving.into(),
            live_version: applet.live,
            versions: applet.versions.iter().map(version_body).collect(),
            grants: GrantsBody::from(&applet.grants),
            embedding: EmbeddingBody {
                enabled: applet.embedding.enabled,
                origins: applet.embedding.origins.clone(),
            },
            guests: applet.guests.as_ref().map(GuestBody::from),
            domains: applet.domains.iter().map(domain_body).collect(),
            pending: applet.pending.as_ref().map(pending_body),
            account: applet.account.map(|account| account.to_string()),
            created_by: applet.created_by.to_string(),
            created_at: rfc3339(applet.created_at_ms),
            revision: applet.revision,
        }
    }
}
