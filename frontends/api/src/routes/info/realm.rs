//! Serves the realm info route with its public overview, nodes and urls.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::node::{
    DownloadSlotsResponse, NodeDocumentResponse, NodeInfoResponse, NodeKindInfo, RealmUrlsResponse,
    RealmUtilizationResponse, TelemetryState, executor_response, interface_services_status,
    load_realm_presence, map_realm_nodes,
};
use super::quota::RealmQuotaConfig;
use super::usage::realm_totals;
use crate::error::{ServerError, ServerResult};
use crate::routes::federation::AcceptedRealmsSetting;
use crate::server::state::ServerState;
use aruna_clock::unix_timestamp_millis;
use aruna_core::heartbeat::HeldHeartbeat;
use aruna_core::node::state::ReadInfoOperation;
use aruna_core::structs::identity::auth::AuthContext;
use aruna_core::structs::identity::realm::{RealmConfigDocument, RealmNodeKind};
use aruna_core::structs::storage::node_info::NodeUtilization;
use aruna_ops_data::metadata::realm_counts::{RealmDocumentsOperation, RealmGroupsOperation};
use aruna_ops_identity::device_node::realm_documents::{
    installed_displays, installed_management_urls,
};
use aruna_ops_identity::document_repository::GetConfigOperation;
use aruna_ops_identity::realm::node_name::{NodeDisplays, ReadNamesOperation};
use aruna_ops_work::node::contacts::PeerContactsOperation;
use aruna_ops_work::relay::targets::management_node_urls;
use aruna_runner::driver::drive;
use aruna_wire::info::InterfaceServicesStatus;
use axum::extract::State;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::warn;
use utoipa::ToSchema;

/// Realm information. Identity, description, oidc providers, public urls and the
/// count-only overview are public; topology, quota and listen addresses need a realm
/// token, and gated values are absent or empty rather than restructured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RealmInfoResponse {
    pub realm_id: String,
    pub description: String,
    pub metadata_replication: RealmReplicationResponse,
    pub oidc_providers: Vec<RealmProviderResponse>,
    /// Count-only realm overview, available to anonymous and authenticated
    /// callers. The optional wrapper permits an older or otherwise unable node
    /// to omit the whole extension without changing the rest of the response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_overview: Option<RealmPublicOverview>,
    /// True on a management node, the only kind that mints enrollments and
    /// issues the realm's credentials. Public.
    pub is_management_node: bool,
    /// API base urls of the realm's management nodes, from their node
    /// information documents; a device follows one to enroll. Public.
    pub management_urls: Vec<String>,
    /// Realm server and management nodes with the api base url each published, so a page
    /// without sign-in reaches a named node. Public; user devices are never listed.
    #[serde(default)]
    pub public_nodes: Vec<PublicNode>,
    /// Which other realms' users may log in here, from the federation settings; mode `none`
    /// when the realm has none. Public.
    #[serde(default)]
    pub accepted_realms: AcceptedRealmsSetting,
    /// Realm discovery configuration. Realm-authenticated callers only.
    #[schema(value_type = Object)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery: Option<Value>,
    /// Realm nodes. Realm-authenticated callers only, else empty.
    pub nodes: Vec<NodeInfoResponse>,
    /// Realm quota policy. Realm-authenticated callers only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<RealmQuotaConfig>,
    pub interfaces: InterfaceServicesStatus,
}

/// A realm node a public page can reach: its id and published api base url only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PublicNode {
    pub node_id: String,
    pub api_url: String,
}

/// Public, count-only realm overview. Nullable fields mean this node could not
/// answer; they never use zero as a stand-in for unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RealmPublicOverview {
    /// Lifecycle-live metadata documents in the realm. This is the registry
    /// document count, not caller-filtered and not multiplied by replicas.
    pub live_datasets: Option<u64>,
    /// Groups stored for this realm.
    pub groups: Option<u64>,
    /// Nodes in the replicated realm configuration, regardless of DHT presence
    /// or health.
    pub nodes_configured: Option<u64>,
    /// Current objects in the realm, each counted once, private ones included.
    pub objects: Option<u64>,
    /// Bytes of every version in the realm, stored and referenced alike.
    pub data_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmMetadataReplicationResponse)]
pub struct RealmReplicationResponse {
    pub default_replication_factor: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(as = RealmOidcProviderResponse)]
pub struct RealmProviderResponse {
    pub id: String,
    pub issuer: String,
    pub audience: String,
    pub discovery_url: String,
}

#[utoipa::path(
    get,
    path = "/system/realm",
    tag = "system/realm",
    summary = "Read the realm's public settings and node topology",
    description = r#"Returns the realm's public settings, and its discovery, quota and node topology to a realm member.

**Authentication**: optional bearer token; without a usable token of this realm, and that includes a
token of another realm or one this node cannot validate, the response is the public part only.

**Behavior**
- The public part is the realm id, description, metadata replication policy, the OIDC providers a
  client needs to obtain a token, the public interface urls, `public_overview`, and where the
  realm's management nodes are: `is_management_node` says whether this node is one, and
  `management_urls` lists the api base urls the management nodes published, this node's own first.
  Only a management node mints enrollments and issues the realm's credentials, so a device or a
  client that reached any other node follows one of these urls instead.
- `public_nodes` is also public: each server and management node's id and published api base url,
  nothing else, so a page without sign-in (such as a share link) reaches the node it names. User
  devices are never listed.
- `accepted_realms` is also public: which other realms' users may log in here, from the federation
  settings. Mode `none` (also without federation settings), `any`, or `only` with the accepted
  realm ids in `realms`. A sign-in page offers login with another realm only when it is not `none`.
- `public_overview` carries five nullable aggregates: `live_datasets`, the realm's lifecycle-live
  metadata registry count, never caller-filtered or replica-multiplied; `groups`, the realm's
  stored group count; `nodes_configured`, the membership count in this node's replicated realm
  configuration, never DHT presence or health; and `objects` and `data_bytes`, the realm totals of
  `GET /system/usage` (`realm.objects`, and `realm.logical_bytes` plus `realm.referenced_bytes`).
  They count private data too but name no bucket or dataset. A null value means this node could
  not answer and is never encoded as zero.
- Without a token, `nodes` lists the server and management nodes with `node_id`, `kind`,
  `configured`, `present`, `connection_status`, `display_name`, `place` and `coordinates` only;
  owners, placement, node documents and devices are left out.
- A bearer token of this realm additionally reveals the realm's discovery configuration, its quota
  policy, the full node list and the interface listen addresses. Gated values are absent or empty,
  never restructured, so one parser handles both.
- The node list is the realm's configured membership read from this node's replicated realm
  configuration; `placement` is the node's entry in the placement map and `info` is the last node
  information document that reached this node, so both may lag or be absent, as may
  `management_urls`.
- `info.utilization` comes from the latest heartbeat the node pushed, including its DRS download
  slot use in `download_slots`, with `heartbeat_at_ms` as the
  time it arrived. `info.telemetry` is `fresh` or `stale` with `info.telemetry_age_ms`, or `absent`
  when no heartbeat is held yet, in which case the values are those of the last published document.
- Liveness is a separate, deliberately conservative signal: presence is resolved through a bounded
  realm lookup with a four second budget, and if that lookup is stale, times out or fails, only this
  node counts as present. `present` true with `connection_status` `connected` means the peer was
  confirmed live just now; `configured` means no fresh confirmation, which is not evidence that the
  peer is down.
- A `user` node is a device and publishes no presence at all, so `present` is always false. That is
  the absence of a signal, never a report that the device is down. A device is instead reported
  from what this node itself saw: `last_seen_ms` is when it last reached this node over an
  authorized request, and `connection_status` `seen` means within the last three minutes, otherwise
  `unknown`. Both are this node's own observation, never realm state, and start empty after a
  restart."#,
    responses(
        (
            status = 200,
            description = "Realm information at the caller's level of access; discovery, quota, the full node list and interface bind addresses only for a caller holding a token of this realm",
            body = RealmInfoResponse,
            examples(
                ("Anonymous" = (
                    summary = "What a client needs to reach the realm and obtain a token",
                    value = json!({
                        "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                        "description": "Example realm",
                        "metadata_replication": {"default_replication_factor": 3},
                        "public_overview": {
                            "live_datasets": 4096,
                            "groups": 12,
                            "nodes_configured": 3,
                            "objects": 44438184,
                            "data_bytes": 42770615394304_u64
                        },
                        "is_management_node": false,
                        "management_urls": ["https://mgmt.example.test/api/v1"],
                        "public_nodes": [{"node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978", "api_url": "https://node.example.test/api/v1"}],
                        "accepted_realms": {"mode": "only", "realms": ["BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc"]},
                        "oidc_providers": [
                            {
                                "id": "example",
                                "issuer": "https://idp.example.test/realms/aruna",
                                "audience": "aruna",
                                "discovery_url": "https://idp.example.test/realms/aruna/.well-known/openid-configuration"
                            }
                        ],
                        "nodes": [],
                        "interfaces": {
                            "rest": {"status": "available", "bind": null, "url": "https://node.example.test/api/v1"},
                            "s3": {"status": "available", "bind": null, "url": "https://s3.example.test"}
                        }
                    })
                )),
                ("Realm token" = (
                    summary = "A realm member also sees discovery, quota and the node topology",
                    value = json!({
                        "realm_id": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                        "description": "Example realm",
                        "metadata_replication": {"default_replication_factor": 3},
                        "public_overview": {
                            "live_datasets": 4096,
                            "groups": 12,
                            "nodes_configured": 3,
                            "objects": 44438184,
                            "data_bytes": 42770615394304_u64
                        },
                        "is_management_node": true,
                        "management_urls": ["https://node.example.test/api/v1"],
                        "public_nodes": [{"node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978", "api_url": "https://node.example.test/api/v1"}],
                        "accepted_realms": {"mode": "any"},
                        "oidc_providers": [],
                        "discovery": {"Dynamic": {"methods": [{"DhtSigned": {"ttl_secs": 3600, "refresh_after_secs": 1800}}]}},
                        "nodes": [
                            {
                                "node_id": "1f2e3d4c5b6a79880f1e2d3c4b5a69780f1e2d3c4b5a69780f1e2d3c4b5a6978",
                                "kind": "server",
                                "owner": null,
                                "configured": true,
                                "present": true,
                                "connection_status": "connected",
                                "placement": {"location": "dc-a", "weight": 100, "full": false, "draining": false},
                                "info": {
                                    "executors": [{
                                        "backend": "default",
                                        "kind": "docker",
                                        "file_staging": true,
                                        "direct_s3": false,
                                        "location": "eu-west",
                                        "labels": {"aruna-engine.org/location": "eu-west"},
                                        "limits": {"max_cpu_cores": 8, "max_concurrent": 4},
                                        "capabilities": {
                                            "gpus": 0,
                                            "architectures": ["x86_64"],
                                            "stdin": false,
                                            "multi_container": false,
                                            "services": false,
                                            "ingress": false
                                        },
                                        "suitability": {"batch": "good", "sessions": "good", "services": "possible"},
                                        "group_count": 0,
                                        "config_generation": 1
                                    }],
                                    "labels": {"zone": "dc-a"},
                                    "urls": {"api": "https://node.example.test/api/v1"},
                                    "utilization": {
                                        "storage_bytes_used": 1073741824,
                                        "documents_held": 128,
                                        "load_permille": 120,
                                        "heartbeat_at_ms": 1775744591123_i64
                                    },
                                    "telemetry": "fresh",
                                    "telemetry_age_ms": 4200,
                                    "updated_at_ms": 1775744591123_i64
                                }
                            },
                            {
                                "node_id": "2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a",
                                "kind": "user",
                                "owner": "01JHKMNPQR0123456789ABCDEF@AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8",
                                "configured": true,
                                "present": false,
                                "connection_status": "seen",
                                "last_seen_ms": 1775744591123_i64,
                                "placement": null,
                                "info": null
                            }
                        ],
                        "quota": {
                            "default_group_quota_bytes": 10737418240_i64,
                            "grace_factor_percent": 110,
                            "warn_threshold_percent": 80,
                            "group_overrides": [],
                            "max_groups_per_user": 10,
                            "user_group_cap_overrides": [],
                            "max_devices_per_user": 5,
                            "device_requests_per_minute": 600,
                            "device_concurrent_pulls": 8,
                            "count_replicas": false
                        },
                        "interfaces": {
                            "rest": {"status": "available", "bind": "0.0.0.0:3000", "url": "https://node.example.test/api/v1"},
                            "s3": {"status": "available", "bind": "0.0.0.0:1337", "url": "https://s3.example.test"}
                        }
                    })
                ))
            )
        ),
        (status = 404, description = "This node holds no configuration document for its realm, so there is nothing to report yet", body = crate::error::ErrorResponse)
    ),
    security((), ("bearer_auth" = []))
)]
pub async fn get_realm_info(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
) -> ServerResult<(StatusCode, Json<RealmInfoResponse>)> {
    Ok((StatusCode::OK, Json(run_realm_info(&state, auth).await?)))
}

pub(crate) async fn run_realm_info(
    state: &ServerState,
    auth: Option<AuthContext>,
) -> ServerResult<RealmInfoResponse> {
    let config = drive(
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
    let realm_authenticated = auth.is_some_and(|auth| auth.realm_id == state.get_realm_id());

    let mut interfaces = interface_services_status(state).await;
    if !realm_authenticated {
        interfaces.rest.bind = None;
        interfaces.s3.bind = None;
        if let Some(mcp) = interfaces.mcp.as_mut() {
            mcp.bind = None;
        }
    }

    let metadata_replication = RealmReplicationResponse {
        default_replication_factor: config.effective_replication_factor(),
    };
    let live_datasets = match drive(
        RealmDocumentsOperation::new(config.realm_id),
        &state.get_ctx(),
    )
    .await
    {
        Ok(count) => count,
        Err(error) => {
            warn!(error = %error, "metadata document count unavailable for public realm overview");
            None
        }
    };
    let groups = match drive(RealmGroupsOperation::new(config.realm_id), &state.get_ctx()).await {
        Ok(count) => Some(count),
        Err(error) => {
            warn!(error = %error, "group count unavailable for public realm overview");
            None
        }
    };
    let totals = realm_totals(state).await;
    let public_overview = Some(RealmPublicOverview {
        live_datasets,
        groups,
        nodes_configured: u64::try_from(config.nodes.len()).ok(),
        objects: totals.map(|(objects, _)| objects),
        data_bytes: totals.map(|(_, data_bytes)| data_bytes),
    });

    let node_info_docs = load_node_documents(state, &config).await;
    let mut management_urls = management_urls(
        state,
        &config,
        &node_info_docs,
        interfaces.rest.url.as_deref(),
    );
    // A device holds no peer node-info document, so the list a realm node
    // installed on it is what the portal is offered there.
    if management_urls.is_empty() {
        management_urls = drive(installed_management_urls(config.realm_id), &state.get_ctx())
            .await
            .unwrap_or_default();
    }

    let public_nodes = public_nodes(
        state,
        &config,
        &node_info_docs,
        interfaces.rest.url.as_deref(),
    );

    let (discovery, nodes, quota) = if realm_authenticated {
        let present_nodes = load_realm_presence(state).await;
        let discovery = serde_json::to_value(&config.discovery)
            .map_err(|error| ServerError::InternalError(error.into()))?;
        let contacts = peer_contacts(state, &config).await;
        let names = node_names(state, config.realm_id).await;
        let nodes = map_realm_nodes(
            state,
            &config,
            present_nodes,
            node_info_docs,
            &contacts,
            &names,
            unix_timestamp_millis(),
        );
        (
            Some(discovery),
            nodes,
            Some(RealmQuotaConfig::from(config.quota.clone()).with_replicas(&config.replica_quota)),
        )
    } else {
        (None, visitor_nodes(state, &config).await, None)
    };

    let accepted_realms = config
        .federation
        .as_ref()
        .map(|settings| AcceptedRealmsSetting::from(&settings.accepted_realms))
        .unwrap_or_default();

    Ok(RealmInfoResponse {
        realm_id: config.realm_id.to_string(),
        description: config.description,
        metadata_replication,
        oidc_providers: config
            .oidc_providers
            .into_iter()
            .map(|provider| RealmProviderResponse {
                id: provider.id,
                issuer: provider.issuer,
                audience: provider.audience,
                discovery_url: provider.discovery_url,
            })
            .collect(),
        public_overview,
        is_management_node: state.is_management_node(),
        management_urls,
        public_nodes,
        accepted_realms,
        discovery,
        nodes,
        quota,
        interfaces,
    })
}

/// When this node last saw each node of the realm config; empty when it cannot tell.
async fn peer_contacts(
    state: &ServerState,
    config: &RealmConfigDocument,
) -> BTreeMap<aruna_core::id::NodeId, u64> {
    let peers = (config.nodes.iter())
        .filter_map(|node| node.node_id.parse::<aruna_core::id::NodeId>().ok())
        .collect();
    drive(PeerContactsOperation::new(peers), &state.get_ctx())
        .await
        .unwrap_or_default()
}

/// The realm's infrastructure nodes as a visitor sees them: name, place and whether they are
/// online, without owners, placement, utilization or devices.
async fn visitor_nodes(state: &ServerState, config: &RealmConfigDocument) -> Vec<NodeInfoResponse> {
    let present_nodes = load_realm_presence(state).await;
    let names = node_names(state, config.realm_id).await;
    let contacts = peer_contacts(state, config).await;
    let documents = BTreeMap::new();
    let nodes = map_realm_nodes(
        state,
        config,
        present_nodes,
        documents,
        &contacts,
        &names,
        0,
    );
    let infrastructure = nodes
        .into_iter()
        .filter(|node| node.kind != NodeKindInfo::User);
    infrastructure
        .map(|node| NodeInfoResponse {
            owner: None,
            last_seen_ms: None,
            placement: None,
            info: None,
            ..node
        })
        .collect()
}

/// Node display names and map places. A device runs no document sync, so it shows the copy a
/// realm node last served it.
async fn node_names(
    state: &ServerState,
    realm_id: aruna_core::structs::identity::realm::RealmId,
) -> NodeDisplays {
    if matches!(
        state.node_capabilities(),
        aruna_core::structs::identity::auth::NodeCapabilities::User { .. }
    ) {
        return drive(installed_displays(realm_id), &state.get_ctx())
            .await
            .unwrap_or_default();
    }
    drive(ReadNamesOperation::new(realm_id), &state.get_ctx())
        .await
        .unwrap_or_else(|error| {
            warn!(error = %error, "node display names unavailable for realm info");
            NodeDisplays::default()
        })
}

/// Server and management nodes with a published api url, in realm-config order; this node
/// names its own interface while its document has not landed yet.
fn public_nodes(
    state: &ServerState,
    config: &RealmConfigDocument,
    node_info_docs: &BTreeMap<
        aruna_core::id::NodeId,
        aruna_core::structs::storage::node_info::NodeInfoDocument,
    >,
    own_url: Option<&str>,
) -> Vec<PublicNode> {
    let current = state.get_node_id();
    config
        .nodes
        .iter()
        .filter(|node| !matches!(node.kind, RealmNodeKind::User { .. }))
        .filter_map(|node| node.node_id.parse::<aruna_core::id::NodeId>().ok())
        .filter_map(|node_id| {
            let published = node_info_docs
                .get(&node_id)
                .and_then(|doc| doc.urls.api.clone());
            let own = (node_id == current)
                .then(|| own_url.map(str::to_string))
                .flatten();
            Some(PublicNode {
                node_id: node_id.to_string(),
                api_url: published.or(own)?,
            })
        })
        .collect()
}

/// The management nodes' published api urls, this node first. A management
/// node whose own document has not landed yet names its published interface.
fn management_urls(
    state: &ServerState,
    config: &RealmConfigDocument,
    node_info_docs: &BTreeMap<
        aruna_core::id::NodeId,
        aruna_core::structs::storage::node_info::NodeInfoDocument,
    >,
    own_url: Option<&str>,
) -> Vec<String> {
    let current = state.get_node_id();
    let mut urls: Vec<String> = Vec::new();
    for (node_id, published) in management_node_urls(config, |node| {
        (node_info_docs.get(node)).and_then(|doc| doc.urls.api.clone())
    }) {
        let is_current = node_id == current;
        let url = published.or_else(|| is_current.then(|| own_url.map(str::to_string)).flatten());
        let Some(url) = url else {
            continue;
        };
        if urls.contains(&url) {
            continue;
        }
        if is_current {
            urls.insert(0, url);
        } else {
            urls.push(url);
        }
    }
    urls
}

pub(crate) async fn load_node_documents(
    state: &ServerState,
    config: &RealmConfigDocument,
) -> BTreeMap<aruna_core::id::NodeId, aruna_core::structs::storage::node_info::NodeInfoDocument> {
    let node_ids: Vec<aruna_core::id::NodeId> = config
        .nodes
        .iter()
        .filter_map(|node| node.node_id.parse().ok())
        .collect();
    match drive(ReadInfoOperation::new(node_ids), state.get_ctx().as_ref()).await {
        Ok(documents) => documents,
        Err(error) => {
            warn!(error = %error, "Failed to load node info documents for realm info");
            BTreeMap::new()
        }
    }
}

pub(super) fn map_node_document(
    document: &aruna_core::structs::storage::node_info::NodeInfoDocument,
    held: Option<&HeldHeartbeat>,
) -> NodeDocumentResponse {
    let utilization = held.map_or(document.utilization, |held| NodeUtilization {
        heartbeat_at_ms: held.received_at_ms,
        ..held.heartbeat.utilization
    });
    let jobs = held.and_then(|held| held.heartbeat.jobs);
    NodeDocumentResponse {
        executors: document.executors.iter().map(executor_response).collect(),
        labels: document.labels.clone(),
        urls: RealmUrlsResponse {
            api: document.urls.api.clone(),
            s3: document.urls.s3.clone(),
        },
        utilization: RealmUtilizationResponse {
            storage_bytes_used: utilization.storage_bytes_used,
            documents_held: utilization.documents_held,
            load_permille: utilization.load_permille,
            heartbeat_at_ms: utilization.heartbeat_at_ms,
            download_slots: utilization
                .download_slots
                .map(|slots| DownloadSlotsResponse {
                    active: slots.active,
                    max: slots.max,
                    refused: slots.refused,
                }),
            jobs_running: jobs.map(|jobs| jobs.total),
            imports_running: jobs.map(|jobs| jobs.imports),
        },
        telemetry: TelemetryState::of(held),
        telemetry_age_ms: held.map(|held| held.age_ms),
        updated_at_ms: document.updated_at_ms,
    }
}
