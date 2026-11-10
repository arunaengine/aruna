//! Serves the realm's plugin catalog: realm admins publish artifacts, signed-in users list them.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::errors::StorageError;
use aruna_core::plugins::{MAX_ARTIFACT, PublishedPlugin};
use aruna_core::structs::identity::auth::{Actor, AuthContext, Permission};
use aruna_core::structs::placement::policy::document::policy_admin_path;
use aruna_operations::driver::drive;
use aruna_operations::plugins::artifacts::ArtifactError;
use aruna_operations::plugins::copies::{MemoryPermit, reserve};
use aruna_operations::plugins::publish::{EntryError, PublishError, publish_plugin};
use aruna_operations::realm::get_config::GetConfigOperation;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Query, Request, State};
use axum::http::{StatusCode, header};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::auth::{ensure_permission, require_realm_auth, require_unrestricted_auth};
use crate::error::{ErrorResponse, ServerError, ServerResult};
use crate::server::state::ServerState;

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

#[derive(OpenApi)]
#[openapi(tags((name = "plugins", description = "WASM plugins published to the realm")))]
pub struct PluginsApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(PluginsApiDoc::openapi())
        .routes(routes!(publish, list_plugins))
        .layer(DefaultBodyLimit::max(MAX_ARTIFACT))
}

/// A plugin artifact as a request body: the bytes of a WebAssembly component.
#[derive(ToSchema)]
#[schema(value_type = String, format = Binary)]
pub struct ArtifactBody(pub Vec<u8>);

/// One published plugin version, without its manifest.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PluginView {
    pub name: String,
    pub version: String,
    pub title: Option<String>,
    /// `sha256:<hex>` of the exact artifact bytes; installs pin it.
    pub digest: String,
    pub size: u64,
    pub published_by: String,
    pub published_at_ms: u64,
}

impl From<PublishedPlugin> for PluginView {
    fn from(plugin: PublishedPlugin) -> Self {
        let manifest: Option<serde_json::Value> = serde_json::from_str(&plugin.manifest).ok();
        let title = manifest
            .as_ref()
            .and_then(|m| m.get("title")?.as_str().map(str::to_string));
        Self {
            name: plugin.name,
            version: plugin.version,
            title,
            digest: plugin.digest,
            size: plugin.size,
            published_by: plugin.published_by.to_string(),
            published_at_ms: plugin.published_at_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PluginPage {
    pub plugins: Vec<PluginView>,
    /// Pass as `cursor` for the next page; absent on the last page.
    pub next: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct PluginQuery {
    /// Only versions of this plugin.
    pub name: Option<String>,
    /// `name@version` of the last entry of the previous page.
    pub cursor: Option<String>,
    /// Entries per page, 1 to 1000; 100 by default.
    pub limit: Option<usize>,
}

#[utoipa::path(
    post,
    path = "/plugins",
    tag = "plugins",
    summary = "Publish a plugin version",
    description = r#"Publishes one plugin artifact built with `aruna-cli plugin build`.

**Authentication**: realm bearer token with WRITE on the realm configuration admin path. A
management node serves the call and every other node relays it to one.

**Behavior**
- The body is the artifact: a WebAssembly component with its manifest embedded, at most 64 MiB.
- The artifact is checked, stored durably on this node and copied to the other management
  nodes. The version is published only after a majority of management nodes stored it (at
  least two when the realm has two or more); otherwise nothing is published and the call
  answers 503. A realm with one management node keeps one copy.
- Publishing the same bytes again answers the existing entry, also through another management
  node at the same time. A version is never replaced.
- Two different artifacts of one version published at the same time through different
  management nodes can both answer 201; once the nodes sync, that version is hidden until it is
  published again.
- The caller's rights are checked before the body is read. Each node spends a bounded amount of
  memory on uploads and answers 503 while that is in use.

**Errors**
- 400 when the artifact is not a valid plugin.
- 409 when this version is already published with other bytes on this node.
- 503 when too few current management nodes confirmed a copy, or this node is busy."#,
    request_body(content = ArtifactBody, content_type = "application/wasm", description = "The plugin artifact"),
    responses(
        (status = 201, description = "The published version", body = PluginView,
            example = json!({"name": "dataset-announcer", "version": "0.1.0", "title": "Announce new datasets", "digest": "sha256:0131fc5a684b331710fa6b96517c67a15aef0881f5ef481ad0230e23cfab64d1", "size": 98304, "published_by": "01JBX7Y3S5M3Q8Y1V9K2T4R6P1@realm", "published_at_ms": 1762700000000u64})),
        (status = 400, description = "The artifact is not a valid plugin", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 403, description = "Caller is not a realm config admin", body = ErrorResponse),
        (status = 409, description = "The version is published with other bytes, or another realm configuration update won the race", body = ErrorResponse),
        (status = 413, description = "The artifact exceeds 64 MiB", body = ErrorResponse),
        (status = 502, description = "A relayed call failed after the management node may already have applied it; code `relay_failed`", body = ErrorResponse),
        (status = 503, description = "Too few management nodes stored the artifact, or none was reachable", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn publish(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    request: Request,
) -> ServerResult<(StatusCode, Json<PluginView>)> {
    let (auth, body, _memory) = read_upload(&state, auth, request).await?;
    let actor = Actor {
        node_id: state.get_node_id(),
        user_id: auth.user_id,
        realm_id: state.get_realm_id(),
    };
    let published = publish_plugin(&state.get_ctx(), actor, auth, Vec::from(body))
        .await
        .map_err(map_publish_error)?;
    Ok((StatusCode::CREATED, Json(published.into())))
}

/// Reads an artifact upload only after the caller may publish and this node reserved memory for
/// it; the permit is held until the caller drops it, also when the request is cancelled.
pub(crate) async fn read_upload(
    state: &ServerState,
    auth: Option<AuthContext>,
    request: Request,
) -> ServerResult<(AuthContext, Bytes, MemoryPermit)> {
    let auth = require_unrestricted_auth(state, auth)?;
    // Request policies live at this boundary; the operation only checks roles.
    let path = policy_admin_path(state.get_realm_id());
    ensure_permission(state, &auth, path, Permission::WRITE).await?;
    let too_large =
        || ServerError::PayloadTooLarge(format!("the artifact exceeds {} MiB", MAX_ARTIFACT >> 20));
    let declared = (request.headers().get(header::CONTENT_LENGTH))
        .and_then(|value| value.to_str().ok()?.parse::<usize>().ok());
    if declared.is_some_and(|length| length > MAX_ARTIFACT) {
        return Err(too_large());
    }
    let busy = || {
        ServerError::ServiceUnavailableReason(
            "artifact uploads use this node's memory; retry later".into(),
        )
    };
    let memory = reserve(declared.unwrap_or(MAX_ARTIFACT)).ok_or_else(busy)?;
    let limit = declared.unwrap_or(MAX_ARTIFACT);
    let body = axum::body::to_bytes(request.into_body(), limit)
        .await
        .map_err(|_| too_large())?;
    Ok((auth, body, memory))
}

#[utoipa::path(
    get,
    path = "/plugins",
    tag = "plugins",
    summary = "List published plugin versions",
    description = r#"Lists the plugin versions published to the realm, sorted by name and version.

**Authentication**: any realm bearer token.

**Behavior**
- Answers from this node's copy of the realm configuration, so a version published a moment
  ago on another node may appear a little later.
- Pages with `cursor` and `limit`."#,
    params(PluginQuery),
    responses(
        (status = 200, description = "One page of published versions", body = PluginPage,
            example = json!({"plugins": [{"name": "dataset-announcer", "version": "0.1.0", "title": "Announce new datasets", "digest": "sha256:0131fc5a684b331710fa6b96517c67a15aef0881f5ef481ad0230e23cfab64d1", "size": 98304, "published_by": "01JBX7Y3S5M3Q8Y1V9K2T4R6P1@realm", "published_at_ms": 1762700000000u64}], "next": null})),
        (status = 400, description = "The limit is outside 1 to 1000", body = ErrorResponse),
        (status = 401, description = "Missing or invalid bearer token", body = ErrorResponse),
        (status = 404, description = "This node holds no configuration document for its realm", body = ErrorResponse)
    ),
    security(("bearer_auth" = []))
)]
pub async fn list_plugins(
    State(state): State<Arc<ServerState>>,
    Extension(auth): Extension<Option<AuthContext>>,
    Query(query): Query<PluginQuery>,
) -> ServerResult<Json<PluginPage>> {
    require_realm_auth(&state, auth)?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(ServerError::BadRequestReason(
            "limit must lie between 1 and 1000".into(),
        ));
    }
    let config = drive(
        GetConfigOperation::new(state.get_realm_id()),
        &state.get_ctx(),
    )
    .await
    .map_err(|_| ServerError::NotFound)?;
    Ok(Json(page(config.plugins, &query, limit)))
}

/// The entries after the cursor, optionally of one plugin, at most `limit` of them.
fn page(plugins: Vec<PublishedPlugin>, query: &PluginQuery, limit: usize) -> PluginPage {
    let key = |plugin: &PublishedPlugin| format!("{}@{}", plugin.name, plugin.version);
    let mut selected: Vec<PublishedPlugin> = (plugins.into_iter())
        .filter(|plugin| query.name.as_ref().is_none_or(|name| plugin.name == *name))
        .filter(|plugin| {
            query
                .cursor
                .as_ref()
                .is_none_or(|cursor| key(plugin) > *cursor)
        })
        .take(limit + 1)
        .collect();
    let more = selected.len() > limit;
    selected.truncate(limit);
    let next = more.then(|| selected.last().map(key)).flatten();
    PluginPage {
        plugins: selected.into_iter().map(PluginView::from).collect(),
        next,
    }
}

fn map_publish_error(error: PublishError) -> ServerError {
    match error {
        PublishError::TooLarge => ServerError::PayloadTooLarge(error.to_string()),
        PublishError::Invalid(_) => ServerError::BadRequestReason(error.to_string()),
        PublishError::Unauthorized | PublishError::NotManagementNode => ServerError::Forbidden,
        PublishError::VersionTaken(_) | PublishError::Entry(EntryError::VersionTaken(_)) => {
            ServerError::Conflict(error.to_string())
        }
        PublishError::Entry(EntryError::StorageError(StorageError::TransactionConflict)) => {
            ServerError::Conflict("concurrent realm configuration update conflict; retry".into())
        }
        PublishError::Entry(EntryError::Unauthorized | EntryError::NotManagementNode) => {
            ServerError::Forbidden
        }
        PublishError::TooFewCopies { .. }
        | PublishError::Entry(EntryError::TooFewCopies { .. })
        | PublishError::Artifact(ArtifactError::Busy)
        | PublishError::Unavailable(_) => ServerError::ServiceUnavailableReason(error.to_string()),
        PublishError::Artifact(ArtifactError::Unavailable(_)) => {
            ServerError::ServiceUnavailableReason(error.to_string())
        }
        other => ServerError::InternalError(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::routes::{test_context, test_state, test_storage};
    use aruna_core::UserId;
    use aruna_core::keys::generate_signing_key;
    use aruna_core::structs::identity::auth::NodeCapabilities;
    use aruna_core::structs::identity::realm::RealmId;
    use ulid::Ulid;

    /// An upload whose body fails the test as soon as anything reads it.
    fn unread() -> Request {
        let stream = futures_util::stream::poll_fn(
            |_| -> std::task::Poll<Option<Result<Bytes, std::io::Error>>> {
                panic!("the body was read before the caller was checked")
            },
        );
        Request::new(axum::body::Body::from_stream(stream))
    }

    #[test]
    fn binary_body() {
        // The artifact body is declared as a binary string, not as an array of integers.
        let (_, doc) = router().split_for_parts();
        let doc = serde_json::to_value(doc).unwrap();
        let media = &doc["paths"]["/plugins"]["post"]["requestBody"]["content"]["application/wasm"];
        let schema = match media["schema"]["$ref"].as_str() {
            Some(reference) => &doc["components"]["schemas"][reference.rsplit('/').next().unwrap()],
            None => &media["schema"],
        };
        assert_eq!(
            (schema["type"].as_str(), schema["format"].as_str()),
            (Some("string"), Some("binary")),
            "{schema}"
        );
    }

    #[tokio::test]
    async fn checks_before_reading() {
        let (_dir, storage) = test_storage();
        let signing_key = generate_signing_key();
        let realm_id = RealmId::from_bytes(signing_key.verifying_key().to_bytes());
        let capabilities = NodeCapabilities::management_node(signing_key).unwrap();
        let node_id = iroh::SecretKey::generate().public();
        let context = std::sync::Arc::new(test_context(storage));
        let state = test_state(context, realm_id, node_id, capabilities).await;
        let anonymous = read_upload(&state, None, unread()).await;
        assert!(matches!(anonymous, Err(ServerError::Unauthorized)));
        let stranger = AuthContext {
            user_id: UserId::local(Ulid::generate(), realm_id),
            realm_id,
            path_restrictions: None,
            session: None,
        };
        assert!(read_upload(&state, Some(stranger), unread()).await.is_err());
    }

    fn plugin(name: &str, version: &str) -> PublishedPlugin {
        PublishedPlugin {
            name: name.into(),
            version: version.into(),
            digest: format!("sha256:{}", "ab".repeat(32)),
            size: 1,
            manifest: r#"{"title":"Example"}"#.into(),
            published_by: UserId::nil(RealmId::from_bytes([1u8; 32])),
            published_at_ms: 1,
        }
    }

    #[test]
    fn pages_entries() {
        let all = vec![
            plugin("a", "1.0.0"),
            plugin("a", "2.0.0"),
            plugin("b", "1.0.0"),
        ];
        let query = |name: Option<&str>, cursor: Option<&str>| PluginQuery {
            name: name.map(str::to_string),
            cursor: cursor.map(str::to_string),
            limit: None,
        };
        let first = page(all.clone(), &query(None, None), 2);
        let names: Vec<_> = first
            .plugins
            .iter()
            .map(|p| (p.name.as_str(), p.version.as_str()))
            .collect();
        assert_eq!(names, [("a", "1.0.0"), ("a", "2.0.0")]);
        assert_eq!(first.next.as_deref(), Some("a@2.0.0"));
        assert_eq!(first.plugins[0].title.as_deref(), Some("Example"));
        let second = page(all.clone(), &query(None, first.next.as_deref()), 2);
        assert_eq!(second.plugins.len(), 1);
        assert_eq!(second.next, None);
        let only_b = page(all, &query(Some("b"), None), 10);
        assert_eq!(only_b.plugins.len(), 1);
    }
}
