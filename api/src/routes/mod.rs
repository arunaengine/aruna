use crate::auth::auth_middleware;
use crate::server_state::ServerState;
use crate::telemetry::request_tracing_middleware;
use axum::Router;
use axum::middleware::from_fn_with_state;
use std::sync::Arc;
use utoipa::openapi::{Components, OpenApi};
use utoipa_axum::router::{OpenApiRouter, UtoipaMethodRouter};

pub mod assistant;
pub mod audit;
pub mod blobs;
pub mod bucket_usage;
pub mod compute;
pub mod connectors;
pub mod credentials;
pub mod device;
pub mod device_compute;
pub mod drs;
pub mod group_backends;
pub mod groups;
pub mod info;
pub mod job_audit;
pub mod jobs;
pub mod management_relay;
pub mod metadata;
pub mod notifications;
pub mod oai;
pub mod onboarding;
pub mod pid;
pub mod placement;
pub mod policies;
pub mod rocrate_import;
pub mod search;
pub mod sessions;
pub mod staging;
pub mod storage_deletion;
pub mod storage_routing;
pub mod sync;
pub mod sync_quarantine;
pub mod tes;
pub mod tokens;
pub mod users;

/// The single REST source: every route is registered from a `#[utoipa::path]`
/// handler, so the runtime router and the generated document cannot diverge.
fn rest_api() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::new()
        .merge(audit::router())
        .merge(assistant::router())
        .merge(info::router())
        .merge(onboarding::router())
        .merge(blobs::router())
        .merge(bucket_usage::router())
        .merge(drs::router())
        .merge(staging::router())
        .merge(storage_deletion::router())
        .merge(group_backends::router())
        .merge(storage_routing::router())
        .merge(sync::router())
        .merge(sync_quarantine::router())
        .merge(compute::router())
        .merge(connectors::router())
        .merge(credentials::router())
        .merge(device::router())
        .merge(device_compute::router())
        .merge(groups::router())
        .merge(jobs::router())
        .merge(job_audit::router())
        .merge(metadata::router())
        .merge(oai::router())
        .merge(pid::router())
        .merge(placement::router())
        .merge(rocrate_import::router())
        .merge(notifications::router())
        .merge(policies::router())
        .merge(search::router())
        .merge(sessions::router())
        .merge(tes::router())
        .merge(tokens::router())
        .merge(users::router())
}

pub fn rest_router(state: Arc<ServerState>) -> Router {
    let (router, _) = rest_api().split_for_parts();
    router
        .layer(from_fn_with_state(
            state.clone(),
            management_relay::relay_middleware,
        ))
        .layer(from_fn_with_state(
            state.clone(),
            crate::rate_limit::principal_middleware,
        ))
        .layer(from_fn_with_state(state.clone(), auth_middleware))
        .layer(from_fn_with_state(
            state.clone(),
            request_tracing_middleware,
        ))
        .with_state(state)
}

/// The co-registered REST operations, merged into `ApiDoc` and served by Swagger UI.
pub fn rest_openapi() -> OpenApi {
    rest_api().into_openapi()
}

/// Registers documented handlers under a runtime template that differs from the
/// documented path. Catch-all ids and the TES `:cancel` suffix use it; all are
/// asserted by `preserves_route_inventory`.
fn routes_at(
    mut router: OpenApiRouter<Arc<ServerState>>,
    path: &str,
    (schemas, paths, method_router): UtoipaMethodRouter<Arc<ServerState>>,
) -> OpenApiRouter<Arc<ServerState>> {
    let api = router.get_openapi_mut();
    for (documented, item) in paths.paths {
        match api.paths.paths.get_mut(&documented) {
            Some(existing) => existing.merge_operations(item),
            None => {
                api.paths.paths.insert(documented, item);
            }
        }
    }
    api.components
        .get_or_insert_with(Components::new)
        .schemas
        .extend(schemas);
    router.route(path, method_router)
}

#[cfg(test)]
mod tests {
    use super::rest_openapi;
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};

    /// Runtime method/path pairs registered before REST/OpenAPI co-registration.
    /// A route added or removed without this fixture changing is a regression.
    const RUNTIME_ROUTES: &[(&str, &str)] = &[
        ("DELETE", "/access/devices/{node_id}"),
        ("DELETE", "/access/onboarding/secrets/{id}"),
        ("DELETE", "/data/sync/quarantine"),
        ("DELETE", "/data/sync/relationships/{id}"),
        ("DELETE", "/device/drafts/{draft_id}"),
        ("DELETE", "/device/folders/{folder_id}"),
        (
            "DELETE",
            "/data/groups/{group_id}/connectors/{connector_id}",
        ),
        (
            "DELETE",
            "/data/groups/{group_id}/storage/backends/{backend_id}",
        ),
        ("DELETE", "/access/groups/{id}/members/{user_id}"),
        ("DELETE", "/access/groups/{id}/roles/{role_id}"),
        ("DELETE", "/metadata/{document_id}"),
        ("DELETE", "/system/notifications/watches/{id}"),
        ("DELETE", "/pid/{document_id}"),
        ("DELETE", "/access/credentials/{access_key_id}"),
        ("DELETE", "/system/assistant/providers/{id}"),
        ("DELETE", "/access/users/me/devices/{id}"),
        ("DELETE", "/access/sessions/{session_id}"),
        ("DELETE", "/access/s3/sessions/{access_key_id}"),
        ("GET", "/compute/config"),
        ("GET", "/compute/snapshots"),
        ("GET", "/access/onboarding/secrets"),
        ("GET", "/data/placement/diagnostics"),
        ("GET", "/data/placement/policies"),
        ("GET", "/data/placement/policies/{policy_id}"),
        ("GET", "/data/sync/quarantine"),
        ("GET", "/data/sync/quarantine/{record_id}"),
        ("GET", "/metadata/audit"),
        ("GET", "/data/blobs/locations"),
        ("GET", "/data/buckets/{bucket}/placement"),
        ("GET", "/data/buckets/{bucket}/placement/coverage"),
        ("GET", "/data/buckets/{bucket}/placement/objects"),
        ("GET", "/data/buckets/{bucket}/storage/routing"),
        ("GET", "/data/buckets/{bucket}/usage"),
        ("GET", "/data/sync/relationships"),
        ("GET", "/data/sync/relationships/{id}"),
        ("GET", "/device/compute"),
        ("GET", "/device/documents"),
        ("GET", "/device/drafts"),
        ("GET", "/device/drafts/{draft_id}"),
        ("GET", "/device/folders"),
        ("GET", "/device/folders/{folder_id}"),
        ("GET", "/device/folders/{folder_id}/actions"),
        ("GET", "/device/folders/{folder_id}/entries"),
        ("GET", "/device/sync/status"),
        ("GET", "/device/transfers"),
        ("GET", "/ga4gh/drs/v1/download"),
        ("GET", "/ga4gh/drs/v1/objects/{*object_id}"),
        ("GET", "/ga4gh/drs/v1/service-info"),
        ("GET", "/ga4gh/tes/v1/service-info"),
        ("GET", "/ga4gh/tes/v1/tasks"),
        ("GET", "/ga4gh/tes/v1/tasks/{id}"),
        ("GET", "/access/groups"),
        ("GET", "/data/groups/{group_id}/connectors"),
        ("GET", "/data/groups/{group_id}/connectors/{connector_id}"),
        (
            "GET",
            "/data/groups/{group_id}/connectors/{connector_id}/entries",
        ),
        ("GET", "/metadata/groups/{group_id}"),
        ("GET", "/metadata/groups/{group_id}/path"),
        ("GET", "/data/groups/{group_id}/storage/backends"),
        (
            "GET",
            "/data/groups/{group_id}/storage/backends/{backend_id}",
        ),
        (
            "GET",
            "/data/groups/{group_id}/storage/backends/{backend_id}/reclaim/status",
        ),
        ("GET", "/data/groups/{group_id}/storage/routing"),
        ("GET", "/access/groups/{id}"),
        ("GET", "/access/groups/{id}/data/paths"),
        ("GET", "/access/groups/{id}/members"),
        ("GET", "/access/groups/{id}/usage"),
        ("PATCH", "/access/groups/{id}"),
        ("GET", "/system/info"),
        ("GET", "/system/realm"),
        ("GET", "/system/realm/placement"),
        ("GET", "/system/usage"),
        ("GET", "/compute/jobs"),
        ("GET", "/compute/jobs/{job_id}"),
        ("GET", "/compute/jobs/{job_id}/artifacts/rocrate"),
        ("GET", "/compute/jobs/{job_id}/audit"),
        ("GET", "/compute/jobs/{job_id}/report"),
        ("GET", "/metadata"),
        ("GET", "/metadata/profile/validation/capabilities"),
        ("GET", "/metadata/references"),
        ("GET", "/metadata/search"),
        ("GET", "/metadata/{document_id}"),
        ("GET", "/metadata/{document_id}/profile/validation"),
        ("GET", "/metadata/{document_id}/pids"),
        ("GET", "/metadata/{document_id}/rocrate"),
        ("GET", "/system/notifications"),
        ("GET", "/system/notifications/stream"),
        ("GET", "/system/notifications/unread"),
        ("GET", "/system/notifications/watches"),
        ("GET", "/oai"),
        ("GET", "/access/onboarding/secrets/{id}/status"),
        ("GET", "/pid/{document_id}"),
        ("GET", "/profile/{document_id}"),
        ("GET", "/access/policies/effective"),
        ("GET", "/access/policies/group/{group_id}"),
        ("GET", "/access/policies/realm"),
        ("GET", "/search"),
        ("GET", "/search/buckets"),
        ("GET", "/search/objects"),
        ("GET", "/data/staging/jobs"),
        ("GET", "/data/staging/jobs/{job_id}"),
        ("GET", "/data/staging/references"),
        ("GET", "/access/users"),
        ("GET", "/system/assistant/providers"),
        ("GET", "/system/assistant/providers/{id}/models"),
        ("GET", "/system/assistant/providers/{id}/proxy/{*path}"),
        ("GET", "/access/credentials"),
        ("GET", "/access/users/me"),
        ("GET", "/access/users/me/devices"),
        ("GET", "/access/users/search"),
        ("GET", "/access/sessions"),
        ("GET", "/access/s3/sessions"),
        ("GET", "/access/token"),
        ("GET", "/access/users/{id}"),
        ("HEAD", "/compute/jobs/{job_id}/artifacts/rocrate"),
        ("OPTIONS", "/ga4gh/drs/v1/objects/{*object_id}"),
        ("PATCH", "/data/sync/relationships/{id}"),
        ("PATCH", "/system/realm/placement"),
        ("PATCH", "/access/users/me"),
        ("PATCH", "/system/assistant/providers/{id}"),
        ("PATCH", "/access/users/{id}"),
        ("POST", "/compute/drain"),
        ("POST", "/access/onboarding/secrets"),
        ("POST", "/data/placement/policies"),
        ("POST", "/data/placement/quarantine"),
        ("POST", "/data/sync/quarantine/{record_id}/acknowledge"),
        ("POST", "/data/blobs/replicate"),
        ("POST", "/data/buckets/{bucket}/placement/objects"),
        ("POST", "/data/buckets/{bucket}/placement/runs"),
        ("POST", "/data/sync/relationships"),
        ("POST", "/data/sync/relationships/{id}/run"),
        ("POST", "/device/drafts"),
        ("POST", "/device/drafts/preview"),
        ("POST", "/device/folders"),
        ("POST", "/device/folders/{folder_id}/actions"),
        ("POST", "/device/folders/{folder_id}/entries/{path}/actions"),
        ("POST", "/device/folders/{folder_id}/pause"),
        ("POST", "/device/folders/{folder_id}/resume"),
        ("POST", "/device/folders/{folder_id}/sync"),
        ("POST", "/device/sync/run"),
        ("POST", "/device/wipe"),
        ("POST", "/ga4gh/drs/v1/objects"),
        ("POST", "/ga4gh/tes/v1/tasks"),
        ("POST", "/ga4gh/tes/v1/tasks/{id}"),
        ("POST", "/access/groups"),
        ("POST", "/data/groups/{group_id}/connectors"),
        ("POST", "/data/groups/{group_id}/connectors/check"),
        (
            "POST",
            "/data/groups/{group_id}/connectors/{connector_id}/check",
        ),
        ("POST", "/data/groups/{group_id}/storage/backends"),
        (
            "POST",
            "/data/groups/{group_id}/storage/backends/{backend_id}/enable",
        ),
        ("POST", "/access/groups/{id}/leave"),
        ("POST", "/access/groups/{id}/members"),
        ("POST", "/access/groups/{id}/roles"),
        ("POST", "/compute/jobs"),
        ("POST", "/compute/jobs/{job_id}/cancel"),
        ("POST", "/metadata"),
        ("POST", "/metadata/profile/validation/preview"),
        ("POST", "/metadata/references/preflight"),
        ("POST", "/metadata/rocrate/imports"),
        ("POST", "/metadata/rocrate/uploads"),
        ("POST", "/metadata/sparql/query"),
        (
            "POST",
            "/metadata/{document_id}/rocrate/entities/contextual",
        ),
        ("POST", "/metadata/{document_id}/rocrate/entities/data"),
        ("POST", "/metadata/{document_id}/rocrate/exports"),
        (
            "POST",
            "/metadata/{document_id}/profile/validation/revalidate",
        ),
        ("POST", "/metadata/{document_id}/sparql/query"),
        ("POST", "/system/notifications/read"),
        ("POST", "/system/notifications/watches"),
        ("POST", "/oai"),
        ("POST", "/access/onboarding/bootstrap"),
        ("POST", "/access/policies/dryrun"),
        ("POST", "/access/policies/validate"),
        ("POST", "/data/staging"),
        ("POST", "/data/storage/deletion/preflight"),
        ("POST", "/data/storage/purge/jobs"),
        ("POST", "/data/staging/batch"),
        ("POST", "/data/staging/jobs"),
        ("POST", "/access/credentials"),
        ("POST", "/access/users/register"),
        ("POST", "/system/assistant/providers"),
        ("POST", "/system/assistant/providers/chatgpt/login"),
        ("POST", "/system/assistant/providers/{id}/login/poll"),
        ("POST", "/system/assistant/providers/{id}/proxy/{*path}"),
        ("POST", "/system/assistant/providers/{id}/test"),
        ("POST", "/access/sessions"),
        ("POST", "/access/users/resolve"),
        ("POST", "/access/s3/sessions"),
        ("POST", "/access/s3/sessions/{access_key_id}/refresh"),
        ("POST", "/access/tokens/revoke"),
        ("PUT", "/compute/config"),
        ("PUT", "/data/buckets/{bucket}/placement"),
        ("PUT", "/data/buckets/{bucket}/storage/routing"),
        ("PUT", "/device/documents/{document_id}/selection"),
        ("PUT", "/data/groups/{group_id}/connectors/{connector_id}"),
        (
            "PUT",
            "/data/groups/{group_id}/storage/backends/{backend_id}",
        ),
        ("PUT", "/data/groups/{group_id}/storage/routing"),
        ("PUT", "/system/realm/quota"),
        ("PUT", "/metadata/{document_id}/rocrate"),
        ("PUT", "/access/policies/group/{group_id}"),
        ("PUT", "/access/policies/realm"),
    ];

    /// Documented path to the runtime template that serves it. Catch-all ids
    /// span segments and TES cancel carries its action suffix inside `{id}`.
    const PATH_ALIASES: &[(&str, &str)] = &[
        (
            "/ga4gh/drs/v1/objects/{object_id}",
            "/ga4gh/drs/v1/objects/{*object_id}",
        ),
        (
            "/ga4gh/tes/v1/tasks/{id}:cancel",
            "/ga4gh/tes/v1/tasks/{id}",
        ),
        (
            "/system/assistant/providers/{id}/proxy/{path}",
            "/system/assistant/providers/{id}/proxy/{*path}",
        ),
    ];

    fn documented_routes() -> BTreeSet<(String, String)> {
        let mut routes = BTreeSet::new();
        for (documented, item) in rest_openapi().paths.paths {
            let path = PATH_ALIASES
                .iter()
                .find(|(alias, _)| *alias == documented)
                .map(|(_, runtime)| (*runtime).to_owned())
                .unwrap_or(documented);
            for (method, operation) in [
                ("DELETE", &item.delete),
                ("GET", &item.get),
                ("HEAD", &item.head),
                ("OPTIONS", &item.options),
                ("PATCH", &item.patch),
                ("POST", &item.post),
                ("PUT", &item.put),
                ("TRACE", &item.trace),
            ] {
                if operation.is_some() {
                    routes.insert((method.to_owned(), path.clone()));
                }
            }
        }
        routes
    }

    fn source_files(dir: &Path, files: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("readable route directory") {
            let path = entry.expect("readable route entry").path();
            if path.is_dir() {
                source_files(&path, files);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }

    fn raw_gaps(file: &Path, source: &str) -> Vec<&'static str> {
        let source = source.split("#[cfg(test)]").next().unwrap_or_default();
        [".route(", ".route_service(", ".nest(", ".nest_service("]
            .into_iter()
            .filter(|form| {
                let allowed_line = if file.file_name().is_some_and(|name| name == "mod.rs")
                    && *form == ".route("
                {
                    Some("router.route(path, method_router)")
                } else if file.file_name().is_some_and(|name| name == "server.rs")
                    && *form == ".nest("
                {
                    Some(".nest(\"/api/v1\", api_v1)")
                } else {
                    None
                };
                if let Some(allowed_line) = allowed_line {
                    let mut allowed = false;
                    return source
                        .lines()
                        .filter(|line| line.contains(*form))
                        .any(|line| {
                            if !allowed && line.trim() == allowed_line {
                                allowed = true;
                                false
                            } else {
                                true
                            }
                        });
                }
                source.contains(*form)
            })
            .collect()
    }

    #[test]
    fn preserves_route_inventory() {
        let expected = RUNTIME_ROUTES
            .iter()
            .map(|(method, path)| ((*method).to_owned(), (*path).to_owned()))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            documented_routes(),
            expected,
            "co-registered routes must match the runtime inventory exactly"
        );
    }

    #[test]
    fn forbids_raw_routes() {
        // Only routes_at and the root /api/v1 nest may reach Axum path assembly;
        // a REST route added any other way would lack a generated operation.
        let mut files = Vec::new();
        let source_root = Path::new(env!("CARGO_MANIFEST_DIR"));
        source_files(&source_root.join("src/routes"), &mut files);
        files.push(source_root.join("src/server.rs"));
        assert!(files.len() > 1, "route modules must be discoverable");
        for file in files {
            let source = std::fs::read_to_string(&file).expect("readable route module");
            if let Some(form) = raw_gaps(&file, &source).first() {
                panic!(
                    "{} registers {form} outside routes!; use routes! or routes_at",
                    file.display()
                );
            }
        }
    }

    #[test]
    fn rejects_raw_fixture() {
        let source = "let router = Router::new().route(\"/health\", get(handler));";
        assert!(!raw_gaps(Path::new("server.rs"), source).is_empty());
    }
}
