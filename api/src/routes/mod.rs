use crate::auth::auth_middleware;
use crate::server_state::ServerState;
use crate::telemetry::request_tracing_middleware;
use axum::Router;
use axum::middleware::from_fn_with_state;
use std::sync::Arc;
use utoipa::openapi::{Components, OpenApi};
use utoipa_axum::router::{OpenApiRouter, UtoipaMethodRouter};

pub mod audit;
pub mod blobs;
pub mod compute;
pub mod connectors;
pub mod credentials;
pub mod drs;
pub mod group_backends;
pub mod groups;
pub mod info;
pub mod job_audit;
pub mod jobs;
pub mod metadata;
pub mod notifications;
pub mod oai;
pub mod onboarding;
pub mod pid;
pub mod placement;
pub mod policies;
pub mod rocrate_import;
pub mod search;
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
        .merge(info::router())
        .merge(onboarding::router())
        .merge(blobs::router())
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
        .merge(tes::router())
        .merge(tokens::router())
        .merge(users::router())
}

pub fn rest_router(state: Arc<ServerState>) -> Router {
    let (router, _) = rest_api().split_for_parts();
    router
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
/// documented path. Only the DRS catch-all object id and the TES `:cancel`
/// action suffix need it; both are asserted by `preserves_route_inventory`.
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
        ("DELETE", "/admin/onboarding/secrets/{id}"),
        ("DELETE", "/admin/sync-quarantine"),
        ("DELETE", "/data/sync-relationships/{id}"),
        ("DELETE", "/groups/{group_id}/connectors/{connector_id}"),
        ("DELETE", "/groups/{group_id}/storage-backends/{backend_id}"),
        ("DELETE", "/groups/{id}/members/{user_id}"),
        ("DELETE", "/groups/{id}/roles/{role_id}"),
        ("DELETE", "/metadata/{document_id}"),
        ("DELETE", "/notifications/watches/{id}"),
        ("DELETE", "/pid/{document_id}"),
        ("DELETE", "/users/credentials/{access_key_id}"),
        ("GET", "/admin/compute/config"),
        ("GET", "/admin/compute/snapshots"),
        ("GET", "/admin/onboarding/secrets"),
        ("GET", "/admin/placement-diagnostics"),
        ("GET", "/admin/placement-policies/{policy_id}"),
        ("GET", "/admin/sync-quarantine"),
        ("GET", "/admin/sync-quarantine/{record_id}"),
        ("GET", "/audit"),
        ("GET", "/blobs/locations"),
        ("GET", "/buckets/{bucket}/placement"),
        ("GET", "/buckets/{bucket}/placement/coverage"),
        ("GET", "/buckets/{bucket}/storage-routing"),
        ("GET", "/data/sync-relationships"),
        ("GET", "/data/sync-relationships/{id}"),
        ("GET", "/ga4gh/drs/v1/download"),
        ("GET", "/ga4gh/drs/v1/objects/{*object_id}"),
        ("GET", "/ga4gh/drs/v1/service-info"),
        ("GET", "/ga4gh/tes/v1/service-info"),
        ("GET", "/ga4gh/tes/v1/tasks"),
        ("GET", "/ga4gh/tes/v1/tasks/{id}"),
        ("GET", "/groups"),
        ("GET", "/groups/{group_id}/connectors"),
        ("GET", "/groups/{group_id}/connectors/{connector_id}"),
        (
            "GET",
            "/groups/{group_id}/connectors/{connector_id}/entries",
        ),
        ("GET", "/groups/{group_id}/metadata"),
        ("GET", "/groups/{group_id}/metadata/path"),
        ("GET", "/groups/{group_id}/storage-backends"),
        ("GET", "/groups/{group_id}/storage-backends/{backend_id}"),
        (
            "GET",
            "/groups/{group_id}/storage-backends/{backend_id}/reclaim-status",
        ),
        ("GET", "/groups/{group_id}/storage-routing"),
        ("GET", "/groups/{id}"),
        ("GET", "/groups/{id}/data-paths"),
        ("GET", "/groups/{id}/members"),
        ("GET", "/groups/{id}/usage"),
        ("GET", "/info"),
        ("GET", "/info/realm"),
        ("GET", "/info/realm/placement"),
        ("GET", "/info/usage"),
        ("GET", "/jobs/"),
        ("GET", "/jobs/{job_id}"),
        ("GET", "/jobs/{job_id}/artifacts/rocrate"),
        ("GET", "/jobs/{job_id}/audit"),
        ("GET", "/jobs/{job_id}/report"),
        ("GET", "/metadata"),
        ("GET", "/metadata/profile-validation/capabilities"),
        ("GET", "/metadata/references"),
        ("GET", "/metadata/search"),
        ("GET", "/metadata/{document_id}"),
        ("GET", "/metadata/{document_id}/profile-validation"),
        ("GET", "/metadata/{document_id}/pids"),
        ("GET", "/metadata/{document_id}/rocrate"),
        ("GET", "/notifications"),
        ("GET", "/notifications/stream"),
        ("GET", "/notifications/unread"),
        ("GET", "/notifications/watches"),
        ("GET", "/oai"),
        ("GET", "/pid/{document_id}"),
        ("GET", "/profile/{document_id}"),
        ("GET", "/policies/effective"),
        ("GET", "/policies/group/{group_id}"),
        ("GET", "/policies/realm"),
        ("GET", "/search"),
        ("GET", "/search/buckets"),
        ("GET", "/search/objects"),
        ("GET", "/staging/jobs"),
        ("GET", "/staging/jobs/{job_id}"),
        ("GET", "/staging/references"),
        ("GET", "/users"),
        ("GET", "/users/credentials"),
        ("GET", "/users/info"),
        ("GET", "/users/search"),
        ("GET", "/users/token"),
        ("GET", "/users/{id}"),
        ("HEAD", "/jobs/{job_id}/artifacts/rocrate"),
        ("OPTIONS", "/ga4gh/drs/v1/objects/{*object_id}"),
        ("PATCH", "/data/sync-relationships/{id}"),
        ("PATCH", "/info/realm/placement"),
        ("PATCH", "/users/info"),
        ("PATCH", "/users/{id}"),
        ("POST", "/admin/compute/drain"),
        ("POST", "/admin/onboarding/secrets"),
        ("POST", "/admin/placement-policies"),
        ("POST", "/admin/placement-quarantine"),
        ("POST", "/admin/sync-quarantine/{record_id}/acknowledge"),
        ("POST", "/blobs/replicate"),
        ("POST", "/buckets/{bucket}/placement/objects"),
        ("POST", "/buckets/{bucket}/placement/runs"),
        ("POST", "/data/sync-relationships"),
        ("POST", "/data/sync-relationships/{id}/run"),
        ("POST", "/ga4gh/drs/v1/objects"),
        ("POST", "/ga4gh/tes/v1/tasks"),
        ("POST", "/ga4gh/tes/v1/tasks/{id}"),
        ("POST", "/groups"),
        ("POST", "/groups/{group_id}/connectors"),
        ("POST", "/groups/{group_id}/connectors/check"),
        ("POST", "/groups/{group_id}/connectors/{connector_id}/check"),
        ("POST", "/groups/{group_id}/storage-backends"),
        (
            "POST",
            "/groups/{group_id}/storage-backends/{backend_id}/enable",
        ),
        ("POST", "/groups/{id}/leave"),
        ("POST", "/groups/{id}/members"),
        ("POST", "/groups/{id}/roles"),
        ("POST", "/jobs/"),
        ("POST", "/jobs/{job_id}/cancel"),
        ("POST", "/metadata"),
        ("POST", "/metadata/profile-validation/preview"),
        ("POST", "/metadata/references/preflight"),
        ("POST", "/metadata/rocrate/imports"),
        ("POST", "/metadata/rocrate/uploads"),
        ("POST", "/metadata/sparql/query"),
        (
            "POST",
            "/metadata/{document_id}/rocrate/contextual-entities",
        ),
        ("POST", "/metadata/{document_id}/rocrate/data-entities"),
        ("POST", "/metadata/{document_id}/rocrate/exports"),
        (
            "POST",
            "/metadata/{document_id}/profile-validation/revalidate",
        ),
        ("POST", "/metadata/{document_id}/sparql/query"),
        ("POST", "/notifications/read"),
        ("POST", "/notifications/watches"),
        ("POST", "/oai"),
        ("POST", "/onboarding/bootstrap"),
        ("POST", "/pid/{document_id}"),
        ("POST", "/policies/dry-run"),
        ("POST", "/policies/validate"),
        ("POST", "/staging/"),
        ("POST", "/storage/deletion-preflight"),
        ("POST", "/storage/purge-jobs"),
        ("POST", "/staging/batch"),
        ("POST", "/staging/jobs"),
        ("POST", "/users/credentials"),
        ("POST", "/users/register"),
        ("POST", "/users/resolve"),
        ("POST", "/users/s3-sessions"),
        ("POST", "/users/s3-sessions/{access_key_id}/refresh"),
        ("POST", "/users/tokens/revoke"),
        ("PUT", "/admin/compute/config"),
        ("PUT", "/buckets/{bucket}/placement"),
        ("PUT", "/buckets/{bucket}/storage-routing"),
        ("PUT", "/groups/{group_id}/connectors/{connector_id}"),
        ("PUT", "/groups/{group_id}/storage-backends/{backend_id}"),
        ("PUT", "/groups/{group_id}/storage-routing"),
        ("PUT", "/info/realm/quota"),
        ("PUT", "/metadata/{document_id}/rocrate"),
        ("PUT", "/policies/group/{group_id}"),
        ("PUT", "/policies/realm"),
    ];

    /// Documented path to the runtime template that serves it. DRS object ids
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
