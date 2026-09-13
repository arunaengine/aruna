//! Metadata REST routes, grouped by family. Each handler converts one request
//! family into a call on the shared `crate::metadata` adapter; the adapter and
//! every MCP-facing mapper live there, so MCP never reaches into a handler.

pub(crate) mod documents;
pub(crate) mod query;
pub(crate) mod references;
pub(crate) mod rocrate;
pub(crate) mod validation;

#[cfg(test)]
mod tests;

use crate::server_state::ServerState;
use std::sync::Arc;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "metadata/documents", description = "Metadata document lifecycle"),
        (name = "metadata/query", description = "Metadata search and SPARQL"),
        (name = "metadata/references", description = "Metadata reference resolution"),
        (name = "metadata/rocrate", description = "RO-Crate document operations"),
        (name = "metadata/validation", description = "Metadata profile validation")
    ),
    components(schemas(crate::metadata::MetadataRoCrateView))
)]
pub struct MetadataApiDoc;

pub fn router() -> OpenApiRouter<Arc<ServerState>> {
    OpenApiRouter::with_openapi(MetadataApiDoc::openapi())
        .routes(routes!(
            documents::list_all_documents,
            documents::create_metadata_document
        ))
        .routes(routes!(query::search_metadata))
        .routes(routes!(references::metadata_references))
        .routes(routes!(references::metadata_reference_preflight))
        .routes(routes!(query::query_all_metadata))
        .routes(routes!(documents::list_metadata_documents))
        .routes(routes!(documents::get_metadata_path))
        .routes(routes!(
            documents::get_metadata_document,
            documents::delete_metadata_document
        ))
        .routes(routes!(validation::profile_validation_capabilities))
        .routes(routes!(validation::preview_profile_validation))
        .routes(routes!(validation::get_validation_status))
        .routes(routes!(validation::revalidate_profile))
        .routes(routes!(
            rocrate::export_metadata_rocrate,
            rocrate::replace_metadata_rocrate
        ))
        .routes(routes!(rocrate::submit_rocrate_export))
        .routes(routes!(rocrate::add_data_entity))
        .routes(routes!(rocrate::add_contextual_entity))
        .routes(routes!(query::query_metadata_document))
}
