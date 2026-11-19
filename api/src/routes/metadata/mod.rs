//! Groups the metadata REST routes and hands each request to the shared metadata adapter.
//! Every MCP-facing mapper lives in that adapter, so MCP never reaches into a handler.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

pub mod documents;
pub(crate) mod harvests;
pub(crate) mod links;
pub mod query;
pub mod references;
pub(crate) mod repositories;
pub mod rocrate;
pub mod validation;

#[cfg(test)]
pub(crate) mod tests;

use crate::server::state::ServerState;
use std::sync::Arc;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(components(schemas(crate::metadata::MetadataRoCrateView)))]
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
        .routes(routes!(links::data_links))
        .routes(routes!(links::bucket_links))
        .routes(routes!(links::file_part_of))
        .routes(routes!(links::part_target))
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
            rocrate::negotiated_rocrate,
            rocrate::replace_metadata_rocrate
        ))
        .routes(routes!(rocrate::submit_rocrate_export))
        .routes(routes!(
            rocrate::add_data_entity,
            rocrate::remove_data_entity
        ))
        .routes(routes!(rocrate::add_contextual_entity))
        .routes(routes!(query::query_metadata_document))
        .routes(routes!(
            repositories::create_repository,
            repositories::list_repositories
        ))
        .routes(routes!(
            repositories::get_repository,
            repositories::replace_repository,
            repositories::delete_repository
        ))
        .routes(routes!(harvests::submit_harvest))
}
