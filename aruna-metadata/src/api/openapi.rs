use crate::{
    api::paths::*,
    models::models::{Direction, Permission},
    network::network_trait::Network,
    persistence::search::search::Search,
    transactions::controller::Controller,
};
use aruna_storage::storage::store::Store;
use std::sync::Arc;
use utoipa::{
    Modify, OpenApi,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    components(schemas(Permission, Direction))
)]
pub struct ArunaApi;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            let security_scheme = HttpBuilder::new()
                .scheme(HttpAuthScheme::Bearer)
                .bearer_format("JWT")
                .description(Some("Either an OIDC, Aruna oder Component signed JWT"))
                .build();
            components.add_security_scheme("auth", SecurityScheme::Http(security_scheme));
        }
    }
}

pub fn router<St, Se, N>(store: Arc<Controller<St, Se, N>>) -> OpenApiRouter
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
    N: Network + 'static,
{
    OpenApiRouter::new()
        .routes(routes!(create_resource))
        .routes(routes!(get_resource))
        .routes(routes!(update_resource_name))
        .routes(routes!(update_resource_title))
        .routes(routes!(update_resource_description))
        .routes(routes!(update_resource_visibility))
        .routes(routes!(update_resource_license))
        .routes(routes!(update_resource_labels))
        .routes(routes!(update_resource_identifiers))
        .routes(routes!(update_resource_authors))
        .routes(routes!(search))
        .routes(routes!(add_user))
        .with_state(store)
}
