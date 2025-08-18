use crate::api_json::paths::*;
use crate::controller::controller::Controller;
use aruna_storage::storage::store::Store;
use utoipa::{
    Modify, OpenApi,
    openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme},
};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(modifiers(&SecurityAddon))]
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

pub fn router<St>(store: Controller<St>) -> OpenApiRouter
where
    for<'a> St: Store<'a> + 'static,
{
    OpenApiRouter::new()
        .routes(routes!(create_s3_credentials))
        .routes(routes!(get_s3_credentials))
        .routes(routes!(delete_s3_credentials))
        .routes(routes!(register_data))
        .with_state(store)
}
