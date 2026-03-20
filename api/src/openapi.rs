use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::{Modify, OpenApi};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Aruna Server API",
        version = "0.1.0",
        description = "REST API for the Aruna federated data orchestration network.",
        license(name = "Apache-2.0", url = "https://www.apache.org/licenses/LICENSE-2.0"),
        contact(name = "Aruna Team", url = "https://github.com/aruna-storage/aruna")
    ),
    servers(
        (url = "/api/v1", description = "REST API v1"),
        (url = "/", description = "Admin API")
    ),
    tags(
        (name = "groups", description = "Group management operations"),
    ),
    paths(
        // REST API endpoints - Groups
        crate::routes::create_s3_credentials,
        crate::routes::replicate_blob,
        crate::routes::create_group,
        crate::routes::list_groups,
        crate::routes::get_group,
    ),
    modifiers(&SecurityAddon)
)]
pub struct ApiDoc;

/// Security configuration for REST/Admin OpenAPI.
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer_auth",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
        }
    }
}
