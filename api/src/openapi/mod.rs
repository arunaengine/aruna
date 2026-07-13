use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};
use utoipa::{Modify, OpenApi};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Aruna Server API",
        version = "3.0.0-alpha.1",
        description = "REST API for the Aruna federated data orchestration network",
        license(name = "Apache-2.0", url = "https://www.apache.org/licenses/LICENSE-2.0"),
        contact(name = "Aruna Team", url = "https://github.com/arunaengine/aruna")
    ),
    servers(
        (url = "/api/v1", description = "REST API v1"),
        (url = "/", description = "Admin API")
    ),
    modifiers(&SecurityAddon)
)]
struct BaseApiDoc;

pub struct ApiDoc;

impl ApiDoc {
    pub fn openapi() -> utoipa::openapi::OpenApi {
        let mut openapi = BaseApiDoc::openapi();
        openapi.merge(crate::routes::groups::GroupsApiDoc::openapi());
        openapi.merge(crate::routes::connectors::ConnectorsApiDoc::openapi());
        openapi.merge(crate::routes::staging::StagingApiDoc::openapi());
        openapi.merge(crate::routes::metadata::MetadataApiDoc::openapi());
        openapi.merge(crate::routes::credentials::CredentialsApiDoc::openapi());
        openapi.merge(crate::routes::blobs::BlobsApiDoc::openapi());
        openapi.merge(crate::routes::drs::DrsApiDoc::openapi());
        openapi.merge(crate::routes::info::InfoApiDoc::openapi());
        openapi.merge(crate::routes::jobs::JobsApiDoc::openapi());
        openapi.merge(crate::routes::notifications::NotificationsApiDoc::openapi());
        openapi.merge(crate::routes::onboarding::OnboardingApiDoc::openapi());
        openapi.merge(crate::routes::tes::TesApiDoc::openapi());
        openapi.merge(crate::routes::users::UsersApiDoc::openapi());
        openapi
    }
}

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
