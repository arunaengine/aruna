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
        openapi.merge(crate::routes::audit::AuditApiDoc::openapi());
        openapi.merge(crate::routes::groups::GroupsApiDoc::openapi());
        openapi.merge(crate::routes::connectors::ConnectorsApiDoc::openapi());
        openapi.merge(crate::routes::staging::StagingApiDoc::openapi());
        openapi.merge(crate::routes::group_backends::GroupBackendsApiDoc::openapi());
        openapi.merge(crate::routes::storage_routing::StorageRoutingApiDoc::openapi());
        openapi.merge(crate::routes::sync::SyncApiDoc::openapi());
        openapi.merge(crate::routes::metadata::MetadataApiDoc::openapi());
        openapi.merge(crate::routes::rocrate_import::RoCrateImportApiDoc::openapi());
        openapi.merge(crate::routes::credentials::CredentialsApiDoc::openapi());
        openapi.merge(crate::routes::blobs::BlobsApiDoc::openapi());
        openapi.merge(crate::routes::drs::DrsApiDoc::openapi());
        openapi.merge(crate::routes::info::InfoApiDoc::openapi());
        openapi.merge(crate::routes::jobs::JobsApiDoc::openapi());
        openapi.merge(crate::routes::notifications::NotificationsApiDoc::openapi());
        openapi.merge(crate::routes::onboarding::OnboardingApiDoc::openapi());
        openapi.merge(crate::routes::policies::PoliciesApiDoc::openapi());
        openapi.merge(crate::routes::search::SearchApiDoc::openapi());
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
            components.add_security_scheme(
                "basic_auth",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Basic)),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ApiDoc;
    use serde_json::{Value, json};

    /// Operations that serve anonymous callers but return more to an authenticated one.
    /// They pair an empty requirement with `bearer_auth` so clients send an available
    /// token without presenting the operation as authentication-only.
    const OPTIONAL_AUTH: &[(&str, &str)] = &[
        ("/metadata", "get"),
        ("/groups/{group_id}/metadata", "get"),
        ("/metadata/{document_id}", "get"),
        ("/metadata/{document_id}/rocrate", "get"),
        ("/metadata/{document_id}/sparql/query", "post"),
        ("/metadata/sparql/query", "post"),
        ("/metadata/search", "get"),
        ("/info", "get"),
        ("/info/realm", "get"),
        ("/ga4gh/drs/v1/objects", "post"),
        ("/ga4gh/drs/v1/objects/{object_id}", "get"),
        ("/ga4gh/drs/v1/download", "get"),
    ];

    /// Operations that reject an anonymous caller.
    const REQUIRED_AUTH: &[(&str, &str)] = &[
        ("/users/register", "post"),
        ("/metadata", "post"),
        ("/metadata/{document_id}", "delete"),
        ("/metadata/references", "get"),
        ("/audit", "get"),
    ];

    fn operation_security(doc: &Value, path: &str, method: &str) -> Vec<Value> {
        doc["paths"][path][method]["security"]
            .as_array()
            .unwrap_or_else(|| panic!("{method} {path} declares no security requirement"))
            .clone()
    }

    #[test]
    fn pins_optional_security() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        for (path, method) in OPTIONAL_AUTH {
            let security = operation_security(&doc, path, method);
            assert!(
                security.contains(&json!({ "bearer_auth": [] })),
                "{method} {path} must offer bearer_auth so clients send a held token"
            );
            assert!(
                security.contains(&json!({})),
                "{method} {path} must keep the empty requirement that marks auth optional"
            );
        }
    }

    #[test]
    fn pins_required_security() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        for (path, method) in REQUIRED_AUTH {
            let security = operation_security(&doc, path, method);
            assert!(
                security.contains(&json!({ "bearer_auth": [] })),
                "{method} {path} must require bearer_auth"
            );
            assert!(
                !security.contains(&json!({})),
                "{method} {path} rejects anonymous callers, so auth must not read as optional"
            );
        }
    }

    #[test]
    fn declares_security_schemes() {
        let doc = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let schemes = &doc["components"]["securitySchemes"];
        assert_eq!(schemes["bearer_auth"]["scheme"], json!("bearer"));
        assert_eq!(schemes["basic_auth"]["scheme"], json!("basic"));
    }
}
