use crate::auth::OidcValidator;
use crate::openapi::ApiDoc;
use aruna_core::structs::RealmId;
use aruna_operations::driver::DriverContext;
use std::sync::Arc;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone, Debug)]
pub struct ServerState {
    driver_ctx: Arc<DriverContext>,
    realm_keypair: Option<[u8; 64]>,
    realm_id: Option<RealmId>,
    oidc_validator: Option<Arc<OidcValidator>>,
}

impl ServerState {
    pub fn new(
        driver_ctx: Arc<DriverContext>,
        realm_keypair: Option<[u8; 64]>,
        realm_id: Option<RealmId>,
        oidc_validator: Option<Arc<OidcValidator>>,
    ) -> Self {
        Self {
            driver_ctx,
            realm_keypair,
            realm_id,
            oidc_validator,
        }
    }
    pub fn get_ctx(&self) -> Arc<DriverContext> {
        self.driver_ctx.clone()
    }
    pub fn get_keypair(&self) -> Option<[u8; 64]> {
        self.realm_keypair
    }
}

/// Create the SwaggerUI router for API documentation.
///
/// Provides two separate OpenAPI specs:
/// - `/api-docs/openapi.json` - REST & Admin API
/// - `/api-docs/s3-openapi.json` - S3-compatible API
pub fn swagger_ui() -> SwaggerUi {
    SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi())
}
