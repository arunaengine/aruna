use std::sync::Arc;
use aruna_core::structs::RealmId;
use aruna_operations::driver::DriverContext;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
use crate::openapi::ApiDoc;

#[derive(Clone, Debug)]
pub struct ServerState {
    driver_ctx: Arc<DriverContext>,
    realm_pubkey: Option<RealmId>,
    realm_keypair: Option<[u8; 64]>,
    realm_id: Option<RealmId>,
    oidc_validator: Option<Arc<OidcValidator>>,
}

#[derive(Debug)]
pub struct OidcValidator {}

#[derive(Debug, Clone)]
pub struct AuthContext {}

/// Create the SwaggerUI router for API documentation.
///
/// Provides two separate OpenAPI specs:
/// - `/api-docs/openapi.json` - REST & Admin API
/// - `/api-docs/s3-openapi.json` - S3-compatible API
pub fn swagger_ui() -> SwaggerUi {
    SwaggerUi::new("/swagger-ui")
        .url("/api-docs/openapi.json", ApiDoc::openapi())
}
