use crate::auth::OidcValidator;
use crate::error::TokenError;
use crate::openapi::ApiDoc;
use aruna_core::structs::{NodeCapabilities, RealmId};
use aruna_operations::driver::DriverContext;
use base64::Engine;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use jsonwebtoken::DecodingKey;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone, Debug)]
pub struct ServerState {
    driver_ctx: Arc<DriverContext>,
    node_capabilities: NodeCapabilities,
    // Base64 encoded issuer pubkeys and jsonwebtoken serialized DecodingKeys
    issuer_keys: Arc<RwLock<HashMap<String, DecodingKey, ahash::RandomState>>>,
    realm_id: RealmId,
    oidc_validator: Option<Arc<OidcValidator>>,
}

impl ServerState {
    pub fn new(
        driver_ctx: Arc<DriverContext>,
        realm_id: RealmId,
        node_capabilities: NodeCapabilities,
        oidc_validator: Option<Arc<OidcValidator>>,
    ) -> Self {
        Self {
            driver_ctx,
            realm_id,
            oidc_validator,
            node_capabilities,
            issuer_keys: Arc::new(RwLock::new(HashMap::default())),
        }
    }
    pub fn get_ctx(&self) -> Arc<DriverContext> {
        self.driver_ctx.clone()
    }
    pub fn get_pubkey(&self) -> [u8; 113] {
        match self.node_capabilities {
            NodeCapabilities::Management {
                realm_verifying_key,
                ..
            } => realm_verifying_key,
            NodeCapabilities::Server {
                realm_verifying_key,
                ..
            } => realm_verifying_key,
            NodeCapabilities::Local {
                realm_verifying_key,
            } => realm_verifying_key,
        }
    }

    pub fn get_realm_id(&self) -> RealmId {
        self.realm_id.clone()
    }

    pub async fn get_issuer_key(&self, pubkey: String) -> Result<DecodingKey, TokenError> {
        // Just to be double sure this is not producing deadlocks
        let read_lock = self.issuer_keys.read().await;
        let key = read_lock.get(&pubkey).cloned();
        drop(read_lock);
        if let Some(key) = key {
            return Ok(key);
        }

        let issuer_pubkey: [u8; 32] = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(pubkey.clone())?
            .try_into()
            .map_err(|_| TokenError::InvalidIssuerKey)?;
        let pub_pem_key =
            VerifyingKey::from_bytes(&issuer_pubkey)?.to_public_key_pem(LineEnding::default())?;
        let decoding_key = DecodingKey::from_ed_pem(pub_pem_key.as_bytes())?;
        self.issuer_keys
            .write()
            .await
            .insert(pubkey, decoding_key.clone());
        Ok(decoding_key)
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
