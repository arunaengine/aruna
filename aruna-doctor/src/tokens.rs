use crate::error::CliError;
use aruna::config::load;
use aruna_api::error::TokenError;
use aruna_api::routes::users::{
    RegisterBootstrapUserRequest, RegisterBootstrapUserResponse, RegisterOidcUserRequest,
    RegisterOidcUserResponse,
};
use aruna_api::server_state::{
    INITIAL_LOCAL_ONBOARDING_SECRET_KEY, TOKEN_REVOCATION_LIST_KEY, TRUSTED_REALMS_LIST_KEY,
    load_persisted_state,
};
use aruna_core::UserId;
use aruna_core::onboarding::OnboardingSecret;
use aruna_core::structs::{OidcProviderConfig, RealmId, TokenClaims};
use aruna_operations::create_token::{CreateTokenConfig, CreateTokenOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_storage::storage;
use base64::Engine;
use ed25519_dalek::VerifyingKey;
use ed25519_dalek::pkcs8::EncodePublicKey;
use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
use ed25519_dalek::{Signature, Verifier};
use jsonwebtoken::dangerous::insecure_decode;
use jsonwebtoken::{DecodingKey, Validation, decode};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use ulid::Ulid;

#[derive(Debug, Clone)]
struct OidcCliConfig {
    http_socket_addr: SocketAddr,
    oidc_providers: Vec<OidcProviderConfig>,
}

fn url_encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char)
            }
            b' ' => encoded.push('+'),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn oidc_password_grant_body(
    client_id: &str,
    username: &str,
    password: &str,
    scope: &str,
) -> String {
    [
        ("grant_type", "password"),
        ("client_id", client_id),
        ("username", username),
        ("password", password),
        ("scope", scope),
    ]
    .into_iter()
    .map(|(key, value)| format!("{}={}", url_encode_component(key), url_encode_component(value)))
    .collect::<Vec<_>>()
    .join("&")
}

#[allow(clippy::too_many_arguments)]
pub async fn create_token(
    name: Option<String>,
    unsafe_arbitrary_user_id: bool,
    user_id: Option<String>,
    expiry: Option<u64>,
    oidc_username: Option<String>,
    oidc_password: Option<String>,
    oidc_name: Option<String>,
    oidc_scope: String,
) -> Result<String, CliError> {
    if oidc_username.is_some() || oidc_password.is_some() {
        if user_id.is_some() || expiry.is_some() || unsafe_arbitrary_user_id || name.is_some() {
            return Err(CliError::InvalidOidcCreateTokenArgs);
        }
        let username = oidc_username.ok_or(CliError::MissingOidcCredentials)?;
        let password = oidc_password.ok_or(CliError::MissingOidcCredentials)?;
        return create_oidc_token(username, password, oidc_name, oidc_scope).await;
    }

    if user_id.is_some() && !unsafe_arbitrary_user_id {
        return Err(CliError::UnsafeUserIdRequired);
    }

    if unsafe_arbitrary_user_id {
        let user_id = user_id
            .map(|id| Ulid::from_string(&id))
            .unwrap_or(Ok(Ulid::new()))?;
        return create_unsafe_token(user_id, expiry).await;
    }

    let name = name.ok_or(CliError::MissingBootstrapName)?;
    create_local_bootstrap_token(name).await
}

async fn create_unsafe_token(user_id: Ulid, expiry: Option<u64>) -> Result<String, CliError> {
    let (config, _) = load().await.map_err(Box::new)?;
    let storage_handle = storage::FjallStorage::open(&config.storage_path)?;

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        automerge_handle: None,
        metadata_handle: None,
        task_handle: None,
    });

    let token_config = CreateTokenConfig {
        time: chrono::Utc::now().timestamp() as u64,
        expiry,
        user_id: UserId::local(user_id, config.realm_id),
        realm_id: config.realm_id,
        node_capabilities: config.node_capabilities,
    };
    let token_operation = CreateTokenOperation::new(token_config.clone())?;

    Ok(drive(token_operation, &driver_ctx).await?)
}

async fn create_local_bootstrap_token(name: String) -> Result<String, CliError> {
    let (config, _) = load().await.map_err(Box::new)?;
    let storage_handle = storage::FjallStorage::open(&config.storage_path)?;
    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        automerge_handle: None,
        metadata_handle: None,
        task_handle: None,
    });
    let onboarding_secret = load_persisted_state::<OnboardingSecret>(
        &driver_ctx,
        INITIAL_LOCAL_ONBOARDING_SECRET_KEY,
    )
    .await
    .ok_or(CliError::MissingInitialOnboardingSecret)?;
    let aruna_base_url = format!("http://{}", config.http_socket_addr);

    exchange_bootstrap_token(&Client::new(), &aruna_base_url, &name, &onboarding_secret).await
}

async fn create_oidc_token(
    username: String,
    password: String,
    name: Option<String>,
    scope: String,
) -> Result<String, CliError> {
    let config = load_oidc_cli_config()?;
    let provider = config
        .oidc_providers
        .into_iter()
        .next()
        .ok_or_else(|| CliError::OidcProviderNotFound("<token-derived>".to_string()))?;
    let aruna_base_url = format!("http://{}", config.http_socket_addr);

    let client = Client::builder().build()?;
    let oidc_token = request_oidc_token(&client, &provider, &username, &password, &scope).await?;
    exchange_oidc_token(
        &client,
        &aruna_base_url,
        name.as_deref(),
        &oidc_token,
    )
    .await
}

fn load_oidc_cli_config() -> Result<OidcCliConfig, CliError> {
    let _ = dotenvy::dotenv();
    let http_socket_addr = SocketAddr::from_str(&dotenvy::var("SOCKET_ADDRESS")?)?;

    Ok(OidcCliConfig {
        http_socket_addr,
        oidc_providers: load_oidc_providers_from_env()?,
    })
}

fn load_oidc_providers_from_env() -> Result<Vec<OidcProviderConfig>, CliError> {
    let Some(provider_ids) = dotenvy::var("OIDC_PROVIDER_IDS").ok() else {
        return Ok(Vec::new());
    };

    provider_ids
        .split(',')
        .map(str::trim)
        .filter(|provider_id| !provider_id.is_empty())
        .map(|provider_id: &str| {
            let env_prefix = provider_id.to_ascii_uppercase().replace('-', "_");
            Ok(OidcProviderConfig {
                id: provider_id.to_string(),
                issuer: dotenvy::var(format!("OIDC_{env_prefix}_ISSUER"))?,
                audience: dotenvy::var(format!("OIDC_{env_prefix}_AUDIENCE"))?,
                discovery_url: dotenvy::var(format!("OIDC_{env_prefix}_DISCOVERY_URL"))?,
            })
        })
        .collect()
}

async fn request_oidc_token(
    client: &Client,
    provider: &OidcProviderConfig,
    username: &str,
    password: &str,
    scope: &str,
) -> Result<String, CliError> {
    let discovery = client
        .get(&provider.discovery_url)
        .send()
        .await?
        .error_for_status()?
        .json::<OidcDiscoveryDocument>()
        .await?;

    let response = client
        .post(discovery.token_endpoint)
        .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(oidc_password_grant_body(
            &provider.audience,
            username,
            password,
            scope,
        ))
        .send()
        .await?
        .error_for_status()?
        .json::<OidcTokenResponse>()
        .await?;

    Ok(response.id_token.unwrap_or(response.access_token))
}

async fn exchange_oidc_token(
    client: &Client,
    aruna_base_url: &str,
    name: Option<&str>,
    oidc_token: &str,
) -> Result<String, CliError> {
    let response = client
        .post(format!("{aruna_base_url}/api/v1/users/oidc"))
        .bearer_auth(oidc_token)
        .json(&RegisterOidcUserRequest {
            name: name.map(str::to_string),
        })
        .send()
        .await?
        .error_for_status()?
        .json::<RegisterOidcUserResponse>()
        .await?;

    Ok(response.token)
}

async fn exchange_bootstrap_token(
    client: &Client,
    aruna_base_url: &str,
    name: &str,
    onboarding_secret: &OnboardingSecret,
) -> Result<String, CliError> {
    let response = client
        .post(format!("{aruna_base_url}/api/v1/users/bootstrap"))
        .json(&RegisterBootstrapUserRequest {
            onboarding_secret: onboarding_secret.encode()?,
            name: name.to_string(),
        })
        .send()
        .await?
        .error_for_status()?
        .json::<RegisterBootstrapUserResponse>()
        .await?;

    Ok(response.token)
}

#[derive(Debug, Deserialize)]
struct OidcDiscoveryDocument {
    token_endpoint: String,
}

#[derive(Debug, Deserialize)]
struct OidcTokenResponse {
    access_token: String,
    #[serde(default)]
    id_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenView {
    pub claims: TokenClaims,
    pub is_blacklisted: bool,
    pub valid: Valid,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Valid {
    True,
    False { reason: String },
}

pub async fn view_token(token: String) -> Result<String, CliError> {
    let hash = blake3::hash(token.as_bytes()).to_string();
    let unvalidated_claims = insecure_decode::<TokenClaims>(&token)?;

    let (config, _) = load().await.unwrap();
    let storage_handle = storage::FjallStorage::open(&config.storage_path).unwrap();

    let driver_ctx = Arc::new(DriverContext {
        storage_handle,
        net_handle: None,
        blob_handle: None,
        automerge_handle: None,
        metadata_handle: None,
        task_handle: None,
    });
    let persisted_blacklist = load_persisted_state::<HashSet<String, ahash::RandomState>>(
        &driver_ctx,
        TOKEN_REVOCATION_LIST_KEY,
    )
    .await
    .unwrap_or_default();

    let blacklisted = persisted_blacklist.contains(&hash);

    let trusted_realms = load_persisted_state::<HashSet<RealmId, ahash::RandomState>>(
        &driver_ctx,
        TRUSTED_REALMS_LIST_KEY,
    )
    .await
    .unwrap_or_default();

    let valid = match validate(unvalidated_claims.claims.clone(), token, trusted_realms).await {
        Ok(_) => Valid::True,
        Err(err) => Valid::False {
            reason: err.to_string(),
        },
    };

    let token_view = TokenView {
        claims: unvalidated_claims.claims,
        is_blacklisted: blacklisted,
        valid,
    };

    Ok(serde_json::to_string_pretty(&token_view)?)
}

async fn validate(
    claims: TokenClaims,
    token: String,
    trusted_realms: HashSet<RealmId, ahash::RandomState>,
) -> Result<(), TokenError> {
    let claims = match (claims.issuer_pubkey, claims.delegation_signature.is_some()) {
        (Some(issuer), true) => {
            let issuer_pubkey: [u8; 32] = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(issuer.clone())?
                .try_into()
                .map_err(|_| TokenError::InvalidIssuerKey)?;
            let pub_pem_key = VerifyingKey::from_bytes(&issuer_pubkey)?
                .to_public_key_pem(LineEnding::default())?;
            let decoding_key = DecodingKey::from_ed_pem(pub_pem_key.as_bytes())?;

            decode::<TokenClaims>(
                token,
                &decoding_key,
                &Validation::new(jsonwebtoken::Algorithm::EdDSA),
            )?
        }
        (_, _) => {
            let issuer_pubkey: [u8; 32] = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(claims.iss.clone())?
                .try_into()
                .map_err(|_| TokenError::InvalidIssuerKey)?;
            let pub_pem_key = VerifyingKey::from_bytes(&issuer_pubkey)?
                .to_public_key_pem(LineEnding::default())?;
            let decoding_key = DecodingKey::from_ed_pem(pub_pem_key.as_bytes())?;

            decode::<TokenClaims>(
                token,
                &decoding_key,
                &Validation::new(jsonwebtoken::Algorithm::EdDSA),
            )?
        }
    };

    let now = chrono::Utc::now().timestamp() as u64;
    if now > claims.claims.exp {
        return Err(TokenError::Expired);
    }

    if !trusted_realms.contains(
        &RealmId::from_base64(&claims.claims.iss).map_err(|_| TokenError::InvalidIssuerKey)?,
    ) {
        return Err(TokenError::RealmNotTrusted);
    }

    // Check server token claims
    match (
        &claims.claims.delegation_signature,
        &claims.claims.issuer_pubkey,
    ) {
        (Some(delegation_token), Some(issuer_pubkey)) => {
            // Check delegation signature
            let realm_key =
                base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&claims.claims.iss)?;
            let realm_verifying_key = VerifyingKey::from_bytes(realm_key.as_slice().try_into()?)?;
            let signature = Signature::from_str(delegation_token)?;
            realm_verifying_key.verify(issuer_pubkey.as_bytes(), &signature)?;
            Ok(())
        }
        (None, None) => Ok(()),
        (_, _) => Err(TokenError::InvalidServerToken),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        exchange_oidc_token, load_oidc_providers_from_env, oidc_password_grant_body,
        request_oidc_token,
    };
    use aruna_api::routes::users::{RegisterOidcUserRequest, RegisterOidcUserResponse};
    use aruna_core::structs::OidcProviderConfig;
    use axum::extract::State;
    use axum::routing::{get, post};
    use axum::{Form, Json, Router};
    use reqwest::Client;
    use serde::Deserialize;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::{Mutex, OnceLock};
    use tokio::net::TcpListener;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn restore_env(previous: Vec<(String, Option<String>)>) {
        for (key, value) in previous {
            match value {
                Some(value) => unsafe { std::env::set_var(key, value) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }

    #[derive(Clone)]
    struct OidcTestState {
        discovery_url: String,
        expected_client_id: String,
        expected_username: String,
        expected_password: String,
    }

    #[derive(Deserialize)]
    struct TokenForm {
        grant_type: String,
        client_id: String,
        username: String,
        password: String,
        scope: String,
    }

    async fn discovery(State(state): State<Arc<OidcTestState>>) -> Json<HashMap<&'static str, String>> {
        Json(HashMap::from([(
            "token_endpoint",
            format!("{}/token", state.discovery_url),
        )]))
    }

    async fn token(
        State(state): State<Arc<OidcTestState>>,
        Form(form): Form<TokenForm>,
    ) -> Json<HashMap<&'static str, String>> {
        assert_eq!(form.grant_type, "password");
        assert_eq!(form.client_id, state.expected_client_id);
        assert_eq!(form.username, state.expected_username);
        assert_eq!(form.password, state.expected_password);
        assert_eq!(form.scope, "openid profile");

        Json(HashMap::from([
            ("access_token", "oidc-access-token".to_string()),
            ("id_token", "oidc-id-token".to_string()),
        ]))
    }

    async fn aruna_oidc_exchange(
        Json(request): Json<RegisterOidcUserRequest>,
    ) -> Json<RegisterOidcUserResponse> {
        assert_eq!(request.name.as_deref(), Some("Alice Example"));

        Json(RegisterOidcUserResponse {
            user: aruna_api::routes::users::RegisterUserResponse {
                id: "01J00000000000000000000000".to_string(),
                name: "alice".to_string(),
            },
            token: "aruna-bearer-token".to_string(),
        })
    }

    #[tokio::test]
    async fn requests_oidc_token_via_discovery_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let state = Arc::new(OidcTestState {
            discovery_url: base_url.clone(),
            expected_client_id: "aruna-api".to_string(),
            expected_username: "alice".to_string(),
            expected_password: "alice-password".to_string(),
        });
        let router = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/token", post(token))
            .with_state(state);
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let provider = OidcProviderConfig {
            id: "main".to_string(),
            issuer: "http://issuer.example".to_string(),
            audience: "aruna-api".to_string(),
            discovery_url: format!("{base_url}/.well-known/openid-configuration"),
        };
        let token = request_oidc_token(
            &Client::new(),
            &provider,
            "alice",
            "alice-password",
            "openid profile",
        )
        .await
        .unwrap();

        assert_eq!(token, "oidc-id-token");
        server.abort();
    }

    #[tokio::test]
    async fn falls_back_to_access_token_when_id_token_is_missing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let state = Arc::new(OidcTestState {
            discovery_url: base_url.clone(),
            expected_client_id: "aruna-api".to_string(),
            expected_username: "alice".to_string(),
            expected_password: "alice-password".to_string(),
        });
        let router = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route(
                "/token",
                post(
                    |State(state): State<Arc<OidcTestState>>, Form(form): Form<TokenForm>| async move {
                        assert_eq!(form.client_id, state.expected_client_id);
                        Json(HashMap::from([(
                            "access_token",
                            "oidc-access-token".to_string(),
                        )]))
                    },
                ),
            )
            .with_state(state);
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let provider = OidcProviderConfig {
            id: "main".to_string(),
            issuer: "http://issuer.example".to_string(),
            audience: "aruna-api".to_string(),
            discovery_url: format!("{base_url}/.well-known/openid-configuration"),
        };
        let token = request_oidc_token(
            &Client::new(),
            &provider,
            "alice",
            "alice-password",
            "openid profile",
        )
        .await
        .unwrap();

        assert_eq!(token, "oidc-access-token");
        server.abort();
    }

    #[test]
    fn oidc_password_grant_body_encodes_form_values() {
        let body = oidc_password_grant_body(
            "aruna-api",
            "alice@example.com",
            "p@ss word",
            "openid profile",
        );

        assert_eq!(
            body,
            "grant_type=password&client_id=aruna-api&username=alice%40example.com&password=p%40ss+word&scope=openid+profile"
        );
    }

    #[test]
    fn loads_oidc_providers_from_environment() {
        let _guard = env_lock().lock().unwrap();
        let vars = [
            ("OIDC_PROVIDER_IDS", "main".to_string()),
            ("OIDC_MAIN_ISSUER", "https://issuer.example".to_string()),
            ("OIDC_MAIN_AUDIENCE", "aruna-api".to_string()),
            (
                "OIDC_MAIN_DISCOVERY_URL",
                "https://issuer.example/.well-known/openid-configuration".to_string(),
            ),
        ];
        let previous: Vec<_> = vars
            .iter()
            .map(|(key, _)| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        for (key, value) in &vars {
            unsafe { std::env::set_var(key, value) };
        }

        let providers = load_oidc_providers_from_env().unwrap();

        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "main");
        assert_eq!(providers[0].audience, "aruna-api");

        restore_env(previous);
    }

    #[tokio::test]
    async fn exchanges_oidc_token_for_aruna_token() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let router = Router::new().route("/api/v1/users/oidc", post(aruna_oidc_exchange));
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let token = exchange_oidc_token(
            &Client::new(),
            &base_url,
            Some("Alice Example"),
            "oidc-token",
        )
        .await
        .unwrap();

        assert_eq!(token, "aruna-bearer-token");
        server.abort();
    }

    #[tokio::test]
    async fn exchanges_oidc_token_without_optional_name() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let router = Router::new().route(
            "/api/v1/users/oidc",
            post(|Json(request): Json<RegisterOidcUserRequest>| async move {
                assert!(request.name.is_none());

                Json(RegisterOidcUserResponse {
                    user: aruna_api::routes::users::RegisterUserResponse {
                        id: "01J00000000000000000000000".to_string(),
                        name: "subject-123".to_string(),
                    },
                    token: "aruna-bearer-token".to_string(),
                })
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let token = exchange_oidc_token(&Client::new(), &base_url, None, "oidc-token")
            .await
            .unwrap();

        assert_eq!(token, "aruna-bearer-token");
        server.abort();
    }
}
