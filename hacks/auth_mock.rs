use std::sync::Arc;

use anyhow::{Result, anyhow};
use axum::{
    Router,
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::get,
};
use openidconnect::{
    AccessTokenHash, AuthorizationCode, ClientId, ClientSecret, CsrfToken, IssuerUrl, Nonce,
    OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, TokenResponse,
    core::{CoreAuthenticationFlow, CoreClient, CoreProviderMetadata, CoreUserInfoClaims},
};
use tokio::sync::Mutex;

struct AuthState {
    client: Clients,
    pkce_verifier: Option<PkceCodeVerifier>,
    csrf_token: Option<CsrfToken>,
    nonce: Option<Nonce>,
}

#[derive(Clone)]
struct Clients {
    oidc: OIDCClient,
    reqwest: reqwest::Client,
}

type OIDCClient = openidconnect::Client<
    openidconnect::EmptyAdditionalClaims,
    openidconnect::core::CoreAuthDisplay,
    openidconnect::core::CoreGenderClaim,
    openidconnect::core::CoreJweContentEncryptionAlgorithm,
    openidconnect::core::CoreJsonWebKey,
    openidconnect::core::CoreAuthPrompt,
    openidconnect::StandardErrorResponse<openidconnect::core::CoreErrorResponseType>,
    openidconnect::StandardTokenResponse<
        openidconnect::IdTokenFields<
            openidconnect::EmptyAdditionalClaims,
            openidconnect::EmptyExtraTokenFields,
            openidconnect::core::CoreGenderClaim,
            openidconnect::core::CoreJweContentEncryptionAlgorithm,
            openidconnect::core::CoreJwsSigningAlgorithm,
        >,
        openidconnect::core::CoreTokenType,
    >,
    openidconnect::StandardTokenIntrospectionResponse<
        openidconnect::EmptyExtraTokenFields,
        openidconnect::core::CoreTokenType,
    >,
    openidconnect::core::CoreRevocableToken,
    openidconnect::StandardErrorResponse<openidconnect::RevocationErrorResponseType>,
    openidconnect::EndpointSet,
    openidconnect::EndpointNotSet,
    openidconnect::EndpointNotSet,
    openidconnect::EndpointNotSet,
    openidconnect::EndpointMaybeSet,
    openidconnect::EndpointMaybeSet,
>;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().unwrap();
    let bind_addr = dotenvy::var("BIND_ADDR").unwrap();
    let issuer_url = dotenvy::var("ISSUER_URL").unwrap();
    let client_secret = dotenvy::var("CLIENT_SECRET").unwrap();
    let redirect_url = dotenvy::var("REDIRECT_URL").unwrap();
    // build our application with a single route
    // our router
    let client = get_oidc_client(issuer_url, client_secret, redirect_url)
        .await
        .unwrap();
    let shared_state = Arc::new(Mutex::new(AuthState {
        client,
        pkce_verifier: None,
        csrf_token: None,
        nonce: None,
    }));

    let app = Router::new()
        .route("/", get(entry))
        .route("/callback", get(callback))
        .with_state(shared_state);

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn entry(
    State(state): State<Arc<Mutex<AuthState>>>,
    request: Request,
) -> Result<Response, StatusCode> {
    dbg!(&request);

    let mut lock = state.lock().await;
    let client = lock.client.clone();

    if request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .is_some()
    {
        todo!()
    } else {
        let (redirect, pkce, csrf, nonce) = oidc_redirect(client.oidc).await.unwrap();
        lock.pkce_verifier = Some(pkce);
        lock.csrf_token = Some(csrf);
        lock.nonce = Some(nonce);
        Ok(Redirect::to(&redirect).into_response())
    }
}

async fn callback(
    State(state): State<Arc<Mutex<AuthState>>>,
    request: Request,
) -> Result<Response, StatusCode> {
    dbg!(&request);

    let uri = request.uri().query().unwrap().to_string();
    let code = uri.split("code=").last().unwrap().to_string();
    dbg!(&code);

    let mut lock = state.lock().await;
    let client = lock.client.clone();
    let pkce = lock.pkce_verifier.take().unwrap();
    let csrf = lock.csrf_token.take().unwrap();
    let nonce = lock.nonce.take().unwrap();

    let id = oidc_callback(code, client.clone(), pkce, csrf, nonce)
        .await
        .unwrap();

    //let request: HashMap<String, String> = client.reqwest.post("http://localhost:8080/api/v3/users").headers(
    //    reqwest::header::HeaderMap::from_iter(
    //    [
    //            (ACCEPT, HeaderValue::from_static("application/json'")),
    //            (AUTHORIZATION, HeaderValue::from_static("eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJocS1BcGJfWS15RzJ1YktjSDFmTGN4UmltZ3YzSlBSelRQUENKbEtpOW9zIn0.eyJleHAiOjE3NTE1MzY5NDQsImlhdCI6MTc1MTUzNjY0NCwiYXV0aF90aW1lIjoxNzUxNTMzMTMzLCJqdGkiOiI3MjAxN2M1My02NmE3LTQzOWItYmFhZS1hZDYyNjJlNDc1ZWMiLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjE5OTgvcmVhbG1zL3Rlc3QiLCJhdWQiOiJ0ZXN0Iiwic3ViIjoiOGRiZWUwMDktYTNlOC00NjY0LTg4NTYtMTQxNzNkOWFiZDViIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoidGVzdCIsInNpZCI6Ijk3MmEzNzUxLWRmNmYtNDkyNS1hMTljLTFkZGZmZGZiZmVmMyIsInNjb3BlIjoib3BlbmlkIn0.Lu97RlebILigfexe_-CTRkE-GHezawtagkqk1RTGSc-82Vh5LuIqI5L61iCiiG9q0Ts-d9r4MSqjCgxnlkK3TmwHgINVet7zeLrAHsInRFtrnTJtkxsG8eveagXwqxVqufoNtcLjKG20L3Zt0eFrkkywAYSKf_ccYxQ5h6EBUkh2Fy6DDCxM70hwoJDWhzqie0ebwWG1L6vAfDN5tpKdSu5d_V34FJelbFTzqAZyeEVEnO8DkiDxnKgK5nU6rdeTNZEE1oTyycNQsoAJTXeWZ2-zqyWzKSliN82S6P-d85EFgsB0W0q16zxa4rDfVz-uTpA4fH8Rg4JVgGC78oHIlA")),
    //            (CONTENT_TYPE, HeaderValue::from_static("application/json"))
    //    ].into_iter()))
    //.json(&HashMap::<&str, &str>::from_iter([("name", "regular")])).send().await.unwrap().json().await.unwrap();

    //let response = format!(
    //        "
    //oidc_token: {id}
    //user: {request:?}
    //"
    //    );

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(id)
        .unwrap()
        .into_response();
    Ok(response)
}

async fn get_oidc_client(
    issuer_url: String,
    client_secret: String,
    redirect_url: String,
) -> Result<Clients> {
    let http_client = reqwest::ClientBuilder::new()
        // Following redirects opens the client up to SSRF vulnerabilities.
        .redirect(reqwest::redirect::Policy::none())
        .build()?;

    let provider_metadata = CoreProviderMetadata::discover_async(
        IssuerUrl::new(issuer_url)?,
        &http_client,
    )
    .await?;

    // Create an OpenID Connect client by specifying the client ID, client secret, authorization URL
    // and token URL.
    let core_client = CoreClient::from_provider_metadata(
        provider_metadata,
        ClientId::new("test".to_string()),
        Some(ClientSecret::new(client_secret)),
    )
    // Set the URL the user will be redirected to after the authorization process.
    .set_redirect_uri(RedirectUrl::new(redirect_url)?);

    Ok(Clients {
        oidc: core_client,
        reqwest: http_client,
    })
}

async fn oidc_redirect(client: OIDCClient) -> Result<(String, PkceCodeVerifier, CsrfToken, Nonce)> {
    // Generate a PKCE challenge.
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();

    // Generate the full authorization URL.
    let (auth_url, csrf_token, nonce) = client
        .authorize_url(
            CoreAuthenticationFlow::AuthorizationCode,
            CsrfToken::new_random,
            Nonce::new_random,
        )
        // Set the desired scopes.
        //.add_scope(Scope::new("read".to_string()))
        //.add_scope(Scope::new("write".to_string()))
        // Set the PKCE code challenge.
        .set_pkce_challenge(pkce_challenge)
        .url();
    Ok((auth_url.to_string(), pkce_verifier, csrf_token, nonce))
}

async fn oidc_callback(
    code: String,
    Clients { oidc, reqwest }: Clients,
    pkce_verifier: PkceCodeVerifier,
    _csrf_token: CsrfToken,
    nonce: Nonce,
) -> Result<String> {
    // Once the user has been redirected to the redirect URL, you'll have access to the
    // authorization code. For security reasons, your code should verify that the `state`
    // parameter returned by the server matches `csrf_state`.

    // Now you can exchange it for an access token and ID token.
    let token_response = oidc
        .exchange_code(AuthorizationCode::new(code))?
        // Set the PKCE code verifier.
        .set_pkce_verifier(pkce_verifier)
        .request_async(&reqwest)
        .await?;
    let access_token = token_response.access_token().secret();

    // Extract the ID token claims after verifying its authenticity and nonce.
    let id_token = token_response
        .id_token()
        .ok_or_else(|| anyhow!("Server did not return an ID token"))?;
    let id_token_verifier = oidc.id_token_verifier();
    dbg!(&id_token);
    let claims = id_token.claims(&oidc.id_token_verifier(), &nonce)?;

    // Verify the access token hash to ensure that the access token hasn't been substituted for
    // another user's.
    if let Some(expected_access_token_hash) = claims.access_token_hash() {
        let actual_access_token_hash = AccessTokenHash::from_token(
            token_response.access_token(),
            id_token.signing_alg()?,
            id_token.signing_key(&id_token_verifier)?,
        )?;
        if actual_access_token_hash != *expected_access_token_hash {
            return Err(anyhow!("Invalid access token"));
        }
    }

    // If available, we can use the UserInfo endpoint to request additional information.

    // The user_info request uses the AccessToken returned in the token response. To parse custom
    // claims, use UserInfoClaims directly (with the desired type parameters) rather than using the
    // CoreUserInfoClaims type alias.
    let userinfo: CoreUserInfoClaims = oidc
        .user_info(token_response.access_token().to_owned(), None)
        .map_err(|err| anyhow!("No user info endpoint: {:?}", err))?
        .request_async(&reqwest)
        .await
        .map_err(|err| anyhow!("Failed requesting user info: {:?}", err))?;

    dbg!(&userinfo);

    Ok(access_token.to_string())
}
