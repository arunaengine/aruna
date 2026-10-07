//! Looks up S3 signing secrets and authorizes each S3 request against permissions and policy.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::scope::resolve_scope;
use super::server::S3OpLabel;
use super::util::{anonymous_read_allowed, operation_permission};
use crate::object_key::ObjectKey;
use crate::rate_limit::{LocalKey, LocalLease, LocalPermit};
use aruna_core::compute::{SecretBytes, SharedSecret};
use aruna_core::credential_encryption::{CredentialEncryptionKey, EncryptedS3Secret};
use aruna_core::errors::StorageError;
use aruna_core::structs::identity::auth::{AuthContext, Permission};
use aruna_core::structs::identity::realm::RealmId;
use aruna_core::structs::identity::s3_session::{S3Session, SESSION_ACCESS_PREFIX};
use aruna_core::structs::storage::blob::{
    BucketInfo, UserAccess, bucket_permission_path, group_permission_path, object_permission_path,
};
use aruna_core::structs::storage::encryption::TokenCredential;
use aruna_core::{NodeId, UserId};
use aruna_operations::auth::bearer_token::realm_user_cutoff;
use aruna_operations::auth::request_authorization::{AuthorizeError, authorize};
use aruna_operations::auth::request_policy::{
    PolicyRequestExtras, enforce_policies, policy_request_with,
};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::realm::get_config::GetConfigOperation;
use aruna_operations::s3::access::get::{GetAccessError, GetAccessOperation};
use aruna_operations::s3::bucket::get::{GetBucketError, GetBucketOperation};
use aruna_operations::s3::session::{
    GetS3Operation, S3SessionError, TouchS3Config, TouchS3Operation,
};
use aruna_operations::staging::offered_directory::{OfferedDirectoryError, guard_bucket_write};
use base64::{Engine, engine::general_purpose::STANDARD};
use http::{HeaderMap, Uri};
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::debug;
use zeroize::Zeroizing;

tokio::task_local! {
    /// The session token hash of the request being verified, which selects its signing secret.
    static REQUEST_TOKEN: Option<String>;
}

/// Runs `future` with the request's session token available to secret lookup.
pub(crate) async fn with_request_token<F: Future>(token: Option<String>, future: F) -> F::Output {
    REQUEST_TOKEN.scope(token, future).await
}

pub(crate) fn request_token(headers: &HeaderMap, uri: &Uri) -> Option<String> {
    request_token_hash(headers, uri).ok()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Access {
    Read,
    Write,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Action {
    Read,
    Write,
}

impl Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Read => write!(f, "read"),
            Action::Write => write!(f, "write"),
        }
    }
}

#[derive(Clone)]
pub struct AuthProvider {
    pub(crate) driver_ctx: Arc<DriverContext>,
    pub(crate) realm_id: RealmId,
    pub(crate) node_id: NodeId,
    pub(crate) encryption_key: CredentialEncryptionKey,
    pub(crate) rate_limits: Arc<crate::rate_limit::ApiRateLimits>,
}

#[async_trait::async_trait]
impl S3Auth for AuthProvider {
    async fn get_secret_key(&self, access_key_id: &str) -> S3Result<SecretKey> {
        if S3Session::is_session_key(access_key_id) {
            let session = self.query_session(access_key_id).await?;
            if session.issued_by != *self.node_id.as_bytes() {
                return Err(s3_error!(
                    InvalidAccessKeyId,
                    "The Access Key Id you provided does not exist in our records."
                ));
            }
            let token = REQUEST_TOKEN.try_with(Clone::clone).ok().flatten();
            let secret = session
                .open_secret_for(&self.encryption_key, token.as_deref(), SystemTime::now())
                .map_err(|_| {
                    s3_error!(
                        InvalidAccessKeyId,
                        "The Access Key Id you provided does not exist in our records."
                    )
                })?;
            return Ok(SecretKey::from(secret));
        }
        let user_access = self.query_user_access(access_key_id).await?;
        // Only the issuing node can decrypt this secret; copied or rebound records never open.
        if user_access.issued_by != *self.node_id.as_bytes() {
            return Err(s3_error!(
                InvalidAccessKeyId,
                "The Access Key Id you provided does not exist in our records."
            ));
        }
        let secret = user_access.open_secret(&self.encryption_key).map_err(|_| {
            s3_error!(
                InvalidAccessKeyId,
                "The Access Key Id you provided does not exist in our records."
            )
        })?;
        Ok(SecretKey::from(secret))
    }
}

#[async_trait::async_trait]
impl S3Access for AuthProvider {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // Label request metrics with the resolved operation as early as possible.
        let operation_name = cx.s3_op().name().to_string();
        if let Some(label) = cx.extensions_mut().get::<S3OpLabel>() {
            label.set(&operation_name);
        }

        // Evaluate action from S3 operation name
        let action = operation_permission(&operation_name)
            .ok_or_else(|| s3_error!(InvalidRequest, "Unknown Operation"))?;

        // An object key is accepted only on GetObject, in a signed header.
        let object_key = request_object_key(cx.headers(), cx.uri(), &operation_name)?;

        // Unsigned requests are checked as the Everyone principal, but only for
        // the public object-byte read surface.
        let access_key_id = match cx
            .credentials()
            .map(|credentials| credentials.access_key.clone())
        {
            Some(access_key_id) => access_key_id,
            None => return self.check_anonymous(cx, action).await,
        };

        let now = SystemTime::now();
        let (user_access, permit, session_token_hash) = if S3Session::is_session_key(&access_key_id)
        {
            let session = self.query_session(&access_key_id).await?;
            let token_hash = request_token_hash(cx.headers(), cx.uri())?;
            let permit = self.admit_session(&session, &token_hash, now)?;
            (session.as_user_access(), permit, Some(token_hash))
        } else {
            let token = credential_token(cx.headers(), cx.uri(), &access_key_id)?;
            let user_access = self.query_user_access(&access_key_id).await?;
            let permit = self.admit_credential(&user_access)?;
            if let Some(token) = token {
                cx.extensions_mut().insert(token);
            }
            (user_access, permit, None)
        };
        let lease = cx
            .extensions_mut()
            .get::<LocalLease>()
            .cloned()
            .unwrap_or_default();
        lease.replace(permit);
        cx.extensions_mut().insert(lease);

        // Decryption proves this node issued the credential; it must still belong to the realm.
        if !self.issuer_in_realm(&user_access.issued_by).await? {
            return Err(s3_error!(
                InvalidAccessKeyId,
                "Credential issuer not in realm"
            ));
        }
        self.check_cutoff(&access_key_id, &user_access.user_identity)
            .await?;

        let required_permission = match &action {
            Action::Read => Permission::READ,
            Action::Write => Permission::WRITE,
        };

        let (path, auth_context) = self
            .build_authorization_path(cx, &user_access, &action)
            .await?;

        // An offered directory is an observation of the owner's own files: it is
        // served read-only, so no write may claim to change it.
        if matches!(action, Action::Write)
            && let Some(bucket) = cx.s3_path().get_bucket_name().map(str::to_owned)
        {
            guard_bucket_write(self.driver_ctx.as_ref(), &bucket)
                .await
                .map_err(map_offered_error)?;
        }

        // Reuse this request context for ordinary and per-object policy checks.
        let extras = request_extras(cx, &operation_name);

        // DeleteObjects checks its body keys in the handler after these credential checks.
        if cx.s3_op().name() != "DeleteObjects" {
            match authorize(
                self.driver_ctx.as_ref(),
                self.realm_id,
                &auth_context,
                &path,
                &required_permission,
                extras.clone(),
            )
            .await
            {
                Ok(()) => {}
                Err(AuthorizeError::PermissionDenied) if is_listing_operation(&operation_name) => {
                    self.admit_subpath_listing(
                        cx,
                        &user_access,
                        &auth_context,
                        &path,
                        extras.clone(),
                    )
                    .await?;
                }
                Err(error) => return Err(map_authorize_error(error)),
            }
        }

        if let Some(token_hash) = session_token_hash {
            drive(
                TouchS3Operation::new(TouchS3Config {
                    access_key: access_key_id,
                    token_hash,
                    now,
                    issued_by: *self.node_id.as_bytes(),
                }),
                self.driver_ctx.as_ref(),
            )
            .await
            .map_err(map_session_error)?;
        }

        cx.extensions_mut().insert(extras);
        cx.extensions_mut().insert(user_access);
        if let Some(key) = object_key {
            cx.extensions_mut().insert(ObjectKey(Ok(Some(key))));
        }
        Ok(())
    }
}

/// Listings a member holding only part of a bucket may still run, because their
/// result is narrowed to that part. The location probe leads the listings every
/// AWS client makes, so it is admitted with them.
fn is_listing_operation(operation_name: &str) -> bool {
    matches!(
        operation_name,
        "ListBuckets" | "HeadBucket" | "GetBucketLocation" | "ListObjects" | "ListObjectsV2"
    )
}

/// Maps an authorization failure to an S3 error, keeping RBAC and policy denials
/// indistinguishable and control-plane failures fail-closed. Exhausted
/// transaction-cleanup capacity is retryable, so it stays a `SlowDown`.
pub(super) fn map_authorize_error(error: AuthorizeError) -> s3s::S3Error {
    match error {
        AuthorizeError::Storage(StorageError::CleanupCapacity) => {
            s3_error!(SlowDown, "Reduce your request rate")
        }
        AuthorizeError::CheckFailed(_) | AuthorizeError::Storage(_) => {
            s3_error!(InternalError, "Failed to check permissions")
        }
        _ => s3_error!(AccessDenied, "Permission denied"),
    }
}

/// Threads the S3 operation, query parameters (last value wins), and an
/// allowlisted, lowercased header subset into the policy context. Object bytes
/// are never buffered, so the body stays absent.
fn request_extras(cx: &S3AccessContext<'_>, operation_name: &str) -> PolicyRequestExtras {
    let mut params = BTreeMap::new();
    if let Some(query) = cx.uri().query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            params.insert(key.into_owned(), value.into_owned());
        }
    }
    let mut headers = BTreeMap::new();
    for (name, value) in cx.headers() {
        let name = name.as_str().to_ascii_lowercase();
        if header_allowed(&name)
            && let Ok(value) = value.to_str()
        {
            headers.insert(name, value.to_string());
        }
    }
    PolicyRequestExtras {
        operation: format!("s3.{operation_name}"),
        params,
        headers,
        body: None,
    }
}

/// Header allowlist for policy context; authorization and cookies never appear.
fn header_allowed(name: &str) -> bool {
    matches!(
        name,
        "content-type" | "content-length" | "x-amz-tagging" | "x-amz-acl"
    ) || name.starts_with("x-amz-meta-")
}

fn request_token_hash(headers: &HeaderMap, uri: &Uri) -> S3Result<String> {
    let mut token = None;
    for value in headers.get_all("x-amz-security-token") {
        let value = value
            .to_str()
            .map_err(|_| s3_error!(InvalidToken, "Invalid session token"))?;
        if token.replace(value.to_string()).is_some() {
            return Err(s3_error!(InvalidToken, "Invalid session token"));
        }
    }
    if let Some(query) = uri.query() {
        for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if name != "X-Amz-Security-Token" {
                continue;
            }
            if token.replace(value.into_owned()).is_some() {
                return Err(s3_error!(InvalidToken, "Invalid session token"));
            }
        }
    }
    token
        .map(|token| S3Session::hash_token(&token))
        .ok_or_else(|| s3_error!(MissingAuthenticationToken, "Session token is required"))
}

const TOKEN_HEADER: &str = "x-amz-security-token";
const TOKEN_QUERY: &str = "X-Amz-Security-Token";
const OBJECT_KEY_HEADER: &str = "x-aruna-object-key";

/// Hides every token and object key header value from formatting, so a logged request shows
/// neither.
pub(crate) fn hide_tokens(headers: &mut HeaderMap) {
    for name in [TOKEN_HEADER, OBJECT_KEY_HEADER] {
        if let http::header::Entry::Occupied(mut entry) = headers.entry(name) {
            for value in entry.iter_mut() {
                value.set_sensitive(true);
            }
        }
    }
}

/// The object key of a GetObject request. It must be one SigV4 signed header and never come
/// with a presigned URL, so the request signature covers it.
fn request_object_key(
    headers: &HeaderMap,
    uri: &Uri,
    operation: &str,
) -> S3Result<Option<SharedSecret>> {
    let mut values = headers.get_all(OBJECT_KEY_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    let presigned = url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .any(|(name, _)| name == "X-Amz-Signature");
    if values.next().is_some()
        || presigned
        || operation != "GetObject"
        || !signs_header(headers, OBJECT_KEY_HEADER)
    {
        return Err(s3_error!(
            AccessDenied,
            "The object key must be a signed header of a GetObject request"
        ));
    }
    let mut bytes = Zeroizing::new([0u8; 32]);
    match STANDARD.decode_slice(value.as_bytes(), &mut bytes[..]) {
        Ok(32) if value.len() == 44 => {
            Ok(Some(SharedSecret::new(SecretBytes::new(bytes.to_vec()))))
        }
        _ => Err(s3_error!(
            InvalidArgument,
            "The object key header is malformed"
        )),
    }
}

/// Whether the request carries a query token for anything but a session key. Such a token
/// would sit in the logged URI, so it is refused before the request is parsed.
pub(crate) fn query_token_refused(headers: &HeaderMap, uri: &Uri) -> bool {
    let query = uri.query().unwrap_or_default();
    let mut tokens = 0;
    let mut credentials = 0;
    let mut legacy = false;
    let mut presigned = false;
    let mut access_key = None;
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        match name.as_ref() {
            TOKEN_QUERY => tokens += 1,
            "X-Amz-Credential" => {
                credentials += 1;
                access_key = value.split('/').next().map(str::to_string);
            }
            "AWSAccessKeyId" | "Signature" => legacy = true,
            "X-Amz-Signature" => presigned = true,
            _ => {}
        }
    }
    if tokens == 0 {
        return false;
    }
    let authorizations = headers.get_all(http::header::AUTHORIZATION).iter().count();
    if tokens != 1
        || credentials > 1
        || authorizations > 1
        || ((credentials != 0 || presigned) && authorizations != 0)
        || headers.contains_key(TOKEN_HEADER)
        || legacy
    {
        return true;
    }
    !access_key
        .or_else(|| authorization_key(headers).map(str::to_string))
        .is_some_and(|key| S3Session::is_session_key(&key))
}

fn authorization_key(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(http::header::AUTHORIZATION)?.to_str().ok()?;
    let value = value.strip_prefix("AWS4-HMAC-SHA256 ")?;
    let mut credentials = value
        .split(',')
        .filter_map(|field| field.trim().strip_prefix("Credential="));
    let credential = credentials.next()?;
    if credentials.next().is_some() {
        return None;
    }
    credential.split('/').next()
}

/// Refuses unsupported long-lived token authentication before s3s logs canonical strings.
pub(crate) fn header_token_refused(headers: &HeaderMap, uri: &Uri) -> bool {
    if !headers.contains_key(TOKEN_HEADER) {
        return false;
    }
    let query: Vec<_> =
        url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes()).collect();
    if query
        .iter()
        .any(|(name, _)| name == "Signature" || name == "AWSAccessKeyId")
    {
        let credentials: Vec<_> = query
            .iter()
            .filter(|(name, _)| name == "AWSAccessKeyId")
            .collect();
        return headers.get_all(TOKEN_HEADER).iter().count() != 1
            || headers.contains_key(http::header::AUTHORIZATION)
            || query
                .iter()
                .any(|(name, _)| name == "X-Amz-Credential" || name == "X-Amz-Signature")
            || credentials.len() != 1
            || !S3Session::is_session_key(&credentials[0].1)
            || query.iter().filter(|(name, _)| name == "Signature").count() != 1;
    }
    if !headers.contains_key(http::header::AUTHORIZATION) {
        let credentials: Vec<_> = query
            .iter()
            .filter(|(name, _)| name == "X-Amz-Credential")
            .collect();
        return headers.get_all(TOKEN_HEADER).iter().count() != 1
            || credentials.len() != 1
            || !credentials[0]
                .1
                .split('/')
                .next()
                .is_some_and(S3Session::is_session_key)
            || query
                .iter()
                .filter(|(name, _)| name == "X-Amz-Algorithm")
                .count()
                != 1
            || !query
                .iter()
                .any(|(name, value)| name == "X-Amz-Algorithm" && value == "AWS4-HMAC-SHA256");
    }
    headers.get_all(TOKEN_HEADER).iter().count() != 1
        || headers.get_all(http::header::AUTHORIZATION).iter().count() != 1
        || query
            .iter()
            .any(|(name, _)| name == "X-Amz-Credential" || name == "X-Amz-Signature")
        || !headers
            .get(http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .strip_prefix("AWS ")
                    .and_then(|value| value.split_once(':'))
                    .is_some_and(|(key, _)| S3Session::is_session_key(key))
                    || authorization_key(headers).is_some_and(|key| {
                        S3Session::is_session_key(key) || signs_header(headers, TOKEN_HEADER)
                    })
            })
}

/// Whether the SigV4 `Authorization` header lists `name` among its signed headers.
pub(crate) fn signs_header(headers: &HeaderMap, name: &str) -> bool {
    headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split_once("SignedHeaders="))
        .and_then(|(_, rest)| rest.split(',').next())
        .is_some_and(|names| {
            names
                .split(';')
                .any(|signed| signed.trim().eq_ignore_ascii_case(name))
        })
}

/// The token credential a long-lived key sends in a signed `x-amz-security-token` header.
/// A query token, a presigned request or an unsigned or malformed token is refused.
fn credential_token(
    headers: &HeaderMap,
    uri: &Uri,
    access_key: &str,
) -> S3Result<Option<TokenCredential>> {
    let invalid = || s3_error!(InvalidToken, "Invalid security token");
    let (mut query_token, mut presigned) = (false, false);
    let query = uri.query().unwrap_or_default();
    for (name, _) in url::form_urlencoded::parse(query.as_bytes()) {
        query_token |= name == TOKEN_QUERY;
        presigned |= name == "X-Amz-Signature";
    }
    if query_token {
        return Err(invalid());
    }
    let mut values = headers.get_all(TOKEN_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() || presigned || !signs_header(headers, TOKEN_HEADER) {
        return Err(invalid());
    }
    TokenCredential::parse(access_key, value.as_bytes())
        .map(Some)
        .ok_or_else(invalid)
}

fn map_session_error(error: S3SessionError) -> s3s::S3Error {
    match error {
        S3SessionError::NotFound | S3SessionError::WrongIssuer => s3_error!(
            InvalidAccessKeyId,
            "The Access Key Id you provided does not exist in our records."
        ),
        S3SessionError::InvalidToken => s3_error!(InvalidToken, "Invalid session token"),
        S3SessionError::Expired => s3_error!(ExpiredToken, "Session token has expired"),
        _ => s3_error!(InternalError, "Failed to update session activity"),
    }
}

fn map_offered_error(error: OfferedDirectoryError) -> s3s::S3Error {
    match error {
        OfferedDirectoryError::ReadOnly(bucket) => s3_error!(
            AccessDenied,
            "Bucket {bucket} is an offered directory and is read-only"
        ),
        error => s3_error!(InternalError, "{}", error),
    }
}

impl AuthProvider {
    /// Anonymous access permits only concrete object bytes granted to Everyone.
    /// The bucket's group scopes the permission path.
    async fn check_anonymous(&self, cx: &mut S3AccessContext<'_>, action: Action) -> S3Result<()> {
        if !matches!(action, Action::Read) || !anonymous_read_allowed(cx.s3_op().name()) {
            return Err(s3_error!(
                AccessDenied,
                "Anonymous access is limited to object reads"
            ));
        }
        let Some((bucket, key)) = cx
            .s3_path()
            .as_object()
            .map(|(bucket, key)| (bucket.to_owned(), key.to_owned()))
        else {
            return Err(s3_error!(
                AccessDenied,
                "Anonymous requests must address an object"
            ));
        };
        let Some(bucket_info) = self.find_bucket_info(&bucket).await? else {
            return Err(s3_error!(AccessDenied, "Permission denied"));
        };
        let group_id = bucket_info.group_id;

        let path = object_permission_path(self.realm_id, group_id, self.node_id, &bucket, &key);

        let extras = request_extras(cx, cx.s3_op().name());
        authorize(
            self.driver_ctx.as_ref(),
            self.realm_id,
            &AuthContext::anonymous(self.realm_id),
            &path,
            &Permission::READ,
            extras.clone(),
        )
        .await
        .map_err(map_authorize_error)?;

        // Handlers receive an Everyone identity scoped to this bucket's group.
        // Blank keys cannot sign downstream requests.
        cx.extensions_mut().insert(extras);
        cx.extensions_mut().insert(bucket_info);
        cx.extensions_mut().insert(UserAccess {
            access_key: String::new(),
            user_identity: UserId::nil(self.realm_id),
            group_id,
            secret: EncryptedS3Secret::empty(),
            expiry: SystemTime::now(),
            path_restrictions: None,
            issued_by: *self.node_id.as_bytes(),
            revoked_at: None,
        });
        Ok(())
    }

    /// Rejects an unusable credential before any budget is spent: a revoked or
    /// expired credential must not drain the owner's shared request rate or
    /// take one of the owner's admission permits.
    fn admit_credential(&self, user_access: &UserAccess) -> S3Result<LocalPermit> {
        self.admit_credential_at(user_access, SystemTime::now())
    }

    fn admit_credential_at(
        &self,
        user_access: &UserAccess,
        now: SystemTime,
    ) -> S3Result<LocalPermit> {
        if user_access.is_revoked() {
            return Err(s3_error!(AccessDenied, "Credential has been revoked"));
        }
        if user_access.is_expired(now) {
            return Err(s3_error!(AccessDenied, "Credential has expired"));
        }
        // Charge the stable identity after lookup so credential rotation cannot
        // multiply the authenticated request budget.
        if self
            .rate_limits
            .check_principal(user_access.user_identity)
            .is_err()
        {
            return Err(s3_error!(SlowDown, "Reduce your request rate"));
        }
        self.rate_limits
            .try_acquire_local(LocalKey::User(user_access.user_identity))
            .ok_or_else(|| s3_error!(SlowDown, "Reduce your request rate"))
    }

    fn admit_session(
        &self,
        session: &S3Session,
        token_hash: &str,
        now: SystemTime,
    ) -> S3Result<LocalPermit> {
        if session.issued_by != *self.node_id.as_bytes() {
            return Err(s3_error!(
                InvalidAccessKeyId,
                "The Access Key Id you provided does not exist in our records."
            ));
        }
        if session.is_expired(now) {
            return Err(s3_error!(ExpiredToken, "Session token has expired"));
        }
        if !session.token_matches(token_hash, now) {
            return Err(s3_error!(InvalidToken, "Invalid session token"));
        }
        self.admit_credential_at(&session.as_user_access(), now)
    }

    /// Denies a credential issued before its owner's cutoff, like a bearer token issued before it.
    async fn check_cutoff(&self, access_key_id: &str, user_id: &UserId) -> S3Result<()> {
        let key_id = access_key_id
            .strip_prefix(SESSION_ACCESS_PREFIX)
            .unwrap_or(access_key_id);
        let issued = ulid::Ulid::from_string(key_id).map_or(0, |id| id.timestamp_ms() / 1000);
        let cutoff = realm_user_cutoff(&self.driver_ctx.storage_handle, self.realm_id, user_id)
            .await
            .map_err(|_| s3_error!(ServiceUnavailable, "Revocation state is unavailable"))?;
        if cutoff.is_some_and(|cutoff| issued < cutoff) {
            return Err(s3_error!(AccessDenied, "Credential has been revoked"));
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn query_user_access(&self, access_key_id: &str) -> S3Result<UserAccess> {
        // Legacy-format key ids can never match a stored credential; reject them
        // before the lookup, indistinguishably from an unknown key.
        if UserAccess::build_access_key(access_key_id).is_err() {
            return Err(s3_error!(
                InvalidAccessKeyId,
                "The Access Key Id you provided does not exist in our records."
            ));
        }
        let operation = GetAccessOperation::new(access_key_id.to_string());
        match drive(operation, self.driver_ctx.as_ref()).await {
            Ok(user_access) => Ok(user_access),
            Err(GetAccessError::NotFound) => Err(s3_error!(
                InvalidAccessKeyId,
                "The Access Key Id you provided does not exist in our records."
            )),
            Err(_) => Err(s3_error!(InternalError, "Failed to query user access")),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn query_session(&self, access_key_id: &str) -> S3Result<S3Session> {
        if !S3Session::valid_access_key(access_key_id) {
            return Err(s3_error!(
                InvalidAccessKeyId,
                "The Access Key Id you provided does not exist in our records."
            ));
        }
        match drive(
            GetS3Operation::new(access_key_id.to_string()),
            self.driver_ctx.as_ref(),
        )
        .await
        {
            Ok(Some(session)) => Ok(session),
            Ok(None) | Err(S3SessionError::NotFound | S3SessionError::InvalidAccessKey) => {
                Err(s3_error!(
                    InvalidAccessKeyId,
                    "The Access Key Id you provided does not exist in our records."
                ))
            }
            Err(_) => Err(s3_error!(InternalError, "Failed to query S3 session")),
        }
    }

    /// Checks that the issuer proven by local decryption still belongs to this realm.
    async fn issuer_in_realm(&self, issued_by: &[u8; 32]) -> S3Result<bool> {
        let config = drive(
            GetConfigOperation::new(self.realm_id),
            self.driver_ctx.as_ref(),
        )
        .await
        .map_err(|_| s3_error!(InternalError, "Failed to load realm config"))?;
        let node_ids = config
            .node_ids()
            .map_err(|_| s3_error!(InternalError, "Malformed realm node id"))?;
        Ok(node_ids
            .iter()
            .any(|node_id| node_id.as_bytes() == issued_by))
    }

    async fn find_bucket_info(&self, bucket: &str) -> S3Result<Option<BucketInfo>> {
        let operation = GetBucketOperation::new(bucket.to_string());
        match drive(operation, self.driver_ctx.as_ref()).await {
            Ok(bucket_info) => Ok(Some(bucket_info)),
            Err(GetBucketError::NotFound) => Ok(None),
            Err(_) => Err(s3_error!(InternalError, "Failed to query bucket")),
        }
    }

    async fn build_authorization_path(
        &self,
        cx: &mut S3AccessContext<'_>,
        user_access: &UserAccess,
        action: &Action,
    ) -> S3Result<(String, AuthContext)> {
        let mut auth_context = AuthContext {
            user_id: user_access.user_identity,
            realm_id: user_access.user_identity.realm_id,
            path_restrictions: user_access.path_restrictions.clone(),
            session: None,
        };
        let Some(bucket) = cx.s3_path().get_bucket_name().map(str::to_owned) else {
            return Ok((self.group_data_path(user_access.group_id), auth_context));
        };
        let key = cx.s3_path().get_object_key().map(str::to_owned);

        let group_id = match self.find_bucket_info(&bucket).await? {
            Some(bucket_info) => {
                if bucket_info.group_id != user_access.group_id {
                    if !matches!(action, Action::Read)
                        || !anonymous_read_allowed(cx.s3_op().name())
                        || key.is_none()
                    {
                        return Err(s3_error!(
                            AccessDenied,
                            "Bucket belongs to a different group"
                        ));
                    }
                    auth_context = AuthContext::anonymous(self.realm_id);
                }
                cx.extensions_mut().insert(bucket_info.clone());
                bucket_info.group_id
            }
            None if cx.s3_op().name() == "CreateBucket" && key.is_none() => user_access.group_id,
            None => {
                return Err(s3_error!(
                    NoSuchBucket,
                    "The specified bucket does not exist."
                ));
            }
        };

        Ok((
            match key {
                Some(key) => {
                    object_permission_path(self.realm_id, group_id, self.node_id, &bucket, &key)
                }
                None => bucket_permission_path(self.realm_id, group_id, self.node_id, &bucket),
            },
            auth_context,
        ))
    }

    /// Admits a listing whose caller reaches only part of the requested path,
    /// and hands the resolved scope to the handler so the page stays inside it.
    /// A caller without any such scope keeps the ordinary denial.
    async fn admit_subpath_listing(
        &self,
        cx: &mut S3AccessContext<'_>,
        user_access: &UserAccess,
        auth_context: &AuthContext,
        path: &str,
        extras: PolicyRequestExtras,
    ) -> S3Result<()> {
        let has_bucket = cx.s3_path().get_bucket_name().is_some();
        let scope = resolve_scope(self.driver_ctx.as_ref(), user_access, path).await?;
        if scope.is_empty() {
            return Err(map_authorize_error(AuthorizeError::PermissionDenied));
        }
        // The ordinary path enforces policies after RBAC, so an admitted
        // listing runs them too instead of skipping the verdict.
        enforce_policies(
            self.driver_ctx.as_ref(),
            self.realm_id,
            &policy_request_with(path, &Permission::READ, Some(auth_context), extras),
        )
        .await
        .map_err(|error| map_authorize_error(AuthorizeError::Policy(error)))?;

        debug!(
            path = path,
            "Narrowing listing to the caller's permitted subpaths"
        );
        if has_bucket {
            cx.extensions_mut().insert(scope);
        }
        Ok(())
    }

    fn group_data_path(&self, group_id: ulid::Ulid) -> String {
        group_permission_path(self.realm_id, group_id, self.node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aruna_storage::FjallStorage;

    fn provider(path: &str) -> AuthProvider {
        let storage = FjallStorage::open(path).unwrap();
        let driver_ctx = Arc::new(DriverContext {
            storage_handle: storage,
            net_handle: None,
            blob_handle: None,
            metadata_handle: None,
            task_handle: None,
            compute_handle: None,
        });
        AuthProvider {
            driver_ctx,
            realm_id: RealmId([1u8; 32]),
            node_id: iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            encryption_key: CredentialEncryptionKey::derive(&[7u8; 32]),
            rate_limits: Arc::new(crate::rate_limit::ApiRateLimits::default()),
        }
    }

    async fn store_access(provider: &AuthProvider, access: &UserAccess) {
        use aruna_core::effects::StorageEffect;
        use aruna_core::keyspaces::USER_ACCESS_KEYSPACE;
        provider
            .driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: USER_ACCESS_KEYSPACE.to_string(),
                key: access.access_key.as_bytes().into(),
                value: access.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    async fn store_session(provider: &AuthProvider, session: &S3Session) {
        use aruna_core::effects::StorageEffect;
        use aruna_core::keyspaces::S3_SESSION_KEYSPACE;
        provider
            .driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: S3_SESSION_KEYSPACE.to_string(),
                key: session.access_key.as_bytes().into(),
                value: session.to_bytes().unwrap().into(),
                txn_id: None,
            })
            .await;
    }

    fn issued_access(provider: &AuthProvider, issued_by: [u8; 32]) -> UserAccess {
        use ulid::Ulid;
        let mut access = UserAccess {
            access_key: UserAccess::build_access_key(&Ulid::generate().to_string()).unwrap(),
            user_identity: UserId::local(Ulid::generate(), provider.realm_id),
            group_id: Ulid::generate(),
            secret: EncryptedS3Secret::empty(),
            expiry: SystemTime::now() + std::time::Duration::from_secs(3600),
            path_restrictions: None,
            issued_by,
            revoked_at: None,
        };
        access
            .encrypt_secret(
                &CredentialEncryptionKey::derive(&[7u8; 32]),
                "plaintext-secret",
            )
            .unwrap();
        access
    }

    fn issued_session(
        provider: &AuthProvider,
        issued_by: [u8; 32],
        expiry: SystemTime,
    ) -> S3Session {
        use ulid::Ulid;
        let mut session = S3Session {
            access_key: S3Session::build_access_key(&Ulid::generate().to_string()).unwrap(),
            user_identity: UserId::local(Ulid::generate(), provider.realm_id),
            group_id: Ulid::generate(),
            secret: EncryptedS3Secret::empty(),
            token_hash: S3Session::hash_token("temporary-token"),
            expiry,
            path_restrictions: None,
            issued_by,
            last_used_at: None,
            previous: None,
        };
        session
            .encrypt_secret(
                &CredentialEncryptionKey::derive(&[7u8; 32]),
                "temporary-secret",
            )
            .unwrap();
        session
    }

    #[tokio::test]
    async fn cutoff_denies_older() {
        // A user cutoff denies S3 keys issued before it and keeps later keys usable.
        use aruna_core::auth::{user_cutoff_expiry, user_cutoff_hash};
        use aruna_core::document::DocumentTarget;
        use aruna_core::effects::StorageEffect;
        use aruna_core::structs::identity::realm::{RealmConfigDocument, TokenRevocation};
        use ulid::Ulid;
        let dir = tempfile::tempdir().unwrap();
        let provider = provider(dir.path().to_str().unwrap());
        let user = UserId::local(Ulid::generate(), provider.realm_id);
        let cutoff = aruna_core::time::unix_timestamp_secs() + 60;
        let mut config = RealmConfigDocument::new(provider.realm_id, Vec::new(), 3);
        config.revoked_tokens.push(TokenRevocation {
            token_hash: user_cutoff_hash(&user),
            expires_at: user_cutoff_expiry(cutoff),
        });
        let target = DocumentTarget::RealmConfig {
            realm_id: provider.realm_id,
        };
        provider
            .driver_ctx
            .storage_handle
            .send_storage_effect(StorageEffect::Write {
                key_space: target.storage_keyspace().to_string(),
                key: target.storage_key(),
                value: config
                    .to_bytes(&aruna_core::structs::identity::auth::Actor {
                        node_id: provider.node_id,
                        user_id: user,
                        realm_id: provider.realm_id,
                    })
                    .unwrap()
                    .into(),
                txn_id: None,
            })
            .await;
        let older = Ulid::generate().to_string();
        let later = Ulid::from_parts((cutoff + 1) * 1000, 0).to_string();

        for key in [older.clone(), format!("{SESSION_ACCESS_PREFIX}{older}")] {
            let error = provider.check_cutoff(&key, &user).await.unwrap_err();
            assert_eq!(error.code(), &s3s::S3ErrorCode::AccessDenied);
        }
        provider.check_cutoff(&later, &user).await.unwrap();
        let other = UserId::local(Ulid::generate(), provider.realm_id);
        provider.check_cutoff(&older, &other).await.unwrap();
    }

    #[tokio::test]
    async fn rejects_legacy_key() {
        // Legacy `{ulid}@{ulid}:workspace-{ulid}` ids must fail before any lookup.
        let dir = tempfile::tempdir().unwrap();
        let provider = provider(dir.path().to_str().unwrap());
        let legacy = "01ARZ3NDEKTSV4RRFFQ69G5FAV@01ARZ3NDEKTSV4RRFFQ69G5FAW:workspace-01ARZ3";
        let error = provider.get_secret_key(legacy).await.unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidAccessKeyId);
    }

    #[tokio::test]
    async fn revoked_spends_nothing() {
        // A revoked credential must be rejected before it charges the owner's
        // shared rate budget, so a replayed revoked key cannot starve the owner.
        let dir = tempfile::tempdir().unwrap();
        let mut provider = provider(dir.path().to_str().unwrap());
        provider.rate_limits = Arc::new(crate::rate_limit::ApiRateLimits::for_test(1));

        let live = issued_access(&provider, *provider.node_id.as_bytes());
        let revoked = UserAccess {
            revoked_at: Some(SystemTime::now()),
            ..live.clone()
        };
        let Err(error) = provider.admit_credential(&revoked) else {
            panic!("revoked credential admitted");
        };
        assert_eq!(error.code(), &s3s::S3ErrorCode::AccessDenied);

        assert!(provider.admit_credential(&live).is_ok());
    }

    #[tokio::test]
    async fn decrypts_on_issuer() {
        let dir = tempfile::tempdir().unwrap();
        let provider = provider(dir.path().to_str().unwrap());

        let local = issued_access(&provider, *provider.node_id.as_bytes());
        store_access(&provider, &local).await;
        let secret = provider.get_secret_key(&local.access_key).await.unwrap();
        assert_eq!(secret.expose(), "plaintext-secret");

        // A record issued by another node (a copied DB) yields no usable secret.
        let foreign = issued_access(&provider, [9u8; 32]);
        store_access(&provider, &foreign).await;
        let error = provider
            .get_secret_key(&foreign.access_key)
            .await
            .unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidAccessKeyId);
    }

    #[tokio::test]
    async fn token_required_exact() {
        let dir = tempfile::tempdir().unwrap();
        let provider = provider(dir.path().to_str().unwrap());
        let now = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000);
        let session = issued_session(
            &provider,
            *provider.node_id.as_bytes(),
            now + std::time::Duration::from_secs(600),
        );
        let uri = Uri::from_static("/");

        let missing = request_token_hash(&HeaderMap::new(), &uri).unwrap_err();
        assert_eq!(
            missing.code(),
            &s3s::S3ErrorCode::MissingAuthenticationToken
        );

        let mut headers = HeaderMap::new();
        headers.insert("x-amz-security-token", "wrong-token".parse().unwrap());
        let mismatch = request_token_hash(&headers, &uri).unwrap();
        let Err(error) = provider.admit_session(&session, &mismatch, now) else {
            panic!("mismatched session token admitted");
        };
        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidToken);

        headers.insert("x-amz-security-token", "temporary-token".parse().unwrap());
        let exact = request_token_hash(&headers, &uri).unwrap();
        assert!(provider.admit_session(&session, &exact, now).is_ok());
        let Err(expired) = provider.admit_session(&session, &exact, session.expiry) else {
            panic!("expired session admitted");
        };
        assert_eq!(expired.code(), &s3s::S3ErrorCode::ExpiredToken);
    }

    #[tokio::test]
    async fn session_never_degrades() {
        let dir = tempfile::tempdir().unwrap();
        let provider = provider(dir.path().to_str().unwrap());
        let mut long_lived = issued_access(&provider, *provider.node_id.as_bytes());
        long_lived.access_key =
            S3Session::build_access_key(&ulid::Ulid::generate().to_string()).unwrap();
        long_lived.secret = EncryptedS3Secret::empty();
        long_lived
            .encrypt_secret(&provider.encryption_key, "long-lived-secret")
            .unwrap();
        store_access(&provider, &long_lived).await;

        let error = provider
            .get_secret_key(&long_lived.access_key)
            .await
            .unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidAccessKeyId);
    }

    #[test]
    fn query_token_supported() {
        let headers = HeaderMap::new();
        let uri = Uri::from_static("/?X-Amz-Security-Token=temporary-token");
        assert_eq!(
            request_token_hash(&headers, &uri).unwrap(),
            S3Session::hash_token("temporary-token")
        );

        let duplicate = Uri::from_static("/?X-Amz-Security-Token=one&X-Amz-Security-Token=two");
        let error = request_token_hash(&headers, &duplicate).unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidToken);
    }

    #[tokio::test]
    async fn refreshed_keeps_signed() {
        // A request signed before a refresh carries the old token and verifies with the old secret.
        use aruna_core::structs::identity::s3_session::PreviousCredential;
        let dir = tempfile::tempdir().unwrap();
        let provider = provider(dir.path().to_str().unwrap());
        let now = SystemTime::now();
        let node = *provider.node_id.as_bytes();
        let mut old = issued_session(&provider, node, now + std::time::Duration::from_secs(120));
        old.token_hash = S3Session::hash_token("previous-token");
        old.encrypt_secret(
            &CredentialEncryptionKey::derive(&[7u8; 32]),
            "previous-secret",
        )
        .unwrap();
        let mut session = S3Session {
            expiry: now + std::time::Duration::from_secs(3_600),
            ..old.clone()
        };
        session.token_hash = S3Session::hash_token("temporary-token");
        session
            .encrypt_secret(
                &CredentialEncryptionKey::derive(&[7u8; 32]),
                "temporary-secret",
            )
            .unwrap();
        session.previous = Some(PreviousCredential {
            secret: old.secret,
            token_hash: old.token_hash.clone(),
            expiry: old.expiry,
        });
        store_session(&provider, &session).await;

        for (token, expected) in [
            (Some(old.token_hash.clone()), "previous-secret"),
            (Some(session.token_hash.clone()), "temporary-secret"),
            (None, "temporary-secret"),
        ] {
            let secret = with_request_token(token, provider.get_secret_key(&session.access_key))
                .await
                .unwrap();
            assert_eq!(secret.expose(), expected);
        }
        assert!(
            provider
                .admit_session(&session, &old.token_hash, now)
                .is_ok()
        );
    }

    #[tokio::test]
    async fn wrong_node_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let provider = provider(dir.path().to_str().unwrap());
        let expiry = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(2_000);
        let local = issued_session(&provider, *provider.node_id.as_bytes(), expiry);
        store_session(&provider, &local).await;
        assert_eq!(
            provider
                .get_secret_key(&local.access_key)
                .await
                .unwrap()
                .expose(),
            "temporary-secret"
        );
        let foreign = issued_session(&provider, [9u8; 32], expiry);
        store_session(&provider, &foreign).await;

        let error = provider
            .get_secret_key(&foreign.access_key)
            .await
            .unwrap_err();
        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidAccessKeyId);
        let Err(error) = provider.admit_session(
            &foreign,
            &S3Session::hash_token("temporary-token"),
            SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_000),
        ) else {
            panic!("foreign session admitted");
        };
        assert_eq!(error.code(), &s3s::S3ErrorCode::InvalidAccessKeyId);
    }
}
#[test]
fn subpath_operations_limited() {
    for operation in [
        "ListBuckets",
        "HeadBucket",
        "GetBucketLocation",
        "ListObjects",
        "ListObjectsV2",
    ] {
        assert!(is_listing_operation(operation));
    }
    assert!(!is_listing_operation("PutObject"));
    assert!(!is_listing_operation("GetObject"));
}

#[cfg(test)]
mod token_tests {
    use super::*;

    /// The hex of `canary-token-key-7a2c-0000-00000`, a token no log or error may show.
    const CANARY: &str = "63616e6172792d746f6b656e2d6b65792d376132632d303030302d3030303030";
    const SIGNED: &str = "AWS4-HMAC-SHA256 Credential=TOKENKEY/20261005/us-east-1/s3/aws4_request, \
        SignedHeaders=host;x-amz-date;x-amz-security-token, Signature=00";
    const UNSIGNED: &str = "AWS4-HMAC-SHA256 Credential=TOKENKEY/20261005/us-east-1/s3/aws4_request, \
        SignedHeaders=host;x-amz-date, Signature=00";

    fn headers(authorization: &str, tokens: &[&str]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let value = http::HeaderValue::from_str(authorization).unwrap();
        headers.insert(http::header::AUTHORIZATION, value);
        for token in tokens {
            headers.append(TOKEN_HEADER, http::HeaderValue::from_str(token).unwrap());
        }
        headers
    }

    fn checked(headers: &HeaderMap, uri: &'static str) -> S3Result<Option<TokenCredential>> {
        credential_token(headers, &Uri::from_static(uri), "TOKENKEY")
    }

    fn refused(result: S3Result<Option<TokenCredential>>) -> bool {
        result.is_err_and(|error| error.code() == &s3s::S3ErrorCode::InvalidToken)
    }

    #[test]
    fn signed_token_accepted() {
        let token = checked(&headers(SIGNED, &[CANARY]), "/bucket/key")
            .unwrap()
            .unwrap();
        assert_eq!(token.access_key, "TOKENKEY");
        assert_eq!(*TokenCredential::encode(token.token.bytes()), CANARY);
        // A long-lived key without a token stays an ordinary credential.
        assert!(
            checked(&headers(SIGNED, &[]), "/bucket/key")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn unsigned_token_refused() {
        assert!(refused(checked(
            &headers(UNSIGNED, &[CANARY]),
            "/bucket/key"
        )));
        assert!(refused(checked(
            &headers(SIGNED, &[CANARY, CANARY]),
            "/bucket/key"
        )));
        assert!(refused(checked(
            &headers(SIGNED, &["not-a-token"]),
            "/bucket/key"
        )));
        let short = &CANARY[..62];
        assert!(refused(checked(&headers(SIGNED, &[short]), "/bucket/key")));
    }

    #[test]
    fn presigned_token_refused() {
        // A token in the query of a long-lived key is refused before s3s logs the URI.
        let query = Uri::from_static(
            "/bucket/key?X-Amz-Credential=TOKENKEY%2F20261005%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Security-Token=secret&X-Amz-Signature=00",
        );
        assert!(query_token_refused(&HeaderMap::new(), &query));
        let header_auth = Uri::from_static("/bucket/key?X-Amz-Security-Token=secret");
        assert!(query_token_refused(&headers(SIGNED, &[]), &header_auth));
        assert!(query_token_refused(&HeaderMap::new(), &header_auth));
        // Session keys keep their presigned tokens; requests without a query token pass.
        let session = Uri::from_static(
            "/bucket/key?X-Amz-Credential=ASIAKEY%2F20261005%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Security-Token=secret",
        );
        assert!(!query_token_refused(&HeaderMap::new(), &session));
        let plain = Uri::from_static("/bucket/key?X-Amz-Signature=00");
        assert!(!query_token_refused(&HeaderMap::new(), &plain));
        // The access check refuses a presigned request with a header token, and a query token.
        let presigned = "/bucket/key?X-Amz-Signature=00";
        assert!(refused(checked(&headers(SIGNED, &[CANARY]), presigned)));
        let token_query = "/bucket/key?X-Amz-Security-Token=00";
        assert!(refused(checked(&headers(SIGNED, &[]), token_query)));
    }

    #[test]
    fn session_tokens_preserved() {
        let plain = Uri::from_static("/bucket/key");
        assert!(!header_token_refused(&headers(SIGNED, &[CANARY]), &plain));
        let session = SIGNED.replace("TOKENKEY", "ASIAKEY");
        assert!(!header_token_refused(&headers(&session, &[CANARY]), &plain));
        let unsigned = UNSIGNED.replace("TOKENKEY", "ASIAKEY");
        assert!(!header_token_refused(
            &headers(&unsigned, &[CANARY]),
            &plain
        ));
        assert!(!header_token_refused(
            &headers("AWS ASIAKEY:00", &[CANARY]),
            &plain
        ));
        let query = Uri::from_static("/bucket/key?X-Amz-Security-Token=session-token");
        assert!(!query_token_refused(&headers(&session, &[]), &query));
        let presigned = Uri::from_static(
            "/bucket/key?X-Amz-Credential=ASIAKEY%2F20261005%2Fus-east-1%2Fs3%2Faws4_request\
             &X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=00",
        );
        let mut token = HeaderMap::new();
        token.insert(
            TOKEN_HEADER,
            http::HeaderValue::from_static("session-token"),
        );
        assert!(!header_token_refused(&token, &presigned));
        let legacy =
            Uri::from_static("/bucket/key?AWSAccessKeyId=ASIAKEY&Signature=00&Expires=2000000000");
        assert!(!header_token_refused(&token, &legacy));
        assert!(header_token_refused(&headers(UNSIGNED, &[CANARY]), &plain));
        assert!(header_token_refused(
            &headers("AWS TOKENKEY:00", &[CANARY]),
            &plain
        ));
    }

    #[test]
    fn tokens_never_logged() {
        let mut request = http::Request::builder()
            .uri("/bucket/key")
            .header(http::header::AUTHORIZATION, SIGNED)
            .header(TOKEN_HEADER, CANARY)
            .body(())
            .unwrap();
        assert!(format!("{request:?}").contains(CANARY));
        hide_tokens(request.headers_mut());
        let formatted = format!("{request:?}");
        assert!(!formatted.contains(CANARY), "{formatted}");
        // Signing still sees the value; only formatting hides it.
        assert!(signs_header(request.headers(), TOKEN_HEADER));
        let kept = request.headers().get(TOKEN_HEADER).unwrap();
        assert_eq!(kept.as_bytes(), CANARY.as_bytes());
        // Refusals and the admitted credential show no token either.
        let error = checked(&headers(UNSIGNED, &[CANARY]), "/bucket/key").unwrap_err();
        let token = checked(&headers(SIGNED, &[CANARY]), "/bucket/key").unwrap();
        for formatted in [
            format!("{error:?}"),
            error.to_string(),
            format!("{token:?}"),
        ] {
            assert!(!formatted.contains(CANARY), "{formatted}");
        }
    }

    /// Padded base64 of `canary-object-key-5e1b-0000-0000`.
    const OBJECT_KEY: &str = "Y2FuYXJ5LW9iamVjdC1rZXktNWUxYi0wMDAwLTAwMDA=";

    fn keyed(authorization: &str, uri: &'static str, operation: &str) -> S3Result<bool> {
        let mut headers = headers(authorization, &[]);
        headers.insert(OBJECT_KEY_HEADER, OBJECT_KEY.parse().unwrap());
        request_object_key(&headers, &Uri::from_static(uri), operation).map(|key| key.is_some())
    }

    #[test]
    fn object_key_signed() {
        let signed = "AWS4-HMAC-SHA256 Credential=KEY/20261005/us-east-1/s3/aws4_request, \
            SignedHeaders=host;x-amz-date;x-aruna-object-key, Signature=00";
        assert!(keyed(signed, "/bucket/key", "GetObject").unwrap());
        // Unsigned, presigned or on another operation, the header is refused.
        let denied = |result: S3Result<bool>| {
            result.is_err_and(|error| error.code() == &s3s::S3ErrorCode::AccessDenied)
        };
        assert!(denied(keyed(UNSIGNED, "/bucket/key", "GetObject")));
        assert!(denied(keyed(
            signed,
            "/bucket/key?X-Amz-Signature=00",
            "GetObject"
        )));
        assert!(denied(keyed(signed, "/bucket/key", "HeadObject")));
        let mut headers = headers(signed, &[]);
        headers.insert(OBJECT_KEY_HEADER, "c2hvcnQ=".parse().unwrap());
        let uri = Uri::from_static("/bucket/key");
        assert!(request_object_key(&headers, &uri, "GetObject").is_err());
        // Formatting a request shows no object key once hidden.
        headers.insert(OBJECT_KEY_HEADER, OBJECT_KEY.parse().unwrap());
        hide_tokens(&mut headers);
        assert!(!format!("{headers:?}").contains(OBJECT_KEY));
    }
}
