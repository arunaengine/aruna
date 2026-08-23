use super::s3_server::S3OpLabel;
use super::util::{get_s3_operation_permission, is_anonymous_object_read_operation};
use crate::rate_limit::{LocalKey, LocalLease, LocalPermit};
use aruna_core::credential_seal::{CredentialSealKey, SealedS3Secret};
use aruna_core::errors::StorageError;
use aruna_core::structs::{
    AuthContext, BucketInfo, Permission, RealmId, S3Session, UserAccess,
    blob_bucket_permission_path, blob_group_permission_path, blob_object_permission_path,
};
use aruna_core::{NodeId, UserId};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::get_realm_config::GetRealmConfigOperation;
use aruna_operations::request_authorization::{AuthorizeError, authorize};
use aruna_operations::request_policy::PolicyRequestExtras;
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::get_user_access::{GetUserAccessError, GetUserAccessOperation};
use aruna_operations::s3::session::{
    GetS3SessionOperation, S3SessionError, TouchS3SessionConfig, TouchS3SessionOperation,
};
use http::{HeaderMap, Uri};
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::SystemTime;

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
    pub(crate) seal_key: CredentialSealKey,
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
            let secret = session.open_secret(&self.seal_key).map_err(|_| {
                s3_error!(
                    InvalidAccessKeyId,
                    "The Access Key Id you provided does not exist in our records."
                )
            })?;
            return Ok(SecretKey::from(secret));
        }
        let user_access = self.query_user_access(access_key_id).await?;
        // Secrets seal at rest with an issuer-local key, so only the issuing
        // node can recover the plaintext s3s needs to verify a signature. A
        // record copied to another node, or with a rebound field, never opens.
        if user_access.issued_by != *self.node_id.as_bytes() {
            return Err(s3_error!(
                InvalidAccessKeyId,
                "The Access Key Id you provided does not exist in our records."
            ));
        }
        let secret = user_access.open_secret(&self.seal_key).map_err(|_| {
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
        let action = get_s3_operation_permission(&operation_name)
            .ok_or_else(|| s3_error!(InvalidRequest, "Unknown Operation"))?;

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
            let user_access = self.query_user_access(&access_key_id).await?;
            let permit = self.admit_credential(&user_access)?;
            (user_access, permit, None)
        };
        let lease = cx
            .extensions_mut()
            .get::<LocalLease>()
            .cloned()
            .unwrap_or_default();
        lease.replace(permit);
        cx.extensions_mut().insert(lease);

        // Credentials are issuer-local and sealed at rest: s3s only had a secret
        // to verify this signature because `get_secret_key` unsealed it on the
        // issuing node. Confirm that node is still a member of this realm.
        if !self.issuer_in_realm(&user_access.issued_by).await? {
            return Err(s3_error!(
                InvalidAccessKeyId,
                "Credential issuer not in realm"
            ));
        }

        let required_permission = match &action {
            Action::Read => Permission::READ,
            Action::Write => Permission::WRITE,
        };

        let (path, auth_context) = self
            .build_authorization_path(cx, &user_access, &action)
            .await?;

        // The policy request context is built once: ordinary authorization uses a
        // clone and the original is stashed so per-object and secondary-resource
        // handlers evaluate against the real query and allowlisted headers.
        let extras = request_extras(cx, &operation_name);

        // DeleteObjects keys live in the body, so per-object RBAC/policy checks stay in
        // the handler against one loaded policy set. Prior credential, issuer, expiry,
        // revocation, and ownership checks keep anonymous/cross-group requests fail-closed.
        if cx.s3_op().name() != "DeleteObjects" {
            authorize(
                self.driver_ctx.as_ref(),
                self.realm_id,
                &auth_context,
                &path,
                &required_permission,
                extras.clone(),
            )
            .await
            .map_err(map_authorize_error)?;
        }

        if let Some(token_hash) = session_token_hash {
            drive(
                TouchS3SessionOperation::new(TouchS3SessionConfig {
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
        Ok(())
    }
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

impl AuthProvider {
    /// Anonymous access: object bytes only, addressed to a concrete object, and
    /// allowed only when a public role — one assigned to the Everyone principal
    /// — grants READ on the object permission path. The bucket's own group
    /// scopes that path, so the authenticated flow's group-ownership check has
    /// no analogue here.
    async fn check_anonymous(&self, cx: &mut S3AccessContext<'_>, action: Action) -> S3Result<()> {
        if !matches!(action, Action::Read) || !is_anonymous_object_read_operation(cx.s3_op().name())
        {
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

        let path =
            blob_object_permission_path(self.realm_id, group_id, self.node_id, &bucket, &key);

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

        // Handlers read UserAccess/BucketInfo from the request extensions;
        // hand them the Everyone principal scoped to the bucket's group. The
        // key/secret fields are blank — nothing downstream signs with them —
        // and expiry is irrelevant because this access was just checked.
        cx.extensions_mut().insert(extras);
        cx.extensions_mut().insert(bucket_info);
        cx.extensions_mut().insert(UserAccess {
            access_key: String::new(),
            user_identity: UserId::nil(self.realm_id),
            group_id,
            secret: SealedS3Secret::empty(),
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
        if !session.token_matches(token_hash) {
            return Err(s3_error!(InvalidToken, "Invalid session token"));
        }
        self.admit_credential_at(&session.as_user_access(), now)
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
        let operation = GetUserAccessOperation::new(access_key_id.to_string());
        match drive(operation, self.driver_ctx.as_ref())
            .await
            .and_then(|result| result.transpose())
        {
            Ok(Some(user_access)) => Ok(user_access),
            Ok(None) | Err(GetUserAccessError::NotFound) => Err(s3_error!(
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
            GetS3SessionOperation::new(access_key_id.to_string()),
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

    /// Whether `issued_by` is a node configured in this realm. Credentials are
    /// issuer-local and sealed with an issuer-local key, so a verified signature
    /// already proves this serving node issued them; this only confirms the
    /// issuing node is still a realm member.
    async fn issuer_in_realm(&self, issued_by: &[u8; 32]) -> S3Result<bool> {
        let config = drive(
            GetRealmConfigOperation::new(self.realm_id),
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
        let operation = GetBucketInfoOperation::new(bucket.to_string());
        match drive(operation, self.driver_ctx.as_ref())
            .await
            .and_then(|result| result.transpose())
        {
            Ok(Some(bucket_info)) => Ok(Some(bucket_info)),
            Ok(None) | Err(GetBucketInfoError::NotFound) => Ok(None),
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
        };
        let Some(bucket) = cx.s3_path().get_bucket_name().map(str::to_owned) else {
            return Ok((self.group_data_path(user_access.group_id), auth_context));
        };
        let key = cx.s3_path().get_object_key().map(str::to_owned);

        let group_id = match self.find_bucket_info(&bucket).await? {
            Some(bucket_info) => {
                if bucket_info.group_id != user_access.group_id {
                    if !matches!(action, Action::Read)
                        || !is_anonymous_object_read_operation(cx.s3_op().name())
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
                Some(key) => blob_object_permission_path(
                    self.realm_id,
                    group_id,
                    self.node_id,
                    &bucket,
                    &key,
                ),
                None => blob_bucket_permission_path(self.realm_id, group_id, self.node_id, &bucket),
            },
            auth_context,
        ))
    }

    fn group_data_path(&self, group_id: ulid::Ulid) -> String {
        blob_group_permission_path(self.realm_id, group_id, self.node_id)
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
            seal_key: CredentialSealKey::derive(&[7u8; 32]),
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

    fn sealed_access(provider: &AuthProvider, issued_by: [u8; 32]) -> UserAccess {
        use ulid::Ulid;
        let mut access = UserAccess {
            access_key: UserAccess::build_access_key(&Ulid::generate().to_string()).unwrap(),
            user_identity: UserId::local(Ulid::generate(), provider.realm_id),
            group_id: Ulid::generate(),
            secret: SealedS3Secret::empty(),
            expiry: SystemTime::now() + std::time::Duration::from_secs(3600),
            path_restrictions: None,
            issued_by,
            revoked_at: None,
        };
        access
            .seal_secret(&CredentialSealKey::derive(&[7u8; 32]), "unsealed-secret")
            .unwrap();
        access
    }

    fn sealed_session(
        provider: &AuthProvider,
        issued_by: [u8; 32],
        expiry: SystemTime,
    ) -> S3Session {
        use ulid::Ulid;
        let mut session = S3Session {
            access_key: S3Session::build_access_key(&Ulid::generate().to_string()).unwrap(),
            user_identity: UserId::local(Ulid::generate(), provider.realm_id),
            group_id: Ulid::generate(),
            secret: SealedS3Secret::empty(),
            token_hash: S3Session::hash_token("temporary-token"),
            expiry,
            path_restrictions: None,
            issued_by,
            last_used_at: None,
        };
        session
            .seal_secret(&CredentialSealKey::derive(&[7u8; 32]), "temporary-secret")
            .unwrap();
        session
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

        let live = sealed_access(&provider, *provider.node_id.as_bytes());
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
    async fn unseals_on_issuer() {
        let dir = tempfile::tempdir().unwrap();
        let provider = provider(dir.path().to_str().unwrap());

        let local = sealed_access(&provider, *provider.node_id.as_bytes());
        store_access(&provider, &local).await;
        let secret = provider.get_secret_key(&local.access_key).await.unwrap();
        assert_eq!(secret.expose(), "unsealed-secret");

        // A record issued by another node (a copied DB) yields no usable secret.
        let foreign = sealed_access(&provider, [9u8; 32]);
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
        let session = sealed_session(
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
        let mut long_lived = sealed_access(&provider, *provider.node_id.as_bytes());
        long_lived.access_key =
            S3Session::build_access_key(&ulid::Ulid::generate().to_string()).unwrap();
        long_lived.secret = SealedS3Secret::empty();
        long_lived
            .seal_secret(&provider.seal_key, "long-lived-secret")
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
    async fn wrong_node_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let provider = provider(dir.path().to_str().unwrap());
        let expiry = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(2_000);
        let local = sealed_session(&provider, *provider.node_id.as_bytes(), expiry);
        store_session(&provider, &local).await;
        assert_eq!(
            provider
                .get_secret_key(&local.access_key)
                .await
                .unwrap()
                .expose(),
            "temporary-secret"
        );
        let foreign = sealed_session(&provider, [9u8; 32], expiry);
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
