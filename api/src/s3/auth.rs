use super::s3_server::S3OpLabel;
use super::util::{get_s3_operation_permission, is_anonymous_object_read_operation};
use aruna_core::structs::{
    AuthContext, BucketInfo, Permission, RealmId, UserAccess, blob_bucket_permission_path,
    blob_group_permission_path, blob_object_permission_path,
};
use aruna_core::{NodeId, UserId};
use aruna_operations::check_permissions::{CheckPermissionsConfig, CheckPermissionsOperation};
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::get_bucket_info::{GetBucketInfoError, GetBucketInfoOperation};
use aruna_operations::s3::get_user_access::{GetUserAccessError, GetUserAccessOperation};
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Result, s3_error};
use serde::{Deserialize, Serialize};
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
}

#[async_trait::async_trait]
impl S3Auth for AuthProvider {
    async fn get_secret_key(&self, access_key_id: &str) -> S3Result<SecretKey> {
        let user_access = self.query_user_access(access_key_id).await?;
        Ok(SecretKey::from(user_access.secret))
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
        let access_key_id = match cx.credentials() {
            Some(credentials) => credentials.access_key.clone(),
            None => return self.check_anonymous(cx, action).await,
        };

        // Fetch user access -> GetUserAccess state machine
        let user_access = self.query_user_access(&access_key_id).await?;

        if user_access.issued_by != *self.node_id.as_bytes() {
            return Err(s3_error!(InvalidAccessKeyId, "Credential issuer mismatch"));
        }

        if user_access.is_revoked() {
            return Err(s3_error!(AccessDenied, "Credential has been revoked"));
        }

        if user_access.is_expired(SystemTime::now()) {
            return Err(s3_error!(AccessDenied, "Credential has expired"));
        }

        let required_permission = match &action {
            Action::Read => Permission::READ,
            Action::Write => Permission::WRITE,
        };

        let (path, auth_context) = self
            .build_authorization_path(cx, &user_access, &action)
            .await?;

        // DeleteObjects lists its target keys in the request body rather than the
        // URL, so the path resolves to the bucket and a bucket-level check would
        // deny prefix-scoped tokens outright. Defer object authorization to the
        // handler, which checks each entry individually. Credentials, issuer,
        // expiry, revocation and bucket ownership are already validated above, so
        // anonymous and cross-group requests still fail closed here.
        if cx.s3_op().name() != "DeleteObjects" {
            let allowed = drive(
                CheckPermissionsOperation::new(CheckPermissionsConfig {
                    auth_context,
                    path,
                    required_permission,
                }),
                self.driver_ctx.as_ref(),
            )
            .await
            .map_err(|_| s3_error!(InternalError, "Failed to check permissions"))?;

            if !allowed {
                return Err(s3_error!(AccessDenied, "Permission denied"));
            }
        }

        cx.extensions_mut().insert(user_access);
        Ok(())
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

        let allowed = drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: AuthContext::anonymous(self.realm_id),
                path,
                required_permission: Permission::READ,
            }),
            self.driver_ctx.as_ref(),
        )
        .await
        .map_err(|_| s3_error!(InternalError, "Failed to check permissions"))?;

        if !allowed {
            return Err(s3_error!(AccessDenied, "Permission denied"));
        }

        // Handlers read UserAccess/BucketInfo from the request extensions;
        // hand them the Everyone principal scoped to the bucket's group. The
        // key/secret fields are blank — nothing downstream signs with them —
        // and expiry is irrelevant because this access was just checked.
        cx.extensions_mut().insert(bucket_info);
        cx.extensions_mut().insert(UserAccess {
            access_key: String::new(),
            user_identity: UserId::nil(self.realm_id),
            group_id,
            secret: String::new(),
            expiry: SystemTime::now(),
            path_restrictions: None,
            issued_by: *self.node_id.as_bytes(),
            revoked_at: None,
        });
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn query_user_access(&self, access_key_id: &str) -> S3Result<UserAccess> {
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
