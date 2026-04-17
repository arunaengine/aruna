use super::util::get_s3_operation_permission;
use aruna_core::NodeId;
use aruna_core::structs::{AuthContext, BucketInfo, Permission, RealmId, UserAccess};
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
        // Evaluate action from S3 operation name
        let action = get_s3_operation_permission(cx.s3_op().name())
            .ok_or_else(|| s3_error!(InvalidRequest, "Unknown Operation"))?;

        // Fetch user access -> GetUserAccess state machine
        let access_key_id = &cx
            .credentials()
            .ok_or(s3_error!(
                AccessDenied,
                "Credentials missing in access context"
            ))?
            .access_key;
        let user_access = self.query_user_access(access_key_id).await?;

        if user_access.issued_by != *self.node_id.as_bytes() {
            return Err(s3_error!(InvalidAccessKeyId, "Credential issuer mismatch"));
        }

        if user_access.is_revoked() {
            return Err(s3_error!(AccessDenied, "Credential has been revoked"));
        }

        if user_access.is_expired(SystemTime::now()) {
            return Err(s3_error!(AccessDenied, "Credential has expired"));
        }

        let required_permission = match action {
            Action::Read => Permission::READ,
            Action::Write => Permission::WRITE,
        };

        let path = self.build_authorization_path(cx, &user_access).await?;

        let allowed = drive(
            CheckPermissionsOperation::new(CheckPermissionsConfig {
                auth_context: AuthContext {
                    user_id: user_access.user_identity.user_id,
                    realm_id: user_access.user_identity.realm_key.clone(),
                    path_restrictions: user_access.path_restrictions.clone(),
                },
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

        cx.extensions_mut().insert(user_access);
        Ok(())
    }
}

impl AuthProvider {
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
    ) -> S3Result<String> {
        let Some(bucket) = cx.s3_path().get_bucket_name().map(str::to_owned) else {
            return Ok(self.group_data_path(user_access.group_id));
        };
        let key = cx.s3_path().get_object_key().map(str::to_owned);

        let group_id = match self.find_bucket_info(&bucket).await? {
            Some(bucket_info) => {
                if bucket_info.group_id != user_access.group_id {
                    return Err(s3_error!(
                        AccessDenied,
                        "Bucket belongs to a different group"
                    ));
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

        let mut path = self.bucket_path(group_id, &bucket);
        if let Some(key) = key {
            path.push('/');
            path.push_str(&key);
        }
        Ok(path)
    }

    fn bucket_path(&self, group_id: ulid::Ulid, bucket: &str) -> String {
        format!(
            "/{}/g/{}/data/{}/{bucket}",
            self.realm_id, group_id, self.node_id
        )
    }

    fn group_data_path(&self, group_id: ulid::Ulid) -> String {
        format!("/{}/g/{}/data/{}", self.realm_id, group_id, self.node_id)
    }
}
