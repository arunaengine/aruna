use super::util::get_s3_operation_permission;
use aruna_core::structs::UserAccess;
use aruna_operations::driver::{DriverContext, drive};
use aruna_operations::s3::get_user_access::{GetUserAccessError, GetUserAccessOperation};
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::Arc;

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
    pub(crate) _driver_ctx: Arc<DriverContext>,
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
        let _action = get_s3_operation_permission(cx.s3_op().name())
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

        //TODO: Check permissions on path+action -> CheckPermissions state machine

        let authorized = true;
        match authorized {
            true => cx.extensions_mut().insert(user_access),
            false => return Err(s3_error!(UnauthorizedAccess, "Permission denied")),
        };
        Ok(())
    }
}

impl AuthProvider {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn query_user_access(&self, access_key_id: &str) -> S3Result<UserAccess> {
        let operation = GetUserAccessOperation::new(access_key_id.to_string());
        match drive(operation, self._driver_ctx.as_ref()).await {
            Ok(Some(Ok(user_access))) => Ok(user_access),
            Ok(None) => Err(s3_error!(
                InvalidAccessKeyId,
                "The Access Key Id you provided does not exist in our records."
            )),
            Ok(Some(Err(err))) | Err(err) => match err {
                GetUserAccessError::NotFound => Err(s3_error!(
                    InvalidAccessKeyId,
                    "The Access Key Id you provided does not exist in our records."
                )),
                _ => Err(s3_error!(InternalError, "Failed to query user access")),
            },
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn _query_content_hash(&self, _path: &str) -> S3Result<[u8; 32]> {
        //TODO: Query content hash with state machine
        unimplemented!()
    }
}
