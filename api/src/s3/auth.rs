use super::util::get_s3_operation_permission;
use aruna_core::structs::UserAccess;
use aruna_operations::driver::DriverContext;
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
    async fn get_secret_key(&self, _access_key: &str) -> S3Result<SecretKey> {
        //let user_access = self._query_user_access(access_key).await?;
        //Ok(SecretKey::from(user_access.secret))
        Ok(SecretKey::from("SECRET_KEY"))
    }
}

#[async_trait::async_trait]
impl S3Access for AuthProvider {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // Evaluate action from S3 operation name
        let _action = get_s3_operation_permission(cx.s3_op().name())
            .ok_or_else(|| s3_error!(InvalidRequest, "Unknown Operation"))?;

        //TODO: Fetch user access -> GetUserAccess state machine

        //TODO: Check permissions on path+action -> CheckPermissions state machine

        let authorized = true;
        match authorized {
            true => {
                //TODO: Insert user access into context to provide for service functions
                //cx.extensions_mut().insert(user_access);
                Ok(())
            }
            false => Err(s3_error!(UnauthorizedAccess, "Permission denied")),
        }
    }
}

impl AuthProvider {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn _query_user_access(&self, _access_key_id: &str) -> S3Result<UserAccess> {
        //TODO: Query user access with state machine
        unimplemented!()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn _query_content_hash(&self, _path: &str) -> S3Result<[u8; 32]> {
        //TODO: Query content hash with state machine
        unimplemented!()
    }
}
