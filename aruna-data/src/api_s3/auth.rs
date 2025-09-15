use crate::api_s3::util::get_s3_operation_permission;
use crate::controller::controller::Controller;
use crate::io::io_handler::tables::{ACCESS_DB_NAME, PATH_LOCATION_DB_NAME};
use anyhow::anyhow;
use aruna_permission::UserIdentity;
use aruna_permission::paths::PathBuilder;
use aruna_storage::storage::store::Store;
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::path::S3Path;
use s3s::{S3Result, s3_error};
use serde::{Deserialize, Serialize};
use tracing::debug;
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAccess {
    pub user_id: UserIdentity,
    pub group_id: Ulid,
    pub secret: String,
    //pub expiry: SystemTime
    //filter
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModifyAccess {
    pub user_id: UserIdentity,
    pub group_id: Ulid,
    pub bucket: String,
    pub key: Option<String>,
    pub modifier: Access,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Access {
    Read,
    Write,
}

#[derive(Clone)]
pub struct AuthProvider<St>
where
    for<'a> St: Store<'a> + 'static,
{
    pub(crate) controller: Controller<St>,
}

#[async_trait::async_trait]
impl<St> S3Auth for AuthProvider<St>
where
    for<'a> St: Store<'a> + 'static,
{
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        let user_access = self.query_user_access(access_key.to_string()).await?;
        Ok(SecretKey::from(user_access.secret))
    }
}

#[async_trait::async_trait]
impl<St> S3Access for AuthProvider<St>
where
    for<'a> St: Store<'a> + 'static,
{
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // Fetch user access
        let user_access = self
            .query_user_access(
                cx.credentials()
                    .ok_or_else(|| {
                        s3_error!(UnauthorizedAccess, "No credentials provided in request")
                    })?
                    .access_key
                    .to_string(),
            )
            .await?;
        debug!(?user_access);

        // Evaluate action from operation name
        let action = get_s3_operation_permission(cx.s3_op().name())
            .ok_or_else(|| s3_error!(InvalidRequest, "Unknown Operation"))?;

        let builder = PathBuilder::new().realm_id(self.controller.network.get_realm_key());
        let perm_path = match cx.s3_path() {
            S3Path::Root => builder.group_admin(user_access.group_id),
            S3Path::Bucket { bucket } => {
                builder.group_data_bucket_wildcard(user_access.group_id, bucket.to_string())
            }
            S3Path::Object { bucket, key } => {
                debug!("Bucket: {}, Key: {}", bucket, key);
                let kkey = key.to_string(); // For real?
                let path = std::path::Path::new(&kkey);
                let parent = path.parent().ok_or_else(|| s3_error!(InvalidKeyPath))?;

                builder.group_data_key_prefix_wildcard(
                    user_access.group_id,
                    bucket.to_string(),
                    parent
                        .to_path_buf()
                        .into_os_string()
                        .into_string()
                        .map_err(|_| s3_error!(InvalidKeyPath))?,
                )
            }
        }
        .build()
        .map_err(|e| s3_error!(InternalError, "{}", e))?;

        debug!(?perm_path);

        let allowed = self
            .controller
            .io_handler
            .permission_manager
            .check_path(
                &user_access.user_id,
                &perm_path,
                action,
                &self.controller.io_handler.store,
            )
            .await
            .map_err(|e| s3_error!(InternalError, "Permission check failed: {}", e))?;
        debug!(allowed);

        match allowed {
            true => {
                cx.extensions_mut().insert(user_access);
                Ok(())
            }
            false => Err(s3_error!(UnauthorizedAccess, "Permission denied")),
        }
    }
}

impl<St> AuthProvider<St>
where
    for<'a> St: Store<'a> + 'static,
{
    #[tracing::instrument(level = "trace", skip(self))]
    async fn query_user_access(&self, access_key_id: String) -> S3Result<UserAccess> {
        let store_clone = self.controller.io_handler.store.clone();
        let user_access = tokio::task::spawn_blocking(move || {
            let read_txn = store_clone
                .create_txn(false)
                .map_err(|e| s3_error!(InternalError, "{}", e))?;

            // Fetch access info for user
            let info: UserAccess = match store_clone.get(
                &read_txn,
                ACCESS_DB_NAME,
                &Ulid::from_string(&access_key_id)?.to_bytes(),
            )? {
                Some(info_raw) => postcard::from_bytes(&*info_raw)?,
                None => {
                    return Err(anyhow!("No access info found"));
                }
            };

            store_clone.commit(read_txn)?;

            Ok::<UserAccess, anyhow::Error>(info)
        })
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))?
        .map_err(|e| s3_error!(InternalError, "{}", e))?;

        Ok(user_access)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn _query_content_hash(&self, path: String) -> S3Result<[u8; 32]> {
        let store_clone = self.controller.io_handler.store.clone();
        let content_hash = tokio::task::spawn_blocking(move || {
            let read_txn = store_clone
                .create_txn(false)
                .map_err(|e| s3_error!(InternalError, "{}", e))?;

            // Fetch content hash associated with provided path
            let mut arr: [u8; 32] = [0; 32];
            arr.copy_from_slice(
                &store_clone
                    .get(&read_txn, PATH_LOCATION_DB_NAME, path.as_bytes())
                    .map_err(|e| s3_error!(InternalError, "{}", e))?
                    .ok_or_else(|| s3_error!(NoSuchKey, "No such key"))?,
            );

            Ok::<[u8; 32], anyhow::Error>(arr)
        })
        .await
        .map_err(|e| s3_error!(InternalError, "{}", e))?
        .map_err(|e| s3_error!(InternalError, "{}", e))?;

        Ok(content_hash)
    }
}
