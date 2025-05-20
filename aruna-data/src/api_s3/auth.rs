use std::sync::Arc;
use anyhow::anyhow;
use aruna_storage::storage::store::Store;
use s3s::{s3_error, S3Result};
use s3s::auth::{S3Auth, SecretKey};
use serde::{Deserialize, Serialize};
use crate::io::io_handler::{ObjectInfo, ACCESS_DB_NAME, LOCATION_DB_NAME, PATH_LOCATION_DB_NAME};

pub struct Credentials {
    key_id: String,
    secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAccess {
    pub user_id: String,
    pub group_id: String,
    pub secret: String,
    //credentials: Credentials,
    //filter
}

pub struct AuthProvider<St>
where
    for<'a> St: Store<'a>,
{
    pub(crate) store: Arc<St>,
}

#[async_trait::async_trait]
impl<St> S3Auth for AuthProvider<St>
where
    for<'a> St: Store<'a> + 'static,
{
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        let store_clone = self.store.clone();
        let access_key_clone = access_key.to_string();
        let secret_key = tokio::task::spawn_blocking(move || {
            let mut read_txn = store_clone
                .create_txn(false)
                .map_err(|e| s3_error!(InternalError, "{}", e))?;

            // Fetch access info for user
            let info: UserAccess = if let Some(info_raw) =
                store_clone.get(&mut read_txn, ACCESS_DB_NAME, access_key_clone.as_bytes())?
            {
                bincode::serde::decode_from_slice(&*info_raw, bincode::config::standard())?.0
            } else {
                return Err(anyhow!("No access info found"));
            };

            Ok::<SecretKey, anyhow::Error>(SecretKey::from(info.secret))
        })
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e))?
            .map_err(|e| s3_error!(InternalError, "{}", e))?;
        
        Ok(secret_key)
    }
}
