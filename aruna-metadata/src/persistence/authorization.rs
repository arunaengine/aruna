use aruna_permission::{Action, OidcToken, Path, ResourceId, UserIdentity};
use aruna_storage::storage::store::Store;
use tracing::{error, trace};
use ulid::Ulid;

use crate::error::ArunaMetadataError;

use super::{
    persistence::{Persistor, tables::*},
    search::search::Search,
};

#[async_trait::async_trait]
pub trait Authorize {
    async fn authorize(
        &self,
        user_identity: Option<UserIdentity>,
        action: Action,
        resource_id: Ulid,
    ) -> Result<Option<(UserIdentity, Path)>, ArunaMetadataError>;
}

impl<Se, St> Persistor<St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
{
    pub async fn check_public(&self, resource_id: &Ulid) -> Result<bool, ArunaMetadataError> {
        let store = self.store.clone();
        let id = resource_id.to_bytes();
        tokio::task::spawn_blocking(move || -> Result<bool, ArunaMetadataError> {
            let txn = store.create_txn(false)?;

            let mapping = store.get(&txn, RESOURCE_MAPPINGS_DB_NAME, id.as_slice())?;

            Ok(mapping.is_some())
        })
        .await
        .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))?
    }

    pub async fn check_path(
        &self,
        path: &Path,
        identity: &UserIdentity,
        action: Action,
    ) -> Result<bool, ArunaMetadataError> {
        Ok(self
            .permission_manager
            .check_path(identity, path, action, &self.store)
            .await?)
    }

    pub async fn get_identity(&self, token: String) -> Result<UserIdentity, ArunaMetadataError> {
        let store = self.store.clone();
        let token_handler = self.token_handler.clone();
        // TODO: Query user from kademlia
        tokio::task::spawn_blocking(move || -> Result<UserIdentity, ArunaMetadataError> {
            let txn = store.create_txn(false)?;
            let user_identity = token_handler.read().get_identity(&token, &store, &txn)?;
            store.commit(txn)?;
            Ok(user_identity)
        })
        .await
        .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))?
    }

    pub async fn check_oidc_token(&self, token: String) -> Result<OidcToken, ArunaMetadataError> {
        let store = self.store.clone();
        let token_handler = self.token_handler.clone();
        tokio::task::spawn_blocking(move || -> Result<OidcToken, ArunaMetadataError> {
            let txn = store.create_txn(false)?;
            let token = token_handler.read().verify_oidc_token(&token)?;
            let exists = token_handler
                .read()
                .get_user_from_oidc(&token.iss, &token.sub, &store, &txn)?
                .is_some();
            if !exists {
                trace!("User does not exist");
                return Err(ArunaMetadataError::Unauthorized);
            }
            store.commit(txn)?;
            Ok(token)
        })
        .await
        .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))?
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_user_groups(
        &self,
        user_identity: &UserIdentity,
    ) -> Result<Vec<Ulid>, ArunaMetadataError> {
        let store = self.store.clone();
        let perm_manager = self.permission_manager.clone();
        let user_identity = user_identity.clone();
        let perm_ulid = tokio::task::spawn_blocking(move || -> Result<Ulid, ArunaMetadataError> {
            let txn = store.create_txn(false)?;
            let perm_ulid = perm_manager.resolve_permission_ulid(&user_identity, &store, &txn);
            store.commit(txn)?;
            Ok(perm_ulid?)
        })
        .await
        .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))??;

        self.permission_manager
            .get_roles_for_permission(&perm_ulid.to_string())
            .await
            .iter()
            .flatten()
            .filter_map(|r| match r.split_once("_") {
                Some((id, _)) => Some(id),
                None => None,
            })
            .map(|id| -> Result<Ulid, ArunaMetadataError> {
                Ulid::from_string(id).map_err(|_e| ArunaMetadataError::ConversionError {
                    from: "String".to_string(),
                    to: "Ulid".to_string(),
                })
            })
            .collect::<Result<Vec<Ulid>, ArunaMetadataError>>()
    }

    pub async fn check_is_group(&self, group_id: Ulid) -> Result<bool, ArunaMetadataError> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || -> Result<bool, ArunaMetadataError> {
            let txn = store.create_txn(false)?;
            let byte_id = group_id.to_bytes();
            let is_group = store
                .get(&txn, GROUPS_DB_NAME, byte_id.as_slice())?
                .is_some();
            store.commit(txn)?;
            Ok(is_group)
        })
        .await
        .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))?
    }
}

#[async_trait::async_trait]
impl<St, Se> Authorize for Persistor<St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
{
    async fn authorize(
        &self,
        user_identity: Option<UserIdentity>,
        action: Action,
        resource_id: Ulid,
    ) -> Result<Option<(UserIdentity, Path)>, ArunaMetadataError> {
        match user_identity {
            Some(user_identity) => {
                let path = self
                    .permission_manager
                    .check_permission(
                        &user_identity,
                        ResourceId::Ulid(resource_id),
                        action,
                        &self.store,
                    )
                    .await?;
                Ok(Some((user_identity, path)))
            }
            None => match self.check_public(&resource_id).await? {
                true => Ok(None),
                false => Err(ArunaMetadataError::Unauthorized),
            },
        }
    }
}
