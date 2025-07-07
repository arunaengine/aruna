use aruna_permission::{Action, OidcToken, Path, ResourceId, UserIdentity};
use aruna_storage::storage::store::Store;
use tracing::{Instrument, error};
use ulid::Ulid;

use crate::{error::ArunaMetadataError, logerr};

use super::{
    persistor::{Persistor, tables::*},
    search::generic::Search,
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
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn check_public(&self, resource_id: &Ulid) -> Result<bool, ArunaMetadataError> {
        let store = self.store.clone();
        let id = resource_id.to_bytes();
        let current_span = tracing::Span::current();
        tokio::task::spawn_blocking(move || -> Result<bool, ArunaMetadataError> {
            current_span.in_scope(|| {
                let txn = store.create_txn(false)?;

                let mapping = store.get(&txn, RESOURCE_MAPPINGS_DB_NAME, id.as_slice())?;

                Ok(mapping.is_some())
            })
        })
        .await
        .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))?
    }

    #[tracing::instrument(level = "trace", skip(self))]
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

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_identity(&self, token: String) -> Result<UserIdentity, ArunaMetadataError> {
        let store = self.store.clone();
        let token_handler = self.token_handler.clone();
        // TODO: Query user from kademlia
        let current_span = tracing::Span::current();
        tokio::task::spawn_blocking(move || -> Result<UserIdentity, ArunaMetadataError> {
            current_span.in_scope(|| {
                let txn = store.create_txn(false)?;
                let user_identity = token_handler.write().get_identity(&token, &store, &txn)?;
                store.commit(txn)?;
                Ok(user_identity)
            })
        })
        .await
        .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))?
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn check_oidc_token(
        &self,
        token: String,
    ) -> Result<(OidcToken, Option<Ulid>), ArunaMetadataError> {
        let store = self.store.clone();
        let token_handler = self.token_handler.clone();
        let current_span = tracing::Span::current();
        tokio::task::spawn_blocking(
            move || -> Result<(OidcToken, Option<Ulid>), ArunaMetadataError> {
                current_span.in_scope(|| {
                    let txn = store.create_txn(false).map_err(logerr!())?;
                    let token = token_handler
                        .write()
                        .verify_oidc_token(&token)
                        .map_err(logerr!())?;
                    let existing = token_handler
                        .read()
                        .get_user_from_oidc(&token.iss, &token.sub, &store, &txn)
                        .map_err(logerr!())?;
                    store.commit(txn).map_err(logerr!())?;
                    Ok((token, existing))
                })
            },
        )
        .await
        .map_err(|e| {
            error!(?e);
            ArunaMetadataError::ServerError(e.to_string())
        })?
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_user_groups(
        &self,
        user_identity: &UserIdentity,
    ) -> Result<Vec<Ulid>, ArunaMetadataError> {
        let store = self.store.clone();
        let perm_manager = self.permission_manager.clone();
        let user_identity = user_identity.clone();
        let current_span = tracing::Span::current();
        let perm_ulid = tokio::task::spawn_blocking(move || -> Result<Ulid, ArunaMetadataError> {
            current_span.in_scope(|| {
                let txn = store.create_txn(false)?;
                let perm_ulid = perm_manager.resolve_permission_ulid(&user_identity, &store, &txn);
                store.commit(txn)?;
                Ok(perm_ulid?)
            })
        })
        .await
        .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))??;

        self.permission_manager
            .get_roles_for_permission(&perm_ulid.to_string())
            .await
            .iter()
            .flatten()
            .filter_map(|r| r.split_once("_").map(|(id, _)| id))
            .map(|id| -> Result<Ulid, ArunaMetadataError> {
                Ulid::from_string(id).map_err(|_e| ArunaMetadataError::ConversionError {
                    from: "String".to_string(),
                    to: "Ulid".to_string(),
                })
            })
            .collect::<Result<Vec<Ulid>, ArunaMetadataError>>()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn check_is_group(&self, group_id: Ulid) -> Result<bool, ArunaMetadataError> {
        let store = self.store.clone();

        let current_span = tracing::Span::current();
        tokio::task::spawn_blocking(move || -> Result<bool, ArunaMetadataError> {
            current_span.in_scope(|| {
                let txn = store.create_txn(false)?;
                let byte_id = group_id.to_bytes();
                let is_group = store
                    .get(&txn, GROUPS_DB_NAME, byte_id.as_slice())?
                    .is_some();
                store.commit(txn)?;
                Ok(is_group)
            })
        })
        .await
        .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))?
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn create_token(
        &self,
        user: &UserIdentity,
        expiration_hours: Option<u64>,
    ) -> Result<String, ArunaMetadataError> {
        if let Some(expiration_hours) = expiration_hours {
            self.token_handler
                .read()
                .generate_token_with_expiration(user, expiration_hours)
                .map_err(|e| {
                    error!(?e);
                    ArunaMetadataError::ServerError(e.to_string())
                })
        } else {
            self.token_handler.read().generate_token(user).map_err(|e| {
                error!(?e);
                ArunaMetadataError::ServerError(e.to_string())
            })
        }
    }
}

#[async_trait::async_trait]
impl<St, Se> Authorize for Persistor<St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
{
    #[tracing::instrument(level = "trace", skip(self))]
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
