use super::{
    search::search::Search,
    utils::{create_mappings, idx_from_cow, update_mappings, visiblity_from_doc},
};
use crate::{
    error::ArunaMetadataError,
    models::models::{Group, Resource, User},
    persistence::persistence::tables::*,
};
use ahash::RandomState;
use aruna_permission::{
    Action, OidcToken, Path, PermissionManager, ResourceId, TokenSystem, UserIdentity,
    manager::CreateGroupPrepare, token::OidcTrustConfig,
};
use aruna_storage::storage::store::Store;
use automerge::{ActorId, AutoCommit, sync::State};
use autosurgeon::reconcile;
use iroh::PublicKey;
use parking_lot::{Mutex, RwLock};
use roaring::RoaringBitmap;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU32},
};
use tracing::error;
use ulid::Ulid;

#[async_trait::async_trait]
pub trait Authorize {
    async fn authorize(
        &self,
        token: Option<String>,
        action: Action,
        resource_id: Ulid,
    ) -> Result<Option<(UserIdentity, Path)>, ArunaMetadataError>;
}

pub mod tables {
    pub const RESOURCE_DB_NAME: &str = "resources";
    pub const RESOURCE_MAPPINGS_DB_NAME: &str = "resource_mappings";
    pub const USER_DB_NAME: &str = "users";
    pub const GROUPS_DB_NAME: &str = "groups";
    pub const USER_MAPPINGS_DB_NAME: &str = "user_mappings";
    pub const PUBLIC_MAPPINGS_DB_NAME: &str = "public_mappings";
}

pub struct Persistor<St, Se>
where
    for<'a> St: Store<'a>,
    Se: Search,
{
    pub(super) store: St,
    pub(super) search: Arc<Se>,
    pub(super) idx_counter: Arc<AtomicU32>,
    pub(super) connection_states:
        Arc<Mutex<HashMap<Ulid, HashMap<PublicKey, State, RandomState>, RandomState>>>,
    pub(super) permission_manager: PermissionManager,
    pub token_handler: Arc<RwLock<TokenSystem>>,
}

impl<Se, St> Persistor<St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
{
    #[tracing::instrument(level = "trace", skip(store_config, search_config))]
    pub async fn new(
        res_sdx: tokio::sync::mpsc::Sender<(u32, Resource)>,
        store_config: <St as Store<'static>>::StoreConfig,
        search_config: Se::SearchConfig,
        realm_key: [u8; 32],
        oidc_trust_config: OidcTrustConfig,
    ) -> Result<Self, ArunaMetadataError> {
        let store = St::new(store_config)?;

        let permission_manager = PermissionManager::new().await?;
        let token_handler = Arc::new(RwLock::new(TokenSystem::new(realm_key, oidc_trust_config)));

        // TODO: Init perm/auth

        let search = tokio::task::spawn_blocking(move || {
            let search = Se::new(search_config)?;
            Ok::<Arc<Se>, ArunaMetadataError>(Arc::new(search))
        });

        let (idx_sdx, idx_rcv) = tokio::sync::oneshot::channel();

        let store_clone = store.clone();
        tokio::task::spawn_blocking(move || {
            let mut txn = store_clone.create_txn(true)?;

            let resource_iter = store_clone.iter_db(&txn, RESOURCE_DB_NAME)?;
            for resource in resource_iter {
                let (id, res) = resource;
                let idx = store_clone
                    .get(&txn, RESOURCE_MAPPINGS_DB_NAME, id.as_ref())?
                    .ok_or_else(|| {
                        ArunaMetadataError::ServerError("No valid mapping found".to_string())
                    })?;

                let doc = automerge::AutoCommit::load(res.as_ref())?;
                let resource: Resource = autosurgeon::hydrate(&doc)?;

                let idx = u32::from_be_bytes(idx.as_ref().try_into().map_err(|_| {
                    ArunaMetadataError::ConversionError {
                        from: "Cow<'_, [u8]>".to_string(),
                        to: "&[u8;4]".to_string(),
                    }
                })?);
                // new idx because this is a local only sorting
                res_sdx
                    .blocking_send((idx, resource))
                    .map_err(|e| ArunaMetadataError::DatabaseError(e.to_string()))?;
            }

            let idx: u32 = store_clone
                .iter_db(&txn, RESOURCE_MAPPINGS_DB_NAME)
                .map_err(|e| ArunaMetadataError::DatabaseError(e.to_string()))?
                .last()
                .map(|(_, idx)| u32::from_be_bytes(idx.as_ref().try_into().unwrap()))
                .unwrap_or(0);
            idx_sdx
                .send(idx)
                .map_err(|e| ArunaMetadataError::DatabaseError(e.to_string()))?;

            if store_clone
                .get(&txn, PUBLIC_MAPPINGS_DB_NAME, &Ulid::default().to_bytes())?
                .is_none()
            {
                let mut vec = Vec::new();
                RoaringBitmap::new().serialize_into(&mut vec)?;

                store_clone.put(
                    &mut txn,
                    PUBLIC_MAPPINGS_DB_NAME,
                    &Ulid::default().to_bytes(),
                    &vec,
                )?;
            }

            store_clone
                .commit(txn)
                .map_err(|e| ArunaMetadataError::DatabaseError(e.to_string()))?;
            Ok::<(), ArunaMetadataError>(())
        });

        let search = search.await.unwrap().unwrap();

        let idx_counter = Arc::new(AtomicU32::new(idx_rcv.await.unwrap()));

        Ok(Self {
            store,
            search,
            idx_counter,
            connection_states: Arc::new(Mutex::new(HashMap::default())),
            permission_manager,
            token_handler,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn add_resource(
        &self,
        node_key: &[u8; 32],
        user: &UserIdentity,
        path: Path,
        resource: Resource,
    ) -> Result<AutoCommit, ArunaMetadataError> {
        let store = self.store.clone();
        let permission_manager = self.permission_manager.clone();
        let user_id = user.user_ulid;
        let actor_id = ActorId::from(
            [
                user.user_ulid.to_bytes().as_slice(),
                user.realm_key.as_slice(),
                node_key.as_slice(),
            ]
            .concat(),
        );

        // spawn search in its own block and dont await outcome
        let idx = self
            .idx_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let res_clone = resource.clone();

        let res = tokio::task::spawn_blocking(move || {
            let mut write_txn = store.create_txn(true)?;

            let mut doc = automerge::AutoCommit::new().with_actor(actor_id);
            autosurgeon::reconcile(&mut doc, resource.clone())?;
            let res = doc.save();

            store.put(
                &mut write_txn,
                RESOURCE_DB_NAME,
                &resource.id.to_bytes(),
                &res,
            )?;
            store.put(
                &mut write_txn,
                RESOURCE_MAPPINGS_DB_NAME,
                &resource.id.to_bytes(),
                &idx.to_be_bytes(),
            )?;
            create_mappings(&store, &mut write_txn, resource, &user_id, idx)?;
            permission_manager.add_resource(
                ResourceId::Ulid(res_clone.id),
                &path,
                &store,
                &mut write_txn,
            )?;

            store.commit(write_txn)?;

            Ok::<AutoCommit, ArunaMetadataError>(doc)
        })
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;

        self.search.add_resource(idx, res_clone).await?;

        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_resource(&self, resource_id: Ulid) -> Result<Resource, ArunaMetadataError> {
        let store = self.store.clone();
        let result = tokio::task::spawn_blocking(move || {
            let read_txn = store.create_txn(false)?;
            let byte_id = resource_id.to_bytes();
            let Some(res) = store.get(&read_txn, RESOURCE_DB_NAME, &byte_id)? else {
                return Err(ArunaMetadataError::NotFound(format!(
                    "{resource_id} not found"
                )));
            };

            let doc = automerge::AutoCommit::load(res.as_ref())?;
            let resource: Resource = autosurgeon::hydrate(&doc)?;

            store.commit(read_txn)?;
            Ok(resource)
        })
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn add_user(
        &self,
        node_key: &[u8; 32],
        user_name: String,
        oidc_token: OidcToken,
    ) -> Result<(User, AutoCommit), ArunaMetadataError> {
        let store = self.store.clone();
        let token_handler = self.token_handler.clone();
        let permission_handler = self.permission_manager.clone();
        let iss = oidc_token.iss;
        let sub = oidc_token.sub;
        let node_key = node_key.clone();

        let result = tokio::task::spawn_blocking(move || {
            let mut write_txn = store.create_txn(true)?;
            let user_identity = token_handler.read().register_user_from_oidc_claims(
                &iss,
                &sub,
                &store,
                &mut write_txn,
            )?;
            let actor_id = ActorId::from(
                [
                    user_identity.user_ulid.to_bytes().as_slice(),
                    user_identity.realm_key.as_slice(),
                    node_key.as_slice(),
                ]
                .concat(),
            );

            let user = User {
                id: user_identity.user_ulid,
                realm_key: user_identity.realm_key,
                name: user_name,
            };

            // Serialize into &[u8]
            let id = user.id.to_bytes();

            let mut doc = automerge::AutoCommit::new().with_actor(actor_id);

            autosurgeon::reconcile(&mut doc, &user)?;
            let doc_vec = doc.save();

            let mut bitmap = Vec::new();
            RoaringBitmap::new().serialize_into(&mut bitmap)?;

            // Write in store
            store.put(&mut write_txn, USER_DB_NAME, &id, &doc_vec)?;
            store.put(&mut write_txn, USER_MAPPINGS_DB_NAME, &id, &bitmap)?;
            permission_handler.create_user_identity(&user_identity, &store, &mut write_txn)?;

            // Commit
            store.commit(write_txn)?;
            Ok::<(User, AutoCommit), ArunaMetadataError>((user, doc))
        })
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_user(&self, id: &Ulid) -> Result<Option<User>, ArunaMetadataError> {
        let store = self.store.clone();
        let id = *id;
        let result = tokio::task::spawn_blocking(move || {
            let read_txn = store.create_txn(false)?;
            let id = id.to_bytes();
            let user = store.get(&read_txn, USER_DB_NAME, &id)?;
            let user = if let Some(user) = user {
                let doc = automerge::AutoCommit::load(user.as_ref())?;
                let user: User = autosurgeon::hydrate(&doc)?;

                Some(user)
            } else {
                None
            };
            store.commit(read_txn)?;
            Ok::<Option<User>, ArunaMetadataError>(user)
        })
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn search(
        &self,
        user: Option<Ulid>,
        query: String,
    ) -> Result<Vec<Resource>, ArunaMetadataError> {
        let store = self.store.clone();
        let search = self.search.clone();
        let result = tokio::task::spawn_blocking(move || {
            let read_txn = store.create_txn(false)?;
            let id = Ulid::default().to_bytes();
            let Some(bytes) = store.get(&read_txn, PUBLIC_MAPPINGS_DB_NAME, &id)? else {
                return Err(ArunaMetadataError::DatabaseError(
                    "Public universe not found".to_string(),
                ));
            };
            let mut universe = RoaringBitmap::deserialize_from(bytes.as_ref())?;
            if let Some(id) = user {
                let id = id.to_bytes();
                if let Some(byte_universe) = store.get(&read_txn, USER_MAPPINGS_DB_NAME, &id)? {
                    let user_universe = RoaringBitmap::deserialize_from(byte_universe.as_ref())?;
                    universe |= user_universe;
                }
            };

            let ids = search.search::<Self>(universe, query)?;

            let mut result = Vec::new();
            for res in ids {
                if let Some(res) = store.get(&read_txn, RESOURCE_DB_NAME, &res.to_bytes())? {
                    let doc = automerge::AutoCommit::load(res.as_ref())?;
                    let resource: Resource = autosurgeon::hydrate(&doc)?;
                    result.push(resource);
                }
            }

            store.commit(read_txn)?;
            Ok(result)
        })
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn update_resource(
        &self,
        node_key: &[u8; 32],
        user_identity: &UserIdentity,
        resource: Resource,
    ) -> Result<AutoCommit, ArunaMetadataError> {
        // Clones for spawn_blocking
        let store = self.store.clone();
        let search = self.search.clone();
        let user_id = user_identity.user_ulid;
        let resource_id = resource.id.to_bytes();
        let res_clone = resource.clone();

        // Generate actor id
        let actor_id = ActorId::from(
            [
                user_id.to_bytes().as_slice(),
                user_identity.realm_key.as_slice(),
                node_key.as_slice(),
            ]
            .concat(),
        );

        let (result, idx) = tokio::task::spawn_blocking(
            move || -> Result<(AutoCommit, u32), ArunaMetadataError> {
                let mut write_txn = store.create_txn(true)?;

                let idx = idx_from_cow(
                    store
                        .get(&write_txn, RESOURCE_MAPPINGS_DB_NAME, &resource_id)?
                        .ok_or_else(|| {
                            ArunaMetadataError::NotFound(format!("{} not found", resource.id))
                        })?,
                )?;

                let res = store
                    .get(&write_txn, RESOURCE_DB_NAME, &resource_id)?
                    .ok_or_else(|| {
                        ArunaMetadataError::NotFound(format!("{} not found", resource.id))
                    })?;

                let mut doc = automerge::AutoCommit::load(res.as_ref())?.with_actor(actor_id);
                let visibility = visiblity_from_doc(&doc)?;
                reconcile(&mut doc, resource.clone())?;
                doc.commit();
                let res = doc.save();

                store.put(
                    &mut write_txn,
                    RESOURCE_DB_NAME,
                    &resource.id.to_bytes(),
                    &res,
                )?;
                store.put(
                    &mut write_txn,
                    RESOURCE_MAPPINGS_DB_NAME,
                    &resource.id.to_bytes(),
                    &idx.to_be_bytes(),
                )?;

                if visibility != resource.visibility {
                    update_mappings(&store, &mut write_txn, resource, &user_id, idx)?;
                }

                store.commit(write_txn)?;
                Ok::<(AutoCommit, u32), ArunaMetadataError>((doc, idx))
            },
        )
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;

        search.add_resource(idx, res_clone).await?;

        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn add_group(
        &self,
        node_key: &[u8; 32],
        realm_key: &[u8; 32],
        user: &UserIdentity,
        group: Group,
    ) -> Result<AutoCommit, ArunaMetadataError> {
        // Clones for spawn_blocking
        let store = self.store.clone();
        let permission = self.permission_manager.clone();
        let group_id = group.id;
        let user = user.clone();
        let realm_key = realm_key.clone();

        // Generate actor id
        let actor_id = ActorId::from(
            [
                user.user_ulid.to_bytes().as_slice(),
                &user.realm_key,
                node_key.as_slice(),
            ]
            .concat(),
        );

        let (commit_handle, result) = tokio::task::spawn_blocking(
            move || -> Result<(CreateGroupPrepare, AutoCommit), ArunaMetadataError> {
                let mut write_txn = store.create_txn(true)?;

                // Serialize into &[u8]
                let id = group.id.to_bytes();

                let mut doc = automerge::AutoCommit::new().with_actor(actor_id);

                autosurgeon::reconcile(&mut doc, group)?;
                let group_doc = doc.save();

                // let mut bitmap = Vec::new();
                // RoaringBitmap::new().serialize_into(&mut bitmap)?;

                // Write in store
                store.put(&mut write_txn, GROUPS_DB_NAME, &id, &group_doc)?;

                let handle = permission.create_group_prepare(
                    group_id,
                    &user,
                    realm_key,
                    &store,
                    &mut write_txn,
                )?;

                //TODO: Group mappings:
                // store.put(&mut write_txn, USER_MAPPINGS_DB_NAME, &id, &bitmap)?;

                // Commit
                store.commit(write_txn)?;
                Ok::<(CreateGroupPrepare, AutoCommit), ArunaMetadataError>((handle, doc))
            },
        )
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;

        self.permission_manager
            .create_group_commit(commit_handle)
            .await?;

        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn add_user_to_group(
        &self,
        node_key: &[u8; 32],
        user: &UserIdentity,
        group: Ulid,
    ) -> Result<AutoCommit, ArunaMetadataError> {
        todo!()
    }

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
        let store = self.store.clone();
        let permission_manager = self.permission_manager.clone();
        let identity = identity.clone();

        let ulid = tokio::task::spawn_blocking(move || -> Result<Ulid, ArunaMetadataError> {
            let txn = store.create_txn(false)?;

            let permission_ulid =
                permission_manager.resolve_permission_ulid(&identity, &store, &txn)?;

            store.commit(txn)?;

            Ok(permission_ulid)
        })
        .await
        .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))??;
        println!("{}", ulid.to_string());
        self.permission_manager
            .enforcer
            .read()
            .await
            .enforce(&ulid.to_string(), &path.to_string(), &action.to_string())
            .map_err(|e| {
                error!(?e);
                ArunaMetadataError::Unauthorized
            })
    }

    pub async fn get_identity(&self, token: String) -> Result<UserIdentity, ArunaMetadataError> {
        let store = self.store.clone();
        let token_handler = self.token_handler.clone();
        tokio::task::spawn_blocking(move || -> Result<UserIdentity, ArunaMetadataError> {
            let mut txn = store.create_txn(false)?;
            let user_identity = token_handler
                .read()
                .get_identity(&token, &store, &mut txn)?;
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
            if exists {
                return Err(ArunaMetadataError::Unauthorized);
            }
            store.commit(txn)?;
            Ok(token)
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
        token: Option<String>,
        action: Action,
        resource_id: Ulid,
    ) -> Result<Option<(UserIdentity, Path)>, ArunaMetadataError> {
        match token {
            Some(token) => {
                let store = self.store.clone();
                let token_handler = self.token_handler.clone();
                let user_identity = tokio::task::spawn_blocking(
                    move || -> Result<UserIdentity, ArunaMetadataError> {
                        let mut txn = store.create_txn(false)?;
                        let user_identity = token_handler
                            .read()
                            .get_identity(&token, &store, &mut txn)?;
                        store.commit(txn)?;
                        Ok(user_identity)
                    },
                )
                .await
                .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))??;
                let cloned_user_identity = user_identity.clone();
                let store = self.store.clone();
                let permission_manager = self.permission_manager.clone();

                let path = permission_manager
                    .check_permission(
                        &cloned_user_identity,
                        ResourceId::Ulid(resource_id),
                        action,
                        &store,
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
