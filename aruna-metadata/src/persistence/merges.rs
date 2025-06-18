use std::collections::HashMap;

use super::{
    persistence::{Persistor, tables::*},
    search::search::Search,
    utils::{create_mappings, group_from_path, idx_from_cow, update_mappings, visiblity_from_doc},
};
use crate::{
    error::ArunaMetadataError,
    models::models::{Group, HandleHelper, Resource, User},
    network::network_trait::ReplicationSubject,
};
use aruna_permission::{Path, ResourceId, UserIdentity};
use aruna_storage::storage::store::Store;
use automerge::{
    AutoCommit,
    sync::{Message, State, SyncDoc},
};
use autosurgeon::hydrate;
use iroh::PublicKey;
use roaring::RoaringBitmap;
use tracing::warn;
use ulid::Ulid;

impl<Se, St> Persistor<St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
{
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn generate_sync_message(
        &self,
        doc_id: &Ulid,
        doc: &mut AutoCommit,
        node: PublicKey,
    ) -> Result<Option<Vec<u8>>, ArunaMetadataError> {
        let mut lock = self.connection_states.lock();

        let msg = match lock.get_mut(&doc_id) {
            Some(persisted) => {
                let mut state = persisted.entry(node).or_insert(State::new());
                let msg = doc
                    .sync()
                    .generate_sync_message(&mut state)
                    .map(|msg| msg.encode());
                msg
            }
            None => {
                let mut state = State::new();
                let mut map = HashMap::default();
                let msg = doc
                    .sync()
                    .generate_sync_message(&mut state)
                    .map(|msg| msg.encode());
                map.insert(node, state.clone());
                lock.insert(*doc_id, map);
                msg
            }
        };
        drop(lock);
        Ok(msg)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn receive_sync_message(
        &self,
        doc_id: &Ulid,
        doc: &mut AutoCommit,
        message: Message,
        node: PublicKey,
    ) -> Result<(), ArunaMetadataError> {
        let mut lock = self.connection_states.lock();

        match lock.get_mut(&doc_id) {
            Some(persisted) => {
                let mut state = persisted.entry(node).or_insert(State::new());
                doc.sync().receive_sync_message(&mut state, message)?;
            }
            None => {
                let mut state = State::new();
                let mut map = HashMap::default();
                doc.sync().receive_sync_message(&mut state, message)?;
                map.insert(node, state);
                lock.insert(*doc_id, map);
            }
        };
        drop(lock);
        Ok(())
    }
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn handle_replication(
        &self,
        node: PublicKey,
        subject_id: Ulid,
        path: Path,
        msg: ReplicationSubject,
        doc: &mut AutoCommit,
    ) -> Result<Option<Vec<u8>>, ArunaMetadataError> {
        let response = match msg {
            crate::network::network_trait::ReplicationSubject::User(message) => {
                if let Some(message) = &message {
                    self.receive_sync_message(
                        &subject_id,
                        doc,
                        Message::decode(message)?,
                        node.clone(),
                    )
                    .await?;
                }
                let response = self
                    .generate_sync_message(&subject_id, doc, node.clone())
                    .await?;
                if response.is_none() && message.is_none() {
                    self.handle_user_merges(doc.save()).await?;
                }
                response
            }
            crate::network::network_trait::ReplicationSubject::Object(message) => {
                if let Some(message) = &message {
                    self.receive_sync_message(
                        &subject_id,
                        doc,
                        Message::decode(message)?,
                        node.clone(),
                    )
                    .await?;
                }
                let response = self
                    .generate_sync_message(&subject_id, doc, node.clone())
                    .await?;
                if response.is_none() && message.is_none() {
                    self.handle_object_merges(path, doc.save()).await?;
                }
                response
            }
            crate::network::network_trait::ReplicationSubject::Group(message) => {
                if let Some(message) = &message {
                    self.receive_sync_message(
                        &subject_id,
                        doc,
                        Message::decode(message)?,
                        node.clone(),
                    )
                    .await?;
                }
                let response = self
                    .generate_sync_message(&subject_id, doc, node.clone())
                    .await?;
                if response.is_none() && message.is_none() {
                    self.handle_group_merges(doc.save()).await?;
                }
                response
            }
        };
        Ok(response)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn handle_group_merges(&self, doc: Vec<u8>) -> Result<(), ArunaMetadataError> {
        let store = self.store.clone();
        let permission = self.permission_manager.clone();

        let handles = tokio::task::spawn_blocking(
            move || -> Result<Vec<HandleHelper>, ArunaMetadataError> {
                let mut wtxn = store.create_txn(true)?;

                // Parse document, actor_id and ulid
                let mut doc = automerge::AutoCommit::load(doc.as_ref())?;
                let foreign_group: Group = autosurgeon::hydrate(&doc)?;
                let ulid_bytes = foreign_group.id.to_bytes();

                // Decide if merge or create
                let (mut merged_resource, handle) =
                    match store.get(&wtxn, GROUPS_DB_NAME, &ulid_bytes)? {
                        Some(existing_doc) => {
                            let mut existing_group =
                                automerge::AutoCommit::load(existing_doc.as_ref())?;
                            existing_group.merge(&mut doc)?;
                            //TODO: Add users & admins to perm handler
                            (existing_group, vec![])
                        }
                        None => {
                            let mut handles = vec![];
                            let Some((admin, _)) = foreign_group
                                .members
                                .iter()
                                .filter(|(_, r)| r.iter().any(|r| r == &"admin"))
                                .next()
                            else {
                                warn!("No admin found in group");
                                return Ok(vec![]);
                            };
                            let identity = UserIdentity::from_string(admin.clone())?;
                            let handle = permission.create_group_prepare(
                                foreign_group.id,
                                &identity,
                                foreign_group.realm_key,
                                &store,
                                &mut wtxn,
                            )?;

                            let path = Path::builder()
                                .realm_id(foreign_group.realm_key)
                                .group(foreign_group.id)
                                .build()
                                .map_err(|e| ArunaMetadataError::ServerError(e.to_string()))?;

                            permission.add_resource(
                                ResourceId::Ulid(foreign_group.id),
                                &path,
                                &store,
                                &mut wtxn,
                            )?;
                            handles.push(HandleHelper::AddGroup(handle));

                            for user in &foreign_group.members {
                                let (id, roles) = user;

                                let identity = UserIdentity::from_string(id.clone())?;
                                for role in roles {
                                    if id == admin && role == "admin" {
                                        continue;
                                    }
                                    let handle = permission.add_user_prepare(
                                        foreign_group.id,
                                        &identity,
                                        &role,
                                        &store,
                                        &mut wtxn,
                                    )?;
                                    handles.push(HandleHelper::AddUser(handle));
                                }
                            }

                            let mut bitmap = Vec::new();
                            RoaringBitmap::new().serialize_into(&mut bitmap)?; // TODO: Group mappings
                            store.put(&mut wtxn, GROUPS_MAPPINGS_DB_NAME, &ulid_bytes, &bitmap)?;

                            (doc, handles)
                        }
                    };

                let res = merged_resource.save();

                // Persist
                store.put(&mut wtxn, GROUPS_DB_NAME, &ulid_bytes, &res)?;

                store.commit(wtxn)?;
                Ok(handle)
            },
        )
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;
        for handle in handles {
            match handle {
                HandleHelper::AddGroup(handle) => {
                    self.permission_manager.create_group_commit(handle).await?
                }
                HandleHelper::AddUser(handle) => {
                    self.permission_manager.add_user_commit(handle).await?
                }
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn handle_user_merges(&self, doc: Vec<u8>) -> Result<(), ArunaMetadataError> {
        let store = self.store.clone();
        let permission_handler = self.permission_manager.clone();
        tokio::task::spawn_blocking(move || -> Result<(), ArunaMetadataError> {
            let mut wtxn = store.create_txn(true)?;

            // Parse document, actor_id and ulid
            let mut doc = automerge::AutoCommit::load(doc.as_ref())?;
            let foreign_user: User = autosurgeon::hydrate(&doc)?;
            let ulid_bytes = foreign_user.id.to_bytes();

            // Decide if merge or create
            let mut merged_resource = match store.get(&wtxn, USER_DB_NAME, &ulid_bytes)? {
                Some(existing_doc) => {
                    let mut existing_user = automerge::AutoCommit::load(existing_doc.as_ref())?;
                    existing_user.merge(&mut doc)?;
                    existing_user
                }
                None => doc,
            };

            let res = merged_resource.save();

            let _ = permission_handler.ensure_user_identity(&foreign_user.id, &store, &mut wtxn)?;
            // Persist
            store.put(&mut wtxn, USER_DB_NAME, &ulid_bytes, &res)?;

            store.commit(wtxn)?;
            Ok::<(), ArunaMetadataError>(())
        })
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn handle_object_merges(
        &self,
        path: Path,
        doc: Vec<u8>,
    ) -> Result<(), ArunaMetadataError> {
        let store = self.store.clone();
        let permission_manager = self.permission_manager.clone();
        let idx_counter = self.idx_counter.clone();
        let group_id = group_from_path(path.clone())?;
        let (idx, resource) =
            tokio::task::spawn_blocking(move || -> Result<(u32, Resource), ArunaMetadataError> {
                let mut wtxn = store.create_txn(true)?;

                // Parse document, actor_id and ulid
                let mut doc = automerge::AutoCommit::load(doc.as_ref())?;
                let foreign_resource: Resource = autosurgeon::hydrate(&doc)?;
                let ulid_bytes = foreign_resource.id.to_bytes();

                // Decide if merge or create
                let (idx, visibility, mut merged_resource) =
                    match store.get(&wtxn, RESOURCE_DB_NAME, &ulid_bytes)? {
                        Some(existing_doc) => {
                            let mut existing_resource =
                                automerge::AutoCommit::load(existing_doc.as_ref())?;
                            existing_resource.merge(&mut doc)?;
                            let idx = idx_from_cow(
                                store
                                    .get(&wtxn, RESOURCE_MAPPINGS_DB_NAME, &ulid_bytes)?
                                    .ok_or_else(|| {
                                        ArunaMetadataError::NotFound(format!(
                                            "Idx not found for object with id {}",
                                            foreign_resource.id
                                        ))
                                    })?,
                            )?;

                            let visibility = visiblity_from_doc(&doc)?;
                            (idx, visibility, existing_resource)
                        }
                        None => (
                            idx_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                            foreign_resource.visibility.clone(),
                            doc,
                        ),
                    };
                let result = hydrate(&merged_resource)?;
                let res = merged_resource.save();

                // Persist
                store.put(
                    &mut wtxn,
                    RESOURCE_DB_NAME,
                    &foreign_resource.id.to_bytes(),
                    &res,
                )?;
                store.put(
                    &mut wtxn,
                    RESOURCE_MAPPINGS_DB_NAME,
                    &foreign_resource.id.to_bytes(),
                    &idx.to_be_bytes(),
                )?;

                let id = foreign_resource.id;

                // Update mappings if neccessary
                if visibility != foreign_resource.visibility {
                    update_mappings(&store, &mut wtxn, foreign_resource, &group_id, idx)?;
                } else {
                    create_mappings(&store, &mut wtxn, foreign_resource, &group_id, idx)?;
                }
                permission_manager.add_resource(ResourceId::Ulid(id), &path, &store, &mut wtxn)?;

                store.commit(wtxn)?;
                Ok::<(u32, Resource), ArunaMetadataError>((idx, result))
            })
            .await
            .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;

        // Update search idx
        self.search.add_resource(idx, resource).await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_or_create_doc(
        &self,
        subject_id: Ulid,
        table: &'static str,
    ) -> Result<AutoCommit, ArunaMetadataError> {
        let store = self.store.clone();
        let doc = tokio::task::spawn_blocking(move || -> Result<AutoCommit, ArunaMetadataError> {
            let txn = store.create_txn(false)?;
            let doc = match store.get(&txn, table, &subject_id.to_bytes())? {
                Some(doc) => AutoCommit::load(doc.as_ref())?,
                None => AutoCommit::new(),
            };
            store.commit(txn)?;
            Ok::<AutoCommit, ArunaMetadataError>(doc)
        })
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;
        Ok(doc)
    }
}
