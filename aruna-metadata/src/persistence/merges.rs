use super::{
    persistence::Persistor,
    persistence::tables::*,
    search::search::Search,
    utils::{create_mappings, idx_from_cow, update_mappings, visiblity_from_doc},
};
use crate::{
    error::ArunaMetadataError,
    models::models::{Resource, User},
    network::network_trait::{Body, MetadataMessage, ReplicationSubject},
};
use aruna_storage::storage::store::Store;
use automerge::sync::State;
use autosurgeon::hydrate;
use iroh::{NodeId, PublicKey};
use roaring::RoaringBitmap;
use tracing::trace;
use ulid::Ulid;

impl<Se, St> Persistor<St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
{
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_states(
        &self,
        doc_id: Ulid,
        nodes: Vec<NodeId>,
    ) -> Result<Vec<(PublicKey, State)>, ArunaMetadataError> {
        let mut lock = self.connection_states.lock();

        let mut states = Vec::new();
        match lock.get_mut(&doc_id) {
            Some(persisted) => {
                for node in nodes {
                    let state = persisted.entry(node).or_insert(State::new());
                    states.push((node, state.clone()));
                }
            }
            None => {
                for node in nodes {
                    states.push((node, State::new()))
                }
            }
        }
        drop(lock);
        Ok(states)
    }
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn handle_replication(
        &self,
        msg: ReplicationSubject,
    ) -> Result<MetadataMessage, ArunaMetadataError> {
        match msg {
            crate::network::network_trait::ReplicationSubject::User(doc) => {
                self.handle_user_merges(doc).await?;
            }
            crate::network::network_trait::ReplicationSubject::Object(doc) => {
                self.handle_object_merges(doc).await?;
            }
            crate::network::network_trait::ReplicationSubject::Group(doc) => {
                self.handle_group_merges(doc).await?;
            }
        };

        Ok(MetadataMessage {
            from: [0u8; 32],
            to: [0u8; 32],
            subject: [0u8; 32],
            body: Body::Empty,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn handle_group_merges(&self, doc: Vec<u8>) -> Result<(), ArunaMetadataError> {
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn handle_user_merges(&self, doc: Vec<u8>) -> Result<(), ArunaMetadataError> {
        let store = self.store.clone();
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
                None => {
                    let mut bitmap = Vec::new();
                    RoaringBitmap::new().serialize_into(&mut bitmap)?;

                    // Write in store
                    store.put(&mut wtxn, USER_MAPPINGS_DB_NAME, &ulid_bytes, &bitmap)?;
                    doc
                }
            };
            let res = merged_resource.save();

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
    pub async fn handle_object_merges(&self, doc: Vec<u8>) -> Result<(), ArunaMetadataError> {
        trace!("Handle object merge");
        let store = self.store.clone();
        let idx_counter = self.idx_counter.clone();
        let (idx, resource) =
            tokio::task::spawn_blocking(move || -> Result<(u32, Resource), ArunaMetadataError> {
                let mut wtxn = store.create_txn(true)?;

                // Parse document, actor_id and ulid
                let mut doc = automerge::AutoCommit::load(doc.as_ref())?;
                let (user_id_bytes, _node_pubkey) = doc.get_actor().to_bytes().split_at(16);
                let user_id = Ulid::from_bytes(user_id_bytes.try_into().map_err(|_e| {
                    ArunaMetadataError::ConversionError {
                        from: "[u8]".to_string(),
                        to: "[u8;16]".to_string(),
                    }
                })?);
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

                // Update mappings if neccessary
                if visibility != foreign_resource.visibility {
                    update_mappings(store.as_ref(), &mut wtxn, foreign_resource, &user_id, idx)?;
                } else {
                    create_mappings(store.as_ref(), &mut wtxn, foreign_resource, &user_id, idx)?;
                }

                store.commit(wtxn)?;
                Ok::<(u32, Resource), ArunaMetadataError>((idx, result))
            })
            .await
            .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;

        // Update search idx
        self.search.add_resource(idx, resource).await?;
        trace!("Finished replicate");

        Ok(())
    }
}
