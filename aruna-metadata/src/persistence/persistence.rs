use super::{
    search::search::Search,
    utils::{create_mappings, idx_from_cow, update_mappings, visiblity_from_doc},
};
use crate::{
    error::ArunaMetadataError,
    models::models::{Resource, User},
    network::network_trait::{Body, MetadataMessage},
    persistence::persistence::tables::*,
};
use aruna_storage::storage::store::Store;
use automerge::ActorId;
use autosurgeon::{hydrate, reconcile};
use roaring::RoaringBitmap;
use std::sync::{Arc, atomic::AtomicU32};
use tracing::trace;
use ulid::Ulid;

pub trait Authorize {
    fn authorize(&self, user_id: &Ulid, resource_id: &Ulid) -> bool;
}

pub mod tables {
    pub const RESOURCE_DB_NAME: &str = "resources";
    pub const RESOURCE_MAPPINGS_DB_NAME: &str = "resource_mappings";
    pub const USER_DB_NAME: &str = "users";
    pub const USER_MAPPINGS_DB_NAME: &str = "user_mappings";
    pub const PUBLIC_MAPPINGS_DB_NAME: &str = "public_mappings";
}

#[derive(Debug)]
pub struct Persistor<St, Se>
where
    for<'a> St: Store<'a>,
    Se: Search,
{
    store: Arc<St>,
    search: Arc<Se>,
    idx_counter: Arc<AtomicU32>,
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
    ) -> Result<Self, ArunaMetadataError> {
        let store = Arc::new(St::new(store_config).unwrap());
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
                    .expect("No valid mapping found"); // TODO: Remove unwraps and replace with

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
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn add_resource(
        &self,
        actor_id: &[u8; 32],
        user_id: &Ulid,
        resource: Resource,
    ) -> Result<Vec<u8>, ArunaMetadataError> {
        let store = self.store.clone();
        let search = self.search.clone();
        let user_id = *user_id;
        let actor_id = ActorId::from([user_id.to_bytes().as_slice(), actor_id.as_slice()].concat());

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

            create_mappings(store.as_ref(), &mut write_txn, resource, &user_id, idx)?;

            store.commit(write_txn)?;
            Ok::<Vec<u8>, ArunaMetadataError>(res)
        })
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;

        search.add_resource(idx, res_clone).await?;

        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_resources(&self, ids: Vec<Ulid>) -> Result<Vec<Resource>, ArunaMetadataError> {
        let store = self.store.clone();
        let result = tokio::task::spawn_blocking(move || {
            let read_txn = store.create_txn(false)?;
            let mut result = Vec::new();
            for id in &ids {
                let byte_id = id.to_bytes();
                let Some(res) = store.get(&read_txn, RESOURCE_DB_NAME, &byte_id)? else {
                    return Err(ArunaMetadataError::NotFound(format!("{id} not found")));
                };

                let doc = automerge::AutoCommit::load(res.as_ref())?;
                let resource: Resource = autosurgeon::hydrate(&doc)?;

                result.push(resource);
            }
            store.commit(read_txn)?;
            Ok(result)
        })
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn add_user(
        &self,
        actor_id: &[u8; 32],
        user: User,
    ) -> Result<Vec<u8>, ArunaMetadataError> {
        let store = self.store.clone();
        let actor_id = ActorId::from([user.id.to_bytes().as_slice(), actor_id.as_slice()].concat());
        let result = tokio::task::spawn_blocking(move || {
            let mut write_txn = store.create_txn(true)?;

            // Serialize into &[u8]
            let id = user.id.to_bytes();

            let mut doc = automerge::AutoCommit::new().with_actor(actor_id);

            autosurgeon::reconcile(&mut doc, user)?;
            let user = doc.save();

            let mut bitmap = Vec::new();
            RoaringBitmap::new().serialize_into(&mut bitmap)?;

            // Write in store
            store.put(&mut write_txn, USER_DB_NAME, &id, &user)?;
            store.put(&mut write_txn, USER_MAPPINGS_DB_NAME, &id, &bitmap)?;

            // Commit
            store.commit(write_txn)?;
            Ok::<Vec<u8>, ArunaMetadataError>(user)
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
    ) -> Result<Vec<String>, ArunaMetadataError> {
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
            store.commit(read_txn)?;

            search.search::<Self>(universe, query)
        })
        .await
        .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn update_resource(
        &self,
        actor_id: &[u8; 32],
        user_id: &Ulid,
        resource: Resource,
    ) -> Result<Vec<u8>, ArunaMetadataError> {
        // Clones for spawn_blocking
        let store = self.store.clone();
        let search = self.search.clone();
        let user_id = *user_id;
        let resource_id = resource.id.to_bytes();
        let res_clone = resource.clone();

        // Generate actor id
        let actor_id = ActorId::from([user_id.to_bytes().as_slice(), actor_id.as_slice()].concat());

        let (result, idx) =
            tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, u32), ArunaMetadataError> {
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
                    update_mappings(store.as_ref(), &mut write_txn, resource, &user_id, idx)?;
                }

                store.commit(write_txn)?;
                Ok::<(Vec<u8>, u32), ArunaMetadataError>((res, idx))
            })
            .await
            .map_err(|_e| ArunaMetadataError::ServerError("Join task error".to_string()))??;

        search.add_resource(idx, res_clone).await?;

        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn handle_message(
        &self,
        msg: MetadataMessage,
    ) -> Result<MetadataMessage, ArunaMetadataError> {
        match msg.body {
            crate::network::network_trait::Body::User(_doc) => {
                // TODO: Add or merge user
            }
            crate::network::network_trait::Body::Object(doc) => {
                self.handle_object_merges(doc).await?;
            }
            crate::network::network_trait::Body::Empty => (),
        };

        Ok(MetadataMessage {
            from: [0u8; 32],
            to: [0u8; 32],
            subject: [0u8; 32],
            body: Body::Empty,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn handle_object_merges(&self, doc: Vec<u8>) -> Result<(), ArunaMetadataError> {
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

// TODO
impl<St, Se> Authorize for Persistor<St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
{
    fn authorize(&self, _user_id: &Ulid, _resource_id: &Ulid) -> bool {
        // TODO: Use casbin here
        true
    }
}

// #[async_trait::async_trait]
// impl<St, Se> ProtocolHandler for Persistor<St, Se>
// where
//     for<'a> St: Store<'a> + 'static,
//     Se: Search + 'static,
// {
//     async fn handle_stream(
//         &self,
//         mut _send_stream: SendStream,
//         mut recv_stream: RecvStream,
//     ) -> anyhow::Result<()> {
//         while let Ok(len) = recv_stream.read_u32().await {
//             let mut buf = vec![0; len as usize];

// TODO:
// - dispatch into API requests
//             recv_stream.read_exact(&mut buf).await?;
//             let message = postcard::from_bytes::<MetadataMessage>(&buf)
//                 .map_err(|e| anyhow!("Failed to deserialize message: {e:#}"))?;
//             match self.handle_message(message).await {
//                 Ok(_res) => {
//                     // TODO: Respond with something if need arises
//                     //
//                     // Serialize the response
//                     // let response_buf = postcard::to_allocvec(&response)
//                     //     .map_err(|e| anyhow!("Failed to serialize response: {e:#}"))?;

//                     // Send the response
//                     // send_stream.write_u32(response_buf.len() as u32).await?;
//                     // send_stream.write_all(&response_buf).await?;
//                 }
//                 Err(err) => return Err(anyhow!(err)),
//             }
//         }

//         Ok(())
//     }
// }
