use super::{
    search::search::Search,
    storage::store::{
        Store,
        tables::{
            PUBLIC_MAPPINGS_DB_NAME, RESOURCE_DB_NAME, RESOURCE_MAPPINGS_DB_NAME, USER_DB_NAME,
            USER_MAPPINGS_DB_NAME,
        },
    },
    utils::{create_mappings, idx_from_cow, update_mappings, visiblity_from_doc},
};
use crate::{
    error::ArunaError,
    models::models::{Resource, User},
    network::network_trait::{Body, MetadataMessage},
};
use anyhow::anyhow;
use aruna_net::ProtocolHandler;
use automerge::ActorId;
use autosurgeon::{hydrate, reconcile};
use iroh::endpoint::{RecvStream, SendStream};
use roaring::RoaringBitmap;
use std::sync::{Arc, atomic::AtomicU32};
use tokio::{io::AsyncReadExt, join};
use ulid::Ulid;

pub trait Authorize {
    fn authorize(&self, user_id: &Ulid, resource_id: &Ulid) -> bool;
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
        idx_rcv: tokio::sync::oneshot::Receiver<u32>,
        store_config: <St as Store<'static>>::StoreConfig,
        search_config: Se::SearchConfig,
    ) -> Result<Self, ArunaError> {
        let store = tokio::task::spawn_blocking(move || Arc::new(St::new(store_config).unwrap()));

        let search = tokio::task::spawn_blocking(move || {
            let search = Se::new(search_config)?;
            Ok::<Arc<Se>, ArunaError>(Arc::new(search))
        });
        let (store_result, search_result) = join!(store, search);
        let (store, search) = (store_result.unwrap(), search_result.unwrap().unwrap());

        let waiter = idx_rcv
            .await
            .map_err(|e| ArunaError::DatabaseError(e.to_string()))?;
        let idx_counter = Arc::new(AtomicU32::new(waiter));

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
    ) -> Result<Vec<u8>, ArunaError> {
        let store = self.store.clone();
        let search = self.search.clone();
        let user_id = user_id.clone();
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
            Ok::<Vec<u8>, ArunaError>(res)
        })
        .await
        .map_err(|_e| ArunaError::ServerError("Join task error".to_string()))??;

        search.add_resource(idx, res_clone).await?;

        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_resources(&self, ids: Vec<Ulid>) -> Result<Vec<Resource>, ArunaError> {
        let store = self.store.clone();
        let result = tokio::task::spawn_blocking(move || {
            let read_txn = store.create_txn(false)?;
            let mut result = Vec::new();
            for id in &ids {
                let byte_id = id.to_bytes();
                let Some(res) = store.get(&read_txn, RESOURCE_DB_NAME, &byte_id)? else {
                    return Err(ArunaError::NotFound(format!("{id} not found")));
                };

                let doc = automerge::AutoCommit::load(res.as_ref())?;
                let resource: Resource = autosurgeon::hydrate(&doc)?;

                result.push(resource);
            }
            store.commit(read_txn)?;
            Ok(result)
        })
        .await
        .map_err(|_e| ArunaError::ServerError("Join task error".to_string()))??;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn add_user(&self, actor_id: &[u8; 32], user: User) -> Result<Vec<u8>, ArunaError> {
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
            Ok::<Vec<u8>, ArunaError>(user)
        })
        .await
        .map_err(|_e| ArunaError::ServerError("Join task error".to_string()))??;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_user(&self, id: &Ulid) -> Result<Option<User>, ArunaError> {
        let store = self.store.clone();
        let id = id.clone();
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
            Ok::<Option<User>, ArunaError>(user)
        })
        .await
        .map_err(|_e| ArunaError::ServerError("Join task error".to_string()))??;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn search(
        &self,
        user: Option<Ulid>,
        query: String,
    ) -> Result<Vec<String>, ArunaError> {
        let store = self.store.clone();
        let search = self.search.clone();
        let result = tokio::task::spawn_blocking(move || {
            let read_txn = store.create_txn(false)?;
            let id = Ulid::default().to_bytes();
            let Some(bytes) = store.get(&read_txn, PUBLIC_MAPPINGS_DB_NAME, &id)? else {
                return Err(ArunaError::DatabaseError(
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
        .map_err(|_e| ArunaError::ServerError("Join task error".to_string()))??;
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn clear(&self) -> Result<(), ArunaError> {
        self.search.purge().await?;
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            store.purge()?;
            Ok::<(), ArunaError>(())
        })
        .await
        .map_err(|_e| ArunaError::ServerError("Join task error".to_string()))??;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn update_resource(
        &self,
        actor_id: &[u8; 32],
        user_id: &Ulid,
        resource: Resource,
    ) -> Result<Vec<u8>, ArunaError> {
        // Clones for spawn_blocking
        let store = self.store.clone();
        let search = self.search.clone();
        let user_id = user_id.clone();
        let resource_id = resource.id.to_bytes();
        let res_clone = resource.clone();

        // Generate actor id
        let actor_id = ActorId::from([user_id.to_bytes().as_slice(), actor_id.as_slice()].concat());

        let (result, idx) =
            tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, u32), ArunaError> {
                let mut write_txn = store.create_txn(true)?;

                let idx = idx_from_cow(
                    store
                        .get(&write_txn, RESOURCE_MAPPINGS_DB_NAME, &resource_id)?
                        .ok_or_else(|| {
                            ArunaError::NotFound(format!("{} not found", resource.id))
                        })?,
                )?;

                let res = store
                    .get(&write_txn, RESOURCE_DB_NAME, &resource_id)?
                    .ok_or_else(|| ArunaError::NotFound(format!("{} not found", resource.id)))?;

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
                Ok::<(Vec<u8>, u32), ArunaError>((res, idx))
            })
            .await
            .map_err(|_e| ArunaError::ServerError("Join task error".to_string()))??;

        search.add_resource(idx, res_clone).await?;

        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn handle_message(
        &self,
        msg: MetadataMessage,
    ) -> Result<MetadataMessage, ArunaError> {
        match msg.body {
            crate::network::network_trait::Body::User(_doc) => {
                // TODO: Add or merge user
                ()
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
    pub async fn handle_object_merges(&self, doc: Vec<u8>) -> Result<(), ArunaError> {
        let store = self.store.clone();
        let idx_counter = self.idx_counter.clone();
        let (idx, resource) =
            tokio::task::spawn_blocking(move || -> Result<(u32, Resource), ArunaError> {
                let mut wtxn = store.create_txn(true)?;

                // Parse document, actor_id and ulid
                let mut doc = automerge::AutoCommit::load(doc.as_ref())?;
                let (user_id_bytes, _node_pubkey) = doc.get_actor().to_bytes().split_at(16);
                let user_id = Ulid::from_bytes(user_id_bytes.try_into().map_err(|_e| {
                    ArunaError::ConversionError {
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
                                        ArunaError::NotFound(format!(
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
                Ok::<(u32, Resource), ArunaError>((idx, result))
            })
            .await
            .map_err(|_e| ArunaError::ServerError("Join task error".to_string()))??;

        // Update search idx
        self.search.add_resource(idx, resource).await?;

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

#[async_trait::async_trait]
impl<St, Se> ProtocolHandler for Persistor<St, Se>
where
    for<'a> St: Store<'a> + 'static,
    Se: Search + 'static,
{
    async fn handle_stream(
        &self,
        mut _send_stream: SendStream,
        mut recv_stream: RecvStream,
    ) -> anyhow::Result<()> {
        while let Ok(len) = recv_stream.read_u32().await {
            let mut buf = vec![0; len as usize];

            recv_stream.read_exact(&mut buf).await?;
            let message = postcard::from_bytes::<MetadataMessage>(&buf)
                .map_err(|e| anyhow!("Failed to deserialize message: {e:#}"))?;

            match self.handle_message(message).await {
                Ok(_res) => {
                    // TODO: Respond with something if need arises
                    //
                    // Serialize the response
                    // let response_buf = postcard::to_allocvec(&response)
                    //     .map_err(|e| anyhow!("Failed to serialize response: {e:#}"))?;

                    // Send the response
                    // send_stream.write_u32(response_buf.len() as u32).await?;
                    // send_stream.write_all(&response_buf).await?;
                }
                Err(err) => return Err(anyhow!(err)),
            }
        }

        Ok(())
    }
}
