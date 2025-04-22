use crate::{
    error::ArunaError,
    models::models::{Resource, User, VisibilityClass},
    network::network_trait::{Body, MetadataMessage},
    persistence::{
        persistence::{Authorize, Persistor},
        search::{
            search::Search,
            tantivy::{TantivyConfig, TantivySearch},
        },
        storage::{
            lmdb::{LmdbConfig, LmdbStore},
            store::{
                Store,
                tables::{
                    PUBLIC_MAPPINGS_DB_NAME, RESOURCE_DB_NAME, RESOURCE_MAPPINGS_DB_NAME,
                    USER_DB_NAME, USER_MAPPINGS_DB_NAME,
                },
            },
        },
    },
};
use anyhow::anyhow;
use aruna_net::ProtocolHandler;
use automerge::{ActorId, ReadDoc};
use autosurgeon::reconcile;
use iroh::endpoint::{RecvStream, SendStream};
use roaring::RoaringBitmap;
use std::sync::{Arc, atomic::AtomicU32};
use tokio::{io::AsyncReadExt, join};
use ulid::Ulid;

use super::utils::{create_mappings, idx_from_cow, update_mappings, visiblity_from_doc};

#[derive(Debug)]
pub struct LmdbTantivyPersistence {
    pub store: Arc<LmdbStore>,
    pub search: Arc<TantivySearch>,
    idx_counter: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl Persistor<LmdbStore, TantivySearch> for LmdbTantivyPersistence {
    type Context = String;
    #[tracing::instrument(level = "trace", skip(ctx))]
    async fn new(ctx: String) -> Result<Self, ArunaError> {
        let (res_sdx, res_rcv) = tokio::sync::mpsc::channel(1000);
        let (idx_sdx, idx_rcv) = tokio::sync::oneshot::channel();
        let tantivy_path = format!("{ctx}/tantivy");
        let search_config = TantivyConfig {
            path: tantivy_path,
            index_buffer: 1_000_000_000,
            resources: res_rcv,
        };
        let store_path = format!("{ctx}/heed");
        let store_config = LmdbConfig {
            path: store_path,
            res_sdx,
            idx_sdx,
        };
        let store =
            tokio::task::spawn_blocking(move || Arc::new(LmdbStore::new(store_config).unwrap()));

        let search = tokio::task::spawn_blocking(move || {
            let search = TantivySearch::new(search_config)?;
            Ok::<Arc<TantivySearch>, ArunaError>(Arc::new(search))
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
    async fn add_resource(
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
            if !matches!(resource.visibility, VisibilityClass::Public) {
                let user_id = user_id.to_bytes();
                let res = store
                    .get(&mut write_txn, USER_MAPPINGS_DB_NAME, &user_id)?
                    .ok_or_else(|| ArunaError::NotFound("No user mapping found".to_string()))?;
                let mut mut_map = RoaringBitmap::deserialize_from(res.as_ref())?;
                mut_map.insert(idx);
                let mut bitmap = Vec::new();
                mut_map.serialize_into(&mut bitmap)?;
                store.put(&mut write_txn, USER_MAPPINGS_DB_NAME, &user_id, &bitmap)?;
            } else {
                let public_id = Ulid::default().to_bytes();
                let res = store
                    .get(&mut write_txn, PUBLIC_MAPPINGS_DB_NAME, &public_id)?
                    .ok_or_else(|| ArunaError::NotFound("No user mapping found".to_string()))?;
                let mut mut_map = RoaringBitmap::deserialize_from(res.as_ref())?;
                mut_map.insert(idx);
                let mut bitmap = Vec::new();
                mut_map.serialize_into(&mut bitmap)?;
                store.put(&mut write_txn, PUBLIC_MAPPINGS_DB_NAME, &public_id, &bitmap)?;
            }
            store.commit(write_txn)?;
            Ok::<Vec<u8>, ArunaError>(res)
        })
        .await
        .map_err(|_e| ArunaError::ServerError("Join task error".to_string()))??;

        //tokio::task::spawn_blocking(move || {
        //tokio::spawn(async move {
        search.add_resource(idx, res_clone).await?;
        //    Ok::<(), ArunaError>(())
        //});

        Ok(res)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_resources(&self, ids: Vec<Ulid>) -> Result<Vec<Resource>, ArunaError> {
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
    async fn add_user(&self, actor_id: &[u8; 32], user: User) -> Result<Vec<u8>, ArunaError> {
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
    async fn get_user(&self, id: &Ulid) -> Result<Option<User>, ArunaError> {
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
    async fn search(&self, user: Option<Ulid>, query: String) -> Result<Vec<String>, ArunaError> {
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
    async fn clear(&self) -> Result<(), ArunaError> {
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
    async fn update_resource(
        &self,
        actor_id: &[u8; 32],
        user_id: &Ulid,
        resource: Resource,
    ) -> Result<Vec<u8>, ArunaError> {
        let store = self.store.clone();
        let search = self.search.clone();
        let user_id = user_id.clone();
        let resource_id = resource.id.to_bytes();
        let actor_id = ActorId::from([user_id.to_bytes().as_slice(), actor_id.as_slice()].concat());
        // spawn search in its own block and dont await outcome
        let res_clone = resource.clone();

        let (result, idx) = tokio::task::spawn_blocking(move || {
            let mut write_txn = store.create_txn(true)?;

            let idx = u32::from_be_bytes(
                store
                    .get(&write_txn, RESOURCE_MAPPINGS_DB_NAME, &resource_id)?
                    .ok_or_else(|| ArunaError::NotFound(format!("{} not found", resource.id)))?
                    .as_ref()
                    .try_into()
                    .map_err(|e| ArunaError::ConversionError {
                        from: "Cow".to_string(),
                        to: "&[u8;4]".to_string(),
                    })?,
            );

            let res = store
                .get(&write_txn, RESOURCE_DB_NAME, &resource_id)?
                .ok_or_else(|| ArunaError::NotFound(format!("{} not found", resource.id)))?;

            let mut doc = automerge::AutoCommit::load(res.as_ref())?.with_actor(actor_id);

            let visibility = match doc.get(automerge::ROOT, "visibility")?.ok_or_else(|| {
                ArunaError::DeserializeError("visibility field not found".to_string())
            })? {
                (automerge::Value::Scalar(cow), _) => match cow.to_str() {
                    Some("Private") => VisibilityClass::Private,
                    Some("Public") => VisibilityClass::Public,
                    Some("Invisible") => VisibilityClass::Invisible,
                    _ => {
                        return Err(ArunaError::ConversionError {
                            from: "Cow".to_string(),
                            to: "VisibilityClass".to_string(),
                        });
                    }
                },
                (_, _) => {
                    return Err(ArunaError::ConversionError {
                        from: "Cow".to_string(),
                        to: "VisibilityClass".to_string(),
                    });
                }
            };

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
                if !matches!(resource.visibility, VisibilityClass::Public) {
                    // Add to private mappings
                    let user_id = user_id.to_bytes();
                    let res = store
                        .get(&mut write_txn, USER_MAPPINGS_DB_NAME, &user_id)?
                        .ok_or_else(|| ArunaError::NotFound("No user mapping found".to_string()))?;
                    let mut mut_map = RoaringBitmap::deserialize_from(res.as_ref())?;
                    mut_map.insert(idx);
                    let mut bitmap = Vec::new();
                    mut_map.serialize_into(&mut bitmap)?;
                    store.put(&mut write_txn, USER_MAPPINGS_DB_NAME, &user_id, &bitmap)?;

                    // Remove from public mappings
                    let public_id = Ulid::default().to_bytes();
                    let res = store
                        .get(&mut write_txn, PUBLIC_MAPPINGS_DB_NAME, &public_id)?
                        .ok_or_else(|| ArunaError::NotFound("No user mapping found".to_string()))?;
                    let mut mut_map = RoaringBitmap::deserialize_from(res.as_ref())?;
                    mut_map.remove(idx);
                    let mut bitmap = Vec::new();
                    mut_map.serialize_into(&mut bitmap)?;
                    store.put(&mut write_txn, PUBLIC_MAPPINGS_DB_NAME, &public_id, &bitmap)?;
                } else {
                    // Add to public mappings
                    let public_id = Ulid::default().to_bytes();
                    let res = store
                        .get(&mut write_txn, PUBLIC_MAPPINGS_DB_NAME, &public_id)?
                        .ok_or_else(|| ArunaError::NotFound("No user mapping found".to_string()))?;
                    let mut mut_map = RoaringBitmap::deserialize_from(res.as_ref())?;
                    mut_map.insert(idx);
                    let mut bitmap = Vec::new();
                    mut_map.serialize_into(&mut bitmap)?;
                    store.put(&mut write_txn, PUBLIC_MAPPINGS_DB_NAME, &public_id, &bitmap)?;

                    // Remove from private mappings
                    let user_id = user_id.to_bytes();
                    let res = store
                        .get(&mut write_txn, USER_MAPPINGS_DB_NAME, &user_id)?
                        .ok_or_else(|| ArunaError::NotFound("No user mapping found".to_string()))?;
                    let mut mut_map = RoaringBitmap::deserialize_from(res.as_ref())?;
                    mut_map.remove(idx);
                    let mut bitmap = Vec::new();
                    mut_map.serialize_into(&mut bitmap)?;
                    store.put(&mut write_txn, USER_MAPPINGS_DB_NAME, &user_id, &bitmap)?;
                }
            }

            store.commit(write_txn)?;
            Ok::<(Vec<u8>, u32), ArunaError>((res, idx))
        })
        .await
        .map_err(|_e| ArunaError::ServerError("Join task error".to_string()))??;

        //tokio::task::spawn_blocking(move || {
        //tokio::spawn(async move {
        search.add_resource(idx, res_clone).await?;
        //    Ok::<(), ArunaError>(())
        //});

        Ok(result)
    }

    async fn handle_message(&self, msg: MetadataMessage) -> Result<MetadataMessage, ArunaError> {
        match msg.body {
            crate::network::network_trait::Body::User(doc) => {
                todo!()
            }
            crate::network::network_trait::Body::Object(doc) => {
                let store = self.store.clone();
                let search = self.search.clone();
                let idx_counter = self.idx_counter.clone();
                let idx = tokio::task::spawn_blocking(move || -> Result<u32, ArunaError> {
                    let mut wtxn = store.create_txn(true)?;
                    let mut doc = automerge::AutoCommit::load(doc.as_ref())?;
                    let (user_id_bytes, _node_pubkey) = doc.get_actor().to_bytes().split_at(16);
                    let user_id = Ulid::from_bytes(user_id_bytes.try_into().map_err(|e| {
                        ArunaError::ConversionError {
                            from: "[u8]".to_string(),
                            to: "[u8;16]".to_string(),
                        }
                    })?);
                    let foreign_resource: Resource = autosurgeon::hydrate(&doc)?;
                    let ulid_bytes = foreign_resource.id.to_bytes();
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

                                let visibility = visiblity_from_doc(doc)?;
                                (idx, visibility, existing_resource)
                            }
                            None => (
                                idx_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                                foreign_resource.visibility.clone(),
                                doc,
                            ),
                        };

                    let res = merged_resource.save();

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
                    if visibility != foreign_resource.visibility {
                        update_mappings(store.as_ref(), &mut wtxn, foreign_resource, &user_id, idx)?;
                    } else {
                        create_mappings(&store, &mut wtxn, foreign_resource, &user_id, idx)?;
                    }

                    store.commit(wtxn)?;

                    Ok::<u32, ArunaError>(idx)
                })
                .await;
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
}

// TODO
impl Authorize for LmdbTantivyPersistence {
    fn authorize(&self, _user_id: &Ulid, _resource_id: &Ulid) -> bool {
        // TODO: Use casbin here
        true
    }
}

#[async_trait::async_trait]
impl ProtocolHandler for LmdbTantivyPersistence {
    async fn handle_stream(
        &self,
        mut send_stream: SendStream,
        mut recv_stream: RecvStream,
    ) -> anyhow::Result<()> {
        while let Ok(len) = recv_stream.read_u32().await {
            let mut buf = vec![0; len as usize];

            recv_stream.read_exact(&mut buf).await?;
            let message = postcard::from_bytes::<MetadataMessage>(&buf)
                .map_err(|e| anyhow!("Failed to deserialize message: {e:#}"))?;

            if let Ok(_response) = self.handle_message(message).await {
                // TODO: Respond with something if need arises
                //
                // Serialize the response
                // let response_buf = postcard::to_allocvec(&response)
                //     .map_err(|e| anyhow!("Failed to serialize response: {e:#}"))?;

                // Send the response
                // send_stream.write_u32(response_buf.len() as u32).await?;
                // send_stream.write_all(&response_buf).await?;
            }
        }

        Ok(())
    }
}
