use super::grpc_query_handler::GrpcQueryHandler;
use crate::auth::auth::AuthHandler;
use crate::caching::grpc_query_handler::sort_objects;
use crate::data_backends::storage_backend::StorageBackend;
use crate::database::persistence::delete_parts_by_upload_id;
use crate::replication::replication_handler::ReplicationMessage;
use crate::s3_frontend::data_handler::DataHandler;
use crate::structs::{
    AccessKeyPermissions, Bundle, DbPermissionLevel, LocationBinding, ObjectType, TypedId,
    UploadPart, User,
};
use crate::{
    database::{database::Database, persistence::WithGenericBytes},
    structs::{Object, ObjectLocation, PubKey},
};
use ahash::RandomState;
use anyhow::Result;
use anyhow::{anyhow, bail};
use aruna_rust_api::api::storage::models::v2::User as GrpcUser;
use async_channel::Sender;
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use jsonwebtoken::DecodingKey;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use s3s::auth::SecretKey;
use std::collections::{HashMap, VecDeque};
use std::ops::Deref;
use std::time::Duration;
use std::{str::FromStr, sync::Arc};
use tokio::sync::RwLock;
use tokio_postgres::GenericClient;
use tracing::{debug, error, info_span, trace, warn, Instrument};

pub struct Cache {
    // Map DieselUlid as key and (User, Vec<String>) as value -> Vec<String> is a list of registered access keys -> access_keys
    #[allow(clippy::type_complexity)]
    users: DashMap<DieselUlid, Arc<RwLock<(User, Vec<String>)>>, RandomState>,
    // Permissions Maybe TODO: Arc<RwLock<AccessKeyPermissions>>?
    access_keys: DashMap<String, Arc<RwLock<AccessKeyPermissions>>, RandomState>,
    // Map with ObjectId as key and Object as value
    #[allow(clippy::type_complexity)]
    resources: DashMap<
        DieselUlid,
        (Arc<RwLock<Object>>, Arc<RwLock<Option<ObjectLocation>>>),
        RandomState,
    >,
    // Map with bundle id as key and (access_key, Vec<ObjectId>, Timestamp<u64>) as value
    bundles: DashMap<DieselUlid, Bundle>,

    // Parts sorted by upload_id
    multi_parts: DashMap<String, Vec<UploadPart>>,

    // Maps with path / key as key and set of all ObjectIds as value
    // /project1/collection1/dataset1 -> ObjectID
    // /project1/collection1/exaset1/object1 -> ObjectID
    // /project1/collection1/dataset1/0
    // /project1/collection1/data/a/a/a/aset1/object1 -> None
    // -> /project1
    // -> /project1/collection1
    // -> /project1/collection1/dataset1
    // -> /project1/collection1/dataset1/object1
    paths: SkipMap<String, DieselUlid>,

    // enum {
    // Server {},
    // Proxy {}
    //}
    // Pubkeys; TODO: Expand to endpoint ?
    pubkeys: DashMap<i32, (PubKey, DecodingKey), RandomState>,

    // Persistence layer
    persistence: RwLock<Option<Database>>,
    pub(crate) aruna_client: RwLock<Option<Arc<GrpcQueryHandler>>>,
    pub(crate) auth: RwLock<Option<AuthHandler>>,
    pub(crate) sender: Sender<ReplicationMessage>,
    backend: Option<Arc<Box<dyn StorageBackend>>>,

    pub(crate) self_arc: RwLock<Option<Arc<Cache>>>,
}

impl Cache {
    #[tracing::instrument(
        level = "debug",
        skip(
            notifications_url,
            with_persistence,
            self_id,
            encoding_key,
            encoding_key_serial,
            backend
        )
    )]
    pub async fn new(
        notifications_url: Option<impl Into<String>>,
        with_persistence: bool,
        self_id: DieselUlid,
        encoding_key: String,
        encoding_key_serial: i32,
        sender: Sender<ReplicationMessage>,
        backend: Option<Arc<Box<dyn StorageBackend>>>,
    ) -> Result<Arc<Self>> {
        // Initialize cache
        let cache = Arc::new(Cache {
            users: DashMap::default(),
            access_keys: DashMap::default(),
            resources: DashMap::default(),
            bundles: DashMap::default(),
            multi_parts: DashMap::default(),
            paths: SkipMap::new(),
            pubkeys: DashMap::default(),
            persistence: RwLock::new(None),
            aruna_client: RwLock::new(None),
            auth: RwLock::new(None),
            sender,
            backend,
            self_arc: RwLock::new(None),
        });
        cache.self_arc.write().await.replace(cache.clone());

        // Initialize auth handler
        let auth_handler =
            AuthHandler::new(cache.clone(), self_id, encoding_key, encoding_key_serial)?;

        // Set auth handler in cache
        cache.set_auth(auth_handler).await;

        // Set database conn in cache
        if with_persistence {
            let persistence = Database::new().await?;
            cache.set_persistence(persistence).await?;
        }

        // Fully sync cache (and database if persistent DataProxy)
        if let Some(url) = notifications_url {
            let notication_handler: Arc<GrpcQueryHandler> = Arc::new(
                GrpcQueryHandler::new(url, cache.clone(), self_id.to_string())
                    .await
                    .map_err(|e| {
                        error!(error = ?e, msg = e.to_string());
                        e
                    })?,
            );

            let notifications_handler_clone = notication_handler.clone();
            tokio::spawn(
                async move {
                    notifications_handler_clone
                        .clone()
                        .create_notifications_channel()
                        .await
                        .unwrap()
                }
                .instrument(info_span!("create_notifications_channel")),
            );

            cache.set_notifications(notication_handler).await;
            debug!("initialized notification handler");
        };

        Ok(cache)
    }

    #[tracing::instrument(level = "trace", skip(self, notifications))]
    async fn set_notifications(&self, notifications: Arc<GrpcQueryHandler>) {
        let mut guard = self.aruna_client.write().await;
        *guard = Some(notifications);
    }

    #[tracing::instrument(level = "trace", skip(self, auth))]
    async fn set_auth(&self, auth: AuthHandler) {
        let mut guard = self.auth.write().await;
        *guard = Some(auth);
    }

    #[tracing::instrument(level = "trace", skip(self, persistence))]
    async fn set_persistence(&self, persistence: Database) -> Result<()> {
        let persistence = self.sync_with_persistence(persistence).await?;
        let mut guard = self.persistence.write().await;
        *guard = Some(persistence);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, access_key))]
    /// Requests a secret key from the cache
    pub async fn get_secret(&self, access_key: &str) -> Result<SecretKey> {
        let secret = self
            .access_keys
            .get(access_key)
            .ok_or_else(|| anyhow!("User not found"))?
            .value()
            .read()
            .await
            .secret
            .clone();
        if secret.is_empty() {
            error!("secret is empty");
            Err(anyhow!("Secret is empty"))
        } else {
            Ok(SecretKey::from(secret))
        }
    }

    pub async fn get_cache(&self) -> Result<Arc<Cache>> {
        self.self_arc
            .read()
            .await
            .clone()
            .ok_or_else(|| anyhow!("Cache not found"))
    }

    #[tracing::instrument(level = "trace", skip(self, database))]
    pub async fn sync_with_persistence(&self, database: Database) -> Result<Database> {
        let client = database.get_client().await?;

        let access_keys = AccessKeyPermissions::get_all(&client).await?;
        let user_keys = access_keys.iter().fold(
            HashMap::new(),
            |mut map: HashMap<DieselUlid, Vec<String>>, elem| {
                map.entry(elem.user_id)
                    .or_default()
                    .push(elem.access_key.clone());
                map
            },
        );
        for key in access_keys.into_iter() {
            let access_key = key.access_key.clone();
            let key = Arc::new(RwLock::new(key));
            self.access_keys.insert(access_key, key);
        }
        for user in User::get_all(&client).await? {
            let keys = user_keys.get(&user.user_id).cloned().unwrap_or_default();
            self.users
                .insert(user.user_id, Arc::new(RwLock::new((user, keys))));
        }
        debug!("synced users");

        self.sync_pubkeys(PubKey::get_all(&client).await?).await?;
        debug!("synced pubkeys");

        // Sort objects from database before sync
        let mut database_objects = Object::get_all(&client).await?;
        sort_objects(&mut database_objects);

        let mut prefixes: HashMap<DieselUlid, Vec<String>> = HashMap::new();

        let mut parts_map = HashMap::new();
        let parts = UploadPart::get_all(&client).await?;

        for part in parts {
            let part_vec: &mut Vec<UploadPart> =
                parts_map.entry(part.upload_id.clone()).or_default();
            part_vec.push(part);
        }

        for (upload_id, parts) in parts_map {
            self.multi_parts.insert(upload_id, parts);
        }

        debug!("synced parts");

        for object in database_objects {
            let mut location = None;
            if object.object_type == ObjectType::Object {
                let binding = LocationBinding::get_by_object_id(&object.id, &client).await?;
                if let Some(binding) = binding {
                    location = Some(ObjectLocation::get(&binding.location_id, &client).await?);
                }
            }

            let cache = self.get_cache().await?;
            if let Some(before_location) = &location {
                if before_location.is_temporary {
                    if let Some(backend) = &self.backend {
                        let backend = backend.clone();
                        let before_location = before_location.clone();
                        let object = object.clone();
                        trace!(
                            ?object,
                            ?before_location,
                            "finalizing missing temp location"
                        );
                        tokio::spawn(
                            async move {
                                if let Err(e) = DataHandler::finalize_location(
                                    object,
                                    cache,
                                    backend,
                                    before_location,
                                    None,
                                )
                                .await
                                {
                                    error!(error = ?e, "Failed to finalize location");
                                }
                            }
                            .instrument(info_span!("finalize_location")),
                        );
                    }
                }
            }

            self.resources.insert(
                object.id,
                (
                    Arc::new(RwLock::new(object.clone())),
                    Arc::new(RwLock::new(location)),
                ),
            );
            if let Some(parents) = object.parents {
                if parents.is_empty() {
                    prefixes.insert(object.id, vec![object.name.clone()]);
                    self.paths.insert(object.name.clone(), object.id);
                } else {
                    for parent in parents {
                        let new_prefix = prefixes
                            .get(&parent.get_id())
                            .ok_or_else(|| anyhow!("Expected parent for non root object"))?
                            .iter()
                            .map(|e| {
                                let path = format!("{}/{}", e, object.name);
                                self.paths.insert(path.clone(), object.id);
                                path
                            })
                            .collect::<Vec<_>>();
                        prefixes.insert(object.id, new_prefix);
                    }
                }
            } else {
                prefixes.insert(object.id, vec![object.name.clone()]);
                self.paths.insert(object.name.clone(), object.id);
            }
        }

        debug!("synced resources");
        Ok(database)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Requests a secret key from the cache
    pub async fn create_or_update_secret(
        &self,
        access_key: &str,
        user_id: &DieselUlid,
    ) -> Result<(String, String)> {
        let user = self
            .users
            .get(user_id)
            .ok_or_else(|| anyhow!("User not found"))?
            .clone();
        let permissions = if user_id.to_string().as_str() == access_key {
            let mut user_info = user.write().await;
            user_info.1.push(access_key.to_string());
            user_info.0.personal_permissions.clone()
        } else {
            let mut user_info = user.write().await;
            let token = user_info
                .0
                .tokens
                .get(&DieselUlid::from_str(access_key)?)
                .cloned()
                .ok_or_else(|| anyhow!("Access key not found"))?;
            user_info.1.push(access_key.to_string());
            token
        };
        let new_secret = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect::<String>();

        let new_access_key = AccessKeyPermissions {
            access_key: access_key.to_string(),
            user_id: *user_id,
            is_service_account: user.read().await.0.is_service_account,
            secret: new_secret.clone(),
            permissions,
        };

        if let Some(pers) = self.persistence.read().await.as_ref() {
            new_access_key
                .upsert(pers.get_client().await?.client())
                .await?;
        }

        let new_key_access = Arc::new(RwLock::new(new_access_key));
        self.access_keys
            .insert(access_key.to_string(), new_key_access);
        Ok((access_key.to_string(), new_secret))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Requests a secret key from the cache
    pub async fn revoke_secret(&self, access_key: &str) -> Result<()> {
        self.access_keys.remove(access_key);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, pks))]
    pub async fn sync_pubkeys(&self, pks: Vec<PubKey>) -> Result<()> {
        for pk in pks.into_iter() {
            trace!(pk = ?pk, "syncing pubkey");
            let dec_key = DecodingKey::from_ed_pem(
                format!(
                    "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
                    pk.key
                )
                .as_bytes(),
            )?;
            self.pubkeys.insert(pk.id.into(), (pk.clone(), dec_key));
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, pks))]
    pub async fn set_pubkeys(&self, pks: Vec<PubKey>) -> Result<()> {
        trace!(num_pks = pks.len(), "overwriting pks in persistence");
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            PubKey::delete_all(persistence.get_client().await?.client()).await?;
            for pk in pks.iter() {
                pk.upsert(persistence.get_client().await?.client()).await?;
            }
        }
        trace!("clearing pks in cache");
        self.pubkeys.clear();
        for pk in pks.into_iter() {
            let dec_key = DecodingKey::from_ed_pem(
                format!(
                    "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
                    pk.key
                )
                .as_bytes(),
            )?;
            self.pubkeys.insert(pk.id.into(), (pk.clone(), dec_key));
        }
        trace!("updated pks in cache");
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn add_pubkey(&self, pk: PubKey) -> Result<()> {
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            pk.upsert(persistence.get_client().await?.client()).await?;
        }
        let dec_key = DecodingKey::from_ed_pem(
            format!(
                "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
                pk.key
            )
            .as_bytes(),
        )?;
        self.pubkeys.insert(pk.id.into(), (pk, dec_key));
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, res))]
    pub fn get_resource_by_path(&self, res: &str) -> Option<DieselUlid> {
        self.paths.get(res).map(|e| *e.value())
    }

    #[tracing::instrument(level = "trace", skip(self, res))]
    pub async fn get_full_resource_by_path(&self, res: &str) -> Option<Object> {
        let id = self.get_resource_by_path(res)?;
        let elem = self.resources.get(&id)?;
        let obj = elem.value().0.read().await.clone();
        Some(obj)
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id))]
    pub async fn get_location(&self, resource_id: &DieselUlid) -> Option<ObjectLocation> {
        let resource = self.resources.get(resource_id)?;
        let location = resource.value().1.read().await;
        location.clone()
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id, with_intermediates))]
    pub async fn get_prefixes(
        &self,
        resource_id: &TypedId,
        with_intermediates: bool,
    ) -> Vec<(TypedId, String)> {
        // /foo/bar/baz
        // /foo/bar/ <baz: resource_id>
        // /bar/baz/ <baz: resource_id>
        // -> /foo/bar (~id /foo)
        // -> /bar/baz (~id /bar)

        // /foo: id-foo
        // /foo/bar: id-foo/bar

        // VecDeque<(id, [Option<(String, DieselUlid)>; 4])>
        const ARRAY_REPEAT_VALUE: Option<(String, TypedId)> = None::<(String, TypedId)>;
        let mut prefixes = VecDeque::from([(*resource_id, [ARRAY_REPEAT_VALUE; 3])]);
        let mut final_result = Vec::new();
        while let Some((id, visited)) = prefixes.pop_front() {
            if let Some(parents) = self.get_parents(&id.get_id()).await {
                for (parent_name, parent_id) in parents {
                    let mut visited_here = visited.clone();
                    for x in (0..3).rev() {
                        if visited_here[x].is_none() {
                            visited_here[x] = Some((parent_name.clone(), parent_id));
                            break;
                        }
                    }
                    prefixes.push_back((parent_id, visited_here));
                }
            } else {
                let mut current_path = String::new();
                for (x, item) in visited.iter().enumerate() {
                    match item {
                        Some((name, id)) => {
                            if !current_path.is_empty() {
                                current_path.push('/');
                            }
                            current_path.push_str(name);
                            if with_intermediates {
                                final_result.push((*id, current_path.clone()));
                            } else if x == 2 {
                                final_result.push((*id, current_path.clone()));
                            }
                        }
                        None => continue,
                    }
                }
            }
        }
        final_result
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id))]
    pub async fn get_suffixes(
        &self,
        resource_id: &TypedId,
        with_intermediates: bool,
    ) -> Vec<(TypedId, String)> {
        let mut prefixes = VecDeque::from([(*resource_id, "".to_string())]);
        let mut final_result = Vec::new();
        while let Some((id, name)) = prefixes.pop_front() {
            if let Some(children) = self.get_children(&id.get_id()).await {
                for (child_name, child_id) in children {
                    prefixes.push_back((child_id, format!("{}/{}", name, child_name)));
                    if with_intermediates {
                        final_result.push((child_id, format!("{}/{}", name, child_name)));
                    }
                }
            } else {
                final_result.push((id, name));
            }
        }
        final_result
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id))]
    pub async fn get_name_trees(
        &self,
        resource_id: &TypedId,
        resource_name: String,
        new_name: Option<String>,
    ) -> (Vec<String>, Option<Vec<String>>) {
        // /project1/collection1/<dataset1>/object1
        // /project2/collection2/<dataset1>/object2
        // Prefix
        // -> /project1/collection1: ID
        // -> /project2/collection2: ID
        // Suffix
        // -> /object1: ID
        // -> /object2: ID

        let prefixes = self.get_prefixes(resource_id, false).await;
        let suffixes = self.get_suffixes(resource_id, false).await;
        let mut final_paths = Vec::new();
        let mut new_paths = Vec::new();
        for (_, pre) in prefixes.iter() {
            for (_, suf) in suffixes.iter() {
                final_paths.push(format!("{}/{}{}", pre, resource_name, suf));
                if let Some(new_name) = &new_name {
                    new_paths.push(format!("{}/{}{}", pre, new_name, suf));
                }
            }
        }
        (
            final_paths,
            if new_name.is_some() {
                Some(new_paths)
            } else {
                None
            },
        )
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id))]
    pub async fn get_parents(&self, resource_id: &DieselUlid) -> Option<Vec<(String, TypedId)>> {
        let resource = self.resources.get(resource_id)?;
        let resource = resource.value().0.read().await;
        if let Some(parents) = &resource.parents {
            let mut collected_parents = Vec::new();
            for parent in parents {
                let parent = self.resources.get(&parent.get_id())?;
                let parent = parent.value().0.read().await;
                collected_parents.push((parent.name.clone(), TypedId::from(parent.deref())));
            }
            if parents.is_empty() {
                return None;
            }
            return Some(collected_parents);
        }
        None
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id))]
    pub async fn get_children(&self, resource_id: &DieselUlid) -> Option<Vec<(String, TypedId)>> {
        let resource = self.resources.get(resource_id)?;
        let resource = resource.value().0.read().await;
        if let Some(children) = &resource.children {
            let mut collected_children = Vec::new();
            for child in children {
                let child = self.resources.get(&child.get_id())?;
                let child = child.value().0.read().await;
                collected_children.push((child.name.clone(), TypedId::from(child.deref())));
            }
            return Some(collected_children);
        }
        None
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_pubkey(&self, kid: i32) -> Result<(PubKey, DecodingKey)> {
        Ok(self
            .pubkeys
            .get(&kid)
            .ok_or_else(|| {
                error!(error = "Pubkey not found");
                anyhow!("Pubkey not found")
            })?
            .clone())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn upsert_user(self: Arc<Cache>, user: GrpcUser) -> Result<()> {
        let user_id = DieselUlid::from_str(&user.id).inspect_err(|&e| {
            error!(error = ?e, msg = e.to_string());
        })?;
        let proxy_user = User::try_from(user)?;
        let (to_update, to_delete) = if let Some(user) = self.users.get(&user_id) {
            let mut user = user.value().write().await;
            trace!(?proxy_user, "GRPC  USER");
            trace!(user=?user.0, "DATABASE USER");
            let comparison = proxy_user.compare_permissions(&user.0);
            let new_keys = user
                .1
                .iter()
                .filter(|k| !comparison.1.contains(k))
                .cloned()
                .collect::<Vec<_>>();
            *user = (proxy_user.clone(), new_keys);
            comparison
        } else {
            self.users.insert(
                user_id,
                Arc::new(RwLock::new((proxy_user.clone(), Vec::new()))),
            );
            (vec![], vec![])
        };
        trace!(?to_update);
        for key in to_delete {
            self.access_keys.remove(&key);
            if let Some(persistence) = self.persistence.read().await.as_ref() {
                AccessKeyPermissions::delete(&key, persistence.get_client().await?.client())
                    .await?;
            }
        }
        for key in to_update {
            trace!(?key);
            let access_key_ref = if let Some(key) = self.access_keys.get(&key.0) {
                key.value().clone()
            } else {
                continue;
            };
            let mut access_key = access_key_ref.write().await;
            access_key.permissions = key.1;
            if let Some(persistence) = self.persistence.read().await.as_ref() {
                access_key
                    .upsert(persistence.get_client().await?.client())
                    .await?;
            }
        }
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            proxy_user
                .upsert(persistence.get_client().await?.client())
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn remove_user(&self, user_id: DieselUlid) -> Result<()> {
        if let Some((u, v)) = self.users.remove(&user_id) {
            for key in v.read().await.1.iter() {
                self.access_keys.remove(key.as_str());
                if let Some(persistence) = self.persistence.read().await.as_ref() {
                    AccessKeyPermissions::delete(key, persistence.get_client().await?.client())
                        .await?;
                }
            }
            if let Some(persistence) = self.persistence.read().await.as_ref() {
                User::delete(&u, persistence.get_client().await?.client()).await?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, object))]
    pub async fn upsert_object(&self, object: Object) -> Result<()> {
        trace!(?object, "upserting object");
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            let mut client = persistence.get_client().await?;
            let transaction = client.transaction().await?;
            let transaction_client = transaction.client();
            object.upsert(transaction_client).await?;
            transaction.commit().await?;
        }
        let old_name = if let Some(o) = self.resources.get(&object.id) {
            let (obj, _) = o.value();
            let mut dash_map_object = obj.try_write()?;
            let old_name = dash_map_object.name.clone();
            *dash_map_object = object.clone();
            old_name
        } else {
            self.resources.insert(
                object.id,
                (
                    Arc::new(RwLock::new(object.clone())),
                    Arc::new(RwLock::new(None)),
                ),
            );
            object.name.to_string()
        };

        let prefixes = self.get_prefixes(&TypedId::Unknown(object.id), false).await;

        trace!(?prefixes, "prefixes");
        if object
            .parents
            .as_ref()
            .map(|e| e.is_empty())
            .unwrap_or_else(|| true)
            && object.object_type != ObjectType::Project
        {
            for (_, pre) in prefixes.iter() {
                self.paths
                    .remove(&format!("{}/{}", pre.clone(), object.name));
            }
        } else {
            for (_, pre) in prefixes.iter() {
                self.paths
                    .insert(format!("{}/{}", pre.clone(), object.name), object.id);

                trace!(
                    inserted = format!("{}/{}", pre.clone(), object.name),
                    "inserted"
                );
            }
        }

        if prefixes.is_empty() && object.object_type == ObjectType::Project {
            self.paths.insert(object.name.clone(), object.id);
        }

        if old_name != object.name {
            self.update_object_name(TypedId::from(&object), old_name, object.name)
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, object_id, new_name))]
    pub async fn update_object_name(
        &self,
        object_id: TypedId,
        old_name: String,
        new_name: String,
    ) -> Result<()> {
        let (old_paths, new_paths) = self
            .get_name_trees(&object_id, old_name, Some(new_name))
            .await;
        for (old, new) in old_paths.into_iter().zip(new_paths.unwrap_or_default()) {
            let old_opt_entry = self.paths.remove(&old);
            if let Some(old_entry) = old_opt_entry {
                self.paths.insert(new, *old_entry.value());
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn delete_object(&self, id: DieselUlid) -> Result<()> {
        // Remove object and location from database
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            let mut client = persistence.get_client().await?;
            let transaction = client.transaction().await?;
            let transaction_client = transaction.client();

            ObjectLocation::delete(&id, transaction_client).await?;
            Object::delete(&id, transaction_client).await?;

            transaction.commit().await?;
        }

        // Remove data from storage backend
        if let Some(s3_backend) = &self.backend {
            if let Some(resource) = self.resources.get(&id) {
                let (_, loc) = resource.value();
                if let Some(location) = loc.read().await.as_ref() {
                    s3_backend.delete_object(location.clone()).await?;
                }
            }
        }

        // Remove object and location from cache
        let Some(old) = self.resources.remove(&id) else {
            warn!(?id, "Resource not found");
            return Ok(());
        };

        let object = old.1 .0.read().await;
        for p in self
            .get_name_trees(&TypedId::from(object.deref()), object.name.clone(), None)
            .await
            .0
        {
            self.paths.remove(&p);
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_key_perms(&self, access_key: &str) -> Option<AccessKeyPermissions> {
        let result = self.access_keys.get(access_key)?;
        let result = result.value().read().await.clone();
        trace!(?result);
        Some(result)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    #[allow(clippy::type_complexity)]
    pub async fn get_resource(
        &self,
        resource_id: &DieselUlid,
    ) -> Result<(Arc<RwLock<Object>>, Arc<RwLock<Option<ObjectLocation>>>)> {
        let resource = self
            .resources
            .get(resource_id)
            .ok_or_else(|| anyhow!("Resource not found"))?;
        Ok(resource.value().clone())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_resource_cloned(
        &self,
        resource_id: &DieselUlid,
        skip_location: bool,
    ) -> Result<(Object, Option<ObjectLocation>)> {
        let (obj, loc) = self.get_resource(resource_id).await?;
        let obj = obj.read().await.clone();
        let loc = if !skip_location {
            loc.read().await.clone()
        } else {
            None
        };
        Ok((obj, loc))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_path(&self, path: &str) -> Option<DieselUlid> {
        self.paths.get(path).map(|e| *e.value())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_user_attributes(
        &self,
        resource_id: &DieselUlid,
    ) -> Option<HashMap<String, String>> {
        let user_reference = self.users.get(resource_id)?;
        let user = user_reference.value().read().await;
        return Some(user.0.attributes.clone());
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_path_range(&self, bucket_name: &str, skip: &str) -> Vec<(String, DieselUlid)> {
        let prefix = format!("{}/", bucket_name);

        self.paths
            .range(format!("{prefix}{skip}")..=format!("{prefix}~"))
            .map(|e| {
                (
                    e.key()
                        .strip_prefix(&prefix)
                        .unwrap_or_default()
                        .to_string(),
                    *e.value(),
                )
            })
            .collect()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn add_bundle(&self, bundle: Bundle) {
        self.bundles.insert(bundle.id, bundle);
        // TODO: Add to persistence layer
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_bundle(&self, bundle_id: &DieselUlid) -> Option<Bundle> {
        let result = self.bundles.get(bundle_id).map(|e| e.clone());
        result
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn delete_bundle(&self, bundle_id: &DieselUlid) {
        self.bundles.remove(bundle_id);
    }

    #[tracing::instrument(level = "trace", skip(self, bundle_id, access_key))]
    pub fn check_delete_bundle(&self, bundle_id: &DieselUlid, access_key: &str) -> Result<()> {
        if let Some(bundle) = self.get_bundle(bundle_id) {
            if bundle.owner_access_key == access_key {
                Ok(())
            } else {
                error!("Unable to delete bundle");
                Err(anyhow!("Access denied"))
            }
        } else {
            Err(anyhow!("Bundle not found"))
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn check_access_parents(
        &self,
        key_info: &AccessKeyPermissions,
        resource_id: &DieselUlid,
        needed: DbPermissionLevel,
    ) -> Result<()> {
        if let Some(parents) = self.get_parents(resource_id).await {
            for (_, id) in parents {
                if let Some(has) = key_info.permissions.get(&id.get_id()) {
                    if *has >= needed {
                        return Ok(());
                    }
                }
            }
        }
        error!("Insufficient permissions");
        Err(anyhow!("Insufficient permissions"))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_resource_name(&self, id: &DieselUlid) -> Option<String> {
        let (res, _) = self.get_resource(id).await.ok()?;
        let res = res.read().await.name.clone();
        Some(res)
    }

    #[tracing::instrument(level = "trace", skip(self, id))]
    pub async fn get_single_parent(
        &self,
        id: &DieselUlid,
    ) -> Result<[Option<(DieselUlid, String)>; 4]> {
        let mut id = *id;

        let mut result = [
            None,
            None,
            None,
            Some((
                id,
                self.get_resource_name(&id)
                    .await
                    .ok_or_else(|| anyhow!("Resource not found"))?,
            )),
        ];
        while let Some(r_id) = self.get_parents(&id).await {
            if let Some((name, typed_id)) = r_id.first() {
                match typed_id {
                    TypedId::Dataset(p_id) => {
                        result[2] = Some((*p_id, name.clone()));
                        id = *p_id;
                    }
                    TypedId::Collection(p_id) => {
                        result[1] = Some((*p_id, name.clone()));
                        id = *p_id;
                    }
                    TypedId::Project(p_id) => {
                        result[0] = Some((*p_id, name.clone()));
                        id = *p_id;
                    }
                    _ => bail!("Unexpected parent type"),
                }
            }
        }
        Ok(result)
    }

    #[tracing::instrument(level = "trace", skip(self, id))]
    pub async fn get_location_cloned(&self, id: &DieselUlid) -> Option<ObjectLocation> {
        let (_, loc) = self.get_resource(id).await.ok()?;
        let loc = loc.read().await;
        loc.clone()
    }

    #[tracing::instrument(level = "trace", skip(self, starting_points))]
    pub async fn get_path_levels(
        &self,
        starting_points: &[DieselUlid],
    ) -> Result<Vec<(String, Option<ObjectLocation>)>> {
        let mut results = Vec::new();
        for id in starting_points {
            let suffixes = self.get_suffixes(&TypedId::Unknown(*id), true).await;
            for (id, name) in suffixes {
                if let TypedId::Object(id) = id {
                    results.push((name, self.get_location_cloned(&id).await));
                } else {
                    results.push((format!("{}/", name), None))
                }
            }
        }
        Ok(results)
    }

    #[tracing::instrument(level = "trace", skip(self, user_id))]
    pub async fn get_personal_projects(&self, user_id: &DieselUlid) -> Result<Vec<Object>> {
        let mut results = Vec::new();

        let user = self
            .users
            .get(user_id)
            .ok_or_else(|| anyhow!("User not found"))?;
        let user = user.value().read().await;

        for (id, _) in user.0.personal_permissions.iter() {
            if let Some(project) = self.resources.get(id) {
                let maybe_project = project.value().0.read().await.clone();
                if maybe_project.object_type == ObjectType::Project {
                    results.push(maybe_project);
                }
            }
        }
        Ok(results)
    }

    #[tracing::instrument(level = "trace", skip(self, object_id, location))]
    pub async fn add_location_with_binding(
        &self,
        object_id: DieselUlid,
        location: ObjectLocation,
    ) -> Result<()> {
        let (_, loc) = self
            .resources
            .get(&object_id)
            .ok_or_else(|| anyhow!("Resource not found {}", object_id))?
            .value()
            .clone();
        *loc.write().await = Some(location.clone());

        if let Some(persistence) = self.persistence.read().await.as_ref() {
            location
                .upsert(persistence.get_client().await?.client())
                .await?;

            let binding = LocationBinding {
                object_id,
                location_id: location.id,
            };

            binding
                .insert_binding(persistence.get_client().await?.client())
                .await?;
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, object_id, location))]
    pub async fn update_location(
        &self,
        object_id: DieselUlid,
        location: ObjectLocation,
    ) -> Result<()> {
        let old_location_id = if let Some(resource) = self.resources.get(&object_id) {
            let (_, loc) = resource.value();
            let old_id = {
                let reference = loc.read().await;
                reference.as_ref().map(|e| e.id)
            };
            *loc.write().await = Some(location.clone());
            old_id
        } else {
            bail!("Resource not found")
        };

        if let Some(persistence) = self.persistence.read().await.as_ref() {
            location
                .upsert(persistence.get_client().await?.client())
                .await?;

            let new_binding = LocationBinding {
                object_id,
                location_id: location.id,
            };

            if let Some(old_id) = old_location_id {
                if old_id != location.id {
                    ObjectLocation::delete(&old_id, persistence.get_client().await?.client())
                        .await?;
                    new_binding
                        .insert_binding(persistence.get_client().await?.client())
                        .await?;
                }
            } else {
                new_binding
                    .insert_binding(persistence.get_client().await?.client())
                    .await?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, object_id, location))]
    pub async fn delete_location_with_mappings(
        &self,
        object_id: DieselUlid,
        location: ObjectLocation,
    ) -> Result<()> {
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            let mut client = persistence.get_client().await?;
            let transaction = client.transaction().await?;
            let transaction_client = transaction.client();

            ObjectLocation::delete(&location.id, transaction_client).await?;
            LocationBinding::delete_by_object_id(&object_id, transaction_client).await?;
            transaction.commit().await?;
        }

        if let Some(resource) = self.resources.get(&object_id) {
            let (_, loc) = resource.value();
            *loc.write().await = None;
        } else {
            bail!("Resource not found")
        };

        Ok(())
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, upload_id, object_id, part_number, raw_size, final_size)
    )]
    pub async fn create_multipart_upload(
        &self,
        upload_id: String,
        object_id: DieselUlid,
        part_number: u64,
        raw_size: u64,
        final_size: u64,
    ) -> Result<()> {
        let part = UploadPart {
            id: DieselUlid::generate(),
            part_number,
            size: final_size,
            object_id,
            upload_id: upload_id.clone(),
            raw_size,
        };
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            part.upsert(persistence.get_client().await?.client())
                .await?;
        }
        let mut counter = 0;
        loop {
            let upload_id = upload_id.clone();
            let Some(entry) = self.multi_parts.try_entry(upload_id) else {
                tokio::time::sleep(Duration::from_millis(100)).await;
                counter += 1;
                continue;
            };

            if counter > 10 {
                return Err(anyhow!("Failed to create multipart upload"));
            }

            let mut entry = entry.or_insert(Vec::new());
            entry.value_mut().push(part);
            break;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, upload_id))]
    pub fn get_parts(&self, upload_id: &str) -> Vec<UploadPart> {
        let mut parts = self
            .multi_parts
            .get(upload_id)
            .map(|e| e.value().clone())
            .unwrap_or_default();
        parts.sort_by(|a, b| a.part_number.cmp(&b.part_number));
        parts
    }

    #[tracing::instrument(level = "trace", skip(self, upload_id))]
    pub async fn delete_parts_by_upload_id(&self, upload_id: String) -> Result<()> {
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            delete_parts_by_upload_id(
                persistence.get_client().await?.client(),
                upload_id.to_string(),
            )
            .await?;
        }
        self.multi_parts.remove(&upload_id);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, upload_id))]
    pub async fn delete_part(&self, upload_id: String, part_number: u64) -> Result<()> {
        let mut entry = self
            .multi_parts
            .get_mut(&upload_id)
            .ok_or_else(|| anyhow!("Upload not found"))?;
        let value = entry.value_mut();
        let index = value
            .iter()
            .position(|x| x.part_number == part_number)
            .ok_or_else(|| anyhow!("Part not found"))?;
        let removed = value.remove(index);
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            UploadPart::delete(&removed.id, persistence.get_client().await?.client()).await?;
        }
        Ok(())
    }
}
