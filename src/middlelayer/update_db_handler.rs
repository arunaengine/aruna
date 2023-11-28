use super::update_request_types::{LicenseUpdate, UpdateObject};
use crate::database::crud::CrudDb;
use crate::database::dsls::hook_dsl::TriggerVariant;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_VERSION,
};
use crate::database::dsls::license_dsl::ALL_RIGHTS_RESERVED;
use crate::database::dsls::object_dsl::{KeyValue, KeyValueVariant, Object, ObjectWithRelations};
use crate::database::enums::{EndpointStatus, ObjectStatus};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_rust_api::api::storage::services::v2::{FinishObjectStagingRequest, UpdateObjectRequest};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use postgres_types::Json;
use std::str::FromStr;

impl DatabaseHandler {
    pub async fn update_dataclass(&self, request: DataClassUpdate) -> Result<ObjectWithRelations> {
        // Extract parameter from request
        let dataclass = request.get_dataclass()?;
        let id = request.get_id()?;

        // Init transaction
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        // Update object in database
        let old_object = Object::get(id, transaction_client)
            .await?
            .ok_or(anyhow!("Resource not found."))?;

        if old_object.data_class < dataclass {
            return Err(anyhow!("Dataclasses can only be relaxed."));
        }

        Object::update_dataclass(id, dataclass, transaction_client).await?;
        transaction.commit().await?;

        // Fetch hierarchies and object relations for notifications
        let object_plus = Object::get_object_with_relations(&id, &client).await?; // Why not just modify the original object?
        let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;

        // Try to emit object updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(
                &object_plus,
                hierarchies,
                EventVariant::Updated,
                Some(&DieselUlid::generate()), // block_id for deduplication
            )
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok(object_plus)
        }
    }

    pub async fn update_name(&self, request: NameUpdate) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let name = request.get_name()?;
        let id = request.get_id()?;
        Object::update_name(id, name, transaction_client).await?;
        transaction.commit().await?;

        // Fetch hierarchies and object relations for notifications
        let object_plus = Object::get_object_with_relations(&id, &client).await?;

        let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;

        // Try to emit object updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(
                &object_plus,
                hierarchies,
                EventVariant::Updated,
                Some(&DieselUlid::generate()), // block_id for deduplication
            )
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok(object_plus)
        }
    }

    pub async fn update_description(
        &self,
        request: DescriptionUpdate,
    ) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let description = request.get_description();
        let id = request.get_id()?;
        Object::update_description(id, description, transaction_client).await?;
        transaction.commit().await?;

        // Fetch hierarchies and object relations for notifications
        let object_plus = Object::get_object_with_relations(&id, &client).await?;

        let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;

        // Try to emit object updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(
                &object_plus,
                hierarchies,
                EventVariant::Updated,
                Some(&DieselUlid::generate()), // block_id for deduplication
            )
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok(object_plus)
        }
    }

    pub async fn update_keyvals(
        &self,
        request: KeyValueUpdate,
        user_id: DieselUlid,
    ) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let id = request.get_id()?;
        let (add_key_values, rm_key_values) = request.get_keyvals()?;
        let mut trigger = Vec::new();

        if add_key_values.0.is_empty() && rm_key_values.0.is_empty() {
            return Err(anyhow!(
                "Both add_key_values and remove_key_values are empty.",
            ));
        }
        if !add_key_values.0.is_empty() {
            for kv in add_key_values.0 {
                match kv.variant {
                    KeyValueVariant::HOOK => {
                        trigger.push(TriggerVariant::HOOK_ADDED);
                    }
                    KeyValueVariant::HOOK_STATUS => {
                        return Err(anyhow!(
                            "Can't create hook status outside of hook callbacks"
                        ));
                    }
                    KeyValueVariant::LABEL => {
                        trigger.push(TriggerVariant::LABEL_ADDED);
                    }
                    KeyValueVariant::STATIC_LABEL => {
                        trigger.push(TriggerVariant::STATIC_LABEL_ADDED);
                    }
                }
                Object::add_key_value(&id, transaction_client, kv).await?;
            }
        }

        if !rm_key_values.0.is_empty() {
            let object = Object::get(id, transaction_client)
                .await?
                .ok_or(anyhow!("Dataset does not exist."))?;
            for kv in rm_key_values.0 {
                if kv.variant == KeyValueVariant::STATIC_LABEL {
                    return Err(anyhow!("Cannot remove static labels."));
                } else if kv.variant == KeyValueVariant::HOOK_STATUS {
                    return Err(anyhow!(
                        "Cannot remove hook_status outside of hook_callback"
                    ));
                } else {
                    object.remove_key_value(transaction_client, kv).await?;
                }
            }
        }
        transaction.commit().await?;

        // Trigger hook
        let object_plus = Object::get_object_with_relations(&id, &client).await?;
        let db_handler = DatabaseHandler {
            database: self.database.clone(),
            natsio_handler: self.natsio_handler.clone(),
            cache: self.cache.clone(),
            hook_sender: self.hook_sender.clone(),
        };
        let object_clone = object_plus.clone();
        tokio::spawn(async move {
            let response = db_handler
                .trigger_hooks(object_clone, user_id, trigger, None)
                .await;
            if response.is_err() {
                log::error!("{:?}", response)
            }
        });

        // Fetch hierarchies and object relations for notifications
        let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;

        // Try to emit object updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(
                &object_plus,
                hierarchies,
                EventVariant::Updated,
                Some(&DieselUlid::generate()), // block_id for deduplication
            )
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok(object_plus)
        }
    }

    pub async fn update_license(&self, request: LicenseUpdate) -> Result<ObjectWithRelations> {
        let client = self.database.get_client().await?;
        let id = request.get_id()?;
        let old = Object::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("Resource not found"))?;
        let (metadata_tag, data_tag) = request.get_licenses(&old, &client).await?;
        Object::update_licenses(id, data_tag, metadata_tag, &client).await?;

        // Fetch hierarchies and object relations for notifications
        let object_plus = Object::get_object_with_relations(&id, &client).await?;
        let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;

        // Try to emit object updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(
                &object_plus,
                hierarchies,
                EventVariant::Updated,
                Some(&DieselUlid::generate()), // block_id for deduplication
            )
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok(object_plus)
        }
    }

    pub async fn update_grpc_object(
        &self,
        request: UpdateObjectRequest,
        user_id: DieselUlid,
        is_service_account: bool,
    ) -> Result<(
        ObjectWithRelations,
        bool, // Creates revision
    )> {
        let mut client = self.database.get_client().await?;
        let req = UpdateObject(request.clone());
        let id = req.get_id()?;
        let owr = Object::get_object_with_relations(&id, &client).await?;
        let old = owr.object.clone();
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        // If license is updated from all rights reserved to anything no new revision is triggered
        let license_triggers_new_revision = match (
            (old.data_license == ALL_RIGHTS_RESERVED),
            (old.metadata_license == ALL_RIGHTS_RESERVED),
        ) {
            (true, true) => false,
            (true, false) => request.metadata_license_tag.is_some(),
            (false, true) => request.data_license_tag.is_some(),
            (false, false) => match (request.data_license_tag, request.metadata_license_tag) {
                // If nothing is updated, no new revision is triggered
                (None, None) => false,
                // If one or both are updated, new revision is triggered
                (_, _) => true,
            },
        };
        let (data_class, dataclass_triggers_new_revision) =
            req.get_dataclass(old.clone(), is_service_account)?;
        let (id, is_new, affected) = if request.force_revision
            || request.name.is_some()
            || !request.remove_key_values.is_empty()
            || !request.hashes.is_empty()
            || license_triggers_new_revision
            || dataclass_triggers_new_revision
        {
            let id = DieselUlid::generate();
            let (metadata_license, data_license) =
                req.get_license(&old, transaction_client).await?;
            // Create new object
            let mut create_object = Object {
                id,
                content_len: old.content_len,
                count: 1,
                revision_number: old.revision_number + 1,
                external_relations: old.clone().external_relations,
                created_at: None,
                created_by: user_id,
                data_class,
                description: req.get_description(old.clone()),
                name: req.get_name(old.clone()),
                key_values: Json(req.get_all_kvs(old.clone())?),
                hashes: Json(req.get_hashes(old.clone())?),
                object_type: crate::database::enums::ObjectType::OBJECT,
                object_status: ObjectStatus::INITIALIZING, // New revisions must be finished
                dynamic: false,
                endpoints: Json(req.get_endpoints(old.clone(), true)?),
                metadata_license,
                data_license,
            };
            create_object.create(transaction_client).await?;

            // Clone all relations of old object with new object id
            let relations = UpdateObject::get_all_relations(owr.clone(), create_object.clone());
            let (mut new, (delete, mut affected)): (
                Vec<InternalRelation>,
                (Vec<DieselUlid>, Vec<DieselUlid>),
            ) = relations.into_iter().unzip();
            // Add version relation for old -> new
            let version = InternalRelation {
                id: DieselUlid::generate(),
                origin_pid: old.id,
                origin_type: old.object_type,
                relation_name: INTERNAL_RELATION_VARIANT_VERSION.to_string(),
                target_pid: create_object.id,
                target_type: create_object.object_type,
                target_name: create_object.name.clone(),
            };
            new.push(version);
            // Delete all relations for old object
            InternalRelation::batch_delete(&delete, transaction_client).await?;
            // Create all relations for new_object
            InternalRelation::batch_create(&new, transaction_client).await?;
            // Add parent if updated
            if let Some(p) = request.parent.clone() {
                let mut relation = UpdateObject::add_parent_relation(
                    id,
                    p.clone(),
                    create_object.name.to_string(),
                )?;
                relation.create(transaction_client).await?;
                let changed = match p {
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::ProjectId(id) => DieselUlid::from_str(&id)?,
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::CollectionId(id) => DieselUlid::from_str(&id)?,
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::DatasetId(id) => DieselUlid::from_str(&id)?,
                };
                affected.push(changed);
            }
            // Return all affected ids for cache sync
            affected.push(old.id);

            (id, true, affected)
        } else {
            // Update in place
            let update_object = Object {
                id: old.id,
                content_len: old.content_len,
                count: 1,
                revision_number: old.revision_number,
                external_relations: old.clone().external_relations,
                created_at: None,
                created_by: old.created_by,
                data_class,
                description: req.get_description(old.clone()),
                name: old.clone().name,
                key_values: Json(req.get_add_keyvals(old.clone())?),
                hashes: old.clone().hashes,
                object_type: crate::database::enums::ObjectType::OBJECT,
                object_status: old.object_status.clone(),
                dynamic: false,
                endpoints: Json(req.get_endpoints(old.clone(), false)?),
                metadata_license: old.metadata_license,
                data_license: old.data_license,
            };
            update_object.update(transaction_client).await?;
            // Create & return all affected ids for cache sync
            let affected = if let Some(p) = request.parent.clone() {
                let mut relation = UpdateObject::add_parent_relation(
                    id,
                    p.clone(),
                    update_object.name.to_string(),
                )?;
                relation.create(transaction_client).await?;
                let p_id = match p {
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::ProjectId(id) => DieselUlid::from_str(&id)?,
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::CollectionId(id) => DieselUlid::from_str(&id)?,
                    aruna_rust_api::api::storage::services::v2::update_object_request::Parent::DatasetId(id) => DieselUlid::from_str(&id)?,
                };
                vec![p_id, update_object.id]
            } else {
                vec![update_object.id]
            };

            (id, false, affected)
        };
        transaction.commit().await?;

        // Cache sync of newly created object
        let owr = Object::get_object_with_relations(&id, &client).await?;
        self.cache.add_object(owr.clone());

        // Update all affected objects in cache
        if !affected.is_empty() {
            let affected = Object::get_objects_with_relations(&affected, &client).await?;
            for o in affected {
                dbg!(&o);
                self.cache.upsert_object(&o.object.id.clone(), o);
            }
        }

        let hierarchies = owr.object.fetch_object_hierarchies(&client).await?;

        // Trigger hooks for the 4 combinations:
        // 1. update in place & new key_vals
        // 2. New object & new key_vals
        // 3. New object & no added key_vals -> Only old key_vals
        // 4. update in place & no key_vals

        let object = owr.clone();
        if !req.0.add_key_values.is_empty() && !is_new {
            dbg!("TRIGGER");
            let kvs: Vec<KeyValue> = req
                .0
                .add_key_values
                .iter()
                .map(|kv| kv.try_into())
                .collect::<Result<Vec<KeyValue>>>()?;
            // FIXME: Inefficient, theoretically this could always be set to all TriggerVariants,
            // but needs additional testing
            let trigger_variants = kvs
                .iter()
                .filter_map(|kv| match kv.variant {
                    KeyValueVariant::HOOK => Some(TriggerVariant::HOOK_ADDED),
                    KeyValueVariant::LABEL => Some(TriggerVariant::LABEL_ADDED),
                    KeyValueVariant::STATIC_LABEL => Some(TriggerVariant::STATIC_LABEL_ADDED),
                    KeyValueVariant::HOOK_STATUS => None,
                })
                .dedup()
                .collect();
            let db_handler = DatabaseHandler {
                database: self.database.clone(),
                natsio_handler: self.natsio_handler.clone(),
                cache: self.cache.clone(),
                hook_sender: self.hook_sender.clone(),
            };
            // tokio::spawn cannot return errors, so manual error logs are returned
            tokio::spawn(async move {
                let call = db_handler
                    .trigger_hooks(object, user_id, trigger_variants, Some(kvs))
                    .await;
                if call.is_err() {
                    log::error!("{:?}", call);
                }
            });
        } else if !req.0.add_key_values.is_empty() && is_new {
            let kvs = req
                .0
                .add_key_values
                .iter()
                .map(|kv| kv.try_into())
                .collect::<Result<Vec<KeyValue>>>()?;
            // FIXME: Inefficient, theoretically this could always be set to all TriggerVariants,
            // but needs additional testing
            let mut trigger_variants: Vec<TriggerVariant> = kvs
                .iter()
                .filter_map(|kv| match kv.variant {
                    KeyValueVariant::HOOK => Some(TriggerVariant::HOOK_ADDED),
                    KeyValueVariant::LABEL => Some(TriggerVariant::LABEL_ADDED),
                    KeyValueVariant::STATIC_LABEL => Some(TriggerVariant::STATIC_LABEL_ADDED),
                    KeyValueVariant::HOOK_STATUS => None,
                })
                .dedup()
                .collect();
            trigger_variants.push(TriggerVariant::RESOURCE_CREATED);
            let db_handler = DatabaseHandler {
                database: self.database.clone(),
                natsio_handler: self.natsio_handler.clone(),
                cache: self.cache.clone(),
                hook_sender: self.hook_sender.clone(),
            };
            tokio::spawn(async move {
                let call_on_create = db_handler
                    .trigger_hooks(object, user_id, trigger_variants, Some(kvs))
                    .await;
                if call_on_create.is_err() {
                    log::error!("{:?}", call_on_create);
                }
            });
        } else if is_new {
            let db_handler = DatabaseHandler {
                database: self.database.clone(),
                natsio_handler: self.natsio_handler.clone(),
                cache: self.cache.clone(),
                hook_sender: self.hook_sender.clone(),
            };
            tokio::spawn(async move {
                let on_append = db_handler
                    .trigger_hooks(
                        object,
                        user_id,
                        vec![TriggerVariant::RESOURCE_CREATED],
                        None,
                    )
                    .await;
                if on_append.is_err() {
                    log::error!("{:?}", on_append);
                }
            });
        };

        // Try to emit object updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(
                &owr,
                hierarchies,
                EventVariant::Updated,
                Some(&DieselUlid::generate()), // block_id for deduplication
            )
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{}", err);
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed"))
        } else {
            //transaction.commit().await?;
            Ok((owr, is_new))
        }
    }

    pub async fn finish_object(
        &self,
        request: FinishObjectStagingRequest,
        user_id: DieselUlid,
    ) -> Result<ObjectWithRelations> {
        let mut client = self.database.get_client().await?;
        let id = DieselUlid::from_str(&request.object_id)?;
        let object = Object::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("Object not found"))?;
        let temp = object
            .endpoints
            .0
            .iter()
            .next()
            .ok_or_else(|| anyhow!("No endpoints defined in object"))?;
        let (endpoint_id, ep_info) = temp.pair();

        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let hashes = if request.hashes.is_empty() {
            None
        } else {
            Some(request.hashes.try_into()?)
        };
        let content_len = request.content_len;
        Object::finish_object_staging(
            &id,
            &transaction_client,
            hashes,
            content_len,
            ObjectStatus::AVAILABLE,
        )
        .await?;
        Object::update_endpoints(
            *endpoint_id,
            crate::database::dsls::object_dsl::EndpointInfo {
                replication: ep_info.replication,
                status: Some(crate::database::enums::ReplicationStatus::Finished),
            },
            vec![id],
            transaction_client,
        )
        .await?;

        transaction.commit().await?;

        let object = Object::get_object_with_relations(&id, &client).await?;
        let db_handler = DatabaseHandler {
            database: self.database.clone(),
            natsio_handler: self.natsio_handler.clone(),
            cache: self.cache.clone(),
            hook_sender: self.hook_sender.clone(),
        };
        let owr = object.clone();
        tokio::spawn(async move {
            let call = db_handler
                .trigger_hooks(owr, user_id, vec![TriggerVariant::OBJECT_FINISHED], None)
                .await;
            if call.is_err() {
                log::error!("{:?}", call);
            }
        });

        // TODO
        // - UpdateObjectEvent
        // - Replication logic
        //  ->  DataProxy, that calls finish, is updated to
        //      ReplicationStatus::Finished
        Ok(object)
    }
}
