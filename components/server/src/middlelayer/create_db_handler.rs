use crate::database::crud::CrudDb;
use crate::database::dsls::hook_dsl::TriggerVariant;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::object_dsl::{KeyValue, KeyValueVariant, Object, ObjectWithRelations};
use crate::database::dsls::user_dsl::User;
use crate::database::enums::{DbPermissionLevel, ObjectMapping, ObjectStatus, ObjectType};
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use ahash::RandomState;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use tokio_postgres::Client;

impl DatabaseHandler {
    pub async fn create_resource(
        &self,
        request: CreateRequest,
        user_id: DieselUlid,
        is_dataproxy: bool,
    ) -> Result<(ObjectWithRelations, Option<User>)> {
        // Init transaction
        let mut client = self.database.get_client().await?;

        // check if resource with same name on same hierarchy exists
        match request.get_type() {
            ObjectType::PROJECT => {
                let name = request.get_name()?;
                let object = Object::check_existing_projects(name, &client).await?;
                if let Some(object) = object {
                    if !matches!(object.object.object_status, ObjectStatus::DELETED) {
                        return if is_dataproxy {
                            // return existing project
                            Ok((object, None))
                        } else {
                            // return err
                            Err(anyhow!("Project exists!"))
                        };
                    }
                };
            }
            ObjectType::DATASET | ObjectType::COLLECTION => {
                self.check_hierarchy(&request).await?;
            }
            ObjectType::OBJECT => {
                self.check_object(&request).await?;
            }
        }
        // Transaction setup
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let mut user = None;

        // Create object in database
        let mut object = request
            .as_new_db_object(user_id, transaction_client, self.cache.clone())
            .await?;
        object.create(transaction_client).await?;

        // Create internal relation for parent and add user permissions for resource
        let (parent, _internal_relation): (
            Option<ObjectWithRelations>,
            DashMap<DieselUlid, InternalRelation, RandomState>,
        ) = match request.get_type() {
            ObjectType::PROJECT => {
                user = Some(
                    self.add_permission_to_user(
                        user_id,
                        object.id,
                        &object.name,
                        ObjectMapping::PROJECT(DbPermissionLevel::ADMIN),
                        false,
                    )
                    .await?,
                );

                (None, DashMap::default())
            }
            _ => {
                let parent = request
                    .get_parent()
                    .ok_or_else(|| anyhow!("No parent provided"))?;

                let mut ir = InternalRelation {
                    id: DieselUlid::generate(),
                    origin_pid: parent.get_id()?,
                    origin_type: parent.get_type(),
                    target_pid: object.id,
                    target_type: object.object_type,
                    relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
                    target_name: object.name.to_string(),
                };
                let result = ir.create(transaction_client).await;

                // When dataproxy creates object and object exists, return Ok(existing_object)
                if result.is_err() && is_dataproxy {
                    transaction.rollback().await?;
                    // Get parent ...
                    if let Some(parent) = self.cache.get_object(&parent.get_id()?) {
                        // ... iterate over outbound relations ...
                        for (id, irel) in parent.outbound_belongs_to.0 {
                            // ... if object name exists in outbound relations ...
                            if irel.target_name == object.name {
                                // ... return existing object
                                return Ok((
                                    self.cache
                                        .get_object(&id)
                                        .ok_or_else(|| anyhow!("Cache not synced"))?
                                        .clone(),
                                    None,
                                ));
                            }
                        }
                    }
                    return Err(anyhow!(
                        "Either cache not synced or other database error while creating object"
                    ));
                }

                let parent =
                    Object::get_object_with_relations(&parent.get_id()?, transaction_client)
                        .await?;

                (
                    Some(parent.clone()),
                    DashMap::from_iter([(parent.object.id, ir)]),
                )
            }
        };

        let affected = self
            .collect_and_create_affected(request, &object, transaction_client)
            .await?;

        if let Err(err) = self
            .evaluate_and_update_rules(&affected, &object.id, transaction_client)
            .await
        {
            transaction.rollback().await?;
            return Err(err);
        }

        transaction.commit().await?;

        // Update cache with affected objects
        let affected_owrs = Object::get_objects_with_relations(&affected, &client).await?;
        for affected_owr in affected_owrs {
            self.cache
                .upsert_object(&affected_owr.object.id, affected_owr.clone())
        }

        // Create DTO which combines the object and its internal relations
        let owr = Object::get_object_with_relations(&object.id, &client).await?;

        // Update cache
        self.cache.add_object(owr.clone());
        if let Some(parent_plus) = parent {
            self.cache
                .upsert_object(&parent_plus.object.id, parent_plus.clone());

            // If created resource has parent emit notification for updated parent
            let parent_hierarchies = parent_plus.object.fetch_object_hierarchies(&client).await?;

            // Try to emit object created notification(s)
            if let Err(err) = self
                .natsio_handler
                .register_resource_event(
                    &parent_plus,
                    parent_hierarchies,
                    EventVariant::Updated,
                    Some(&DieselUlid::generate()), // block_id for deduplication
                )
                .await
            {
                // Log error and return
                log::error!("{err}");
                //transaction.rollback().await?;
                return Err(anyhow::anyhow!("Notification emission failed: {err}"));
            }
        };

        // Trigger hooks
        let db_handler = DatabaseHandler {
            database: self.database.clone(),
            natsio_handler: self.natsio_handler.clone(),
            cache: self.cache.clone(),
            hook_sender: self.hook_sender.clone(),
        };
        let trigger: Vec<TriggerVariant> = {
            let mut trigger = vec![TriggerVariant::RESOURCE_CREATED];
            if !object.key_values.0 .0.is_empty() {
                for KeyValue { variant, .. } in &object.key_values.0 .0 {
                    match variant {
                        KeyValueVariant::HOOK => trigger.push(TriggerVariant::HOOK_ADDED),
                        KeyValueVariant::LABEL => trigger.push(TriggerVariant::LABEL_ADDED),
                        KeyValueVariant::STATIC_LABEL => {
                            trigger.push(TriggerVariant::STATIC_LABEL_ADDED)
                        }
                        KeyValueVariant::HOOK_STATUS => {
                            trigger.push(TriggerVariant::HOOK_STATUS_CHANGED)
                        }
                    }
                }
            }
            trigger
        };
        let object_with_relation = owr.clone();
        tokio::spawn(async move {
            let hook_trigger = db_handler
                .trigger_hooks(object_with_relation, trigger, None)
                .await;
            if hook_trigger.is_err() {
                log::error!("{hook_trigger:?}")
            }
        });
        // Fetch all object paths for the notification subjects
        let object_hierarchies = object.fetch_object_hierarchies(&client).await?;
        // Try to emit object created notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(
                &owr,
                object_hierarchies,
                EventVariant::Created,
                Some(&DieselUlid::generate()), // block_id for deduplication
            )
            .await
        {
            // Log error, rollback transaction and return
            log::error!("{err}");
            //transaction.rollback().await?;
            Err(anyhow::anyhow!("Notification emission failed: {err}"))
        } else {
            // Commit transaction and return
            //transaction.commit().await?;
            Ok((owr, user))
        }
    }

    async fn check_hierarchy(&self, request: &CreateRequest) -> Result<()> {
        let client = self.database.get_client().await?;
        let parent_id = request
            .get_parent()
            .ok_or_else(|| anyhow!("No parent found"))?
            .get_id()?;
        let parent = Object::get_object_with_relations(&parent_id, &client).await?;

        // Check if parent already has a child with same name
        let query = request.get_name()?;
        let children = parent
            .outbound_belongs_to
            .0
            .iter()
            .map(|rel| match rel.target_type {
                ObjectType::OBJECT => {
                    // Check if object splits by '/'
                    match rel.target_name.split('/').next().map(|s| s.to_string()) {
                        // return first path
                        Some(split) => split,
                        // return full name
                        None => rel.target_name.to_string(),
                    }
                }
                // return name of other resources
                _ => rel.target_name.to_string(),
            })
            .collect_vec();

        log::debug!("[Check Hierarchy] {query:?} {children:?}");
        if children.contains(&query) {
            return Err(anyhow!("Name is invalid: Contains path of object"));
        }
        Ok(())
    }

    async fn check_object(&self, request: &CreateRequest) -> Result<()> {
        let client = self.database.get_client().await?;

        // Fetch parent
        let parent_id = request
            .get_parent()
            .ok_or_else(|| anyhow!("No parent found"))?
            .get_id()?;
        let parent = Object::get_object_with_relations(&parent_id, &client).await?;

        // Check if parent already has a child with same name
        let query = request.get_name()?;
        let children = parent
            .outbound_belongs_to
            .0
            .iter()
            .map(|rel| rel.target_name.to_string())
            .collect_vec();

        log::debug!("[Check Object] {query:#?} {children:#?}");
        if children.contains(&query) {
            return Err(anyhow!(
                "Name is invalid: Contains substring that matches same hierarchy object"
            ));
        }
        Ok(())
    }

    async fn collect_and_create_affected(
        &self,
        request: CreateRequest,
        object: &Object,
        transaction_client: &Client,
    ) -> Result<Vec<DieselUlid>> {
        // Create specified relations
        let internal_relations = request
            .get_internal_relations(object.id, transaction_client)
            .await?;
        if !internal_relations.is_empty() {
            InternalRelation::batch_create(&internal_relations, transaction_client).await?;
        }
        // Collect affected objects
        let mut affected: Vec<DieselUlid> = Vec::new();
        for (source, destination) in internal_relations
            .iter()
            .map(|ir| (ir.origin_pid, ir.target_pid))
        {
            if source == object.id && destination == object.id {
                // Are relations from self to self even possible?
                continue;
            }
            if source == object.id {
                affected.push(destination);
            } else if destination == object.id {
                affected.push(source);
            } else {
                affected.push(source);
                affected.push(destination);
            }
        }
        Ok(affected)
    }
}
