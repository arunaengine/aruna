use super::replication_request_types::ReplicationVariant;
use crate::{
    database::{
        crud::CrudDb,
        dsls::object_dsl::{EndpointInfo, Object},
        enums::{ObjectType, ReplicationStatus, ReplicationType},
    },
    middlelayer::db_handler::DatabaseHandler,
    utils::database_utils::sort_objects,
};
use anyhow::{anyhow, Result};
use aruna_rust_api::api::{
    notification::services::v2::EventVariant,
    storage::{
        models::v2::{DataEndpoint, ReplicationStatus as APIReplicationStatus},
        services::v2::{
            partial_replicate_data_request::DataVariant, GetReplicationStatusResponse,
            ReplicationInfo, UpdateReplicationStatusRequest,
        },
    },
};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use std::str::FromStr;

impl DatabaseHandler {
    pub async fn replicate(&self, request: ReplicationVariant) -> Result<APIReplicationStatus> {
        let mut client = self.database.get_client().await?;
        let (resource_id, proxy_id, endpoint_status_objects, endpoint_status_hierarchy) =
            match request {
                ReplicationVariant::Full(request) => {
                    let project_id = DieselUlid::from_str(&request.project_id)?;
                    if let Some(project) = Object::get(project_id, &client).await? {
                        if project.object_type != ObjectType::PROJECT {
                            return Err(anyhow!("Resource is not a project"));
                        }
                    } else {
                        return Err(anyhow!("Resource not found"));
                    };
                    let proxy_id = DieselUlid::from_str(&request.endpoint_id)?;
                    let endpoint_status_objects = EndpointInfo {
                        replication: ReplicationType::FullSync,
                        status: Some(ReplicationStatus::Waiting),
                    };
                    let endpoint_status_hierarchy = EndpointInfo {
                        replication: ReplicationType::FullSync,
                        status: None,
                    };

                    (
                        project_id,
                        proxy_id,
                        endpoint_status_objects,
                        endpoint_status_hierarchy,
                    )
                }
                ReplicationVariant::Partial(request) => {
                    let (request_type, resource_id) = match request.data_variant {
                        Some(res) => match res {
                            DataVariant::CollectionId(id) => (
                                ObjectType::COLLECTION,
                                diesel_ulid::DieselUlid::from_str(&id)?,
                            ),
                            DataVariant::DatasetId(id) => {
                                (ObjectType::DATASET, diesel_ulid::DieselUlid::from_str(&id)?)
                            }
                            DataVariant::ObjectId(id) => {
                                (ObjectType::OBJECT, diesel_ulid::DieselUlid::from_str(&id)?)
                            }
                        },
                        None => {
                            return Err(anyhow!("Invalid resource id"));
                        }
                    };
                    let proxy_id = DieselUlid::from_str(&request.endpoint_id)?;
                    let origin = Object::get(resource_id, &client)
                        .await?
                        .ok_or_else(|| anyhow!("Object not found"))?;
                    if origin.object_type != request_type {
                        return Err(anyhow!("Wrong data variant provided"));
                    }
                    if let Some(ep) = origin.endpoints.0.get(&proxy_id) {
                        if ep.replication == ReplicationType::FullSync {
                            return Err(anyhow!("Cannot overwrite FullSync status for Endpoint"));
                            // TODO: At least for now this is not allowed, because additional
                            // logic for checking is needed
                        }
                    }
                    let proxy_id = DieselUlid::from_str(&request.endpoint_id)?;
                    let endpoint_status_objects = EndpointInfo {
                        replication: ReplicationType::PartialSync(true),
                        status: Some(ReplicationStatus::Waiting),
                    };
                    let endpoint_status_hierarchy = EndpointInfo {
                        replication: ReplicationType::PartialSync(true),
                        status: None,
                    };

                    (
                        resource_id,
                        proxy_id,
                        endpoint_status_objects,
                        endpoint_status_hierarchy,
                    )
                }
            };
        // Get all sub resources for project
        let res = Object::get(resource_id, &client)
            .await?
            .ok_or_else(|| anyhow!("ReplicationResource not found"))?;
        let mut sub_res: Vec<Object> =
            Object::fetch_recursive_objects(&resource_id, &client).await?;
        sub_res.push(res);

        // Collect all objects
        let objects: Vec<DieselUlid> = sub_res
            .iter()
            .filter_map(|o| match o.object_type {
                ObjectType::OBJECT => Some(o.id),
                _ => None,
            })
            .collect();

        // Collect all none-objects
        let hierarchy_resources: Vec<DieselUlid> = sub_res
            .iter()
            .filter_map(|r| match r.object_type {
                ObjectType::OBJECT => None,
                _ => Some(r.id),
            })
            .collect();

        // TODO:
        // - Find out which resources are outside of fullsync
        let mut all_affected = Vec::new();
        for object in &sub_res {
            let mut hierarchy = Object::fetch_object_hierarchies_by_id(&object.id, &client).await?;
            all_affected.append(&mut hierarchy);
        }

        let mut partial_synced_hierarchy = Vec::new();
        let mut partial_synced_objects = Vec::new();
        for affected_hierarchy in &all_affected {
            let mut flattened_hierarchy = vec![affected_hierarchy.project_id.clone()];
            if let Some(collection) = &affected_hierarchy.collection_id {
                flattened_hierarchy.push(collection.to_string());
            }
            if let Some(dataset) = &affected_hierarchy.dataset_id {
                flattened_hierarchy.push(dataset.to_string());
            }
            if let Some(object) = &affected_hierarchy.object_id {
                if !objects.iter().map(|id| id.to_string()).contains(object) {
                    partial_synced_objects.push(DieselUlid::from_str(object)?);
                }
            }
            for id in flattened_hierarchy {
                if !hierarchy_resources
                    .iter()
                    .map(|id| id.to_string())
                    .contains(&id)
                {
                    partial_synced_hierarchy.push(DieselUlid::from_str(&id)?);
                }
            }
        }

        // Create transaction for status & endpoint updates
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        // Update objects with Status and EndpointInfo
        if !objects.is_empty() {
            Object::update_endpoints(
                proxy_id,
                endpoint_status_objects.clone(),
                objects,
                transaction_client,
            )
            .await?;
        }

        // Update non-objects only with EndpointInfo
        if !hierarchy_resources.is_empty() {
            Object::update_endpoints(
                proxy_id,
                endpoint_status_hierarchy.clone(),
                hierarchy_resources,
                transaction_client,
            )
            .await?;
        }

        // Update not-explicit synced with PartialSyncInfo
        if !partial_synced_hierarchy.is_empty() {
            let ep_status_hierarchy = match endpoint_status_hierarchy.replication {
                ReplicationType::FullSync => EndpointInfo {
                    replication: ReplicationType::PartialSync(false),
                    status: None,
                },
                ReplicationType::PartialSync(_) => endpoint_status_hierarchy,
            };
            Object::update_endpoints(
                proxy_id,
                ep_status_hierarchy,
                partial_synced_hierarchy.clone(),
                transaction_client,
            )
            .await?;
        }

        if !partial_synced_objects.is_empty() {
            let ep_status_objects = match endpoint_status_objects.replication {
                ReplicationType::FullSync => EndpointInfo {
                    replication: ReplicationType::PartialSync(false),
                    status: Some(ReplicationStatus::Waiting),
                },
                ReplicationType::PartialSync(_) => endpoint_status_objects,
            };
            Object::update_endpoints(
                proxy_id,
                ep_status_objects,
                partial_synced_objects.clone(),
                transaction_client,
            )
            .await?;
        }

        // Try to emit object updated notification(s)
        let mut all_updated: Vec<DieselUlid> = sub_res.iter().map(|r| r.id).collect();
        all_updated.push(resource_id);
        all_updated.append(&mut partial_synced_objects);
        all_updated.append(&mut partial_synced_hierarchy);

        self.evaluate_rules(&all_updated, transaction_client)
            .await?;
        transaction.commit().await?;
        let mut all = Object::get_objects_with_relations(&all_updated, &client).await?;

        sort_objects(&mut all);
        for owr in all {
            self.cache.upsert_object(&owr.object.id, owr.clone());
            if let Err(err) = self
                .natsio_handler
                .register_resource_event(
                    &owr,
                    owr.object.fetch_object_hierarchies(&client).await?,
                    EventVariant::Updated,
                    Some(&DieselUlid::generate()), // block_id for deduplication
                )
                .await
            {
                log::error!("{err}");
                return Ok(APIReplicationStatus::Error);
            };
        }
        Ok(APIReplicationStatus::Waiting)
    }

    pub async fn update_replication_status(
        &self,
        request: UpdateReplicationStatusRequest,
    ) -> Result<()> {
        let client = self.database.get_client().await?;
        let object_id = DieselUlid::from_str(&request.object_id)?;
        let object = Object::get(object_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Object not found"))?;
        let endpoint_id = DieselUlid::from_str(&request.endpoint_id)?; // TODO: Remove from API and
                                                                       // query from token
        let mut endpoint_info = object
            .endpoints
            .0
            .get_mut(&endpoint_id)
            .ok_or_else(|| anyhow!("Endpoint not found in object"))?;
        let status = match request.status() {
            APIReplicationStatus::Unspecified => {
                return Err(anyhow!("Unspecified replication status"))
            }
            APIReplicationStatus::Waiting => ReplicationStatus::Waiting,
            APIReplicationStatus::Running => ReplicationStatus::Running,
            APIReplicationStatus::Finished => ReplicationStatus::Finished,
            APIReplicationStatus::Error => ReplicationStatus::Error,
        };
        endpoint_info.status = Some(status);
        Object::update_endpoints(endpoint_id, endpoint_info.clone(), vec![object_id], &client)
            .await?;

        // Update cache
        let updated = Object::get_object_with_relations(&object_id, &client).await?;
        self.cache.upsert_object(&object_id, updated.clone());
        if let Err(err) = self
            .natsio_handler
            .register_resource_event(
                &updated,
                updated.object.fetch_object_hierarchies(&client).await?,
                EventVariant::Updated,
                Some(&DieselUlid::generate()), // block_id for deduplication
            )
            .await
        {
            log::error!("{err}");
            return Err(anyhow::anyhow!("Notification emission failed"));
        };
        Ok(())
    }
    pub async fn get_replication_status(
        &self,
        endpoint_id: DieselUlid,
        resource_id: DieselUlid,
    ) -> Result<GetReplicationStatusResponse> {
        let mut sub_resources = vec![resource_id];
        sub_resources.append(&mut self.cache.get_subresources(&resource_id)?);

        let mut infos: Vec<ReplicationInfo> = Vec::new();
        for id in sub_resources {
            if let Some(resource) = self.cache.get_object(&id) {
                let endpoint_info = if let Some(ep) = resource.object.endpoints.0.get(&endpoint_id)
                {
                    Some(DataEndpoint {
                        id: endpoint_id.to_string(),
                        variant: Some(ep.replication.into()),
                        status: ep
                            .status
                            .map(|status| APIReplicationStatus::from(status) as i32),
                    })
                } else {
                    continue; // not sure if this is the right call, but for partial synced there
                              // is the possibility of objects that do not have the requested
                              // endpoint id as a sub-resource
                };
                let info = ReplicationInfo {
                    resource: Some(
                        match resource.object.object_type {
                            ObjectType::PROJECT => aruna_rust_api::api::storage::services::v2::replication_info::Resource::ProjectId(id.to_string()),
                            ObjectType::COLLECTION => aruna_rust_api::api::storage::services::v2::replication_info::Resource::CollectionId(id.to_string()),
                            ObjectType::DATASET => aruna_rust_api::api::storage::services::v2::replication_info::Resource::DatasetId(id.to_string()),
                            ObjectType::OBJECT => aruna_rust_api::api::storage::services::v2::replication_info::Resource::ObjectId(id.to_string()),
                        },
                    ),
                    endpoint_info,
                };
                infos.push(info);
            } else {
                return Err(anyhow!("Resource not found"));
            }
        }
        Ok(GetReplicationStatusResponse { infos })
    }

    pub async fn delete_replication(
        &self,
        endpoint_id: DieselUlid,
        resource_id: DieselUlid,
    ) -> Result<()> {
        let mut client = self.database.get_client().await?;
        let specified_resource = Object::get(resource_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Specified resource not found"))?;
        if let Some(ep) = specified_resource.endpoints.0.get(&endpoint_id) {
            match ep.replication {
                ReplicationType::FullSync => {
                    if specified_resource.object_type != ObjectType::PROJECT {
                        return Err(anyhow!(
                            "Can not delete lower hierarchy objects when full synced"
                        ));
                    }
                }
                ReplicationType::PartialSync(inherits) => {
                    if !inherits {
                        return Err(anyhow!("Can only delete root partial synced objects"));
                    }
                    let higher_ups: Vec<DieselUlid> =
                        Object::fetch_object_hierarchies_by_id(&resource_id, &client)
                            .await?
                            .into_iter()
                            .map(|hierarchy| -> Result<Vec<DieselUlid>> {
                                let mut flattened =
                                    vec![DieselUlid::from_str(&hierarchy.project_id)?];
                                if let Some(id) = hierarchy.collection_id {
                                    flattened.push(DieselUlid::from_str(&id)?);
                                }
                                if let Some(id) = hierarchy.dataset_id {
                                    flattened.push(DieselUlid::from_str(&id)?);
                                }
                                if let Some(id) = hierarchy.object_id {
                                    flattened.push(DieselUlid::from_str(&id)?);
                                }
                                Ok(flattened)
                            })
                            .collect::<Result<Vec<Vec<DieselUlid>>>>()?
                            .into_iter()
                            .flatten()
                            .dedup()
                            .collect();
                    let higher_ups = Object::get_objects(&higher_ups, &client).await?;
                    for resource in higher_ups {
                        if resource.endpoints.0.get(&endpoint_id).is_some() {
                            return Err(anyhow!("Cannot delete replication if higher resources exist with specified dataproxy replication"));
                        }
                    }
                }
            }
        }
        let mut resource_ids: Vec<DieselUlid> = vec![resource_id];
        let mut updated_objects = Vec::new();
        resource_ids.append(&mut self.cache.get_subresources(&resource_id)?);
        for res in resource_ids {
            let mut object = Object::get_object_with_relations(&res, &client).await?;

            // Remove endpoint and check if at least one FullSync proxy remains
            let temp_eps = object.object.endpoints.0.clone();
            temp_eps.remove(&endpoint_id);
            if temp_eps
                .iter()
                .any(|ep| matches!(ep.replication, ReplicationType::FullSync))
            {
                object.object.endpoints.0 = temp_eps;
                updated_objects.push(object);
            } else {
                return Err(anyhow!(
                    "At least one FullSync proxy needs to be defined for object"
                ));
            }
        }
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let mut ids = Vec::new();
        for object in &updated_objects {
            object.object.update(transaction_client).await?;
            ids.push(object.object.id);
        }
        self.evaluate_rules(&ids, transaction_client).await?;
        transaction.commit().await?;

        for object in &updated_objects {
            self.cache.upsert_object(&object.object.id, object.clone());
        }
        for object in updated_objects {
            let hierarchies = object.object.fetch_object_hierarchies(&client).await?;
            let block_id = DieselUlid::generate();

            if let Err(err) = self
                .natsio_handler
                .register_dataproxy_event(
                    &object,
                    hierarchies.clone(),
                    EventVariant::Deleted,
                    endpoint_id,
                    Some(&block_id),
                )
                .await
            {
                // Log error, rollback transaction and return
                log::error!("{err}");
                return Err(anyhow::anyhow!("Notification emission failed"));
            }
            let block_id = DieselUlid::generate();

            if let Err(err) = self
                .natsio_handler
                .register_resource_event(
                    &object,
                    hierarchies,
                    EventVariant::Updated,
                    Some(&block_id),
                )
                .await
            {
                // Log error, rollback transaction and return
                log::error!("{err}");
                return Err(anyhow::anyhow!("Notification emission failed"));
            }
        }

        Ok(())
    }
}
