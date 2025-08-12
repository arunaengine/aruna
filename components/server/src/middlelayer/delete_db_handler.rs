use std::collections::{HashSet, VecDeque};

use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_VERSION,
};
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::{ObjectStatus, ObjectType};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::{database::dsls::object_dsl::Object, middlelayer::delete_request_types::DeleteRequest};
use anyhow::{bail, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use diesel_ulid::DieselUlid;
use itertools::Itertools;

impl DatabaseHandler {
    pub async fn delete_resource(
        &self,
        delete_request: DeleteRequest,
    ) -> Result<Vec<ObjectWithRelations>> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let id = delete_request.get_id()?;

        // Fetch full object including relations
        let root_object = Object::get_object_with_relations(&id, transaction_client).await?;

        let (object_ids_to_delete, relation_ids_to_delete, affected_resources) =
            match delete_request {
                DeleteRequest::Object(request) => {
                    //  - Set all inbound 'BELONGS_TO' relations to 'DELETED'
                    //  - Set object_status to 'DELETED'
                    //  - if 'with_revisions: true' repeat for all versions
                    let mut objects = vec![root_object.clone()];
                    let mut affected_resources: HashSet<DieselUlid> = HashSet::default();

                    let version_ids = root_object
                        .inbound
                        .0
                        .iter()
                        .filter_map(|o| match o.relation_name.as_str() {
                            INTERNAL_RELATION_VARIANT_VERSION => Some(o.origin_pid),
                            _ => None,
                        })
                        .collect::<Vec<_>>();

                    // Collect objects for deletion depending if with revisions
                    if !version_ids.is_empty() {
                        let mut version_objects =
                            Object::get_objects_with_relations(&version_ids, transaction_client)
                                .await?;

                        if request.with_revisions {
                            objects.append(&mut version_objects);
                        } else {
                            for version in version_objects {
                                if version.object.object_status != ObjectStatus::DELETED {
                                    bail!("Object has undeleted versions");
                                }
                            }
                        }
                    }

                    let mut relation_ids = vec![];
                    objects.iter().for_each(|o| {
                        // Collect parents for updated notification
                        o.get_parents().into_iter().for_each(|p| {
                            affected_resources.insert(p);
                        });
                        // Collect relations to parents for deletion
                        o.inbound_belongs_to
                            .0
                            .iter()
                            .for_each(|entry| relation_ids.push(entry.value().id))
                    });

                    (
                        objects.into_iter().map(|o| o.object.id).collect_vec(),
                        relation_ids,
                        affected_resources,
                    )
                }
                _ => {
                    let mut affected_resources: HashSet<DieselUlid> = HashSet::default();
                    let mut ids_to_delete: HashSet<DieselUlid> = HashSet::default();
                    let mut relations_to_delete: Vec<DieselUlid> = Vec::new();
                    let mut queue = VecDeque::new();
                    queue.push_back(root_object.clone());

                    while let Some(resource) = queue.pop_front() {
                        match resource.object.object_type {
                            ObjectType::PROJECT | ObjectType::COLLECTION | ObjectType::DATASET => {
                                // Check if undeleted versions exist (Always 0 for Projects)
                                let version_ids = resource
                                    .inbound
                                    .0
                                    .iter()
                                    .filter_map(|o| match o.relation_name.as_str() {
                                        INTERNAL_RELATION_VARIANT_VERSION => Some(o.origin_pid),
                                        _ => None,
                                    })
                                    .collect::<Vec<_>>();

                                let versions =
                                    Object::get_objects(&version_ids, transaction_client).await?;

                                for version in versions {
                                    if version.object_status != ObjectStatus::DELETED {
                                        bail!(
                                            "{:?} has undeleted versions",
                                            resource.object.object_type
                                        )
                                    }
                                }

                                // Check if parents are all already marked for deletion (except root)
                                if resource.object.id != root_object.object.id {
                                    for parent_id in resource.get_parents() {
                                        if !ids_to_delete.contains(&parent_id) {
                                            bail!("Resource {} still has parents in multiple hierarchies", resource.object.id)
                                        }
                                    }
                                } else {
                                    // Collect affected resources for update notifications
                                    resource.get_parents().into_iter().for_each(|p| {
                                        affected_resources.insert(p);
                                    });

                                    // Collect parent relations for deletion
                                    relations_to_delete.append(
                                        &mut resource
                                            .inbound_belongs_to
                                            .0
                                            .iter()
                                            .map(|entry| entry.value().id)
                                            .collect_vec(),
                                    )
                                }

                                // Mark object and its outbound relations for deletion
                                ids_to_delete.insert(resource.object.id);
                                relations_to_delete.append(
                                    &mut resource
                                        .outbound_belongs_to
                                        .0
                                        .iter()
                                        .map(|entry| entry.value().id)
                                        .collect_vec(),
                                );

                                // Add all children of resource to queue
                                Object::get_objects_with_relations(
                                    &resource.get_children(),
                                    transaction_client,
                                )
                                .await?
                                .into_iter()
                                .for_each(|o| queue.push_back(o))
                            }
                            ObjectType::OBJECT => {
                                for parent_id in resource.get_parents() {
                                    if !ids_to_delete.contains(&parent_id) {
                                        bail!(
                                            "Resource {} still has parents in multiple hierarchies",
                                            resource.object.id
                                        )
                                    }
                                }
                                ids_to_delete.insert(resource.object.id);

                                let version_ids = resource
                                    .inbound
                                    .0
                                    .iter()
                                    .filter_map(|o| match o.relation_name.as_str() {
                                        INTERNAL_RELATION_VARIANT_VERSION => Some(o.origin_pid),
                                        _ => None,
                                    })
                                    .collect::<Vec<_>>();

                                let versions = Object::get_objects_with_relations(
                                    &version_ids,
                                    transaction_client,
                                )
                                .await?;

                                for version in versions {
                                    for parent_id in version.get_parents() {
                                        if ids_to_delete.contains(&parent_id) {
                                            bail!("Object version has parents outside the deletion hierarchy")
                                        }
                                    }
                                    ids_to_delete.insert(version.object.id);
                                }
                            }
                        }
                    }

                    (
                        ids_to_delete.into_iter().collect_vec(),
                        relations_to_delete,
                        affected_resources,
                    )
                }
            };

        // "Delete" relations
        InternalRelation::set_deleted(&relation_ids_to_delete, transaction_client).await?;

        // Delete Objects
        println!("DELETING ... ");
        Object::set_deleted(&object_ids_to_delete, transaction_client).await?;

        // Evaluate rules
        let mut all: Vec<DieselUlid> = affected_resources.clone().into_iter().collect();
        all.push(id);
        self.evaluate_rules(&all, transaction_client).await?;
        // Commit transaction
        transaction.commit().await?;
        println!(" ... DELETED ");

        // Fetch hierarchies and object relations for notifications
        let deleted_objects =
            Object::get_objects_with_relations(&object_ids_to_delete, &client).await?;

        let updated_objects = Object::get_objects_with_relations(
            &affected_resources.into_iter().collect_vec(),
            &client,
        )
        .await?;

        // Update cache before notifications
        for deleted_object in &deleted_objects {
            self.cache
                .upsert_object(&deleted_object.object.id, deleted_object.clone());
        }

        for updated_object in &updated_objects {
            self.cache
                .upsert_object(&updated_object.object.id, updated_object.clone());
        }

        // Send notifications for deleted and updated resources
        for deleted_object in &deleted_objects {
            let hierarchies = deleted_object
                .object
                .fetch_object_hierarchies(&client)
                .await?;
            let block_id = DieselUlid::generate();

            if let Err(err) = self
                .natsio_handler
                .register_resource_event(
                    deleted_object,
                    hierarchies,
                    EventVariant::Deleted,
                    Some(&block_id),
                )
                .await
            {
                // Log error, rollback transaction and return
                log::error!("{err}");
                //transaction.rollback().await?;
                return Err(anyhow::anyhow!("Notification emission failed"));
            }
        }

        for updated_object in &updated_objects {
            let hierarchies = updated_object
                .object
                .fetch_object_hierarchies(&client)
                .await?;
            let block_id = DieselUlid::generate();

            if let Err(err) = self
                .natsio_handler
                .register_resource_event(
                    updated_object,
                    hierarchies,
                    EventVariant::Updated,
                    Some(&block_id),
                )
                .await
            {
                // Log error, rollback transaction and return
                log::error!("{err}");
                //transaction.rollback().await?;
                return Err(anyhow::anyhow!("Notification emission failed"));
            }
        }

        Ok(deleted_objects)
    }
}
