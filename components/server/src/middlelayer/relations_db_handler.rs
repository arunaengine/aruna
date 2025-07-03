use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO, INTERNAL_RELATION_VARIANT_VERSION,
};
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::relations_request_types::{
    ModifyRelations, RelationsToAdd, RelationsToModify, RelationsToRemove,
};
use ahash::HashSet;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use diesel_ulid::DieselUlid;

impl DatabaseHandler {
    pub async fn modify_relations(
        &self,
        resource: Object,
        relations_add: RelationsToAdd,
        relations_remove: RelationsToRemove,
    ) -> Result<ObjectWithRelations> {
        // Collect all affected ids before transaction
        let mut affected_objects: HashSet<DieselUlid> = HashSet::default();
        affected_objects.insert(resource.id);
        relations_add.internal.iter().for_each(|internal| {
            affected_objects.insert(internal.id);
        });
        relations_remove.internal.iter().for_each(|internal| {
            affected_objects.insert(internal.id);
        });

        // Create client
        let mut client = self.database.get_client().await?;
        // Check if BelongsTo relations are removed and at least one Version or BelongsTo relation remains
        let check_relations: Vec<InternalRelation> = relations_remove
            .internal
            .clone()
            .into_iter()
            .filter(|ir| ir.relation_name == INTERNAL_RELATION_VARIANT_BELONGS_TO)
            .collect();
        if !check_relations.is_empty() {
            for relation in check_relations {
                let target_id = relation.target_pid;
                let target = Object::get_object_with_relations(&target_id, &client).await?;
                // Check if at least one belongs to
                if target.inbound_belongs_to.0.len() < 2 {
                    // if not, are there any version relations?
                    if !target
                        .outbound
                        .0
                        .iter()
                        .any(|map| map.relation_name == INTERNAL_RELATION_VARIANT_VERSION)
                    {
                        return Err(anyhow!(
                            "Resources need at least one BelongsTo or Version relation!"
                        ));
                    }
                }
            }
        }

        // Transaction
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        if !relations_add.external.is_empty() {
            Object::add_external_relations(
                &resource.id,
                transaction_client,
                relations_add.external,
            )
            .await?;
        }
        if !relations_add.internal.is_empty() {
            InternalRelation::batch_create(&relations_add.internal, transaction_client).await?;
        }
        if !relations_remove.external.is_empty() {
            Object::remove_external_relation(
                &resource.id,
                transaction_client,
                relations_remove.external,
            )
            .await?;
        }
        if !relations_remove.internal.is_empty() {
            InternalRelation::batch_delete(
                &relations_remove.internal.iter().map(|r| r.id).collect(),
                transaction_client,
            )
            .await?;
        }

        self.evaluate_and_update_rules(
            &affected_objects.clone().into_iter().collect(),
            &resource.id,
            transaction_client,
        )
        .await?;
        transaction.commit().await?;

        // Try to emit object updated notification(s)
        let affected_ids = Vec::from_iter(affected_objects);

        let objects_plus = Object::get_objects_with_relations(&affected_ids, &client).await?;

        for object in &objects_plus {
            self.cache.upsert_object(&object.object.id, object.clone())
        }

        for object_plus in &objects_plus {
            let hierarchies = object_plus.object.fetch_object_hierarchies(&client).await?;
            let block_id = DieselUlid::generate();

            if let Err(err) = self
                .natsio_handler
                .register_resource_event(
                    object_plus,
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

        let object = Object::get_object_with_relations(&resource.id, &client).await?;
        Ok(object)
    }

    pub async fn get_resource(
        &self,
        request: ModifyRelations,
    ) -> Result<(Object, RelationsToModify)> {
        let client = self.database.get_client().await?;
        let id = request.get_id()?;
        let resource = Object::get_object_with_relations(&id, &client).await?;
        Ok((
            resource.object.clone(),
            request.get_relations(resource, &client).await?, // Client instead of transaction client is okay here, because only get requests are made before modifications
        ))
    }
}
