use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::{
    InternalRelation, INTERNAL_RELATION_VARIANT_BELONGS_TO,
};
use crate::database::dsls::object_dsl::Object;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::{ObjectMapping, ObjectType};
use crate::middlelayer::db_handler::DatabaseHandler;

use anyhow::{anyhow, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use deadpool_postgres::GenericClient;
use diesel_ulid::DieselUlid;
use std::collections::HashMap;

impl DatabaseHandler {
    pub async fn clone_object(
        &self,
        user_id: &DieselUlid,
        object_id: &DieselUlid,
        parent: ObjectMapping<DieselUlid>,
    ) -> Result<ObjectWithRelations> {
        // Get standard database client
        let client = self.database.get_client().await?;

        // Fetch object to be cloned
        let original_object = Object::get_object_with_relations(object_id, &client).await?;

        // Prepare cloned object
        let new_id = DieselUlid::generate();
        let mut clone = original_object.object;
        clone.id = new_id;
        clone.created_by = *user_id;

        let (origin_pid, origin_type) = match parent {
            ObjectMapping::PROJECT(id) => (id, ObjectType::PROJECT),
            ObjectMapping::COLLECTION(id) => (id, ObjectType::COLLECTION),
            ObjectMapping::DATASET(id) => (id, ObjectType::DATASET),
            _ => return Err(anyhow!("Invalid parent")),
        };
        let mut relation = InternalRelation {
            id: DieselUlid::generate(),
            origin_pid,
            origin_type,
            relation_name: INTERNAL_RELATION_VARIANT_BELONGS_TO.to_string(),
            target_pid: new_id,
            target_type: ObjectType::OBJECT,
            target_name: clone.name.to_string(),
        };

        // Create object and relation in transaction
        let mut db_client = self.database.get_client().await?;
        let transaction = db_client.transaction().await?;
        let transaction_client = transaction.client();
        clone.create(transaction_client).await?;
        relation.create(transaction_client).await?;
        self.evaluate_and_update_rules(
            &vec![new_id, relation.origin_pid],
            &new_id,
            transaction_client,
        )
        .await?;
        transaction.commit().await?;

        // Fetch object DTOs for cloned object and parent
        let mut objects_plus: HashMap<DieselUlid, ObjectWithRelations> =
            Object::get_objects_with_relations(&vec![new_id, origin_pid], &client)
                .await?
                .into_iter()
                .map(|object_plus| (object_plus.object.id, object_plus))
                .collect();

        // Fetch all object paths for the notification subjects
        for object_plus in objects_plus.values() {
            let object_hierarchies = clone.fetch_object_hierarchies(&client).await?;
            let block_id = DieselUlid::generate();

            // Try to emit object created notification(s)
            if let Err(err) = self
                .natsio_handler
                .register_resource_event(
                    object_plus,
                    object_hierarchies,
                    EventVariant::Created,
                    Some(&block_id),
                )
                .await
            {
                // Log and return error
                log::error!("{err}");
                return Err(anyhow::anyhow!("Notification emission failed"));
            }
        }

        objects_plus
            .remove(&new_id)
            .ok_or_else(|| anyhow!("Object disappeared into the void"))
    }
}
