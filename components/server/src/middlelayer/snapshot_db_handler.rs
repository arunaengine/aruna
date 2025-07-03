use crate::database::dsls::object_dsl::{Object, ObjectWithRelations};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::snapshot_request_types::{SnapshotRequest, SnapshotResponse};
use anyhow::Result;
use aruna_rust_api::api::notification::services::v2::EventVariant;
use diesel_ulid::DieselUlid;

impl DatabaseHandler {
    pub async fn snapshot(
        &self,
        request: SnapshotRequest,
    ) -> Result<(DieselUlid, Vec<ObjectWithRelations>)> {
        let client = self.database.get_client().await?;
        let id = request.get_id()?;
        let resource = Object::get_object_with_relations(&id, &client).await?;

        // Evaluate snapshot type
        let (new_object_id, mut snapshot_resources) = match request {
            SnapshotRequest::Project(_) => (
                id,
                SnapshotResponse::ArchiveProject(
                    SnapshotRequest::get_archived_project(&resource, &client).await?,
                ),
            ),
            SnapshotRequest::Collection(_) => {
                let request =
                    SnapshotRequest::get_cloned_collection(resource.clone(), &client).await?;
                (
                    request.collection.id,
                    SnapshotResponse::SnapshotCollection(request),
                )
            }
            SnapshotRequest::Dataset(_) => {
                let request = SnapshotRequest::get_cloned_dataset(resource.clone()).await?;
                (
                    request.dataset.id,
                    SnapshotResponse::SnapshotDataset(request),
                )
            }
        };

        // Execute archive/snapshot
        let result = snapshot_resources.snapshot(self).await?;
        // Update cache
        for o in &result {
            self.cache.upsert_object(&o.object.id, o.clone());
        }

        // Notifications
        let client = self.database.get_client().await?; // Refresh database connection ...
        let emissions = match snapshot_resources {
            SnapshotResponse::ArchiveProject(_) => {
                //  - Project archive -> All affected objects EventVariant::Updated
                let mut notifications = vec![];
                for obj in &result {
                    notifications.push((
                        obj,
                        obj.object.fetch_object_hierarchies(&client).await?,
                        EventVariant::Updated,
                        DieselUlid::generate(), // block_id for deduplication
                    ))
                }

                notifications
            }
            SnapshotResponse::SnapshotCollection(snapshot) => {
                //  - Collection snapshot -> Old Object updated, New Object snapshotted
                vec![
                    (
                        &resource,
                        resource.object.fetch_object_hierarchies(&client).await?,
                        EventVariant::Updated,
                        DieselUlid::generate(), // block_id for deduplication
                    ),
                    (
                        &result[0],
                        snapshot
                            .collection
                            .fetch_object_hierarchies(&client)
                            .await?,
                        EventVariant::Snapshotted,
                        DieselUlid::generate(), // block_id for deduplication
                    ),
                ]
            }
            SnapshotResponse::SnapshotDataset(snapshot) => {
                //  - Dataset snapshot -> Old Object updated, New Object snapshotted
                vec![
                    (
                        &resource,
                        resource.object.fetch_object_hierarchies(&client).await?,
                        EventVariant::Updated,
                        DieselUlid::generate(), // block_id for deduplication
                    ),
                    (
                        &result[0],
                        snapshot.dataset.fetch_object_hierarchies(&client).await?,
                        EventVariant::Snapshotted,
                        DieselUlid::generate(), // block_id for deduplication
                    ),
                ]
            }
        };

        for (object_plus, hierarchies, event_variant, block_id) in emissions {
            if let Err(err) = self
                .natsio_handler
                .register_resource_event(object_plus, hierarchies, event_variant, Some(&block_id))
                .await
            {
                // Log error, rollback transaction and return
                log::error!("{err}");
                //transaction.rollback().await?;
                return Err(anyhow::anyhow!("Notification emission failed"));
            }
        }

        Ok((new_object_id, result))
    }
}
