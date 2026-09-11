use super::*;

impl DocumentSyncService {
    pub(in crate::document_sync) async fn apply_document_event(
        &self,
        event: DocumentSyncEvent,
    ) -> Result<()> {
        match event {
            DocumentSyncEvent::Upsert {
                target,
                bytes,
                change,
                ..
            } => self.apply_upsert(target, bytes, change).await,
            DocumentSyncEvent::Delete { target, change, .. } => {
                self.apply_delete(target, change).await
            }
            DocumentSyncEvent::AdminOperation { target, event, .. } => {
                apply_admin_document_operation_to_storage(&self.storage, target, *event).await
            }
        }
    }

    pub(in crate::document_sync) async fn apply_upsert(
        &self,
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
        change: DocumentSyncChange,
    ) -> Result<()> {
        if admin_document_target_for_reduced_document(&target).is_some() {
            return Err(NetError::Bootstrap(
                "whole-document admin sync is unsupported; admin documents must sync as operations"
                    .to_string(),
            ));
        }
        if let DocumentSyncTarget::MetadataCreateEvent {
            document_id,
            event_id,
        } = target
        {
            let record: MetadataCreateEventRecord = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if record.record.document_id != document_id || record.event_id != event_id {
                return Err(NetError::Bootstrap(format!(
                    "replicated metadata create-event target {document_id}/{event_id} does not match payload {}/{}",
                    record.record.document_id, record.event_id
                )));
            }
            validate_metadata_event(&record)?;
            return apply_create_event(
                &self.storage,
                &record,
                DocumentSyncTarget::MetadataCreateEvent {
                    document_id,
                    event_id,
                },
                bytes,
            )
            .await;
        }
        if let DocumentSyncTarget::MetadataDocumentLifecycle { document_id } = target {
            let record: MetadataDocumentLifecycleRecord = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if record.document_id() != document_id {
                return Err(NetError::Bootstrap(format!(
                    "replicated metadata document lifecycle target {document_id} does not match payload document {}",
                    record.document_id()
                )));
            }
            return self
                .apply_metadata_document_lifecycle(record, change)
                .await
                .map(|_| ());
        }
        if let DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        } = target
        {
            let record: MetadataRegistryRecord = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if record.group_id != group_id || record.document_id != document_id {
                return Err(NetError::Bootstrap(format!(
                    "replicated metadata registry target {group_id}/{document_id} does not match payload {}/{}",
                    record.group_id, record.document_id
                )));
            }
            self.apply_metadata_registry_upsert(record, bytes).await?;
            return Ok(());
        }
        if let DocumentSyncTarget::PersistentIdMapping { document_id } = target {
            let mapping = PersistentIdMapping::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            validate_pid_mapping(document_id, &mapping, &change).map_err(NetError::Bootstrap)?;
            // The generic write below would clobber a local tombstone with a
            // replayed Active row, so the mapping always goes through its merge.
            return match self.apply_pid_mapping(&mapping, change.placement).await? {
                MetadataPlacementOutcome::Accepted(_) => Ok(()),
                MetadataPlacementOutcome::Deferred(_) => Err(NetError::Dht(
                    "persistent id mapping placement configuration is unavailable".to_string(),
                )),
                MetadataPlacementOutcome::Rejected => Err(NetError::Bootstrap(
                    "persistent id mapping has a mismatched placement configuration".to_string(),
                )),
            };
        }
        if let DocumentSyncTarget::PlacementPolicy { policy_id } = target {
            let document: PlacementPolicyDocument = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            validate_policy_document(policy_id, self.realm_id, &document, &change)
                .map_err(NetError::Bootstrap)?;
            // The generic write below would replace a stored rule with different
            // bytes under the same id, so a policy always goes through its merge.
            return match self
                .apply_policy_document(&document, change.placement)
                .await?
            {
                MetadataPlacementOutcome::Accepted(_) => Ok(()),
                // The live loop handles this target itself and registers the
                // dependency; here the error aborts the batch, so the topic
                // cursor stays put and the event is redelivered.
                MetadataPlacementOutcome::Deferred(dependency) => {
                    warn!(
                        %policy_id,
                        ?dependency,
                        "Deferring a placement policy document until its dependency replicates"
                    );
                    Err(NetError::Deferred(format!(
                        "placement policy {policy_id} awaits its authority evidence"
                    )))
                }
                MetadataPlacementOutcome::Rejected => Err(NetError::Bootstrap(
                    "placement policy has a mismatched placement or reuses its id".to_string(),
                )),
            };
        }
        if let DocumentSyncTarget::MetadataGraphLifecycle { graph_iri } = target {
            let record: MetadataGraphLifecycleRecord = postcard::from_bytes(&bytes)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            if record.graph_iri != graph_iri {
                return Err(NetError::Bootstrap(format!(
                    "replicated metadata graph lifecycle target `{graph_iri}` does not match payload graph `{}`",
                    record.graph_iri
                )));
            }
            return self
                .apply_metadata_graph_lifecycle(record, bytes)
                .await
                .map(|_| ());
        }
        if let DocumentSyncTarget::NodeUsage { .. } = target {
            // Structural guard for the shared node-usage keyspace. The reconcile
            // loop already validated the signed publisher and this payload, but
            // re-check the snapshot's self-consistency here so the generic
            // storage write below can never persist an unvalidated snapshot.
            validate_node_usage_upsert(&target, &bytes).map_err(NetError::Bootstrap)?;
        }
        if let DocumentSyncTarget::WatchInterest { .. } = target {
            // Structural guard for the shared watch-interest keyspace. The
            // reconcile loop already validated the signed publisher and this
            // payload, but re-check the digest's self-consistency here so the
            // generic storage write below can never persist an unvalidated digest.
            validate_watch_interest(&target, &bytes).map_err(NetError::Bootstrap)?;
        }
        if let DocumentSyncTarget::NodeInfo { .. } = target {
            // Structural guard for the shared node-info keyspace, mirroring the
            // node-usage guard above so the generic storage write can never
            // persist an unvalidated node info document.
            let incoming =
                validate_node_info_upsert(&target, &bytes).map_err(NetError::Bootstrap)?;
            // Read-then-write without a transaction: only safe because the
            // reconcile loop applies one event at a time per topic.
            let stored = self
                .storage_read(target.storage_keyspace().to_string(), target.storage_key())
                .await?;
            if !node_info_supersedes(&incoming, stored.as_ref().map(|value| value.as_ref())) {
                return Ok(());
            }
        }
        self.storage_write(
            target.storage_keyspace().to_string(),
            target.storage_key(),
            bytes.into(),
        )
        .await
    }
}
