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
                apply_admin_operation(&self.storage, target, *event).await
            }
        }
    }

    pub(in crate::document_sync) async fn apply_upsert(
        &self,
        target: DocumentSyncTarget,
        bytes: Vec<u8>,
        change: DocumentSyncChange,
    ) -> Result<()> {
        if reduced_admin_target(&target).is_some() {
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
                .apply_document_lifecycle(record, change)
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
            self.apply_registry_upsert(record, bytes).await?;
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
                // The live loop handles this target and registers the dependency;
                // aborting here keeps the cursor and redelivers the event.
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
            return self.apply_graph_lifecycle(record, bytes).await.map(|_| ());
        }
        if let DocumentSyncTarget::NodeUsage { .. } = target {
            // Structural guard for the shared node-usage keyspace: re-check the
            // snapshot's self-consistency before the generic storage write.
            validate_usage_upsert(&target, &bytes).map_err(NetError::Bootstrap)?;
        }
        if let DocumentSyncTarget::WatchInterest { .. } = target {
            // Structural guard for the shared watch-interest keyspace: re-check
            // the digest's self-consistency before the generic storage write.
            validate_watch_interest(&target, &bytes).map_err(NetError::Bootstrap)?;
        }
        if let DocumentSyncTarget::NodeInfo { .. } = target {
            // Structural guard for the shared node-info keyspace, mirroring the
            // node-usage guard above.
            let incoming = validate_node_upsert(&target, &bytes).map_err(NetError::Bootstrap)?;
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

impl DocumentSyncService {
    pub(in crate::document_sync) async fn apply_watch_change(
        &self,
        target: DocumentSyncTarget,
        bytes: Option<Vec<u8>>,
        change: DocumentSyncChange,
    ) -> Result<bool> {
        store_watch_change(&self.storage, target, bytes, change).await
    }

    pub(in crate::document_sync) async fn apply_registry_upsert(
        &self,
        record: MetadataRegistryRecord,
        primary_bytes: Vec<u8>,
    ) -> Result<MetadataPlacementOutcome<()>> {
        store_registry_upsert(&self.storage, record, primary_bytes).await
    }

    pub(in crate::document_sync) async fn apply_document_lifecycle(
        &self,
        record: MetadataDocumentLifecycleRecord,
        change: DocumentSyncChange,
    ) -> Result<bool> {
        store_document_lifecycle(&self.storage, &record, change).await
    }

    pub(in crate::document_sync) async fn apply_graph_lifecycle(
        &self,
        record: MetadataGraphLifecycleRecord,
        primary_bytes: Vec<u8>,
    ) -> Result<bool> {
        store_graph_lifecycle(&self.storage, &record, primary_bytes).await
    }

    pub(in crate::document_sync) async fn apply_pid_mapping(
        &self,
        mapping: &PersistentIdMapping,
        placement: PlacementRef,
    ) -> Result<MetadataPlacementOutcome<bool>> {
        store_pid_mapping(&self.storage, self.realm_id, mapping, placement).await
    }

    /// A publication counts only against this realm's current view: the original
    /// publisher must be permitted and its authorizing user must still hold
    /// write on the named path; a relay never supplies that authority.
    pub(in crate::document_sync) async fn apply_policy_document(
        &self,
        document: &PlacementPolicyDocument,
        placement: PlacementRef,
    ) -> Result<MetadataPlacementOutcome<bool>> {
        let Some(config) = read_realm_config(&self.storage, self.realm_id).await? else {
            return Ok(MetadataPlacementOutcome::Deferred(
                DocumentSyncDependency::RealmConfig(self.realm_id),
            ));
        };
        let Some(auth) = read_realm_authorization(&self.storage, self.realm_id).await? else {
            return Ok(MetadataPlacementOutcome::Deferred(
                DocumentSyncDependency::RealmAuthorization(self.realm_id),
            ));
        };
        // A group-owned rule is decided against its owner's roles; without them
        // nothing is accepted, so the document waits instead of being trusted.
        let group_auth = match document.policy.owner_group_id {
            Some(group_id) => match read_group_authorization(&self.storage, group_id).await? {
                Some(group_auth) => Some(group_auth),
                None => {
                    return Ok(MetadataPlacementOutcome::Deferred(
                        DocumentSyncDependency::GroupAuthorization(group_id),
                    ));
                }
            },
            None => None,
        };
        if let Err(reason) = verify_policy_authority(document, &config, &auth, group_auth.as_ref())
        {
            warn!(
                policy_id = %document.policy_id(),
                %reason,
                "Rejecting placement policy without publication authority"
            );
            return Ok(MetadataPlacementOutcome::Rejected);
        }
        store_policy_document(&self.storage, self.realm_id, document, placement).await
    }

    pub(in crate::document_sync) async fn apply_delete(
        &self,
        target: DocumentSyncTarget,
        change: DocumentSyncChange,
    ) -> Result<()> {
        if change.kind != DocumentSyncChangeKind::Delete {
            return Err(NetError::Bootstrap(
                "document sync delete must carry a delete change".to_string(),
            ));
        }
        if let DocumentSyncTarget::MetadataGraphLifecycle { .. } = target {
            return Ok(());
        }
        if let DocumentSyncTarget::MetadataDocumentLifecycle { .. } = target {
            return Ok(());
        }
        // A minted PID is a permanent identity: the row is never removed, only
        // flipped to Withdrawn, so a delete for it is a no-op rather than an error.
        if let DocumentSyncTarget::PersistentIdMapping { .. } = target {
            return Ok(());
        }
        if let DocumentSyncTarget::MetadataRegistry {
            group_id,
            document_id,
        } = target
        {
            return delete_registry_record(&self.storage, group_id, document_id).await;
        }
        if reduced_admin_target(&target).is_some() {
            return Err(NetError::Bootstrap(
                "whole-document admin sync is unsupported; admin documents must sync as operations"
                    .to_string(),
            ));
        }
        Err(NetError::Bootstrap(
            "document sync delete target is unsupported".to_string(),
        ))
    }
}

impl DocumentSyncService {
    /// Evidence rows for permanently rejected operations, chained through the
    /// usage total and deduplicated by transport identity within the batch.
    /// `None` means the store is full: leave cursors unwritten to redeliver.
    pub(in crate::document_sync) async fn quarantine_entries(
        &self,
        rejections: &[SyncRejection],
        txn_id: TxnId,
    ) -> Result<Option<Vec<(String, ByteView, Value)>>> {
        if rejections.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut usage = match transaction_read(
            &self.storage,
            SYNC_QUARANTINE_USAGE_KEYSPACE.to_string(),
            ByteView::from(SYNC_QUARANTINE_USAGE_KEY),
            Some(txn_id),
        )
        .await?
        {
            Some(value) => SyncQuarantineUsage::from_bytes(value.as_ref())
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
            None => SyncQuarantineUsage::default(),
        };
        let quarantined_at_ms = unix_timestamp_millis();
        let mut pending: BTreeMap<Vec<u8>, (String, ByteView, Value)> = BTreeMap::new();
        for rejection in rejections {
            let key = rejection.identity.storage_key();
            // A redelivered operation swaps its pending bytes instead of adding
            // to the usage total; an earlier row in this batch swaps the same.
            let replaced_bytes = match pending.get(&key) {
                Some((_, _, value)) => Some(value.len() as u64),
                None => transaction_read(
                    &self.storage,
                    SYNC_QUARANTINE_KEYSPACE.to_string(),
                    ByteView::from(key.clone()),
                    Some(txn_id),
                )
                .await?
                .map(|value| value.len() as u64),
            };
            match build_quarantine_entries(
                SyncQuarantineInput {
                    identity: rejection.identity,
                    evidence: rejection.evidence.clone(),
                    reason: &rejection.reason,
                    quarantined_at_ms,
                    replaced_bytes,
                },
                usage,
                SyncQuarantineCapacity::default(),
            ) {
                Ok(write) => {
                    usage = write.usage;
                    pending.insert(key, write.row);
                }
                Err(SyncQuarantineError::AtCapacity { .. }) => {
                    warn!(
                        topic_id = %rejection.identity.topic,
                        actor_id = %rejection.identity.actor,
                        actor_seq = rejection.identity.actor_seq,
                        records = usage.records,
                        bytes = usage.bytes,
                        "Holding the sync cursor: the quarantine store is at capacity"
                    );
                    return Ok(None);
                }
                Err(error) => return Err(NetError::Bootstrap(error.to_string())),
            }
        }
        let mut entries = pending.into_values().collect::<Vec<_>>();
        entries.push(
            quarantine_usage_entry(usage)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?,
        );
        Ok(Some(entries))
    }

    /// Commit rejection evidence and the topic cursor in one transaction so the
    /// cursor can never move past evidence that is not durable. `false` means
    /// the store is at capacity and nothing was written.
    pub(in crate::document_sync) async fn commit_cursor_evidence(
        &self,
        rejections: &[SyncRejection],
        cursor_write: (String, ByteView, Value),
    ) -> Result<bool> {
        let txn_id = start_storage_transaction(&self.storage).await?;
        let mut writes = match self.quarantine_entries(rejections, txn_id).await {
            Ok(Some(entries)) => entries,
            Ok(None) => {
                self.abort_transaction(txn_id).await;
                return Ok(false);
            }
            Err(error) => {
                self.abort_transaction(txn_id).await;
                return Err(error);
            }
        };
        writes.push(cursor_write);
        if let Err(error) = replace_batch_in(&self.storage, txn_id, Vec::new(), writes).await {
            self.abort_transaction(txn_id).await;
            return Err(error);
        }
        Ok(true)
    }

    pub(in crate::document_sync) async fn abort_transaction(&self, txn_id: TxnId) {
        let _ = self
            .storage
            .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
            .await;
    }
}

pub(in crate::document_sync) async fn store_watch_change(
    storage: &StorageHandle,
    target: DocumentSyncTarget,
    bytes: Option<Vec<u8>>,
    change: DocumentSyncChange,
) -> Result<bool> {
    for _ in 0..2 {
        let txn_id = match storage
            .send_storage_effect(StorageEffect::StartTransaction { read: false })
            .await
        {
            Event::Storage(StorageEvent::TransactionStarted { txn_id }) => txn_id,
            Event::Storage(StorageEvent::Error { error }) => {
                return Err(error.into());
            }
            other => {
                return Err(NetError::Dht(format!(
                    "unexpected transaction start while applying watch subscription: {other:?}"
                )));
            }
        };

        let current = match storage
            .send_storage_effect(StorageEffect::Read {
                key_space: DOCUMENT_SYNC_REVISION_KEYSPACE.to_string(),
                key: sync_revision_key(&target),
                txn_id: Some(txn_id),
            })
            .await
        {
            Event::Storage(StorageEvent::ReadResult { value, .. }) => match value
                .map(|value| postcard::from_bytes::<DocumentSyncChange>(value.as_ref()))
                .transpose()
            {
                Ok(current) => current,
                Err(error) => {
                    let _ = storage
                        .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                        .await;
                    return Err(NetError::Bootstrap(error.to_string()));
                }
            },
            Event::Storage(StorageEvent::Error { error }) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error.into());
            }
            other => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(NetError::Dht(format!(
                    "unexpected revision read while applying watch subscription: {other:?}"
                )));
            }
        };

        // Watch ids are immutable and never reused. The first valid upsert wins,
        // and any delete permanently fences delayed/replayed creates.
        let apply = match (current.as_ref().map(|local| local.kind), change.kind) {
            (None, _) => true,
            (Some(DocumentSyncChangeKind::Upsert), DocumentSyncChangeKind::Delete) => true,
            (Some(DocumentSyncChangeKind::Upsert), DocumentSyncChangeKind::Upsert)
            | (Some(DocumentSyncChangeKind::Delete), _) => false,
        };
        if !apply {
            let _ = storage
                .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                .await;
            return Ok(false);
        }

        let realm_id = match &target {
            DocumentSyncTarget::WatchSubscription { owner, .. } => owner.realm_id,
            _ => unreachable!("watch subscription apply requires a subscription target"),
        };
        let revision_entry = match sync_revision_entry(&target, &change) {
            Ok(entry) => entry,
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(NetError::Bootstrap(error.to_string()));
            }
        };
        let mut writes = vec![
            revision_entry,
            (
                NOTIFICATION_WATCH_INTEREST_KEYSPACE.to_string(),
                ByteView::from(interest_dirty_key(realm_id)),
                ByteView::from(Ulid::generate().to_bytes().to_vec()),
            ),
        ];
        let deletes = if let Some(bytes) = bytes.as_ref() {
            writes.push((
                target.storage_keyspace().to_string(),
                target.storage_key(),
                ByteView::from(bytes.clone()),
            ));
            Vec::new()
        } else {
            vec![(target.storage_keyspace().to_string(), target.storage_key())]
        };

        match replace_batch_in(storage, txn_id, deletes, writes).await {
            Ok(()) => return Ok(true),
            Err(NetError::Storage(StorageError::TransactionConflict)) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
            }
            Err(error) => {
                let _ = storage
                    .send_storage_effect(StorageEffect::AbortTransaction { txn_id })
                    .await;
                return Err(error);
            }
        }
    }
    Err(NetError::Dht(
        "watch subscription apply conflicted twice".to_string(),
    ))
}
