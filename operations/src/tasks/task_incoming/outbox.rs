use super::*;

impl DrainInvocation {
    pub(super) fn new(rotation: &OutboxRotation) -> Self {
        Self {
            outcome: DrainSyncOutcome::default(),
            defer: DrainDeferState {
                deferred_topics: rotation.blocked_topics.clone(),
                blocked_origins: rotation.blocked_origins.clone(),
                undeliverable_topics: rotation.undeliverable_topics.clone(),
                ..DrainDeferState::default()
            },
            cursor: rotation.cursor.clone(),
            reached_end: false,
            scan_elapsed: Duration::ZERO,
            publish_elapsed: Duration::ZERO,
            records: 0,
            deferred: 0,
            stuck: 0,
            oldest_stuck: None,
            undeliverable: 0,
            groups: 0,
            subbatches: 0,
            pages: 0,
            oldest_record_ms: None,
            read_failed: false,
            config_drained: false,
        }
    }

    pub(super) fn has_unvisited(&self) -> bool {
        !self.reached_end && !self.read_failed
    }
}

impl DrainSyncOutcome {
    pub(super) fn merge(&mut self, other: DrainSyncOutcome) {
        self.sync_elapsed += other.sync_elapsed;
        self.project_elapsed += other.project_elapsed;
        self.delete_elapsed += other.delete_elapsed;
        self.retry_needed |= other.retry_needed;
        self.deleted += other.deleted;
        self.blocked_topics.extend(other.blocked_topics);
        self.blocked_origins.extend(other.blocked_origins);
    }
}

pub(super) fn document_publish_from_outbox(
    event_id: ulid::Ulid,
    target: DocumentSyncTarget,
    event: DocumentSyncOutboxEvent,
    placement: aruna_core::structs::PlacementRef,
    allow_genesis: bool,
) -> DocumentSyncPublish {
    match event {
        DocumentSyncOutboxEvent::Upsert { bytes, change } => DocumentSyncPublish::Upsert {
            event_id,
            target,
            bytes,
            change,
            allow_genesis,
        },
        DocumentSyncOutboxEvent::Delete { change } => DocumentSyncPublish::Delete {
            event_id,
            target,
            change,
            allow_genesis,
        },
        DocumentSyncOutboxEvent::AdminOperation {
            event,
            origin_signature,
        } => DocumentSyncPublish::AdminOperation {
            target,
            event,
            placement,
            allow_genesis,
            origin_signature,
        },
    }
}

pub(super) async fn load_realm_config_for_drain(
    context: &Arc<DriverContext>,
    realm_id: aruna_core::structs::RealmId,
) -> Option<aruna_core::structs::RealmConfigDocument> {
    let target = DocumentSyncTarget::RealmConfig { realm_id };
    match context
        .storage_handle
        .send_storage_effect(aruna_core::effects::StorageEffect::Read {
            key_space: target.storage_keyspace().to_string(),
            key: target.storage_key(),
            txn_id: None,
        })
        .await
    {
        Event::Storage(StorageEvent::ReadResult { value, .. }) => value
            .and_then(|bytes| aruna_core::structs::RealmConfigDocument::from_bytes(&bytes).ok()),
        _ => None,
    }
}

/// Resolves the shard placement a drained record publishes under: a real ref is
/// kept, a NIL ref (from admin-operation emitters) is resolved from the realm
/// config. Shared realm targets ignore placement, so resolving is harmless.
pub(super) fn resolve_publish_placement(
    config: Option<&aruna_core::structs::RealmConfigDocument>,
    target: &DocumentSyncTarget,
    current: aruna_core::structs::PlacementRef,
) -> aruna_core::structs::PlacementRef {
    if current != aruna_core::structs::PlacementRef::NIL {
        return current;
    }
    match config {
        Some(config) => {
            crate::placement::placement_ref_for_target(config, target, Default::default())
        }
        None => aruna_core::structs::PlacementRef::NIL,
    }
}

impl DrainSubBatch {
    pub(super) fn sync_subset(&self, indices: &[usize]) -> Option<Self> {
        let mut topics = Vec::with_capacity(indices.len());
        let mut origins = Vec::with_capacity(indices.len());
        let mut targets = Vec::with_capacity(indices.len());
        let mut record_keys = Vec::with_capacity(indices.len());
        for &index in indices {
            topics.push(*self.topics.get(index)?);
            origins.push(*self.origins.get(index)?);
            targets.push(self.targets.get(index)?.clone());
            record_keys.push(self.record_keys.get(index)?.clone());
        }
        Some(Self {
            peers: self.peers.clone(),
            documents: Vec::new(),
            topics,
            origins,
            targets,
            record_keys,
        })
    }

    /// Ordering domains blocked together when a publish or sync leaves records
    /// behind.
    pub(super) fn ordering_domains(&self) -> (Vec<irokle::TopicId>, Vec<aruna_core::NodeId>) {
        (
            self.topics.clone(),
            self.origins.iter().flatten().copied().collect(),
        )
    }
}

impl OutboxRotation {
    pub(super) fn admits(&self, key: &[u8]) -> bool {
        if !self.stream_boundaries.is_empty() {
            return self
                .stream_boundaries
                .iter()
                .find(|(prefix, _)| key.starts_with(prefix))
                .is_some_and(|(_, boundary)| key <= boundary.as_slice());
        }
        self.boundary
            .as_deref()
            .is_none_or(|boundary| key <= boundary)
    }

    pub(super) fn at_end(&self, cursor: Option<&[u8]>) -> bool {
        cursor.is_some_and(|cursor| {
            self.boundary
                .as_deref()
                .is_some_and(|boundary| cursor >= boundary)
        })
    }

    pub(super) fn close(&mut self) -> RotationTotals {
        let totals = self.totals;
        *self = Self::default();
        totals
    }
}

#[cfg(debug_assertions)]
impl OutboxBarrier {
    pub(super) fn new() -> Option<Self> {
        let marker = std::env::var("ARUNA_TEST_OUTBOX_BARRIER")
            .ok()
            .map(std::path::PathBuf::from)?;
        let barrier = Self { marker };
        if let Err(error) = std::fs::write(&barrier.marker, b"active") {
            warn!(error = %error, "Failed to arm outbox test barrier");
            return None;
        }
        Some(barrier)
    }

    pub(super) async fn wait_start(&self) {
        std::future::pending::<()>().await;
    }
}

/// Whether a record whose shard topic is missing locally can ever publish from here.
/// Holdership comes from the live realm config, never a local copy (rebalances leave
/// stale copies). Without a readable config nothing is decided and the record retries.
pub(super) fn classify_deferred_record(
    config: Option<&aruna_core::structs::RealmConfigDocument>,
    net_handle: &aruna_net::NetHandle,
    record: &DocumentSyncOutboxRecord,
) -> DeferOutcome {
    let Some(config) = config else {
        return DeferOutcome::Retry;
    };
    let node_id = net_handle.node_id();
    // A draining former-holder keeps publish rights until flushed (flush-then-leave),
    // so its retained records are publishable; a true non-holder stays undeliverable
    // (DECISIONS K3), as the receiver's history cutoff bounds a departing holder.
    if crate::placement::holds_placement(config, &record.placement, node_id)
        || crate::placement::is_draining_former_holder(config, &record.placement, node_id)
        || crate::placement::retained_departing_holder(config, &record.placement, node_id)
    {
        DeferOutcome::Retry
    } else {
        DeferOutcome::Undeliverable
    }
}

/// Splits FIFO-ordered drain records into publish-now, deferred (no local shard genesis
/// yet) and undeliverable. Holdership decides publishability, not local topic presence,
/// and a topic never straddles the defer/publish boundary; `publishes_shared` gates devices.
pub(super) fn partition_drain_records(
    records: Vec<DrainRecord>,
    defer: &mut DrainDeferState,
    publishes_shared: bool,
    mut topic_available: impl FnMut(irokle::TopicId) -> bool,
    mut classify_defer: impl FnMut(&DocumentSyncOutboxRecord) -> DeferOutcome,
) -> (Vec<DrainRecord>, Vec<DrainRecord>, Vec<DrainRecord>) {
    let mut to_publish = Vec::with_capacity(records.len());
    let mut deferred = Vec::new();
    let mut undeliverable = Vec::new();
    for (record_key, record, topic) in records {
        // Origin order is the contract, whatever topic the later records ride.
        if admin_origin(&record).is_some_and(|origin| defer.blocked_origins.contains(&origin)) {
            deferred.push((record_key, record, topic));
            continue;
        }
        if !record.target.uses_shard_topic() {
            match publishes_shared {
                true => to_publish.push((record_key, record, topic)),
                false => undeliverable.push((record_key, record, topic)),
            }
            continue;
        }
        if defer.undeliverable_topics.contains(&topic) {
            if let Some(origin) = admin_origin(&record) {
                defer.blocked_origins.insert(origin);
            }
            undeliverable.push((record_key, record, topic));
            continue;
        }
        let held = *defer
            .topic_held
            .entry(topic)
            .or_insert_with(|| classify_defer(&record) == DeferOutcome::Retry);
        if !held {
            defer.undeliverable_topics.insert(topic);
            if let Some(origin) = admin_origin(&record) {
                defer.blocked_origins.insert(origin);
            }
            undeliverable.push((record_key, record, topic));
            continue;
        }
        let already_deferred = defer.deferred_topics.contains(&topic);
        let available = !already_deferred
            && *defer
                .topic_exists
                .entry(topic)
                .or_insert_with(|| topic_available(topic));
        if available {
            to_publish.push((record_key, record, topic));
            continue;
        }
        defer.deferred_topics.insert(topic);
        if let Some(origin) = admin_origin(&record) {
            defer.blocked_origins.insert(origin);
        }
        debug!(
            event = "pipeline.drain.deferred",
            target = ?record.target,
            %topic,
            "Deferring outbox record: shard topic genesis not yet known"
        );
        deferred.push((record_key, record, topic));
    }
    (to_publish, deferred, undeliverable)
}

/// The admin origin stream a record belongs to. Origin sequence is ordered
/// within one origin node.
pub(super) fn admin_origin(record: &DocumentSyncOutboxRecord) -> Option<aruna_core::NodeId> {
    match &record.event {
        DocumentSyncOutboxEvent::AdminOperation { event, .. } => Some(event.origin_node_id),
        _ => None,
    }
}
