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

pub(super) fn publish_from_outbox(
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

pub(super) async fn load_drain_config(
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
        Some(config) => crate::placement::target_placement_ref(config, target, Default::default()),
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
    // Draining former holders may publish until flushed; true non-holders remain undeliverable.
    // The receiver history cutoff bounds a departing holder.
    if crate::placement::holds_placement(config, &record.placement, node_id)
        || crate::placement::is_draining_holder(config, &record.placement, node_id)
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

impl OperationsTaskHandler {
    async fn read_drain_page(
        &self,
        retry_key: &TaskKey,
        rotation: &OutboxRotation,
        invocation: &mut DrainInvocation,
    ) -> DrainPage {
        let page_limit = OUTBOX_DRAIN_BATCH_SIZE.min(
            self.outbox_limits
                .records
                .saturating_sub(invocation.records),
        );
        let scan_started = Instant::now();
        let mut batch = match read_outbox_records(
            &self.context.storage_handle,
            &[],
            invocation.cursor.clone(),
            page_limit,
        )
        .await
        {
            Ok(batch) => batch,
            Err(error) => {
                warn!(task_id = ?retry_key, error = %error, "Failed to read document sync outbox record");
                invocation.read_failed = true;
                return DrainPage::Stop;
            }
        };
        invocation.scan_elapsed += scan_started.elapsed();
        let has_more = batch.has_more;
        invocation.cursor = batch.next_start_after;
        let boundary_reached = rotation.at_end(invocation.cursor.as_deref());
        batch.records.retain(|(key, _)| rotation.admits(key));
        if !batch.records.is_empty() {
            return DrainPage::Records {
                records: batch.records,
                has_more,
                boundary_reached,
            };
        }
        if boundary_reached || !has_more {
            invocation.reached_end = true;
            DrainPage::Stop
        } else if invocation.cursor.is_some() {
            DrainPage::Skip
        } else {
            invocation.reached_end = true;
            DrainPage::Stop
        }
    }
}

impl OperationsTaskHandler {
    async fn run_drain(&self) {
        let retry_key = TaskKey::DrainDocumentSyncOutbox;
        let drain_started = Instant::now();

        let Some(net_handle) = self.context.net_handle.as_ref() else {
            warn!(task_id = ?retry_key, "Cannot drain document sync outbox without net handle");
            self.reschedule_with_backoff(retry_key).await;
            return;
        };

        let realm_id = *net_handle.realm_id();
        let realm_config = load_drain_config(&self.context, realm_id).await;

        let rotation = self.take_rotation();
        let Some(rotation) = self.open_rotation(&retry_key, rotation).await else {
            return;
        };
        let mut invocation = DrainInvocation::new(&rotation);
        loop {
            if invocation.records >= self.outbox_limits.records
                || invocation.pages >= self.outbox_limits.pages
            {
                break;
            }
            match self
                .read_drain_page(&retry_key, &rotation, &mut invocation)
                .await
            {
                DrainPage::Records {
                    records,
                    has_more,
                    boundary_reached,
                } => {
                    self.process_drain_page(
                        &retry_key,
                        net_handle,
                        realm_config.as_ref(),
                        realm_id,
                        records,
                        &mut invocation,
                    )
                    .await;
                    if boundary_reached || !has_more {
                        invocation.reached_end = true;
                        break;
                    }
                }
                DrainPage::Skip => continue,
                DrainPage::Stop => break,
            }
        }

        self.finish_drain_invocation(
            retry_key,
            net_handle,
            realm_id,
            rotation,
            invocation,
            drain_started,
        )
        .await;
    }
}

impl OperationsTaskHandler {
    /// Runs one bounded invocation of the open rotation, then continues, yields
    /// through the timer, or closes it. No record is ever deleted, truncated, or
    /// overwritten to satisfy a bound.
    pub(super) async fn drain_sync_outbox(&self) {
        let _drain = self.drain_guard.lock().await;
        #[cfg(debug_assertions)]
        if let Some(barrier) = OutboxBarrier::new() {
            barrier.wait_start().await;
        }

        self.run_drain().await;
    }
}

impl OperationsTaskHandler {
    pub(super) async fn close_rotation(&self, retry_key: TaskKey, mut rotation: OutboxRotation) {
        let closed = rotation.close();
        self.store_rotation(rotation);
        if closed.examined > 0 {
            info!(
                event = "pipeline.drain.rotation",
                examined = closed.examined,
                deleted = closed.deleted,
                deferred = closed.deferred,
                undeliverable = closed.undeliverable,
                retry_invocations = closed.retry_invocations,
                invocations = closed.invocations,
                "Document sync outbox rotation complete"
            );
        }

        if closed.retry_invocations > 0 {
            if closed.deleted > 0 {
                self.reset_backoff(&retry_key);
            }
            self.reschedule_with_backoff(retry_key).await;
        } else if closed.deferred > 0 {
            if closed.deleted > 0 {
                self.reset_backoff(&retry_key);
            }
            self.reschedule_timer(retry_key, DOCUMENT_SYNC_DEFER_RETRY_AFTER)
                .await;
        } else {
            self.reset_backoff(&retry_key);
        }
    }
}

impl OperationsTaskHandler {
    pub(super) async fn open_rotation(
        &self,
        retry_key: &TaskKey,
        mut rotation: OutboxRotation,
    ) -> Option<OutboxRotation> {
        if rotation.boundary.is_none() {
            match read_outbox_tails(&self.context.storage_handle).await {
                Ok(boundaries) if boundaries.is_empty() => {
                    self.reset_backoff(retry_key);
                    return None;
                }
                Ok(boundaries) => {
                    rotation.boundary = boundaries.iter().map(|(_, key)| key).max().cloned();
                    rotation.stream_boundaries = boundaries;
                }
                Err(error) => {
                    warn!(task_id = ?retry_key, %error, "Failed to open document sync outbox rotation");
                    self.store_rotation(rotation);
                    self.reschedule_with_backoff(retry_key.clone()).await;
                    return None;
                }
            }
        }
        debug_assert!(rotation.boundary.is_some());
        Some(rotation)
    }
}

impl OperationsTaskHandler {
    pub(super) fn store_rotation(&self, rotation: OutboxRotation) {
        *self
            .rotation
            .lock()
            .expect("outbox rotation mutex poisoned") = rotation;
    }
}

impl OperationsTaskHandler {
    /// Takes the open rotation, leaving a fresh one for a concurrent
    /// invocation.
    fn take_rotation(&self) -> OutboxRotation {
        std::mem::take(
            &mut *self
                .rotation
                .lock()
                .expect("outbox rotation mutex poisoned"),
        )
    }
}

impl OperationsTaskHandler {
    async fn process_drain_page(
        &self,
        retry_key: &TaskKey,
        net_handle: &aruna_net::NetHandle,
        config: Option<&aruna_core::structs::RealmConfigDocument>,
        realm_id: RealmId,
        records: Vec<(Vec<u8>, DocumentSyncOutboxRecord)>,
        invocation: &mut DrainInvocation,
    ) {
        invocation.pages += 1;
        invocation.records += records.len();
        if let Some(page_oldest) = records
            .iter()
            .map(|(_, record)| record.outbox_id.timestamp_ms())
            .min()
        {
            invocation.oldest_record_ms = Some(
                invocation
                    .oldest_record_ms
                    .map_or(page_oldest, |current| current.min(page_oldest)),
            );
        }
        let records = self
            .prepare_drain_records(retry_key, net_handle, config, realm_id, records, invocation)
            .await;
        let (to_publish, deferred, undeliverable) = partition_drain_records(
            records,
            &mut invocation.defer,
            !self.is_device(config),
            |topic| net_handle.sync_topic_exists(topic).unwrap_or(false),
            |record| classify_deferred_record(config, net_handle, record),
        );
        invocation.deferred += deferred.len();
        let now_ms = unix_timestamp_millis();
        for (_, record, topic) in &deferred {
            let record_ms = record.outbox_id.timestamp_ms();
            if now_ms.saturating_sub(record_ms) < OUTBOX_STUCK_AFTER.as_millis() as u64 {
                continue;
            }
            invocation.stuck += 1;
            if invocation
                .oldest_stuck
                .as_ref()
                .is_none_or(|(oldest_ms, ..)| record_ms < *oldest_ms)
            {
                invocation.oldest_stuck =
                    Some((record_ms, record.target.clone(), *topic, record.placement));
            }
        }
        let relayed = self
            .relay_undeliverable_records(config, &undeliverable)
            .await;
        invocation.undeliverable += undeliverable.len().saturating_sub(relayed.len());
        if !relayed.is_empty()
            && let Err(error) = crate::sync::document_outbox::delete_outbox_records(
                &self.context.storage_handle,
                relayed,
            )
            .await
        {
            warn!(%error, "Failed to delete relayed admin outbox records");
        }

        let (groups, subbatches) = Self::build_drain_batches(to_publish);
        invocation.groups += groups;
        invocation.subbatches += subbatches.len();
        let (publish_elapsed, outcome) = self
            .publish_drain_batches(retry_key, net_handle, subbatches)
            .await;
        invocation.publish_elapsed += publish_elapsed;
        invocation.outcome.merge(outcome);
    }
}

impl OperationsTaskHandler {
    async fn prepare_drain_records(
        &self,
        retry_key: &TaskKey,
        net_handle: &aruna_net::NetHandle,
        config: Option<&aruna_core::structs::RealmConfigDocument>,
        realm_id: RealmId,
        records: Vec<(Vec<u8>, DocumentSyncOutboxRecord)>,
        invocation: &mut DrainInvocation,
    ) -> Vec<DrainRecord> {
        let mut records: Vec<DrainRecord> = records
            .into_iter()
            .map(|(record_key, mut record)| {
                invocation.config_drained |=
                    matches!(record.target, DocumentSyncTarget::RealmConfig { .. });
                record.placement =
                    resolve_publish_placement(config, &record.target, record.placement);
                let topic = record.target.sync_topic_id(realm_id, &record.placement);
                (record_key, record, topic)
            })
            .collect();

        // Pull a missing shard genesis from stamped ex-holders and live holders.
        // Only current or draining former holders may adopt it for publication.
        let mut missing_topics: BTreeMap<
            Vec<aruna_core::NodeId>,
            (Vec<aruna_core::NodeId>, BTreeSet<irokle::TopicId>),
        > = BTreeMap::new();
        for (_, record, topic) in &records {
            if !record.target.uses_shard_topic()
                || invocation.defer.deferred_topics.contains(topic)
                || !config.is_none_or(|config| {
                    crate::placement::holds_placement(
                        config,
                        &record.placement,
                        net_handle.node_id(),
                    ) || crate::placement::is_draining_holder(
                        config,
                        &record.placement,
                        net_handle.node_id(),
                    )
                })
                || net_handle.sync_topic_exists(*topic).unwrap_or(false)
            {
                continue;
            }
            let mut bootstrap_peers = record.peers.clone();
            if let Some(config) = config {
                for holder in crate::placement::resolve_shard_holders(config, &record.placement) {
                    if !bootstrap_peers.contains(&holder) {
                        bootstrap_peers.push(holder);
                    }
                }
            }
            bootstrap_peers.retain(|peer| *peer != net_handle.node_id());
            if bootstrap_peers.is_empty() {
                continue;
            }
            let mut peer_key = bootstrap_peers.clone();
            crate::sync::shard_placement::sort_node_ids(&mut peer_key);
            missing_topics
                .entry(peer_key)
                .or_insert_with(|| (bootstrap_peers, BTreeSet::new()))
                .1
                .insert(*topic);
        }
        for (_, (peers, topics)) in missing_topics {
            let event = net_handle
                .sync_document_topics(topics.into_iter().collect(), peers)
                .await;
            let outcome = self
                .finish_sync_batch(
                    retry_key,
                    Vec::new(),
                    Vec::new(),
                    Event::Net(NetEvent::DocumentSync(event)),
                    Default::default(),
                )
                .await;
            invocation.outcome.merge(outcome);
        }

        // Publish to the bucket's sync members (admitted targets and retained departing holders included), but
        // keep stamped peers above as genesis sources: a target must see writes made during the window.
        if let Some(config) = config {
            let now_ms = aruna_core::time::unix_timestamp_millis();
            for (_, record, _) in &mut records {
                if !record.target.uses_shard_topic() {
                    continue;
                }
                let members =
                    crate::placement::bucket_membership(config, &record.placement, now_ms).members;
                if !members.is_empty() {
                    record.peers = members;
                }
            }
        }
        records
    }
}

impl OperationsTaskHandler {
    async fn publish_drain_batches(
        &self,
        retry_key: &TaskKey,
        net_handle: &aruna_net::NetHandle,
        subbatches: Vec<DrainSubBatch>,
    ) -> (Duration, DrainSyncOutcome) {
        let mut publish_elapsed = Duration::ZERO;
        let mut outcome = DrainSyncOutcome::default();
        let mut awaiting_sync: Option<DrainSubBatch> = None;
        for mut subbatch in subbatches {
            let documents = std::mem::take(&mut subbatch.documents);
            let peers = subbatch.peers.clone();
            let (batch_topics, batch_origins) = subbatch.ordering_domains();
            let publish = async {
                let publish_started = Instant::now();
                let event = net_handle
                    .send_effect(Effect::Net(NetEffect::DocumentSync(
                        DocumentSyncEffect::PublishDocuments { documents, peers },
                    )))
                    .await;
                (event, publish_started.elapsed())
            };
            let ((publish_event, publish_time), sync_outcome) = tokio::join!(
                publish,
                self.sync_drain_subbatch(retry_key, net_handle, awaiting_sync.take())
            );
            publish_elapsed += publish_time;
            outcome.merge(sync_outcome);
            match publish_event {
                Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsPublished {
                    ..
                })) => awaiting_sync = Some(subbatch),
                Event::Net(NetEvent::DocumentSync(
                    DocumentSyncNetEvent::DocumentsPartiallyPublished {
                        published_indices,
                        retry_indices,
                        error,
                    },
                )) => {
                    warn!(
                        task_id = ?retry_key,
                        published = published_indices.len(),
                        retry = retry_indices.len(),
                        error = %error,
                        "Partially created local document sync batch"
                    );
                    outcome.retry_needed = true;
                    if let Some(retried) = subbatch.sync_subset(&retry_indices) {
                        let (topics, origins) = retried.ordering_domains();
                        outcome.blocked_topics.extend(topics);
                        outcome.blocked_origins.extend(origins);
                    } else {
                        outcome.blocked_topics.extend(batch_topics.iter().copied());
                        outcome
                            .blocked_origins
                            .extend(batch_origins.iter().copied());
                    }
                    match subbatch.sync_subset(&published_indices) {
                        Some(published) if !published.record_keys.is_empty() => {
                            awaiting_sync = Some(published);
                        }
                        Some(_) => {}
                        None => {
                            warn!(task_id = ?retry_key, "Invalid partial document publish indices");
                        }
                    }
                }
                Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error {
                    error, ..
                })) => {
                    warn!(task_id = ?retry_key, error = %error, "Failed to create local document sync batch");
                    outcome.retry_needed = true;
                    outcome.blocked_topics.extend(batch_topics.iter().copied());
                    outcome
                        .blocked_origins
                        .extend(batch_origins.iter().copied());
                }
                Event::Net(NetEvent::Error(error)) => {
                    warn!(task_id = ?retry_key, error = ?error, "Failed to create local document sync batch");
                    outcome.retry_needed = true;
                    outcome.blocked_topics.extend(batch_topics.iter().copied());
                    outcome
                        .blocked_origins
                        .extend(batch_origins.iter().copied());
                }
                other => {
                    warn!(task_id = ?retry_key, event = ?other, "Unexpected local document sync batch result");
                    outcome.retry_needed = true;
                    outcome.blocked_topics.extend(batch_topics.iter().copied());
                    outcome
                        .blocked_origins
                        .extend(batch_origins.iter().copied());
                }
            }
        }
        let sync_outcome = self
            .sync_drain_subbatch(retry_key, net_handle, awaiting_sync.take())
            .await;
        outcome.merge(sync_outcome);
        (publish_elapsed, outcome)
    }
}

impl OperationsTaskHandler {
    fn build_drain_batches(records: Vec<DrainRecord>) -> (usize, Vec<DrainSubBatch>) {
        let mut publish_groups: BTreeMap<
            Vec<aruna_core::NodeId>,
            (Vec<aruna_core::NodeId>, Vec<DrainSubBatch>),
        > = BTreeMap::new();
        for (record_key, record, topic) in records {
            let origin = admin_origin(&record);
            let document = publish_from_outbox(
                record.outbox_id,
                record.target.clone(),
                record.event,
                record.placement,
                record.allow_genesis,
            );
            let mut peer_key = record.peers.clone();
            crate::sync::shard_placement::sort_node_ids(&mut peer_key);
            let (peers, subbatches) = publish_groups
                .entry(peer_key)
                .or_insert_with(|| (record.peers.clone(), Vec::new()));
            if subbatches
                .last()
                .is_none_or(|subbatch| subbatch.documents.len() >= DRAIN_SUBBATCH_RECORDS)
            {
                subbatches.push(DrainSubBatch {
                    peers: peers.clone(),
                    documents: Vec::new(),
                    topics: Vec::new(),
                    origins: Vec::new(),
                    targets: Vec::new(),
                    record_keys: Vec::new(),
                });
            }
            let subbatch = subbatches.last_mut().expect("sub-batch was just pushed");
            subbatch.documents.push(document);
            subbatch.topics.push(topic);
            subbatch.origins.push(origin);
            subbatch.targets.push(record.target);
            subbatch.record_keys.push(record_key);
        }
        let groups = publish_groups.len();
        let subbatches = publish_groups
            .into_values()
            .flat_map(|(_, subbatches)| subbatches)
            .collect();
        (groups, subbatches)
    }
}

impl OperationsTaskHandler {
    async fn sync_drain_subbatch(
        &self,
        retry_key: &TaskKey,
        net_handle: &aruna_net::NetHandle,
        subbatch: Option<DrainSubBatch>,
    ) -> DrainSyncOutcome {
        let mut outcome = DrainSyncOutcome::default();
        let Some(subbatch) = subbatch else {
            return outcome;
        };
        let requested_targets = subbatch.targets.clone();
        let (batch_topics, batch_origins) = subbatch.ordering_domains();
        let sync_started = Instant::now();
        let event = net_handle
            .send_effect(Effect::Net(NetEffect::DocumentSync(
                DocumentSyncEffect::SyncDocuments {
                    topics: subbatch.topics,
                    peers: subbatch.peers,
                },
            )))
            .await;
        outcome.sync_elapsed = sync_started.elapsed();
        let mut outcome = self
            .finish_sync_batch(
                retry_key,
                subbatch.record_keys,
                requested_targets,
                event,
                outcome,
            )
            .await;
        if outcome.retry_needed {
            outcome.blocked_topics.extend(batch_topics);
            outcome.blocked_origins.extend(batch_origins);
        }
        outcome
    }
}

impl OperationsTaskHandler {
    pub(super) async fn finish_retry(
        &self,
        retry_key: TaskKey,
        mut rotation: OutboxRotation,
        reached_end: bool,
    ) {
        rotation.continuations = 0;
        if reached_end {
            self.close_rotation(retry_key, rotation).await;
        } else {
            self.store_rotation(rotation);
            self.reschedule_with_backoff(retry_key).await;
        }
    }
}

impl OperationsTaskHandler {
    async fn finish_drain_invocation(
        &self,
        retry_key: TaskKey,
        net_handle: &aruna_net::NetHandle,
        realm_id: RealmId,
        mut rotation: OutboxRotation,
        mut invocation: DrainInvocation,
        drain_started: Instant,
    ) {
        invocation
            .defer
            .deferred_topics
            .extend(invocation.outcome.blocked_topics.iter().copied());
        invocation
            .defer
            .blocked_origins
            .extend(invocation.outcome.blocked_origins.iter().copied());
        if let Some((oldest_ms, target, topic, placement)) = &invocation.oldest_stuck {
            error!(
                event = "pipeline.drain.stuck",
                count = invocation.stuck,
                oldest_age_ms = unix_timestamp_millis().saturating_sub(*oldest_ms),
                representative_target = ?target,
                representative_topic = %topic,
                representative_strategy = %placement.strategy_id,
                representative_shard = placement.shard,
                "Document sync outbox records are stuck: this node holds their buckets but their shard topic geneses have never arrived"
            );
        }
        if invocation.config_drained {
            self.schedule_sync_placements(realm_id, net_handle.node_id())
                .await;
        }

        let unvisited = invocation.has_unvisited();
        rotation.cursor = if invocation.reached_end {
            None
        } else {
            invocation.cursor.clone()
        };
        rotation.blocked_topics = invocation.defer.deferred_topics;
        rotation.blocked_origins = invocation.defer.blocked_origins;
        rotation.undeliverable_topics = invocation.defer.undeliverable_topics;
        rotation.totals.examined += invocation.records;
        rotation.totals.deleted += invocation.outcome.deleted;
        rotation.totals.deferred += invocation.deferred;
        rotation.totals.undeliverable += invocation.undeliverable;
        rotation.totals.retry_invocations +=
            usize::from(invocation.outcome.retry_needed || invocation.read_failed);
        rotation.totals.invocations = rotation.totals.invocations.saturating_add(1);

        let oldest_age_ms = invocation
            .oldest_record_ms
            .map(|record_ms| unix_timestamp_millis().saturating_sub(record_ms))
            .unwrap_or(0);
        if invocation.records > 0 {
            info!(
                event = "pipeline.drain.summary",
                records = invocation.records,
                examined = invocation.records,
                deleted = invocation.outcome.deleted,
                deferred = invocation.deferred,
                undeliverable = invocation.undeliverable,
                retry_scheduled = invocation.outcome.retry_needed || invocation.read_failed,
                has_unvisited = unvisited,
                continuation = rotation.continuations,
                rotation_complete = invocation.reached_end,
                groups = invocation.groups,
                subbatches = invocation.subbatches,
                pages = invocation.pages,
                scan_ms = duration_ms(invocation.scan_elapsed),
                publish_ms = duration_ms(invocation.publish_elapsed),
                sync_ms = duration_ms(invocation.outcome.sync_elapsed),
                project_ms = duration_ms(invocation.outcome.project_elapsed),
                delete_ms = duration_ms(invocation.outcome.delete_elapsed),
                total_ms = duration_ms(drain_started.elapsed()),
                oldest_age_ms,
                retry = invocation.outcome.retry_needed || invocation.read_failed,
                "Document sync outbox drain summary"
            );
        }

        if invocation.outcome.retry_needed || invocation.read_failed {
            self.finish_retry(retry_key, rotation, invocation.reached_end)
                .await;
        } else if unvisited {
            if rotation.continuations < self.outbox_limits.continuation_streak {
                rotation.continuations = rotation.continuations.saturating_add(1);
                self.store_rotation(rotation);
                self.reschedule_timer(retry_key, OUTBOX_CONTINUATION_AFTER)
                    .await;
            } else {
                rotation.continuations = 0;
                self.store_rotation(rotation);
                self.reschedule_timer(retry_key, DOCUMENT_SYNC_DEFER_RETRY_AFTER)
                    .await;
            }
        } else {
            self.close_rotation(retry_key, rotation).await;
        }
    }
}

impl OutboxDrainer {
    pub fn new(context: Arc<DriverContext>) -> Self {
        Self {
            handler: Arc::new(OperationsTaskHandler::new(context, JobsRuntime::new())),
        }
    }

    /// Runs one bounded invocation of the open rotation.
    pub async fn run_once(&self) {
        self.handler.drain_sync_outbox().await;
    }

    /// Records examined so far, and whether the cursor is parked mid-rotation.
    pub fn rotation_progress(&self) -> (usize, bool) {
        let rotation = self
            .handler
            .rotation
            .lock()
            .expect("outbox rotation mutex poisoned");
        (rotation.totals.examined, rotation.cursor.is_some())
    }
}

/// Kicks the installed document-sync drain owner without replacing an existing
/// persisted retry deadline.
pub async fn drive_sync_drain(context: Arc<DriverContext>) {
    let Some(task_handle) = context.task_handle.as_ref() else {
        warn!("Cannot kick document sync outbox drain without task handle");
        return;
    };
    restore_outbox_timers(&context.storage_handle, task_handle).await;
}

impl OperationsTaskHandler {
    async fn project_create_events(
        &self,
        retry_key: &TaskKey,
        targets: Vec<DocumentSyncTarget>,
        metadata_create_events: Vec<aruna_core::metadata::MetadataCreateEventRecord>,
    ) -> Result<(), ()> {
        if !metadata_create_events.is_empty() {
            let local_node_id = self.context.net_handle.as_ref().map(|net| net.node_id());
            if let Err(error) =
                project_create_events(&self.context, metadata_create_events, local_node_id).await
            {
                warn!(task_id = ?retry_key, error = ?error, "Failed to project metadata create event batch after document sync");
                return Err(());
            }
            return Ok(());
        }

        let mut create_event_targets = Vec::new();
        for target in targets {
            let DocumentSyncTarget::MetadataCreateEvent {
                document_id,
                event_id,
                ..
            } = target
            else {
                continue;
            };
            create_event_targets.push((document_id, event_id));
        }
        if let Err(error) = project_logged_events(&self.context, create_event_targets).await {
            warn!(task_id = ?retry_key, error = ?error, "Failed to project metadata create event batch from log after document sync");
            return Err(());
        }
        Ok(())
    }
}

impl OperationsTaskHandler {
    pub(super) async fn finish_sync_batch(
        &self,
        retry_key: &TaskKey,
        record_keys: Vec<Vec<u8>>,
        requested_targets: Vec<DocumentSyncTarget>,
        event: Event,
        mut outcome: DrainSyncOutcome,
    ) -> DrainSyncOutcome {
        match event {
            Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::DocumentsReconciled {
                targets,
                metadata_create_events,
                metadata_graph_tombstones,
                ..
            })) => {
                process_graph_tombstones(self.context.as_ref(), metadata_graph_tombstones).await;
                let mut refresh_targets = targets.clone();
                refresh_targets.extend(requested_targets);
                if let Some(net_handle) = self.context.net_handle.as_ref() {
                    refresh_usage_targets(
                        self.context.as_ref(),
                        net_handle.node_id(),
                        &refresh_targets,
                    )
                    .await;
                }
                refresh_target_interest(self.context.as_ref(), &refresh_targets).await;
                let project_started = Instant::now();
                let projected = self
                    .project_create_events(retry_key, targets, metadata_create_events)
                    .await;
                outcome.project_elapsed = project_started.elapsed();
                if projected.is_err() {
                    outcome.retry_needed = true;
                    return outcome;
                }
                let delete_started = Instant::now();
                let delete_count = record_keys.len();
                let deleted = crate::sync::document_outbox::delete_outbox_records(
                    &self.context.storage_handle,
                    record_keys,
                )
                .await;
                outcome.delete_elapsed = delete_started.elapsed();
                if deleted.is_ok() {
                    outcome.deleted += delete_count;
                }
                if let Err(error) = deleted {
                    warn!(task_id = ?retry_key, error = %error, "Failed to delete document sync outbox records");
                    outcome.retry_needed = true;
                } else if targets_change_dashboard(&refresh_targets) {
                    notify_dashboard_change(self.context.as_ref());
                }
            }
            Event::Net(NetEvent::DocumentSync(DocumentSyncNetEvent::Error { error, .. })) => {
                warn!(task_id = ?retry_key, error = %error, "Failed to sync document batch");
                outcome.retry_needed = true;
            }
            Event::Net(NetEvent::Error(error)) => {
                warn!(task_id = ?retry_key, error = ?error, "Failed to sync document batch");
                outcome.retry_needed = true;
            }
            other => {
                warn!(task_id = ?retry_key, event = ?other, "Unexpected document sync batch result");
                outcome.retry_needed = true;
            }
        }
        outcome
    }
}
