use super::*;

impl DocumentSyncService {
    pub async fn publish_documents(
        &self,
        documents: Vec<DocumentSyncPublish>,
        peers: Vec<NodeId>,
    ) -> DocumentSyncNetEvent {
        let targets = documents
            .iter()
            .map(|document| document.target().clone())
            .collect::<Vec<_>>();
        match self.publish_events(documents, peers).await {
            Ok(outcome) if outcome.retry_indices.is_empty() => {
                DocumentSyncNetEvent::DocumentsPublished { targets }
            }
            Ok(outcome) if outcome.published_indices.is_empty() => DocumentSyncNetEvent::Error {
                target: outcome
                    .retry_indices
                    .first()
                    .and_then(|index| targets.get(*index).cloned()),
                error: outcome
                    .retry_error
                    .unwrap_or_else(|| "Document sync topic not ready".to_string()),
            },
            Ok(outcome) => DocumentSyncNetEvent::DocumentsPartiallyPublished {
                published_indices: outcome.published_indices,
                retry_indices: outcome.retry_indices,
                error: outcome
                    .retry_error
                    .unwrap_or_else(|| "Document sync topic not ready".to_string()),
            },
            Err(error) => DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    pub async fn reconcile_documents_event(&self) -> DocumentSyncNetEvent {
        match self.reconcile_documents().await {
            Ok(result) => DocumentSyncNetEvent::DocumentsReconciled {
                applied: result.applied(),
                targets: result.targets,
                metadata_create_events: result.metadata_create_events,
                metadata_graph_tombstones: result.metadata_graph_tombstones,
            },
            Err(error) => DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    pub async fn sync_document_event(
        &self,
        topic_id: ::irokle::TopicId,
        peers: Vec<NodeId>,
    ) -> DocumentSyncNetEvent {
        match self.has_topic(topic_id) {
            Ok(true) => {
                let selection = match self.sync_peer_selection(&peers, &topic_id) {
                    Ok(selection) => selection,
                    Err(error) => {
                        return DocumentSyncNetEvent::Error {
                            target: None,
                            error: error.to_string(),
                        };
                    }
                };
                self.log_peer_selection(topic_id, &selection);
                if let Err(error) = self.allow_sync_peers(&selection.peers) {
                    return DocumentSyncNetEvent::Error {
                        target: None,
                        error: error.to_string(),
                    };
                }
                let round = selection.round;
                let result = self.sync_topic(topic_id, selection).await;
                if let Err(error) = self.advance_cursor(topic_id, round) {
                    return DocumentSyncNetEvent::Error {
                        target: None,
                        error: error.to_string(),
                    };
                }
                if let Err(error) = result {
                    if let Err(persist_error) = self.flush_database() {
                        return DocumentSyncNetEvent::Error {
                            target: None,
                            error: persist_error.to_string(),
                        };
                    }
                    return DocumentSyncNetEvent::Error {
                        target: None,
                        error: error.to_string(),
                    };
                }
            }
            Ok(false) => {
                if let Err(error) = self.bootstrap_from_peers(topic_id, &peers).await {
                    if let Err(persist_error) = self.flush_database() {
                        warn!(%persist_error, %topic_id, "Failed to persist document sync bootstrap cleanup");
                    }
                    return DocumentSyncNetEvent::Error {
                        target: None,
                        error: error.to_string(),
                    };
                }
            }
            Err(error) => {
                return DocumentSyncNetEvent::Error {
                    target: None,
                    error: error.to_string(),
                };
            }
        }
        if let Err(error) = self.flush_database() {
            return DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            };
        }
        match self.reconcile_document_topics([topic_id]).await {
            Ok(result) => DocumentSyncNetEvent::DocumentsReconciled {
                applied: result.applied(),
                targets: result.targets,
                metadata_create_events: result.metadata_create_events,
                metadata_graph_tombstones: result.metadata_graph_tombstones,
            },
            Err(error) => DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    pub async fn sync_documents_event(
        &self,
        topic_ids: Vec<::irokle::TopicId>,
        peers: Vec<NodeId>,
    ) -> DocumentSyncNetEvent {
        let sync_started = Instant::now();
        let target_count = topic_ids.len();

        let mut seen_topics = BTreeSet::new();
        let mut topic_ids_out: Vec<::irokle::TopicId> = Vec::new();
        let mut bootstrap_cursor_dirty = false;
        for topic_id in topic_ids {
            if !seen_topics.insert(topic_id) {
                continue;
            }
            match self.has_topic(topic_id) {
                Ok(true) => topic_ids_out.push(topic_id),
                Ok(false) => {
                    bootstrap_cursor_dirty = true;
                    // Join-only: an unknown genesis-less topic is skipped, not
                    // fatal; it arrives via gossip or a later anti-entropy pass.
                    match self.bootstrap_from_peers(topic_id, &peers).await {
                        Ok(()) => topic_ids_out.push(topic_id),
                        Err(error) => {
                            debug!(%topic_id, error = %error, "skipping unbootstrappable document sync topic");
                        }
                    }
                }
                Err(error) => {
                    return DocumentSyncNetEvent::Error {
                        target: None,
                        error: error.to_string(),
                    };
                }
            }
        }

        if bootstrap_cursor_dirty && let Err(error) = self.flush_database() {
            return DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            };
        }

        let bootstrap_elapsed = sync_started.elapsed();
        let topic_ids = topic_ids_out;
        let peer_sync_started = Instant::now();
        if let Err(error) = self.sync_topics(topic_ids.clone(), &peers).await {
            return DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            };
        }
        let peer_sync_elapsed = peer_sync_started.elapsed();

        let flush_started = Instant::now();
        if let Err(error) = self.flush_database() {
            return DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            };
        }
        let flush_elapsed = flush_started.elapsed();
        let reconcile_started = Instant::now();
        match self.reconcile_document_topics(topic_ids).await {
            Ok(result) => {
                info!(
                    event = "pipeline.sync.summary",
                    targets = target_count,
                    applied = result.applied(),
                    bootstrap_ms = duration_ms(bootstrap_elapsed),
                    peer_sync_ms = duration_ms(peer_sync_elapsed),
                    flush_ms = duration_ms(flush_elapsed),
                    reconcile_ms = duration_ms(reconcile_started.elapsed()),
                    total_ms = duration_ms(sync_started.elapsed()),
                    "Document sync batch summary"
                );
                DocumentSyncNetEvent::DocumentsReconciled {
                    applied: result.applied(),
                    targets: result.targets,
                    metadata_create_events: result.metadata_create_events,
                    metadata_graph_tombstones: result.metadata_graph_tombstones,
                }
            }
            Err(error) => DocumentSyncNetEvent::Error {
                target: None,
                error: error.to_string(),
            },
        }
    }

    async fn publish_events(
        &self,
        documents: Vec<DocumentSyncPublish>,
        peers: Vec<NodeId>,
    ) -> Result<PublishEventsOutcome> {
        if documents.is_empty() {
            return Ok(PublishEventsOutcome::default());
        }
        let sync_peers = self.sync_peers(peers);
        self.allow_sync_peers(&sync_peers)?;
        let service = self.clone();
        let mut outcome = tokio::task::spawn_blocking(move || {
            service.publish_events_blocking(documents, &sync_peers)
        })
        .await
        .map_err(|error| NetError::Bootstrap(error.to_string()))??;
        let published = std::mem::take(&mut outcome.published);
        self.advance_topic_cursors(published).await?;
        self.flush_database()?;
        Ok(outcome)
    }

    fn publish_events_blocking(
        &self,
        documents: Vec<DocumentSyncPublish>,
        sync_peers: &BTreeSet<PeerId>,
    ) -> Result<PublishEventsOutcome> {
        let publish_started = Instant::now();
        let document_count = documents.len();
        let mut fast_path = 0usize;
        let mut fallback = 0usize;
        let oplog = Oplog::with_storage(self.node.storage().clone());
        let mut outcome = PublishEventsOutcome::default();
        for (index, document) in documents.into_iter().enumerate() {
            let allow_genesis = document.allow_genesis();
            let event = match document {
                DocumentSyncPublish::Upsert {
                    event_id,
                    target,
                    bytes,
                    change,
                    ..
                } => DocumentSyncEvent::Upsert {
                    event_id,
                    target,
                    bytes,
                    change,
                },
                DocumentSyncPublish::Delete {
                    event_id,
                    target,
                    change,
                    ..
                } => DocumentSyncEvent::Delete {
                    event_id,
                    target,
                    change,
                },
                DocumentSyncPublish::AdminOperation {
                    target,
                    event,
                    placement,
                    origin_signature,
                    ..
                } => {
                    let origin_signature = match origin_signature {
                        Some(signature) => signature,
                        None => match self.sign_admin_event(&event, &placement) {
                            Ok(signature) => signature,
                            Err(error) => {
                                // Retained, never counted as published: deleting
                                // the outbox row would lose the mutation.
                                error!(
                                    event = "pipeline.publish.unsigned_admin",
                                    origin = %event.origin_node_id,
                                    %error,
                                    "Refusing to publish an admin event this node cannot sign"
                                );
                                outcome.retry_indices.push(index);
                                outcome.retry_error.get_or_insert_with(|| error.to_string());
                                continue;
                            }
                        },
                    };
                    DocumentSyncEvent::AdminOperation {
                        target,
                        event,
                        placement,
                        origin_signature,
                    }
                }
            };
            let target = event.target().clone();
            let topic_id = target.sync_topic_id(self.realm_id, &event.placement());
            // Shard topics are join-only: only rank-0 creates the genesis, so a
            // publish onto a genesis-less shard defers the outbox record.
            let may_create_topic = !target.uses_shard_topic();
            let actor_id = ::irokle::actor_id_for(topic_id, self.node.peer_id());
            let envelope = EventEnvelope::encode_event(&event)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?;
            let op = match self.publish_event_op(
                &oplog,
                topic_id,
                actor_id,
                envelope,
                sync_peers,
                allow_genesis,
                may_create_topic,
                &mut fast_path,
                &mut fallback,
            ) {
                Ok(op) => op,
                Err(NetError::TopicNotReady(topic)) => {
                    outcome.retry_indices.push(index);
                    outcome
                        .retry_error
                        .get_or_insert_with(|| NetError::TopicNotReady(topic).to_string());
                    continue;
                }
                Err(error) => return Err(error),
            };
            outcome.published_indices.push(index);
            outcome
                .published
                .entry(topic_id)
                .or_default()
                .observe(op.signed.body.actor_id, op.signed.body.actor_seq);
        }
        // Member fan-out for admin publishes without explicit peers: the drain
        // sync stage pushes only to record peers, so publishing fans out itself.
        if sync_peers.is_empty() {
            for topic_id in outcome.published.keys() {
                self.net.schedule_topic_recheck(*topic_id)?;
            }
        }
        let published_count = outcome.published_indices.len();
        info!(
            event = "pipeline.publish.summary",
            documents = document_count,
            published = published_count,
            retry = outcome.retry_indices.len(),
            fast_path,
            fallback,
            existing = published_count.saturating_sub(fast_path + fallback),
            total_ms = duration_ms(publish_started.elapsed()),
            "Document sync publish batch breakdown"
        );
        Ok(outcome)
    }

    /// Signs an admin envelope this node originated. A record carrying another
    /// origin must arrive already signed: re-signing here would substitute the
    /// relay's identity for the origin's.
    fn sign_admin_event(
        &self,
        event: &AdminDocumentEvent,
        placement: &PlacementRef,
    ) -> Result<iroh::Signature> {
        if node_to_peer(&event.origin_node_id) != self.node.peer_id() {
            return Err(NetError::PublisherUnauthorized(format!(
                "admin event originated by {} arrived unsigned",
                event.origin_node_id
            )));
        }
        let bytes = event
            .signing_bytes(placement)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let signature = ::irokle::Signer::sign(self.node.signer(), &bytes)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        Ok(iroh::Signature::from_bytes(&signature.to_bytes()))
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_event_op(
        &self,
        oplog: &Oplog<::irokle::FjallStorage>,
        topic_id: ::irokle::TopicId,
        actor_id: ::irokle::ActorId,
        envelope: EventEnvelope,
        sync_peers: &BTreeSet<PeerId>,
        allow_genesis: bool,
        may_create_topic: bool,
        fast_path: &mut usize,
        fallback: &mut usize,
    ) -> Result<::irokle::Op> {
        let topic_missing = self
            .node
            .storage()
            .topic_state(&topic_id)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?
            .is_none();
        if topic_missing && !may_create_topic {
            return Err(NetError::Bootstrap(format!(
                "shard topic {topic_id} has no genesis yet; only its rank-0 holder creates it"
            )));
        }
        if topic_missing {
            // Only the document's origin may mint its topic genesis. Any other
            // publisher waits (retryable) for that genesis to replicate in.
            if !allow_genesis {
                return Err(NetError::TopicNotReady(topic_id.to_string()));
            }
            // Fast path for brand-new topics: genesis and first event in one
            // transaction; a lost genesis race falls back to the two-step flow.
            let genesis = TopicGenesis {
                event_type_id: DocumentSyncEvent::TYPE_ID.to_string(),
                initial_peers: sync_peers.clone(),
                replication_policy: ReplicationPolicy::all(),
            };
            match oplog.create_topic_genesis_with_event(
                topic_id,
                actor_id,
                genesis,
                envelope.clone(),
                self.node.signer(),
            ) {
                Ok((_, event_op)) => {
                    *fast_path += 1;
                    self.net.schedule_topic_recheck(topic_id)?;
                    return Ok(event_op);
                }
                Err(error) => {
                    *fallback += 1;
                    debug!(%topic_id, error = %error, "genesis+event fast path failed, falling back");
                }
            }
        }
        if may_create_topic {
            self.ensure_topic(topic_id, sync_peers, allow_genesis)?;
        }
        oplog
            .create_event_op(topic_id, actor_id, envelope, self.node.signer())
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    /// Signs one op carrying `payload` under the document-sync event type
    /// without encoding an event, so tests can deliver a payload no peer can
    /// decode. Returns the op's transport identity.
    #[cfg(test)]
    pub(crate) fn publish_raw_event(
        &self,
        topic_id: ::irokle::TopicId,
        payload: Vec<u8>,
    ) -> Result<SyncQuarantineIdentity> {
        let oplog = Oplog::with_storage(self.node.storage().clone());
        let actor_id = ::irokle::actor_id_for(topic_id, self.node.peer_id());
        let envelope = EventEnvelope {
            type_id: DocumentSyncEvent::TYPE_ID.to_string(),
            payload: payload.into(),
        };
        let op = self.publish_event_op(
            &oplog,
            topic_id,
            actor_id,
            envelope,
            &BTreeSet::new(),
            true,
            true,
            &mut 0,
            &mut 0,
        )?;
        self.flush_database()?;
        Ok(SyncQuarantineIdentity {
            topic: topic_id,
            actor: op.signed.body.actor_id,
            actor_seq: op.signed.body.actor_seq,
        })
    }

    /// Marks locally published ops as applied by advancing the per-topic
    /// cursor, so the origin's own reconcile does not re-emit them. Their
    /// effects are always applied locally before the outbox publish runs.
    async fn advance_topic_cursors(
        &self,
        published: BTreeMap<::irokle::TopicId, ::irokle::ActorClock>,
    ) -> Result<()> {
        if published.is_empty() {
            return Ok(());
        }
        let mut writes = Vec::with_capacity(published.len());
        for (topic_id, clock) in published {
            // A tie-break between the publish and this write leaves the ops on a
            // chain that no longer exists; the next reconcile replays the winner.
            let Some(genesis) = self.topic_genesis(topic_id)? else {
                continue;
            };
            let cursor_key = topic_cursor_key(topic_id);
            let mut cursor = applied_cursor_clock(
                self.node.storage(),
                topic_id,
                genesis,
                self.storage_read(
                    DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                    cursor_key.clone(),
                )
                .await?,
            )?;
            cursor.merge(&clock);
            let value = applied_cursor_value(self.node.storage(), topic_id, genesis, &cursor)?;
            writes.push((
                DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                cursor_key,
                value,
            ));
        }
        self.storage_batch_write(writes).await
    }

    pub(in crate::document_sync) fn flush_database(&self) -> Result<()> {
        self.db
            .persist(self.persist_policy.as_fjall())
            .map_err(|error| NetError::Bootstrap(error.to_string()))
    }

    pub(in crate::document_sync) fn ensure_topic(
        &self,
        topic_id: ::irokle::TopicId,
        peers: &BTreeSet<PeerId>,
        allow_genesis: bool,
    ) -> Result<::irokle::TopicId> {
        let mut genesis_error = None;
        for _ in 0..2 {
            if let Some(state) = self
                .node
                .storage()
                .topic_state(&topic_id)
                .map_err(|error| NetError::Bootstrap(error.to_string()))?
            {
                if state.event_type_id != DocumentSyncEvent::TYPE_ID {
                    return Err(NetError::Bootstrap(format!(
                        "Document sync topic {topic_id} has event type {}, expected {}",
                        state.event_type_id,
                        DocumentSyncEvent::TYPE_ID
                    )));
                }
                let missing_peers = self
                    .eligible_peers(peers.iter().copied(), Some(topic_id))
                    .into_iter()
                    .filter(|peer| !state.members.contains(peer))
                    .collect::<Vec<_>>();
                if !missing_peers.is_empty() {
                    let actor_id = ::irokle::actor_id_for(topic_id, self.node.peer_id());
                    let oplog = Oplog::with_storage(self.node.storage().clone());
                    for peer in missing_peers {
                        oplog
                            .create_control_op(
                                topic_id,
                                actor_id,
                                TopicControl::AddPeer { peer },
                                self.node.signer(),
                            )
                            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
                    }
                    self.net.schedule_topic_recheck(topic_id)?;
                }
                return Ok(topic_id);
            }

            // Only the document's origin may mint the genesis; other publishers
            // wait (retryable) for it to replicate in.
            if !allow_genesis {
                return Err(NetError::TopicNotReady(topic_id.to_string()));
            }

            let actor_id = ::irokle::actor_id_for(topic_id, self.node.peer_id());
            let genesis = TopicGenesis {
                event_type_id: DocumentSyncEvent::TYPE_ID.to_string(),
                initial_peers: self.eligible_peers(peers.iter().copied(), Some(topic_id)),
                replication_policy: ReplicationPolicy::all(),
            };
            let oplog = Oplog::with_storage(self.node.storage().clone());
            match oplog.create_topic_genesis(topic_id, actor_id, genesis, self.node.signer()) {
                Ok(_) => {
                    self.net.schedule_topic_recheck(topic_id)?;
                    return Ok(topic_id);
                }
                // A concurrent admission may have created the topic between the
                // state read and the genesis commit; re-check and reuse it.
                Err(error) => genesis_error = Some(error),
            }
        }
        Err(NetError::Bootstrap(
            genesis_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| format!("failed to ensure document sync topic {topic_id}")),
        ))
    }
}
