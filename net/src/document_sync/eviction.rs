use super::*;

impl DocumentSyncService {
    /// Takes the genesis tie-break eviction receiver. The embedder calls this
    /// once to drive the re-emission consumer; later calls return `None`.
    pub fn take_eviction_receiver(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<TopicEviction>> {
        self.eviction_rx.lock().take()
    }

    /// Decodes a genesis tie-break eviction into outbox events to re-emit with
    /// `allow_genesis: false`, preserving ids where the format can. Control ops
    /// and whole-document admin payloads carry nothing a peer can accept.
    pub fn decode_eviction(&self, eviction: TopicEviction) -> Vec<DocumentSyncEvictedDocument> {
        self.clear_cursor(eviction.topic_id);
        if let Err(error) = self.flush_database() {
            warn!(%error, topic_id = %eviction.topic_id, "Failed to persist document sync fan-out cursor reset");
        }
        let mut documents = Vec::new();
        for evicted in eviction.evicted {
            let TopicPayload::Event(envelope) = evicted.payload else {
                // Non-event control op (e.g. AddPeer/RemovePeer): nothing to re-emit.
                continue;
            };
            let event = match envelope.decode_event::<DocumentSyncEvent>() {
                Ok(event) => event,
                Err(error) => {
                    warn!(
                        topic_id = %eviction.topic_id,
                        op_id = %evicted.op_id,
                        %error,
                        "Skipping evicted op that is not a document sync event"
                    );
                    continue;
                }
            };
            match event {
                DocumentSyncEvent::AdminOperation {
                    target,
                    event,
                    placement,
                    origin_signature,
                } => {
                    documents.push(DocumentSyncEvictedDocument {
                        event_id: event.event_id,
                        target,
                        event: DocumentSyncOutboxEvent::AdminOperation {
                            event,
                            origin_signature: Some(origin_signature),
                        },
                        placement,
                        allow_genesis: false,
                    });
                }
                DocumentSyncEvent::Upsert {
                    event_id,
                    target,
                    bytes,
                    change,
                } => {
                    if reduced_admin_target(&target).is_some() {
                        warn!(
                            topic_id = %eviction.topic_id,
                            ?target,
                            "Dropping evicted whole-document admin upsert"
                        );
                        continue;
                    }
                    documents.push(DocumentSyncEvictedDocument {
                        event_id,
                        target,
                        placement: change.placement,
                        event: DocumentSyncOutboxEvent::Upsert { bytes, change },
                        allow_genesis: false,
                    });
                }
                DocumentSyncEvent::Delete {
                    event_id,
                    target,
                    change,
                } => {
                    if reduced_admin_target(&target).is_some() {
                        warn!(
                            topic_id = %eviction.topic_id,
                            ?target,
                            "Dropping evicted whole-document admin delete"
                        );
                        continue;
                    }
                    documents.push(DocumentSyncEvictedDocument {
                        event_id,
                        target,
                        placement: change.placement,
                        event: DocumentSyncOutboxEvent::Delete { change },
                        allow_genesis: false,
                    });
                }
            }
        }
        documents
    }

    /// Forwards evictions produced by this service's own admission paths into
    /// the shared eviction sink.
    pub(in crate::document_sync) fn forward_evictions(&self, evictions: Vec<TopicEviction>) {
        forward_evictions_to(&self.eviction_tx, evictions);
    }

    /// Decodes an eviction Irokle already journalled with its reset and drops
    /// the replaced chain's cursors. The journal remains until every replacement
    /// outbox row is committed.
    pub async fn consume_eviction(&self, eviction: TopicEviction) -> Option<PendingEviction> {
        let topic_id = eviction.topic_id;
        let key = eviction.key();
        // Irokle journals nothing for an eviction with no payloads, so treating
        // one as pending would arm the retry timer against a phantom entry.
        let journalled = !eviction.evicted.is_empty();
        let documents = self.decode_eviction(eviction);
        self.reset_applied_cursor(topic_id).await;
        if !journalled {
            return None;
        }
        self.eviction_buckets.write().insert(
            key,
            Some(
                documents
                    .iter()
                    .map(|document| document.placement)
                    .collect(),
            ),
        );
        Some(PendingEviction { key, documents })
    }

    /// Irokle journal entries left by an interrupted eviction handoff.
    pub async fn pending_evictions(&self) -> Result<Vec<PendingEviction>> {
        let evictions = self
            .node
            .pending_evictions()
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        let mut pending = Vec::with_capacity(evictions.len());
        for eviction in evictions {
            pending.extend(self.consume_eviction(eviction).await);
        }
        Ok(pending)
    }

    /// Releases a journal entry once every replacement outbox row is durable.
    pub async fn clear_eviction(&self, key: ::irokle::EvictionKey) -> Result<()> {
        self.node
            .clear_eviction(&key)
            .map_err(|error| NetError::Bootstrap(error.to_string()))?;
        self.eviction_buckets.write().remove(&key);
        Ok(())
    }

    /// Whether a journalled eviction may still write replacement outbox rows for
    /// `placement`. An entry recovered at open blocks every bucket until the
    /// consumer decodes the buckets it actually targets.
    pub fn eviction_pending(&self, placement: &PlacementRef) -> bool {
        self.eviction_buckets
            .read()
            .values()
            .any(|buckets| buckets.as_ref().is_none_or(|list| list.contains(placement)))
    }

    /// Drops the applied-ops cursor of a topic whose chain was replaced by a
    /// genesis tie-break. The winning chain renumbers every actor sequence, so
    /// a cursor from the losing chain silently skips the winner's first ops.
    async fn reset_applied_cursor(&self, topic_id: ::irokle::TopicId) {
        let _reconcile_guard = self.reconcile_lock.lock().await;
        debug!(%topic_id, "Resetting document sync applied-ops cursor after a genesis tie-break");
        match self
            .storage
            .send_storage_effect(StorageEffect::Delete {
                key_space: DOCUMENT_SYNC_APPLIED_OPS_KEYSPACE.to_string(),
                key: topic_cursor_key(topic_id),
                txn_id: None,
            })
            .await
        {
            Event::Storage(StorageEvent::DeleteResult { .. }) => {}
            other => {
                warn!(%topic_id, ?other, "Failed to reset document sync applied-ops cursor");
            }
        }
    }
}
