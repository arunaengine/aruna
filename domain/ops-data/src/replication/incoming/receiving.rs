//! Receives the transferred blob, then runs the purge-fence and destination-drift checks.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

impl IncomingVersionOperation {
    pub(super) fn receive_blob(&mut self) -> Turn {
        // The receiver routes with its own snapshot; the sender's stamped
        // backend crossed the wire but is ignored.
        let resolved = match self.resolve_destination() {
            Ok(resolved) => resolved.with_encryption(self.seal_plan),
            Err(error) => return self.fail(error),
        };
        let receive = BlobEffect::HandleReplication {
            replication_id: None,
            stream_id: self.stream_id,
            resolved,
            keep_alive: true,
            object: self.envelope.as_ref().map(|e| e.context.public_key),
        };
        self.blob(IncomingVersionState::ReceiveBlob, receive)
    }

    /// The apply runs as a child, in one write transaction of its own.
    pub(super) fn start_apply(&mut self) -> Turn {
        let child = IoCall::child(self.apply_child());
        self.call(IncomingVersionState::Apply, child)
    }

    pub(super) fn apply_child(&self) -> Self {
        Self {
            applying: true,
            publisher_node_id: self.publisher_node_id,
            now_ms: self.now_ms,
            destination_group_id: self.destination_group_id,
            destination_compression: self.destination_compression,
            destination_bound: self.destination_bound,
            destination_binding: self.destination_binding.clone(),
            quota_ceiling: self.quota_ceiling,
            existing_blob_location: self.existing_blob_location.clone(),
            replaced_version: self.replaced_version.clone(),
            existing_marker: self.existing_marker,
            advance_version_exists: self.advance_version_exists,
            received_blob: self.received_blob.clone(),
            replaced_logical_bytes: self.replaced_logical_bytes,
            replaced_reference_bytes: self.replaced_reference_bytes,
            rocrate_limits: self.rocrate_limits.clone(),
            gated_refs: self.gated_refs.clone(),
            gated_bucket: self.gated_bucket.clone(),
            seal_plan: self.seal_plan,
            envelope: self.envelope.clone(),
            ..Self::new(
                self.stream_id,
                self.local_node_id,
                self.local_realm_id,
                self.manifest.clone(),
            )
        }
    }

    /// The apply committed, or failed; an error that does not prove the commit never landed
    /// leaves the received copy to reconciliation.
    pub(super) fn accept_applied(
        &mut self,
        applied: Result<IncomingVersionResult, IncomingVersionError>,
    ) -> Turn {
        if let Err(error) = applied {
            if let IncomingVersionError::StorageError(storage) = &error
                && !storage.proves_no_commit()
            {
                self.keep_received();
            }
            return self.fail(error);
        }
        if let Some(received) = self.received_blob.as_mut() {
            received.cleanup_on_abort = false;
        }
        self.apply_committed = true;
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            kind = ?self.manifest.kind,
            "Committed incoming replication transaction"
        );
        match self.received_blob.as_ref().map(|it| it.location.ulid) {
            Some(id) => {
                self.release_id = Some(id);
                self.blob(
                    IncomingVersionState::ReleaseReservation,
                    BlobEffect::ReleaseReservation { id },
                )
            }
            None => self.call(
                IncomingVersionState::ScheduleUsage,
                schedule_snapshot_publish().into(),
            ),
        }
    }

    pub(super) fn check_purge_fence(&mut self) -> Turn {
        let key = fence_key(&self.manifest.bucket);
        self.read(
            IncomingVersionState::CheckPurgeFence,
            PURGE_FENCE_KEYSPACE,
            key,
        )
    }

    /// Re-reads the details the negotiation gate decided on, inside the very
    /// transaction that exposes the replica.
    pub(super) fn check_drift(&mut self) -> Turn {
        let bucket: Key = self.manifest.bucket.as_bytes().to_vec().into();
        let mut reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket.clone()),
            (
                NODE_SUBJECT_KEYSPACE.to_string(),
                Key::from(NODE_SUBJECT_KEY.to_vec()),
            ),
            // Encryption enabled since the negotiation must still refuse the replica.
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket),
        ];
        // The envelope is published only under the parameters and epoch read here.
        if let (Some(plan), Some(_)) = (self.seal_plan, &self.envelope) {
            reads.extend(abe_reads(plan.key));
        }
        self.reads(IncomingVersionState::CheckDrift, reads)
    }

    /// Received bytes for an encrypting destination must be sealed to its captured plan. An
    /// archive kept as sent has no content hash yet: only its original size is checked here.
    pub(super) fn validate_received(
        &self,
        location: &BackendLocation,
    ) -> Result<(), IncomingVersionError> {
        let StoredLayout::Pithos(layout) = &location.format.layout else {
            return self.validate_materialized_location(location);
        };
        let plan = self.seal_plan.ok_or(IncomingVersionError::StalePlan)?;
        if location.format.bucket_key() != Some(plan.key)
            || layout.storage_generation != plan.storage_generation
        {
            return Err(IncomingVersionError::StalePlan);
        }
        if location.get_blake3().is_some() {
            return self.validate_materialized_location(location);
        }
        let blob = self
            .manifest
            .blob
            .as_ref()
            .ok_or(IncomingVersionError::MissingBlobInfo)?;
        if location.blob_size != blob.size {
            return Err(IncomingVersionError::BlobSizeMismatch);
        }
        Ok(())
    }

    pub(super) fn accept_source_approval(&mut self, event: Event) -> Turn {
        let received = event.label();
        let Ok(BlobEvent::MessageReceived { payload, .. }) = event.into_blob() else {
            return self.invalid("VersionApplyApproved", received);
        };
        match VersionReplicationMessage::from_bytes(&payload) {
            Ok(VersionReplicationMessage::VersionApplyApproved) => self.start_apply(),
            Ok(_) => {
                let stream_id = self.stream_id;
                let received = Event::blob(BlobEvent::MessageReceived { stream_id, payload });
                self.invalid("VersionApplyApproved", received.label())
            }
            Err(error) => self.fail(error.into()),
        }
    }

    pub(super) fn accept_blob_finish(&mut self, event: Event) -> Turn {
        let location = match event.into_blob() {
            Ok(BlobEvent::ReplicationFinished { location }) => location,
            Ok(BlobEvent::Error(BlobError::WriteCleanup { location, .. })) => {
                self.received_blob = Some(ReceivedBlob::reserved(location));
                return self.fail(IncomingVersionError::ReplicationError(
                    ReplicationError::ReplicationFailed,
                ));
            }
            Ok(other) => {
                return self.invalid(
                    "Event::Blob(BlobEvent::{ReplicationFinished|WriteCleanup})",
                    other.label(),
                );
            }

            Err(other) => {
                return self.invalid(
                    "Event::Blob(BlobEvent::{ReplicationFinished|WriteCleanup})",
                    other.label(),
                );
            }
        };
        if let Err(err) = self.validate_received(&location) {
            self.received_blob = Some(ReceivedBlob::reserved(location));
            return self.fail(err);
        }
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            blob_size = location.blob_size,
            backend_path = %location.backend_path,
            "Received and validated replicated blob"
        );
        self.received_blob = Some(ReceivedBlob::reserved(location));
        self.start_apply()
    }

    pub(super) fn accept_purge_fence(&mut self, value: Option<Value>) -> Turn {
        let checked = check_fence_row(value.as_deref(), &self.manifest.bucket, &self.manifest.key);
        if let Err(error) = checked {
            return self.fail(error.into());
        }
        self.check_drift()
    }

    pub(super) fn accept_drift_check(&mut self, mut values: Vec<Option<Value>>) -> Turn {
        if let (Some(plan), Some(envelope)) = (self.seal_plan, &self.envelope) {
            let abe = values.split_off(values.len().saturating_sub(2));
            let anchored = parse_abe(&keyed(&abe), plan.key)
                .and_then(|(parameters, epoch)| envelope.anchored(&parameters, epoch));
            if anchored.is_err() {
                return self.fail(IncomingVersionError::StalePlan);
            }
        }
        let Some(settings) = values.pop().filter(|_| values.len() == 2) else {
            return self.fail(PolicyGateError::InvalidEvent.into());
        };
        let settings = match BucketEncryption::from_row(settings.as_deref()) {
            Ok(settings) => settings,
            Err(err) => return self.fail(err.into()),
        };
        // The captured plan must still be current; a bucket that started encrypting since the
        // negotiation refuses content that was not sealed for it.
        let stale = match self.seal_plan {
            Some(plan) => plan.still_current(&settings).is_err(),
            None => {
                settings.is_encrypted() && self.manifest.kind == ReplicationItemKind::Materialized
            }
        };
        if stale {
            return self.fail(IncomingVersionError::StalePlan);
        }
        let (bucket, subject) = match split_drift_reads(keyed(&values)) {
            Ok(split) => split,
            Err(error) => return self.fail(error.into()),
        };
        // A bucket bound or replaced since the negotiation takes nothing from this stream.
        let binding = bucket
            .as_ref()
            .and_then(|bucket| bucket.distribution.clone());
        if binding != self.destination_binding {
            return self.fail(IncomingVersionError::ForeignPublication);
        }
        let observed = GatedBucket::observe(bucket.as_ref());
        if let Some(gated) = self.gated_bucket.as_ref() {
            if !gated.matches(&observed) {
                return self.fail(PolicyGateError::Drift.into());
            }
            if let Err(error) = gated.check_subject(subject.as_ref()) {
                return self.fail(error.into());
            }
        }
        // A replica encoded under an older setting is never published: the bucket's
        // migration may already have passed this key.
        let written = self
            .effective_materialized_location()
            .ok()
            .filter(|written| written.format.bucket_key().is_none());
        if bucket
            .as_ref()
            .zip(written)
            .is_some_and(|(bucket, written)| {
                EncodingClass::from(bucket.compression) != written.format.encoding()
            })
        {
            return self.fail(StorageError::TransactionConflict.into());
        }
        match self.manifest.distribution {
            Some(_) => self.read_publication_record(true),
            None => self.verify_replaced(),
        }
    }
}
