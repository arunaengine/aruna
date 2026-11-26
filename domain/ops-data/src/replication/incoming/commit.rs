//! Exposes the replica: head transition, version and side records, usage and the commit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

impl IncomingVersionOperation {
    /// Why this replica lands here, from what this node itself observed: an
    /// advance of a reference it serves, a standing relationship the sender
    /// names, or an explicitly requested copy.
    pub(super) fn copy_origin(&self) -> CopyOrigin {
        if self.manifest.reference_advance.is_some() {
            return CopyOrigin::Reference;
        }
        match self.manifest.origin.as_ref() {
            Some(origin) => CopyOrigin::Sync {
                relationship_id: origin.relationship_id,
            },
            None => CopyOrigin::Replicate,
        }
    }

    /// Subject generation the gate admitted this replica under; zero when the
    /// version is ungoverned.
    pub(super) fn stored_subject(&self) -> u64 {
        self.gated_bucket
            .as_ref()
            .and_then(|gated| gated.subject_generation)
            .unwrap_or_default()
    }

    /// Rows that publish an archive: its pending location with the claimed hash, or its
    /// location, and the owner row of this version in both cases.
    pub(super) fn archive_rows(
        &self,
        location: &BackendLocation,
    ) -> Result<Vec<(String, Key, Value)>, IncomingVersionError> {
        let archive = ArchiveKey::of(location);
        let version = VersionKey::new(
            &self.manifest.bucket,
            &self.manifest.key,
            self.manifest.version_id,
        );
        let owner = CopyOwner::new(archive.clone(), version);
        let mut rows = vec![(
            COPY_OWNER_KEYSPACE.to_string(),
            owner.key()?.into(),
            Vec::new().into(),
        )];
        match location.get_blake3() {
            Some(hash) => {
                let key = BlobLocationKey::from_blake3(
                    hash,
                    location.format.encoding(),
                    location.backend.clone(),
                )?;
                rows.push((
                    BLOB_LOCATIONS_KEYSPACE.to_string(),
                    key.to_bytes().into(),
                    location.to_bytes()?.into(),
                ));
            }
            None => {
                let claimed = self
                    .manifest
                    .blob
                    .as_ref()
                    .ok_or(IncomingVersionError::MissingBlobInfo)?
                    .hash;
                rows.push((
                    PENDING_LOCATION_KEYSPACE.to_string(),
                    archive.to_bytes().into(),
                    location.to_bytes()?.into(),
                ));
                rows.push((
                    PENDING_CLAIM_KEYSPACE.to_string(),
                    archive.to_bytes().into(),
                    claimed.to_vec().into(),
                ));
            }
        }
        Ok(rows)
    }

    pub(super) fn write_hash_lookup(&mut self) -> Turn {
        if self.is_reference_item() {
            return self.write_object_lookup();
        }
        if let Some(received) = self.received_blob.as_ref()
            && let Err(err) = self.validate_received(&received.location)
        {
            return self.fail(err);
        }

        if self.received_blob.is_none() && self.existing_blob_location.is_none() {
            return self.write_object_lookup();
        }

        self.begin_blob_location()
    }

    pub(super) fn begin_blob_location(&mut self) -> Turn {
        let Ok(location) = self.effective_materialized_location() else {
            return self.write_object_lookup();
        };
        if let BackendRef::Group(backend_id) = location.backend {
            return self.read(
                IncomingVersionState::FenceBackend,
                STORAGE_BACKEND_KEYSPACE,
                backend_key(backend_id),
            );
        }
        self.verify_existing_blob()
    }

    /// A negotiation that adopted an existing copy read it outside the
    /// transaction. Re-reading it inside makes the commit fail rather than
    /// leave the version naming bytes another writer has since removed.
    pub(super) fn verify_existing_blob(&mut self) -> Turn {
        if self.received_blob.is_some() {
            return self.write_blob_location();
        }
        let Some(location) = self.existing_blob_location.clone() else {
            return self.write_blob_location();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.write_blob_location();
        };
        let hash: [u8; 32] = match blake3_hash.try_into() {
            Ok(hash) => hash,
            Err(err) => return self.fail(ConversionError::from(err).into()),
        };
        let key = BlobLocationKey::new(hash, location.format.encoding(), location.backend);
        self.read(
            IncomingVersionState::VerifyExistingBlob,
            BLOB_LOCATIONS_KEYSPACE,
            key.to_bytes(),
        )
    }

    pub(super) fn write_blob_location(&mut self) -> Turn {
        let Ok(location) = self.effective_materialized_location() else {
            return self.write_object_lookup();
        };
        if location.format.bucket_key().is_some() {
            let writes = match self.archive_rows(&location) {
                Ok(writes) => writes,
                Err(error) => return self.fail(error),
            };
            return self.write(IncomingVersionState::WriteBlobLocation, writes);
        }
        let Some(blake3_hash) = location.get_blake3() else {
            return self.write_object_lookup();
        };
        let hash: [u8; 32] = match blake3_hash.try_into() {
            Ok(hash) => hash,
            Err(err) => return self.fail(ConversionError::from(err).into()),
        };
        let key = BlobLocationKey::new(hash, location.format.encoding(), location.backend.clone());
        let value = match location.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.fail(err.into()),
        };
        let row = (
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            key.to_bytes().into(),
            value.into(),
        );
        self.write(IncomingVersionState::WriteBlobLocation, vec![row])
    }

    pub(super) fn write_object_lookup(&mut self) -> Turn {
        if !self.manifest.current_version {
            // The head may still name the replaced row, whose liveness then changes.
            let marker = self.manifest.kind == ReplicationItemKind::DeleteMarker;
            if self
                .existing_marker
                .is_some_and(|existing| existing != marker)
            {
                return self.read_head(IncomingVersionState::ReadNamedHead);
            }
            return self.write_version();
        }
        if self.manifest.current_version_generation.is_none() {
            return self.fail(IncomingVersionError::MissingVersionGeneration);
        }
        self.read_head(IncomingVersionState::ReadObjectLookup)
    }

    pub(super) fn read_head(&mut self, state: IncomingVersionState) -> Turn {
        let key = match BlobHeadKey::new(&self.manifest.bucket, &self.manifest.key).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        self.read(state, BLOB_HEAD_KEYSPACE, key)
    }

    pub(super) fn read_current(&mut self, version_id: Ulid) -> Turn {
        let key = match VersionKey::new(&self.manifest.bucket, &self.manifest.key, version_id)
            .to_bytes()
        {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        self.read(
            IncomingVersionState::ReadCurrentVersion,
            BLOB_VERSIONS_KEYSPACE,
            key,
        )
    }

    /// The liveness change of a replaced row the head names while the head stays: a version
    /// replaced by a marker deletes the object, the reverse revives it.
    pub(super) fn named_head_delta(&self, head: Option<&CurrentVersionPointer>) -> i128 {
        let names = head.is_some_and(|head| head.version_id == self.manifest.version_id);
        match self.existing_marker.filter(|_| names) {
            Some(was_marker) => {
                let live = self.manifest.kind == ReplicationItemKind::Materialized;
                i128::from(u8::from(live)) - i128::from(u8::from(!was_marker))
            }
            None => 0,
        }
    }

    pub(super) fn accept_named_head(&mut self, value: Option<Value>) -> Turn {
        let head = value.as_deref().map(CurrentVersionPointer::from_bytes);
        match head.transpose() {
            Ok(head) => {
                self.object_delta = self.named_head_delta(head.as_ref());
                self.write_named_life(head)
            }
            Err(error) => self.fail(error.into()),
        }
    }

    /// A replaced row the head still names flips liveness without a new head order, so only the
    /// local life changes: a fresh one for a revived object, none for a marker.
    pub(super) fn write_named_life(&mut self, head: Option<CurrentVersionPointer>) -> Turn {
        let Some(head) = head.filter(|_| self.object_delta != 0) else {
            return self.write_version();
        };
        let live = self.manifest.kind == ReplicationItemKind::Materialized;
        let life_id = live.then(|| self.ids.mint(self.now_ms));
        let row = BlobHeadKey::new(&self.manifest.bucket, &self.manifest.key)
            .to_bytes()
            .and_then(|key| Ok((key, head.with_life(life_id).to_bytes()?)));
        match row {
            Ok((key, value)) => {
                let row = (BLOB_HEAD_KEYSPACE.to_string(), key.into(), value.into());
                self.write(IncomingVersionState::ApplyHeadTransition, vec![row])
            }
            Err(error) => self.fail(error.into()),
        }
    }

    /// Sets the local life of the pending head: a live successor of a live head keeps it, a
    /// revived or new object gets a fresh one and a marker none. The sender's life never counts.
    pub(super) fn apply_liveness(&mut self, previous_live: bool) -> Turn {
        let next_live = self.manifest.kind == ReplicationItemKind::Materialized;
        self.object_delta = i128::from(u8::from(next_live)) - i128::from(u8::from(previous_live));
        let kept = (self.existing_current_pointer.as_ref())
            .and_then(|pointer| pointer.life_id)
            .filter(|_| previous_live);
        let life_id = match next_live {
            true => Some(kept.unwrap_or_else(|| self.ids.mint(self.now_ms))),
            false => None,
        };
        if let Some(pending) = self.pending_head.as_mut() {
            pending.pointer.life_id = life_id;
        }
        self.prepare_head_transition()
    }

    pub(super) fn write_compared_object(&mut self, existing: Option<&[u8]>) -> Turn {
        let Some(incoming_generation) = self.manifest.current_version_generation else {
            return self.write_version();
        };

        let existing_pointer = match existing.map(CurrentVersionPointer::from_bytes).transpose() {
            Ok(pointer) => pointer,
            Err(err) => return self.fail(err.into()),
        };
        if let Some(advance) = self.manifest.reference_advance.as_ref() {
            let Some(previous_generation) = advance.generation.checked_sub(1) else {
                return self.fail(IncomingVersionError::InvalidReferenceAdvance);
            };
            let Some(pointer) = existing_pointer else {
                return self.fail(IncomingVersionError::InvalidReferenceAdvance);
            };
            if incoming_generation != advance.generation {
                return self.fail(IncomingVersionError::InvalidReferenceAdvance);
            }
            let advanced = CurrentVersionPointer::new_with_generation(
                self.manifest.version_id,
                advance.generation,
            )
            .with_epoch(self.original_epoch());
            if self.advance_version_exists
                && (advanced == pointer || pointer.order() > advanced.order())
            {
                self.existing_current_pointer = Some(pointer);
                self.object_delta = 0;
                self.pending_head = None;
                return self.write_live_obligation();
            }
            // Lineage validation stands, and the accepted head order still has
            // the final word: an advance may never regress the observed head.
            if pointer.generation != previous_generation
                || pointer.version_id != advance.predecessor
                || !advanced.supersedes(Some(&pointer))
            {
                return self.fail(IncomingVersionError::InvalidReferenceAdvance);
            }

            self.existing_current_pointer = Some(pointer.clone());
            self.pending_head = Some(PendingHeadTransition {
                pointer: advanced,
                current_hash: None,
            });
            return self.read_current(pointer.version_id);
        }
        let candidate = CurrentVersionPointer::new_with_generation(
            self.manifest.version_id,
            incoming_generation,
        )
        .with_epoch(self.original_epoch());
        let should_write = candidate.supersedes(existing_pointer.as_ref());

        self.existing_current_pointer = existing_pointer.clone();
        if should_write {
            self.pending_head = Some(PendingHeadTransition {
                pointer: candidate.clone(),
                current_hash: self.current_manifest_hash(),
            });
        } else {
            self.pending_head = None;
            self.object_delta = self.named_head_delta(existing_pointer.as_ref());
            return self.write_named_life(existing_pointer);
        }
        self.resume_head_transition()
    }

    /// Continues exactly where `write_compared_object` left off; the
    /// stored pointers carry the decision, so no continuation state is needed.
    pub(super) fn resume_head_transition(&mut self) -> Turn {
        if self.pending_head.is_none() {
            return self.write_version();
        }
        match self.existing_current_pointer.clone() {
            Some(pointer) => self.read_current(pointer.version_id),
            None => self.apply_liveness(false),
        }
    }

    pub(super) fn write_version(&mut self) -> Turn {
        self.write_blob_version()
    }

    /// The manifest's epoch inside a distributed bucket; an ordinary bucket keeps 0, so its
    /// heads order by generation exactly as before.
    pub(super) fn original_epoch(&self) -> OriginalEpoch {
        match self.destination_bound {
            true => self.manifest.original_epoch,
            false => OriginalEpoch::default(),
        }
    }

    /// The generation the version was written as, kept inside a distributed bucket whether
    /// or not it is the current version; an ordinary bucket keeps 0.
    pub(super) fn version_generation(&self) -> u64 {
        match (self.destination_bound, self.manifest.version_generation) {
            (false, _) => 0,
            (true, 0) => self.manifest.current_version_generation.unwrap_or_default(),
            (true, generation) => generation,
        }
    }

    pub(super) fn write_blob_version(&mut self) -> Turn {
        match self.version_rows() {
            Ok(rows) => self.write(IncomingVersionState::WriteBlobVersion, rows),
            Err(error) => self.fail(error),
        }
    }

    pub(super) fn copy_registration<'a>(
        &'a self,
        version: VersionKey,
        location: &'a BackendLocation,
    ) -> CopyRegistration<'a> {
        CopyRegistration {
            version,
            node_id: self.local_node_id,
            location,
            policies: &self.gated_refs,
            origin: self.copy_origin(),
            subject_generation: self.stored_subject(),
            registered_at_ms: self.manifest.version_id.timestamp_ms(),
        }
    }

    /// The version and every row that commits with it, written in one call.
    pub(super) fn version_rows(
        &mut self,
    ) -> Result<Vec<(String, Key, Value)>, IncomingVersionError> {
        let version_key = VersionKey::new(
            &self.manifest.bucket,
            &self.manifest.key,
            self.manifest.version_id,
        );
        let mut extra = Vec::new();
        let (version, materialized_hash) = match self.manifest.kind {
            ReplicationItemKind::Materialized if self.is_reference_item() => {
                let version = self.reference_version()?;
                (version.with_policies(self.gated_refs.clone())?, None)
            }
            ReplicationItemKind::Materialized if self.pending_replica() => {
                let location = self.effective_materialized_location()?;
                let pending = BlobVersion::pending(
                    ArchiveKey::of(&location),
                    self.manifest.created_at,
                    self.manifest.created_by,
                    self.manifest.source.clone(),
                )
                .with_metadata(self.manifest.metadata.clone())
                .with_publisher(self.publisher_node_id)
                .with_policies(self.gated_refs.clone())?;
                // No hash alias and no DHT entry until promotion verified the claimed hash.
                let copy = self.copy_registration(version_key.clone(), &location);
                extra.push(register_entry(copy)?);
                (pending, None)
            }
            ReplicationItemKind::Materialized => {
                let location = self.effective_materialized_location()?;
                let blake3_hash = location.get_blake3();
                let blake3_hash = blake3_hash.ok_or(IncomingVersionError::MissingBlobLocation)?;
                let hash: [u8; 32] = blake3_hash.try_into().map_err(ConversionError::from)?;
                let materialized = BlobVersion::materialized(
                    hash,
                    location.backend.clone(),
                    location.format.encoding(),
                    self.manifest.created_at,
                    self.manifest.created_by,
                    self.manifest.source.clone(),
                )
                .with_metadata(self.manifest.metadata.clone())
                .with_publisher(self.publisher_node_id)
                .with_policies(self.gated_refs.clone())?;
                (materialized, Some(hash))
            }
            ReplicationItemKind::DeleteMarker => (
                BlobVersion::deleted(self.manifest.created_at, self.manifest.created_by)
                    .with_publisher(self.publisher_node_id),
                None,
            ),
        };

        let version = self.stamped(version);
        self.written_identity = logical_identity(&version);
        self.written_pending = matches!(version.state, BlobVersionState::PendingContent { .. });
        // A member stores the binding compact too, with the root record it names.
        let (version, root) = compact_version(version)?;
        let mut rows = vec![(
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes()?.into(),
            version.to_bytes()?.into(),
        )];
        rows.append(&mut extra);
        if let Some(root) = root {
            rows.push(root_write(&root)?);
        }
        // A member keeps the receipt of a pulled version, so a later coordinator answers replays.
        rows.extend(self.pull_receipt());
        // Only a same-set publication keeps release tags; its row commits with the version.
        rows.extend(release_rows(&version_key, &version)?);
        // The envelope, its mappings and usage are published with the version they name.
        if let (Some(envelope), Some(received)) = (&self.envelope, &self.received_blob) {
            let metadata = (&self.manifest.metadata, self.rocrate_limits.metadata_bytes);
            let located = located_rows(envelope, &version_key, &received.location, metadata);
            let (written, charge) = located.map_err(BlobError::from)?;
            self.envelope_bytes = charge;
            rows.extend(written);
        }
        if let Some(hash) = materialized_hash {
            let context = self.alias_context()?;
            rows.push(self.index_row(&context, hash)?);
            // The replica becomes serveable exactly when its version commits.
            // A reference item materializes nothing and registers nothing.
            let location = self.effective_materialized_location()?;
            let copy = self.copy_registration(version_key, &location);
            rows.push(register_entry(copy)?);
        }
        // A version new to this node is journaled last in its transaction. A delete marker is
        // not an upload, and a rewrite of a version already here keeps its first anchor.
        if self.manifest.kind != ReplicationItemKind::DeleteMarker
            && self.replaced_version.is_none()
        {
            let group_id = self.destination_group_id.unwrap_or(self.manifest.group_id);
            let uploaded = SourceEvent::data_uploaded(
                (self.local_realm_id, group_id, self.local_node_id),
                (&self.manifest.bucket, &self.manifest.key),
                self.manifest.version_id,
                self.now_ms,
            );
            rows.extend(journal_entries(&uploaded)?);
        }
        Ok(rows)
    }

    pub(super) fn write_multipart_metadata(&mut self) -> Turn {
        let Some(multipart) = self.manifest.multipart.as_ref() else {
            return self.write_live_obligation();
        };

        let mut writes = Vec::with_capacity(multipart.parts.len() + 1);

        let summary_key = match MultipartObjectKey::summary(self.manifest.version_id).to_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        let summary_value = match multipart.summary.to_bytes() {
            Ok(value) => value,
            Err(err) => return self.fail(err.into()),
        };
        writes.push((
            OBJECT_METADATA_KEYSPACE.to_string(),
            summary_key.into(),
            summary_value.into(),
        ));

        for part in &multipart.parts {
            let key = match MultipartObjectKey::part(self.manifest.version_id, part.part_number)
                .to_bytes()
            {
                Ok(key) => key,
                Err(err) => return self.fail(err.into()),
            };
            let value = match part.to_bytes() {
                Ok(value) => value,
                Err(err) => return self.fail(err.into()),
            };
            writes.push((
                OBJECT_METADATA_KEYSPACE.to_string(),
                key.into(),
                value.into(),
            ));
        }

        self.write(IncomingVersionState::WriteMultipartMetadata, writes)
    }

    pub(super) fn write_live_obligation(&mut self) -> Turn {
        let auth_context = match self.manifest.reference_advance.as_ref() {
            Some(_) => self.manifest.auth_context.clone(),
            None => {
                let Some(auth_context) = self.manifest.writer_auth_context.clone() else {
                    return self.start_commit_quota();
                };
                auth_context
            }
        };
        let mut record = LiveObligationRecord::new(
            self.local_node_id,
            auth_context,
            self.manifest.bucket.clone(),
            self.manifest.key.clone(),
            self.manifest.version_id,
            self.manifest.kind == ReplicationItemKind::DeleteMarker,
        )
        .with_origin(self.manifest.origin.clone())
        .with_sources(self.manifest.upstream_sources.clone());
        if let Some(advance) = self.manifest.reference_advance {
            record = record.with_reference_advance(advance);
        }
        match live_obligation_entry(&record) {
            Ok(row) => self.write(IncomingVersionState::WriteLiveObligation, vec![row]),
            Err(error) => self.fail(error.into()),
        }
    }

    /// The row that reconciles the received write when its commit outcome is never learned.
    pub(super) fn cleanup_row(&self) -> Result<Option<(String, Key, Value)>, IncomingVersionError> {
        let Some(received) = self.received_blob.as_ref() else {
            return Ok(None);
        };
        let location = &received.location;
        let blake3 = location
            .get_blake3()
            .and_then(|hash| <[u8; 32]>::try_from(hash).ok());
        let owner = match blake3 {
            Some(blake3) => WriteOwner::Blob {
                blake3,
                realm_id: self.local_realm_id,
                ttl_ms: self.rocrate_limits.holder_ttl_ms,
                announce: !self.destination_bound,
            },
            // A pending archive is kept while its pending location names it.
            None if location.format.bucket_key().is_some() => WriteOwner::Pending,
            None => return Err(IncomingVersionError::MissingBlobLocation),
        };
        let work = BlobCleanupWork::ReconcileWrite {
            location: location.clone(),
            owner,
        };
        Ok(Some((
            BLOB_CLEANUP_KEYSPACE.to_string(),
            location.ulid.to_bytes().to_vec().into(),
            work.to_bytes()?.into(),
        )))
    }

    pub(super) fn commit_or_cleanup(&mut self) -> Turn {
        if self.manifest.kind != ReplicationItemKind::Materialized
            || self.is_reference_item()
            || self.received_blob.is_none()
        {
            return self.finish_apply();
        }
        match self.cleanup_row() {
            Ok(Some(row)) => self.write(IncomingVersionState::WriteCleanupRow, vec![row]),
            Ok(None) => self.fail(ReplicationError::ReplicationFailed.into()),
            Err(error) => self.fail(error),
        }
    }

    /// The apply's outcome; the runner commits its transaction with it.
    pub(super) fn finish_apply(&mut self) -> Turn {
        Ok(Next::Done(self.result(true)))
    }

    pub(super) fn usage_delta(&self) -> Result<UsageDelta, IncomingVersionError> {
        let bytes = match self.manifest.kind {
            ReplicationItemKind::Materialized => i128::from(self.incoming_logical_bytes()?),
            ReplicationItemKind::DeleteMarker => 0,
        };
        let envelope = i128::from(self.envelope_bytes);
        Ok(UsageDelta {
            objects: self.object_delta,
            logical_bytes: if self.is_reference_item() {
                0
            } else {
                bytes + envelope
            } - i128::from(self.replaced_logical_bytes),
            referenced_bytes: if self.is_reference_item() { bytes } else { 0 }
                - i128::from(self.replaced_reference_bytes),
            ..Default::default()
        })
    }

    /// The receipt row of a device pull the received version of a distributed bucket records.
    pub(super) fn pull_receipt(&self) -> Option<(String, Key, Value)> {
        self.destination_binding.as_ref()?;
        let marker = self.manifest.kind == ReplicationItemKind::DeleteMarker;
        let target = (self.manifest.bucket.as_str(), self.manifest.key.as_str());
        let key = tagged_receipt(target, &self.manifest.metadata, marker)?;
        Some((
            SYNC_RECEIPT_KEYSPACE.to_string(),
            key.into(),
            self.manifest.version_id.to_bytes().to_vec().into(),
        ))
    }

    /// The received version and, when this apply installs it, the head of a distributed bucket.
    pub(super) fn inventory_change(&self) -> Option<InventoryChange> {
        let marker = self.manifest.kind == ReplicationItemKind::DeleteMarker;
        let size = match marker {
            true => 0,
            false => self.incoming_logical_bytes().unwrap_or_default(),
        };
        let added = AddedVersion {
            version_id: self.manifest.version_id,
            epoch: self.original_epoch(),
            generation: self.version_generation(),
            entry: VersionEntry {
                marker,
                size,
                referenced: self.is_reference_item(),
                identity: self.written_identity,
                pending: self.written_pending,
                searchable: searchable(&self.manifest.metadata),
            },
        };
        let head = self.installed_head.is_some();
        let mut change = version_written(
            self.destination_binding.as_ref(),
            &self.manifest.key,
            added,
            head,
        )?;
        // A reference carries its source root, which a member taking over the range checks.
        let source = self.manifest.source.as_ref();
        change.root = source
            .filter(|_| self.is_reference_item())
            .and_then(RootEntry::of);
        Some(change)
    }

    pub(super) fn start_commit_quota(&mut self) -> Turn {
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionError::DestinationNotFound);
        };
        let group_delta = match self.usage_delta() {
            Ok(delta) => delta,
            Err(error) => return self.fail(error),
        };
        let marker = self.manifest.kind == ReplicationItemKind::DeleteMarker;
        let row = RowChange::written(self.existing_marker, marker);
        let inventory = self.inventory_change();
        if marker {
            let update = UsageCounterUpdate::for_group(group_id, group_delta)
                .in_bucket(&self.manifest.bucket, row)
                .with_inventory(inventory);
            if update.is_noop() {
                return self.commit_or_cleanup();
            }
            self.usage_update = Some(update);
            return self.start_usage_update();
        }
        if self.is_reference_item() {
            let update = UsageCounterUpdate::for_group(group_id, group_delta)
                .in_bucket(&self.manifest.bucket, row)
                .with_inventory(inventory);
            if self.manifest.reference_advance.is_some() && update.is_noop() {
                return self.commit_or_cleanup();
            }
            self.usage_update = Some(update);
            return self.start_usage_update();
        }
        let Some(blob) = self.manifest.blob.as_ref() else {
            return self.fail(IncomingVersionError::MissingBlobInfo);
        };
        let update = match self.received_blob.as_ref() {
            None => UsageCounterUpdate::for_group(group_id, group_delta),
            Some(received) => match StoredDelta::for_location(&received.location, true) {
                Some(stored) => UsageCounterUpdate::with_stored(group_id, group_delta, stored),
                None => return self.fail(IncomingVersionError::MissingBlobInfo),
            },
        };
        self.usage_update = Some(
            update
                .in_bucket(&self.manifest.bucket, row)
                .with_inventory(inventory),
        );
        let quota_bytes = (blob.size.saturating_add(self.envelope_bytes))
            .saturating_sub(self.replaced_logical_bytes);
        if let Some(ceiling) = self.quota_ceiling
            && quota_bytes > 0
        {
            let mut gate = QuotaGate::new_for_realm(
                ceiling,
                quota_bytes,
                group_id,
                self.local_node_id,
                self.local_realm_id,
            );
            let call = gate.start_calls();
            self.quota_gate = Some(gate);
            self.call(IncomingVersionState::CheckCommitQuota, call)
        } else {
            self.start_usage_update()
        }
    }

    pub(super) fn start_usage_update(&mut self) -> Turn {
        match self.usage_update.as_mut() {
            Some(update) => {
                let call = update.start_calls(self.ids.mint(self.now_ms)).into_call();
                self.call(IncomingVersionState::UpdateUsage, call)
            }
            None => self.fail(ReplicationError::ReplicationFailed.into()),
        }
    }

    pub(super) fn accept_reservation_release(&mut self, event: Event) -> Turn {
        let received = event.label();
        let Ok(BlobEvent::ReservationReleased { id }) = event.into_blob() else {
            return self.invalid("Event::Blob(BlobEvent::ReservationReleased)", received);
        };
        if self.release_id != Some(id) {
            let received = Event::blob(BlobEvent::ReservationReleased { id }).label();
            return self.invalid("matching reservation id", received);
        }
        self.release_id = None;
        self.call(
            IncomingVersionState::ScheduleUsage,
            schedule_snapshot_publish().into(),
        )
    }

    pub(super) fn register_blob_dht(&mut self) -> Turn {
        // Distributed buckets are found through their content indexes, not the DHT.
        if self.is_reference_item() || self.destination_bound {
            return self.send_apply_complete();
        }
        let Ok(location) = self.effective_materialized_location() else {
            return self.send_apply_complete();
        };
        let Some(blake3_hash) = location.get_blake3() else {
            return self.send_apply_complete();
        };

        let effect =
            match dht_registration_effect(blake3_hash, self.local_realm_id, &self.rocrate_limits) {
                Ok(effect) => effect,
                Err(_) => return self.send_apply_complete(),
            };
        self.call(IncomingVersionState::RegisterBlobDht, effect.into())
    }

    pub(super) fn finish_live_drain(&mut self) -> Turn {
        match self.manifest.kind {
            ReplicationItemKind::Materialized if self.pending_replica() => {
                self.schedule_promotion()
            }
            ReplicationItemKind::Materialized => self.register_blob_dht(),
            ReplicationItemKind::DeleteMarker => self.send_apply_complete(),
        }
    }

    /// A pending replica registers nothing here; its promotion does once the key is usable.
    pub(super) fn schedule_promotion(&mut self) -> Turn {
        let Some(key) = self.seal_plan.map(|plan| plan.key) else {
            return self.send_apply_complete();
        };
        let wake = TaskEffect::ShortenTimer {
            key: TaskKey::PromotePending {
                bucket_id: key.bucket_id,
                generation: key.generation,
            },
            after: std::time::Duration::ZERO,
        };
        self.call(
            IncomingVersionState::SchedulePromotion,
            Effect::Task(wake).into(),
        )
    }

    pub(super) fn accept_promotion_scheduled(&mut self, event: Event) -> Turn {
        if !matches!(event, Event::Task(_)) {
            warn!(event = ?event, "Incoming replication committed but promotion scheduling returned an unexpected event");
        }
        self.send_apply_complete()
    }

    pub(super) fn send_apply_complete(&mut self) -> Turn {
        let payload = match VersionReplicationMessage::VersionApplyComplete.to_bytes() {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };
        let stream_id = self.stream_id;
        self.blob(
            IncomingVersionState::SendApplyComplete,
            BlobEffect::SendMessage { stream_id, payload },
        )
    }

    pub(super) fn close_connection(&mut self) -> Turn {
        let stream_id = self.stream_id;
        self.blob(
            IncomingVersionState::CloseConnection,
            BlobEffect::CloseConnection { stream_id },
        )
    }

    pub(super) fn accept_backend_fence(&mut self, value: Option<Value>) -> Turn {
        let record = (value.as_deref()).map(GroupStorage::from_bytes).transpose();
        let record = record.map_err(RecordReadError::Conversion);
        match record {
            Ok(Some(record)) if !record.disabled => self.verify_existing_blob(),
            Ok(_) => self.fail(BackendFenceError::Unavailable.into()),
            Err(error) => self.fail(BackendFenceError::Read(error).into()),
        }
    }

    pub(super) fn accept_verified_blob(&mut self, value: Option<Value>) -> Turn {
        let stored = match value
            .map(|value| BackendLocation::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(stored) => stored,
            Err(error) => return self.fail(error.into()),
        };
        if stored != self.existing_blob_location {
            return self.fail(IncomingVersionError::ExistingBlobChanged);
        }
        self.write_blob_location()
    }

    pub(super) fn accept_object_lookup(&mut self, value: Option<Value>) -> Turn {
        let existing_pointer = value
            .as_ref()
            .and_then(|value| CurrentVersionPointer::from_bytes(value.as_ref()).ok());
        let incoming_generation = self.manifest.current_version_generation;
        let pointer_will_update = incoming_generation.is_some_and(|generation| {
            CurrentVersionPointer::new_with_generation(self.manifest.version_id, generation)
                .with_epoch(self.original_epoch())
                .supersedes(existing_pointer.as_ref())
        });
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            existing_generation = existing_pointer.as_ref().map(|pointer| pointer.generation),
            existing_version_id = ?existing_pointer.as_ref().map(|pointer| pointer.version_id),
            incoming_generation = ?incoming_generation,
            pointer_will_update,
            "Compared destination current version pointer"
        );
        self.write_compared_object(value.as_deref())
    }

    pub(super) fn accept_current_version(&mut self, value: Option<Value>) -> Turn {
        let Some(value) = value else {
            return self.fail(if self.manifest.reference_advance.is_some() {
                IncomingVersionError::InvalidReferenceAdvance
            } else {
                IncomingVersionError::CurrentVersionMissing
            });
        };
        let version = match BlobVersion::from_bytes(value.as_ref()) {
            Ok(version) => version,
            Err(error) => return self.fail(error.into()),
        };
        if let Err(error) = self.validate_advance(&version) {
            return self.fail(error);
        }
        // A new head releases an offered directory observation. A replaced row is
        // already debited, so the same version is not released twice.
        let replaced = self
            .existing_current_pointer
            .as_ref()
            .is_some_and(|pointer| pointer.version_id == self.manifest.version_id);
        if !replaced {
            let released = version.offered_size(&self.manifest.bucket, &self.manifest.key);
            self.replaced_reference_bytes = self
                .replaced_reference_bytes
                .saturating_add(released.unwrap_or(0));
        }
        self.apply_liveness(!version.is_deleted())
    }

    pub(super) fn accept_commit_quota(&mut self, result: IoResult) -> Turn {
        let Some(gate) = self.quota_gate.as_mut() else {
            return self.fail(ReplicationError::ReplicationFailed.into());
        };
        match gate.step_calls(result) {
            Ok(Some(call)) => Ok(Next::Call(call)),
            Ok(None) if gate.is_exceeded() => self.fail(IncomingVersionError::QuotaExceeded),
            Ok(None) => self.start_usage_update(),
            Err(error) => self.fail(error.into()),
        }
    }

    pub(super) fn accept_usage_update(&mut self, result: IoResult) -> Turn {
        let Some(update) = self.usage_update.as_mut() else {
            return self.fail(ReplicationError::ReplicationFailed.into());
        };
        match update.step_calls(result) {
            Ok(Some(call)) => Ok(Next::Call(call.into_call())),
            Ok(None) => self.commit_or_cleanup(),
            Err(error) => self.fail(error.into()),
        }
    }

    pub(super) fn accept_usage_schedule(&mut self, event: Event) -> Turn {
        if !matches!(
            event,
            Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. })
        ) {
            warn!(event = ?event, "Incoming replication committed but usage scheduling returned an unexpected event");
        }
        self.call(
            IncomingVersionState::ScheduleLiveDrain,
            schedule_blob_drain().into(),
        )
    }

    pub(super) fn accept_live_drain(&mut self, event: Event) -> Turn {
        if !matches!(
            event,
            Event::Task(TaskEvent::TimerScheduled { .. } | TaskEvent::Error { .. })
        ) {
            warn!(event = ?event, "Incoming replication committed but drain scheduling returned an unexpected event");
        }
        self.finish_live_drain()
    }

    pub(super) fn accept_blob_registration(&mut self, event: Event) -> Turn {
        match event {
            Event::Net(NetEvent::Dht(DhtEvent::PutComplete { .. }))
            | Event::Net(NetEvent::Dht(DhtEvent::Error { .. }))
            | Event::Net(NetEvent::Error(_)) => self.send_apply_complete(),
            _ => self.invalid("Event::Net(NetEvent::Dht(DhtEvent::*))", event.label()),
        }
    }

    pub(super) fn accept_completion_sent(&mut self, event: Event) -> Turn {
        let received = event.label();
        let Ok(BlobEvent::MessageSent { .. }) = event.into_blob() else {
            return self.invalid("Event::Blob(BlobEvent::MessageSent)", received);
        };
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            "Sent incoming replication apply-complete acknowledgement"
        );
        self.close_connection()
    }

    pub(super) fn accept_connection_close(&mut self, event: Event) -> Turn {
        let received = event.label();
        let Ok(BlobEvent::ConnectionClosed { .. }) = event.into_blob() else {
            return self.invalid("Event::Blob(BlobEvent::ConnectionClosed)", received);
        };
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            "Closed incoming replication connection"
        );
        let output = self.output.take();
        Ok(Next::Done(
            output.unwrap_or_else(|| self.result(self.apply_committed)),
        ))
    }
}
