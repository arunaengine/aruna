//! Publishes a composed multipart object in one write transaction: version, head, records and
//! counters commit together, or none of them do.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

impl CompleteUploadOperation {
    pub(super) fn read_publication(&mut self) -> Turn {
        (self.dedup, self.dedup_checked) = (None, false);
        let reads = publication_rows(&self.input.bucket, self.authority.as_ref());
        self.call(CompleteUploadState::CheckPurgeFinalize, IoCall::Read(reads))
    }

    /// The version snapshots the default this transaction observes, not one read while the
    /// parts were still being uploaded.
    pub(super) fn publication_checked(&mut self, values: Vec<Option<Value>>) -> Turn {
        let (bucket, key) = (&self.input.bucket, &self.input.key);
        check_published(values, bucket, key, self.authority.as_ref())?;
        let reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), bucket.as_bytes().into()),
            (
                NODE_SUBJECT_KEYSPACE.to_string(),
                Key::from(NODE_SUBJECT_KEY.to_vec()),
            ),
        ];
        self.call(CompleteUploadState::ReadBucketDefault, IoCall::Read(reads))
    }

    pub(super) fn handle_default_read(&mut self, values: Vec<Option<Value>>) -> Turn {
        let (bucket, subject) = split_drift_reads(keyed(&values))?;
        // The refs the version commits must be the refs the gate admitted: a
        // default or subject changed during the compose was never evaluated.
        let observed = GatedBucket::observe(bucket.as_ref());
        if let Some(gated) = self.gated_bucket.as_ref() {
            if !gated.matches(&observed) {
                return Err(PolicyGateError::Drift.into());
            }
            gated.check_subject(subject.as_ref())?;
        }
        self.bucket_policies = observed.policies;

        let location = self.composed_location.as_ref();
        let location = location.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        // A copy encoded under an older setting is never published: the bucket's
        // migration may already have passed this key. A sealed copy checks its plan instead.
        if location.format.bucket_key().is_none()
            && bucket.as_ref().is_some_and(|bucket| {
                EncodingClass::from(bucket.compression) != location.format.encoding().setting()
            })
        {
            return Err(StorageError::TransactionConflict.into());
        }
        let settings = read_row(
            BUCKET_ENCRYPTION_KEYSPACE,
            self.input.bucket.as_bytes().to_vec(),
        );
        self.call(CompleteUploadState::CheckSealSettings, settings)
    }

    /// A rotation or mode change since the upload started never publishes its old plan, and a
    /// plain upload never publishes into a bucket that encrypts now.
    pub(super) fn seal_settings_read(&mut self, value: Option<Value>) -> Turn {
        let plan = self.plan();
        let location = self.composed_location.clone();
        let location = location.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let settings = BucketEncryption::from_row(value.as_deref())?;
        storage_current(plan.as_ref(), &settings)?;
        let Some(plan) = plan else {
            return self.fence_composed(&location);
        };
        let mut reads = vec![(
            ABE_PENDING_KEYSPACE.to_string(),
            self.input.upload_id.to_bytes().to_vec().into(),
        )];
        reads.extend(abe_reads(plan.key));
        self.call(CompleteUploadState::FenceAbe, IoCall::Read(reads))
    }

    /// The pending envelope must match the parameters and epoch this transaction reads.
    pub(super) fn abe_fenced(&mut self, values: Vec<Option<Value>>) -> Turn {
        let location = self.composed_location.clone();
        let location = location.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let envelope = pending_envelope(self.plan(), &keyed(&values), &self.input);
        self.envelope = envelope.map_err(BlobError::from)?;
        self.fence_composed(&location)
    }

    /// The compose already ran on the pinned backend, so the finalize must prove it is still
    /// enabled or roll the composed object back.
    fn fence_composed(&mut self, location: &BackendLocation) -> Turn {
        let BackendRef::Group(backend_id) = &location.backend else {
            return self.check_hash_lookup();
        };
        let read = read_row(STORAGE_BACKEND_KEYSPACE, backend_key(*backend_id));
        self.call(CompleteUploadState::FenceBackend, read)
    }

    pub(super) fn backend_fenced(&mut self, value: Option<Value>) -> Turn {
        let record = (value.as_deref())
            .map(GroupStorage::from_bytes)
            .transpose()
            .map_err(|error| BackendFenceError::Read(RecordReadError::Conversion(error)))?;
        match record {
            Some(record) if !record.disabled => self.check_hash_lookup(),
            _ => Err(BackendFenceError::Unavailable.into()),
        }
    }

    pub(super) fn check_hash_lookup(&mut self) -> Turn {
        if let Some(turn) = self.read_current() {
            return turn;
        }
        let location = self.composed_location.clone();
        let location = location.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let Some(blake3_hash) = location.get_blake3() else {
            return self.write_pending_location(location);
        };
        // Only the copy on the upload's pinned backend may be deduplicated.
        let key = BlobLocationKey::from_blake3(
            blake3_hash,
            location.format.encoding(),
            location.backend.clone(),
        )?;
        let read = read_row(BLOB_LOCATIONS_KEYSPACE, key.to_bytes());
        self.call(CompleteUploadState::CheckHashLookup, read)
    }

    /// A sealed archive whose trees did not line up publishes without a content hash: its
    /// location waits in `pending_locations` until a verified read records the hash.
    fn write_pending_location(&mut self, location: BackendLocation) -> Turn {
        if location.format.bucket_key().is_none() {
            return Err(CompleteUploadError::MissingExpectedChecksum("blake3"));
        }
        let row = (
            PENDING_LOCATION_KEYSPACE.to_string(),
            ArchiveKey::of(&location).to_bytes().into(),
            location.to_bytes()?.into(),
        );
        self.new_blob = true;
        self.final_location = Some(location);
        self.call(
            CompleteUploadState::WriteBlobLocation,
            IoCall::Write(vec![row]),
        )
    }

    pub(super) fn hash_checked(&mut self, value: Option<Value>) -> Turn {
        let composed = self.composed_location.clone();
        let composed = composed.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        self.final_location = match value {
            Some(value) => Some(BackendLocation::from_bytes(value.as_ref())?),
            None => {
                self.new_blob = true;
                Some(composed)
            }
        };
        self.write_blob_location()
    }

    /// Keyed by the location's own backend, so a rewrite of identical content only ever
    /// replaces the copy on the backend the write resolved to.
    pub(super) fn write_blob_location(&mut self) -> Turn {
        let location = self.final_location.clone();
        let location = location.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let blake3_hash = location.get_blake3();
        let blake3_hash =
            blake3_hash.ok_or(CompleteUploadError::MissingExpectedChecksum("blake3"))?;
        let blake3_hash: [u8; 32] = blake3_hash
            .try_into()
            .map_err(|error: std::array::TryFromSliceError| ConversionError::from(error))?;
        let key = BlobLocationKey::new(
            blake3_hash,
            location.format.encoding(),
            location.backend.clone(),
        );
        let row = (
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            key.to_bytes().into(),
            location.to_bytes()?.into(),
        );
        self.call(
            CompleteUploadState::WriteBlobLocation,
            IoCall::Write(vec![row]),
        )
    }

    pub(super) fn read_object_lookup(&mut self) -> Turn {
        let key = BlobHeadKey::new(&self.input.bucket, &self.input.key).to_bytes()?;
        let read = read_row(BLOB_HEAD_KEYSPACE, key);
        self.call(CompleteUploadState::ReadObjectLookup, read)
    }

    pub(super) fn object_lookup_read(&mut self, value: Option<Value>) -> Turn {
        let existing = (value.as_ref())
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()?;
        self.existing_pointer = existing.clone();
        if let Some(pointer) = existing.as_ref() {
            let key = VersionKey::new(&self.input.bucket, &self.input.key, pointer.version_id);
            let read = read_row(BLOB_VERSIONS_KEYSPACE, key.to_bytes()?);
            return self.call(CompleteUploadState::ReadLivenessVersion, read);
        }
        self.write_current_lookup(None)
    }

    pub(super) fn liveness_read(&mut self, value: Option<Value>) -> Turn {
        let head = value.and_then(|value| BlobVersion::from_bytes(value.as_ref()).ok());
        self.was_live = head.as_ref().is_some_and(|version| !version.is_deleted());
        self.released_bytes = head
            .and_then(|version| version.offered_size(&self.input.bucket, &self.input.key))
            .unwrap_or(0);
        let existing = self.existing_pointer.clone();
        self.write_current_lookup(existing.as_ref())
    }

    pub(super) fn write_current_lookup(
        &mut self,
        existing: Option<&CurrentVersionPointer>,
    ) -> Turn {
        let set_hint = self.set_hint;
        let version_id = *self
            .version_id
            .get_or_insert_with(|| mint_version(set_hint));
        // An import may register a listed object's version before the finalize commits it; an
        // upload opened at a provider path stays one after its rule was removed.
        if self.plain.as_ref().is_some_and(|target| target.listed) || self.plain_upload().is_some()
        {
            return self.read_registered(version_id);
        }
        self.write_head(existing, version_id)
    }

    pub(super) fn write_head(
        &mut self,
        existing: Option<&CurrentVersionPointer>,
        version_id: Ulid,
    ) -> Turn {
        let pointer = CurrentVersionPointer::next_for(existing, version_id)?;
        let pointer = stamp_head(pointer, self.binding.as_ref(), self.version_epoch);
        let alias_context = self.alias_context()?;
        let row = (
            BLOB_HEAD_KEYSPACE.to_string(),
            alias_context.head_key().to_bytes()?.into(),
            pointer.to_bytes()?.into(),
        );
        self.head_pointer = Some(pointer);
        self.call(CompleteUploadState::WriteBlobHead, IoCall::Write(vec![row]))
    }

    pub(super) fn write_path_index(&mut self) -> Turn {
        let location = self.final_location.clone();
        let location = location.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        // A pending archive has no content hash to index yet.
        let Some(blake3_hash) = location.get_blake3() else {
            return self.write_version();
        };
        let blake3_hash: [u8; 32] = blake3_hash
            .try_into()
            .map_err(|error: std::array::TryFromSliceError| ConversionError::from(error))?;
        let version_id = self.version_id;
        let version_id = version_id.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let alias_context = self.alias_context()?;
        let key = alias_context.path_index_key(blake3_hash, version_id);
        let row = (
            PATHS_INDEX_KEYSPACE.to_string(),
            key.to_bytes()?.into(),
            Value::from(Vec::new()),
        );
        self.call(
            CompleteUploadState::WritePathIndex,
            IoCall::Write(vec![row]),
        )
    }

    pub(super) fn write_version(&mut self) -> Turn {
        let location = self.final_location.clone();
        let location = location.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let version_id = self.version_id;
        let version_id = version_id.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let created_at = *self.version_created_at.get_or_insert_with(SystemTime::now);
        let upload_record = self.upload_record.as_ref();
        let upload_record = upload_record.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let group_id = upload_record.group_id;
        let version = match location.get_blake3().map(<[u8; 32]>::try_from) {
            Some(Ok(hash)) => BlobVersion::materialized(
                hash,
                location.backend.clone(),
                location.format.encoding(),
                created_at,
                self.input.created_by,
                None,
            ),
            Some(Err(err)) => return Err(ConversionError::from(err).into()),
            None => BlobVersion::pending(
                ArchiveKey::of(&location),
                created_at,
                self.input.created_by,
                None,
            ),
        }
        .with_metadata(upload_record.metadata.clone())
        .with_origin(Some(self.version_origin.clone().unwrap_or(
            VersionOrigin::Upload {
                node_id: self.input.node_id,
                device: None,
                client: None,
            },
        )));
        // Union with what part copies inherited: a part-wise copy of a governed
        // source can only add refs to the composed object.
        let mut policies = self.bucket_policies.clone();
        policies.extend(upload_record.placement_policies.iter().copied());
        let version = version.with_policies(policies)?;
        let version_key = VersionKey::new(&self.input.bucket, &self.input.key, version_id);
        let generation = self.head_pointer.as_ref().map_or(0, |head| head.generation);
        let version = stamp_version(
            version,
            self.binding.as_ref(),
            generation,
            self.version_epoch,
        );
        self.stored_policies = version.placement_policies.clone();
        self.version_epoch = version.original_epoch;
        let uploaded = SourceEvent::data_uploaded(
            (self.input.realm_id, group_id, self.input.node_id),
            (&self.input.bucket, &self.input.key),
            version_id,
            unix_timestamp_millis(),
        );
        let mut rows = vec![(
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes()?.into(),
            version.to_bytes()?.into(),
        )];
        rows.extend(journal_entries(&uploaded)?);
        self.call(CompleteUploadState::WriteVersionRecord, IoCall::Write(rows))
    }

    pub(super) fn version_written(&mut self) -> Turn {
        let Some(envelope) = self.envelope.take() else {
            return self.write_owner();
        };
        let (Some(version_id), Some(location), Some(upload)) = (
            self.version_id,
            self.final_location.as_ref(),
            self.upload_record.as_ref(),
        ) else {
            return Err(CompleteUploadError::CompleteUploadFailed);
        };
        let version = VersionKey::new(&self.input.bucket, &self.input.key, version_id);
        let limit = self.rocrate_limits.metadata_bytes;
        let (rows, charge) = located_rows(&envelope, &version, location, (&upload.metadata, limit))
            .map_err(BlobError::from)?;
        self.envelope_bytes = charge;
        self.call(CompleteUploadState::WriteEnvelope, IoCall::Write(rows))
    }

    /// The archive's owner row commits with the version and its first physical credit.
    pub(super) fn write_owner(&mut self) -> Turn {
        let (Some(version_id), Some(location)) = (self.version_id, self.final_location.as_ref())
        else {
            return Err(CompleteUploadError::CompleteUploadFailed);
        };
        if location.format.bucket_key().is_none() {
            return self.register_managed_copy();
        }
        let version = VersionKey::new(&self.input.bucket, &self.input.key, version_id);
        let owner = CopyOwner::new(ArchiveKey::of(location), version);
        let row = (
            COPY_OWNER_KEYSPACE.to_string(),
            owner.key()?.into(),
            Value::from(Vec::new()),
        );
        self.call(
            CompleteUploadState::WriteCopyOwner,
            IoCall::Write(vec![row]),
        )
    }

    /// Joins the finalize transaction, so the composed copy becomes serveable
    /// exactly when the logical version does and never before.
    pub(super) fn register_managed_copy(&mut self) -> Turn {
        let (Some(version_id), Some(location)) = (self.version_id, self.final_location.clone())
        else {
            return Err(CompleteUploadError::CompleteUploadFailed);
        };
        let row = register_entry(CopyRegistration {
            version: VersionKey::new(&self.input.bucket, &self.input.key, version_id),
            node_id: self.input.node_id,
            location: &location,
            policies: &self.stored_policies,
            origin: CopyOrigin::Write,
            subject_generation: self.stored_subject(),
            registered_at_ms: version_id.timestamp_ms(),
        })?;
        self.call(
            CompleteUploadState::RegisterManagedCopy,
            IoCall::Write(vec![row]),
        )
    }

    pub(super) fn write_object_metadata(&mut self) -> Turn {
        let version_id = self.version_id;
        let version_id = version_id.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let summary = MultipartObjectSummary {
            checksum_type: self.input.checksum_type,
            part_count: self.resolved_parts.len(),
            composite_hashes: self.composite_hashes.clone(),
        };
        let mut writes = Vec::with_capacity(self.resolved_parts.len() + 1);
        writes.push((
            OBJECT_METADATA_KEYSPACE.to_string(),
            MultipartObjectKey::summary(version_id).to_bytes()?.into(),
            summary.to_bytes()?.into(),
        ));
        for record in &self.resolved_parts {
            let object_part = MultipartObjectPart {
                part_number: record.part_number,
                size: record.location.blob_size,
                hashes: record.location.hashes.clone(),
            };
            let key = MultipartObjectKey::part(version_id, record.part_number).to_bytes()?;
            writes.push((
                OBJECT_METADATA_KEYSPACE.to_string(),
                key.into(),
                object_part.to_bytes()?.into(),
            ));
        }
        self.call(
            CompleteUploadState::WriteObjectMetadata,
            IoCall::Write(writes),
        )
    }

    pub(super) fn delete_upload_records(&mut self) -> Turn {
        let read = read_row(UPLOAD_KEYSPACE, self.input.upload_id.to_bytes().to_vec());
        self.call(CompleteUploadState::ReadUploadRecord, read)
    }

    /// A concurrent delete of the record conflicts with this read, so a record is counted
    /// closed once.
    pub(super) fn upload_record_read(&mut self, value: Option<Value>) -> Turn {
        if value.is_none() {
            return self.delete_records();
        }
        let count = upload_count(&self.input.bucket, -1);
        self.call(CompleteUploadState::CountUpload, count)
    }

    pub(super) fn delete_records(&mut self) -> Turn {
        let deletes = record_rows(self.input.upload_id, &self.upload_parts)?;
        self.call(
            CompleteUploadState::DeleteUploadRecords,
            IoCall::Delete(deletes),
        )
    }

    pub(super) fn records_deleted(&mut self) -> Turn {
        if let Some(turn) = self.plain_fence() {
            return turn;
        }
        self.write_cleanup_records(true)
    }

    /// Deferred housekeeping commits atomically with the completed upload, so a crash after
    /// commit can never leak part blobs.
    pub(super) fn write_cleanup_records(&mut self, rule_exists: bool) -> Turn {
        let mut works: Vec<BlobCleanupWork> = (self.upload_parts.iter())
            .map(|record| BlobCleanupWork::DeleteBlob {
                location: record.location.clone(),
            })
            .collect();
        if let (Some(composed), Some(chosen)) = (
            self.composed_location.as_ref(),
            self.final_location.as_ref(),
        ) {
            // A linked file that gave way goes only through its exact release.
            if self.dedup.is_some() {
            } else if composed != chosen {
                works.push(BlobCleanupWork::DeleteBlob {
                    location: composed.clone(),
                });
            } else if let Some(blake3) = composed.get_blake3()
                && let Ok(blake3) = blake3.try_into()
            {
                works.push(BlobCleanupWork::ReconcileWrite {
                    location: composed.clone(),
                    owner: WriteOwner::Blob {
                        blake3,
                        realm_id: self.input.realm_id,
                        ttl_ms: self.rocrate_limits.holder_ttl_ms,
                        announce: self.binding.is_none(),
                    },
                });
            }
        }
        if let Some(blake3) =
            (self.final_location.as_ref()).and_then(|location| location.get_blake3())
            && self.composed_location.as_ref() != self.final_location.as_ref()
            && self.binding.is_none()
            && let Ok(blake3) = blake3.try_into()
        {
            works.push(BlobCleanupWork::RegisterDht {
                blake3,
                realm_id: self.input.realm_id,
                ttl_ms: self.rocrate_limits.holder_ttl_ms,
            });
        }

        let mut writes = self.plain_records(rule_exists)?;
        for work in works {
            let key = cleanup_key(&work);
            writes.push((
                BLOB_CLEANUP_KEYSPACE.to_string(),
                key,
                work.to_bytes()?.into(),
            ));
        }
        self.call(
            CompleteUploadState::WriteCleanupRecords,
            IoCall::Write(writes),
        )
    }

    pub(super) fn cleanup_written(&mut self) -> Turn {
        if let Some(turn) = self.retire_intent() {
            return turn;
        }
        self.write_obligation()
    }

    pub(super) fn write_obligation(&mut self) -> Turn {
        // A registered version keeps its obligation and charges; only the upload is cleared.
        if self.registered {
            let Some(binding) = self.binding.as_ref() else {
                return self.finish_finalize();
            };
            let mut change = InventoryChange::new(binding.set_id, self.input.key.clone());
            change.upload = Some((self.input.upload_id, false));
            let close = self.close.insert(InventoryUpdate::new(change));
            return match close.start_calls() {
                Some(call) => self.call(CompleteUploadState::CloseInventory, call.into_call()),
                None => self.finish_finalize(),
            };
        }
        let version_id = self.version_id;
        let version_id = version_id.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let record = LiveObligationRecord::new(
            self.input.node_id,
            AuthContext {
                user_id: self.input.created_by,
                realm_id: self.input.realm_id,
                path_restrictions: self.restrictions.clone(),
                session: None,
            },
            self.input.bucket.clone(),
            self.input.key.clone(),
            version_id,
            false,
        );
        let row = live_obligation_entry(&record)?;
        let call = IoCall::Write(vec![row]);
        self.call(CompleteUploadState::WriteReplicationObligation, call)
    }

    pub(super) fn handle_inventory_closed(&mut self, result: IoResult) -> Turn {
        let close = self.close.as_mut();
        let close = close.ok_or(CompleteUploadError::InvalidOperationState)?;
        match close.step_calls(result).map_err(UsageUpdateError::from)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.finish_finalize(),
        }
    }

    /// The composed version, the head it becomes and the upload it closes.
    fn inventory_change(&self, size: i128) -> Option<InventoryChange> {
        // Without a hash the composed version is pending content.
        let identity = (self.final_location.as_ref())
            .and_then(|location| location.get_blake3())
            .and_then(|hash| <[u8; 32]>::try_from(hash).ok());
        let added = AddedVersion {
            version_id: self.version_id?,
            epoch: self.version_epoch,
            generation: self.head_pointer.as_ref().map_or(0, |head| head.generation),
            entry: VersionEntry {
                marker: false,
                size: u64::try_from(size).unwrap_or_default(),
                referenced: false,
                identity,
                pending: identity.is_none(),
                searchable: (self.upload_record.as_ref())
                    .is_some_and(|upload| searchable(&upload.metadata)),
            },
        };
        let head = self.head_pointer.is_some();
        let mut change = version_written(self.binding.as_ref(), &self.input.key, added, head)?;
        change.upload = Some((self.input.upload_id, false));
        Some(change)
    }

    pub(super) fn obligation_written(&mut self) -> Turn {
        let group_id = self.upload_record.as_ref().map(|record| record.group_id);
        let group_id = group_id.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let location = self.final_location.as_ref();
        let location = location.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let size = i128::from(location.blob_size);
        let group_delta = UsageDelta {
            objects: if self.was_live { 0 } else { 1 },
            logical_bytes: size + i128::from(self.envelope_bytes),
            referenced_bytes: -i128::from(self.released_bytes),
            ..Default::default()
        };
        let stored = StoredDelta::for_location(location, self.new_blob);
        let stored = stored.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        // Enforce the hard group quota before the counters commit. Only a positive
        // logical-bytes delta can push a group over its ceiling.
        let object_size = location.blob_size.saturating_add(self.envelope_bytes);
        let inventory = self.inventory_change(size);
        self.usage_update = Some(
            UsageCounterUpdate::with_stored(group_id, group_delta, stored)
                .in_bucket(&self.input.bucket, RowChange::AddedVersion)
                .with_inventory(inventory),
        );
        let Some(ceiling) = self.input.quota_ceiling.filter(|_| object_size > 0) else {
            return self.start_usage_update();
        };
        let mut gate = QuotaGate::new_for_realm(
            ceiling,
            object_size,
            group_id,
            self.input.node_id,
            self.input.realm_id,
        );
        let call = gate.start_calls();
        self.quota_gate = Some(gate);
        self.call(CompleteUploadState::EnforceQuota, call)
    }

    fn start_usage_update(&mut self) -> Turn {
        let update = self.usage_update.as_mut();
        let update = update.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let call = update.start_calls().into_call();
        self.call(CompleteUploadState::UpdateUsage, call)
    }

    pub(super) fn handle_enforce_quota(&mut self, result: IoResult) -> Turn {
        let gate = self.quota_gate.as_mut();
        let gate = gate.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        if let Some(call) = gate.step_calls(result)? {
            return Ok(Next::Call(call));
        }
        if gate.is_rebuilding() {
            return Err(CompleteUploadError::UsageRebuilding);
        }
        if gate.is_exceeded() {
            let (limit, usage) = (gate.ceiling(), gate.projected_usage());
            return Err(CompleteUploadError::QuotaExceeded { limit, usage });
        }
        self.start_usage_update()
    }

    pub(super) fn handle_usage_update(&mut self, result: IoResult) -> Turn {
        let update = self.usage_update.as_mut();
        let update = update.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        match update.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.finish_finalize(),
        }
    }

    /// The finalize's result; the transaction commits with it.
    fn finish_finalize(&mut self) -> Turn {
        let location = self.final_location.clone();
        let location = location.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let version_id = self.version_id;
        let version_id = version_id.ok_or(CompleteUploadError::CompleteUploadFailed)?;
        let response_hashes = match self.input.checksum_type {
            MultipartChecksumType::FullObject => location.hashes.clone(),
            MultipartChecksumType::Composite => self.composite_hashes.clone(),
        };
        Ok(Next::Done(CompleteUploadResult {
            location,
            version_id,
            checksum_type: self.input.checksum_type,
            response_hashes,
            part_count: self.resolved_parts.len(),
        }))
    }
}
