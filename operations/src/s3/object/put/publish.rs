//! Publishes written bytes as a version in one write transaction: location, head, version,
//! registration and counters commit together, or none of them do.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::structs::storage::blob::BackendRef;

impl PutObjectOperation {
    pub(super) fn read_publication(&mut self) -> Turn {
        let reads = publication_rows(&self.config.request.bucket, self.authority.as_ref());
        self.call(PutObjectState::CheckPurgeFence, IoCall::Read(reads))
    }

    /// The version snapshots the default this transaction observes, not one read before the
    /// bytes streamed.
    pub(super) fn publication_checked(&mut self, values: Vec<Option<Value>>) -> Turn {
        let request = &self.config.request;
        let authority = self.authority.as_ref();
        check_published(values, &request.bucket, &request.key, authority)?;
        let reads = vec![
            (
                S3_BUCKET_KEYSPACE.to_string(),
                request.bucket.as_bytes().into(),
            ),
            (
                NODE_SUBJECT_KEYSPACE.to_string(),
                Key::from(NODE_SUBJECT_KEY.to_vec()),
            ),
        ];
        self.call(PutObjectState::CheckBucket, IoCall::Read(reads))
    }

    pub(super) fn handle_bucket_checked(&mut self, values: Vec<Option<Value>>) -> Turn {
        let (current, subject) = split_drift_reads(abe::keyed(&values))?;
        if self.expected_bucket.is_some()
            && (current.as_ref().map(BucketInfo::identity)
                != self.expected_bucket.as_ref().map(BucketInfo::identity)
                || (current.as_ref()).is_none_or(|bucket| bucket.group_id != self.config.group_id))
        {
            return Err(StorageError::TransactionConflict.into());
        }
        // The refs the version commits must be the refs the gate admitted: a
        // default changed while the bytes streamed was never evaluated.
        let observed = GatedBucket::observe(current.as_ref());
        if let Some(gated) = self.gated_bucket.as_ref() {
            if !gated.matches(&observed) {
                return Err(PolicyGateError::Drift.into());
            }
            gated.check_subject(subject.as_ref())?;
        }
        // A copy encoded under an older setting is never published: the bucket's
        // migration may already have passed this key.
        let written =
            (self.get_written_location()).map(|location| location.format.encoding().setting());
        if self.seal_plan.is_none()
            && (current.as_ref())
                .zip(written)
                .is_some_and(|(bucket, written)| EncodingClass::from(bucket.compression) != written)
        {
            return Err(StorageError::TransactionConflict.into());
        }
        self.bucket_policies = observed.policies;
        let read = read_row(
            BUCKET_ENCRYPTION_KEYSPACE,
            self.config.request.bucket.as_bytes().to_vec(),
        );
        self.call(PutObjectState::CheckSealFence, read)
    }

    /// A copy sealed under an older plan, or plain bytes for a bucket that now encrypts, is
    /// never published.
    pub(super) fn seal_fence_checked(&mut self, value: Option<Value>) -> Turn {
        let settings = BucketEncryption::from_row(value.as_deref())?;
        let written = (self.get_written_location()).map(|location| location.format.bucket_key());
        let current = match self.seal_plan {
            Some(plan) if written != Some(Some(plan.key)) => Err(BucketKeyError::Unsupported),
            Some(plan) => plan.still_current(&settings),
            None if settings.is_encrypted() => Err(BucketKeyError::StaleGeneration {
                requested: 0,
                current: settings.key_generation,
            }),
            None => Ok(()),
        };
        current.map_err(seal_error)?;
        match self.envelope.is_some() {
            true => self.read_abe(true),
            false => self.start_fence(),
        }
    }

    /// The bytes already went to the resolved backend, so the version transaction must prove
    /// it is still enabled.
    pub(super) fn start_fence(&mut self) -> Turn {
        let location = self.get_written_location();
        let location = location.ok_or(PutObjectError::MissingOutput)?;
        let BackendRef::Group(backend_id) = &location.backend else {
            return self.start_hash_lookup();
        };
        let read = read_row(STORAGE_BACKEND_KEYSPACE, backend_key(*backend_id));
        self.call(PutObjectState::FenceBackend, read)
    }

    pub(super) fn handle_backend_fenced(&mut self, value: Option<Value>) -> Turn {
        let record = (value.as_deref())
            .map(GroupStorage::from_bytes)
            .transpose()
            .map_err(|error| BackendFenceError::Read(RecordReadError::Conversion(error)))?;
        match record {
            Some(record) if !record.disabled => self.start_hash_lookup(),
            _ => Err(BackendFenceError::Unavailable.into()),
        }
    }

    /// Looks up only the copy on the backend this write resolved to, so identical content on
    /// another backend never overrides the placement.
    pub(super) fn start_hash_lookup(&mut self) -> Turn {
        if let Some(turn) = self.read_current() {
            return turn;
        }
        let written = self.get_written_location();
        let written = written.ok_or(PutObjectError::MissingOutput)?;
        let blake3_hash = written.get_blake3();
        let blake3_hash = blake3_hash.ok_or(PutObjectError::MissingHash("blake3".to_string()))?;
        let key = BlobLocationKey::from_blake3(
            blake3_hash,
            written.format.encoding(),
            written.backend.clone(),
        )?;
        let read = read_row(BLOB_LOCATIONS_KEYSPACE, key.to_bytes());
        self.call(PutObjectState::CheckHashLookup, read)
    }

    pub(super) fn hash_checked(&mut self, value: Option<Value>) -> Turn {
        let written = self.get_written_location().cloned();
        let written = written.ok_or(PutObjectError::MissingOutput)?;
        match value {
            Some(value) => {
                let existing = BackendLocation::from_bytes(value.as_ref())?;
                if !self.adopted && existing != written {
                    self.cleanup_location = Some(written);
                }
                self.output = Some(existing);
            }
            None => {
                self.new_blob = true;
                self.output = Some(written);
            }
        }
        self.read_object_lookup()
    }

    /// The location row of the published bytes, written with the head or the path index, so
    /// a version an import registered meanwhile commits nothing.
    fn location_row(&self) -> Result<(String, Key, Value), PutObjectError> {
        let location = self.get_output().cloned();
        let location = location.ok_or(PutObjectError::MissingOutput)?;
        let blake3_hash = location.get_blake3();
        let blake3_hash = blake3_hash.ok_or(PutObjectError::MissingHash("blake3".to_string()))?;
        let blake3_hash = <[u8; 32]>::try_from(blake3_hash).map_err(ConversionError::from)?;
        let key = BlobLocationKey::new(
            blake3_hash,
            location.format.encoding(),
            location.backend.clone(),
        );
        Ok((
            BLOB_LOCATIONS_KEYSPACE.to_string(),
            key.to_bytes().into(),
            location.to_bytes()?.into(),
        ))
    }

    fn alias_context(&self) -> HeadAliasContext {
        HeadAliasContext::new(
            self.config.realm_id,
            self.config.group_id,
            self.config.node_id,
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
        )
    }

    pub(super) fn read_object_lookup(&mut self) -> Turn {
        let request = &self.config.request;
        let key = BlobHeadKey::new(request.bucket.clone(), request.key.clone()).to_bytes()?;
        let read = read_row(BLOB_HEAD_KEYSPACE, key);
        self.call(PutObjectState::ReadObjectLookup, read)
    }

    pub(super) fn object_lookup_read(&mut self, value: Option<Value>) -> Turn {
        let existing = (value.as_ref())
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()?;
        if let Some(expected) = self.expected_head
            && existing.as_ref().map(|pointer| pointer.version_id) != expected
        {
            return Err(PutObjectError::HeadChanged);
        }
        self.existing_pointer = existing.clone();
        if let Some(pointer) = existing.as_ref() {
            let key = self.version_key(pointer.version_id).to_bytes()?;
            let read = read_row(BLOB_VERSIONS_KEYSPACE, key);
            return self.call(PutObjectState::ReadLivenessVersion, read);
        }
        self.write_current_lookup(None)
    }

    pub(super) fn liveness_read(&mut self, value: Option<Value>) -> Turn {
        let head = value.and_then(|value| BlobVersion::from_bytes(value.as_ref()).ok());
        self.was_live = head.as_ref().is_some_and(|version| !version.is_deleted());
        let request = &self.config.request;
        // A history write leaves the head charged as it is.
        self.released_bytes = head
            .filter(|_| !self.keep_head)
            .and_then(|version| version.offered_size(&request.bucket, &request.key))
            .unwrap_or(0);
        if self.create_only && self.was_live {
            return Err(PutObjectError::ObjectExists);
        }
        let existing = self.existing_pointer.clone();
        self.write_current_lookup(existing.as_ref())
    }

    fn write_current_lookup(&mut self, existing: Option<&CurrentVersionPointer>) -> Turn {
        let set_hint = self.set_hint;
        let version_id = *self
            .version_id
            .get_or_insert_with(|| mint_version(set_hint));
        // An import may register a listed write's version before the write commits it.
        if self.keep_head || self.listed_write() {
            return self.read_history(version_id);
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
        let head = (
            BLOB_HEAD_KEYSPACE.to_string(),
            self.alias_context().head_key().to_bytes()?.into(),
            pointer.to_bytes()?.into(),
        );
        self.head_pointer = Some(pointer);
        let rows = vec![self.location_row()?, head];
        self.call(PutObjectState::WriteBlobHead, IoCall::Write(rows))
    }

    /// A concurrent registration of the same version makes this transaction conflict.
    fn read_history(&mut self, version_id: Ulid) -> Turn {
        let key = self.version_key(version_id).to_bytes()?;
        let read = read_row(BLOB_VERSIONS_KEYSPACE, key);
        self.call(PutObjectState::ReadHistory, read)
    }

    pub(super) fn history_read(&mut self, value: Option<Value>) -> Turn {
        let Some(value) = value else {
            if self.keep_head {
                return self.write_path_index();
            }
            if self.trail_scan.as_ref().is_none_or(|scan| scan.writer) {
                return self.writer_trail();
            }
            let (existing, version_id) = (self.existing_pointer.clone(), self.version_id);
            let version_id = version_id.ok_or(PutObjectError::PutObjectFailed)?;
            return self.write_head(existing.as_ref(), version_id);
        };
        // A listed write an import already registered with the same bytes is done: it is not
        // charged or moved again. Other content under its version id is a conflict.
        let stored = BlobVersion::from_bytes(value.as_ref()).ok();
        let hash = stored.as_ref().and_then(|version| version.blob_hash());
        let written = self.get_output().and_then(|location| location.get_blake3());
        if self.keep_head || hash.is_none() || hash.map(|hash| &hash[..]) != written {
            return Err(PutObjectError::VersionExists);
        }
        // The registered version stays as the import committed it; nothing is written.
        self.registered = true;
        self.finish_publish()
    }

    pub(super) fn write_path_index(&mut self) -> Turn {
        let location = self.get_output().cloned();
        let location = location.ok_or(PutObjectError::MissingOutput)?;
        let blake3_hash = location.get_blake3();
        let blake3_hash = blake3_hash.ok_or(PutObjectError::MissingHash("blake3".to_string()))?;
        let blake3_hash = <[u8; 32]>::try_from(blake3_hash).map_err(ConversionError::from)?;
        let version_id = self.version_id.ok_or(PutObjectError::PutObjectFailed)?;
        let index = self.alias_context().path_index_key(blake3_hash, version_id);
        let mut rows = Vec::with_capacity(2);
        // A history write moves no head, so its location row goes with the path index.
        if self.keep_head {
            rows.push(self.location_row()?);
        }
        rows.push((
            PATHS_INDEX_KEYSPACE.to_string(),
            index.to_bytes()?.into(),
            Value::from(Vec::new()),
        ));
        self.call(PutObjectState::WritePathIndex, IoCall::Write(rows))
    }

    pub(super) fn write_version(&mut self) -> Turn {
        let version_id = self.version_id.ok_or(PutObjectError::PutObjectFailed)?;
        let output = self.get_output().cloned();
        let output = output.ok_or(PutObjectError::MissingOutput)?;
        let blake3_hash = output.get_blake3();
        let blake3_hash = blake3_hash.ok_or(PutObjectError::MissingHash("blake3".to_string()))?;
        let blake3_hash = <[u8; 32]>::try_from(blake3_hash).map_err(ConversionError::from)?;
        let version_created_at = UNIX_EPOCH + Duration::from_millis(version_id.timestamp_ms());
        // A deduplicated location keeps its first writer; the version keeps its own.
        let created_by =
            (self.get_written_location()).map_or(output.created_by, |written| written.created_by);
        let version = BlobVersion::materialized(
            blake3_hash,
            output.backend.clone(),
            output.format.encoding(),
            version_created_at,
            created_by,
            self.config.version_source.clone(),
        )
        .with_metadata(self.metadata.clone())
        .with_origin(Some(self.version_origin.clone().unwrap_or(
            match self.config.version_source {
                Some(_) => VersionOrigin::Source { job_id: None },
                None => VersionOrigin::Upload {
                    node_id: self.config.node_id,
                    device: None,
                    client: None,
                },
            },
        )));
        let version = version.with_policies(self.effective_policies())?;
        let version_key = self.version_key(version_id);
        let generation = self.head_pointer.as_ref().map_or(0, |head| head.generation);
        let version = stamp_version(
            version,
            self.binding.as_ref(),
            generation,
            self.version_epoch,
        );
        self.stored_policies = version.placement_policies.clone();
        self.version_epoch = version.original_epoch;
        let mut rows = vec![(
            BLOB_VERSIONS_KEYSPACE.to_string(),
            version_key.to_bytes()?.into(),
            version.to_bytes()?.into(),
        )];
        // A tagged release crate commits its release row with the version.
        rows.extend(release_rows(&version_key, &version)?);
        let config = &self.config;
        let uploaded = SourceEvent::data_uploaded(
            (config.realm_id, config.group_id, config.node_id),
            (&config.request.bucket, &config.request.key),
            version_id,
            unix_timestamp_millis(),
        );
        rows.extend(journal_entries(&uploaded)?);
        self.call(PutObjectState::CreateVersionRecord, IoCall::Write(rows))
    }

    /// A Pithos archive records its owning version in the version transaction, so cleanup
    /// never frees an archive a committed version uses.
    pub(super) fn write_copy_owner(&mut self) -> Turn {
        let (Some(version_id), Some(location)) = (self.version_id, self.get_output()) else {
            return Err(PutObjectError::MissingOutput);
        };
        if location.format.bucket_key().is_none() {
            return self.register_managed_copy();
        }
        let owner = CopyOwner::new(ArchiveKey::of(location), self.version_key(version_id));
        let row = (
            COPY_OWNER_KEYSPACE.to_string(),
            owner.key()?.into(),
            Value::from(Vec::new()),
        );
        self.call(PutObjectState::WriteCopyOwner, IoCall::Write(vec![row]))
    }

    /// Joins the version transaction, so the copy becomes serveable exactly when the logical
    /// version does and never before.
    pub(super) fn register_managed_copy(&mut self) -> Turn {
        let version_id = self.version_id.ok_or(PutObjectError::PutObjectFailed)?;
        let location = self.get_output().cloned();
        let location = location.ok_or(PutObjectError::MissingOutput)?;
        let row = register_entry(CopyRegistration {
            version: self.version_key(version_id),
            node_id: self.config.node_id,
            location: &location,
            policies: &self.stored_policies,
            origin: self.origin,
            subject_generation: self.stored_subject(),
            registered_at_ms: version_id.timestamp_ms(),
        })?;
        self.call(
            PutObjectState::RegisterManagedCopy,
            IoCall::Write(vec![row]),
        )
    }

    pub(super) fn write_obligation(&mut self) -> Turn {
        let version_id = self.version_id.ok_or(PutObjectError::PutObjectFailed)?;
        let record = LiveObligationRecord::new(
            self.config.node_id,
            AuthContext {
                user_id: self.config.user_id,
                realm_id: self.config.realm_id,
                path_restrictions: self.restrictions.clone(),
                session: None,
            },
            self.config.request.bucket.clone(),
            self.config.request.key.clone(),
            version_id,
            false,
        );
        let row = live_obligation_entry(&record)?;
        let call = IoCall::Write(vec![row]);
        self.call(PutObjectState::WriteReplicationObligation, call)
    }

    pub(super) fn charge_usage(&mut self) -> Turn {
        let location = self.get_output().cloned();
        let location = location.ok_or(PutObjectError::MissingOutput)?;
        let size = i128::from(location.blob_size);
        let group_delta = UsageDelta {
            objects: if self.was_live || self.keep_head {
                0
            } else {
                1
            },
            logical_bytes: size + i128::from(self.envelope_bytes),
            referenced_bytes: -i128::from(self.released_bytes),
            ..Default::default()
        };
        let stored = StoredDelta::for_location(&location, self.new_blob);
        let stored = stored.ok_or(PutObjectError::MissingHash("blake3".to_string()))?;
        let added = self.version_id.map(|version_id| AddedVersion {
            version_id,
            epoch: self.version_epoch,
            generation: self.head_pointer.as_ref().map_or(0, |head| head.generation),
            entry: VersionEntry {
                marker: false,
                size: location.blob_size,
                referenced: false,
                identity: location
                    .get_blake3()
                    .and_then(|hash| <[u8; 32]>::try_from(hash).ok()),
                pending: false,
                searchable: searchable(&self.metadata),
            },
        });
        let head = self.head_pointer.is_some();
        let key = &self.config.request.key;
        let inventory =
            added.and_then(|added| version_written(self.binding.as_ref(), key, added, head));
        self.usage_update = Some(
            UsageCounterUpdate::with_stored(self.config.group_id, group_delta, stored)
                .in_bucket(&self.config.request.bucket, RowChange::AddedVersion)
                .with_inventory(inventory),
        );

        // Enforce the hard quota before counters commit; only a positive logical delta can
        // breach it, so deletes pass.
        let charged = location.blob_size.saturating_add(self.envelope_bytes);
        let Some(ceiling) = self.config.quota_ceiling.filter(|_| charged > 0) else {
            return self.start_usage_update();
        };
        let mut gate = QuotaGate::new_for_realm(
            ceiling,
            charged,
            self.config.group_id,
            self.config.node_id,
            self.config.realm_id,
        );
        let call = gate.start_calls();
        self.quota_gate = Some(gate);
        self.call(PutObjectState::EnforceQuota, call)
    }

    fn start_usage_update(&mut self) -> Turn {
        let update = self.usage_update.as_mut();
        let update = update.ok_or(PutObjectError::PutObjectFailed)?;
        let call = update.start_calls().into_call();
        self.call(PutObjectState::UpdateUsage, call)
    }

    pub(super) fn handle_enforce_quota(&mut self, result: IoResult) -> Turn {
        let gate = self.quota_gate.as_mut();
        let gate = gate.ok_or(PutObjectError::PutObjectFailed)?;
        if let Some(call) = gate.step_calls(result)? {
            return Ok(Next::Call(call));
        }
        if gate.is_rebuilding() {
            return Err(PutObjectError::UsageRebuilding);
        }
        if gate.is_exceeded() {
            let (limit, usage) = (gate.ceiling(), gate.projected_usage());
            return Err(PutObjectError::QuotaExceeded { limit, usage });
        }
        self.start_usage_update()
    }

    pub(super) fn handle_usage_update(&mut self, result: IoResult) -> Turn {
        let update = self.usage_update.as_mut();
        let update = update.ok_or(PutObjectError::PutObjectFailed)?;
        match update.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.write_cleanup_row(),
        }
    }

    /// The caller's row fence, the import rows, then the reconcile row of the written blob,
    /// which commits with the version.
    pub(super) fn write_cleanup_row(&mut self) -> Turn {
        let next = match (self.row_fence.as_mut(), self.version_id) {
            (Some(fence), Some(version_id)) => fence.next_call(version_id),
            (Some(_), None) => return Err(PutObjectError::InvalidOperationState),
            (None, _) => None,
        };
        if let Some(call) = next {
            return self.call(PutObjectState::RowFence, call);
        }
        if let Some(turn) = self.plain_records() {
            return turn;
        }
        if self.adopted {
            return self.finish_publish();
        }
        let location = self.written_location.clone();
        let location = location.ok_or(PutObjectError::MissingOutput)?;
        let key = location.ulid.to_bytes().to_vec().into();
        let work = self.reconcile_work(location)?;
        let row = (
            BLOB_CLEANUP_KEYSPACE.to_string(),
            key,
            work.to_bytes()?.into(),
        );
        self.call(PutObjectState::WriteCleanupRow, IoCall::Write(vec![row]))
    }

    pub(super) fn fence_answered(&mut self, result: IoResult) -> Turn {
        let fence = self.row_fence.as_mut();
        match fence.map(|fence| fence.answer_call(result)) {
            Some(Ok(())) => self.write_cleanup_row(),
            Some(Err(FenceError::Taken)) => Err(PutObjectError::FenceTaken),
            Some(Err(FenceError::Storage(error))) => Err(error.into()),
            Some(Err(FenceError::Unexpected)) | None => Err(PutObjectError::InvalidOperationState),
        }
    }

    /// The version's result; the transaction commits with it, then the reservation of the
    /// written bytes is released, the bytes are announced and a duplicate copy goes.
    pub(super) fn finish_publish(&mut self) -> Turn {
        let location = self.get_output().cloned();
        let location = location.ok_or(PutObjectError::MissingOutput)?;
        let version_id = self.version_id.ok_or(PutObjectError::PutObjectFailed)?;
        let mut calls = Vec::new();
        if let Some(written) = self.written_location.as_ref() {
            let release = BlobEffect::ReleaseReservation { id: written.ulid };
            calls.push(Effect::Blob(release).into());
        }
        // Distributed buckets are found through their content indexes, not the DHT.
        if self.binding.is_none()
            && let Some(blake3) = location.get_blake3()
            && let Ok(register) =
                dht_registration_effect(blake3, self.config.realm_id, &self.rocrate_limits)
        {
            calls.push(register.into());
        }
        match self.cleanup_location.take() {
            Some(duplicate) => {
                let delete = BlobEffect::Delete {
                    location: duplicate,
                };
                calls.push(Effect::Blob(delete).into());
                calls.push(schedule_snapshot_publish().into());
            }
            None => {
                calls.push(schedule_snapshot_publish().into());
                if self.dedup.is_some() {
                    calls.push(schedule_cleanup_effect().into());
                }
            }
        }
        Ok(Next::Then {
            output: PutObjectResult {
                location,
                version_id,
                replayed: false,
            },
            calls,
        })
    }
}
