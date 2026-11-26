//! Publishes written bytes as a version in one write transaction: location, head, version,
//! registration and counters commit together, or none of them do.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;
use aruna_core::keyspaces::{
    BUCKET_CREATION_KEYSPACE, DELETE_AUDIT_KEYSPACE, DELETE_TRAIL_KEYSPACE,
};
use aruna_core::structs::storage::blob::{BackendRef, incarnation};
use aruna_core::structs::storage::delete_audit::{
    BlobAuditKind, BlobAuditRecord, delete_audit_key, trail_key,
};

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

    /// The location row of new bytes, written with the head or the path index, so a version an
    /// import registered meanwhile commits nothing. A row read in this transaction is not
    /// rewritten: identical uploads would conflict on it, and its read already fences a reclaim.
    pub(super) fn location_rows(&self) -> Result<Vec<(String, Key, Value)>, PutObjectError> {
        match self.new_blob {
            true => Ok(vec![self.location_row()?]),
            false => Ok(Vec::new()),
        }
    }

    pub(super) fn location_row(&self) -> Result<(String, Key, Value), PutObjectError> {
        let location = self.get_output();
        stored_row(location.ok_or(PutObjectError::MissingOutput)?)
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
        if let Some(expected) = self.expected_life
            && existing.as_ref().and_then(|pointer| pointer.life_id) != expected
        {
            return Err(PutObjectError::HeadChanged);
        }
        self.existing_pointer = existing;
        if self.source_delete.is_some() {
            return self.read_source();
        }
        self.read_guards()
    }

    /// The caller's guards and ranges are read before this transaction writes any row they
    /// could hit, one call at a time.
    fn read_guards(&mut self) -> Turn {
        if let Some(call) = self.row_fence.as_mut().and_then(RowFence::check_guards) {
            return self.call(PutObjectState::RowGuards, call);
        }
        self.lookup_checked()
    }

    pub(super) fn guards_answered(&mut self, result: IoResult) -> Turn {
        let fence = self.row_fence.as_mut();
        match fence.map(|fence| fence.answer_call(result)) {
            Some(Ok(())) => self.read_guards(),
            Some(Err(error)) => Err(fence_error(error)),
            None => Err(PutObjectError::InvalidOperationState),
        }
    }

    fn lookup_checked(&mut self) -> Turn {
        let existing = self.existing_pointer.clone();
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

    pub(super) fn write_current_lookup(
        &mut self,
        existing: Option<&CurrentVersionPointer>,
    ) -> Turn {
        let set_hint = self.set_hint;
        let version_id = *self
            .version_id
            .get_or_insert_with(|| mint_version(self.ids.mint(self.now_ms), set_hint));
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
        let pointer = match self.life {
            Some(life_id) => CurrentVersionPointer::next_live_fresh(existing, version_id, life_id)?,
            None => {
                let life_id = self.ids.mint(self.now_ms);
                CurrentVersionPointer::next_live_inherit(existing, version_id, life_id)?
            }
        };
        let pointer = stamp_head(pointer, self.binding.as_ref(), self.version_epoch);
        let head = (
            BLOB_HEAD_KEYSPACE.to_string(),
            self.alias_context().head_key().to_bytes()?.into(),
            pointer.to_bytes()?.into(),
        );
        self.head_pointer = Some(pointer);
        let mut rows = self.location_rows()?;
        rows.push(head);
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
            rows.extend(self.location_rows()?);
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
            self.now_ms,
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
        let call = update.start_calls(self.ids.mint(self.now_ms)).into_call();
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

    /// The caller's fence rows, the import rows, then the reconcile row of the written blob,
    /// which commits with the version.
    pub(super) fn write_cleanup_row(&mut self) -> Turn {
        if let Some(source) = self.source_delete.clone().filter(|_| !self.source_written) {
            self.source_written = true;
            return self.write_source(source);
        }
        if self.parent_marker.is_some() && !self.marker_read {
            self.marker_read = true;
            return self.read_marker_rows();
        }
        let next = match (self.row_fence.as_mut(), self.version_id) {
            (Some(fence), Some(version_id)) => fence.apply_mutations(version_id),
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
            Some(Err(error)) => Err(fence_error(error)),
            None => Err(PutObjectError::InvalidOperationState),
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
            // A refused release leaves the reconcile row of the stored bytes behind.
            let stored = Some(&location).filter(|stored| stored.ulid == written.ulid);
            let reconcile = stored.and_then(|stored| self.reconcile_work(stored.clone()).ok());
            calls.push(IoCall::child(ReleaseWrite::new(written.ulid, reconcile)));
        }
        // Marker bytes the marker did not publish go, as a duplicate copy does.
        if let Some(written) = self.marker_location.as_ref() {
            let stored = (self.marker_output.as_ref()).filter(|stored| stored.ulid == written.ulid);
            let reconcile = stored.and_then(|stored| self.reconcile_work(stored.clone()).ok());
            calls.push(IoCall::child(ReleaseWrite::new(written.ulid, reconcile)));
            if stored.is_none() {
                let location = written.clone();
                calls.push(Effect::blob(BlobEffect::Delete { location }).into());
            }
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
                calls.push(Effect::blob(delete).into());
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

fn fence_error(error: FenceError) -> PutObjectError {
    match error {
        FenceError::Taken => PutObjectError::FenceTaken,
        FenceError::Storage(error) => error.into(),
        FenceError::Unexpected => PutObjectError::InvalidOperationState,
    }
}

/// The location row of `location`, keyed by its hash, encoding and backend.
fn stored_row(location: &BackendLocation) -> Result<(String, Key, Value), PutObjectError> {
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

/// The source delete and the parent marker, both written in the version transaction.
impl PutObjectOperation {
    /// The purge fence, head and incarnation of the source, read before any row is written.
    fn read_source(&mut self) -> Turn {
        let source = self.source_delete.as_ref();
        let source = source.ok_or(PutObjectError::InvalidOperationState)?;
        // A distributed bucket's markers belong to its coordinator and inventory.
        if self.binding.is_some() {
            return Err(PutObjectError::PutObjectFailed);
        }
        let bucket = &self.config.request.bucket;
        let head = BlobHeadKey::new(bucket.clone(), source.key.clone()).to_bytes()?;
        let reads = vec![
            (PURGE_FENCE_KEYSPACE.to_string(), fence_key(bucket)),
            (BLOB_HEAD_KEYSPACE.to_string(), head.into()),
            (
                BUCKET_CREATION_KEYSPACE.to_string(),
                bucket.as_bytes().to_vec().into(),
            ),
        ];
        self.call(PutObjectState::ReadSourceHead, IoCall::Read(reads))
    }

    /// Only the live head the caller saw may be deleted; any other refuses the whole write.
    pub(super) fn source_head_read(&mut self, values: Vec<Option<Value>>) -> Turn {
        let [fence, head, creation] = <[Option<Value>; 3]>::try_from(values)
            .map_err(|_| PutObjectError::InvalidOperationState)?;
        let source = self.source_delete.clone();
        let source = source.ok_or(PutObjectError::InvalidOperationState)?;
        let bucket = self.config.request.bucket.clone();
        check_fence_row(fence.as_deref(), &bucket, &source.key)?;
        self.source_incarnation = incarnation(creation.as_deref());
        let head = (head.as_ref())
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()?;
        let Some(head) = head.filter(|head| head.life_id.is_some()) else {
            return Err(PutObjectError::HeadChanged);
        };
        if source
            .expected_life
            .is_some_and(|life| head.life_id != Some(life))
            || source
                .expected_version
                .is_some_and(|id| head.version_id != id)
        {
            return Err(PutObjectError::HeadChanged);
        }
        let key = VersionKey::new(bucket, source.key, head.version_id).to_bytes()?;
        self.source_head = Some(head);
        let read = read_row(BLOB_VERSIONS_KEYSPACE, key);
        self.call(PutObjectState::ReadSourceVersion, read)
    }

    pub(super) fn source_version_read(&mut self, value: Option<Value>) -> Turn {
        let version = value.and_then(|value| BlobVersion::from_bytes(value.as_ref()).ok());
        let version = version.filter(|version| !version.is_deleted());
        let version = version.ok_or(PutObjectError::HeadChanged)?;
        let source = self.source_delete.as_ref();
        let source = source.ok_or(PutObjectError::InvalidOperationState)?;
        let released = version.offered_size(&self.config.request.bucket, &source.key);
        self.source_released = released.unwrap_or(0);
        self.read_guards()
    }

    /// The marker version, head, replication obligation and delete trail of the source.
    fn write_source(&mut self, source: SourceDelete) -> Turn {
        let config = &self.config;
        let (bucket, marker) = (config.request.bucket.clone(), source.marker_version);
        let pointer = CurrentVersionPointer::next_deleted(self.source_head.as_ref(), marker)?;
        let head = BlobHeadKey::new(bucket.clone(), source.key.clone()).to_bytes()?;
        let created_at = UNIX_EPOCH + Duration::from_millis(marker.timestamp_ms());
        let version = BlobVersion::deleted(created_at, source.deleted_by);
        let version_key = VersionKey::new(bucket.clone(), source.key.clone(), marker);
        let auth = AuthContext {
            user_id: source.deleted_by,
            realm_id: config.realm_id,
            path_restrictions: self.restrictions.clone(),
            session: None,
        };
        let obligation = LiveObligationRecord::new(
            config.node_id,
            auth,
            bucket.clone(),
            source.key.clone(),
            marker,
            true,
        );
        let record = BlobAuditRecord {
            realm_id: config.realm_id,
            group_id: config.group_id,
            node_id: config.node_id,
            user_id: source.deleted_by,
            kind: BlobAuditKind::DeleteMarker,
            bucket,
            key: source.key,
            version_id: Some(marker),
            occurred_at_ms: self.now_ms,
        };
        let audit: Value = record.to_bytes()?.into();
        let audit_id = self.ids.mint(self.now_ms);
        let trail = trail_key(&record, self.source_incarnation, audit_id);
        let rows = vec![
            (
                BLOB_HEAD_KEYSPACE.to_string(),
                head.into(),
                pointer.to_bytes()?.into(),
            ),
            (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                version_key.to_bytes()?.into(),
                version.to_bytes()?.into(),
            ),
            live_obligation_entry(&obligation)?,
            (
                DELETE_AUDIT_KEYSPACE.to_string(),
                delete_audit_key(self.config.group_id, audit_id).into(),
                audit.clone(),
            ),
            (DELETE_TRAIL_KEYSPACE.to_string(), trail.into(), audit),
        ];
        self.call(PutObjectState::WriteSource, IoCall::Write(rows))
    }

    /// The source stops counting as a live object and gains a marker row.
    pub(super) fn start_source_usage(&mut self) -> Turn {
        let delta = UsageDelta {
            objects: -1,
            referenced_bytes: -i128::from(self.source_released),
            ..Default::default()
        };
        let mut update = UsageCounterUpdate::for_group(self.config.group_id, delta)
            .in_bucket(&self.config.request.bucket, RowChange::AddedMarker);
        let call = update.start_calls(self.ids.mint(self.now_ms)).into_call();
        self.source_usage = Some(update);
        self.call(PutObjectState::SourceUsage, call)
    }

    pub(super) fn source_usage_step(&mut self, result: IoResult) -> Turn {
        let update = self.source_usage.as_mut();
        let update = update.ok_or(PutObjectError::PutObjectFailed)?;
        match update.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.write_cleanup_row(),
        }
    }

    /// The marker's empty bytes, written like any upload before the version transaction.
    pub(super) fn write_marker_blob(&mut self) -> Turn {
        let marker = self.parent_marker.as_ref();
        let marker = marker.ok_or(PutObjectError::InvalidOperationState)?;
        if self.binding.is_some() {
            return Err(PutObjectError::PutObjectFailed);
        }
        let bucket = self.config.request.bucket.clone();
        let resolved = resolve_backend(&self.config.routing, &bucket, &marker.key)?;
        let write = BlobEffect::Write {
            bucket,
            key: marker.key.clone(),
            resolved: resolved.with_encryption(self.seal_plan),
            created_by: marker.owner,
            blob: BackendStream::empty(),
            size: Some(0),
        };
        self.effect(PutObjectState::WriteMarkerBlob, Effect::blob(write))
    }

    pub(super) fn marker_written(&mut self, event: Event) -> Turn {
        match event.into_blob() {
            Ok(BlobEvent::WriteFinished { location }) => {
                self.marker_location = Some(location);
                self.start_publish()
            }
            Ok(BlobEvent::Error(error)) => Err(PutObjectError::BlobWriteFailed(error)),
            _ => Err(PutObjectError::InvalidOperationState),
        }
    }

    /// The marker's head and the stored copy of its bytes, read in the version transaction.
    fn read_marker_rows(&mut self) -> Turn {
        let marker = self.parent_marker.as_ref();
        let marker = marker.ok_or(PutObjectError::InvalidOperationState)?;
        let written = self.marker_location.as_ref();
        let written = written.ok_or(PutObjectError::MissingOutput)?;
        let hash = written.get_blake3();
        let hash = hash.ok_or(PutObjectError::MissingHash("blake3".to_string()))?;
        let stored =
            BlobLocationKey::from_blake3(hash, written.format.encoding(), written.backend.clone())?;
        let bucket = self.config.request.bucket.clone();
        let head = BlobHeadKey::new(bucket, marker.key.clone()).to_bytes()?;
        let reads = vec![
            (BLOB_HEAD_KEYSPACE.to_string(), head.into()),
            (
                BLOB_LOCATIONS_KEYSPACE.to_string(),
                stored.to_bytes().into(),
            ),
        ];
        self.call(PutObjectState::ReadMarkerRows, IoCall::Read(reads))
    }

    /// A live marker needs nothing; otherwise the marker commits as a fresh empty object.
    pub(super) fn marker_rows_read(&mut self, values: Vec<Option<Value>>) -> Turn {
        let [head, stored] = <[Option<Value>; 2]>::try_from(values)
            .map_err(|_| PutObjectError::InvalidOperationState)?;
        let head = (head.as_ref())
            .map(|value| CurrentVersionPointer::from_bytes(value.as_ref()))
            .transpose()?;
        if head.as_ref().is_some_and(|head| head.life_id.is_some()) {
            self.marker_skipped = true;
            return self.write_cleanup_row();
        }
        let marker = self.parent_marker.clone();
        let marker = marker.ok_or(PutObjectError::InvalidOperationState)?;
        let written = self.marker_location.clone();
        let written = written.ok_or(PutObjectError::MissingOutput)?;
        self.marker_new = stored.is_none();
        let output = match stored {
            Some(value) => BackendLocation::from_bytes(value.as_ref())?,
            None => written.clone(),
        };
        let mut rows = Vec::new();
        if self.marker_new {
            rows.push(stored_row(&output)?);
        }
        rows.extend(self.marker_entries(&marker, head.as_ref(), &output)?);
        let work = self.reconcile_work(written.clone())?;
        rows.push((
            BLOB_CLEANUP_KEYSPACE.to_string(),
            written.ulid.to_bytes().to_vec().into(),
            work.to_bytes()?.into(),
        ));
        self.marker_output = Some(output);
        self.call(PutObjectState::WriteMarkerRows, IoCall::Write(rows))
    }

    /// Head, path index, version, archive owner, registration and obligation of the marker.
    fn marker_entries(
        &self,
        marker: &ParentMarker,
        existing: Option<&CurrentVersionPointer>,
        output: &BackendLocation,
    ) -> Result<Vec<(String, Key, Value)>, PutObjectError> {
        let (config, version_id) = (&self.config, marker.version_id);
        let hash = output.get_blake3();
        let hash = hash.ok_or(PutObjectError::MissingHash("blake3".to_string()))?;
        let hash = <[u8; 32]>::try_from(hash).map_err(ConversionError::from)?;
        let pointer = CurrentVersionPointer::next_live_fresh(existing, version_id, marker.life_id)?;
        let context = HeadAliasContext::new(
            config.realm_id,
            config.group_id,
            config.node_id,
            config.request.bucket.clone(),
            marker.key.clone(),
        );
        let created_at = UNIX_EPOCH + Duration::from_millis(version_id.timestamp_ms());
        let origin = VersionOrigin::Upload {
            node_id: config.node_id,
            device: None,
            client: None,
        };
        let version = BlobVersion::materialized(
            hash,
            output.backend.clone(),
            output.format.encoding(),
            created_at,
            marker.owner,
            None,
        )
        .with_origin(Some(origin))
        .with_policies(self.bucket_policies.clone())?;
        let key = VersionKey::new(
            config.request.bucket.clone(),
            marker.key.clone(),
            version_id,
        );
        let mut rows = vec![
            (
                BLOB_HEAD_KEYSPACE.to_string(),
                context.head_key().to_bytes()?.into(),
                pointer.to_bytes()?.into(),
            ),
            (
                PATHS_INDEX_KEYSPACE.to_string(),
                context.path_index_key(hash, version_id).to_bytes()?.into(),
                Value::from(Vec::new()),
            ),
            (
                BLOB_VERSIONS_KEYSPACE.to_string(),
                key.to_bytes()?.into(),
                version.to_bytes()?.into(),
            ),
        ];
        if output.format.bucket_key().is_some() {
            let owner = CopyOwner::new(ArchiveKey::of(output), key.clone());
            let empty = Value::from(Vec::new());
            rows.push((COPY_OWNER_KEYSPACE.to_string(), owner.key()?.into(), empty));
        }
        rows.push(register_entry(CopyRegistration {
            version: key,
            node_id: config.node_id,
            location: output,
            policies: &version.placement_policies,
            origin: CopyOrigin::Write,
            subject_generation: self.stored_subject(),
            registered_at_ms: version_id.timestamp_ms(),
        })?);
        let auth = AuthContext {
            user_id: marker.owner,
            realm_id: config.realm_id,
            path_restrictions: self.restrictions.clone(),
            session: None,
        };
        let obligation = LiveObligationRecord::new(
            config.node_id,
            auth,
            config.request.bucket.clone(),
            marker.key.clone(),
            version_id,
            false,
        );
        rows.push(live_obligation_entry(&obligation)?);
        Ok(rows)
    }

    /// The marker counts as one more live object; new bytes also count as stored.
    pub(super) fn start_marker_usage(&mut self) -> Turn {
        let output = self.marker_output.clone();
        let output = output.ok_or(PutObjectError::MissingOutput)?;
        let stored = StoredDelta::for_location(&output, self.marker_new);
        let stored = stored.ok_or(PutObjectError::MissingHash("blake3".to_string()))?;
        let delta = UsageDelta {
            objects: 1,
            logical_bytes: i128::from(output.blob_size),
            ..Default::default()
        };
        let mut update = UsageCounterUpdate::with_stored(self.config.group_id, delta, stored)
            .in_bucket(&self.config.request.bucket, RowChange::AddedVersion);
        let call = update.start_calls(self.ids.mint(self.now_ms)).into_call();
        self.marker_usage = Some(update);
        self.call(PutObjectState::MarkerUsage, call)
    }

    pub(super) fn marker_usage_step(&mut self, result: IoResult) -> Turn {
        let update = self.marker_usage.as_mut();
        let update = update.ok_or(PutObjectError::PutObjectFailed)?;
        match update.step_calls(result)? {
            Some(call) => Ok(Next::Call(call.into_call())),
            None => self.write_cleanup_row(),
        }
    }
}
