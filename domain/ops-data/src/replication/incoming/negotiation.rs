//! Negotiates an inbound version: destination, quota probe and placement gate decide the reply.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

impl IncomingVersionOperation {
    /// Reads the bucket with its encryption settings, so an encrypting bucket captures its seal
    /// plan before any byte arrives.
    pub(super) fn read_destination_bucket(&mut self) -> Turn {
        let key: Key = self.manifest.bucket.as_bytes().to_vec().into();
        let reads = vec![
            (S3_BUCKET_KEYSPACE.to_string(), key.clone()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), key),
        ];
        self.reads(IncomingVersionState::ReadDestinationBucket, reads)
    }

    pub(super) fn destination_bucket_info(&self) -> BucketInfo {
        BucketInfo {
            group_id: self.manifest.group_id,
            created_at: self.manifest.created_at,
            created_by: self.manifest.created_by,
            cors_configuration: None,
            storage_routing: Vec::new(),
            placement_policies: Vec::new(),
            placement_policy_generation: 0,
            compression: Compression::Off,
            distribution: None,
        }
    }

    pub(super) fn create_destination_bucket(&mut self) -> Turn {
        self.create_attempted = true;
        let child = boxed_suboperation(
            CreateBucketOperation::new(
                self.manifest.bucket.clone(),
                self.destination_bucket_info(),
            ),
            |result| {
                Event::SubOperation(SubOperationEvent::BucketCreated {
                    result: match result {
                        Ok(_) => Ok(()),
                        Err(err) => Err(err.to_string()),
                    },
                })
            },
        );
        self.call(
            IncomingVersionState::CreateDestinationBucket,
            IoCall::Child(child),
        )
    }

    /// The receiver's own group default and registered backend ids, loaded once
    /// the destination group is known so that the existing-copy probe and the
    /// transfer resolve the destination from identical inputs.
    pub(super) fn load_destination_routing(&mut self) -> Turn {
        let group_id = self.destination_group_id.unwrap_or(self.manifest.group_id);
        self.call(
            IncomingVersionState::LoadDestinationRouting,
            load_group_inputs(group_id).into(),
        )
    }

    pub(super) fn check_permissions(&mut self, group_id: Ulid) -> Turn {
        if self.manifest.distribution.is_some() {
            return self.read_publication_record(false);
        }
        let path = self.target_authorization_path(group_id);
        if self.manifest_policy.as_deref() != Some(path.as_str()) {
            return self.reject_negotiation(IncomingVersionError::ManifestPermissionDenied);
        }
        if self.manifest.reference_advance.is_some() {
            if !self.valid_advance_manifest() {
                return self.reject_negotiation(IncomingVersionError::InvalidReferenceAdvance);
            }
            return self.read_existing_version();
        }
        match self.writer_policy.as_deref() {
            Some(allowed) if allowed == path.as_str() => self.read_existing_version(),
            _ => self.reject_negotiation(IncomingVersionError::WriterPermissionDenied),
        }
    }

    pub(super) fn read_existing_version(&mut self) -> Turn {
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(err) => return self.fail(err.into()),
        };
        self.read(
            IncomingVersionState::ReadExistingVersion,
            BLOB_VERSIONS_KEYSPACE,
            key,
        )
    }

    pub(super) fn read_replaced_blob(&mut self, key: BlobLocationKey) -> Turn {
        self.read(
            IncomingVersionState::ReadReplacedBlob,
            BLOB_LOCATIONS_KEYSPACE,
            key.to_bytes(),
        )
    }

    pub(super) fn read_quota_config(&mut self) -> Turn {
        let target = DocumentTarget::RealmConfig {
            realm_id: self.local_realm_id,
        };
        self.read(
            IncomingVersionState::ReadQuotaConfig,
            target.storage_keyspace(),
            target.storage_key(),
        )
    }

    pub(super) fn start_quota_check(&mut self, ceiling: u64) -> Turn {
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionError::DestinationNotFound);
        };
        let logical_bytes = match self.incoming_logical_bytes() {
            Ok(logical_bytes) => logical_bytes,
            Err(error) => return self.fail(error),
        }
        .saturating_sub(self.replaced_logical_bytes);
        self.quota_ceiling = Some(ceiling);
        let gate = QuotaGate::new_for_realm(
            ceiling,
            logical_bytes,
            group_id,
            self.local_node_id,
            self.local_realm_id,
        );
        self.call(
            IncomingVersionState::EnforceQuota,
            IoCall::child(QuotaProbe { gate }),
        )
    }

    /// Asks only about the backend this node would route the blob to: a copy on
    /// any other backend cannot satisfy the destination placement.
    pub(super) fn read_existing_blob(&mut self) -> Turn {
        let Some(blob) = self.manifest.blob.as_ref() else {
            return self.fail(IncomingVersionError::MissingBlobInfo);
        };
        let hash = blob.hash;
        let bucket = &self.manifest.bucket;
        let mut imports = self.destination_inputs.imports.iter();
        if imports.any(|rule| &rule.bucket == bucket && rule.mapped_bucket().is_some()) {
            return self.reject_negotiation(IncomingVersionError::MappedDestination);
        }
        // A full destination still probes, because a copy it already holds
        // costs it nothing; the cap only refuses the transfer itself.
        let backend = match self.resolve_destination() {
            Ok(resolved) => resolved.backend,
            Err(IncomingVersionError::RoutingFailed(RoutingError::BackendFull(backend))) => {
                self.destination_full = Some(RoutingError::BackendFull(backend.clone()));
                backend
            }
            // Still before the negotiation reply, so a node that cannot place
            // the blob owes the sender a reason rather than a dropped stream.
            Err(error) => return self.reject_negotiation(error),
        };
        // An encrypting destination never adopts a plain or foreign copy of the content.
        if self.seal_plan.is_some() {
            return self.request_blob_version();
        }
        // Only a copy in this node's own encoding for the bucket can be reused.
        let key = BlobLocationKey::new(hash, self.destination_compression.into(), backend);
        self.read(
            IncomingVersionState::ReadExistingBlob,
            BLOB_LOCATIONS_KEYSPACE,
            key.to_bytes(),
        )
    }

    /// The only negotiation result that stores bytes, so the destination's cap
    /// is answered here rather than at the probe that keys the deduplication.
    pub(super) fn request_blob_version(&mut self) -> Turn {
        match self.destination_full.take() {
            Some(error) => self.reject_negotiation(IncomingVersionError::RoutingFailed(error)),
            None => match self.seal_plan {
                Some(plan) => self.reads(IncomingVersionState::ReadAbe, abe_reads(plan.key)),
                None => self.send_negotiation(ReplicationNegotiationResult::NeedBlobVersion),
            },
        }
    }

    /// Admitted ABE parameters give the replica its own object key and envelope before the
    /// transfer; a generation without them keeps bucket-only grants.
    pub(super) fn accept_abe(&mut self, values: Vec<Option<Value>>) -> Turn {
        let Some(plan) = self.seal_plan else {
            return self.fail(IncomingVersionError::StalePlan);
        };
        if values.first().is_some_and(Option::is_none) {
            return self.send_negotiation(ReplicationNegotiationResult::NeedSealedBlob(plan));
        }
        let (parameters, epoch) = match parse_abe(&keyed(&values), plan.key) {
            Ok(parsed) => parsed,
            Err(error) => return self.reject_negotiation(BlobError::from(error).into()),
        };
        let envelope = AbeEffect::Envelope(EnvelopePlan {
            parameters,
            epoch,
            write_id: self.ids.mint(self.now_ms),
            object_key: self.manifest.key.clone(),
            bucket_public: plan.public_key,
        });
        self.blob(
            IncomingVersionState::CreateEnvelope,
            BlobEffect::Abe(Box::new(envelope)),
        )
    }

    pub(super) fn accept_envelope(&mut self, event: Event) -> Turn {
        let envelope = match event.into_blob() {
            Ok(BlobEvent::Abe(event)) => match *event {
                AbeEvent::Envelope(envelope) => envelope,
                event => {
                    let received = Event::blob(BlobEvent::Abe(Box::new(event))).label();
                    return self.invalid("AbeEvent::Envelope", received);
                }
            },
            Ok(BlobEvent::Error(error)) => return self.reject_negotiation(error.into()),
            Ok(event) => {
                return self.invalid("Event::Blob(BlobEvent::Abe)", event.label());
            }

            Err(event) => {
                return self.invalid("Event::Blob(BlobEvent::Abe)", event.label());
            }
        };
        let Some(plan) = self.seal_plan else {
            return self.fail(IncomingVersionError::StalePlan);
        };
        let object = envelope.context.public_key;
        self.envelope = Some(envelope);
        self.send_negotiation(ReplicationNegotiationResult::NeedAbeBlob(plan, object))
    }

    pub(super) fn resolve_destination(&self) -> Result<ResolvedBackend, IncomingVersionError> {
        let snapshot = self
            .routing
            .snapshot(self.destination_group_id.unwrap_or(self.manifest.group_id))
            .with_group_inputs(self.destination_inputs.clone())
            .with_bucket_rules(self.destination_rules.clone())
            .with_compression(self.destination_compression);
        resolve_backend(&snapshot, &self.manifest.bucket, &self.manifest.key)
            .map_err(IncomingVersionError::RoutingFailed)
    }

    /// Nothing is admitted before the destination is evaluated: the reply that
    /// invites bytes is itself behind the gate.
    pub(super) fn send_negotiation(&mut self, result: ReplicationNegotiationResult) -> Turn {
        // A rejection invites no bytes, so it is not gated. Gating it would
        // re-enter the gate from its own denial and never terminate.
        if matches!(
            result,
            ReplicationNegotiationResult::AlreadyReplicatedVersion
                | ReplicationNegotiationResult::Rejected(_)
        ) {
            return self.reply_negotiation(result);
        }
        let mut inherited = self
            .replaced_version
            .as_ref()
            .map(|version| version.placement_policies.clone())
            .unwrap_or_default();
        // The local default governs this copy too: a sender's manifest can only
        // add refs to what this destination already requires.
        if let Some(gated) = self.gated_bucket.as_ref() {
            inherited.extend(gated.policies.iter().copied());
        }
        let refs = match union_refs(&self.manifest.placement_policies, &inherited) {
            Ok(refs) => refs,
            Err(error) => return self.reject_negotiation(error.into()),
        };
        self.gated_refs = refs.clone();
        self.gated_bucket = self
            .gated_bucket
            .take()
            .map(|gated| gated.stored_under(self.gate_context.as_ref(), !refs.is_empty()));
        match write_gate(self.gate_context.as_ref(), &refs, self.destination_group_id) {
            Ok(None) => self.reply_negotiation(result),
            Ok(Some(gate)) => {
                self.pending_negotiation = Some(result);
                self.call(IncomingVersionState::PolicyGate, IoCall::child(gate))
            }
            Err(error) => self.reject_negotiation(error.into()),
        }
    }

    pub(super) fn accept_policy_gate(
        &mut self,
        outcome: Result<PolicyGateOutcome, ReadPolicyError>,
    ) -> Turn {
        let Some(result) = self.pending_negotiation.take() else {
            return self.fail(IncomingVersionError::GateNotPending);
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => return self.reject_negotiation(PolicyGateError::from(error).into()),
        };
        match gate_decision(outcome) {
            Ok(()) => self.reply_negotiation(result),
            Err(error) => self.reject_negotiation(error.into()),
        }
    }

    pub(super) fn reply_negotiation(&mut self, result: ReplicationNegotiationResult) -> Turn {
        self.negotiation_result = Some(result.clone());
        let payload = match VersionReplicationMessage::VersionNegotiationResponse(result).to_bytes()
        {
            Ok(payload) => payload,
            Err(err) => return self.fail(err.into()),
        };
        let stream_id = self.stream_id;
        self.blob(
            IncomingVersionState::SendNegotiation,
            BlobEffect::SendMessage { stream_id, payload },
        )
    }

    pub(super) fn accept_destination_bucket(&mut self, values: Vec<Option<Value>>) -> Turn {
        let [value, settings] = match <[_; 2]>::try_from(values) {
            Ok(values) => values,
            Err(_) => return self.fail(PolicyGateError::InvalidEvent.into()),
        };
        let settings = match BucketEncryption::from_row(settings.as_deref()) {
            Ok(settings) => settings,
            Err(err) => return self.fail(err.into()),
        };
        // A reference would serve bytes this bucket never sealed.
        if settings.is_encrypted()
            && (self.is_reference_item() || self.manifest.reference_advance.is_some())
        {
            return self.reject_negotiation(IncomingVersionError::EncryptedDestination);
        }

        let Some(value) = value else {
            if self.create_attempted || self.manifest.distribution.is_some() {
                return self.reject_negotiation(IncomingVersionError::DestinationNotFound);
            }
            if self.manifest.reference_advance.is_some() {
                return self.reject_negotiation(IncomingVersionError::DestinationNotFound);
            }
            if self.manifest_policy.is_none() {
                return self.reject_negotiation(IncomingVersionError::ManifestPermissionDenied);
            }
            return self.create_destination_bucket();
        };
        let bucket_info = match BucketInfo::from_bytes(value.as_ref()) {
            Ok(bucket_info) => bucket_info,
            Err(err) => return self.fail(err.into()),
        };

        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            group_id = %bucket_info.group_id,
            kind = ?self.manifest.kind,
            current_version = self.manifest.current_version,
            current_version_generation = ?self.manifest.current_version_generation,
            "Loaded destination bucket for incoming replication"
        );

        self.destination_group_id = Some(bucket_info.group_id);
        self.gated_bucket = Some(GatedBucket::observe(Some(&bucket_info)));
        self.destination_rules = bucket_info.storage_routing;
        self.destination_compression = bucket_info.compression;
        self.destination_bound = bucket_info.distribution.is_some();
        self.destination_binding = bucket_info.distribution.clone();
        if self.destination_bound != self.manifest.distribution.is_some() {
            return self.reject_negotiation(IncomingVersionError::ForeignPublication);
        }
        // A version from another bucket is never a release here.
        if self.manifest.distribution.is_none() {
            strip_release_keys(&mut self.manifest.metadata);
        }
        let Some(key) = settings
            .active_key()
            .filter(|_| self.manifest.kind == ReplicationItemKind::Materialized)
        else {
            return self.load_destination_routing();
        };
        self.destination_settings = Some(settings);
        self.read(
            IncomingVersionState::ReadDestinationKey,
            BUCKET_KEY_KEYSPACE,
            key.key(),
        )
    }

    /// Captures how this encrypting destination seals: its active key and stored format.
    pub(super) fn accept_destination_key(&mut self, value: Option<Value>) -> Turn {
        let Some(settings) = self.destination_settings.take() else {
            return self.fail(IncomingVersionError::DestinationNotFound);
        };
        let record = match value.map(|value| BucketKeyRecord::from_bytes(&value)) {
            Some(Ok(record)) => record,
            Some(Err(error)) => return self.fail(error.into()),
            None => return self.reject_negotiation(BucketKeyError::Unsupported.into()),
        };
        match SealPlan::capture(&settings, &record) {
            Ok(plan) => {
                self.seal_plan = plan;
                self.load_destination_routing()
            }
            Err(error) => self.reject_negotiation(error.into()),
        }
    }

    pub(super) fn accept_bucket_created(&mut self, event: Event) -> Turn {
        let Event::SubOperation(SubOperationEvent::BucketCreated { result }) = event else {
            return self.invalid(
                "Event::SubOperation(SubOperationEvent::BucketCreated)",
                event.label(),
            );
        };
        if let Err(reason) = result {
            debug!(
                bucket = %self.manifest.bucket,
                stream_id = %self.stream_id,
                reason = %reason,
                "Destination bucket auto-create did not create; re-reading"
            );
        }
        self.read_destination_bucket()
    }

    pub(super) fn accept_routing_loaded(&mut self, event: Event) -> Turn {
        let Event::SubOperation(SubOperationEvent::GroupRoutingLoaded { result }) = event else {
            return self.invalid(
                "Event::SubOperation(SubOperationEvent::GroupRoutingLoaded)",
                event.label(),
            );
        };
        match *result {
            Ok(inputs) => self.destination_inputs = inputs,
            Err(error) => {
                return self.fail(IncomingVersionError::RoutingInputsFailed(error));
            }
        }
        self.check_permissions(self.destination_group_id.unwrap_or(self.manifest.group_id))
    }

    pub(super) fn accept_existing_version(&mut self, value: Option<Value>) -> Turn {
        debug!(
            bucket = %self.manifest.bucket,
            key = %self.manifest.key,
            version_id = %self.manifest.version_id,
            stream_id = %self.stream_id,
            existing_version_present = value.is_some(),
            kind = ?self.manifest.kind,
            "Loaded existing destination version metadata"
        );

        if let Some(value) = value {
            let existing = match BlobVersion::from_bytes(value.as_ref()) {
                Ok(existing) => existing,
                Err(error) => return self.fail(error.into()),
            };
            self.existing_marker = Some(existing.is_deleted());
            // The origin is fixed at mint, so a replay must carry the stored one.
            if existing.origin.is_some() && existing.origin != self.manifest.version_origin {
                return self.reject_negotiation(IncomingVersionError::OriginMismatch);
            }
            if self.manifest.reference_advance.is_some() {
                let incoming = match self.reference_version() {
                    Ok(incoming) => incoming,
                    Err(error) => return self.reject_negotiation(error),
                };
                if !same_reference(&existing, &incoming) {
                    return self.reject_negotiation(IncomingVersionError::InvalidReferenceAdvance);
                }
                self.replaced_reference_bytes = self
                    .manifest
                    .reference_metadata
                    .as_ref()
                    .map_or(0, |metadata| metadata.content_length);
                self.replaced_version = Some(existing);
                self.advance_version_exists = true;
                return self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly);
            }
            self.replaced_version = Some(existing.clone());
            match &existing.state {
                BlobVersionState::Reference {
                    cached_metadata, ..
                } => {
                    self.replaced_reference_bytes = cached_metadata.content_length;
                    if self.is_reference_item() {
                        let incoming = match self.reference_version() {
                            Ok(incoming) => incoming,
                            Err(error) => return self.fail(error),
                        };
                        if same_reference(&existing, &incoming) {
                            return self.send_negotiation(
                                ReplicationNegotiationResult::AlreadyReplicatedVersion,
                            );
                        }
                        return self
                            .send_negotiation(ReplicationNegotiationResult::NeedVersionOnly);
                    }
                }
                BlobVersionState::Materialized { blob_hash, .. } => {
                    if !self.is_reference_item()
                        && self
                            .manifest
                            .blob
                            .as_ref()
                            .is_some_and(|blob| blob.hash == *blob_hash)
                    {
                        return self.send_negotiation(
                            ReplicationNegotiationResult::AlreadyReplicatedVersion,
                        );
                    }
                    let Some(key) = existing.location_key() else {
                        return self.fail(IncomingVersionError::MissingBlobLocation);
                    };
                    return self.read_replaced_blob(key);
                }
                BlobVersionState::Deleted
                    if self.manifest.kind == ReplicationItemKind::DeleteMarker =>
                {
                    return self
                        .send_negotiation(ReplicationNegotiationResult::AlreadyReplicatedVersion);
                }
                BlobVersionState::Deleted => {}
                // Only the same content, as the earlier sender claimed it, is already here.
                BlobVersionState::PendingContent { archive, .. }
                    if self.manifest.blob.is_some() && !self.is_reference_item() =>
                {
                    return self.read(
                        IncomingVersionState::ReadExistingClaim,
                        PENDING_CLAIM_KEYSPACE,
                        archive.to_bytes(),
                    );
                }
                BlobVersionState::PendingContent { .. } => {
                    return self.reject_negotiation(IncomingVersionError::EncryptedVersion);
                }
            }
        }

        match self.manifest.kind {
            ReplicationItemKind::DeleteMarker => {
                self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
            }
            ReplicationItemKind::Materialized => self.read_quota_config(),
        }
    }

    pub(super) fn accept_existing_claim(&mut self, value: Option<Value>) -> Turn {
        let claimed = self.manifest.blob.as_ref().map(|blob| blob.hash.to_vec());
        if value.is_some_and(|value| Some(value.to_vec()) == claimed) {
            return self.send_negotiation(ReplicationNegotiationResult::AlreadyReplicatedVersion);
        }
        self.reject_negotiation(IncomingVersionError::EncryptedVersion)
    }

    pub(super) fn accept_replaced_blob(&mut self, value: Option<Value>) -> Turn {
        self.replaced_logical_bytes = match value
            .as_ref()
            .map(|value| BackendLocation::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(location) => location.map_or(0, |location| location.blob_size),
            Err(error) => return self.fail(error.into()),
        };
        self.defer_quota = self.seal_plan.is_some();
        if self.is_reference_item() {
            self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
        } else {
            self.read_quota_config()
        }
    }

    pub(super) fn accept_quota_config(&mut self, value: Option<Value>) -> Turn {
        let Some(group_id) = self.destination_group_id else {
            return self.fail(IncomingVersionError::DestinationNotFound);
        };
        let ceiling = match value
            .map(|value| RealmConfigDocument::from_bytes(value.as_ref()))
            .transpose()
        {
            // A bound destination only takes its coordinator's publications: always a copy.
            Ok(Some(config))
                if self.destination_bound && !config.replica_quota.counts(&group_id) =>
            {
                None
            }
            Ok(Some(config)) => config.quota.effective_group_ceiling(&group_id),
            Ok(None) => None,
            Err(err) => return self.fail(err.into()),
        };

        match ceiling {
            _ if self.is_reference_item() => {
                self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
            }
            Some(ceiling) if self.defer_quota => {
                self.quota_ceiling = Some(ceiling);
                self.read_existing_blob()
            }
            Some(ceiling) => self.start_quota_check(ceiling),
            None => self.read_existing_blob(),
        }
    }

    pub(super) fn accept_quota_probe(&mut self, exceeded: Result<bool, QuotaGateError>) -> Turn {
        match exceeded {
            Ok(true) => {
                self.output = Some(self.result(false));
                self.send_negotiation(ReplicationNegotiationResult::Rejected("quota".to_string()))
            }
            Ok(false) if self.is_reference_item() => {
                self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
            }
            Ok(false) => self.read_existing_blob(),
            Err(error) => self.fail(error.into()),
        }
    }

    pub(super) fn accept_existing_blob(&mut self, value: Option<Value>) -> Turn {
        if let Some(value) = value {
            match BackendLocation::from_bytes(value.as_ref()) {
                Ok(location) => {
                    if self.validate_materialized_location(&location).is_err() {
                        debug!(
                            bucket = %self.manifest.bucket,
                            key = %self.manifest.key,
                            version_id = %self.manifest.version_id,
                            stream_id = %self.stream_id,
                            existing_blob_size = location.blob_size,
                            "Existing destination blob differs; requesting blob and version"
                        );
                        return self.request_blob_version();
                    }
                    self.existing_blob_location = Some(location);
                    debug!(
                        bucket = %self.manifest.bucket,
                        key = %self.manifest.key,
                        version_id = %self.manifest.version_id,
                        stream_id = %self.stream_id,
                        "Existing destination blob matches manifest; requesting version only"
                    );
                    self.send_negotiation(ReplicationNegotiationResult::NeedVersionOnly)
                }
                Err(_) => {
                    debug!(
                        bucket = %self.manifest.bucket,
                        key = %self.manifest.key,
                        version_id = %self.manifest.version_id,
                        stream_id = %self.stream_id,
                        "Destination blob missing or invalid; requesting blob and version"
                    );
                    self.request_blob_version()
                }
            }
        } else {
            debug!(
                bucket = %self.manifest.bucket,
                key = %self.manifest.key,
                version_id = %self.manifest.version_id,
                stream_id = %self.stream_id,
                "Destination blob absent; requesting blob and version"
            );
            self.request_blob_version()
        }
    }

    pub(super) fn accept_reply_sent(&mut self, event: Event) -> Turn {
        let received = event.label();
        let Ok(BlobEvent::MessageSent { .. }) = event.into_blob() else {
            return self.invalid("Event::Blob(BlobEvent::MessageSent)", received);
        };

        match self.negotiation_result.clone() {
            Some(ReplicationNegotiationResult::AlreadyReplicatedVersion)
            | Some(ReplicationNegotiationResult::Rejected(_)) => self.close_connection(),
            Some(ReplicationNegotiationResult::NeedVersionOnly) => {
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    decision = ?self.negotiation_result,
                    "Negotiation sent; awaiting version apply"
                );
                if self.manifest.blob.is_some() {
                    let stream_id = self.stream_id;
                    self.blob(
                        IncomingVersionState::AwaitApproval,
                        BlobEffect::ReadMessage { stream_id },
                    )
                } else {
                    self.start_apply()
                }
            }
            Some(
                ReplicationNegotiationResult::NeedBlobVersion
                | ReplicationNegotiationResult::NeedSealedBlob(_)
                | ReplicationNegotiationResult::NeedAbeBlob(..),
            ) => {
                debug!(
                    bucket = %self.manifest.bucket,
                    key = %self.manifest.key,
                    version_id = %self.manifest.version_id,
                    stream_id = %self.stream_id,
                    decision = ?self.negotiation_result,
                    "Negotiation sent; awaiting blob transfer"
                );
                self.receive_blob()
            }
            None => self.fail(IncomingVersionError::ReplicationError(
                ReplicationError::ReplicationFailed,
            )),
        }
    }
}
