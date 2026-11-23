//! Cleans up the version an inbound replica replaces: its metadata rows and reclaim candidate.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

impl IncomingVersionOperation {
    pub(super) fn read_replaced_metadata(&mut self) -> Turn {
        if self.replaced_version.is_none() {
            return self.write_hash_lookup();
        }
        let prefix = match MultipartObjectKey::part_prefix(self.manifest.version_id) {
            Ok(prefix) => prefix.into(),
            Err(error) => return self.fail(error.into()),
        };
        let scan = IoCall::Page {
            space: OBJECT_METADATA_KEYSPACE.to_string(),
            prefix,
            after: None,
            limit: 10_000,
        };
        self.call(IncomingVersionState::ReadReplacedMetadata, scan)
    }

    pub(super) fn verify_replaced(&mut self) -> Turn {
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        self.read(
            IncomingVersionState::VerifyReplaced,
            BLOB_VERSIONS_KEYSPACE,
            key,
        )
    }

    pub(super) fn delete_replaced_metadata(&mut self, values: Vec<(Key, Value)>) -> Turn {
        let mut deletes = Vec::with_capacity(values.len() + 2);
        let summary_key = match MultipartObjectKey::summary(self.manifest.version_id).to_bytes() {
            Ok(key) => key.into(),
            Err(error) => return self.fail(error.into()),
        };
        deletes.push((OBJECT_METADATA_KEYSPACE.to_string(), summary_key));
        deletes.extend(
            values
                .into_iter()
                .map(|(key, _)| (OBJECT_METADATA_KEYSPACE.to_string(), key)),
        );
        deletes.append(&mut self.replaced_envelope);
        if let Some(hash) = self
            .replaced_version
            .as_ref()
            .and_then(BlobVersion::blob_hash)
        {
            let context = match self.alias_context() {
                Ok(context) => context,
                Err(error) => return self.fail(error),
            };
            let key = match context
                .path_index_key(*hash, self.manifest.version_id)
                .to_bytes()
            {
                Ok(key) => key.into(),
                Err(error) => return self.fail(error.into()),
            };
            deletes.push((PATHS_INDEX_KEYSPACE.to_string(), key));
        }
        self.call(
            IncomingVersionState::DeleteReplacedMetadata,
            IoCall::Delete(deletes),
        )
    }

    /// The copy the replaced version named, once the replacement stops naming
    /// it. Nothing else drops that reference, so without this the bytes stay
    /// charged to the backend forever.
    pub(super) fn replaced_reclaim_key(&self) -> Option<ReclaimCandidateKey> {
        let replaced = self.replaced_version.as_ref()?.location_key()?;
        let replacement = self.effective_materialized_location().ok().and_then(|it| {
            let hash: [u8; 32] = it.get_blake3()?.try_into().ok()?;
            Some(BlobLocationKey::new(hash, it.format.encoding(), it.backend))
        });
        (replacement.as_ref() != Some(&replaced)).then(|| {
            ReclaimCandidateKey::new(replaced.backend, replaced.encoding, replaced.blake3_hash)
        })
    }

    pub(super) fn write_replaced_candidate(&mut self, key: ReclaimCandidateKey) -> Turn {
        let candidate = ReclaimCandidate {
            enqueued_at: SystemTime::UNIX_EPOCH + Duration::from_millis(self.now_ms),
        };
        let value = match candidate.to_bytes() {
            Ok(value) => value,
            Err(error) => return self.fail(error.into()),
        };
        let row = (
            BLOB_RECLAIM_KEYSPACE.to_string(),
            key.to_bytes().into(),
            value.into(),
        );
        self.write(IncomingVersionState::WriteReclaimCandidate, vec![row])
    }

    pub(super) fn accept_replaced_version(&mut self, value: Option<Value>) -> Turn {
        let current = match value
            .as_ref()
            .map(|value| BlobVersion::from_bytes(value.as_ref()))
            .transpose()
        {
            Ok(current) => current,
            Err(error) => return self.fail(error.into()),
        };
        if current != self.replaced_version {
            return self.fail(IncomingVersionError::StorageError(
                StorageError::TransactionConflict,
            ));
        }
        if self.advance_version_exists {
            return self.write_hash_lookup();
        }
        if current.is_some_and(|version| version.location_key().is_some()) {
            return self.read_replaced_envelope();
        }
        self.read_replaced_metadata()
    }

    /// Like a delete, a replacement removes the replaced envelope or pending copy and its charge.
    pub(super) fn read_replaced_envelope(&mut self) -> Turn {
        let key = match self.version_key_bytes() {
            Ok(key) => key,
            Err(error) => return self.fail(error.into()),
        };
        let bucket = self.manifest.bucket.as_bytes().to_vec();
        let reads = vec![
            (ABE_VERSION_KEYSPACE.to_string(), key.into()),
            (BUCKET_ENCRYPTION_KEYSPACE.to_string(), bucket.into()),
        ];
        self.replaced_reads = reads.clone();
        self.reads(IncomingVersionState::ReadReplacedEnvelope, reads)
    }

    pub(super) fn accept_replaced_envelope(&mut self, values: Vec<Option<Value>>) -> Turn {
        let asked = std::mem::take(&mut self.replaced_reads);
        let ([id, settings], [(_, key), _]) = (values.as_slice(), asked.as_slice()) else {
            return self.fail(PolicyGateError::InvalidEvent.into());
        };
        let reads = match id {
            Some(id) => {
                self.replaced_envelope = vec![(ABE_VERSION_KEYSPACE.to_string(), key.clone())];
                vec![
                    (ABE_ENVELOPE_KEYSPACE.to_string(), id.clone()),
                    (ABE_ARCHIVE_KEYSPACE.to_string(), id.clone()),
                ]
            }
            None => {
                let settings = match BucketEncryption::from_row(settings.as_deref()) {
                    Ok(settings) => settings,
                    Err(error) => return self.fail(error.into()),
                };
                let Some(bucket_id) = settings.bucket_id else {
                    return self.read_replaced_metadata();
                };
                let version = VersionKey::new(
                    &self.manifest.bucket,
                    &self.manifest.key,
                    self.manifest.version_id,
                );
                match copy_row(bucket_id, &version) {
                    Ok(row) => vec![(ABE_COPY_KEYSPACE.to_string(), row.into())],
                    Err(error) => return self.fail(error.into()),
                }
            }
        };
        self.replaced_reads = reads.clone();
        self.reads(IncomingVersionState::ReadReplacedRows, reads)
    }

    pub(super) fn accept_replaced_rows(&mut self, values: Vec<Option<Value>>) -> Turn {
        let asked = std::mem::take(&mut self.replaced_reads);
        let charge = match (values.as_slice(), asked.as_slice()) {
            ([pending], [(_, row)]) => {
                let Some(pending) = pending else {
                    return self.read_replaced_metadata();
                };
                self.replaced_envelope = vec![(ABE_COPY_KEYSPACE.to_string(), row.clone())];
                pending.len() as u64
            }
            ([envelope, archive], [(_, id), _]) => {
                self.replaced_envelope.extend([
                    (ABE_ENVELOPE_KEYSPACE.to_string(), id.clone()),
                    (ABE_ARCHIVE_KEYSPACE.to_string(), id.clone()),
                ]);
                envelope_charge(
                    envelope.as_deref().unwrap_or_default(),
                    archive.as_deref().unwrap_or_default(),
                )
            }
            _ => return self.fail(PolicyGateError::InvalidEvent.into()),
        };
        self.replaced_logical_bytes = self.replaced_logical_bytes.saturating_add(charge);
        self.read_replaced_metadata()
    }

    pub(super) fn accept_metadata_iterated(
        &mut self,
        (values, next_start_after): (Vec<(Key, Value)>, Option<Key>),
    ) -> Turn {
        if next_start_after.is_some() {
            return self.fail(IncomingVersionError::MultipartMetadataOverflow);
        }
        self.delete_replaced_metadata(values)
    }

    pub(super) fn accept_metadata_deleted(&mut self) -> Turn {
        match self.replaced_reclaim_key() {
            Some(key) => self.write_replaced_candidate(key),
            None => {
                self.replaced_version = None;
                self.write_hash_lookup()
            }
        }
    }
}
