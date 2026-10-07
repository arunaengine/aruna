//! Publication of a rewritten copy: its rows, the old owner and usage, and the commit.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

impl RewriteVersionOperation {
    pub(super) fn handle_target(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::ReadResult { value, .. }) = event else {
            return self.unexpected(event);
        };
        match self.rows(value.map(|value| value.to_vec())) {
            Ok(writes) => {
                self.state = RewriteState::WriteRows;
                smallvec![Effect::Storage(StorageEffect::BatchWrite {
                    writes,
                    txn_id: self.txn_id,
                })]
            }
            Err(error) => self.fail(error),
        }
    }

    /// An existing plain copy in the target class is adopted; otherwise the new copy owns its
    /// row. The old copy goes to reclaim, and the transition waits for its removal.
    fn rows(
        &mut self,
        existing: Option<Vec<u8>>,
    ) -> Result<Vec<(String, Key, Value)>, RewriteError> {
        let (Some(old), Some(version)) = (self.old.as_ref(), self.version.as_ref()) else {
            return Err(RewriteError::NotFinished);
        };
        let new = self.new.clone().ok_or(RewriteError::NotFinished)?;
        let old_key = old.location_key()?;
        let (version, mut stored) = (version.clone(), None);
        let mut writes = Vec::new();
        let published = match existing {
            Some(value) => {
                let existing = BackendLocation::from_bytes(&value)?;
                self.owns_row = existing.same_object(&new);
                existing
            }
            None => {
                writes.push((
                    BLOB_LOCATIONS_KEYSPACE.to_string(),
                    new.location_key()?.to_bytes().into(),
                    new.to_bytes()?.into(),
                ));
                self.owns_row = true;
                stored =
                    Some(StoredDelta::for_location(&new, true).ok_or(RewriteError::NotFinished)?);
                new
            }
        };
        if published.format.bucket_key().is_some() {
            let owner = CopyOwner::new(ArchiveKey::of(&published), self.version_key.clone());
            writes.push((
                COPY_OWNER_KEYSPACE.to_string(),
                owner.key()?.into(),
                Vec::new().into(),
            ));
        }
        let (envelope, added) = self.envelope_writes(&published, &version.metadata)?;
        writes.extend(envelope);
        self.gate = self.quota_gate(added);
        self.usage = self.usage_with(stored, added);
        let mut moved = version;
        if let BlobVersionState::Materialized { encoding, .. } = &mut moved.state {
            *encoding = published.format.encoding();
        }
        if let Some(copy) = self.copy.as_ref() {
            let copy = ManagedCopyRecord {
                location: published,
                ..copy.clone()
            };
            writes.push((
                MANAGED_COPY_KEYSPACE.to_string(),
                copy.key().to_bytes()?.into(),
                copy.to_bytes()?.into(),
            ));
        }
        writes.push((
            BLOB_VERSIONS_KEYSPACE.to_string(),
            self.version_key.to_bytes()?.into(),
            moved.to_bytes()?.into(),
        ));
        let cleanup = cleanup_key(&self.version_key.bucket, &old_key.to_bytes());
        writes.push((
            TRANSITION_CLEANUP_KEYSPACE.to_string(),
            cleanup.into(),
            Vec::new().into(),
        ));
        let candidate =
            ReclaimCandidateKey::new(old_key.backend, old_key.encoding, old_key.blake3_hash);
        let enqueued = ReclaimCandidate {
            enqueued_at: self.now,
        };
        writes.push((
            BLOB_RECLAIM_KEYSPACE.to_string(),
            candidate.to_bytes().into(),
            enqueued.to_bytes()?.into(),
        ));
        Ok(writes)
    }

    /// The old archive loses this version as owner, so reclaim may free it once unpinned.
    pub(super) fn handle_rows(&mut self, event: Event) -> Effects {
        let Event::Storage(StorageEvent::BatchWriteResult { .. }) = event else {
            return self.unexpected(event);
        };
        let sealed = self
            .old
            .as_ref()
            .filter(|old| old.format.bucket_key().is_some());
        let Some(old) = sealed else {
            return self.drop_envelope();
        };
        let owner = CopyOwner::new(ArchiveKey::of(old), self.version_key.clone());
        match owner_delete_effect(&owner, self.txn_id) {
            Ok(effect) => {
                self.state = RewriteState::DropOwner;
                smallvec![effect]
            }
            Err(error) => self.fail(error.into()),
        }
    }

    pub(super) fn update_usage(&mut self) -> Effects {
        if let (Some(txn_id), Some(gate)) = (self.txn_id, self.gate.as_mut()) {
            self.state = RewriteState::Quota;
            return gate.start(txn_id);
        }
        let usage = self.usage.as_mut().filter(|usage| !usage.is_noop());
        let (Some(txn_id), Some(usage)) = (self.txn_id, usage) else {
            return self.commit();
        };
        self.state = RewriteState::UpdateUsage;
        usage.start(txn_id)
    }

    pub(super) fn handle_usage(&mut self, event: Event) -> Effects {
        let (Some(txn_id), Some(usage)) = (self.txn_id, self.usage.as_mut()) else {
            return self.fail(RewriteError::NotFinished);
        };
        match usage.step(event, txn_id) {
            Ok(Some(effects)) => effects,
            Ok(None) => self.commit(),
            Err(error) => self.fail(error.into()),
        }
    }

    fn commit(&mut self) -> Effects {
        let Some(txn_id) = self.txn_id else {
            return self.fail(RewriteError::NotFinished);
        };
        self.state = RewriteState::Commit;
        smallvec![Effect::Storage(StorageEffect::CommitTransaction { txn_id })]
    }

    pub(super) fn handle_commit(&mut self, event: Event) -> Effects {
        self.txn_id = None;
        match event {
            Event::Storage(StorageEvent::TransactionCommitted { .. }) => {
                self.output = Some(Ok(RewriteOutcome::Moved));
                self.release()
            }
            Event::Storage(StorageEvent::Error { error }) => {
                self.owns_row = false;
                self.fail(error.into())
            }
            other => self.unexpected(other),
        }
    }
}
