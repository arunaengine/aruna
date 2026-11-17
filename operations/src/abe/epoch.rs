//! Marks and raises bucket epochs after lost READ scopes.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

/// Progress row value: the raised epoch, the phase (grants, then requests) and the last key.
pub(super) fn progress(epoch: u64, requests: bool, cursor: &[u8]) -> Vec<u8> {
    [&epoch.to_be_bytes()[..], &[u8::from(requests)], cursor].concat()
}

pub(super) fn parse_progress(value: &[u8]) -> Option<(u64, bool, Vec<u8>)> {
    let epoch = u64::from_be_bytes(value.get(..8)?.try_into().ok()?);
    match value.get(8)? {
        0 => Some((epoch, false, value[9..].to_vec())),
        1 => Some((epoch, true, value[9..].to_vec())),
        _ => None,
    }
}

impl KeyOperation {
    /// Raises the epoch and restarts reissue in one transaction; a due marker written meanwhile
    /// conflicts with it and stays due.
    pub(super) fn raise(&mut self, instant: bool) -> Effects {
        let Some(snapshot) = &self.snapshot else {
            return self.fail(KeyError::Missing);
        };
        let holder = snapshot.holder && self.auth.path_restrictions.is_none();
        if (instant || self.action == KeyAction::Rekey) && !holder {
            return self.fail(KeyError::Denied);
        }
        if !instant && !(snapshot.due && (holder || self.managed)) {
            self.result = Some(KeyResult::Epoch(snapshot.epoch));
            return self.flush();
        }
        match self.raised() {
            Ok(next) => {
                self.result = Some(KeyResult::Epoch(next));
                self.flush()
            }
            Err(error) => self.fail(error),
        }
    }
    /// Queues a raise by one in this transaction and moves the snapshot to the new epoch.
    fn raised(&mut self) -> Result<u64, KeyError> {
        let snapshot = self.snapshot.as_mut().ok_or(KeyError::Missing)?;
        let next = snapshot.epoch.checked_add(1).ok_or(AbeError::Limit)?;
        snapshot.epoch = next;
        snapshot.due = false;
        let bucket: Key = snapshot.parameters.key.bucket_id.to_bytes().to_vec().into();
        let row = progress(next, false, &[]);
        self.deletes
            .push((ABE_DUE_KEYSPACE.to_string(), bucket.clone()));
        self.writes.push((
            ABE_EPOCH_KEYSPACE.to_string(),
            bucket.clone(),
            next.to_be_bytes().to_vec().into(),
        ));
        self.writes
            .push((ABE_REISSUE_KEYSPACE.to_string(), bucket, row.into()));
        Ok(next)
    }
    /// Node issuance consumes a due raise first while the node can issue: its key is node managed
    /// or unlocked. Otherwise requests wait for a holder, whose run raises first.
    pub(super) fn due_check(&mut self) -> Effects {
        if self.managed {
            return self.raise_first();
        }
        let Some(snapshot) = &self.snapshot else {
            return self.fail(KeyError::Missing);
        };
        let bucket_id = snapshot.parameters.key.bucket_id;
        self.state = State::Unlock;
        smallvec![Effect::Blob(BlobEffect::ReadKeyStatus { bucket_id })]
    }
    pub(super) fn raise_first(&mut self) -> Effects {
        if let Err(error) = self.raised() {
            return self.fail(error);
        }
        self.prepare()
    }
}
