//! Registers an unmapped file whose key the bucket holds: as a second copy of a version with the
//! same bytes, or as another version of the key dated by the file.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::sync::Arc;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{
    BACKEND_VERSION_KEYSPACE, BLOB_VERSIONS_KEYSPACE, DELETE_CLAIM_KEYSPACE, RENAMED_ROW_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::structs::storage::backend_import::{
    ExternalObject, ImportedObject, claim_key, object_key, version_key,
};
use aruna_core::structs::storage::blob::{BackendLocation, ResolvedBackend, VersionKey};
use aruna_core::structs::storage::format::StoredLayout;
use ulid::Ulid;

use super::reads::{Candidate, CandidatesOperation, hash_call, listed_hash, note_call, noted};
use super::{
    Fence, ImportJob, ImportRun, ItemError, Outcome, complete_call, encode, import_version, item,
    moved_away, observed, put_failure, put_operation, rule_guard, shared_guards, sharing,
};
use crate::jobs::import_listing::ListingNote;
use crate::s3::object::put::{PutObjectError, PutObjectResult};

#[derive(Debug, PartialEq)]
enum Step {
    Candidates,
    Hash(Vec<Candidate>),
    Note,
    Put(Ulid, u64, bool),
    Complete,
}

type Turn = Result<Next<Outcome>, ItemError>;

/// An unmapped file with the bytes of a version of its key is a second copy of that version, so
/// the copy cleanup can let the key keep one file. A file with other bytes becomes another
/// version: the newest one is current, an older one joins the history.
#[derive(Debug, PartialEq)]
pub(super) struct RegisterTwinOperation {
    job: ImportJob,
    run: Arc<ImportRun>,
    object: ExternalObject,
    key: String,
    head: Option<Ulid>,
    resolved: ResolvedBackend,
    index: u64,
    now_ms: u64,
    step: Step,
}

impl RegisterTwinOperation {
    pub(super) fn new(
        job: ImportJob,
        run: Arc<ImportRun>,
        object: ExternalObject,
        (key, head): (String, Option<Ulid>),
        resolved: ResolvedBackend,
        index: u64,
    ) -> Self {
        Self {
            job,
            run,
            object,
            key,
            head,
            resolved,
            index,
            now_ms: 0,
            step: Step::Candidates,
        }
    }

    fn second_copy(
        &mut self,
        object: &ExternalObject,
        (stored, value, version_id): (aruna_core::types::Key, aruna_core::types::Value, Ulid),
        (mut location, version): (BackendLocation, Option<String>),
    ) -> Turn {
        let run = self.run.clone();
        let rule = &run.rule;
        location.format.layout = StoredLayout::Plain(location.ulid);
        let row = ImportedObject {
            key: Some(self.key.clone()),
            ..ImportedObject::new(version_id, object)
        };
        let mut guards = shared_guards(&run, &object.key);
        guards.push(rule_guard(rule)?);
        guards.push((BLOB_VERSIONS_KEYSPACE.to_string(), stored, Some(value)));
        // A file a release is deleting is no copy to keep.
        let deleting = version.as_deref().unwrap_or_default();
        let claim =
            claim_key(&location, deleting).map_err(|error| ItemError::System(error.to_string()))?;
        guards.push((DELETE_CLAIM_KEYSPACE.to_string(), claim, None));
        // The copy is found by its key like a renamed row, and keeps its file and version there.
        let prefix = VersionKey::object_prefix(&rule.bucket, &self.key)
            .map_err(|error| item(error.to_string()))?;
        let index = [
            &rule.import_id.to_bytes()[..],
            &prefix[..],
            object.key.as_bytes(),
        ]
        .concat();
        let file = location
            .to_bytes()
            .map_err(|error| ItemError::System(error.to_string()))?;
        let mut writes = vec![(RENAMED_ROW_KEYSPACE.to_string(), index.into(), file.into())];
        if let Some(version) = version {
            writes.push((
                BACKEND_VERSION_KEYSPACE.to_string(),
                version_key(location.ulid),
                version.into_bytes().into(),
            ));
        }
        let note = ListingNote {
            guards,
            retired: Vec::new(),
            writes,
            sharing: Some(sharing(&run)),
            fenced: Some(self.key.clone()),
        };
        let row_key = object_key(rule.import_id, &object.key);
        self.step = Step::Note;
        // A refused copy is looked at again by the next run.
        Ok(Next::Call(note_call(
            rule,
            row_key,
            None,
            encode(&row)?,
            note,
        )))
    }

    fn add_version(
        &mut self,
        object: &ExternalObject,
        file: (BackendLocation, Option<String>),
    ) -> Turn {
        let run = self.run.clone();
        let at = object.modified_ms.unwrap_or(self.now_ms);
        let ids = (self.index, self.now_ms);
        let version_id = import_version((&self.job, &run), ids, Some(at));
        let committed = ImportedObject {
            key: Some(self.key.clone()),
            ..ImportedObject::new(version_id, object)
        };
        let keep_head = self.head.is_some_and(|head| head > version_id);
        let fence = Fence {
            head: self.head,
            keep_head,
            guards: shared_guards(&run, &object.key),
            retired: Vec::new(),
            trail: None,
            keep_row: false,
            copy_index: None,
        };
        let size = file.0.blob_size;
        let key = self.key.clone();
        let put = put_operation((&self.job, &run), key, file, committed, fence);
        self.step = Step::Put(version_id, size, keep_head);
        match put {
            Ok(put) => Ok(Next::Call(IoCall::child(put))),
            Err(error) => self.put_ended(Err(error), (version_id, size, keep_head)),
        }
    }

    fn put_ended(
        &mut self,
        put: Result<PutObjectResult, PutObjectError>,
        (version_id, size, keep_head): (Ulid, u64, bool),
    ) -> Turn {
        match put {
            Ok(_) => {}
            // Another write came first; the next run looks again.
            Err(
                PutObjectError::HeadChanged
                | PutObjectError::VersionExists
                | PutObjectError::HistoryDeleted,
            ) => return Ok(Next::Done(Outcome::Skipped)),
            Err(error) if moved_away(&error) => return Ok(Next::Done(Outcome::Ignored)),
            Err(error) => return Err(put_failure(error)),
        }
        if keep_head {
            return Ok(Next::Done(Outcome::Imported));
        }
        self.step = Step::Complete;
        let run = self.run.clone();
        let key = self.key.clone();
        Ok(Next::Call(complete_call(
            (&self.job, &run),
            key,
            version_id,
            size,
        )))
    }
}

impl Operation for RegisterTwinOperation {
    type Output = Outcome;
    type Error = ItemError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn {
        let candidates = CandidatesOperation::new(&self.run.rule, &self.key, &self.object)?;
        Ok(Next::Call(IoCall::child(candidates)))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.step, Step::Complete) {
            Step::Candidates => {
                let candidates = result.child::<Vec<Candidate>, ItemError>()??;
                self.step = Step::Hash(candidates);
                let resolved = self.resolved.clone();
                Ok(Next::Call(hash_call(&self.job, resolved, &self.object)))
            }
            Step::Hash(candidates) => {
                let (location, version, verified) = listed_hash(result, &self.object)?;
                let object = observed(&self.object, verified);
                let same = (candidates.into_iter())
                    .find(|(.., hash)| location.get_blake3() == Some(&hash[..]));
                match same {
                    Some((stored, value, version_id, _)) => {
                        let found = (stored, value, version_id);
                        self.second_copy(&object, found, (location, version))
                    }
                    None => self.add_version(&object, (location, version)),
                }
            }
            Step::Note => Ok(Next::Done(match noted(result)? {
                true => Outcome::Imported,
                false => Outcome::Skipped,
            })),
            Step::Put(version_id, size, keep_head) => {
                let put = result.child::<PutObjectResult, PutObjectError>()?;
                self.put_ended(put, (version_id, size, keep_head))
            }
            Step::Complete => Ok(Next::Done(Outcome::Imported)),
        }
    }
}
