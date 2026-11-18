//! Imports one listed object: decides from its rows, reads and hashes it only when needed, and
//! commits its version with the row that records it.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use std::path::Path;
use std::sync::Arc;

use aruna_core::calls::{IoCall, IoResult, Next};
use aruna_core::keyspaces::{
    BLOB_VERSIONS_KEYSPACE, IMPORTED_OBJECT_KEYSPACE, RENAMED_ROW_KEYSPACE,
};
use aruna_core::operation::Operation;
use aruna_core::rows::values;
use aruna_core::structs::storage::backend_import::{ExternalObject, ImportedObject, object_key};
use aruna_core::structs::storage::blob::{
    BackendLocation, BackendRef, ResolvedBackend, VersionKey, ensure_confined_path,
};
use aruna_core::structs::storage::routing::{RoutingTarget, RuleSource};
use aruna_core::types::{Key, Value};
use ulid::Ulid;

use super::reads::{
    CommittedMd5Operation, TrailOperation, hash_call, head_call, head_of, note_call, noted,
    read_call, version_row,
};
use super::recover::RecoverOperation;
use super::twin::RegisterTwinOperation;
use super::{
    Decision, Fence, Hashed, ImportJob, ImportRun, ItemError, Outcome, bucket_key, claimed,
    complete_call, decide, encode, etag_differs, import_version, item, moved_away, needs_lookup,
    observed, page_skip, parse, put_failure, put_operation, reserved, shared_guards, taken,
    trusted_listing,
};
use crate::jobs::import_listing::ListingNote;
use crate::s3::object::put::{PutObjectError, PutObjectResult};

#[derive(Debug, PartialEq)]
enum Step {
    Row,
    Recover,
    Md5(Box<ImportedObject>),
    Noted,
    Head,
    Version,
    Twin,
    Trail,
    Hash,
    CopyIndex(Key),
    Put(u64),
    Complete,
}

/// Decides one listed object and imports it when it is new or differs. Writes and deletes in
/// Aruna win; a failure of this object alone never ends the run.
#[derive(Debug, PartialEq)]
pub(super) struct ImportObjectOperation {
    job: ImportJob,
    run: Arc<ImportRun>,
    object: ExternalObject,
    index: u64,
    now_ms: u64,
    row_key: Key,
    row: Option<ImportedObject>,
    stored: Option<Value>,
    resolved: Option<ResolvedBackend>,
    key: String,
    head: Option<Ulid>,
    version_id: Option<Ulid>,
    committed: Option<ImportedObject>,
    hashed: Option<(BackendLocation, Option<String>)>,
    step: Step,
}

type Turn = Result<Next<Outcome>, ItemError>;

impl ImportObjectOperation {
    pub(super) fn new(
        job: ImportJob,
        run: Arc<ImportRun>,
        object: ExternalObject,
        index: u64,
    ) -> Self {
        let row_key = object_key(run.rule.import_id, &object.key);
        Self {
            job,
            run,
            object,
            index,
            now_ms: 0,
            row_key,
            row: None,
            stored: None,
            resolved: None,
            key: String::new(),
            head: None,
            version_id: None,
            committed: None,
            hashed: None,
            step: Step::Row,
        }
    }

    fn resolved(&self) -> Result<ResolvedBackend, ItemError> {
        (self.resolved.clone()).ok_or_else(|| ItemError::System("no backend".to_string()))
    }

    /// Takes the row and the backend, then recovers an uncommitted write when one may exist.
    fn with_row(&mut self, result: IoResult) -> Turn {
        let [stored] = values::<1>(result)?;
        self.row = parse(stored.clone(), ImportedObject::from_bytes)?;
        self.stored = stored;
        let run = self.run.clone();
        let rule = &run.rule;
        // The bytes stay on the rule's backend, wherever new writes to the key are routed.
        let target = RoutingTarget::Backend(BackendRef::Group(rule.backend_id));
        let resolved = match run.routing.catalog.resolve_target(&target, RuleSource::Tenant) {
            Ok(Some(resolved)) => resolved,
            Ok(None) => return Err(item("the backend is not usable")),
            Err(error) => return Err(item(error.to_string())),
        };
        self.resolved = Some(resolved.clone());
        let intents = run.intents.get(&self.object.key).map_or(&[][..], Vec::as_slice);
        let names = (rule, run.earlier.as_slice());
        if !needs_lookup(names, &self.object, self.row.as_ref(), !intents.is_empty()) {
            return self.place();
        }
        self.step = Step::Recover;
        let recover = RecoverOperation::new(
            self.job.clone(),
            run.clone(),
            self.object.clone(),
            (self.row.clone(), self.stored.clone()),
            resolved,
        );
        Ok(Next::Call(IoCall::child(recover)))
    }

    /// Places the object at its key; a trusted listing only needs its committed version.
    fn place(&mut self) -> Turn {
        let run = self.run.clone();
        let rule = &run.rule;
        let names = (rule, run.earlier.as_slice(), run.discovery.as_slice());
        self.key = bucket_key(names, self.row.as_ref(), &self.object.key)?;
        if self.row.is_none()
            && let Some(outcome) = claimed(&run, &self.object.key)?
        {
            return Ok(Next::Done(outcome));
        }
        if self.key.is_empty() {
            return Err(item("the object maps to an empty key"));
        }
        ensure_confined_path(Path::new(&self.key)).map_err(|error| item(error.to_string()))?;
        // Only new or differing objects are read; a trusted listing needs its committed version
        // and, when listed with a single-part ETag, the MD5 of that version.
        let trusted = trusted_listing(self.row.as_ref(), &self.object);
        match (trusted, self.stored.is_some()) {
            (Some(listed), true) => {
                let md5 =
                    CommittedMd5Operation::new(&rule.bucket, &self.key, &listed, &self.object)?;
                self.step = Step::Md5(Box::new(listed));
                Ok(Next::Call(IoCall::child(md5)))
            }
            _ => self.read_head(),
        }
    }

    fn read_head(&mut self) -> Turn {
        self.step = Step::Head;
        Ok(Next::Call(head_call(&self.run.rule.bucket, &self.key)?))
    }

    fn decided(&mut self, version_exists: bool) -> Turn {
        let run = self.run.clone();
        match decide(&self.object, self.row.as_ref(), self.head, version_exists) {
            Decision::Skip => Ok(Next::Done(Outcome::Skipped)),
            Decision::Taken => {
                self.step = Step::Twin;
                let twin = RegisterTwinOperation::new(
                    self.job.clone(),
                    run,
                    self.object.clone(),
                    (self.key.clone(), self.head),
                    self.resolved()?,
                    self.index,
                );
                Ok(Next::Call(IoCall::child(twin)))
            }
            Decision::Import => self.begin_import(),
        }
    }

    /// The committed row stays until the new version replaces it in one transaction. A new row
    /// records its key, so later rule changes never move the object. A new file is dated by its
    /// modification time, as a second file of its key would be.
    fn begin_import(&mut self) -> Turn {
        let run = self.run.clone();
        let at = match &self.row {
            None => self.object.modified_ms,
            Some(_) => None,
        };
        let version_id = import_version((&self.job, &run), (self.index, self.now_ms), at);
        self.version_id = Some(version_id);
        self.committed = Some(ImportedObject {
            key: match self.row.as_ref() {
                Some(row) => row.key.clone(),
                None => Some(self.key.clone()),
            },
            ..ImportedObject::new(version_id, &self.object)
        });
        // Aruna's own write that lost its head returns only if no delete removed it on purpose.
        match self.keyed() {
            Some(kept) => {
                self.step = Step::Trail;
                let trail = TrailOperation::new(&run.rule, run.incarnation, &self.key, kept);
                Ok(Next::Call(IoCall::child(trail)))
            }
            None => self.hash(),
        }
    }

    /// The version of Aruna's own write whose key lost its head.
    fn keyed(&self) -> Option<Ulid> {
        (self.row.as_ref())
            .filter(|row| row.key.is_some() && self.head.is_none())
            .map(|row| row.version_id)
    }

    fn hash(&mut self) -> Turn {
        self.step = Step::Hash;
        Ok(Next::Call(hash_call(
            &self.job,
            self.resolved()?,
            &self.object,
        )))
    }

    fn hashed(&mut self, hashed: Hashed) -> Turn {
        let (location, version, verified) = hashed;
        if location.blob_size != self.object.size {
            return Err(item("the object changed while it was read"));
        }
        if let Some(committed) = self.committed.as_mut() {
            committed.etag = observed(&self.object, verified).etag;
        }
        self.hashed = Some((location, version));
        // The key index row of `path` when its row records a second copy, which this version
        // retires.
        let Some(key) = self.row.as_ref().and_then(|row| row.key.clone()) else {
            return self.put(None);
        };
        let rule = &self.run.rule;
        let prefix = VersionKey::object_prefix(&rule.bucket, &key)
            .map_err(|error| item(error.to_string()))?;
        let index: Key = [
            &rule.import_id.to_bytes()[..],
            &prefix[..],
            self.object.key.as_bytes(),
        ]
        .concat()
        .into();
        self.step = Step::CopyIndex(index.clone());
        Ok(Next::Call(read_call([(RENAMED_ROW_KEYSPACE, index)])))
    }

    fn put(&mut self, copy_index: Option<Key>) -> Turn {
        let run = self.run.clone();
        let lost = || ItemError::System("the object lost its state".to_string());
        let file = self.hashed.take().ok_or_else(lost)?;
        let committed = self.committed.clone().ok_or_else(lost)?;
        let size = file.0.blob_size;
        let fence = Fence {
            head: self.head,
            keep_head: false,
            guards: match self.row {
                Some(_) => Vec::new(),
                None => shared_guards(&run, &self.object.key),
            },
            retired: Vec::new(),
            trail: self.keyed(),
            keep_row: false,
            copy_index,
        };
        let put = put_operation((&self.job, &run), self.key.clone(), file, committed, fence);
        self.step = Step::Put(size);
        match put {
            Ok(put) => Ok(Next::Call(IoCall::child(put))),
            Err(error) => self.put_ended(Err(error), size),
        }
    }

    fn put_ended(&mut self, put: Result<PutObjectResult, PutObjectError>, size: u64) -> Turn {
        match put {
            Ok(_) => {}
            // An Aruna write or delete marker came first and keeps the key.
            Err(PutObjectError::HeadChanged) if self.row.is_none() => {
                return Err(taken(&self.key));
            }
            Err(PutObjectError::HeadChanged | PutObjectError::HistoryDeleted) => {
                return Ok(Next::Done(Outcome::Skipped));
            }
            Err(error) if moved_away(&error) => return Ok(Next::Done(Outcome::Ignored)),
            Err(error) => return Err(put_failure(error)),
        }
        let version_id = (self.version_id)
            .ok_or_else(|| ItemError::System("the object lost its version".to_string()))?;
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

impl Operation for ImportObjectOperation {
    type Output = Outcome;
    type Error = ItemError;

    fn stamp(&mut self, now_ms: u64) {
        self.now_ms = now_ms;
    }

    fn first(&mut self) -> Turn {
        let run = self.run.clone();
        let rule = &run.rule;
        let object = &self.object;
        if rule.aruna_key(&object.key).is_none() || run.backend.is_reserved(&object.key) {
            return Ok(Next::Done(Outcome::Ignored));
        }
        // An empty file a write's intent names is that write's reservation, in progress.
        if object.size == 0 && reserved(rule, run.intents.get(&object.key)) {
            return Ok(Next::Done(Outcome::Skipped));
        }
        let names = (rule, run.earlier.as_slice());
        let intents = run.intents.contains_key(&object.key);
        if page_skip(names, &run.page, intents, object) {
            return Ok(Next::Done(Outcome::Skipped));
        }
        let row = (IMPORTED_OBJECT_KEYSPACE, self.row_key.clone());
        Ok(Next::Call(read_call([row])))
    }

    fn next(&mut self, result: IoResult) -> Turn {
        match std::mem::replace(&mut self.step, Step::Complete) {
            Step::Row => self.with_row(result),
            Step::Recover => match result.child::<Option<Outcome>, ItemError>()?? {
                Some(outcome) => Ok(Next::Done(outcome)),
                None => self.place(),
            },
            Step::Md5(listed) => {
                let md5 = result.child::<Option<Option<Vec<u8>>>, ItemError>()??;
                let Some(md5) = md5 else {
                    return self.read_head();
                };
                if etag_differs(self.object.etag.as_deref(), md5.as_deref()) {
                    return self.read_head();
                }
                let encoded = encode(&listed)?;
                let seen = self.stored.clone();
                if seen.as_deref() == Some(encoded.as_slice()) {
                    return Ok(Next::Done(Outcome::Skipped));
                }
                self.step = Step::Noted;
                Ok(Next::Call(note_call(
                    &self.run.rule,
                    self.row_key.clone(),
                    seen,
                    encoded,
                    ListingNote::default(),
                )))
            }
            Step::Noted => {
                noted(result)?;
                Ok(Next::Done(Outcome::Skipped))
            }
            Step::Head => {
                self.head = head_of(result)?;
                match (&self.row, self.head) {
                    (Some(row), None) => {
                        let bucket = &self.run.rule.bucket;
                        let version = version_row(bucket, &self.key, row.version_id)?;
                        self.step = Step::Version;
                        Ok(Next::Call(read_call([(BLOB_VERSIONS_KEYSPACE, version)])))
                    }
                    _ => self.decided(false),
                }
            }
            Step::Version => {
                let [version] = values::<1>(result)?;
                self.decided(version.is_some())
            }
            Step::Twin => Ok(Next::Done(result.child::<Outcome, ItemError>()??)),
            Step::Trail => match result.child::<bool, ItemError>()?? {
                true => Ok(Next::Done(Outcome::Skipped)),
                false => self.hash(),
            },
            Step::Hash => {
                let hashed = super::reads::hashed_of(result, &self.object)?;
                self.hashed(hashed)
            }
            Step::CopyIndex(index) => {
                let [value] = values::<1>(result)?;
                let index = value.filter(|value| !value.is_empty()).map(|_| index);
                self.put(index)
            }
            Step::Put(size) => {
                let put = result.child::<PutObjectResult, PutObjectError>()?;
                self.put_ended(put, size)
            }
            // A failed queue write after the commit leaves the obligation for repair.
            Step::Complete => Ok(Next::Done(Outcome::Imported)),
        }
    }
}
